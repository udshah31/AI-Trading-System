"""
Main Async Entry Point
Runs all trading agents in paper trading mode
"""
import asyncio
import inspect
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from hybrid.messaging import Channel, MessageBus
from hybrid.config import HybridConfig, live_trading_enabled
from hybrid.data_agent import DataAgent
from hybrid.quant_agent import QuantAgent
from hybrid.risk_agent import RiskAgent
from hybrid.execution_agent import ExecutionAgent
from hybrid.orchestrator import Orchestrator, RedisHoldingsStore, holdings_key
from hybrid.sniper_bot import SniperBot
from hybrid.llm_agent import create_llm_agents
from hybrid.llm_schedule import TRADED_PAIRS, llm_analysis_loop, llm_settings
from hybrid.storage import StorageService
from hybrid.storage_agent import StorageAgent
from hybrid.strategies.btc_funding import create_btc_funding_strategy
from hybrid.kraken_executor import (
    KrakenConfig,
    KrakenEnvironment,
    KrakenExecutor,
)

import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://trader:secret@localhost:5432/trading"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
ANALYSIS_INTERVAL_S = 300
LEARNING_INTERVAL_S = 24 * 3600


def broker_choice() -> str:
    """BROKER=alpaca (default: Alpaca paper trading) or BROKER=kraken."""
    return (os.getenv("BROKER") or "alpaca").strip().lower()


def build_broker():
    """The Alpaca paper broker from the environment; exits with a clear message if misconfigured."""
    if live_trading_enabled():
        raise SystemExit("LIVE_TRADING=true isn't supported with BROKER=alpaca: this system runs Alpaca "
                         "in paper mode only. Unset LIVE_TRADING to paper trade.")
    key, secret = os.getenv("ALPACA_API_KEY"), os.getenv("ALPACA_SECRET_KEY")
    if not key or not secret:
        raise SystemExit("BROKER=alpaca needs ALPACA_API_KEY and ALPACA_SECRET_KEY (your Alpaca paper-trading "
                         "keys) in .env")
    from hybrid.brokers.alpaca import AlpacaBroker
    return AlpacaBroker(key, secret, paper=True)


class PaperTradingSystem:
    def __init__(self):
        self.config = HybridConfig()
        self.bus = MessageBus(REDIS_URL)
        self.agents = {}
        self.background_tasks = []
        self.kraken_spot = None
        self.kraken_futures = None
        self.broker_name = broker_choice()
        self.storage = None
        self.running = False
    
    async def initialize(self):
        print("=" * 60)
        print("INITIALIZING PAPER TRADING SYSTEM + BTC FUNDING STRATEGY")
        print("=" * 60)
        
        # Initialize storage (PostgreSQL)
        try:
            self.storage = StorageService(DATABASE_URL)
            await self.storage.initialize()
            print("[Storage] PostgreSQL connected, tables ready")
        except Exception as e:
            print(f"[Storage] PostgreSQL unavailable ({e}) — persistence disabled")
            self.storage = None
        
        # Start the message bus listener (must happen for inter-agent messages)
        await self.bus.start()
        print("[Bus] Redis pub/sub listener started")
        
        dry_run = not live_trading_enabled()
        broker = None
        if self.broker_name == "alpaca":
            broker = build_broker()
            print("[System] Broker: Alpaca PAPER trading (orders go to your Alpaca paper account; "
                  "prices from Kraken's public feed)")
        else:
            await self._init_kraken(dry_run)

        # Core agents
        self.agents['data'] = DataAgent(self.bus, symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"], exchange_id="kraken")
        self.llm_settings = llm_settings()
        self.agents['quant'] = QuantAgent(self.bus, self.config,
                                          llm_max_age_hours=self.llm_settings.max_age_hours)
        self.agents['risk'] = RiskAgent(self.bus, self.config)
        if self.storage:
            self.agents['risk'].set_storage(self.storage)
        if broker is not None:
            # a paper account has real state: balance checks and crash recovery apply
            self.agents['execution'] = ExecutionAgent(self.bus, self.config, dry_run=True, broker=broker)
            self.agents['orchestrator'] = Orchestrator(
                self.bus, self.config, broker=broker,
                store=RedisHoldingsStore(self.bus.client, holdings_key(True, broker="alpaca")))
        else:
            self.agents['execution'] = ExecutionAgent(self.bus, self.config, dry_run=dry_run)
            # live only: dry-run positions are simulated and don't exist on Kraken
            self.agents['orchestrator'] = Orchestrator(
                self.bus, self.config, exchange=None if dry_run else self.kraken_spot,
                store=RedisHoldingsStore(self.bus.client, holdings_key(dry_run)))
        await self._init_remaining_agents()

    async def _init_kraken(self, dry_run: bool):
        """Kraken clients (dry-run unless LIVE_TRADING=true); futures only for the funding strategy."""
        print(f"[System] Broker: Kraken, order mode {'DRY-RUN (simulated)' if dry_run else 'LIVE — REAL ORDERS'}")
        self.kraken_spot = KrakenExecutor(KrakenConfig(
            api_key=os.getenv("KRAKEN_API_KEY") or "",
            api_secret=os.getenv("KRAKEN_API_SECRET") or "",
            environment=KrakenEnvironment.SPOT,
            dry_run=dry_run,
        ))
        await self.kraken_spot.initialize()
        
        if os.getenv("KRAKEN_FUTURES_API_KEY"):
            self.kraken_futures = KrakenExecutor(KrakenConfig(
                api_key=os.getenv("KRAKEN_FUTURES_API_KEY") or "",
                api_secret=os.getenv("KRAKEN_FUTURES_API_SECRET") or "",
                passphrase=os.getenv("KRAKEN_FUTURES_PASSPHRASE", ""),
                environment=KrakenEnvironment.FUTURES,
                dry_run=dry_run,
            ))
            await self.kraken_futures.initialize()
        else:
            print("[System] Futures API not configured — BTC funding strategy disabled")

    async def _init_remaining_agents(self):
        # Storage agent (persists signals/trades/market data to PostgreSQL)
        if self.storage:
            self.agents['storage'] = StorageAgent(self.bus, self.storage)
        
        # LLM agents
        llm_agents = create_llm_agents(self.bus, self.config)
        self.agents.update(llm_agents)
        
        # BTC Funding Strategy (needs a real Kraken futures client for the perp hedge; not with Alpaca)
        if self.kraken_futures:
            self.agents['btc_funding'] = await create_btc_funding_strategy(
                self.bus, self.config, self.kraken_spot, self.kraken_futures,
                min_funding_bps=1.0,
                max_position_usd=5000.0,
                stop_loss_basis_bps=200.0
            )
        
        # Sniper bot (optional)
        if os.getenv("ENABLE_SNIPER", "false").lower() == "true":
            self.agents['sniper'] = SniperBot(self.bus, self.config, chains=["solana", "base"])
        
        # Start agents that have blocking start() methods as background tasks
        self.background_tasks = []
        
        # Start agents that have blocking start() methods
        for name, agent in self.agents.items():
            if hasattr(agent, 'start'):
                print(f"Starting {name}...")
                # Start agents with blocking start() as background tasks
                task = asyncio.create_task(agent.start())
                self.background_tasks.append(task)
        
        # Give background tasks a moment to start
        await asyncio.sleep(1)
        
        print("\n✅ All agents started")
        print("📊 Dashboard: http://localhost:8000")
        print(f"📈 BTC Funding Strategy: {'ACTIVE' if 'btc_funding' in self.agents else 'DISABLED'}")
        print("\nPress Ctrl+C to stop\n")
    
    async def run(self):
        self.running = True
        self._start_time = time.time()
        
        # Schedule periodic analysis
        analysis_task = asyncio.create_task(self._analysis_loop())
        llm_task = asyncio.create_task(llm_analysis_loop(self.bus, TRADED_PAIRS, self.llm_settings))
        learning_task = asyncio.create_task(self._learning_loop())
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        equity_snapshot_task = asyncio.create_task(self._equity_snapshot_loop())
        self.background_tasks.extend([analysis_task, llm_task, heartbeat_task, equity_snapshot_task,
                                      learning_task])
        
        try:
            # Wait for shutdown signal
            while self.running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
    
    async def _publish_status(self):
        """Mode and analysis schedule for the dashboard's decision band"""
        mode = "paper" if self.broker_name == "alpaca" else "live" if live_trading_enabled() else "dry_run"
        await self.bus.client.set("system:mode", mode)
        await self.bus.client.set("system:analysis_interval", str(ANALYSIS_INTERVAL_S))
    
    async def _heartbeat_loop(self):
        """Publish agent heartbeats to Redis for the dashboard every 10s"""
        while self.running:
            now = time.time()
            try:
                await self._publish_status()
            except Exception:
                pass
            for name in list(self.agents.keys()):
                try:
                    await self.bus.client.set(f"agent:heartbeat:{name}", str(now))
                    await self.bus.client.set(
                        f"agent:uptime:{name}",
                        str(now - getattr(self, "_start_time", now))
                    )
                except Exception:
                    pass
            await asyncio.sleep(10)
    
    async def _analysis_loop(self):
        """Trigger analysis every ANALYSIS_INTERVAL_S"""
        while self.running:
            await asyncio.sleep(ANALYSIS_INTERVAL_S)
            if not self.running:
                break
            
            orchestrator = self.agents.get('orchestrator')
            if orchestrator:
                await self._apply_active_weights()
                print("\n🔄 Running scheduled analysis...")
                try:
                    await self.bus.client.set("system:last_analysis", str(time.time()))
                except Exception:
                    pass
                for pair in TRADED_PAIRS:
                    await orchestrator.analyze(pair)
    
    async def _apply_active_weights(self):
        """Use the technical weights last approved on the dashboard (none: keep the starting ones)."""
        if not self.storage:
            return
        try:
            approved = await self.storage.learning.active_weights()
        except Exception as e:
            print(f"[Learning] Could not read approved weights: {e}")
            return
        current = self.config.tech_weights()
        if approved and any(abs(approved[k] - current.get(k, 0)) > 1e-9 for k in approved):
            self.config.set_tech_weights(approved)
            print(f"[Learning] Using approved weights: {approved}")
    
    async def _learning_loop(self):
        """Once a day: record outcomes, re-fit, store a proposal for approval (never applied here)."""
        from hybrid.learning import run_learning_cycle
        await asyncio.sleep(600)  # let the system settle first
        while self.running:
            if self.storage:
                try:
                    await run_learning_cycle(self.storage, self.config)
                except Exception as e:
                    print(f"[Learning] Cycle failed: {e}")
            await asyncio.sleep(LEARNING_INTERVAL_S)
    
    async def _equity_snapshot_loop(self):
        """Publish equity history snapshots to the dashboard every 60s"""
        while self.running:
            try:
                if self.storage:
                    equity_data = await self.storage.get_equity_history("default", days=30)
                    if equity_data:
                        labels = [p.timestamp.isoformat() for p in equity_data]
                        data = [float(p.equity) for p in equity_data]
                        self.bus.publish(Channel.SIGNALS, {
                            "type": "equity_history",
                            "data": {"labels": labels, "data": data}
                        }, "orchestrator")
            except Exception as e:
                print(f"[EquitySnapshot] Error: {e}")
            await asyncio.sleep(60)
    
    async def shutdown(self):
        if getattr(self, "_shutdown_done", False):
            return
        self._shutdown_done = True
        print("\n🛑 Shutting down...")
        self.running = False
        
        # Cancel all background tasks
        for task in self.background_tasks:
            task.cancel()
        
        # Wait for background tasks to finish
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        for name, agent in self.agents.items():
            print(f"Stopping {name}...")
            try:
                if hasattr(agent, 'stop'):
                    result = agent.stop()
                    if inspect.isawaitable(result):
                        await asyncio.wait_for(result, timeout=8.0)
            except asyncio.TimeoutError:
                print(f"Timeout stopping {name} (skipped)")
            except Exception as e:
                print(f"Error stopping {name}: {e}")
        
        if self.kraken_spot:
            await self.kraken_spot.close()
        if self.kraken_futures and self.kraken_futures != self.kraken_spot:
            await self.kraken_futures.close()
        
        try:
            await self.bus.stop()
        except Exception as e:
            print(f"Error stopping bus: {e}")
        
        if self.storage:
            try:
                await self.storage.close()
            except Exception as e:
                print(f"Error closing storage: {e}")
        
        print("✅ Shutdown complete")


async def main():
    system = PaperTradingSystem()
    
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(system.shutdown()))
    
    try:
        await system.initialize()
        await system.run()
    except KeyboardInterrupt:
        await system.shutdown()


if __name__ == "__main__":
    # Check required env vars
    required = (["ALPACA_API_KEY", "ALPACA_SECRET_KEY"] if broker_choice() == "alpaca"
                else ["KRAKEN_API_KEY", "KRAKEN_API_SECRET"])
    missing = [v for v in required if not os.getenv(v)]
    
    if missing:
        print(f"❌ Missing required environment variables: {missing}")
        print("Add them to your .env file")
        sys.exit(1)
    
    print(f"🚀 Starting the trading system (broker: {broker_choice()})...")
    asyncio.run(main())
