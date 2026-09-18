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

from hybrid.messaging import MessageBus
from hybrid.config import HybridConfig
from hybrid.data_agent import DataAgent
from hybrid.quant_agent import QuantAgent
from hybrid.risk_agent import RiskAgent
from hybrid.execution_agent import ExecutionAgent
from hybrid.orchestrator import Orchestrator
from hybrid.sniper_bot import SniperBot
from hybrid.llm_agent import create_llm_agents
from hybrid.storage import StorageService
from hybrid.storage_agent import StorageAgent
from hybrid.strategies.btc_funding import create_btc_funding_strategy
from hybrid.kraken_executor import KrakenExecutor, KrakenConfig, KrakenEnvironment

import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://trader:secret@localhost:5432/trading"
)


class PaperTradingSystem:
    def __init__(self):
        self.config = HybridConfig()
        self.bus = MessageBus("redis://localhost:6379")
        self.agents = {}
        self.background_tasks = []
        self.kraken_spot = None
        self.kraken_futures = None
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
        
        # Initialize Kraken clients
        self.kraken_spot = KrakenExecutor(KrakenConfig(
            api_key=os.getenv("KRAKEN_API_KEY"),
            api_secret=os.getenv("KRAKEN_API_SECRET"),
            environment=KrakenEnvironment.SPOT
        ))
        await self.kraken_spot.initialize()
        
        if os.getenv("KRAKEN_FUTURES_API_KEY"):
            self.kraken_futures = KrakenExecutor(KrakenConfig(
                api_key=os.getenv("KRAKEN_FUTURES_API_KEY"),
                api_secret=os.getenv("KRAKEN_FUTURES_API_SECRET"),
                passphrase=os.getenv("KRAKEN_FUTURES_PASSPHRASE", ""),
                environment=KrakenEnvironment.FUTURES
            ))
            await self.kraken_futures.initialize()
        else:
            print("[System] Futures API not configured, using spot for both (paper mode)")
            self.kraken_futures = self.kraken_spot
        
        # Core agents
        self.agents['data'] = DataAgent(self.bus, symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"], exchange_id="kraken")
        self.agents['quant'] = QuantAgent(self.bus, self.config)
        self.agents['risk'] = RiskAgent(self.bus, self.config)
        self.agents['execution'] = ExecutionAgent(self.bus, self.config, dry_run=True)
        self.agents['orchestrator'] = Orchestrator(self.bus, self.config)
        
        # Storage agent (persists signals/trades/market data to PostgreSQL)
        if self.storage:
            self.agents['storage'] = StorageAgent(self.bus, self.storage)
        
        # LLM agents
        llm_agents = create_llm_agents(self.bus, self.config)
        self.agents.update(llm_agents)
        
        # BTC Funding Strategy
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
        print("📈 BTC Funding Strategy: ACTIVE")
        print("\nPress Ctrl+C to stop\n")
    
    async def run(self):
        self.running = True
        self._start_time = time.time()
        
        # Schedule periodic analysis
        analysis_task = asyncio.create_task(self._analysis_loop())
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        equity_snapshot_task = asyncio.create_task(self._equity_snapshot_loop())
        self.background_tasks.extend([analysis_task, heartbeat_task, equity_snapshot_task])
        
        try:
            # Wait for shutdown signal
            while self.running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
    
    async def _heartbeat_loop(self):
        """Publish agent heartbeats to Redis for the dashboard every 10s"""
        while self.running:
            now = time.time()
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
        """Trigger analysis every 5 minutes"""
        while self.running:
            await asyncio.sleep(300)
            if not self.running:
                break
            
            orchestrator = self.agents.get('orchestrator')
            if orchestrator:
                print("\n🔄 Running scheduled analysis...")
                await orchestrator.analyze("BTC/USDT")
                await orchestrator.analyze("ETH/USDT")
    
    async def _equity_snapshot_loop(self):
        """Publish equity history snapshots to the dashboard every 60s"""
        while self.running:
            try:
                if self.storage:
                    equity_data = await self.storage.get_equity_history("default", days=30)
                    if equity_data:
                        labels = [p.timestamp.isoformat() for p in equity_data]
                        data = [float(p.equity) for p in equity_data]
                        await self.bus.publish("signals", {
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
    required = ["KRAKEN_API_KEY", "KRAKEN_API_SECRET"]
    missing = [v for v in required if not os.getenv(v)]
    
    if missing:
        print(f"❌ Missing required environment variables: {missing}")
        print("Add them to your .env file")
        sys.exit(1)
    
    print("🚀 Starting Paper Trading System with BTC Funding Strategy...")
    asyncio.run(main())
