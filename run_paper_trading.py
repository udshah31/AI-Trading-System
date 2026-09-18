#!/usr/bin/env python3
"""
Paper Trading Runner
Starts all agents in paper trading mode
"""
import asyncio
import os
import sys
import signal
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from hybrid.messaging import MessageBus
from hybrid.config import HybridConfig
from hybrid.agent_base import BaseAgent
from hybrid.data_agent import DataAgent
from hybrid.quant_agent import QuantAgent
from hybrid.risk_agent import RiskAgent
from hybrid.execution_agent import ExecutionAgent
from hybrid.orchestrator import Orchestrator
from hybrid.sniper_bot import SniperBot
from hybrid.llm_agent import create_llm_agents
from hybrid.dashboard.app import app as dashboard_app
import uvicorn


class PaperTradingSystem:
    def __init__(self):
        self.config = HybridConfig()
        self.bus = MessageBus("redis://localhost:6379")
        self.agents = {}
        self.dashboard_server = None
        self.running = False
    
    async def initialize(self):
        print("=" * 60)
        print("INITIALIZING PAPER TRADING SYSTEM")
        print("=" * 60)
        
        # Core agents
        self.agents['data'] = DataAgent(
            self.bus, 
            symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"],
            exchange_id="kraken"
        )
        
        self.agents['quant'] = QuantAgent(self.bus, self.config)
        self.agents['risk'] = RiskAgent(self.bus, self.config)
        self.agents['execution'] = ExecutionAgent(self.bus, self.config, dry_run=True)
        self.agents['orchestrator'] = Orchestrator(self.bus, self.config)
        
        # LLM agents
        llm_agents = create_llm_agents(self.bus, self.config)
        self.agents.update(llm_agents)
        
        # Sniper bot (optional - enable with env var)
        if os.getenv("ENABLE_SNIPER", "false").lower() == "true":
            self.agents['sniper'] = SniperBot(
                self.bus, self.config,
                chains=["solana", "base"],
                max_position_usd=100
            )
        
        # Start all agents
        for name, agent in self.agents.items():
            print(f"Starting {name}...")
            await agent.start()
        
        print("\n✅ All agents started")
        print("📊 Dashboard: http://localhost:8000")
        print("📈 Prometheus: http://localhost:9090")
        print("📉 Grafana: http://localhost:3000")
        print("\nPress Ctrl+C to stop\n")
    
    async def start_dashboard(self):
        """Start FastAPI dashboard in background"""
        config = uvicorn.Config(
            dashboard_app, 
            host="0.0.0.0", 
            port=8001, 
            log_level="warning"
        )
        self.dashboard_server = uvicorn.Server(config)
        await self.dashboard_server.serve()
    
    async def run(self):
        self.running = True
        
        # Start dashboard in background
        # dashboard_task = asyncio.create_task(self.start_dashboard())
        
        # Schedule periodic analysis
        analysis_task = asyncio.create_task(self._analysis_loop())
        
        # Wait for shutdown
        try:
            await asyncio.gather(analysis_task)
        except asyncio.CancelledError:
            pass
    
    async def _analysis_loop(self):
        """Trigger analysis every 5 minutes"""
        while self.running:
            await asyncio.sleep(300)  # 5 minutes
            
            if not self.running:
                break
            
            print("\n🔄 Running scheduled analysis...")
            orchestrator = self.agents.get('orchestrator')
            if orchestrator:
                await orchestrator.analyze("BTC/USDT")
                await orchestrator.analyze("ETH/USDT")
                await orchestrator.analyze("SOL/USDT")
    
    async def shutdown(self):
        print("\n🛑 Shutting down...")
        self.running = False
        
        for name, agent in self.agents.items():
            print(f"Stopping {name}...")
            try:
                await agent.stop()
            except Exception as e:
                print(f"Error stopping {name}: {e}")
        
        if self.dashboard_server:
            self.dashboard_server.should_exit = True
        
        print("✅ Shutdown complete")


async def main():
    system = PaperTradingSystem()
    
    # Handle signals
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
        print("Add them to your .env file or export them")
        sys.exit(1)
    
    print("🚀 Starting Paper Trading System...")
    asyncio.run(main())
