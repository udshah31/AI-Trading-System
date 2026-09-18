"""
Sniper Bot - DEX Version
========================
Monitors DEXs for new token launches and executes via Jupiter/Uniswap.
"""

import asyncio
import aiohttp
from datetime import datetime
from typing import Optional
from dataclasses import dataclass

from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel
from hybrid.config import HybridConfig
from hybrid.dex_executor import (
    MultiChainExecutor, SniperExecutionEngine,
    Chain, QuoteRequest, DexExecutorFactory
)


@dataclass
class TokenInfo:
    address: str
    symbol: str
    name: str
    chain: str
    liquidity_usd: float
    volume_24h: float
    price_usd: float
    age_minutes: int
    created_at: datetime
    dex: str
    pair_address: str


class DexScreenerClient:
    BASE_URL = "https://api.dexscreener.com/latest/dex"
    
    def __init__(self, min_liquidity: float = 10_000, max_age_minutes: int = 60):
        self.min_liquidity = min_liquidity
        self.max_age_minutes = max_age_minutes
        self.session: Optional[aiohttp.ClientSession] = None
        self.seen_pairs = set()
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    async def fetch_new_pairs(self, chain: str = "solana") -> list[TokenInfo]:
        if not self.session:
            return []
        
        url = f"{self.BASE_URL}/pairs/{chain}"
        try:
            async with self.session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
        except Exception as e:
            print(f"[Sniper] API error: {e}")
            return []
        
        tokens = []
        for pair in data.get("pairs", []):
            try:
                pair_addr = pair.get("pairAddress", "")
                if pair_addr in self.seen_pairs:
                    continue
                
                created_ts = pair.get("pairCreatedAt", 0)
                if created_ts == 0:
                    continue
                created_at = datetime.fromtimestamp(created_ts / 1000)
                age_min = (datetime.now() - created_at).total_seconds() / 60
                
                if age_min > self.max_age_minutes:
                    continue
                
                liquidity = float(pair.get("liquidity", {}).get("usd", 0))
                if liquidity < self.min_liquidity:
                    continue
                
                base_token = pair.get("baseToken", {})
                quote_token = pair.get("quoteToken", {})
                
                quote_symbol = quote_token.get("symbol", "").upper()
                if quote_symbol not in ["USDC", "USDT", "SOL", "WETH", "ETH"]:
                    continue
                
                token = TokenInfo(
                    address=base_token.get("address", ""),
                    symbol=base_token.get("symbol", "UNKNOWN"),
                    name=base_token.get("name", "Unknown"),
                    chain=chain,
                    liquidity_usd=liquidity,
                    volume_24h=float(pair.get("volume", {}).get("h24", 0)),
                    price_usd=float(pair.get("priceUsd", 0)),
                    age_minutes=int(age_min),
                    created_at=created_at,
                    dex=pair.get("dexId", "unknown"),
                    pair_address=pair_addr
                )
                
                self.seen_pairs.add(pair_addr)
                tokens.append(token)
            except Exception:
                continue
        
        return tokens


class SniperBot(BaseAgent):
    def __init__(
        self,
        bus: MessageBus,
        config: HybridConfig,
        chains: list[str] = None,
        min_liquidity: float = 10_000,
        max_position_usd: float = 500,
        slippage_bps: int = 300,
        take_profit_pct: float = 0.5,
        stop_loss_pct: float = 0.3
    ):
        super().__init__("sniper_bot", bus)
        self.config = config
        self.chains = chains or ["solana", "base"]
        self.min_liquidity = min_liquidity
        self.max_position_usd = max_position_usd
        self.slippage_bps = slippage_bps
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        
        self.executor: Optional[MultiChainExecutor] = None
        self.sniper_engine: Optional[SniperExecutionEngine] = None
        self.dex_client: Optional[DexScreenerClient] = None
        
        self.running = False
        self.monitor_task: Optional[asyncio.Task] = None
        self.exit_task: Optional[asyncio.Task] = None
        
        self.bus.subscribe(Channel.SIGNALS, self._on_command)
    
    async def _on_command(self, payload: dict):
        if payload.get("type") != "command":
            return
        
        cmd = payload["data"].get("command")
        if cmd == "status":
            await self._send_status()
        elif cmd == "stop":
            await self.stop()
        elif cmd == "positions":
            await self._send_positions()
    
    async def _send_status(self):
        self.bus.publish(Channel.SIGNALS, {
            "type": "status_response",
            "source": self.name,
            "data": {
                "running": self.running,
                "chains": self.chains,
                "active_positions": len(self.sniper_engine.active_positions) if self.sniper_engine else 0
            }
        }, self.name)
    
    async def _send_positions(self):
        positions = []
        if self.sniper_engine:
            for addr, pos in self.sniper_engine.active_positions.items():
                positions.append({
                    "token": pos["token"].symbol,
                    "chain": pos["chain"],
                    "entry_price": pos["entry_price"],
                    "tx_hash": pos["tx_hash"]
                })
        
        self.bus.publish(Channel.SIGNALS, {
            "type": "positions_response",
            "source": self.name,
            "data": {"positions": positions}
        }, self.name)
    
    async def start(self):
        print(f"[Sniper] Starting on chains: {self.chains}")
        
        executor_config = {}
        
        import os
        if "solana" in self.chains:
            executor_config[Chain.SOLANA] = {
                "rpc_url": os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"),
                "private_key": os.getenv("SOLANA_PRIVATE_KEY"),
                "priority_fee_lamports": 200_000
            }
        
        if "base" in self.chains:
            executor_config[Chain.BASE] = {
                "rpc_url": os.getenv("BASE_RPC_URL", "https://mainnet.base.org"),
                "private_key": os.getenv("BASE_PRIVATE_KEY") or os.getenv("EVM_PRIVATE_KEY"),
                "gas_multiplier": 1.5
            }
        
        if "ethereum" in self.chains:
            executor_config[Chain.ETHEREUM] = {
                "rpc_url": os.getenv("ETHEREUM_RPC_URL", "https://eth.llamarpc.com"),
                "private_key": os.getenv("EVM_PRIVATE_KEY"),
                "gas_multiplier": 1.2
            }
        
        if "arbitrum" in self.chains:
            executor_config[Chain.ARBITRUM] = {
                "rpc_url": os.getenv("ARBITRUM_RPC_URL", "https://arb1.arbitrum.io/rpc"),
                "private_key": os.getenv("EVM_PRIVATE_KEY"),
                "gas_multiplier": 1.1
            }
        
        if not executor_config:
            print("[Sniper] No chain configurations found!")
            return
        
        self.executor = MultiChainExecutor(executor_config)
        await self.executor.initialize()
        
        self.sniper_engine = SniperExecutionEngine(self.executor, max_position_usd=self.max_position_usd)
        self.dex_client = DexScreenerClient(min_liquidity=self.min_liquidity, max_age_minutes=60)
        await self.dex_client.__aenter__()
        
        self.running = True
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        self.exit_task = asyncio.create_task(self._exit_monitor_loop())
        print(f"[Sniper] Started successfully")
    
    async def stop(self):
        self.running = False
        if self.monitor_task:
            self.monitor_task.cancel()
        if self.exit_task:
            self.exit_task.cancel()
        if self.dex_client:
            await self.dex_client.__aexit__(None, None, None)
        if self.executor:
            await self.executor.close()
        print(f"[Sniper] Stopped")
    
    async def _monitor_loop(self):
        while self.running:
            for chain in self.chains:
                try:
                    tokens = await self.dex_client.fetch_new_pairs(chain)
                    for token in tokens:
                        await self._analyze_and_snipe(token)
                except Exception as e:
                    print(f"[Sniper] Error monitoring {chain}: {e}")
            await asyncio.sleep(30)
    
    async def _exit_monitor_loop(self):
        while self.running:
            try:
                if self.sniper_engine:
                    results = await self.sniper_engine.check_exit_conditions()
                    for result in results:
                        print(f"[Sniper] Exit executed: {result.signature}")
            except Exception as e:
                print(f"[Sniper] Exit monitor error: {e}")
            await asyncio.sleep(60)
    
    async def _analyze_and_snipe(self, token: TokenInfo):
        print(f"\n[Sniper] New token: {token.symbol} ({token.chain})")
        print(f"  Liquidity: ${token.liquidity_usd:,.0f} | Age: {token.age_minutes}min | DEX: {token.dex}")
        
        if token.volume_24h < 1000:
            print(f"  ⏭️ Skip: Low volume")
            return
        
        if token.age_minutes > 30:
            print(f"  ⏭️ Skip: Too old")
            return
        
        if self.sniper_engine and token.address in self.sniper_engine.active_positions:
            print(f"  ⏭️ Skip: Already have position")
            return
        
        try:
            chain_enum = Chain(token.chain)
        except ValueError:
            print(f"  ⏭️ Skip: Unsupported chain")
            return
        
        if chain_enum not in self.executor.executors:
            print(f"  ⏭️ Skip: No executor for {token.chain}")
            return
        
        # Emit sniper alert for new token detection
        await self._emit_sniper_alert(token, "detected")
        
        print(f"  🎯 SNIPING ${self.max_position_usd}...")
        
        try:
            result = await self.sniper_engine.execute_on_signal(token)
            
            if result.success:
                print(f"  ✅ SNIPED: {token.symbol}")
                print(f"     TX: {result.signature}")
                
                self.bus.publish(Channel.ORDERS, {
                    "type": "snipe_executed",
                    "source": self.name,
                    "data": {
                        "token": {
                            "address": token.address,
                            "symbol": token.symbol,
                            "chain": token.chain,
                            "liquidity_usd": token.liquidity_usd,
                            "price_usd": token.price_usd
                        },
                        "execution": {
                            "signature": result.signature,
                            "input_amount": result.input_amount,
                            "output_amount": result.output_amount,
                            "fee_paid": result.fee_paid,
                            "price_impact_pct": result.price_impact_pct
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                }, self.name)
                
                # Emit sniper alert for successful execution
                await self._emit_sniper_alert(token, "executed", result)
            else:
                print(f"  ❌ FAILED: {result.error}")
        except Exception as e:
            print(f"  ❌ ERROR: {e}")
    
    async def _emit_sniper_alert(self, token: TokenInfo, event: str, result=None):
        """Emit sniper alert to dashboard"""
        try:
            alert_data = {
                "type": "sniper_alert",
                "data": {
                    "chain": token.chain,
                    "token": token.symbol,
                    "address": token.address,
                    "age_minutes": token.age_minutes,
                    "liquidity_usd": token.liquidity_usd,
                    "volume_24h": token.volume_24h,
                    "price_usd": token.price_usd,
                    "dex": token.dex,
                    "event": event,  # "detected", "executed", "failed"
                    "timestamp": datetime.now().isoformat() + "Z"
                }
            }
            if result:
                alert_data["data"]["execution"] = {
                    "signature": result.signature,
                    "input_amount": result.input_amount,
                    "output_amount": result.output_amount,
                    "fee_paid": result.fee_paid,
                    "price_impact_pct": result.price_impact_pct
                }
            
            await self.bus.publish("signals", alert_data, "sniper_bot")
        except Exception as e:
            print(f"[Sniper] Emit error: {e}")


async def run_sniper():
    from hybrid.config import HybridConfig
    from hybrid.messaging import MessageBus
    
    config = HybridConfig()
    bus = MessageBus("redis://localhost:6379")
    
    sniper = SniperBot(
        bus=bus,
        config=config,
        chains=["solana", "base"],
        min_liquidity=10_000,
        max_position_usd=500,
        slippage_bps=300
    )
    
    try:
        await sniper.start()
        while sniper.running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await sniper.stop()


if __name__ == "__main__":
    asyncio.run(run_sniper())
