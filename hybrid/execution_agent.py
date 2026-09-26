"""
Execution Agent - Routes orders to Kraken/Alpaca
"""
from decimal import Decimal
from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel
from hybrid.config import HybridConfig, live_trading_enabled
from hybrid.kraken_executor import (
    KrakenConfig,
    KrakenEnvironment,
    KrakenExecutor,
    OrderRequest,
)


class ExecutionAgent(BaseAgent):
    def __init__(self, bus: MessageBus, config: HybridConfig, dry_run: bool = True):
        super().__init__("execution_agent", bus)
        self.config = config
        self.dry_run = dry_run
        self.kraken_spot = None
        self.kraken_futures = None
        self.alpaca = None
        self.bus.subscribe(Channel.ORDERS, self._on_execution_request)
    
    async def handle_message(self, payload: dict):
        pass
    
    async def start(self):
        import os
        
        # dry_run=True always wins; going live also requires LIVE_TRADING=true
        kraken_dry_run = self.dry_run or not live_trading_enabled()
        
        if os.getenv("KRAKEN_API_KEY"):
            spot_config = KrakenConfig(
                api_key=os.getenv("KRAKEN_API_KEY"),
                api_secret=os.getenv("KRAKEN_API_SECRET"),
                environment=KrakenEnvironment.SPOT,
                dry_run=kraken_dry_run,
            )
            self.kraken_spot = KrakenExecutor(spot_config)
            await self.kraken_spot.initialize()
        
        if os.getenv("KRAKEN_FUTURES_API_KEY"):
            futures_config = KrakenConfig(
                api_key=os.getenv("KRAKEN_FUTURES_API_KEY"),
                api_secret=os.getenv("KRAKEN_FUTURES_API_SECRET"),
                passphrase=os.getenv("KRAKEN_FUTURES_PASSPHRASE", ""),
                environment=KrakenEnvironment.FUTURES,
                dry_run=kraken_dry_run,
            )
            self.kraken_futures = KrakenExecutor(futures_config)
            await self.kraken_futures.initialize()
        
        if not self.dry_run:
            from hybrid.execution import AlpacaExecutor
            self.alpaca = AlpacaExecutor(self.config, dry_run=False)
        
        print("[ExecutionAgent] Started")
    
    async def stop(self):
        if self.kraken_spot:
            await self.kraken_spot.close()
        if self.kraken_futures and self.kraken_futures != self.kraken_spot:
            await self.kraken_futures.close()
    
    async def _on_execution_request(self, payload: dict):
        if payload.get("type") != "execute_order":
            return
        
        data = payload["data"]
        symbol = data["symbol"]
        asset_type = data.get("asset_type", "auto")
        client_order_id = data.get("client_order_id")  # echoed back so callers can match fills
        
        exchange = self._route_exchange(symbol, asset_type)
        
        if not exchange:
            await self._send_result(False, symbol, "No exchange available", client_order_id=client_order_id)
            return
        
        try:
            if exchange in ["kraken_spot", "kraken_futures"]:
                result = await self._execute_kraken(exchange, data)
            else:
                result = await self._execute_alpaca(data)
            
            await self._send_result(result.success, symbol, result.message, result.order_id, exchange,
                                    client_order_id=client_order_id)
        except Exception as e:
            await self._send_result(False, symbol, str(e), client_order_id=client_order_id)
    
    def _route_exchange(self, symbol: str, asset_type: str) -> str:
        is_crypto = "/" in symbol or "-" in symbol or asset_type == "crypto"
        is_futures = "PERP" in symbol.upper() or asset_type == "futures"
        
        if is_futures:
            return "kraken_futures" if self.kraken_futures else None
        if is_crypto:
            # spot orders never fall back to the perp market
            return "kraken_spot" if self.kraken_spot else None
        elif self.alpaca:
            return "alpaca"
        elif self.kraken_spot:
            return "kraken_spot"
        return None
    
    async def _execute_kraken(self, exchange: str, data: dict):
        executor = self.kraken_futures if exchange == "kraken_futures" else self.kraken_spot
        
        order = OrderRequest(
            symbol=data["symbol"],
            side=data["side"].lower(),
            order_type=data.get("order_type", "market").lower(),
            volume=Decimal(str(data["volume"])),
            price=Decimal(str(data["price"])) if data.get("price") else None,
            # not forwarded: Kraken's userref must be an int32; our id stays internal
            reduce_only=data.get("reduce_only", False)
        )
        
        result = await executor.place_order(order)
        
        class SimpleResult:
            success: bool
            order_id: str
            message: str
        
        r = SimpleResult()
        r.success = result.success
        r.order_id = result.order_id
        r.message = f"Kraken: {result.status}" + (f" - {result.error}" if result.error else "")
        return r
    
    async def _execute_alpaca(self, data: dict):
        from hybrid.risk_manager import RiskAssessment
        from hybrid.execution import ExecutionResult
        
        assessment = RiskAssessment()
        assessment.approved = True
        assessment.position_size_shares = data["volume"]
        assessment.stop_loss_price = data.get("stop_loss", 0)
        
        result = self.alpaca.execute(
            ticker=data["symbol"],
            action=data["side"].upper(),
            assessment=assessment,
            current_price=data.get("price", 0)
        )
        
        class SimpleResult:
            success: bool
            order_id: str
            message: str
        
        r = SimpleResult()
        r.success = result.success
        r.order_id = result.order_id
        r.message = result.message
        return r
    
    async def _send_result(self, success: bool, symbol: str, message: str, order_id: str = None, exchange: str = "",
                           client_order_id: str = None):
        self.bus.publish(Channel.ORDERS, {
            "type": "execution_result",
            "source": "execution_agent",
            "data": {
                "success": success,
                "symbol": symbol,
                "order_id": order_id,
                "message": message,
                "exchange": exchange,
                "client_order_id": client_order_id,
            }
        }, "execution_agent")
