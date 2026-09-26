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
from typing import Any, Optional


class ExecutionAgent(BaseAgent):
    def __init__(self, bus: MessageBus, config: HybridConfig, dry_run: bool = True, broker=None):
        super().__init__("execution_agent", bus)
        self.config = config
        self.dry_run = dry_run
        self.broker = broker  # hybrid.brokers.Broker; when set, every order goes through it
        self.kraken_spot: Optional[KrakenExecutor] = None
        self.kraken_futures: Optional[KrakenExecutor] = None
        self.alpaca: Optional[Any] = None  # AlpacaExecutor when not dry-run
        self.bus.subscribe(Channel.ORDERS, self._on_execution_request)
    
    async def handle_message(self, payload: dict):
        pass
    
    async def start(self):
        if self.broker is not None:
            print(f"[ExecutionAgent] Started ({self.broker.name})")
            return
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
        if self.broker is not None:
            await self.broker.close()
            return
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

        if self.broker is not None:
            try:
                report = await self.broker.submit_market(symbol, data["side"].lower(), float(data["volume"]),
                                                         client_order_id, data.get("userref"))
            except Exception as e:
                await self._send_result(False, symbol, f"{self.broker.name}: {e}", exchange=self.broker.name,
                                        client_order_id=client_order_id, status="rejected")
                return
            await self._send_result(report.filled, symbol, report.message, report.order_id, self.broker.name,
                                    client_order_id=client_order_id, avg_price=report.avg_price,
                                    filled_volume=report.filled_volume, status=report.status)
            return
        
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
                                    client_order_id=client_order_id, avg_price=getattr(result, "avg_price", None))
        except Exception as e:
            await self._send_result(False, symbol, str(e), client_order_id=client_order_id)
    
    def _route_exchange(self, symbol: str, asset_type: str) -> Optional[str]:
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
            # Kraken userref (int32) so orders can be found again after a crash; our
            # client_order_id stays internal
            client_order_id=str(data["userref"]) if data.get("userref") is not None else None,
            reduce_only=data.get("reduce_only", False)
        )
        
        if executor is None:
            raise RuntimeError(f"{exchange} not initialized")
        result = await executor.place_order(order)
        
        class SimpleResult:
            success: bool
            order_id: Optional[str]
            message: str
            avg_price: Optional[float]
        
        r = SimpleResult()
        r.success = result.success
        r.order_id = result.order_id
        fill = getattr(result, "avg_fill_price", None)
        r.avg_price = float(fill) if fill is not None else None
        r.message = f"Kraken: {result.status}" + (f" - {result.error}" if result.error else "")
        return r
    
    async def _execute_alpaca(self, data: dict):
        from hybrid.risk_manager import RiskAssessment
        from hybrid.execution import ExecutionResult
        
        assessment = RiskAssessment()
        assessment.approved = True
        assessment.position_size_shares = data["volume"]
        assessment.stop_loss_price = data.get("stop_loss", 0)
        
        if self.alpaca is None:
            raise RuntimeError("Alpaca executor not initialized")
        result = self.alpaca.execute(
            ticker=data["symbol"],
            action=data["side"].upper(),
            assessment=assessment,
            current_price=data.get("price", 0)
        )
        
        class SimpleResult:
            success: bool
            order_id: Optional[str]
            message: str
        
        r = SimpleResult()
        r.success = result.success
        r.order_id = result.order_id
        r.message = result.message
        return r
    
    async def _send_result(self, success: bool, symbol: str, message: str, order_id: Optional[str] = None, exchange: str = "",
                           client_order_id: Optional[str] = None, avg_price: Optional[float] = None,
                           filled_volume: Optional[float] = None, status: Optional[str] = None):
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
                "avg_price": avg_price,
                "filled_volume": filled_volume,
                "status": status,  # filled | partial | open | rejected (broker path)
            }
        }, "execution_agent")
