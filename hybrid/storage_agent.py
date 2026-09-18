"""
Storage Agent - Persists bus messages (signals, trades, market data) to PostgreSQL
"""
import time
from datetime import datetime, timezone
from decimal import Decimal

from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel
from hybrid.storage import StorageService


class StorageAgent(BaseAgent):
    def __init__(self, bus: MessageBus, storage: StorageService, market_data_interval: float = 60.0):
        super().__init__("storage_agent", bus)
        self.storage = storage
        self.market_data_interval = market_data_interval
        self._pending_orders: dict = {}
        self._last_market_write: dict = {}

    async def handle_message(self, payload: dict):
        pass

    async def start(self):
        self.running = True
        self.bus.subscribe(Channel.SIGNALS, self._on_signal)
        self.bus.subscribe(Channel.ORDERS, self._on_order)
        self.bus.subscribe(Channel.MARKET_DATA, self._on_market_data)
        print("[StorageAgent] Started")

    async def _on_signal(self, payload: dict):
        msg_type = payload.get("type")
        data = payload.get("data", {})

        if msg_type == "quant_decision":
            try:
                await self.storage.record_signal(
                    timestamp=datetime.now(timezone.utc),
                    symbol=data.get("ticker", "")[:20],
                    agent="quant_agent",
                    signal_type="quant",
                    action=str(data.get("action", "hold")).lower(),
                    strength=Decimal(str(round(float(data.get("score", 0.5)), 4))),
                    confidence=Decimal(str(round(float(data.get("confidence", 0.0)), 4))),
                    features=data.get("tech_signals", {}),
                )
            except Exception as e:
                print(f"[StorageAgent] signal persist error: {e}")

    async def _on_order(self, payload: dict):
        msg_type = payload.get("type")
        data = payload.get("data", {})

        if msg_type == "execute_order":
            key = data.get("client_order_id") or data.get("symbol")
            self._pending_orders[key] = data

        elif msg_type == "execution_result":
            symbol = data.get("symbol")
            request = self._pending_orders.pop(data.get("order_id"), None)
            if request is None:
                request = self._pending_orders.pop(symbol, None) or {}
            if not data.get("success"):
                return
            try:
                await self.storage.record_trade(
                    timestamp=datetime.now(timezone.utc),
                    symbol=symbol[:20],
                    side=str(request.get("side", "buy")).lower(),
                    volume=Decimal(str(request.get("volume", 0))),
                    price=Decimal(str(request.get("price") or 0)),
                    fee=Decimal("0"),
                    strategy=request.get("strategy", "hybrid"),
                    exchange=data.get("exchange", ""),
                    order_id=data.get("order_id"),
                    client_order_id=request.get("client_order_id"),
                    status="filled",
                    trade_metadata={"message": data.get("message", "")},
                )
            except Exception as e:
                print(f"[StorageAgent] trade persist error: {e}")

    async def _on_market_data(self, payload: dict):
        symbol = payload.get("symbol")
        if not symbol:
            return
        now = time.time()
        last = self._last_market_write.get(symbol, 0)
        if now - last < self.market_data_interval:
            return
        self._last_market_write[symbol] = now
        try:
            price = Decimal(str(payload.get("price") or 0))
            await self.storage.record_market_data(
                timestamp=datetime.fromtimestamp(
                    (payload.get("timestamp") or now * 1000) / 1000, tz=timezone.utc
                ),
                symbol=symbol[:20],
                exchange="kraken",
                close=price,
                volume=Decimal(str(payload.get("volume") or 0)),
                bid=Decimal(str(payload.get("bid") or 0)) if payload.get("bid") else None,
                ask=Decimal(str(payload.get("ask") or 0)) if payload.get("ask") else None,
                timeframe="tick",
            )
        except Exception as e:
            print(f"[StorageAgent] market data persist error: {e}")
