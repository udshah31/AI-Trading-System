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
    EQUITY_INTERVAL_S = 60.0  # equity curve / positions snapshot at most once a minute

    def __init__(self, bus: MessageBus, storage: StorageService, market_data_interval: float = 60.0):
        super().__init__("storage_agent", bus)
        self.storage = storage
        self.market_data_interval = market_data_interval
        self._last_market_write: dict = {}
        self._last_equity_write = 0.0  # time.time() of the last equity/positions snapshot

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

        elif msg_type == "trade_filled":
            try:
                await self.storage.record_trade(
                    timestamp=datetime.now(timezone.utc),
                    symbol=str(data.get("symbol", ""))[:20],
                    side=str(data.get("side", "buy")).lower(),
                    volume=Decimal(str(data.get("volume", 0))),
                    price=Decimal(str(data.get("price") or 0)),
                    fee=Decimal("0"),
                    pnl=Decimal(str(round(float(data.get("realized_pnl", 0.0)), 8))),
                    strategy="quant",
                    exchange=data.get("exchange", ""),
                    client_order_id=data.get("client_order_id"),
                    status="filled",
                    trade_metadata={"reason": data.get("reason", "decision")},
                )
            except Exception as e:
                print(f"[StorageAgent] trade persist error: {e}")

        elif msg_type == "portfolio_update":
            if time.time() - self._last_equity_write < self.EQUITY_INTERVAL_S:
                return
            self._last_equity_write = time.time()
            now = datetime.now(timezone.utc)
            try:
                await self.storage.record_equity(
                    timestamp=now, account="default",
                    equity=Decimal(str(round(float(data["equity"]), 2))),
                    cash=Decimal(str(round(float(data.get("cash", 0.0)), 2))),
                    positions_value=Decimal(str(round(float(data.get("exposure", 0.0)), 2))),
                    daily_pnl=Decimal(str(round(float(data.get("daily_pnl", 0.0)), 2))),
                    drawdown_pct=Decimal(str(round(float(data.get("drawdown_pct", 0.0)) * 100, 6))),  # percent
                )
                await self.storage.positions.sync_open("quant", data.get("positions", []), now)
            except Exception as e:
                print(f"[StorageAgent] equity/positions persist error: {e}")

    async def _on_order(self, payload: dict):
        pass  # fills are recorded from the orchestrator's trade_filled (it knows the realised P&L)

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
