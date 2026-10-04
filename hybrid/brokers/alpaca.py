"""
Alpaca (paper) via alpaca-py
============================
Crypto pairs trade as BASE/USD (BTC/USDT -> BTC/USD); symbols without a slash are
stocks. Crypto takes gtc/ioc market orders with fractional qty in multiples of the
asset's min_trade_increment. Orders are tracked by our own client_order_id, so
crash recovery is a direct lookup. Only paper trading is enabled in this system.
"""
import asyncio
import math
from typing import Any, Callable, Dict, List, Optional

from hybrid.brokers.base import OrderReport

_USD_QUOTES = {"USD", "USDT", "USDC"}
_DONE = {"filled", "canceled", "expired", "rejected", "done_for_day", "stopped", "replaced"}


def alpaca_symbol(symbol: str) -> str:
    """System pair -> Alpaca symbol: BTC/USDT -> BTC/USD, XBT/USD -> BTC/USD, AAPL -> AAPL."""
    if "/" not in symbol and "-" not in symbol:
        return symbol.upper()
    base, quote = symbol.upper().replace("-", "/").split("/", 1)
    base = "BTC" if base == "XBT" else base
    return f"{base}/{'USD' if quote in _USD_QUOTES else quote}"


def _status(order: Any) -> str:
    return getattr(order.status, "value", str(order.status)).lower()


def _report(order: Any) -> OrderReport:
    status = _status(order)
    filled = float(order.filled_qty or 0)
    avg = float(order.filled_avg_price) if order.filled_avg_price not in (None, "") else None
    if status == "filled":
        outcome = "filled"
    elif status in _DONE:
        outcome = "partial" if filled > 0 else "rejected"
    else:
        outcome = "open"
    return OrderReport(outcome, filled, avg, str(order.id), f"Alpaca: {status}")


class AlpacaBroker:
    name = "alpaca_paper"

    def __init__(self, api_key: Optional[str] = None, secret_key: Optional[str] = None, paper: bool = True,
                 client: Any = None, fill_wait_s: float = 10.0, poll_s: float = 0.5,
                 sleep: Callable = asyncio.sleep):
        if not paper:
            raise ValueError("Live Alpaca trading isn't enabled in this system yet; use paper=True.")
        if client is None:
            from alpaca.trading.client import TradingClient
            if not api_key or not secret_key:
                raise ValueError("ALPACA_API_KEY and ALPACA_SECRET_KEY (paper keys) are required")
            client = TradingClient(api_key, secret_key, paper=True)
        self.client = client
        self.fill_wait_s, self.poll_s, self._sleep = fill_wait_s, poll_s, sleep
        self._increments: Dict[str, float] = {}

    async def _call(self, fn: Callable, *args):
        return await asyncio.to_thread(fn, *args)

    async def _increment(self, symbol: str) -> float:
        if symbol not in self._increments:
            try:
                asset = await self._call(self.client.get_asset, symbol)
                self._increments[symbol] = float(getattr(asset, "min_trade_increment", None) or 1e-9)
            except Exception:
                self._increments[symbol] = 1e-9  # let the broker validate
        return self._increments[symbol]

    async def submit_market(self, symbol: str, side: str, volume: float,
                            client_order_id: str, userref: Optional[int] = None) -> OrderReport:
        from alpaca.trading.enums import OrderSide, TimeInForce
        from alpaca.trading.requests import MarketOrderRequest

        pair = alpaca_symbol(symbol)
        crypto = "/" in pair
        step = await self._increment(pair) if crypto else 1e-9
        qty = math.floor(volume / step + 1e-9) * step
        qty = round(qty, 9)
        if qty <= 0:
            return OrderReport("rejected", 0.0, None, None,
                               f"Alpaca: {volume} {pair} is below the minimum order increment {step}")
        request = MarketOrderRequest(
            symbol=pair, qty=qty, side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
            time_in_force=TimeInForce.GTC if crypto else TimeInForce.DAY, client_order_id=client_order_id)
        try:
            order = await self._call(self.client.submit_order, request)
        except Exception as e:
            return OrderReport("rejected", 0.0, None, None, f"Alpaca rejected the order: {e}")

        waited = 0.0
        while _status(order) not in _DONE and waited < self.fill_wait_s:
            await self._sleep(self.poll_s)
            waited += self.poll_s
            try:
                order = await self._call(self.client.get_order_by_id, order.id)
            except Exception:
                continue  # transient: keep waiting; an unresolved order stays "open"
        return _report(order)

    async def position_volume(self, symbol: str) -> float:
        pair = alpaca_symbol(symbol).replace("/", "")  # positions use BTCUSD
        try:
            position = await self._call(self.client.get_open_position, pair)
        except Exception as e:
            if getattr(e, "status_code", None) == 404:
                return 0.0  # no position
            raise  # anything else: the caller treats it as "can't tell" and doesn't trade
        return float(position.qty)

    async def find_orders(self, client_order_id: str, userref: Optional[int] = None) -> List[dict]:
        try:
            order = await self._call(self.client.get_order_by_client_id, client_order_id)
        except Exception as e:
            if getattr(e, "status_code", None) == 404:
                return []  # never reached Alpaca
            raise
        report = _report(order)
        return [{"status": report.status,
                 "side": getattr(order.side, "value", str(order.side)).lower(),
                 "vol_exec": report.filled_volume,
                 "avg_price": report.avg_price}]

    async def close(self) -> None:
        return None
