"""Kraken spot via the existing KrakenExecutor (dry-run simulation lives in the executor)."""
from typing import List, Optional

from hybrid.brokers.base import OrderReport


def kraken_balance_codes(ticker: str) -> set[str]:
    """Kraken spot balance codes for a pair's base asset: BTC/USDT -> {XBT, XXBT}.

    Older assets are X-prefixed (XXBT, XETH). Suffixed codes such as XBT.F or ETH.S
    are earn/staking balances, not spot, so they never match.
    """
    base = ticker.upper().replace("-", "/").split("/")[0]
    base = "XBT" if base == "BTC" else base
    return {base, f"X{base}"}


class KrakenBroker:
    name = "kraken"

    def __init__(self, executor):
        self.executor = executor  # KrakenExecutor (or a test double with the same methods)

    async def submit_market(self, symbol: str, side: str, volume: float,
                            client_order_id: str, userref: Optional[int] = None) -> OrderReport:
        from decimal import Decimal
        from hybrid.kraken_executor import OrderRequest
        result = await self.executor.place_order(OrderRequest(
            symbol=symbol, side=side, order_type="market", volume=Decimal(str(volume)),
            client_order_id=str(userref) if userref is not None else None))  # Kraken userref: int32
        avg = getattr(result, "avg_fill_price", None)
        return OrderReport(
            status="filled" if result.success else "rejected",
            filled_volume=float(getattr(result, "filled_volume", 0) or volume) if result.success else 0.0,
            avg_price=float(avg) if avg is not None else None,
            order_id=result.order_id,
            message=f"Kraken: {result.status}" + (f" - {result.error}" if getattr(result, "error", None) else ""))

    async def position_volume(self, symbol: str) -> float:
        balances = await self.executor.get_balances()
        return sum(float(balances[c].total) for c in kraken_balance_codes(symbol) if c in balances)

    async def find_orders(self, client_order_id: str, userref: Optional[int] = None) -> List[dict]:
        if userref is None:
            return []
        return await self.executor.find_orders_by_userref(userref)

    async def close(self) -> None:
        await self.executor.close()
