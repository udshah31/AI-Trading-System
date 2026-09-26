"""
Broker interface
================
What the orchestrator and execution agent need from any broker. Symbols are the
system's pairs (e.g. BTC/USDT); each broker maps them to its own names.
"""
from dataclasses import dataclass
from typing import List, Optional, Protocol


@dataclass
class OrderReport:
    """Outcome of a market order once the broker has had a moment to fill it.

    status: "filled", "partial" (done, part filled), "open" (still working; outcome
    unknown yet) or "rejected" (nothing filled).
    """
    status: str
    filled_volume: float
    avg_price: Optional[float]
    order_id: Optional[str]
    message: str

    @property
    def filled(self) -> bool:
        return self.status in ("filled", "partial") and self.filled_volume > 0


class Broker(Protocol):
    name: str

    async def submit_market(self, symbol: str, side: str, volume: float,
                            client_order_id: str, userref: Optional[int] = None) -> OrderReport: ...

    async def position_volume(self, symbol: str) -> float:
        """Quantity of the symbol's base asset held in the account. Raises if it can't tell."""
        ...

    async def find_orders(self, client_order_id: str, userref: Optional[int] = None) -> List[dict]:
        """Orders matching our ids: [{"status": "open"|"closed", "side", "vol_exec"}]; [] if none."""
        ...

    async def close(self) -> None: ...
