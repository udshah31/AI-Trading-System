"""
Portfolio: the bot's own books
==============================
Cash and positions change only on the bot's fills; positions are valued at live
prices. From that come equity, the high-water mark (peak), drawdown, exposure and
today's P&L: the real numbers the risk manager sizes and halts trading with.

It covers the capital allocated to the bot (INITIAL_CAPITAL), not the whole exchange
account, which may hold coins the bot didn't buy.
"""
from datetime import date, datetime, timezone
from typing import Dict, Optional


class Portfolio:
    def __init__(self, initial_capital: float, today: Optional[date] = None):
        self.initial_capital = float(initial_capital)
        self.cash = float(initial_capital)
        # ticker -> {"volume", "avg_price", "last_price"}; prices None until first known
        self.positions: Dict[str, dict] = {}
        self.peak = float(initial_capital)
        self.day = today or datetime.now(timezone.utc).date()
        self.day_start_equity = float(initial_capital)
        self.realized_today = 0.0

    # ── derived figures ──
    @property
    def exposure(self) -> float:
        return sum(p["volume"] * p["last_price"] for p in self.positions.values() if p["last_price"] is not None)

    @property
    def equity(self) -> float:
        return self.cash + self.exposure

    @property
    def drawdown(self) -> float:
        """Fall from the peak as a fraction (0.04 = 4% below the high-water mark)."""
        return max(0.0, (self.peak - self.equity) / self.peak) if self.peak > 0 else 0.0

    @property
    def daily_pnl(self) -> float:
        return self.equity - self.day_start_equity

    # ── events ──
    def apply_fill(self, ticker: str, side: str, volume: float, price: float) -> float:
        """Book a fill; returns the realised P&L (0 for buys)."""
        realised = 0.0
        pos = self.positions.get(ticker)
        if side == "buy":
            self.cash -= volume * price
            if pos and pos["avg_price"] is not None:
                total = pos["volume"] + volume
                pos["avg_price"] = (pos["avg_price"] * pos["volume"] + price * volume) / total
                pos["volume"] = total
            else:
                self.positions[ticker] = pos = {"volume": volume + (pos["volume"] if pos else 0.0),
                                                "avg_price": price, "last_price": price}
            pos["last_price"] = price
        else:
            self.cash += volume * price
            if pos:
                sold = min(volume, pos["volume"])
                if pos["avg_price"] is not None:
                    realised = (price - pos["avg_price"]) * sold
                pos["volume"] -= sold
                pos["last_price"] = price
                if pos["volume"] <= 1e-12:
                    del self.positions[ticker]
            self.realized_today += realised
        self._raise_peak()
        return realised

    def set_volume(self, ticker: str, volume: float) -> None:
        """Correct a position's size to what the broker actually holds, keeping its cost:
        a fee taken in the coin raises the average entry instead of vanishing."""
        pos = self.positions.get(ticker)
        if not pos:
            return
        if volume <= 1e-12:
            del self.positions[ticker]
            return
        if pos["avg_price"] is not None and pos["volume"] > 0:
            pos["avg_price"] = pos["avg_price"] * pos["volume"] / volume
        pos["volume"] = volume

    def mark(self, ticker: str, price: float) -> None:
        """Value an open position at a live price."""
        pos = self.positions.get(ticker)
        if pos is None:
            return
        if pos["avg_price"] is None:  # restored without a known entry: start from the first price seen
            pos["avg_price"] = price
        pos["last_price"] = price
        self._raise_peak()

    def roll_day(self, today: date) -> None:
        if today != self.day:
            self.day, self.day_start_equity, self.realized_today = today, self.equity, 0.0

    def _raise_peak(self) -> None:
        self.peak = max(self.peak, self.equity)

    # ── reporting and persistence ──
    def snapshot(self) -> dict:
        return {
            "equity": self.equity, "cash": self.cash, "peak": self.peak, "drawdown_pct": self.drawdown,
            "exposure": self.exposure, "daily_pnl": self.daily_pnl, "realized_pnl_today": self.realized_today,
            "initial_capital": self.initial_capital,
            "positions": [
                {"symbol": t, "volume": p["volume"], "avg_price": p["avg_price"], "last_price": p["last_price"],
                 "unrealized_pnl": (p["last_price"] - p["avg_price"]) * p["volume"]
                 if p["last_price"] is not None and p["avg_price"] is not None else 0.0}
                for t, p in sorted(self.positions.items())
            ],
        }

    def to_dict(self) -> dict:
        return {"initial_capital": self.initial_capital, "cash": self.cash, "positions": self.positions,
                "peak": self.peak, "day": self.day.isoformat(), "day_start_equity": self.day_start_equity,
                "realized_today": self.realized_today}

    @classmethod
    def from_dict(cls, data: dict, today: Optional[date] = None) -> "Portfolio":
        pf = cls(data["initial_capital"], today=date.fromisoformat(data["day"]))
        pf.cash, pf.positions, pf.peak = float(data["cash"]), dict(data["positions"]), float(data["peak"])
        pf.day_start_equity, pf.realized_today = float(data["day_start_equity"]), float(data["realized_today"])
        pf.roll_day(today or datetime.now(timezone.utc).date())
        return pf
