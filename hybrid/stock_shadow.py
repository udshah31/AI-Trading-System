"""ETF/sector-stock daily-bar research only. No bus or order submission access."""
import asyncio
import json
import math
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from hybrid.config import HybridConfig
from hybrid.quant_engine import compute_decision
from hybrid.signal_extractor import LLMSignals
from hybrid.storage import StockShadowDecision
from hybrid.technical_indicators import compute_technical_signals

SECTORS = (
    "IT / Technology", "Healthcare", "Financials", "Consumer staples",
    "Consumer discretionary", "Industrials", "Energy", "Utilities",
    "Real estate", "Communication services",
)


def load_ranking(path: Path) -> dict:
    """Load the reviewed market-cap snapshot, not a mutable live stock screener."""
    snapshot = json.loads(path.read_text())
    retrieved = datetime.fromisoformat(snapshot["retrieved_at"].replace("Z", "+00:00"))
    if retrieved.tzinfo is None or not snapshot["method"]:
        raise ValueError("Ranking snapshot requires timestamp and methodology")
    if {source["sector"] for source in snapshot["sources"]} != set(SECTORS):
        raise ValueError("Ranking snapshot requires a source for each sector")
    stocks = snapshot["stocks"]
    counts = Counter(asset["sector"] for asset in stocks)
    if counts != Counter({sector: 10 for sector in SECTORS}):
        raise ValueError("Stock snapshot must contain ten stocks in each of ten sectors")
    if len({asset["symbol"] for asset in stocks}) != 100:
        raise ValueError("Stock snapshot must contain 100 distinct tickers")
    for sector in SECTORS:
        ranked = sorted((asset for asset in stocks if asset["sector"] == sector), key=lambda a: a["rank"])
        if [asset["rank"] for asset in ranked] != list(range(1, 11)):
            raise ValueError(f"Invalid ranking for {sector}")
        caps = [asset["market_cap_usd"] for asset in ranked]
        if any(not math.isfinite(cap) or cap <= 0 for cap in caps) or caps != sorted(caps, reverse=True):
            raise ValueError(f"Invalid market-cap ordering for {sector}")
        if any(asset["kind"] != "stock" or not asset["source_url"].startswith("https://") for asset in ranked):
            raise ValueError(f"Invalid stock/source metadata for {sector}")
    return snapshot


RANKING = load_ranking(Path(__file__).with_name("stock_watchlist.json"))
WATCHLIST = (
    {"symbol": "SPY", "name": "SPDR S&P 500 ETF Trust", "sector": "Broad market", "kind": "etf"},
    {"symbol": "QQQ", "name": "Invesco QQQ Trust", "sector": "Nasdaq-100", "kind": "etf"},
    *RANKING["stocks"],
)
SYMBOLS = tuple(asset["symbol"] for asset in WATCHLIST)
NEW_YORK = ZoneInfo("America/New_York")
CLOSE_DELAY = timedelta(minutes=20)  # allow the final daily bar to settle


def enabled() -> bool:
    return os.getenv("STOCK_SHADOW_ENABLED", "false").strip().lower() == "true"


class StockShadow:
    def __init__(self, storage, config: HybridConfig, calendar_client,
                 indicators=compute_technical_signals):
        self.storage, self.config = storage, config
        self.calendar_client, self.indicators = calendar_client, indicators

    async def run_once(self, now: Optional[datetime] = None) -> int:
        from alpaca.trading.requests import GetCalendarRequest

        now = now or datetime.now(timezone.utc)
        today = now.astimezone(NEW_YORK).date()
        # Query actual exchange sessions: handles holidays, early closes and DST.
        sessions = await asyncio.to_thread(self.calendar_client.get_calendar,
                                           GetCalendarRequest(start=today - timedelta(days=14), end=today))
        completed = []
        for session in sessions:
            close = session.close
            close = close.replace(tzinfo=NEW_YORK) if close.tzinfo is None else close
            if close + CLOSE_DELAY <= now:
                completed.append((session.date, close))
        if not completed:
            return 0
        session_date, close = max(completed, key=lambda item: item[0])
        count = 0
        for asset in WATCHLIST:
            symbol = asset["symbol"]
            async with self.storage.db.session() as db:
                exists = await db.scalar(select(StockShadowDecision.id).where(
                    StockShadowDecision.symbol == symbol, StockShadowDecision.session_date == session_date))
            if exists:
                continue
            try:
                tech = await asyncio.to_thread(self.indicators, symbol, trade_date=session_date.isoformat())
            except Exception as error:
                # One unavailable stock must not prevent research for other sectors.
                print(f"[StockShadow] {symbol}: data unavailable ({type(error).__name__}); skipped")
                continue
            values = (tech.current_price, tech.rsi_score, tech.ema_crossover_score,
                      tech.bollinger_score, tech.volume_score)
            if (tech.data_points < 36 or tech.last_bar_date != session_date.isoformat()
                    or not all(math.isfinite(v) for v in values) or tech.current_price <= 0
                    or not all(0 <= v <= 1 for v in values[1:])):
                print(f"[StockShadow] {symbol}: missing, stale or invalid completed daily bar; skipped")
                continue
            decision = compute_decision(LLMSignals(), tech, self.config)
            row = StockShadowDecision(
                symbol=symbol, session_date=session_date, session_close=close,
                analyzed_at=now, action=decision.action.lower(), score=decision.composite_score,
                confidence=decision.confidence, close_price=tech.current_price,
                features={"rsi": tech.rsi_score, "ema": tech.ema_crossover_score,
                          "bollinger": tech.bollinger_score, "volume": tech.volume_score,
                          "weights": self.config.tech_weights(),
                          "buy_threshold": self.config.buy_threshold,
                          "sell_threshold": self.config.sell_threshold,
                          "source": "yahoo_daily_adjusted", "sector": asset["sector"],
                          "watchlist_retrieved_at": RANKING["retrieved_at"],
                          "sector_rank": asset.get("rank"), "ranking_source": asset.get("source_url"),
                          "ranking_market_cap_usd": asset.get("market_cap_usd")})
            try:
                async with self.storage.db.session() as db:
                    db.add(row)
                count += 1
                print(f"[StockShadow] {symbol} {session_date}: {decision.action} — research only, no orders")
            except IntegrityError:
                pass  # another run saved this symbol/session first
        return count


async def stock_shadow_loop(service: StockShadow, sleep=asyncio.sleep):
    while True:
        try:
            await service.run_once()
        except Exception as error:
            print(f"[StockShadow] Analysis unavailable ({type(error).__name__}); will retry")
        await sleep(300)
