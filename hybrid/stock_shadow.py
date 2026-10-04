"""SPY/QQQ daily-bar research only. No bus, execution agent, or order submission access."""
import asyncio
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from hybrid.config import HybridConfig
from hybrid.quant_engine import compute_decision
from hybrid.signal_extractor import LLMSignals
from hybrid.storage import StockShadowDecision
from hybrid.technical_indicators import compute_technical_signals

SYMBOLS = ("SPY", "QQQ")
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
        for symbol in SYMBOLS:
            async with self.storage.db.session() as db:
                exists = await db.scalar(select(StockShadowDecision.id).where(
                    StockShadowDecision.symbol == symbol, StockShadowDecision.session_date == session_date))
            if exists:
                continue
            tech = await asyncio.to_thread(self.indicators, symbol, trade_date=session_date.isoformat())
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
                          "source": "yahoo_daily_adjusted"})
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
