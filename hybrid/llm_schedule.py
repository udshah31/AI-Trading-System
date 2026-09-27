"""When the live bot asks for LLM analyses: once at startup, then every LLM_INTERVAL_HOURS.

A full TradingAgents run takes minutes and dozens of LLM calls, so it runs on a slow clock;
the 5-minute trading cycle reuses the latest result (see QuantAgent.fresh_llm).
"""
import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, Iterable, Mapping

from hybrid.messaging import Channel
from hybrid.quant_agent import market_data_symbol

TRADED_PAIRS: tuple[str, ...] = ("BTC/USDT", "ETH/USDT")


@dataclass(frozen=True)
class LLMSettings:
    enabled: bool = True
    interval_hours: float = 4.0
    max_age_hours: float = 8.0


def _hours(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        value = 0.0
    if value <= 0:
        print(f"[LLM] Ignoring {name}={raw!r} (need a positive number of hours); using {default:g}")
        return default
    return value


def llm_settings(env: Mapping[str, str] = os.environ) -> LLMSettings:
    """LLM_ENABLED / LLM_INTERVAL_HOURS / LLM_MAX_AGE_HOURS from the environment; bad values fall back."""
    enabled = env.get("LLM_ENABLED", "true").strip().lower() not in ("false", "0", "no", "off")
    return LLMSettings(
        enabled=enabled,
        interval_hours=_hours(env, "LLM_INTERVAL_HOURS", LLMSettings.interval_hours),
        max_age_hours=_hours(env, "LLM_MAX_AGE_HOURS", LLMSettings.max_age_hours),
    )


def _utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def request_analyses(bus, pairs: Iterable[str], trade_date: str) -> None:
    """Ask the LLM analyst for one analysis per traded pair (it runs them one at a time)."""
    stamp = int(time.time())
    for pair in pairs:
        bus.publish(Channel.SIGNALS, {
            "type": "llm_analysis_request",
            "target": "llm_analyst",
            "request_id": f"{pair}:{trade_date}:{stamp}",
            "data": {"ticker": market_data_symbol(pair), "pair": pair,
                     "trade_date": trade_date, "asset_type": "crypto"},
        }, "llm_schedule")


async def llm_analysis_loop(bus, pairs: Iterable[str], settings: LLMSettings,
                            sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                            today: Callable[[], str] = _utc_today) -> None:
    if not settings.enabled:
        print("[LLM] Disabled (LLM_ENABLED=false): decisions use technical indicators only")
        return
    pairs = tuple(pairs)
    print(f"[LLM] Analysing {', '.join(pairs)} now and every {settings.interval_hours:g}h "
          f"(signals used for up to {settings.max_age_hours:g}h)")
    while True:
        request_analyses(bus, pairs, today())
        await sleep(settings.interval_hours * 3600)
