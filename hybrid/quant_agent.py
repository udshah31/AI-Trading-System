"""
Quant Agent - Technical analysis signals, combined with the latest LLM analysis when fresh
"""
import asyncio
import dataclasses
import time
from typing import Callable, Optional

from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel
from hybrid.config import HybridConfig
from hybrid.pipeline import HybridPipeline
from hybrid.signal_extractor import LLMSignals

_LLM_FIELDS = {f.name for f in dataclasses.fields(LLMSignals)}
_LLM_SCORES = ("sentiment_score", "fundamental_score", "news_score", "research_debate_score",
               "trader_action_score", "portfolio_decision_score")


def _valid_signals(raw) -> Optional[LLMSignals]:
    """LLMSignals from a published dict, or None when it isn't a real, well-formed analysis."""
    if not isinstance(raw, dict) or raw.get("available") is not True:
        return None
    for name in _LLM_SCORES:
        v = raw.get(name, 0.5)
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not 0.0 <= v <= 1.0:
            return None
    # ignore fields this version doesn't know (a newer publisher), never crash on them
    return LLMSignals(**{k: v for k, v in raw.items() if k in _LLM_FIELDS})


# Quote currencies that Yahoo Finance prices crypto in as plain USD
_USD_QUOTES = {"USD", "USDT", "USDC"}


def market_data_symbol(ticker: str) -> str:
    """Exchange pair (BTC/USDT, XBT/USD) -> Yahoo Finance symbol (BTC-USD) for price data.

    Orders keep the exchange pair; only the price-data download uses this symbol.
    """
    if "/" not in ticker:
        return ticker
    base, quote = ticker.upper().split("/", 1)
    base = "BTC" if base == "XBT" else base
    return f"{base}-{'USD' if quote in _USD_QUOTES else quote}"


class QuantAgent(BaseAgent):
    def __init__(self, bus: MessageBus, config: HybridConfig, llm_max_age_hours: float = 8.0,
                 clock: Callable[[], float] = time.time):
        super().__init__("quant_agent", bus)
        self.config = config
        self.pipeline = HybridPipeline(config=config, skip_llm=True)
        self.llm_max_age_s = llm_max_age_hours * 3600
        self.clock = clock
        # exchange pair -> (signals from the latest successful LLM analysis, when they arrived)
        self.llm_latest: dict[str, tuple[LLMSignals, float]] = {}
        self.bus.subscribe(Channel.SIGNALS, self._on_request)
        self.bus.subscribe(Channel.SIGNALS, self._on_llm_result)

    async def handle_message(self, payload: dict):
        pass

    async def start(self):
        print("[QuantAgent] Started")

    async def _on_llm_result(self, payload: dict):
        if payload.get("type") != "llm_analysis_result":
            return
        data = payload.get("data") or {}
        raw = data.get("llm_signals")
        if not data.get("success") or not raw:
            return  # a failed run keeps the last good signals
        pair = data.get("pair") or data["ticker"]
        signals = _valid_signals(raw)
        if signals is None:
            # storing bad values would break every decision for this pair until they went stale
            print(f"[LLM] Ignoring malformed signals for {pair}")
            return
        self.llm_latest[pair] = (signals, self.clock())

    def fresh_llm(self, pair: str) -> tuple[Optional[LLMSignals], Optional[float]]:
        """The stored signals for `pair` and their age in hours, or (None, None) when absent or stale."""
        entry = self.llm_latest.get(pair)
        if entry is None:
            return None, None
        signals, received_at = entry
        age_s = self.clock() - received_at
        if age_s > self.llm_max_age_s:
            return None, None
        return signals, age_s / 3600

    async def _on_request(self, payload: dict):
        if payload.get("type") != "analyze_request":
            return
        if payload.get("target") != "quant_agent":
            return

        data = payload["data"]
        ticker = data["ticker"]
        llm, llm_age = self.fresh_llm(ticker)

        # yfinance download + indicator math is blocking; keep the event loop free
        result = await asyncio.to_thread(
            self.pipeline.analyze_quant_only, market_data_symbol(ticker), llm_signals=llm)

        decision, tech = result.quant_decision, result.tech_signals
        if decision is None or tech is None:  # analyze() always sets both; guard for the type checker
            print(f"[QuantAgent] No decision for {ticker}")
            return

        self.bus.publish(Channel.SIGNALS, {
            "type": "quant_decision",
            "source": "quant_agent",
            "data": {
                "ticker": ticker,
                "action": decision.action,
                "score": decision.composite_score,
                "confidence": decision.confidence,
                "llm_component": decision.llm_component if llm is not None else None,
                "llm_age_hours": round(llm_age, 2) if llm_age is not None else None,
                "agreement": decision.llm_quant_agreement,
                "tech_signals": {
                    "rsi": tech.rsi,
                    "rsi_score": tech.rsi_score,
                    "ema_fast": tech.ema_fast,
                    "ema_slow": tech.ema_slow,
                    "ema_crossover_score": tech.ema_crossover_score,
                    "bollinger_upper": tech.bollinger_upper,
                    "bollinger_lower": tech.bollinger_lower,
                    "bollinger_mid": tech.bollinger_mid,
                    "bollinger_score": tech.bollinger_score,
                    "atr": tech.atr,
                    "current_price": tech.current_price,
                    "volume": tech.current_volume,
                    "volume_sma": tech.volume_sma_20,
                    "volume_score": tech.volume_score,
                }
            }
        }, "quant_agent")
