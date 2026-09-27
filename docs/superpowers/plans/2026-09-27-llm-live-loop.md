# LLM Analysis in the Live Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Live 5-minute decisions use the latest TradingAgents LLM analysis (run every 4 hours per pair) with the existing weights, falling back to technicals-only when the analysis is missing, failed or stale.

**Architecture:** A scheduler publishes `llm_analysis_request` for each traded pair at startup and every `LLM_INTERVAL_HOURS`. The existing `LLMAnalystAgent` runs TradingAgents and publishes `llm_analysis_result`. `QuantAgent` stores the latest successful signals per pair and passes fresh ones (≤ `LLM_MAX_AGE_HOURS`) into `HybridPipeline.analyze_quant_only`, so `compute_decision` applies the LLM weights. Risk, orchestrator routing and execution are unchanged apart from carrying the LLM/quant agreement flag and logging the signal source.

**Tech Stack:** Python 3.14, asyncio, Redis message bus (`hybrid/messaging.py`), pytest (+ asyncio plugin), mypy.

**Spec:** `docs/superpowers/specs/2026-09-27-llm-live-loop-design.md`

## Global Constraints

- Weights and thresholds do not change: LLM 0.75, technicals 0.25, BUY ≥ 0.65, SELL ≤ 0.35.
- `LLM_ENABLED` default `true`; `LLM_INTERVAL_HOURS` default `4`; `LLM_MAX_AGE_HOURS` default `8`; read from the environment at startup.
- Traded pairs: `BTC/USDT`, `ETH/USDT` only. LLM requests use the Yahoo symbol (`BTC-USD`) with `asset_type="crypto"`; results map back to the exchange pair.
- Live analyst: `max_concurrent=1`, `timeout_seconds=900`.
- A failed analysis never overwrites stored signals. No persistence across restarts.
- Tests use fake buses and no real LLM or network calls.
- Run tests with: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 <venv>/bin/python -m pytest -q -p asyncio tests` (global pytest crashes on the anchorpy plugin; the session venv is `$S/mypyenv`, `S=/private/tmp/claude-501/-Users-udaysah-AI-Trading-System/c4eb6748-fae3-4a68-ab7c-26554c839f6d/scratchpad`). Type check: `<venv>/bin/python -m mypy hybrid/ --ignore-missing-imports`.
- Work on branch `feat/llm-live-loop`. Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- Bad env values (`LLM_INTERVAL_HOURS=4h`, `0`, `-1`, `LLM_ENABLED=FALSE`): the bot must start, warn, and use defaults / honour the switch case-insensitively → test in Task 5.
- An LLM result whose `llm_signals` carries keys this version does not know (publisher newer than receiver) or is `null` with `success: true`: stored if valid, ignored otherwise, never a crash → tests in Task 3.
- Freshness boundary: signals exactly `LLM_MAX_AGE_HOURS` old are still used; just over is technicals-only → test in Task 3.
- LLM and technicals disagreeing must halve the position size live, as the risk manager intends; today the risk agent drops the agreement flag → test in Task 4.
- A failed analysis must be visible in the log (`[LLM] BTC/USDT analysis failed: …`) rather than silent → test in Task 4.

---

## File Structure

- Modify `hybrid/llm_agent.py` — request/result carry the exchange pair; published result keeps `available`, `sources`, `confidences`; live analyst limits.
- Modify `hybrid/pipeline.py` — `analyze` / `analyze_quant_only` accept stored `llm_signals`.
- Modify `hybrid/quant_agent.py` — store latest LLM signals per pair; use fresh ones; publish `llm_component`, `llm_age_hours`, `agreement`.
- Modify `hybrid/risk_agent.py` — rebuild `QuantDecision` with the agreement flag.
- Modify `hybrid/orchestrator.py` — log decision source and LLM failures.
- Create `hybrid/llm_schedule.py` — `TRADED_PAIRS`, `LLMSettings`, `llm_settings()`, `request_analyses()`, `llm_analysis_loop()`.
- Modify `hybrid/main_async.py` — build settings, pass max age to `QuantAgent`, start the LLM loop, use `TRADED_PAIRS`.
- Create `tests/test_llm_live_loop.py` — all new tests.
- Modify `tests/test_decision_to_execution.py` — the existing `QuantAgent` fake accepts the new keyword.

---

### Task 1: LLM results keep availability and the exchange pair

**Files:**
- Modify: `hybrid/llm_agent.py` (dataclasses at lines 24-42, `_run_analysis` 111-153, `_run_ta_sync` 155-187, `_publish_result` 189-213, `create_llm_agents` 384-389)
- Test: `tests/test_llm_live_loop.py` (create)

**Interfaces:**
- Produces: `LLMAnalysisRequest.pair: Optional[str] = None`; `LLMAnalysisResult.pair: Optional[str] = None`; `llm_analysis_result` payload `data` gains `"pair": str` and `data["llm_signals"]` gains `"available": bool`, `"sources": dict`, `"confidences": dict`. `create_llm_agents(...)["llm_analyst"]` has `max_concurrent == 1`, `timeout_seconds == 900`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_llm_live_loop.py`:

```python
"""LLM analysis in the live loop: results keep their availability, the quant agent uses fresh
stored signals, and the scheduler asks for analyses every LLM_INTERVAL_HOURS."""
import asyncio
from types import SimpleNamespace

import pytest

from hybrid.config import HybridConfig
from hybrid.signal_extractor import LLMSignals
from hybrid.technical_indicators import TechnicalSignals

run = asyncio.run


class RecordingBus:
    def __init__(self):
        self.published = []

    def subscribe(self, channel, handler):
        pass

    def publish(self, channel, payload, source):
        self.published.append((channel, payload))

    def of_type(self, msg_type):
        return [p for _, p in self.published if p.get("type") == msg_type]


def _bullish():
    return LLMSignals(available=True, sentiment_score=0.9, fundamental_score=0.9, news_score=0.9,
                      research_debate_score=0.9, sources={"news": "typesafe"}, confidences={"news": 0.8})


def _tech(score):
    return TechnicalSignals(rsi_score=score, ema_crossover_score=score, bollinger_score=score,
                            volume_score=score, current_price=100.0, atr=2.0)


# ── Task 1: the published result survives the round trip ──

def test_published_result_keeps_availability_and_pair():
    from hybrid.llm_agent import LLMAnalysisResult, LLMAnalystAgent

    bus = RecordingBus()
    agent = LLMAnalystAgent(bus, HybridConfig())
    result = LLMAnalysisResult(ticker="BTC-USD", trade_date="2026-09-27", llm_signals=_bullish(),
                               raw_state={}, success=True, pair="BTC/USDT")
    run(agent._publish_result("r1", result))

    [payload] = bus.of_type("llm_analysis_result")
    assert payload["data"]["pair"] == "BTC/USDT"
    rebuilt = LLMSignals(**payload["data"]["llm_signals"])
    assert rebuilt.available is True
    assert rebuilt.sources == {"news": "typesafe"} and rebuilt.confidences == {"news": 0.8}


def test_request_pair_reaches_the_result(monkeypatch):
    from hybrid.llm_agent import LLMAnalysisRequest, LLMAnalystAgent

    agent = LLMAnalystAgent(RecordingBus(), HybridConfig())
    graph = SimpleNamespace(
        propagate=lambda ticker, date, asset_type: ({"final_trade_decision": "**Rating**: Buy"}, "Buy"))
    monkeypatch.setattr(agent, "_get_ta_graph", lambda: graph)
    monkeypatch.setattr("hybrid.llm_agent.record_llm_analysis", lambda *a, **k: None)
    monkeypatch.setattr("hybrid.llm_agent.default_judge", lambda: None)

    result = agent._run_ta_sync(LLMAnalysisRequest(ticker="BTC-USD", trade_date="2026-09-27",
                                                   asset_type="crypto", pair="BTC/USDT"))
    assert result.success and result.pair == "BTC/USDT" and result.llm_signals.available


def test_live_analyst_runs_one_at_a_time_with_a_long_timeout():
    from hybrid.llm_agent import create_llm_agents

    analyst = create_llm_agents(RecordingBus(), HybridConfig())["llm_analyst"]
    assert analyst.max_concurrent == 1 and analyst.timeout_seconds == 900
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $S/mypyenv/bin/python -m pytest -q -p asyncio tests/test_llm_live_loop.py`
Expected: 3 FAIL (`unexpected keyword argument 'pair'`, and `max_concurrent == 2`).

- [ ] **Step 3: Implement**

In `hybrid/llm_agent.py`:

Add the field to both dataclasses (keep it last so positional use is unaffected):

```python
@dataclass
class LLMAnalysisRequest:
    ticker: str
    trade_date: str
    asset_type: str = "stock"
    selected_analysts: Optional[List[str]] = None
    max_debate_rounds: int = 1
    max_risk_rounds: int = 1
    pair: Optional[str] = None  # exchange pair the bot trades (BTC/USDT); ticker is the data symbol


@dataclass
class LLMAnalysisResult:
    ticker: str
    trade_date: str
    llm_signals: LLMSignals
    raw_state: Dict
    success: bool
    error: Optional[str] = None
    duration_seconds: float = 0
    pair: Optional[str] = None
```

Pass `pair=request.pair` to every `LLMAnalysisResult(...)` built in `_run_analysis` (the timeout and exception branches) and in `_run_ta_sync` (success and exception branches), e.g.:

```python
            return LLMAnalysisResult(
                ticker=request.ticker,
                trade_date=request.trade_date,
                llm_signals=llm_signals,
                raw_state=final_state,
                success=True,
                pair=request.pair,
            )
```

In `_publish_result`, add the pair and the three missing signal fields:

```python
            "data": {
                "ticker": result.ticker,
                "pair": result.pair or result.ticker,
                "trade_date": result.trade_date,
                "success": result.success,
                "error": result.error,
                "duration_seconds": result.duration_seconds,
                "llm_signals": {
                    "sentiment_score": result.llm_signals.sentiment_score,
                    "fundamental_score": result.llm_signals.fundamental_score,
                    "news_score": result.llm_signals.news_score,
                    "research_debate_score": result.llm_signals.research_debate_score,
                    "trader_action_score": result.llm_signals.trader_action_score,
                    "portfolio_decision_score": result.llm_signals.portfolio_decision_score,
                    "sentiment_band": result.llm_signals.sentiment_band,
                    "sentiment_confidence": result.llm_signals.sentiment_confidence,
                    "portfolio_rating": result.llm_signals.portfolio_rating,
                    "trader_action": result.llm_signals.trader_action,
                    # without `available` a receiver rebuilds placeholder signals and ignores them
                    "available": result.llm_signals.available,
                    "sources": result.llm_signals.sources,
                    "confidences": result.llm_signals.confidences,
                } if result.success else None
            }
```

In `create_llm_agents`:

```python
def create_llm_agents(bus: MessageBus, config: HybridConfig) -> Dict[str, BaseAgent]:
    return {
        # one analysis at a time: the agent shares a single TradingAgentsGraph, which isn't safe
        # to run concurrently; a full debate on a 1-OCPU VM can exceed 5 minutes
        "llm_analyst": LLMAnalystAgent(bus, config, max_concurrent=1, timeout_seconds=900),
        "llm_orchestrator": LLMOrchestratorAgent(bus, config),
        "signal_fusion": SignalFusionAgent(bus, config),
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $S/mypyenv/bin/python -m pytest -q -p asyncio tests/test_llm_live_loop.py`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add hybrid/llm_agent.py tests/test_llm_live_loop.py
git commit -m "fix: LLM analysis results keep availability and the traded pair"
```

---

### Task 2: The pipeline accepts stored LLM signals

**Files:**
- Modify: `hybrid/pipeline.py` (`analyze` at lines 71-162, `analyze_quant_only` at 210-223)
- Test: `tests/test_llm_live_loop.py`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `HybridPipeline.analyze(ticker, trade_date=None, asset_type="stock", execute=False, llm_signals: Optional[LLMSignals] = None)` and `HybridPipeline.analyze_quant_only(ticker, trade_date=None, llm_signals: Optional[LLMSignals] = None) -> PipelineResult`. With `llm_signals` given, Step 1 uses them (no TradingAgents call).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_llm_live_loop.py`:

```python
# ── Task 2: pipeline uses stored signals ──

def _pipeline(monkeypatch, tech):
    from hybrid.pipeline import HybridPipeline

    monkeypatch.setattr("hybrid.pipeline.compute_technical_signals", lambda **kw: tech)
    pipeline = HybridPipeline(HybridConfig(), skip_llm=True)
    monkeypatch.setattr(pipeline, "_save_report", lambda result: None)  # don't write into results/
    return pipeline


def test_quant_only_uses_stored_llm_signals(monkeypatch):
    result = _pipeline(monkeypatch, _tech(0.5)).analyze_quant_only("BTC-USD", llm_signals=_bullish())
    assert result.llm_signals.available
    assert result.quant_decision.llm_component > 0
    assert result.quant_decision.composite_score == pytest.approx(0.8)  # 0.75*0.9 + 0.25*0.5
    assert result.quant_decision.action == "BUY"


def test_quant_only_without_llm_signals_is_unchanged(monkeypatch):
    result = _pipeline(monkeypatch, _tech(0.5)).analyze_quant_only("BTC-USD")
    assert not result.llm_signals.available
    assert result.quant_decision.llm_component == 0.0
    assert result.quant_decision.action == "HOLD"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $S/mypyenv/bin/python -m pytest -q -p asyncio tests/test_llm_live_loop.py -k quant_only`
Expected: `test_quant_only_uses_stored_llm_signals` FAILS with `unexpected keyword argument 'llm_signals'`; the other passes.

- [ ] **Step 3: Implement**

In `hybrid/pipeline.py`, extend `analyze`'s signature and Step 1:

```python
    def analyze(
        self,
        ticker: str,
        trade_date: Optional[str] = None,
        asset_type: str = "stock",
        execute: bool = False,
        llm_signals: Optional[LLMSignals] = None,
    ) -> "PipelineResult":
```

Add to its docstring Args: `llm_signals: Signals from an earlier LLM analysis; used instead of running TradingAgents.`

```python
        # ── Step 1: LLM Analysis (Track A) ──
        print("\n📊 Step 1: Running LLM Research Agents...")
        if llm_signals is not None:
            print("  Using stored signals from the latest LLM analysis.")
            result.llm_signals = llm_signals
            result.llm_raw_state = {}
        elif self.skip_llm:
            print("  [SKIPPED] Using default neutral signals.")
            result.llm_signals = LLMSignals()
            result.llm_raw_state = {}
        else:
            result.llm_signals, result.llm_raw_state = self._run_llm_analysis(
                ticker, trade_date, asset_type
            )
```

Replace `analyze_quant_only`:

```python
    def analyze_quant_only(
        self,
        ticker: str,
        trade_date: Optional[str] = None,
        llm_signals: Optional[LLMSignals] = None,
    ) -> "PipelineResult":
        """Run the analysis without calling TradingAgents (no API costs).

        With `llm_signals` (from an earlier LLM analysis) the decision uses them; without,
        it comes from the technical indicators alone.
        """
        original_skip = self.skip_llm
        self.skip_llm = True
        result = self.analyze(ticker, trade_date, execute=False, llm_signals=llm_signals)
        self.skip_llm = original_skip
        return result
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $S/mypyenv/bin/python -m pytest -q -p asyncio tests/test_llm_live_loop.py`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add hybrid/pipeline.py tests/test_llm_live_loop.py
git commit -m "feat: pipeline can decide with stored LLM signals"
```

---

### Task 3: QuantAgent stores and uses the latest LLM signals

**Files:**
- Modify: `hybrid/quant_agent.py`
- Modify: `tests/test_decision_to_execution.py` (fake `analyze_quant_only` in `test_quant_agent_analyses_yahoo_symbol_and_reports_exchange_pair`)
- Test: `tests/test_llm_live_loop.py`

**Interfaces:**
- Consumes: `llm_analysis_result` payload from Task 1 (`data.pair`, `data.success`, `data.llm_signals` incl. `available`); `analyze_quant_only(symbol, llm_signals=...)` from Task 2.
- Produces: `QuantAgent(bus, config, llm_max_age_hours: float = 8.0, clock: Callable[[], float] = time.time)`; `QuantAgent._on_llm_result(payload) -> None` (async); `QuantAgent.fresh_llm(pair) -> tuple[Optional[LLMSignals], Optional[float]]` (signals, age in hours); `quant_decision` data gains `"llm_component": Optional[float]`, `"llm_age_hours": Optional[float]`, `"agreement": bool`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_llm_live_loop.py`:

```python
# ── Task 3: QuantAgent keeps the latest analysis per pair ──

class Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now


def _llm_result(pair="BTC/USDT", success=True, signals=None):
    sig = _bullish() if signals is None else signals
    llm = None if not success else {**{k: getattr(sig, k) for k in (
        "sentiment_score", "fundamental_score", "news_score", "research_debate_score",
        "trader_action_score", "portfolio_decision_score", "available", "sources", "confidences")}}
    return {"type": "llm_analysis_result", "source": "llm_analyst",
            "data": {"ticker": "BTC-USD", "pair": pair, "success": success,
                     "error": None if success else "quota exceeded", "llm_signals": llm}}


def _quant_agent(clock):
    from hybrid.quant_agent import QuantAgent

    bus = RecordingBus()
    agent = QuantAgent(bus, HybridConfig(), llm_max_age_hours=8, clock=clock)
    calls = []

    def analyze_quant_only(symbol, llm_signals=None):
        calls.append(llm_signals)
        llm_on = llm_signals is not None
        decision = SimpleNamespace(action="BUY" if llm_on else "HOLD", composite_score=0.8 if llm_on else 0.5,
                                   confidence=0.4, llm_component=0.675 if llm_on else 0.0,
                                   llm_quant_agreement=False)
        return SimpleNamespace(quant_decision=decision, tech_signals=_tech(0.5))

    agent.pipeline = SimpleNamespace(analyze_quant_only=analyze_quant_only)
    return bus, agent, calls


def _cycle(agent):
    run(agent._on_request({"type": "analyze_request", "target": "quant_agent", "data": {"ticker": "BTC/USDT"}}))


def test_fresh_analysis_is_used_by_the_next_decision():
    clock = Clock()
    bus, agent, calls = _quant_agent(clock)
    run(agent._on_llm_result(_llm_result()))
    clock.now += 1.5 * 3600
    _cycle(agent)

    assert calls[-1] is not None and calls[-1].available
    [decision] = bus.of_type("quant_decision")
    assert decision["data"]["action"] == "BUY"
    assert decision["data"]["llm_component"] == pytest.approx(0.675)
    assert decision["data"]["llm_age_hours"] == pytest.approx(1.5)
    assert decision["data"]["agreement"] is False


def test_no_analysis_means_technicals_only():
    bus, agent, calls = _quant_agent(Clock())
    _cycle(agent)
    assert calls == [None]
    [decision] = bus.of_type("quant_decision")
    assert decision["data"]["llm_component"] is None and decision["data"]["llm_age_hours"] is None


def test_signals_at_the_age_limit_are_used_and_older_ones_are_not():
    clock = Clock()
    _, agent, _ = _quant_agent(clock)
    run(agent._on_llm_result(_llm_result()))
    clock.now += 8 * 3600
    assert agent.fresh_llm("BTC/USDT")[0] is not None
    clock.now += 1
    assert agent.fresh_llm("BTC/USDT") == (None, None)


def test_failed_analysis_keeps_the_last_good_signals():
    clock = Clock()
    _, agent, _ = _quant_agent(clock)
    run(agent._on_llm_result(_llm_result()))
    run(agent._on_llm_result(_llm_result(success=False)))
    signals, age = agent.fresh_llm("BTC/USDT")
    assert signals is not None and signals.sentiment_score == 0.9 and age == 0


def test_results_are_kept_per_pair():
    _, agent, _ = _quant_agent(Clock())
    run(agent._on_llm_result(_llm_result(pair="ETH/USDT")))
    assert agent.fresh_llm("BTC/USDT") == (None, None)
    assert agent.fresh_llm("ETH/USDT")[0] is not None


def test_unknown_signal_fields_are_ignored_and_null_signals_are_skipped():
    _, agent, _ = _quant_agent(Clock())
    newer = _llm_result()
    newer["data"]["llm_signals"]["macro_score"] = 0.7  # field from a newer publisher
    run(agent._on_llm_result(newer))
    assert agent.fresh_llm("BTC/USDT")[0] is not None

    _, agent, _ = _quant_agent(Clock())
    broken = _llm_result()
    broken["data"]["llm_signals"] = None  # success without signals
    run(agent._on_llm_result(broken))
    assert agent.fresh_llm("BTC/USDT") == (None, None)


def test_placeholder_signals_are_not_stored():
    _, agent, _ = _quant_agent(Clock())
    run(agent._on_llm_result(_llm_result(signals=LLMSignals())))  # available=False
    assert agent.fresh_llm("BTC/USDT") == (None, None)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $S/mypyenv/bin/python -m pytest -q -p asyncio tests/test_llm_live_loop.py`
Expected: the 7 new tests FAIL (`unexpected keyword argument 'llm_max_age_hours'`).

- [ ] **Step 3: Implement**

Replace the imports and class in `hybrid/quant_agent.py` (keep `_USD_QUOTES` and `market_data_symbol` as they are):

```python
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
```

```python
class QuantAgent(BaseAgent):
    def __init__(self, bus: MessageBus, config: HybridConfig, llm_max_age_hours: float = 8.0,
                 clock: Callable[[], float] = time.time):
        super().__init__("quant_agent", bus)
        self.config = config
        self.pipeline = HybridPipeline(config=config, skip_llm=True)
        self.llm_max_age_s = llm_max_age_hours * 3600
        self.clock = clock
        # exchange pair -> (signals from the latest successful LLM analysis, when it arrived)
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
        # ignore fields this version doesn't know (a newer publisher), never crash on them
        signals = LLMSignals(**{k: v for k, v in raw.items() if k in _LLM_FIELDS})
        if not signals.available:
            return
        self.llm_latest[data.get("pair") or data["ticker"]] = (signals, self.clock())

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
```

(the `tech_signals` dict and the rest of the publish call stay exactly as they are).

Update the fake in `tests/test_decision_to_execution.py::test_quant_agent_analyses_yahoo_symbol_and_reports_exchange_pair`:

```python
    def analyze_quant_only(symbol, llm_signals=None):
        analysed.append(symbol)
        decision = SimpleNamespace(action="BUY", composite_score=0.8, confidence=0.4,
                                   llm_component=0.0, llm_quant_agreement=True)
        return SimpleNamespace(quant_decision=decision, tech_signals=_tech(0.8))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $S/mypyenv/bin/python -m pytest -q -p asyncio tests/test_llm_live_loop.py tests/test_decision_to_execution.py`
Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add hybrid/quant_agent.py tests/test_llm_live_loop.py tests/test_decision_to_execution.py
git commit -m "feat: quant agent decides with the latest fresh LLM analysis"
```

---

### Task 4: Risk agent honours agreement; orchestrator logs the signal source and LLM failures

**Files:**
- Modify: `hybrid/risk_agent.py:73-79` (the `QuantDecision(...)` in `_on_quant_decision`)
- Modify: `hybrid/orchestrator.py:207-222` (`_on_signal`)
- Test: `tests/test_llm_live_loop.py`

**Interfaces:**
- Consumes: `quant_decision` data keys `agreement`, `llm_age_hours` (Task 3); `llm_analysis_result` data keys `pair`, `success`, `error`, `duration_seconds` (Task 1).
- Produces: log lines `[Orchestrator] Quant decision: <ACTION> <PAIR> (LLM <x.y>h old)` / `(technicals only)`, `[Orchestrator] LLM analysis complete for <PAIR> (<n>s)`, `[LLM] <PAIR> analysis failed: <error>`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_llm_live_loop.py`:

```python
# ── Task 4: agreement reaches the risk manager; the log says where a decision came from ──

def _quant_decision(agreement, llm_age=None):
    return {"type": "quant_decision", "source": "quant_agent",
            "data": {"ticker": "BTC/USDT", "action": "BUY", "score": 0.8, "confidence": 1.0,
                     "llm_component": 0.675 if llm_age is not None else None,
                     "llm_age_hours": llm_age, "agreement": agreement,
                     "tech_signals": {"current_price": 84000.0, "atr": 2400.0}}}


def test_disagreement_halves_the_position_size():
    from hybrid.risk_agent import RiskAgent

    sizes = {}
    for agreement in (True, False):
        bus = RecordingBus()
        run(RiskAgent(bus, HybridConfig())._on_quant_decision(_quant_decision(agreement, llm_age=1.0)))
        [assessment] = bus.of_type("risk_assessment")
        sizes[agreement] = assessment["data"]["position_size_usd"]
    assert sizes[False] == pytest.approx(sizes[True] * 0.5, rel=1e-3)


def test_decision_without_agreement_field_is_not_penalised():
    from hybrid.risk_agent import RiskAgent

    payload = _quant_decision(True)
    del payload["data"]["agreement"]  # older publisher
    bus = RecordingBus()
    run(RiskAgent(bus, HybridConfig())._on_quant_decision(payload))
    [assessment] = bus.of_type("risk_assessment")
    assert assessment["data"]["position_size_usd"] > 0


def test_orchestrator_logs_decision_source_and_llm_failures(capsys):
    from hybrid.orchestrator import Orchestrator

    orch = Orchestrator(RecordingBus(), HybridConfig())
    run(orch._on_signal(_quant_decision(True, llm_age=1.234)))
    run(orch._on_signal(_quant_decision(True)))
    run(orch._on_signal({"type": "llm_analysis_result", "data": {
        "ticker": "BTC-USD", "pair": "BTC/USDT", "success": True, "duration_seconds": 312.4}}))
    run(orch._on_signal({"type": "llm_analysis_result", "data": {
        "ticker": "ETH-USD", "pair": "ETH/USDT", "success": False, "error": "quota exceeded"}}))

    out = capsys.readouterr().out
    assert "Quant decision: BUY BTC/USDT (LLM 1.2h old)" in out
    assert "Quant decision: BUY BTC/USDT (technicals only)" in out
    assert "LLM analysis complete for BTC/USDT (312s)" in out
    assert "[LLM] ETH/USDT analysis failed: quota exceeded" in out
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $S/mypyenv/bin/python -m pytest -q -p asyncio tests/test_llm_live_loop.py -k "agreement or penalised or orchestrator"`
Expected: `test_disagreement_halves_the_position_size` and `test_orchestrator_logs_decision_source_and_llm_failures` FAIL; `test_decision_without_agreement_field_is_not_penalised` passes.

- [ ] **Step 3: Implement**

In `hybrid/risk_agent.py`, `_on_quant_decision`:

```python
        decision = QuantDecision(
            action=data["action"],
            composite_score=data["score"],
            confidence=data["confidence"],
            llm_component=0,
            quant_component=0,
            # LLM and technicals pointing different ways halves the size (risk_manager check 8)
            llm_quant_agreement=data.get("agreement", True),
        )
```

In `hybrid/orchestrator.py`, `_on_signal`, replace the `quant_decision` and `llm_analysis_result` branches:

```python
        if msg_type == "quant_decision":
            data = payload['data']
            age = data.get("llm_age_hours")
            source = f"LLM {age:.1f}h old" if age is not None else "technicals only"
            print(f"[Orchestrator] Quant decision: {data['action']} {data['ticker']} ({source})")
```

```python
        elif msg_type == "llm_analysis_result":
            data = payload['data']
            pair = data.get("pair") or data['ticker']
            if data['success']:
                print(f"[Orchestrator] LLM analysis complete for {pair} "
                      f"({data.get('duration_seconds') or 0:.0f}s)")
            else:
                print(f"[LLM] {pair} analysis failed: {data.get('error')}")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $S/mypyenv/bin/python -m pytest -q -p asyncio tests`
Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add hybrid/risk_agent.py hybrid/orchestrator.py tests/test_llm_live_loop.py
git commit -m "feat: live risk sizing honours LLM/quant agreement; log decision source"
```

---

### Task 5: Schedule LLM analyses and wire them into the live system

**Files:**
- Create: `hybrid/llm_schedule.py`
- Modify: `hybrid/main_async.py` (`__init__` settings, `QuantAgent(...)` at line 102, `run()` at 190-205, `_analysis_loop` at 227-241)
- Modify: `.env.template` (document the three variables)
- Test: `tests/test_llm_live_loop.py`

**Interfaces:**
- Consumes: `market_data_symbol(pair)` from `hybrid/quant_agent.py`; `QuantAgent(..., llm_max_age_hours=...)` (Task 3); `llm_analysis_request` handling in `LLMAnalystAgent` with `pair` (Task 1).
- Produces: `TRADED_PAIRS: tuple[str, ...] = ("BTC/USDT", "ETH/USDT")`; `LLMSettings(enabled: bool = True, interval_hours: float = 4.0, max_age_hours: float = 8.0)` (frozen dataclass); `llm_settings(env: Mapping[str, str] = os.environ) -> LLMSettings`; `request_analyses(bus, pairs, trade_date: str) -> None`; `async llm_analysis_loop(bus, pairs, settings, sleep=asyncio.sleep, today=<UTC date str fn>) -> None`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_llm_live_loop.py`:

```python
# ── Task 5: scheduler ──

def test_settings_defaults_and_overrides():
    from hybrid.llm_schedule import LLMSettings, llm_settings

    assert llm_settings({}) == LLMSettings(enabled=True, interval_hours=4.0, max_age_hours=8.0)
    assert llm_settings({"LLM_ENABLED": "FALSE", "LLM_INTERVAL_HOURS": "6", "LLM_MAX_AGE_HOURS": "12"}) == \
        LLMSettings(enabled=False, interval_hours=6.0, max_age_hours=12.0)


@pytest.mark.parametrize("bad", ["4h", "0", "-1", ""])
def test_bad_interval_falls_back_to_the_default(bad, capsys):
    from hybrid.llm_schedule import llm_settings

    assert llm_settings({"LLM_INTERVAL_HOURS": bad}).interval_hours == 4.0
    if bad:
        assert "LLM_INTERVAL_HOURS" in capsys.readouterr().out


def test_requests_carry_the_yahoo_symbol_and_the_pair():
    from hybrid.llm_schedule import request_analyses

    bus = RecordingBus()
    request_analyses(bus, ("BTC/USDT", "ETH/USDT"), "2026-09-27")
    requests = bus.of_type("llm_analysis_request")
    assert [r["target"] for r in requests] == ["llm_analyst", "llm_analyst"]
    assert [r["data"] for r in requests] == [
        {"ticker": "BTC-USD", "pair": "BTC/USDT", "trade_date": "2026-09-27", "asset_type": "crypto"},
        {"ticker": "ETH-USD", "pair": "ETH/USDT", "trade_date": "2026-09-27", "asset_type": "crypto"},
    ]
    assert requests[0]["request_id"] != requests[1]["request_id"]


class _Stop(Exception):
    pass


def test_loop_requests_at_startup_and_every_interval():
    from hybrid.llm_schedule import LLMSettings, llm_analysis_loop

    bus, sleeps = RecordingBus(), []

    async def sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 2:
            raise _Stop

    with pytest.raises(_Stop):
        run(llm_analysis_loop(bus, ("BTC/USDT", "ETH/USDT"), LLMSettings(interval_hours=4),
                              sleep=sleep, today=lambda: "2026-09-27"))
    assert len(bus.of_type("llm_analysis_request")) == 4  # startup round + one after 4h
    assert sleeps == [4 * 3600, 4 * 3600]


def test_loop_sends_nothing_when_disabled():
    from hybrid.llm_schedule import LLMSettings, llm_analysis_loop

    bus = RecordingBus()
    run(llm_analysis_loop(bus, ("BTC/USDT",), LLMSettings(enabled=False), today=lambda: "2026-09-27"))
    assert bus.of_type("llm_analysis_request") == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $S/mypyenv/bin/python -m pytest -q -p asyncio tests/test_llm_live_loop.py -k "settings or interval or requests or loop"`
Expected: FAIL with `ModuleNotFoundError: No module named 'hybrid.llm_schedule'`.

- [ ] **Step 3: Implement the scheduler**

Create `hybrid/llm_schedule.py`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $S/mypyenv/bin/python -m pytest -q -p asyncio tests/test_llm_live_loop.py`
Expected: all passed.

- [ ] **Step 5: Wire into `hybrid/main_async.py`**

Add the import next to the other `hybrid` imports:

```python
from hybrid.llm_schedule import TRADED_PAIRS, llm_analysis_loop, llm_settings
```

Where the core agents are built (line 102), read the settings once and pass the max age:

```python
        self.llm_settings = llm_settings()
        self.agents['quant'] = QuantAgent(self.bus, self.config,
                                          llm_max_age_hours=self.llm_settings.max_age_hours)
```

In `run()`, start the loop with the others:

```python
        analysis_task = asyncio.create_task(self._analysis_loop())
        llm_task = asyncio.create_task(llm_analysis_loop(self.bus, TRADED_PAIRS, self.llm_settings))
        learning_task = asyncio.create_task(self._learning_loop())
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        equity_snapshot_task = asyncio.create_task(self._equity_snapshot_loop())
        self.background_tasks.extend([analysis_task, llm_task, heartbeat_task, equity_snapshot_task,
                                      learning_task])
```

In `_analysis_loop`, replace the two hard-coded calls:

```python
                for pair in TRADED_PAIRS:
                    await orchestrator.analyze(pair)
```

Append to `.env.template`:

```bash
# LLM analysis in the live loop (TradingAgents on Gemini; needs GOOGLE_API_KEY)
# LLM_ENABLED=false makes every decision technicals-only
LLM_ENABLED=true
LLM_INTERVAL_HOURS=4   # time between analysis rounds for each pair
LLM_MAX_AGE_HOURS=8    # older signals are ignored
```

- [ ] **Step 6: Full suite and type check**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $S/mypyenv/bin/python -m pytest -q -p asyncio tests && $S/mypyenv/bin/python -m mypy hybrid/ --ignore-missing-imports`
Expected: all passed; `Success: no issues found`.

- [ ] **Step 7: Commit**

```bash
git add hybrid/llm_schedule.py hybrid/main_async.py .env.template tests/test_llm_live_loop.py
git commit -m "feat: run the LLM analysis every LLM_INTERVAL_HOURS in the live loop"
```

---

### Task 6: Live check (after merge and auto-deploy)

Not code; do this after the PR merges and CI deploys to the VM.

- [ ] **Step 1:** On the VM, confirm startup: `docker compose logs --no-log-prefix trading-system | grep -E "\[LLM\]|LLM analysis"` shows `[LLM] Analysing BTC/USDT, ETH/USDT now and every 4h`.
- [ ] **Step 2:** Within ~15 minutes: `LLM analysis complete for BTC/USDT (…s)` then ETH, or an `[LLM] … analysis failed: …` line to diagnose.
- [ ] **Step 3:** The next cycles log `Quant decision: … (LLM 0.xh old)`; the dashboard Signals panel lists the new analyses.
