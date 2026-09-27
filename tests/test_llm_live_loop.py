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


# ── Task 3: QuantAgent keeps the latest analysis per pair ──

class Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now


def _llm_result(pair="BTC/USDT", success=True, signals=None):
    sig = _bullish() if signals is None else signals
    llm = None if not success else {k: getattr(sig, k) for k in (
        "sentiment_score", "fundamental_score", "news_score", "research_debate_score",
        "trader_action_score", "portfolio_decision_score", "available", "sources", "confidences")}
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


# ── Final review fixes ──

@pytest.mark.parametrize("bad", [
    {"sentiment_score": "0.9"}, {"sentiment_score": None}, {"news_score": 1.7},
    {"fundamental_score": True}, {"available": "false"}, "x",
])
def test_malformed_signals_are_ignored(bad, capsys):
    _, agent, _ = _quant_agent(Clock())
    payload = _llm_result()
    if isinstance(bad, dict):
        payload["data"]["llm_signals"].update(bad)
    else:
        payload["data"]["llm_signals"] = bad
    run(agent._on_llm_result(payload))
    assert agent.fresh_llm("BTC/USDT") == (None, None)
    assert "malformed" in capsys.readouterr().out


def test_timed_out_analysis_is_not_overlapped_on_the_shared_graph(monkeypatch):
    import threading
    from hybrid.llm_agent import LLMAnalysisRequest, LLMAnalystAgent

    release, active, overlaps = threading.Event(), [], []

    def propagate(ticker, date, asset_type):
        if active:
            overlaps.append(ticker)
        active.append(ticker)
        release.wait(1)  # hangs past the timeout
        active.remove(ticker)
        return {"final_trade_decision": "**Rating**: Hold"}, "Hold"

    agent = LLMAnalystAgent(RecordingBus(), HybridConfig(), max_concurrent=1, timeout_seconds=0.2)
    monkeypatch.setattr(agent, "_get_ta_graph", lambda: SimpleNamespace(propagate=propagate))
    monkeypatch.setattr("hybrid.llm_agent.record_llm_analysis", lambda *a, **k: None)
    monkeypatch.setattr("hybrid.llm_agent.default_judge", lambda: None)

    def request(pair):
        return LLMAnalysisRequest(ticker=pair, trade_date="2026-09-27", asset_type="crypto", pair=pair)

    async def both():
        first = await agent._run_analysis("a", request("BTC/USDT"))
        second = await agent._run_analysis("b", request("ETH/USDT"))
        return first, second

    try:
        first, second = run(both())
    finally:
        release.set()
    assert not first.success and "timeout" in first.error
    assert not second.success and "still running" in second.error
    assert overlaps == []
