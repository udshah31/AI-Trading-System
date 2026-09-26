"""LLM Signals panel: analyses are recorded with their report text, a person can label each
report, and accuracy compares the extractor (by source) and the old keyword method to labels."""
import asyncio
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from hybrid.signal_eval import accuracy, direction, keyword_direction
from hybrid.signal_extractor import LLMSignals, SIGNAL_REPORTS, TypeSafeJudge, extract_signals
from hybrid.storage import StorageService

STATE = {
    "investment_plan": "**Recommendation**: Sell\n\n**Rationale**: Stretched valuation.",
    "trader_investment_plan": "**Action**: Buy\n\n**Reasoning**: Momentum.",
    "final_trade_decision": "**Rating**: Hold\n\n**Executive Summary**: Mixed.",
    "sentiment_report": "**Overall Sentiment:** **Bearish** (Score: 2.0/10)\n**Confidence:** High\n\nFear.",
    "fundamentals_report": "Margins are strong and we see limited downside risk.",
    "news_report": "A regulator opened a probe; guidance was cut.",
}


def _signals():
    s = LLMSignals(available=True, fundamental_score=0.875, news_score=0.25, research_debate_score=0.0,
                   trader_action_score=1.0, portfolio_decision_score=0.5, sentiment_score=0.2)
    s.sources = {"sentiment": "label", "fundamental": "typesafe", "news": "typesafe",
                 "research_debate": "label", "trader_action": "label", "portfolio_decision": "label"}
    s.confidences = {"fundamental": 0.9, "news": 0.2}
    return s


@pytest.fixture
def storage(tmp_path):
    svc = StorageService(f"sqlite+aiosqlite:///{tmp_path / 'llm.db'}")
    asyncio.run(svc.initialize())
    yield svc
    asyncio.run(svc.close())


# ── recording and labels ──

def test_analysis_round_trip_with_labels(storage):
    run = asyncio.run
    aid = run(storage.llm.save_analysis("AAPL", "2026-09-26", STATE, _signals()))
    run(storage.llm.set_label(aid, "news", "bearish"))
    run(storage.llm.set_label(aid, "news", "neutral"))  # relabel overwrites
    run(storage.llm.set_label(aid, "fundamental", "bullish"))
    run(storage.llm.clear_label(aid, "fundamental"))

    analysis, labels = run(storage.llm.get(aid))
    assert analysis.reports["news"] == STATE["news_report"]
    assert analysis.sources["fundamental"] == "typesafe" and analysis.confidences["news"] == 0.2
    assert labels == {"news": "neutral"}
    [(listed, labelled)] = run(storage.llm.list_recent(10))
    assert listed.id == aid and labelled == 1


def test_record_helper_needs_database_url(tmp_path, monkeypatch):
    from hybrid.llm_records import record_llm_analysis

    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert record_llm_analysis(STATE, _signals(), "AAPL", "2026-09-26") is None
    url = f"sqlite+aiosqlite:///{tmp_path / 'rec.db'}"
    aid = record_llm_analysis(STATE, _signals(), "AAPL", "2026-09-26", database_url=url)

    svc = StorageService(url)
    analysis, _ = asyncio.run(svc.llm.get(aid))
    assert analysis.ticker == "AAPL" and analysis.scores["news"] == 0.25
    asyncio.run(svc.close())


def test_record_helper_never_raises(monkeypatch):
    from hybrid.llm_records import record_llm_analysis
    assert record_llm_analysis(STATE, _signals(), "AAPL", "2026-09-26",
                               database_url="postgresql+asyncpg://nobody@127.0.0.1:1/none") is None


def test_typesafe_confidence_is_kept():
    client = SimpleNamespace(system_one=lambda state, questions: SimpleNamespace(
        scores={k: SimpleNamespace(score=3.0, confidence=0.7) for k in questions}, choices={}))
    s = extract_signals(STATE, judge=TypeSafeJudge(client=client))
    assert s.confidences == {"fundamental": 0.7, "news": 0.7}
    assert set(SIGNAL_REPORTS) == set(s.sources)


# ── accuracy ──

def test_direction_thresholds():
    assert [direction(v) for v in (0.0, 0.39, 0.4, 0.5, 0.6, 0.61, 1.0)] == [
        "bearish", "bearish", "neutral", "neutral", "neutral", "bullish", "bullish"]


def test_keyword_baseline_reproduces_old_misread():
    # the pre-TypeSafe method counted "risk"/"downside"/"concerns" as bearish words
    assert keyword_direction(STATE["fundamentals_report"]) == "bearish"


def test_accuracy_by_method_and_confidence():
    rows = [
        {"label": "bullish", "score": 0.875, "source": "typesafe", "confidence": 0.9, "text": STATE["fundamentals_report"]},
        {"label": "bearish", "score": 0.25, "source": "typesafe", "confidence": 0.2, "text": STATE["news_report"]},
        {"label": "neutral", "score": 0.25, "source": "typesafe", "confidence": 0.5, "text": "flat"},
        {"label": "bearish", "score": 0.0, "source": "label", "confidence": None, "text": STATE["investment_plan"]},
        {"label": "bullish", "score": 0.5, "source": "unavailable", "confidence": None, "text": "strong growth"},
    ]
    result = accuracy(rows)
    by = {m["method"]: m for m in result["by_method"]}
    assert (by["typesafe"]["labelled"], by["typesafe"]["agree"]) == (3, 2)
    assert (by["label"]["labelled"], by["label"]["agree"]) == (1, 1)
    assert (by["unavailable"]["labelled"], by["unavailable"]["agree"]) == (1, 0)
    assert by["keyword"]["labelled"] == 5
    bands = {b["band"]: b for b in result["typesafe_by_confidence"]}
    assert (bands["high"]["labelled"], bands["high"]["agree"]) == (1, 1)
    assert (bands["low"]["labelled"], bands["low"]["agree"]) == (1, 1)
    assert (bands["medium"]["labelled"], bands["medium"]["agree"]) == (1, 0)
    assert accuracy([])["by_method"][0]["agreement"] is None


# ── API ──

@pytest.fixture
def client(storage, monkeypatch):
    from hybrid.dashboard import app as dashboard
    monkeypatch.setenv("DASHBOARD_PASSWORD", "pw")
    monkeypatch.setattr(dashboard, "storage", storage)
    return TestClient(dashboard.app, raise_server_exceptions=False), ("admin", "pw")


def test_api_list_detail_label_and_accuracy(client, storage):
    http, auth = client
    aid = asyncio.run(storage.llm.save_analysis("AAPL", "2026-09-26", STATE, _signals()))

    [row] = http.get("/api/llm/analyses", auth=auth).json()
    assert (row["id"], row["ticker"], row["labelled"], row["total"]) == (aid, "AAPL", 0, 6)

    detail = http.get(f"/api/llm/analyses/{aid}", auth=auth).json()
    news = next(s for s in detail["signals"] if s["signal"] == "news")
    assert news["report"] == STATE["news_report"]
    assert (news["direction"], news["source"], news["confidence"], news["label"]) == ("bearish", "typesafe", 0.2, None)

    assert http.put(f"/api/llm/analyses/{aid}/labels/news", json={"label": "bearish"}, auth=auth).status_code == 200
    assert http.put(f"/api/llm/analyses/{aid}/labels/news", json={"label": "maybe"}, auth=auth).status_code == 422
    assert http.put(f"/api/llm/analyses/{aid}/labels/weather", json={"label": "bearish"}, auth=auth).status_code == 422
    assert http.put("/api/llm/analyses/nope/labels/news", json={"label": "bearish"}, auth=auth).status_code == 404

    by = {m["method"]: m for m in http.get("/api/llm/accuracy", auth=auth).json()["by_method"]}
    assert (by["typesafe"]["labelled"], by["typesafe"]["agree"]) == (1, 1)

    assert http.delete(f"/api/llm/analyses/{aid}/labels/news", auth=auth).status_code == 204
    assert http.get("/api/llm/analyses", auth=auth).json()[0]["labelled"] == 0
    assert http.get("/api/llm/analyses", auth=None).status_code == 401  # behind the dashboard login


def test_detail_404(client):
    http, auth = client
    assert http.get("/api/llm/analyses/missing", auth=auth).status_code == 404
