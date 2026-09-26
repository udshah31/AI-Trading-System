"""LLM signal extraction: exact labels TradingAgents renders are parsed in code; free-text
reports (and labels missing after a provider's free-text fallback) go to TypeSafe when
configured, otherwise stay neutral instead of being keyword-guessed."""
from types import SimpleNamespace

import pytest

from hybrid.signal_extractor import TypeSafeJudge, default_judge, extract_signals

# Shapes rendered by TradingAgents' schemas.py (render_research_plan, render_trader_proposal,
# render_pm_decision, render_sentiment_report).
RESEARCH_SELL = ("**Recommendation**: Sell\n\n**Rationale**: The bull case (buy on dip) was rejected.\n\n"
                 "**Strategic Actions**: Exit on strength; do not buy more.")
TRADER_SELL = "**Action**: Sell\n\n**Reasoning**: We would not buy here.\n\nFINAL TRANSACTION PROPOSAL: **SELL**"
PM_UNDERWEIGHT = ("**Rating**: Underweight\n\n**Executive Summary**: We considered Hold but downgrade "
                  "given the buy-side crowding.\n\n**Investment Thesis**: Buy-side positioning is stretched.")
SENTIMENT_BEARISH = ("**Overall Sentiment:** **Bearish** (Score: 2.5/10)\n**Confidence:** High\n\n"
                     "Retail is not bullish at all; low conviction shorts are growing.")
FUNDAMENTALS = ("Revenue growth slowed but margins are strong. We see limited downside risk and no major "
                "concerns; risk/reward is attractive.")
NEWS = "Guidance was cut sharply after a regulator opened a probe into the core product."


def _state(**overrides):
    state = {"investment_plan": RESEARCH_SELL, "trader_investment_plan": TRADER_SELL,
             "final_trade_decision": PM_UNDERWEIGHT, "sentiment_report": SENTIMENT_BEARISH,
             "fundamentals_report": FUNDAMENTALS, "news_report": NEWS}
    state.update(overrides)
    return state


# ── labels parsed in code (previously first-match word scans that inverted these) ──

def test_labelled_ratings_are_parsed_exactly():
    s = extract_signals(_state())
    assert (s.research_debate_score, s.sources["research_debate"]) == (0.0, "label")
    assert (s.trader_action_score, s.trader_action) == (0.0, "sell")
    assert (s.portfolio_decision_score, s.portfolio_rating) == (0.25, "underweight")


def test_sentiment_uses_rendered_score_band_and_confidence():
    s = extract_signals(_state())
    assert s.sentiment_score == pytest.approx(0.25)
    assert (s.sentiment_band, s.sentiment_confidence) == ("bearish", "high")
    mild = "**Overall Sentiment:** **Mildly Bullish** (Score: 6.5/10)\n**Confidence:** Medium\n\nok"
    assert extract_signals(_state(sentiment_report=mild)).sentiment_score == pytest.approx(0.65)


def test_without_judge_free_text_is_neutral_not_keyword_guessed():
    s = extract_signals(_state())
    assert (s.fundamental_score, s.news_score) == (0.5, 0.5)
    assert s.sources["fundamental"] == s.sources["news"] == "unavailable"


def test_without_judge_unlabelled_rating_is_neutral():
    s = extract_signals(_state(investment_plan="The bull case (buy on dip) was rejected. We'd sell."))
    assert (s.research_debate_score, s.sources["research_debate"]) == (0.5, "unavailable")


# ── TypeSafe for free text and missing labels ──

class FakeTypeSafe:
    def __init__(self, scores=None, choices=None, error=None):
        self.scores, self.choices, self.error = scores or {}, choices or {}, error
        self.calls = []

    def system_one(self, state, questions):
        self.calls.append((state, questions))
        if self.error:
            raise self.error
        return SimpleNamespace(
            scores={k: SimpleNamespace(score=v, confidence=0.8) for k, v in self.scores.items() if k in questions},
            choices={k: SimpleNamespace(choice=v, confidence=0.8) for k, v in self.choices.items() if k in questions})


def test_free_text_reports_scored_in_one_batched_call():
    client = FakeTypeSafe(scores={"fundamental": 3.5, "news": 1.0})
    s = extract_signals(_state(), judge=TypeSafeJudge(client=client))

    assert len(client.calls) == 1
    state, questions = client.calls[0]
    assert set(state) == {"fundamentals_report", "news_report"}  # only what the questions need
    assert set(questions) == {"fundamental", "news"}             # labelled signals aren't re-asked
    assert s.fundamental_score == pytest.approx(3.5 / 4)
    assert s.news_score == pytest.approx(0.25)
    assert s.sources["fundamental"] == "typesafe"


def test_unlabelled_ratings_fall_back_to_typesafe_choice():
    client = FakeTypeSafe(scores={"fundamental": 2.0, "news": 2.0},
                          choices={"research_debate": "sell", "portfolio_decision": "no clear rating"})
    s = extract_signals(_state(investment_plan="We'd rather sell into strength.",
                               final_trade_decision="Hard to call; stay flexible."),
                        judge=TypeSafeJudge(client=client))
    assert (s.research_debate_score, s.sources["research_debate"]) == (0.0, "typesafe")
    assert (s.portfolio_decision_score, s.sources["portfolio_decision"]) == (0.5, "unavailable")


def test_judge_failure_leaves_signals_neutral():
    client = FakeTypeSafe(error=RuntimeError("TypeSafe 503"))
    s = extract_signals(_state(), judge=TypeSafeJudge(client=client))
    assert (s.fundamental_score, s.news_score) == (0.5, 0.5)
    assert s.research_debate_score == 0.0  # labels still parsed


def test_default_judge_needs_api_key(monkeypatch):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    assert default_judge() is None
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    assert isinstance(default_judge(), TypeSafeJudge)
