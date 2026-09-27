"""
Signal Extractor
================
Converts qualitative LLM agent outputs from TradingAgents into
numerical scores (0.0 to 1.0) that the Quant Engine can consume.

This is the BRIDGE between Track A (LLM) and Track B (Math).

Three tiers, most exact first:
1. Labels. TradingAgents renders its structured outputs with fixed headers
   (``**Recommendation**: Sell``, ``**Action**: Buy``, ``**Rating**: Hold``,
   ``**Overall Sentiment:** **Bearish** (Score: 2.5/10)``); these are parsed in code.
2. TypeSafe (when TYPESAFE_API_KEY is set). Free-text reports (fundamentals, news)
   and ratings whose label is missing (a provider's free-text fallback) are judged
   in one batched System One call: a Score for outlook, a Choice for ratings.
3. Otherwise the signal stays neutral (0.5) and is marked "unavailable" rather
   than guessed from keywords.
"""

import os
import re
from dataclasses import dataclass, field
from typing import Callable, Optional


# ── Rating-to-Score Mapping ──
# TradingAgents uses a 5-tier rating system. We map each to a 0-1 score.
RATING_SCORES = {
    "buy": 1.0,
    "overweight": 0.75,
    "hold": 0.50,
    "underweight": 0.25,
    "sell": 0.0,
}
TRADER_ACTION_SCORES = {"buy": 1.0, "hold": 0.5, "sell": 0.0}

SIGNALS = ("sentiment", "fundamental", "news", "research_debate", "trader_action", "portfolio_decision")
# The TradingAgents report each signal is read from (score attribute is f"{signal}_score")
SIGNAL_REPORTS = {
    "sentiment": "sentiment_report",
    "fundamental": "fundamentals_report",
    "news": "news_report",
    "research_debate": "investment_plan",
    "trader_action": "trader_investment_plan",
    "portfolio_decision": "final_trade_decision",
}

# Outlook Score levels, ordered bearish -> bullish; score / 4 gives the 0-1 signal.
OUTLOOK_LEVELS = [
    "Strongly bearish: expects the stock to fall, or recommends selling or avoiding it",
    "Mildly bearish: leans negative; the concerns outweigh the positives",
    "Neutral, mixed, or no clear view on the stock's direction",
    "Mildly bullish: leans positive; the positives outweigh the concerns",
    "Strongly bullish: expects the stock to rise, or recommends buying or adding to it",
]
OUTLOOK_BANDS = ["bearish", "mildly bearish", "neutral", "mildly bullish", "bullish"]
NO_RATING = "no clear rating"


@dataclass
class LLMSignals:
    """Numerical scores extracted from TradingAgents output.

    All scores are normalized to [0.0, 1.0] where:
    - 0.0 = extremely bearish / sell
    - 0.5 = neutral / hold
    - 1.0 = extremely bullish / buy
    """

    sentiment_score: float = 0.5
    fundamental_score: float = 0.5
    news_score: float = 0.5
    research_debate_score: float = 0.5
    trader_action_score: float = 0.5
    portfolio_decision_score: float = 0.5

    # True only when extracted from a real TradingAgents run. Defaults (skipped or
    # failed LLM analysis) are placeholders, not a neutral opinion.
    available: bool = False

    # Raw metadata for logging
    sentiment_band: str = "neutral"
    sentiment_confidence: str = "low"
    portfolio_rating: str = "hold"
    trader_action: str = "hold"
    # How each signal was obtained: "label", "typesafe" or "unavailable" (left neutral)
    sources: dict = field(default_factory=dict)
    # TypeSafe answer confidence (0-1) for signals it judged
    confidences: dict = field(default_factory=dict)

    def summary(self) -> str:
        """Human-readable summary of extracted signals."""
        def src(name):
            return self.sources.get(name, "unavailable")
        return (
            f"LLM Signals:\n"
            f"  Sentiment:  {self.sentiment_score:.2f} ({self.sentiment_band}) [{src('sentiment')}]\n"
            f"  Fundamental: {self.fundamental_score:.2f} [{src('fundamental')}]\n"
            f"  News:        {self.news_score:.2f} [{src('news')}]\n"
            f"  Debate:      {self.research_debate_score:.2f} [{src('research_debate')}]\n"
            f"  Trader:      {self.trader_action_score:.2f} ({self.trader_action}) [{src('trader_action')}]\n"
            f"  Portfolio:   {self.portfolio_decision_score:.2f} ({self.portfolio_rating}) "
            f"[{src('portfolio_decision')}]\n"
        )


class TypeSafeJudge:
    """Asks TypeSafe System One questions; one batched call per extraction."""

    def __init__(self, client=None):
        if client is None:
            from typesafe_sdk import TypeSafeClient  # reads TYPESAFE_API_KEY; model defaults to jev-latest
            client = TypeSafeClient()
        self.client = client

    def ask(self, state: dict, questions: dict):
        return self.client.system_one(state=state, questions=questions)


def default_judge() -> Optional[TypeSafeJudge]:
    """A TypeSafe judge when TYPESAFE_API_KEY is set, otherwise None."""
    return TypeSafeJudge() if os.getenv("TYPESAFE_API_KEY") else None


def extract_signals(final_state: dict, judge: Optional[TypeSafeJudge] = None) -> LLMSignals:
    """Extract numerical scores from TradingAgents' final graph state.

    Args:
        final_state: The complete state dictionary returned by
                     TradingAgentsGraph.propagate().
        judge: Optional TypeSafe judge for free text and missing labels.

    Returns:
        LLMSignals with all scores normalized to [0.0, 1.0].
    """
    signals = LLMSignals(available=True, sources={name: "unavailable" for name in SIGNALS})
    # signal -> (state key, report text, question, apply(answer))
    asks: dict[str, tuple[str, str, object, Callable]] = {}

    # ── 1. Sentiment ──
    text = final_state.get("sentiment_report") or ""
    parsed = _parse_sentiment(text)
    if parsed:
        signals.sentiment_score, signals.sentiment_band, signals.sentiment_confidence = parsed
        signals.sources["sentiment"] = "label"
    elif text:
        def apply_sentiment(answer):
            signals.sentiment_score = answer.score / 4
            signals.sentiment_band = OUTLOOK_BANDS[round(answer.score)]
        asks["sentiment"] = ("sentiment_report", text,
                             _outlook_question("sentiment_report", "the market sentiment it describes"),
                             apply_sentiment)

    # ── 2-3. Fundamentals and news: free text, TypeSafe only ──
    for name, key, subject in (("fundamental", "fundamentals_report", "the company's fundamentals"),
                               ("news", "news_report", "the net effect of the news")):
        text = final_state.get(key) or ""
        if text:
            asks[name] = (key, text, _outlook_question(key, subject),
                          lambda answer, name=name: setattr(signals, f"{name}_score", answer.score / 4))

    # ── 4. Research Manager recommendation ──
    text = final_state.get("investment_plan") or ""
    rating = _parse_label(text, "Recommendation", RATING_SCORES)
    if rating:
        signals.research_debate_score = RATING_SCORES[rating]
        signals.sources["research_debate"] = "label"
    elif text:
        asks["research_debate"] = ("investment_plan", text, _rating_question("investment_plan", RATING_SCORES),
                                   lambda choice: setattr(signals, "research_debate_score", RATING_SCORES[choice]))

    # ── 5. Trader action ──
    text = final_state.get("trader_investment_plan") or ""
    action = _parse_label(text, "Action", TRADER_ACTION_SCORES)
    if action:
        signals.trader_action_score, signals.trader_action = TRADER_ACTION_SCORES[action], action
        signals.sources["trader_action"] = "label"
    elif text:
        def apply_action(choice):
            signals.trader_action_score, signals.trader_action = TRADER_ACTION_SCORES[choice], choice
        asks["trader_action"] = ("trader_investment_plan", text,
                                 _rating_question("trader_investment_plan", TRADER_ACTION_SCORES), apply_action)

    # ── 6. Portfolio Manager decision (final LLM output) ──
    text = final_state.get("final_trade_decision") or ""
    rating = _parse_label(text, "Rating", RATING_SCORES)
    if rating:
        signals.portfolio_decision_score, signals.portfolio_rating = RATING_SCORES[rating], rating
        signals.sources["portfolio_decision"] = "label"
    elif text:
        def apply_rating(choice):
            signals.portfolio_decision_score, signals.portfolio_rating = RATING_SCORES[choice], choice
        asks["portfolio_decision"] = ("final_trade_decision", text,
                                      _rating_question("final_trade_decision", RATING_SCORES), apply_rating)

    if asks and judge:
        _apply_judgments(signals, asks, judge)
    return signals


def _apply_judgments(signals: LLMSignals, asks: dict, judge: TypeSafeJudge) -> None:
    """One batched TypeSafe call; each question sees only its own report."""
    try:
        response = judge.ask(state={key: _compact(text) for key, text, _, _ in asks.values()},
                             questions={name: question for name, (_, _, question, _) in asks.items()})
    except Exception as e:
        print(f"[SignalExtractor] TypeSafe unavailable, leaving {sorted(asks)} neutral: {e}")
        return
    for name, (_, _, _, apply) in asks.items():
        if name in response.scores:
            apply(response.scores[name])
            signals.sources[name] = "typesafe"
            signals.confidences[name] = response.scores[name].confidence
        elif name in response.choices and response.choices[name].choice != NO_RATING:
            apply(response.choices[name].choice)
            signals.sources[name] = "typesafe"
            signals.confidences[name] = response.choices[name].confidence


def _compact(text: str) -> str:
    """Collapse runs of spaces/tabs and blank lines. An LLM once padded a 28k-character report
    with ~1.8M spaces, which pushed the TypeSafe call over its token limit."""
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _outlook_question(state_key: str, subject: str):
    from typesafe_sdk import Score
    return Score(
        instructions=(
            f"Judging {subject}, how bullish or bearish is `{state_key}` about the stock? "
            "Judge the report's overall conclusion, not individual words: a risk the report "
            "mentions but dismisses is not bearish, and a strength it says is fading is not bullish."
        ),
        criteria=OUTLOOK_LEVELS,
    )


def _rating_question(state_key: str, ratings: dict):
    from typesafe_sdk import Choice
    criteria = {name: f"`{state_key}` recommends: {name}" for name in ratings}
    criteria[NO_RATING] = f"`{state_key}` does not recommend a specific position"
    return Choice(instructions=f"What position does `{state_key}` finally recommend for the stock?",
                  criteria=criteria)


def _parse_label(text: str, label: str, values: dict) -> Optional[str]:
    """Value of a rendered ``**Label**: Value`` line, if it's one of ``values``."""
    match = re.search(rf"^\*\*{label}\*\*:\s*\**\s*([A-Za-z]+)", text, re.MULTILINE)
    value = match.group(1).lower() if match else None
    return value if value in values else None


def _parse_sentiment(text: str) -> Optional[tuple[float, str, str]]:
    """``**Overall Sentiment:** **Band** (Score: x/10)`` plus ``**Confidence:** Level``."""
    match = re.search(r"\*\*Overall Sentiment:\*\*\s*\*\*([^*]+)\*\*\s*\(Score:\s*(\d+(?:\.\d+)?)/10\)", text)
    if not match:
        return None
    band, score = match.group(1).strip().lower(), float(match.group(2))
    confidence = re.search(r"^\*\*Confidence:\*\*\s*(\w+)", text, re.MULTILINE)
    return min(max(score / 10.0, 0.0), 1.0), band, confidence.group(1).lower() if confidence else "medium"
