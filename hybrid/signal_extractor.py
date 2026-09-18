"""
Signal Extractor
================
Converts qualitative LLM agent outputs from TradingAgents into
numerical scores (0.0 to 1.0) that the Quant Engine can consume.

This is the BRIDGE between Track A (LLM) and Track B (Math).
The LLM produces text + structured data; this module distills it
into pure numbers.
"""

import re
from dataclasses import dataclass
from typing import Optional


# ── Rating-to-Score Mapping ──
# TradingAgents uses a 5-tier rating system. We map each to a 0-1 score.
RATING_SCORES = {
    "buy": 1.0,
    "overweight": 0.75,
    "hold": 0.50,
    "underweight": 0.25,
    "sell": 0.0,
}

# Sentiment band mapping (from TradingAgents SentimentReport)
SENTIMENT_BAND_SCORES = {
    "bullish": 1.0,
    "mildly bullish": 0.75,
    "neutral": 0.50,
    "mixed": 0.50,
    "mildly bearish": 0.25,
    "bearish": 0.0,
}


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

    # Raw metadata for logging
    sentiment_band: str = "neutral"
    sentiment_confidence: str = "low"
    portfolio_rating: str = "hold"
    trader_action: str = "hold"

    def summary(self) -> str:
        """Human-readable summary of extracted signals."""
        return (
            f"LLM Signals:\n"
            f"  Sentiment:  {self.sentiment_score:.2f} ({self.sentiment_band})\n"
            f"  Fundamental: {self.fundamental_score:.2f}\n"
            f"  News:        {self.news_score:.2f}\n"
            f"  Debate:      {self.research_debate_score:.2f}\n"
            f"  Trader:      {self.trader_action_score:.2f} ({self.trader_action})\n"
            f"  Portfolio:   {self.portfolio_decision_score:.2f} ({self.portfolio_rating})\n"
        )


def extract_signals(final_state: dict) -> LLMSignals:
    """Extract numerical scores from TradingAgents' final graph state.

    Args:
        final_state: The complete state dictionary returned by
                     TradingAgentsGraph.propagate().

    Returns:
        LLMSignals with all scores normalized to [0.0, 1.0].
    """
    signals = LLMSignals()

    # ── 1. Sentiment Score ──
    # TradingAgents' sentiment_analyst outputs a SentimentReport with
    # overall_band (Bullish/Bearish/etc.) and overall_score (0-10).
    sentiment_report = final_state.get("sentiment_report", "")
    signals.sentiment_score, signals.sentiment_band, signals.sentiment_confidence = (
        _extract_sentiment(sentiment_report)
    )

    # ── 2. Fundamental Score ──
    # The fundamentals report is free text. We parse for bullish/bearish
    # keywords and look for the rating embedded in the report.
    fundamentals_report = final_state.get("fundamentals_report", "")
    signals.fundamental_score = _extract_from_report(fundamentals_report)

    # ── 3. News Score ──
    news_report = final_state.get("news_report", "")
    signals.news_score = _extract_from_report(news_report)

    # ── 4. Research Debate Score ──
    # The Research Manager's investment_plan contains a recommendation.
    investment_plan = final_state.get("investment_plan", "")
    signals.research_debate_score = _extract_rating_score(investment_plan)

    # ── 5. Trader Action Score ──
    trader_plan = final_state.get("trader_investment_plan", "")
    signals.trader_action_score, signals.trader_action = _extract_trader_action(
        trader_plan
    )

    # ── 6. Portfolio Decision Score (Final LLM output) ──
    final_decision = final_state.get("final_trade_decision", "")
    signals.portfolio_decision_score, signals.portfolio_rating = (
        _extract_portfolio_decision(final_decision)
    )

    return signals


def _extract_sentiment(report: str) -> tuple[float, str, str]:
    """Extract sentiment score from the sentiment report.

    The SentimentReport contains overall_score (0-10) and overall_band.
    """
    if not report:
        return 0.5, "neutral", "low"

    report_lower = report.lower()

    # Try to find the numerical score (0-10 scale)
    score_match = re.search(
        r"(?:overall[_\s]*score|sentiment[_\s]*score)\s*[:=]\s*(\d+(?:\.\d+)?)",
        report_lower,
    )
    if score_match:
        raw_score = float(score_match.group(1))
        normalized = min(max(raw_score / 10.0, 0.0), 1.0)
    else:
        # Fall back to band detection
        normalized = 0.5
        for band, score in SENTIMENT_BAND_SCORES.items():
            if band in report_lower:
                normalized = score
                break

    # Extract band
    band = "neutral"
    for b in SENTIMENT_BAND_SCORES:
        if b in report_lower:
            band = b
            break

    # Extract confidence
    confidence = "medium"
    if "high" in report_lower and "confidence" in report_lower:
        confidence = "high"
    elif "low" in report_lower and "confidence" in report_lower:
        confidence = "low"

    return normalized, band, confidence


def _extract_from_report(report: str) -> float:
    """Extract a bullish/bearish score from a free-text report.

    Uses keyword frequency analysis as a simple NLP signal.
    """
    if not report:
        return 0.5

    report_lower = report.lower()

    # Bullish keywords and their weights
    bullish_keywords = [
        "strong", "growth", "positive", "bullish", "upside", "outperform",
        "beat", "exceeded", "momentum", "opportunity", "upgrade", "buy",
        "overweight", "impressive", "robust", "accelerating", "expanding",
    ]
    bearish_keywords = [
        "weak", "decline", "negative", "bearish", "downside", "underperform",
        "miss", "missed", "risk", "concern", "downgrade", "sell",
        "underweight", "disappointing", "slowing", "contracting", "warning",
    ]

    bull_count = sum(report_lower.count(kw) for kw in bullish_keywords)
    bear_count = sum(report_lower.count(kw) for kw in bearish_keywords)
    total = bull_count + bear_count

    if total == 0:
        return 0.5

    # Normalize: bull_ratio ranges from 0 (all bearish) to 1 (all bullish)
    return bull_count / total


def _extract_rating_score(text: str) -> float:
    """Extract a 5-tier rating from text and convert to 0-1 score."""
    if not text:
        return 0.5

    text_lower = text.lower()

    # Check for explicit ratings (order matters — check specific first)
    for rating, score in RATING_SCORES.items():
        # Match the rating as a standalone word
        if re.search(rf"\b{rating}\b", text_lower):
            return score

    return 0.5


def _extract_trader_action(text: str) -> tuple[float, str]:
    """Extract trader's buy/sell/hold action."""
    if not text:
        return 0.5, "hold"

    text_lower = text.lower()

    # TradingAgents TraderProposal uses Buy/Sell/Hold
    if re.search(r"\baction\s*[:=]\s*buy\b", text_lower) or (
        re.search(r"\bbuy\b", text_lower)
        and not re.search(r"\bsell\b", text_lower)
    ):
        return 1.0, "buy"
    elif re.search(r"\baction\s*[:=]\s*sell\b", text_lower) or (
        re.search(r"\bsell\b", text_lower)
        and not re.search(r"\bbuy\b", text_lower)
    ):
        return 0.0, "sell"

    return 0.5, "hold"


def _extract_portfolio_decision(text: str) -> tuple[float, str]:
    """Extract the Portfolio Manager's final decision."""
    if not text:
        return 0.5, "hold"

    text_lower = text.lower()

    for rating, score in RATING_SCORES.items():
        if re.search(rf"\b{rating}\b", text_lower):
            return score, rating

    return 0.5, "hold"
