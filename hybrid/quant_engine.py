"""
Quantitative Decision Engine
=============================
The BRAIN of the hybrid system. Takes numerical scores from both
Track A (LLM signals) and Track B (technical indicators) and produces
a single composite score using weighted combination.

KEY PRINCIPLE: This module uses ONLY math. No LLM calls.
"""

from dataclasses import dataclass
from typing import Optional

from hybrid.config import HybridConfig
from hybrid.signal_extractor import LLMSignals
from hybrid.technical_indicators import TechnicalSignals


@dataclass
class QuantDecision:
    """The output of the quantitative decision engine."""

    # Final composite score (0.0 = strong sell, 1.0 = strong buy)
    composite_score: float = 0.5

    # Decision derived from thresholds
    action: str = "HOLD"  # BUY, SELL, or HOLD

    # Confidence level (how far from the neutral zone)
    confidence: float = 0.0  # 0.0 = right at threshold, 1.0 = max conviction

    # Component breakdown for explainability
    llm_component: float = 0.0   # Weighted sum of LLM signals
    quant_component: float = 0.0  # Weighted sum of technical indicators

    # Agreement analysis
    llm_quant_agreement: bool = True  # Do LLM and quant agree on direction?
    agreement_detail: str = ""

    def summary(self) -> str:
        """Human-readable decision summary."""
        agreement = "✅ ALIGNED" if self.llm_quant_agreement else "⚠️ DIVERGENT"
        return (
            f"{'═' * 50}\n"
            f"  QUANT ENGINE DECISION\n"
            f"{'═' * 50}\n"
            f"  Action:          {self.action}\n"
            f"  Composite Score: {self.composite_score:.4f}\n"
            f"  Confidence:      {self.confidence:.1%}\n"
            f"{'─' * 50}\n"
            f"  LLM Component:   {self.llm_component:.4f}\n"
            f"  Quant Component: {self.quant_component:.4f}\n"
            f"  Agreement:       {agreement}\n"
            f"  Detail:          {self.agreement_detail}\n"
            f"{'═' * 50}\n"
        )


def compute_decision(
    llm_signals: LLMSignals,
    tech_signals: TechnicalSignals,
    config: HybridConfig,
) -> QuantDecision:
    """Compute the final trading decision by combining LLM and quant signals.

    This is a pure mathematical function — no LLM calls, no randomness,
    fully deterministic and reproducible given the same inputs.

    Args:
        llm_signals: Numerical scores extracted from TradingAgents.
        tech_signals: Technical indicator scores from price data.
        config: Hybrid system configuration with weights and thresholds.

    Returns:
        QuantDecision with action, score, and explainability breakdown.
    """
    decision = QuantDecision()

    # ── Step 1: Compute LLM Component ──
    # Weighted sum of LLM-derived scores
    llm_weighted = (
        config.weight_sentiment * llm_signals.sentiment_score
        + config.weight_fundamental * llm_signals.fundamental_score
        + config.weight_news * llm_signals.news_score
        + config.weight_research_debate * llm_signals.research_debate_score
    )
    llm_total_weight = (
        config.weight_sentiment
        + config.weight_fundamental
        + config.weight_news
        + config.weight_research_debate
    )
    decision.llm_component = llm_weighted

    # ── Step 2: Compute Quant Component ──
    # Weighted sum of technical indicator scores
    quant_weighted = (
        config.weight_rsi * tech_signals.rsi_score
        + config.weight_ema_crossover * tech_signals.ema_crossover_score
        + config.weight_bollinger * tech_signals.bollinger_score
        + config.weight_volume * tech_signals.volume_score
    )
    quant_total_weight = (
        config.weight_rsi
        + config.weight_ema_crossover
        + config.weight_bollinger
        + config.weight_volume
    )
    decision.quant_component = quant_weighted

    # ── Step 3: Composite Score ──
    # Simply add both weighted components (weights already sum to 1.0)
    decision.composite_score = llm_weighted + quant_weighted

    # ── Step 4: Apply Decision Thresholds ──
    if decision.composite_score >= config.buy_threshold:
        decision.action = "BUY"
        # Confidence: how far above the buy threshold (normalized)
        decision.confidence = min(
            1.0,
            (decision.composite_score - config.buy_threshold)
            / (1.0 - config.buy_threshold),
        )
    elif decision.composite_score <= config.sell_threshold:
        decision.action = "SELL"
        # Confidence: how far below the sell threshold (normalized)
        decision.confidence = min(
            1.0,
            (config.sell_threshold - decision.composite_score)
            / config.sell_threshold,
        )
    else:
        decision.action = "HOLD"
        # Confidence is 0 at center (0.5), increases toward thresholds
        distance_to_center = abs(decision.composite_score - 0.5)
        max_distance = (config.buy_threshold - config.sell_threshold) / 2
        decision.confidence = distance_to_center / max_distance if max_distance > 0 else 0.0

    # ── Step 5: Agreement Analysis ──
    # Check if LLM and quant components agree on direction
    llm_direction = (
        "bullish"
        if (llm_weighted / llm_total_weight) > 0.55
        else "bearish" if (llm_weighted / llm_total_weight) < 0.45 else "neutral"
    )
    quant_direction = (
        "bullish"
        if (quant_weighted / quant_total_weight) > 0.55
        else "bearish" if (quant_weighted / quant_total_weight) < 0.45 else "neutral"
    )

    decision.llm_quant_agreement = (
        llm_direction == quant_direction or "neutral" in (llm_direction, quant_direction)
    )

    if decision.llm_quant_agreement:
        decision.agreement_detail = (
            f"LLM ({llm_direction}) and Quant ({quant_direction}) agree."
        )
    else:
        decision.agreement_detail = (
            f"CAUTION: LLM says {llm_direction} but Quant says {quant_direction}. "
            f"Composite score may be unreliable — consider reducing position size."
        )

    return decision
