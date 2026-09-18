"""
Mathematical Risk Manager
==========================
Enforces hard risk limits using ONLY math. No LLM opinions allowed.

This module is the last line of defense before a trade is executed.
It can BLOCK any trade that violates risk constraints, regardless of
what the LLM agents or the quant engine recommended.

Risk rules are non-negotiable and deterministic.
"""

from dataclasses import dataclass
from typing import Optional

from hybrid.config import HybridConfig
from hybrid.quant_engine import QuantDecision
from hybrid.technical_indicators import TechnicalSignals


@dataclass
class RiskAssessment:
    """Output of the risk management check."""

    # Whether the trade is approved
    approved: bool = False

    # If not approved, why
    rejection_reason: str = ""

    # Position sizing
    position_size_dollars: float = 0.0
    position_size_shares: float = 0.0
    position_pct_of_portfolio: float = 0.0

    # Risk metrics
    risk_per_trade_dollars: float = 0.0
    risk_per_trade_pct: float = 0.0
    stop_loss_price: float = 0.0
    risk_reward_ratio: float = 0.0

    # Portfolio state
    current_portfolio_value: float = 0.0
    current_drawdown_pct: float = 0.0
    remaining_risk_budget: float = 0.0

    def summary(self) -> str:
        """Human-readable risk assessment."""
        status = "✅ APPROVED" if self.approved else "🚫 BLOCKED"
        lines = [
            f"{'═' * 50}",
            f"  RISK ASSESSMENT: {status}",
            f"{'═' * 50}",
        ]

        if not self.approved:
            lines.append(f"  Reason: {self.rejection_reason}")
        else:
            lines.extend([
                f"  Position Size:    ${self.position_size_dollars:,.2f} "
                f"({self.position_pct_of_portfolio:.1%} of portfolio)",
                f"  Shares:           {self.position_size_shares:.2f}",
                f"  Stop Loss:        ${self.stop_loss_price:.2f}",
                f"  Risk per Trade:   ${self.risk_per_trade_dollars:,.2f} "
                f"({self.risk_per_trade_pct:.2%})",
                f"  Risk/Reward:      {self.risk_reward_ratio:.2f}",
            ])

        lines.extend([
            f"{'─' * 50}",
            f"  Portfolio Value:  ${self.current_portfolio_value:,.2f}",
            f"  Current Drawdown: {self.current_drawdown_pct:.2%}",
            f"  Risk Budget Left: ${self.remaining_risk_budget:,.2f}",
            f"{'═' * 50}",
        ])
        return "\n".join(lines)


class RiskManager:
    """Mathematical risk management engine.

    Tracks portfolio state and enforces position sizing rules,
    drawdown limits, and risk-per-trade constraints.
    """

    def __init__(self, config: HybridConfig):
        self.config = config
        self.initial_capital = config.initial_capital
        self.current_capital = config.initial_capital
        self.peak_capital = config.initial_capital

        # Track open positions: {ticker: {"shares": N, "entry_price": P}}
        self.positions: dict[str, dict] = {}

        # Trade log for auditing
        self.trade_log: list[dict] = []

    @property
    def current_drawdown(self) -> float:
        """Current drawdown from peak as a fraction (0.0 = no drawdown)."""
        if self.peak_capital <= 0:
            return 0.0
        return max(0.0, (self.peak_capital - self.current_capital) / self.peak_capital)

    @property
    def portfolio_value(self) -> float:
        """Total portfolio value (cash + positions)."""
        return self.current_capital

    def update_capital(self, new_value: float) -> None:
        """Update the portfolio value (call after each trade/day)."""
        self.current_capital = new_value
        if new_value > self.peak_capital:
            self.peak_capital = new_value

    def evaluate_trade(
        self,
        decision: QuantDecision,
        tech_signals: TechnicalSignals,
        ticker: str,
    ) -> RiskAssessment:
        """Evaluate whether a trade should be executed and size it.

        This is the FINAL GATE before execution. It can approve or
        block any trade based on mathematical risk constraints.

        Args:
            decision: Output from the quant engine.
            tech_signals: Technical indicators (needed for ATR-based stops).
            ticker: The ticker being traded.

        Returns:
            RiskAssessment with approval status and position sizing.
        """
        assessment = RiskAssessment()
        assessment.current_portfolio_value = self.current_capital
        assessment.current_drawdown_pct = self.current_drawdown

        # ── Check 1: Is there even a trade to evaluate? ──
        if decision.action == "HOLD":
            assessment.approved = False
            assessment.rejection_reason = "Action is HOLD — no trade needed."
            return assessment

        # ── Check 2: Drawdown Circuit Breaker ──
        if self.current_drawdown >= self.config.max_drawdown_pct:
            assessment.approved = False
            assessment.rejection_reason = (
                f"CIRCUIT BREAKER: Portfolio drawdown "
                f"({self.current_drawdown:.1%}) exceeds max "
                f"({self.config.max_drawdown_pct:.1%}). "
                f"All trading halted until recovery."
            )
            return assessment

        # ── Check 3: Do we have a valid price? ──
        if tech_signals.current_price <= 0:
            assessment.approved = False
            assessment.rejection_reason = "Invalid price data — cannot size position."
            return assessment

        # ── Check 4: Do we have ATR for stop-loss calculation? ──
        atr = tech_signals.atr
        if atr <= 0:
            # Fallback: use 3% of price as a rough ATR estimate
            atr = tech_signals.current_price * 0.03

        # ── Compute Stop Loss ──
        # Stop loss is placed 2x ATR away from entry
        if decision.action == "BUY":
            assessment.stop_loss_price = tech_signals.current_price - (2.0 * atr)
        else:  # SELL (short)
            assessment.stop_loss_price = tech_signals.current_price + (2.0 * atr)

        risk_per_share = abs(tech_signals.current_price - assessment.stop_loss_price)

        if risk_per_share <= 0:
            assessment.approved = False
            assessment.rejection_reason = "Risk per share is zero — stop loss too tight."
            return assessment

        # ── Compute Position Size (Risk-Based) ──
        # Max dollar risk = max_risk_per_trade_pct * portfolio
        max_risk_dollars = self.current_capital * self.config.max_risk_per_trade_pct
        assessment.remaining_risk_budget = max_risk_dollars

        # Shares = max_risk_dollars / risk_per_share
        shares = max_risk_dollars / risk_per_share
        position_dollars = shares * tech_signals.current_price

        # ── Check 5: Max Position Size ──
        max_position_dollars = self.current_capital * self.config.max_position_pct
        if position_dollars > max_position_dollars:
            # Cap at max position size
            position_dollars = max_position_dollars
            shares = position_dollars / tech_signals.current_price

        # ── Check 6: Can we afford it? ──
        if position_dollars > self.current_capital:
            assessment.approved = False
            assessment.rejection_reason = (
                f"Insufficient capital: need ${position_dollars:,.2f} "
                f"but have ${self.current_capital:,.2f}."
            )
            return assessment

        # ── Check 7: Confidence-Based Scaling ──
        # Scale position size by confidence (low confidence = smaller position)
        confidence_scale = max(0.3, decision.confidence)  # Minimum 30% of full size
        shares *= confidence_scale
        position_dollars *= confidence_scale

        # ── Check 8: LLM/Quant Disagreement Penalty ──
        # If LLM and quant disagree, halve the position size
        if not decision.llm_quant_agreement:
            shares *= 0.5
            position_dollars *= 0.5

        # ── Final Risk Calculations ──
        assessment.position_size_dollars = round(position_dollars, 2)
        assessment.position_size_shares = round(shares, 4)
        assessment.position_pct_of_portfolio = position_dollars / self.current_capital
        assessment.risk_per_trade_dollars = shares * risk_per_share
        assessment.risk_per_trade_pct = (
            assessment.risk_per_trade_dollars / self.current_capital
        )

        # Risk/Reward ratio (using 3x ATR as target)
        reward_per_share = 3.0 * atr
        assessment.risk_reward_ratio = (
            reward_per_share / risk_per_share if risk_per_share > 0 else 0.0
        )

        # ── Approve ──
        assessment.approved = True
        return assessment

    def record_trade(
        self,
        ticker: str,
        action: str,
        shares: float,
        price: float,
        stop_loss: float,
    ) -> None:
        """Record a trade in the log and update positions."""
        from datetime import datetime

        trade = {
            "timestamp": datetime.now().isoformat(),
            "ticker": ticker,
            "action": action,
            "shares": shares,
            "price": price,
            "stop_loss": stop_loss,
            "portfolio_value": self.current_capital,
        }
        self.trade_log.append(trade)

        if action == "BUY":
            self.positions[ticker] = {
                "shares": shares,
                "entry_price": price,
                "stop_loss": stop_loss,
            }
        elif action == "SELL" and ticker in self.positions:
            # Close position and update capital
            entry = self.positions.pop(ticker)
            pnl = (price - entry["entry_price"]) * entry["shares"]
            self.update_capital(self.current_capital + pnl)
