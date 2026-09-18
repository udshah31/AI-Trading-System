"""
Hybrid Trading Pipeline
========================
The master orchestrator that ties everything together:

1. Runs TradingAgents (LLM research) → qualitative analysis
2. Extracts numerical signals from LLM output
3. Computes technical indicators from price data
4. Feeds both into the Quant Engine for a mathematical decision
5. Validates through Risk Management
6. Executes via Alpaca (paper trading)

This is the single entry point for the entire hybrid system.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Add TradingAgents to the path
_ta_root = Path(__file__).parent.parent / "TradingAgents"
sys.path.insert(0, str(_ta_root))

# Explicitly load the .env from TradingAgents directory so API keys
# are available regardless of which directory we run from.
from dotenv import load_dotenv
load_dotenv(_ta_root / ".env", override=False)

from hybrid.config import HybridConfig
from hybrid.execution import AlpacaExecutor, ExecutionResult
from hybrid.quant_engine import QuantDecision, compute_decision
from hybrid.risk_manager import RiskAssessment, RiskManager
from hybrid.signal_extractor import LLMSignals, extract_signals
from hybrid.technical_indicators import TechnicalSignals, compute_technical_signals


class HybridPipeline:
    """The complete hybrid LLM + Quant trading pipeline.

    Usage:
        config = HybridConfig()
        pipeline = HybridPipeline(config)
        result = pipeline.analyze("AAPL", "2024-12-15")
        print(result.full_report())
    """

    def __init__(
        self,
        config: Optional[HybridConfig] = None,
        dry_run: bool = True,
        skip_llm: bool = False,
    ):
        """Initialize the hybrid pipeline.

        Args:
            config: System configuration. Uses defaults if None.
            dry_run: If True, don't submit real orders to Alpaca.
            skip_llm: If True, skip the TradingAgents LLM analysis
                      (useful for testing the quant layer independently).
        """
        self.config = config or HybridConfig()
        self.config.validate()

        self.risk_manager = RiskManager(self.config)
        self.executor = AlpacaExecutor(self.config, dry_run=dry_run)
        self.skip_llm = skip_llm

        # Results storage
        self.results_dir = Path(__file__).parent.parent / "results"
        self.results_dir.mkdir(exist_ok=True)

    def analyze(
        self,
        ticker: str,
        trade_date: Optional[str] = None,
        asset_type: str = "stock",
        execute: bool = False,
    ) -> "PipelineResult":
        """Run the complete hybrid analysis pipeline.

        Args:
            ticker: Ticker symbol (e.g., "AAPL", "BTC-USD").
            trade_date: Analysis date (YYYY-MM-DD). Defaults to today.
            asset_type: "stock" or "crypto".
            execute: If True, submit the trade to Alpaca after approval.

        Returns:
            PipelineResult with all intermediate and final outputs.
        """
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y-%m-%d")

        result = PipelineResult(ticker=ticker, trade_date=trade_date)
        print(f"\n{'🚀' * 5} HYBRID TRADING PIPELINE {'🚀' * 5}")
        print(f"Ticker: {ticker} | Date: {trade_date} | Asset: {asset_type}")
        print(f"{'─' * 60}")

        # ── Step 1: LLM Analysis (Track A) ──
        print("\n📊 Step 1: Running LLM Research Agents...")
        if self.skip_llm:
            print("  [SKIPPED] Using default neutral signals.")
            result.llm_signals = LLMSignals()
            result.llm_raw_state = {}
        else:
            result.llm_signals, result.llm_raw_state = self._run_llm_analysis(
                ticker, trade_date, asset_type
            )
        print(result.llm_signals.summary())

        # ── Step 2: Technical Indicators (Track B) ──
        print("\n📈 Step 2: Computing Technical Indicators...")
        result.tech_signals = compute_technical_signals(
            ticker=ticker,
            trade_date=trade_date,
            rsi_period=self.config.rsi_period,
            ema_fast=self.config.ema_fast,
            ema_slow=self.config.ema_slow,
            bollinger_period=self.config.bollinger_period,
            bollinger_std=self.config.bollinger_std,
            atr_period=self.config.atr_period,
            lookback_days=self.config.lookback_days,
        )
        print(result.tech_signals.summary())

        # ── Step 3: Quant Engine (Hybrid Decision) ──
        print("\n🧮 Step 3: Quant Engine Decision...")
        result.quant_decision = compute_decision(
            result.llm_signals, result.tech_signals, self.config
        )
        print(result.quant_decision.summary())

        # ── Step 4: Risk Management ──
        print("\n🛡️ Step 4: Risk Assessment...")
        result.risk_assessment = self.risk_manager.evaluate_trade(
            result.quant_decision, result.tech_signals, ticker
        )
        print(result.risk_assessment.summary())

        # ── Step 5: Execution ──
        if execute and result.risk_assessment.approved:
            print("\n⚡ Step 5: Executing Trade...")
            result.execution_result = self.executor.execute(
                ticker=ticker,
                action=result.quant_decision.action,
                assessment=result.risk_assessment,
                current_price=result.tech_signals.current_price,
            )
            if result.execution_result.success:
                self.risk_manager.record_trade(
                    ticker=ticker,
                    action=result.quant_decision.action,
                    shares=result.risk_assessment.position_size_shares,
                    price=result.tech_signals.current_price,
                    stop_loss=result.risk_assessment.stop_loss_price,
                )
            print(result.execution_result.summary())
        elif not result.risk_assessment.approved:
            print("\n⏸️ Step 5: Trade BLOCKED by risk manager.")
        else:
            print("\n⏸️ Step 5: Execution skipped (execute=False).")

        # ── Save Report ──
        self._save_report(result)

        print(f"\n{'✅' * 5} PIPELINE COMPLETE {'✅' * 5}\n")
        return result

    def _run_llm_analysis(
        self, ticker: str, trade_date: str, asset_type: str
    ) -> tuple[LLMSignals, dict]:
        """Run TradingAgents and extract signals.

        Returns:
            Tuple of (LLMSignals, raw_state_dict).
        """
        try:
            from tradingagents.graph.trading_graph import TradingAgentsGraph
            from tradingagents.default_config import DEFAULT_CONFIG

            # Merge user config overrides
            config = {**DEFAULT_CONFIG, **self.config.tradingagents_config}

            ta = TradingAgentsGraph(debug=False, config=config)
            final_state, signal = ta.propagate(
                ticker, trade_date, asset_type=asset_type
            )

            llm_signals = extract_signals(final_state)
            return llm_signals, final_state

        except ImportError as e:
            print(f"  [WARNING] TradingAgents import failed: {e}")
            print("  Using default neutral signals.")
            return LLMSignals(), {}
        except Exception as e:
            print(f"  [ERROR] LLM analysis failed: {e}")
            print("  Using default neutral signals.")
            return LLMSignals(), {}

    def _save_report(self, result: "PipelineResult") -> None:
        """Save the full analysis report to disk."""
        ticker_dir = self.results_dir / result.ticker
        ticker_dir.mkdir(exist_ok=True)

        report_file = ticker_dir / f"hybrid_report_{result.trade_date}.md"
        report_file.write_text(result.full_report())
        print(f"📄 Report saved: {report_file}")

    def analyze_quant_only(
        self,
        ticker: str,
        trade_date: Optional[str] = None,
    ) -> "PipelineResult":
        """Run ONLY the quant analysis (no LLM, no API costs).

        Useful for quick technical screening of many tickers.
        """
        original_skip = self.skip_llm
        self.skip_llm = True
        result = self.analyze(ticker, trade_date, execute=False)
        self.skip_llm = original_skip
        return result


class PipelineResult:
    """Container for all pipeline outputs."""

    def __init__(self, ticker: str, trade_date: str):
        self.ticker = ticker
        self.trade_date = trade_date
        self.llm_signals: Optional[LLMSignals] = None
        self.tech_signals: Optional[TechnicalSignals] = None
        self.quant_decision: Optional[QuantDecision] = None
        self.risk_assessment: Optional[RiskAssessment] = None
        self.execution_result: Optional[ExecutionResult] = None
        self.llm_raw_state: dict = {}

    def full_report(self) -> str:
        """Generate a complete markdown report."""
        lines = [
            f"# Hybrid Analysis Report: {self.ticker}",
            f"**Date:** {self.trade_date}",
            f"**Generated:** {datetime.now().isoformat()}",
            "",
            "---",
            "",
            "## 1. LLM Agent Signals (Track A)",
            self.llm_signals.summary() if self.llm_signals else "N/A",
            "",
            "## 2. Technical Indicators (Track B)",
            self.tech_signals.summary() if self.tech_signals else "N/A",
            "",
            "## 3. Quant Engine Decision",
            self.quant_decision.summary() if self.quant_decision else "N/A",
            "",
            "## 4. Risk Assessment",
            self.risk_assessment.summary() if self.risk_assessment else "N/A",
            "",
        ]

        if self.execution_result:
            lines.extend([
                "## 5. Execution",
                self.execution_result.summary(),
                "",
            ])

        return "\n".join(lines)
