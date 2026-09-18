#!/usr/bin/env python3
"""
Hybrid Trading System — Main Entry Point
==========================================
Run the complete hybrid LLM + Quant trading pipeline.

Usage:
    # Quant-only mode (no LLM, no API costs — great for testing):
    python run.py --ticker AAPL --quant-only

    # Full hybrid mode (requires ANTHROPIC_API_KEY):
    python run.py --ticker AAPL

    # Full hybrid with paper trading execution:
    python run.py --ticker AAPL --execute

    # Crypto analysis:
    python run.py --ticker BTC-USD --asset crypto
"""

import argparse
import sys
from pathlib import Path

# Ensure project modules are importable
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "TradingAgents"))

from hybrid.config import HybridConfig
from hybrid.pipeline import HybridPipeline


def main():
    parser = argparse.ArgumentParser(
        description="Hybrid LLM + Quant Trading System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py --ticker AAPL --quant-only       # Quick technical screen
  python run.py --ticker NVDA                     # Full hybrid analysis
  python run.py --ticker BTC-USD --asset crypto   # Crypto analysis
  python run.py --ticker TSLA --execute           # Analyze + paper trade
        """,
    )

    parser.add_argument(
        "--ticker", "-t",
        required=True,
        help="Ticker symbol (e.g., AAPL, BTC-USD, NVDA)",
    )
    parser.add_argument(
        "--date", "-d",
        default=None,
        help="Analysis date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--asset", "-a",
        choices=["stock", "crypto"],
        default="stock",
        help="Asset type (default: stock)",
    )
    parser.add_argument(
        "--quant-only", "-q",
        action="store_true",
        help="Run only quant analysis (no LLM, no API costs)",
    )
    parser.add_argument(
        "--execute", "-x",
        action="store_true",
        help="Execute the trade on Alpaca paper trading",
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=100_000.0,
        help="Starting paper trading capital (default: $100,000)",
    )
    parser.add_argument(
        "--llm-provider",
        default="anthropic",
        help="LLM provider for TradingAgents (default: anthropic)",
    )
    parser.add_argument(
        "--llm-model",
        default="claude-sonnet-4-20250514",
        help="LLM model for TradingAgents",
    )

    args = parser.parse_args()

    # ── Build Configuration ──
    config = HybridConfig(
        initial_capital=args.capital,
        tradingagents_config={
            "llm_provider": args.llm_provider,
            "deep_think_llm": args.llm_model,
            "quick_think_llm": args.llm_model,
        },
    )

    # ── Initialize Pipeline ──
    pipeline = HybridPipeline(
        config=config,
        dry_run=True,  # Always dry run unless Alpaca is configured
        skip_llm=args.quant_only,
    )

    # ── Run Analysis ──
    if args.quant_only:
        print("\n🔬 Running QUANT-ONLY mode (no LLM, zero API cost)\n")
        result = pipeline.analyze_quant_only(args.ticker, args.date)
    else:
        print("\n🧬 Running FULL HYBRID mode (LLM + Quant)\n")
        result = pipeline.analyze(
            ticker=args.ticker,
            trade_date=args.date,
            asset_type=args.asset,
            execute=args.execute,
        )

    # ── Print Final Summary ──
    if result.quant_decision:
        print(f"\n{'🎯' * 3} FINAL VERDICT: {result.quant_decision.action} "
              f"(confidence: {result.quant_decision.confidence:.1%}) {'🎯' * 3}")


if __name__ == "__main__":
    main()
