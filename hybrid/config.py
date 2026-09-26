"""
Hybrid System Configuration
==========================
Central configuration for the hybrid LLM + Quant trading system.
Keeps all tunable parameters in one place.
"""

import os
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LEARNED_WEIGHTS_PATH = Path(__file__).parent / "learned_weights.json"


def load_env_files(root: Path = PROJECT_ROOT) -> None:
    """Load <root>/.env, then <root>/TradingAgents/.env for anything still unset.

    Variables already in the real environment always win (override=False).
    """
    load_dotenv(root / ".env", override=False)
    load_dotenv(root / "TradingAgents" / ".env", override=False)


# Before anything imports tradingagents: its DEFAULT_CONFIG reads TRADINGAGENTS_* at import.
load_env_files()


def live_trading_enabled() -> bool:
    """Real orders (Kraken, DEX swaps) require an explicit opt-in: LIVE_TRADING=true."""
    return os.getenv("LIVE_TRADING", "").strip().lower() == "true"


@dataclass
class HybridConfig:
    """Configuration for the hybrid LLM + Quant trading system."""

    # ── LLM Agent Weights (how much to trust each LLM signal) ──
    weight_sentiment: float = 0.25
    weight_fundamental: float = 0.20
    weight_news: float = 0.15
    weight_research_debate: float = 0.15

    # ── Technical Indicator Weights ──
    weight_rsi: float = 0.10
    weight_ema_crossover: float = 0.08
    weight_bollinger: float = 0.04
    weight_volume: float = 0.03

    # ── Decision Thresholds ──
    buy_threshold: float = 0.65
    sell_threshold: float = 0.35

    # ── Risk Management (Mathematical — NO LLM) ──
    max_position_pct: float = 0.20
    max_risk_per_trade_pct: float = 0.02
    max_drawdown_pct: float = 0.15
    initial_capital: float = 100_000.0

    # ── Technical Indicator Parameters ──
    rsi_period: int = 14
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0
    ema_fast: int = 12
    ema_slow: int = 26
    bollinger_period: int = 20
    bollinger_std: float = 2.0
    atr_period: int = 14

    # ── Alpaca Paper Trading ──
    alpaca_api_key: Optional[str] = None
    alpaca_secret_key: Optional[str] = None
    alpaca_base_url: str = "https://paper-api.alpaca.markets"
    alpaca_paper_trade: bool = True

    # ── TradingAgents Integration ──
    tradingagents_config: dict = field(default_factory=dict)

    # ── Data ──
    lookback_days: int = 365
    data_cache_dir: str = "data_cache"

    def __post_init__(self):
        self.validate()
        self._load_tradingagents_config()
        self._load_learned_weights()

    def _load_tradingagents_config(self):
        """Explicit settings (e.g. CLI flags) on top of TradingAgents' defaults.

        DEFAULT_CONFIG already has the TRADINGAGENTS_* env vars applied and
        type-coerced by TradingAgents itself, so they aren't re-read here.
        """
        try:
            from tradingagents.default_config import DEFAULT_CONFIG
        except ImportError:
            return
        self.tradingagents_config = {**DEFAULT_CONFIG, **self.tradingagents_config}

    def _load_learned_weights(self):
        """Loads machine learning optimized weights if they exist."""
        weights_file = LEARNED_WEIGHTS_PATH
        if not weights_file.exists():
            return
        try:
            with open(weights_file, "r") as f:
                learned = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"[HybridConfig] Warning: Could not load ML weights: {e}")
            return

        # ponytail: direct map, no generalized weight registry until >4 weights needed
        map_ = {"tech_weight_rsi": "weight_rsi", "tech_weight_ema": "weight_ema_crossover",
                "tech_weight_bollinger": "weight_bollinger", "tech_weight_volume": "weight_volume"}
        learned_tech = {map_[k]: float(v) for k, v in learned.items() if k in map_}
        if not learned_tech:
            return
        total = sum(learned_tech.values())
        # learned file is relative weights summing to 1.0, scale to tech budget 0.25
        scale = 0.25 / total if total > 0 else 0
        candidate = {attr: val * scale for attr, val in learned_tech.items()}
        # validate-before-mutate: mutate, validate, rollback on ValueError (let it propagate)
        original = {attr: getattr(self, attr) for attr in candidate}
        for attr, val in candidate.items():
            setattr(self, attr, val)
        try:
            self.validate()
        except ValueError:
            for attr, val in original.items():
                setattr(self, attr, val)
            raise

    def validate(self) -> None:
        """Validate configuration constraints."""
        total = (
            self.weight_sentiment
            + self.weight_fundamental
            + self.weight_news
            + self.weight_research_debate
            + self.weight_rsi
            + self.weight_ema_crossover
            + self.weight_bollinger
            + self.weight_volume
        )
        if abs(total - 1.0) > 0.01:
            raise ValueError(
                f"Signal weights must sum to 1.0, got {total:.4f}. Adjust weights in HybridConfig."
            )

        if not self.alpaca_paper_trade:
            raise ValueError(
                "DANGER: alpaca_paper_trade must be True. This system is for paper trading ONLY."
            )

        if self.buy_threshold <= self.sell_threshold:
            raise ValueError(
                f"buy_threshold ({self.buy_threshold}) must be > "
                f"sell_threshold ({self.sell_threshold})"
            )
