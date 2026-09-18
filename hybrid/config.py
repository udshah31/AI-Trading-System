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

# Load .env file from TradingAgents directory
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "TradingAgents", ".env"))



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
        """Load TradingAgents config from default_config.py and .env"""
        # Load from TradingAgents default_config.py
        try:
            from tradingagents.default_config import DEFAULT_CONFIG
            self.tradingagents_config = {**DEFAULT_CONFIG, **self.tradingagents_config}
        except ImportError:
            pass

        # Override with environment variables
        env_overrides = {
            "llm_provider": "TRADINGAGENTS_LLM_PROVIDER",
            "deep_think_llm": "TRADINGAGENTS_DEEP_THINK_LLM",
            "quick_think_llm": "TRADINGAGENTS_QUICK_THINK_LLM",
            "max_debate_rounds": "TRADINGAGENTS_MAX_DEBATE_ROUNDS",
            "max_risk_discuss_rounds": "TRADINGAGENTS_MAX_RISK_ROUNDS",
        }
        # Explicitly load GOOGLE_API_KEY from .env
        load_dotenv(os.path.join(os.path.dirname(__file__), "..", "TradingAgents", ".env"))

        for key, env_var in env_overrides.items():
            env_val = os.getenv(env_var)
            if env_val:
                self.tradingagents_config[key] = env_val

    def _load_learned_weights(self):
        """Loads machine learning optimized weights if they exist."""
        weights_file = Path(__file__).parent / "learned_weights.json"
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
