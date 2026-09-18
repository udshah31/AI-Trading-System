"""
Hybrid Trading System
=====================
Combines LLM-based research (TradingAgents) with quantitative decision-making
and mathematical risk management. LLMs generate FEATURES; math makes DECISIONS.
"""

from hybrid.config import HybridConfig
from hybrid.pipeline import HybridPipeline

__all__ = ["HybridConfig", "HybridPipeline"]
