"""
Risk Agent - Mathematical risk management
"""
from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel
from hybrid.config import HybridConfig
from hybrid.risk_manager import RiskManager
from hybrid.quant_engine import QuantDecision
from hybrid.technical_indicators import TechnicalSignals
from hybrid.storage import StorageService
from typing import Optional


class RiskAgent(BaseAgent):
    def __init__(self, bus: MessageBus, config: HybridConfig):
        super().__init__("risk_agent", bus)
        self.config = config
        self.risk_manager = RiskManager(config)
        self.storage: Optional[StorageService] = None
        self.bus.subscribe(Channel.SIGNALS, self._on_quant_decision)
        self.bus.subscribe(Channel.SIGNALS, self._on_portfolio_update)
    
    async def start(self):
        print("[RiskAgent] Started")

    async def stop(self):
        print("[RiskAgent] Stopped")

    def set_storage(self, storage: StorageService):
        self.storage = storage

    async def _on_portfolio_update(self, payload: dict):
        """Keep the risk manager on the bot's real equity and peak, and refresh the dashboard's limits."""
        if payload.get("type") != "portfolio_update":
            return
        d = payload["data"]
        equity, peak = float(d["equity"]), float(d["peak"])
        self.risk_manager.sync(equity, peak)
        drawdown_pct = self.risk_manager.current_drawdown * 100
        max_daily_loss = self.config.max_drawdown_pct * equity
        daily_pnl = float(d.get("daily_pnl", 0.0))
        exposure = float(d.get("exposure", 0.0))
        self.bus.publish(Channel.SIGNALS, {
            "type": "risk_update",
            "data": {
                "drawdown_pct": round(drawdown_pct, 2),
                "max_drawdown_pct": self.config.max_drawdown_pct * 100,
                "daily_pnl": round(daily_pnl, 2),
                "max_daily_loss": round(max_daily_loss, 2),
                "margin_used_pct": round(exposure / equity * 100, 1) if equity else 0.0,
                "max_position_pct": self.config.max_position_pct * 100,
                "current_positions": len(d.get("positions", [])),
                "total_exposure_usd": round(exposure, 2),
                "risk_budget_remaining": round(max(0.0, max_daily_loss + daily_pnl), 2),
                "circuit_breaker": self.risk_manager.current_drawdown >= self.config.max_drawdown_pct,
                "total_equity": round(equity, 2),
            }
        }, "risk_agent")

    async def handle_message(self, payload: dict):
        pass
    
    async def _on_quant_decision(self, payload: dict):
        if payload.get("type") != "quant_decision":
            return
        if payload.get("source") != "quant_agent":
            return
        
        data = payload["data"]
        ticker = data["ticker"]
        
        decision = QuantDecision(
            action=data["action"],
            composite_score=data["score"],
            confidence=data["confidence"],
            llm_component=0,
            quant_component=0
        )
        
        tech = TechnicalSignals(
            rsi=data["tech_signals"].get("rsi", 50),
            rsi_score=data["tech_signals"].get("rsi_score", 0.5),
            ema_fast=data["tech_signals"].get("ema_fast", 0),
            ema_slow=data["tech_signals"].get("ema_slow", 0),
            ema_crossover_score=data["tech_signals"].get("ema_crossover_score", 0.5),
            bollinger_upper=data["tech_signals"].get("bollinger_upper", 0),
            bollinger_lower=data["tech_signals"].get("bollinger_lower", 0),
            bollinger_mid=data["tech_signals"].get("bollinger_mid", 0),
            bollinger_score=data["tech_signals"].get("bollinger_score", 0.5),
            atr=data["tech_signals"].get("atr", 0),
            current_price=data["tech_signals"].get("current_price", 0),
            current_volume=data["tech_signals"].get("volume", 0),
            volume_sma_20=data["tech_signals"].get("volume_sma", 0),
            volume_score=data["tech_signals"].get("volume_score", 0.5),
        )
        
        assessment = self.risk_manager.evaluate_trade(decision, tech, ticker)
        
        self.bus.publish(Channel.SIGNALS, {
            "type": "risk_assessment",
            "source": "risk_agent",
            "data": {
                "ticker": ticker,
                "action": decision.action,
                "price": tech.current_price,
                "approved": assessment.approved,
                "rejection_reason": assessment.rejection_reason,
                "position_size_usd": assessment.position_size_dollars,
                "shares": assessment.position_size_shares,
                "stop_loss": assessment.stop_loss_price,
                "risk_per_trade": assessment.risk_per_trade_dollars,
                "risk_reward": assessment.risk_reward_ratio
            }
        }, "risk_agent")
