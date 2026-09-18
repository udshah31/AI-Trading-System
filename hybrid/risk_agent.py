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
import asyncio


class RiskAgent(BaseAgent):
    def __init__(self, bus: MessageBus, config: HybridConfig):
        super().__init__("risk_agent", bus)
        self.config = config
        self.risk_manager = RiskManager(config)
        self.storage: StorageService = None
        self._monitor_task: asyncio.Task = None
        self.bus.subscribe(Channel.SIGNALS, self._on_quant_decision)
    
    async def start(self):
        print("[RiskAgent] Started")
        self._monitor_task = asyncio.create_task(self._monitor_loop())
    
    async def stop(self):
        if self._monitor_task:
            self._monitor_task.cancel()
        print("[RiskAgent] Stopped")

    def set_storage(self, storage: StorageService):
        self.storage = storage

    async def _monitor_loop(self):
        """Emit risk metrics every 10 seconds"""
        while True:
            try:
                await self._emit_risk_update()
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[RiskAgent] Monitor error: {e}")
                await asyncio.sleep(10)

    async def _emit_risk_update(self):
        """Emit risk metrics to dashboard"""
        if not self.storage:
            return
        try:
            async with self.storage.db.session() as session:
                from sqlalchemy import select, func
                from hybrid.storage import Trade, Position, EquityCurve
                from datetime import datetime, timezone, timedelta
                
                today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                
                # Daily P&L
                pnl_today = await session.execute(
                    select(func.sum(Trade.pnl)).where(Trade.timestamp >= today)
                )
                daily_pnl = float(pnl_today.scalar() or 0)
                
                # Total equity
                equity_result = await session.execute(
                    select(EquityCurve).order_by(EquityCurve.timestamp.desc()).limit(1)
                )
                equity_row = equity_result.scalars().first()
                total_equity = float(equity_row.equity) if equity_row else 0
                
                # Drawdown
                max_drawdown = 0.0
                if equity_row:
                    max_drawdown = float(equity_row.drawdown_pct or 0)
                
                # Open positions
                pos_result = await session.execute(
                    select(func.count(Position.id)).where(Position.is_open == True)
                )
                position_count = pos_result.scalar() or 0
                
                # Total exposure
                exposure_result = await session.execute(
                    select(func.sum(Position.volume * Position.current_price)).where(Position.is_open == True)
                )
                total_exposure = float(exposure_result.scalar() or 0)
                
                # Max position %
                max_pos_pct = self.config.max_position_pct * 100
                
                # Risk budget remaining (2% per trade * remaining trades)
                risk_per_trade = self.config.max_risk_per_trade_pct * total_equity
                max_daily_loss = self.config.max_drawdown_pct * total_equity
                risk_budget_remaining = max(0, max_daily_loss + daily_pnl)
                
                await self.bus.publish("signals", {
                    "type": "risk_update",
                    "data": {
                        "drawdown_pct": round(max_drawdown, 2),
                        "max_drawdown_pct": self.config.max_drawdown_pct * 100,
                        "daily_pnl": round(daily_pnl, 2),
                        "max_daily_loss": round(max_daily_loss, 2),
                        "margin_used_pct": round((total_exposure / total_equity * 100) if total_equity else 0, 1),
                        "max_position_pct": max_pos_pct,
                        "current_positions": position_count,
                        "total_exposure_usd": round(total_exposure, 2),
                        "risk_budget_remaining": round(risk_budget_remaining, 2),
                        "circuit_breaker": max_drawdown >= self.config.max_drawdown_pct * 100,
                        "total_equity": round(total_equity, 2),
                        "daily_pnl": round(daily_pnl, 2)
                    }
                }, "risk_agent")
        except Exception as e:
            print(f"[RiskAgent] Emit error: {e}")

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
                "approved": assessment.approved,
                "rejection_reason": assessment.rejection_reason,
                "position_size_usd": assessment.position_size_dollars,
                "shares": assessment.position_size_shares,
                "stop_loss": assessment.stop_loss_price,
                "risk_per_trade": assessment.risk_per_trade_dollars,
                "risk_reward": assessment.risk_reward_ratio
            }
        }, "risk_agent")
