"""
Quant Agent - Technical analysis signals
"""
from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel
from hybrid.config import HybridConfig
from hybrid.pipeline import HybridPipeline


class QuantAgent(BaseAgent):
    def __init__(self, bus: MessageBus, config: HybridConfig):
        super().__init__("quant_agent", bus)
        self.config = config
        self.pipeline = HybridPipeline(config=config, skip_llm=True)
        self.bus.subscribe(Channel.SIGNALS, self._on_request)
    
    async def handle_message(self, payload: dict):
        pass
    
    async def start(self):
        print("[QuantAgent] Started")
    
    async def _on_request(self, payload: dict):
        if payload.get("type") != "analyze_request":
            return
        if payload.get("target") != "quant_agent":
            return
        
        data = payload["data"]
        ticker = data["ticker"]
        
        result = self.pipeline.analyze_quant_only(ticker)
        
        self.bus.publish(Channel.SIGNALS, {
            "type": "quant_decision",
            "source": "quant_agent",
            "data": {
                "ticker": ticker,
                "action": result.quant_decision.action,
                "score": result.quant_decision.composite_score,
                "confidence": result.quant_decision.confidence,
                "tech_signals": {
                    "rsi": result.tech_signals.rsi,
                    "rsi_score": result.tech_signals.rsi_score,
                    "ema_fast": result.tech_signals.ema_fast,
                    "ema_slow": result.tech_signals.ema_slow,
                    "ema_crossover_score": result.tech_signals.ema_crossover_score,
                    "bollinger_upper": result.tech_signals.bollinger_upper,
                    "bollinger_lower": result.tech_signals.bollinger_lower,
                    "bollinger_mid": result.tech_signals.bollinger_mid,
                    "bollinger_score": result.tech_signals.bollinger_score,
                    "atr": result.tech_signals.atr,
                    "current_price": result.tech_signals.current_price,
                    "volume": result.tech_signals.current_volume,
                    "volume_sma": result.tech_signals.volume_sma_20,
                    "volume_score": result.tech_signals.volume_score,
                }
            }
        }, "quant_agent")
