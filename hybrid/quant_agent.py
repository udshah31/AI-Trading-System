"""
Quant Agent - Technical analysis signals
"""
import asyncio

from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel
from hybrid.config import HybridConfig
from hybrid.pipeline import HybridPipeline


# Quote currencies that Yahoo Finance prices crypto in as plain USD
_USD_QUOTES = {"USD", "USDT", "USDC"}


def market_data_symbol(ticker: str) -> str:
    """Exchange pair (BTC/USDT, XBT/USD) -> Yahoo Finance symbol (BTC-USD) for price data.

    Orders keep the exchange pair; only the price-data download uses this symbol.
    """
    if "/" not in ticker:
        return ticker
    base, quote = ticker.upper().split("/", 1)
    base = "BTC" if base == "XBT" else base
    return f"{base}-{'USD' if quote in _USD_QUOTES else quote}"


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
        
        # yfinance download + indicator math is blocking; keep the event loop free
        result = await asyncio.to_thread(self.pipeline.analyze_quant_only, market_data_symbol(ticker))
        
        decision, tech = result.quant_decision, result.tech_signals
        if decision is None or tech is None:  # analyze() always sets both; guard for the type checker
            print(f"[QuantAgent] No decision for {ticker}")
            return
        
        self.bus.publish(Channel.SIGNALS, {
            "type": "quant_decision",
            "source": "quant_agent",
            "data": {
                "ticker": ticker,
                "action": decision.action,
                "score": decision.composite_score,
                "confidence": decision.confidence,
                "tech_signals": {
                    "rsi": tech.rsi,
                    "rsi_score": tech.rsi_score,
                    "ema_fast": tech.ema_fast,
                    "ema_slow": tech.ema_slow,
                    "ema_crossover_score": tech.ema_crossover_score,
                    "bollinger_upper": tech.bollinger_upper,
                    "bollinger_lower": tech.bollinger_lower,
                    "bollinger_mid": tech.bollinger_mid,
                    "bollinger_score": tech.bollinger_score,
                    "atr": tech.atr,
                    "current_price": tech.current_price,
                    "volume": tech.current_volume,
                    "volume_sma": tech.volume_sma_20,
                    "volume_score": tech.volume_score,
                }
            }
        }, "quant_agent")
