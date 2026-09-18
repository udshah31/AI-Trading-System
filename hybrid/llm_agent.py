"""
LLM Agent - Async wrapper for TradingAgents
Integrates with message bus for signal generation
"""
import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

TA_ROOT = Path(__file__).parent.parent / "TradingAgents"
sys.path.insert(0, str(TA_ROOT))

from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel
from hybrid.config import HybridConfig
from hybrid.signal_extractor import extract_signals, LLMSignals

from dotenv import load_dotenv
load_dotenv(TA_ROOT / ".env", override=False)


@dataclass
class LLMAnalysisRequest:
    ticker: str
    trade_date: str
    asset_type: str = "stock"
    selected_analysts: List[str] = None
    max_debate_rounds: int = 1
    max_risk_rounds: int = 1


@dataclass
class LLMAnalysisResult:
    ticker: str
    trade_date: str
    llm_signals: LLMSignals
    raw_state: Dict
    success: bool
    error: Optional[str] = None
    duration_seconds: float = 0


class LLMAnalystAgent(BaseAgent):
    def __init__(
        self,
        bus: MessageBus,
        config: HybridConfig,
        max_concurrent: int = 2,
        timeout_seconds: int = 300
    ):
        super().__init__("llm_analyst", bus)
        self.config = config
        self.max_concurrent = max_concurrent
        self.timeout_seconds = timeout_seconds
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        self._ta_graph = None
        self._ta_config = None
        self.active_analyses: Dict[str, asyncio.Task] = {}
        self._pending_futures: Dict[str, asyncio.Future] = {}
        
        self.bus.subscribe(Channel.SIGNALS, self._on_analysis_request)
    
    def _get_ta_config(self) -> Dict:
        if self._ta_config is None:
            from tradingagents.default_config import DEFAULT_CONFIG
            self._ta_config = {
                **DEFAULT_CONFIG,
                **self.config.tradingagents_config,
                "llm_provider": self.config.tradingagents_config.get("llm_provider", "google"),
                "deep_think_llm": self.config.tradingagents_config.get("deep_think_llm", "gemini-2.5-flash"),
                "quick_think_llm": self.config.tradingagents_config.get("quick_think_llm", "gemini-2.5-flash"),
            }
        return self._ta_config
    
    def _get_ta_graph(self):
        if self._ta_graph is None:
            from tradingagents.graph.trading_graph import TradingAgentsGraph
            self._ta_graph = TradingAgentsGraph(debug=False, config=self._get_ta_config())
        return self._ta_graph
    
    async def handle_message(self, payload: dict):
        pass
    
    async def start(self):
        print(f"[LLMAnalyst] Started (max_concurrent={self.max_concurrent})")
    
    async def stop(self):
        for task in self.active_analyses.values():
            task.cancel()
        await asyncio.gather(*self.active_analyses.values(), return_exceptions=True)
        self.active_analyses.clear()
        print("[LLMAnalyst] Stopped")
    
    async def _on_analysis_request(self, payload: dict):
        if payload.get("type") != "llm_analysis_request":
            return
        if payload.get("target") != "llm_analyst":
            return
        
        request_id = payload.get("request_id", str(datetime.utcnow().timestamp()))
        data = payload["data"]
        request = LLMAnalysisRequest(**data)
        
        task = asyncio.create_task(self._run_analysis(request_id, request))
        self.active_analyses[request_id] = task
        task.add_done_callback(lambda t: self.active_analyses.pop(request_id, None))
    
    async def _run_analysis(self, request_id: str, request: LLMAnalysisRequest) -> LLMAnalysisResult:
        start_time = datetime.utcnow()
        
        try:
            async with self.semaphore:
                loop = asyncio.get_event_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(None, self._run_ta_sync, request),
                    timeout=self.timeout_seconds
                )
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            result.duration_seconds = duration
            await self._publish_result(request_id, result)
            return result
            
        except asyncio.TimeoutError:
            error = f"Analysis timeout after {self.timeout_seconds}s"
            result = LLMAnalysisResult(
                ticker=request.ticker,
                trade_date=request.trade_date,
                llm_signals=LLMSignals(),
                raw_state={},
                success=False,
                error=error,
                duration_seconds=(datetime.utcnow() - start_time).total_seconds()
            )
            await self._publish_result(request_id, result)
            return result
            
        except Exception as e:
            error = f"Analysis failed: {str(e)}"
            result = LLMAnalysisResult(
                ticker=request.ticker,
                trade_date=request.trade_date,
                llm_signals=LLMSignals(),
                raw_state={},
                success=False,
                error=error,
                duration_seconds=(datetime.utcnow() - start_time).total_seconds()
            )
            await self._publish_result(request_id, result)
            return result
    
    def _run_ta_sync(self, request: LLMAnalysisRequest) -> LLMAnalysisResult:
        try:
            ta = self._get_ta_graph()
            
            if request.selected_analysts:
                ta.selected_analysts = tuple(request.selected_analysts)
            
            final_state, signal = ta.propagate(
                request.ticker,
                request.trade_date,
                asset_type=request.asset_type
            )
            
            llm_signals = extract_signals(final_state)
            
            return LLMAnalysisResult(
                ticker=request.ticker,
                trade_date=request.trade_date,
                llm_signals=llm_signals,
                raw_state=final_state,
                success=True
            )
            
        except Exception as e:
            return LLMAnalysisResult(
                ticker=request.ticker,
                trade_date=request.trade_date,
                llm_signals=LLMSignals(),
                raw_state={},
                success=False,
                error=str(e)
            )
    
    async def _publish_result(self, request_id: str, result: LLMAnalysisResult):
        self.bus.publish(Channel.SIGNALS, {
            "type": "llm_analysis_result",
            "source": self.name,
            "request_id": request_id,
            "data": {
                "ticker": result.ticker,
                "trade_date": result.trade_date,
                "success": result.success,
                "error": result.error,
                "duration_seconds": result.duration_seconds,
                "llm_signals": {
                    "sentiment_score": result.llm_signals.sentiment_score,
                    "fundamental_score": result.llm_signals.fundamental_score,
                    "news_score": result.llm_signals.news_score,
                    "research_debate_score": result.llm_signals.research_debate_score,
                    "trader_action_score": result.llm_signals.trader_action_score,
                    "portfolio_decision_score": result.llm_signals.portfolio_decision_score,
                    "sentiment_band": result.llm_signals.sentiment_band,
                    "sentiment_confidence": result.llm_signals.sentiment_confidence,
                    "portfolio_rating": result.llm_signals.portfolio_rating,
                    "trader_action": result.llm_signals.trader_action,
                } if result.success else None
            }
        }, self.name)
    
    async def analyze(self, ticker: str, trade_date: str = None, asset_type: str = "stock") -> LLMAnalysisResult:
        request_id = f"{ticker}_{trade_date or datetime.utcnow().strftime('%Y%m%d')}"
        
        future = asyncio.get_event_loop().create_future()
        self._pending_futures[request_id] = future
        
        async def on_result(payload):
            if payload.get("type") == "llm_analysis_result" and payload.get("request_id") == request_id:
                future.set_result(payload["data"])
        
        self.bus.subscribe(Channel.SIGNALS, on_result)
        
        self.bus.publish(Channel.SIGNALS, {
            "type": "llm_analysis_request",
            "target": "llm_analyst",
            "request_id": request_id,
            "data": {
                "ticker": ticker,
                "trade_date": trade_date or datetime.utcnow().strftime("%Y-%m-%d"),
                "asset_type": asset_type
            }
        }, self.name)
        
        try:
            result = await asyncio.wait_for(future, timeout=self.timeout_seconds + 10)
            return LLMAnalysisResult(**result)
        finally:
            self._pending_futures.pop(request_id, None)


class LLMOrchestratorAgent(BaseAgent):
    def __init__(self, bus: MessageBus, config: HybridConfig):
        super().__init__("llm_orchestrator", bus)
        self.config = config
        self.analysts: Dict[str, LLMAnalystAgent] = {}
        self.bus.subscribe(Channel.SIGNALS, self._on_orchestrate_request)
    
    async def handle_message(self, payload: dict):
        pass
    
    def get_analyst(self, name: str = "default") -> LLMAnalystAgent:
        if name not in self.analysts:
            self.analysts[name] = LLMAnalystAgent(self.bus, self.config)
        return self.analysts[name]
    
    async def start(self):
        for analyst in self.analysts.values():
            await analyst.start()
        print("[LLMOrchestrator] Started")
    
    async def stop(self):
        for analyst in self.analysts.values():
            await analyst.stop()
        print("[LLMOrchestrator] Stopped")
    
    async def _on_orchestrate_request(self, payload: dict):
        if payload.get("type") != "orchestrate_analysis":
            return
        
        data = payload["data"]
        tickers = data.get("tickers", [])
        trade_date = data.get("trade_date")
        asset_type = data.get("asset_type", "stock")
        
        tasks = []
        for ticker in tickers:
            analyst = self.get_analyst(ticker)
            task = analyst.analyze(ticker, trade_date, asset_type)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        self.bus.publish(Channel.SIGNALS, {
            "type": "orchestrated_analysis_complete",
            "source": self.name,
            "data": {
                "trade_date": trade_date,
                "results": [
                    {
                        "ticker": r.ticker if hasattr(r, 'ticker') else str(e),
                        "success": r.success if hasattr(r, 'success') else False,
                        "error": r.error if hasattr(r, 'error') else str(e),
                        "llm_signals": {k: v for k, v in r.llm_signals.__dict__.items()} if hasattr(r, 'llm_signals') else None
                    }
                    for r, e in [(res, None) if not isinstance(res, Exception) else (None, res) for res in results]
                ]
            }
        }, self.name)


class SignalFusionAgent(BaseAgent):
    def __init__(self, bus: MessageBus, config: HybridConfig):
        super().__init__("signal_fusion", bus)
        self.config = config
        self.latest_llm: Dict[str, LLMSignals] = {}
        self.latest_quant: Dict[str, Any] = {}
        self.bus.subscribe(Channel.SIGNALS, self._on_signal)
    
    async def handle_message(self, payload: dict):
        pass
    
    async def _on_signal(self, payload: dict):
        msg_type = payload.get("type")
        source = payload.get("source")
        data = payload.get("data", {})
        ticker = data.get("ticker") or data.get("symbol")
        
        if not ticker:
            return
        
        if msg_type == "llm_analysis_result" and data.get("success"):
            signals_data = data.get("llm_signals", {})
            if signals_data:
                self.latest_llm[ticker] = LLMSignals(**signals_data)
                await self._try_fuse(ticker)
        
        elif msg_type == "quant_decision" and source == "quant_agent":
            self.latest_quant[ticker] = data
            await self._try_fuse(ticker)
    
    async def _try_fuse(self, ticker: str):
        if ticker in self.latest_llm and ticker in self.latest_quant:
            from hybrid.quant_engine import QuantDecision, compute_decision
            from hybrid.technical_indicators import TechnicalSignals
            
            llm = self.latest_llm[ticker]
            quant_data = self.latest_quant[ticker]
            
            tech = TechnicalSignals(**quant_data.get("tech_signals", {}))
            
            quant_decision = QuantDecision(
                action=quant_data.get("action", "HOLD"),
                composite_score=quant_data.get("score", 0.5),
                confidence=quant_data.get("confidence", 0.0),
                llm_component=0,
                quant_component=0
            )
            
            fused = compute_decision(llm, tech, self.config)
            
            self.bus.publish(Channel.SIGNALS, {
                "type": "fused_signal",
                "source": self.name,
                "data": {
                    "ticker": ticker,
                    "fused_action": fused.action,
                    "fused_score": fused.composite_score,
                    "fused_confidence": fused.confidence,
                    "llm_component": fused.llm_component,
                    "quant_component": fused.quant_component,
                    "agreement": fused.llm_quant_agreement,
                    "agreement_detail": fused.agreement_detail,
                    "llm_signals": {
                        "sentiment": llm.sentiment_score,
                        "fundamental": llm.fundamental_score,
                        "news": llm.news_score,
                        "debate": llm.research_debate_score,
                    },
                    "quant_signals": {
                        "rsi": tech.rsi_score,
                        "ema": tech.ema_crossover_score,
                        "bollinger": tech.bollinger_score,
                        "volume": tech.volume_score,
                    }
                }
            }, self.name)
            
            del self.latest_llm[ticker]
            del self.latest_quant[ticker]


def create_llm_agents(bus: MessageBus, config: HybridConfig) -> Dict[str, BaseAgent]:
    return {
        "llm_analyst": LLMAnalystAgent(bus, config),
        "llm_orchestrator": LLMOrchestratorAgent(bus, config),
        "signal_fusion": SignalFusionAgent(bus, config),
    }
