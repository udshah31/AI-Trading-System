"""
Orchestrator - Coordinates analysis schedule
"""
from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel


class Orchestrator(BaseAgent):
    def __init__(self, bus: MessageBus, config):
        super().__init__("orchestrator", bus)
        self.config = config
        self.bus.subscribe(Channel.SIGNALS, self._on_signal)
    
    async def handle_message(self, payload: dict):
        pass
    
    async def start(self):
        print("[Orchestrator] Started")
    
    async def _on_signal(self, payload: dict):
        msg_type = payload.get("type")
        
        if msg_type == "quant_decision":
            print(f"[Orchestrator] Quant decision: {payload['data']['action']} {payload['data']['ticker']}")
        
        elif msg_type == "risk_assessment":
            status = "✅ APPROVED" if payload['data']['approved'] else f"🚫 BLOCKED: {payload['data']['rejection_reason']}"
            print(f"[Orchestrator] Risk: {status} for {payload['data']['ticker']}")
        
        elif msg_type == "execution_result":
            status = "✅" if payload['data']['success'] else "❌"
            print(f"[Orchestrator] Execution: {status} {payload['data']['symbol']} - {payload['data']['message']}")
        
        elif msg_type == "llm_analysis_result":
            if payload['data']['success']:
                print(f"[Orchestrator] LLM analysis complete for {payload['data']['ticker']}")
        
        elif msg_type == "btc_funding_position":
            action = payload['data'].get('action', 'unknown')
            print(f"[Orchestrator] BTC Funding: {action} - {payload['data']}")
    
    async def analyze(self, ticker: str):
        self.bus.publish(Channel.SIGNALS, {
            "type": "analyze_request",
            "target": "quant_agent",
            "data": {"ticker": ticker}
        }, self.name)
