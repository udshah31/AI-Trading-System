"""
Orchestrator - Coordinates analysis schedule and turns approved decisions into orders
"""
import uuid

from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel


class Orchestrator(BaseAgent):
    """Spot-only order policy for risk-approved quant decisions.

    BUY opens a long only when flat; SELL only closes a long we hold (spot can't
    short); nothing is sent for a ticker while its previous order is in flight.
    Holdings are tracked from execution results in memory, so they reset on restart.
    """

    def __init__(self, bus: MessageBus, config):
        super().__init__("orchestrator", bus)
        self.config = config
        self.holdings: dict[str, float] = {}  # ticker -> volume held long
        self.pending: dict[str, dict] = {}    # client_order_id -> {ticker, side, volume}
        self.bus.subscribe(Channel.SIGNALS, self._on_signal)
        self.bus.subscribe(Channel.ORDERS, self._on_order)

    async def handle_message(self, payload: dict):
        pass

    async def start(self):
        print("[Orchestrator] Started")

    async def _on_signal(self, payload: dict):
        msg_type = payload.get("type")

        if msg_type == "quant_decision":
            print(f"[Orchestrator] Quant decision: {payload['data']['action']} {payload['data']['ticker']}")

        elif msg_type == "risk_assessment":
            data = payload['data']
            status = "✅ APPROVED" if data['approved'] else f"🚫 BLOCKED: {data['rejection_reason']}"
            print(f"[Orchestrator] Risk: {status} for {data['ticker']}")
            if data['approved']:
                self._route_approved(data)

        elif msg_type == "llm_analysis_result":
            if payload['data']['success']:
                print(f"[Orchestrator] LLM analysis complete for {payload['data']['ticker']}")

        elif msg_type == "btc_funding_position":
            action = payload['data'].get('action', 'unknown')
            print(f"[Orchestrator] BTC Funding: {action} - {payload['data']}")

    async def _on_order(self, payload: dict):
        if payload.get("type") != "execution_result":
            return
        data = payload["data"]
        order = self.pending.pop(data.get("client_order_id"), None)
        status = "✅" if data["success"] else "❌"
        print(f"[Orchestrator] Execution: {status} {data['symbol']} - {data['message']}")
        if not order or not data["success"]:
            return
        if order["side"] == "buy":
            self.holdings[order["ticker"]] = order["volume"]
        else:
            self.holdings.pop(order["ticker"], None)

    def _route_approved(self, data: dict):
        ticker, action = data["ticker"], data.get("action")
        if any(o["ticker"] == ticker for o in self.pending.values()):
            print(f"[Orchestrator] Skip {action} {ticker}: previous order still in flight")
            return
        held = self.holdings.get(ticker, 0.0)
        if action == "BUY" and not held:
            self._send_order(ticker, "buy", data["shares"], data)
        elif action == "SELL" and held:
            self._send_order(ticker, "sell", held, data)
        else:
            reason = "already long" if action == "BUY" else "no long position to close (spot, no shorting)"
            print(f"[Orchestrator] Skip {action} {ticker}: {reason}")

    def _send_order(self, ticker: str, side: str, volume: float, data: dict):
        client_order_id = uuid.uuid4().hex[:16]
        self.pending[client_order_id] = {"ticker": ticker, "side": side, "volume": volume}
        self.bus.publish(Channel.ORDERS, {
            "type": "execute_order",
            "source": self.name,
            "data": {
                "symbol": ticker,
                "side": side,
                "volume": volume,
                "order_type": "market",
                "asset_type": "crypto" if "/" in ticker or "-" in ticker else "stock",
                "price": data.get("price"),
                "stop_loss": data.get("stop_loss"),
                "client_order_id": client_order_id,
            }
        }, self.name)
        print(f"[Orchestrator] Order sent: {side} {volume} {ticker} ({client_order_id})")

    async def analyze(self, ticker: str):
        self.bus.publish(Channel.SIGNALS, {
            "type": "analyze_request",
            "target": "quant_agent",
            "data": {"ticker": ticker}
        }, self.name)
