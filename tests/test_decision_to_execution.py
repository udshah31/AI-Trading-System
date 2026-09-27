"""Quant decisions can be BUY/SELL without LLM data, and risk-approved decisions reach
the execution agent as spot orders (no stacking, no shorting)."""
import asyncio
from types import SimpleNamespace

import pytest

from hybrid.config import HybridConfig
from hybrid.messaging import Channel
from hybrid.quant_engine import compute_decision
from hybrid.signal_extractor import LLMSignals, extract_signals
from hybrid.technical_indicators import TechnicalSignals


class RecordingBus:
    def __init__(self):
        self.published = []

    def subscribe(self, channel, handler):
        pass

    def publish(self, channel, payload, source):
        self.published.append((channel, payload))

    def of_type(self, msg_type):
        return [(c, p) for c, p in self.published if p.get("type") == msg_type]


def _tech(score):
    return TechnicalSignals(rsi_score=score, ema_crossover_score=score, bollinger_score=score,
                            volume_score=score, current_price=100.0, atr=2.0)


# ── #1: decisions without LLM data ──

@pytest.mark.parametrize("tech,action", [(0.9, "BUY"), (0.1, "SELL"), (0.5, "HOLD")])
def test_quant_only_decision_uses_full_range(tech, action):
    decision = compute_decision(LLMSignals(), _tech(tech), HybridConfig())
    assert decision.action == action
    assert decision.composite_score == pytest.approx(tech)


def test_real_llm_signals_keep_hybrid_weighting():
    neutral_llm = LLMSignals(available=True)
    decision = compute_decision(neutral_llm, _tech(1.0), HybridConfig())
    assert decision.composite_score == pytest.approx(0.625)  # 0.75 * 0.5 + 0.25 * 1.0


def test_extracted_signals_are_marked_available():
    assert extract_signals({"final_trade_decision": "**Rating**: Hold"}).available is True
    assert LLMSignals().available is False


# ── symbols: exchange pair for orders, Yahoo symbol for price data ──

@pytest.mark.parametrize("pair,yahoo", [
    ("BTC/USDT", "BTC-USD"), ("ETH/USDC", "ETH-USD"), ("XBT/USD", "BTC-USD"),
    ("SOL/USDT", "SOL-USD"), ("BTC-USD", "BTC-USD"), ("AAPL", "AAPL"),
])
def test_market_data_symbol(pair, yahoo):
    from hybrid.quant_agent import market_data_symbol
    assert market_data_symbol(pair) == yahoo


def test_quant_agent_analyses_yahoo_symbol_and_reports_exchange_pair():
    from hybrid.quant_agent import QuantAgent

    analysed = []

    def analyze_quant_only(symbol, llm_signals=None):
        analysed.append(symbol)
        decision = SimpleNamespace(action="BUY", composite_score=0.8, confidence=0.4,
                                   llm_component=0.0, llm_quant_agreement=True)
        return SimpleNamespace(quant_decision=decision, tech_signals=_tech(0.8))

    bus = RecordingBus()
    agent = QuantAgent(bus, HybridConfig())
    agent.pipeline = SimpleNamespace(analyze_quant_only=analyze_quant_only)
    asyncio.run(agent._on_request({"type": "analyze_request", "target": "quant_agent",
                                   "data": {"ticker": "BTC/USDT"}}))

    assert analysed == ["BTC-USD"]
    [(_, payload)] = bus.of_type("quant_decision")
    assert payload["data"]["ticker"] == "BTC/USDT"


# ── #2: risk-approved decisions become orders ──

def test_risk_assessment_carries_action_and_price():
    from hybrid.risk_agent import RiskAgent

    bus = RecordingBus()
    agent = RiskAgent(bus, HybridConfig())
    asyncio.run(agent._on_quant_decision({
        "type": "quant_decision", "source": "quant_agent",
        "data": {"ticker": "BTC/USDT", "action": "BUY", "score": 0.8, "confidence": 0.5,
                 "tech_signals": {"current_price": 84000.0, "atr": 2400.0}},
    }))
    [(_, payload)] = bus.of_type("risk_assessment")
    assert payload["data"]["approved"] is True
    assert payload["data"]["action"] == "BUY"
    assert payload["data"]["price"] == 84000.0
    assert payload["data"]["shares"] > 0


def _assessment(action, shares=0.05, approved=True, ticker="BTC/USDT"):
    return {"type": "risk_assessment", "source": "risk_agent",
            "data": {"ticker": ticker, "action": action, "approved": approved, "shares": shares,
                     "price": 84000.0, "stop_loss": 79000.0, "rejection_reason": ""}}


def _fill(order, success=True):
    return {"type": "execution_result", "source": "execution_agent",
            "data": {"success": success, "symbol": order["symbol"], "message": "",
                     "client_order_id": order["client_order_id"]}}


def test_orchestrator_opens_and_closes_spot_positions():
    from hybrid.orchestrator import Orchestrator

    bus = RecordingBus()
    orch = Orchestrator(bus, HybridConfig())
    run = asyncio.run

    run(orch._on_signal(_assessment("SELL")))  # flat: spot can't short
    assert bus.of_type("execute_order") == []

    run(orch._on_signal(_assessment("BUY", shares=0.05)))
    [(channel, buy)] = bus.of_type("execute_order")
    assert channel is Channel.ORDERS
    assert (buy["data"]["symbol"], buy["data"]["side"], buy["data"]["volume"]) == ("BTC/USDT", "buy", 0.05)
    assert buy["data"]["asset_type"] == "crypto" and buy["data"]["client_order_id"]

    run(orch._on_signal(_assessment("BUY")))  # order still in flight
    assert len(bus.of_type("execute_order")) == 1

    run(orch._on_order(_fill(buy["data"])))
    assert orch.holdings == {"BTC/USDT": 0.05}
    run(orch._on_signal(_assessment("BUY")))  # already long: no stacking
    assert len(bus.of_type("execute_order")) == 1

    run(orch._on_signal(_assessment("SELL", shares=9.9)))
    sell = bus.of_type("execute_order")[-1][1]["data"]
    assert (sell["side"], sell["volume"]) == ("sell", 0.05)  # closes what we hold
    run(orch._on_order(_fill(sell)))
    assert orch.holdings == {}


def test_orchestrator_ignores_rejections_and_retries_after_failed_fill():
    from hybrid.orchestrator import Orchestrator

    bus = RecordingBus()
    orch = Orchestrator(bus, HybridConfig())
    asyncio.run(orch._on_signal(_assessment("BUY", approved=False)))
    asyncio.run(orch._on_signal(_assessment("HOLD")))
    assert bus.of_type("execute_order") == []

    asyncio.run(orch._on_signal(_assessment("BUY")))
    first = bus.of_type("execute_order")[0][1]["data"]
    asyncio.run(orch._on_order(_fill(first, success=False)))
    assert orch.holdings == {}
    asyncio.run(orch._on_signal(_assessment("BUY")))
    assert len(bus.of_type("execute_order")) == 2


def test_execution_agent_echoes_client_order_id():
    from hybrid.execution_agent import ExecutionAgent

    async def place_order(order):
        return SimpleNamespace(success=True, order_id="DRYRUN-1", status="closed", error=None)

    bus = RecordingBus()
    agent = ExecutionAgent(bus, HybridConfig(), dry_run=True)
    agent.kraken_spot = SimpleNamespace(place_order=place_order)
    asyncio.run(agent._on_execution_request({"type": "execute_order", "data": {
        "symbol": "BTC/USDT", "side": "buy", "volume": 0.05, "order_type": "market",
        "asset_type": "crypto", "client_order_id": "abc123"}}))
    [(_, result)] = bus.of_type("execution_result")
    assert result["data"]["success"] is True
    assert result["data"]["client_order_id"] == "abc123"
