"""Live risk tracking: the bot keeps its own books (cash, positions, equity, peak,
drawdown), the risk manager sizes and blocks trades from those real numbers, and
the dashboard and database see the same state."""
import asyncio
from datetime import date
from decimal import Decimal

import pytest

from hybrid.config import HybridConfig
from hybrid.messaging import Channel
from hybrid.portfolio import Portfolio
from hybrid.quant_engine import QuantDecision
from hybrid.technical_indicators import TechnicalSignals

run = asyncio.run


# ── the books ──

def test_buy_mark_and_sell():
    pf = Portfolio(100_000.0, today=date(2026, 9, 26))
    pf.apply_fill("BTC/USDT", "buy", 0.5, 80_000.0)
    assert (pf.cash, pf.equity, pf.peak, pf.drawdown) == (60_000.0, 100_000.0, 100_000.0, 0.0)

    pf.mark("BTC/USDT", 72_000.0)
    assert pf.equity == pytest.approx(96_000.0)
    assert pf.drawdown == pytest.approx(0.04)
    assert pf.exposure == pytest.approx(36_000.0)

    realised = pf.apply_fill("BTC/USDT", "sell", 0.5, 72_000.0)
    assert realised == pytest.approx(-4_000.0)
    assert pf.positions == {} and pf.cash == pytest.approx(96_000.0)
    assert pf.realized_today == pytest.approx(-4_000.0)


def test_partial_sell_uses_average_entry_and_new_highs_raise_the_peak():
    pf = Portfolio(100_000.0, today=date(2026, 9, 26))
    pf.apply_fill("ETH/USDT", "buy", 10, 2_000.0)
    pf.apply_fill("ETH/USDT", "buy", 10, 2_200.0)   # average entry 2,100
    assert pf.apply_fill("ETH/USDT", "sell", 5, 2_300.0) == pytest.approx(1_000.0)
    pf.mark("ETH/USDT", 2_500.0)
    assert pf.peak == pytest.approx(pf.equity) and pf.drawdown == 0.0


def test_daily_pnl_resets_at_the_day_boundary():
    pf = Portfolio(100_000.0, today=date(2026, 9, 26))
    pf.apply_fill("BTC/USDT", "buy", 1, 80_000.0)
    pf.mark("BTC/USDT", 81_000.0)
    assert pf.daily_pnl == pytest.approx(1_000.0)
    pf.roll_day(date(2026, 9, 27))
    assert pf.daily_pnl == 0.0 and pf.realized_today == 0.0
    pf.mark("BTC/USDT", 80_500.0)
    assert pf.daily_pnl == pytest.approx(-500.0)


def test_state_round_trips():
    pf = Portfolio(100_000.0, today=date(2026, 9, 26))
    pf.apply_fill("BTC/USDT", "buy", 0.5, 80_000.0)
    pf.mark("BTC/USDT", 70_000.0)
    restored = Portfolio.from_dict(pf.to_dict(), today=date(2026, 9, 26))
    assert (restored.equity, restored.peak, restored.drawdown) == (pf.equity, pf.peak, pf.drawdown)


# ── config honours .env ──

def test_risk_limits_come_from_env(monkeypatch):
    monkeypatch.setenv("INITIAL_CAPITAL", "25000")
    monkeypatch.setenv("MAX_DRAWDOWN_PCT", "0.10")
    monkeypatch.setenv("MAX_POSITION_PCT", "0.15")
    monkeypatch.setenv("MAX_RISK_PER_TRADE", "0.01")
    cfg = HybridConfig()
    assert (cfg.initial_capital, cfg.max_drawdown_pct, cfg.max_position_pct, cfg.max_risk_per_trade_pct) == (
        25_000.0, 0.10, 0.15, 0.01)


# ── orchestrator -> risk manager -> dashboard ──

class RecordingBus:
    def __init__(self):
        self.published = []

    def subscribe(self, channel, handler):
        pass

    def publish(self, channel, payload, source):
        self.published.append((channel, payload))

    def of_type(self, t):
        return [p["data"] for _, p in self.published if p.get("type") == t]


class Clock:
    now = 1_000.0

    def __call__(self):
        return self.now


def _orch(bus=None):
    from hybrid.orchestrator import Orchestrator
    bus = bus or RecordingBus()
    orch = Orchestrator(bus, HybridConfig(), clock=Clock())
    run(orch.start())
    return bus, orch


def _buy_and_fill(orch, bus, price=80_000.0, fill_price=None, shares=0.5):
    run(orch._on_signal({"type": "risk_assessment", "data": {
        "ticker": "BTC/USDT", "action": "BUY", "approved": True, "shares": shares,
        "price": price, "stop_loss": price * 0.9, "rejection_reason": ""}}))
    order = bus.of_type("execute_order")[-1]
    run(orch._on_order({"type": "execution_result", "data": {
        "success": True, "symbol": "BTC/USDT", "message": "", "client_order_id": order["client_order_id"],
        "avg_price": fill_price}}))


def test_orchestrator_books_fills_at_the_fill_price_and_publishes():
    bus, orch = _orch()
    _buy_and_fill(orch, bus, price=80_000.0, fill_price=80_400.0)
    assert orch.portfolio.positions["BTC/USDT"]["avg_price"] == 80_400.0
    [trade] = bus.of_type("trade_filled")
    assert (trade["side"], trade["volume"], trade["price"], trade["realized_pnl"]) == ("buy", 0.5, 80_400.0, 0.0)
    assert bus.of_type("portfolio_update")[-1]["equity"] == pytest.approx(100_000.0)


def test_fill_without_exchange_price_uses_the_latest_tick():
    bus, orch = _orch()
    run(orch._on_market_data({"symbol": "BTC/USDT", "price": 81_000.0, "bid": 80_990.0}))
    _buy_and_fill(orch, bus, price=79_000.0, fill_price=None)  # the decision's price can be hours old
    assert orch.portfolio.positions["BTC/USDT"]["avg_price"] == 81_000.0


def test_price_ticks_mark_the_books_and_publish_at_most_every_few_seconds():
    bus, orch = _orch()
    _buy_and_fill(orch, bus, fill_price=80_000.0)
    before = len(bus.of_type("portfolio_update"))
    run(orch._on_market_data({"symbol": "BTC/USDT", "price": 76_000.0, "bid": 76_000.0}))
    run(orch._on_market_data({"symbol": "BTC/USDT", "price": 75_000.0, "bid": 75_000.0}))
    updates = bus.of_type("portfolio_update")
    assert len(updates) == before  # throttled
    orch._clock.now += 10
    run(orch._on_market_data({"symbol": "BTC/USDT", "price": 74_000.0, "bid": 74_000.0}))
    assert bus.of_type("portfolio_update")[-1]["drawdown_pct"] == pytest.approx(0.03)


def _risk_agent():
    from hybrid.risk_agent import RiskAgent
    bus = RecordingBus()
    return bus, RiskAgent(bus, HybridConfig())


def _update(equity, peak, exposure=0.0, daily_pnl=0.0):
    return {"type": "portfolio_update", "data": {
        "equity": equity, "peak": peak, "drawdown_pct": max(0.0, (peak - equity) / peak),
        "exposure": exposure, "cash": equity - exposure, "daily_pnl": daily_pnl, "positions": []}}


def _buy_decision():
    return (QuantDecision(action="BUY", composite_score=0.8, confidence=1.0, llm_component=0, quant_component=0),
            TechnicalSignals(current_price=100.0, atr=5.0))


def test_risk_manager_sizes_from_real_equity():
    bus, agent = _risk_agent()
    run(agent._on_portfolio_update(_update(equity=50_000.0, peak=50_000.0)))
    assessment = agent.risk_manager.evaluate_trade(*_buy_decision(), "BTC/USDT")
    assert assessment.approved and assessment.current_portfolio_value == 50_000.0
    assert assessment.position_size_dollars <= 50_000.0 * 0.20 + 1e-6


def test_circuit_breaker_trips_on_real_drawdown():
    bus, agent = _risk_agent()
    run(agent._on_portfolio_update(_update(equity=84_000.0, peak=100_000.0)))  # 16% down, limit 15%
    assessment = agent.risk_manager.evaluate_trade(*_buy_decision(), "BTC/USDT")
    assert not assessment.approved and "CIRCUIT BREAKER" in assessment.rejection_reason
    [risk] = bus.of_type("risk_update")
    assert risk["circuit_breaker"] is True and risk["drawdown_pct"] == pytest.approx(16.0)


def test_risk_panel_numbers():
    bus, agent = _risk_agent()
    run(agent._on_portfolio_update(_update(equity=100_000.0, peak=102_000.0, exposure=12_000.0, daily_pnl=-500.0)))
    [risk] = bus.of_type("risk_update")
    assert risk["margin_used_pct"] == pytest.approx(12.0)
    assert risk["max_drawdown_pct"] == pytest.approx(15.0)
    assert risk["circuit_breaker"] is False
    assert risk["total_equity"] == 100_000.0 and risk["daily_pnl"] == -500.0


# ── storage: equity curve, positions, trades with P&L ──

@pytest.fixture
def storage(tmp_path):
    from hybrid.storage import StorageService
    svc = StorageService(f"sqlite+aiosqlite:///{tmp_path / 'risk.db'}")
    run(svc.initialize())
    yield svc
    run(svc.close())


def test_storage_agent_records_live_state(storage):
    from hybrid.storage_agent import StorageAgent
    agent = StorageAgent(RecordingBus(), storage)
    run(agent._on_signal({"type": "trade_filled", "data": {
        "symbol": "BTC/USDT", "side": "sell", "volume": 0.5, "price": 72_000.0, "realized_pnl": -4_000.0,
        "client_order_id": "abc", "reason": "stop-loss", "exchange": "kraken_spot"}}))
    update = _update(equity=96_000.0, peak=100_000.0, exposure=36_000.0)
    update["data"]["positions"] = [{"symbol": "ETH/USDT", "volume": 2.0, "avg_price": 2_000.0,
                                    "last_price": 2_100.0, "unrealized_pnl": 200.0}]
    run(agent._on_signal(update))
    run(agent._on_signal(update))  # second snapshot within a minute: not written again

    trades = run(storage.trades.get_by_symbol("BTC/USDT"))
    assert float(trades[0].pnl) == -4_000.0 and trades[0].side == "sell"
    equity = run(storage.get_equity_history("default", days=1))
    assert len(equity) == 1 and float(equity[0].equity) == 96_000.0
    assert float(equity[0].drawdown_pct) == pytest.approx(4.0)  # percent, as the dashboard reads it
    [pos] = run(storage.positions.get_open_positions("quant"))
    assert (pos.symbol, float(pos.volume), float(pos.unrealized_pnl)) == ("ETH/USDT", 2.0, 200.0)

    update["data"]["positions"] = []  # ETH sold since
    agent._last_equity_write = 0
    run(agent._on_signal(update))
    assert run(storage.positions.get_open_positions("quant")) == []
