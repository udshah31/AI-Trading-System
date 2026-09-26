"""Alpaca paper broker: symbol mapping, orders by client_order_id, waiting for fills,
partial/open outcomes, position lookups, and wiring into orchestrator/execution."""
import asyncio
from types import SimpleNamespace

import pytest

pytest.importorskip("alpaca")  # alpaca-py

from hybrid.brokers.alpaca import AlpacaBroker, alpaca_symbol  # noqa: E402
from hybrid.brokers.base import OrderReport  # noqa: E402

run = asyncio.run


class APIError(Exception):
    def __init__(self, status_code, msg="error"):
        super().__init__(msg)
        self.status_code = status_code


def _order(status, filled="0", avg=None, side="buy", oid="o-1"):
    return SimpleNamespace(id=oid, status=SimpleNamespace(value=status), filled_qty=filled,
                           filled_avg_price=avg, side=SimpleNamespace(value=side))


class FakeAlpaca:
    """Methods and fields of alpaca-py's TradingClient that AlpacaBroker uses."""

    def __init__(self, fills=None, positions=None, increment="0.0001", submit_error=None):
        self.fills = list(fills or [_order("filled", "0.05", "84000.5")])
        self.positions = positions or {}
        self.increment, self.submit_error = increment, submit_error
        self.submitted, self.by_client_id = [], {}

    def get_asset(self, symbol):
        return SimpleNamespace(min_trade_increment=self.increment)

    def submit_order(self, request):
        if self.submit_error:
            raise self.submit_error
        self.submitted.append(request)
        self.by_client_id[request.client_order_id] = self.fills[-1]
        return _order("pending_new")

    def get_order_by_id(self, order_id):
        return self.fills.pop(0) if len(self.fills) > 1 else self.fills[0]

    def get_order_by_client_id(self, client_id):
        if client_id not in self.by_client_id:
            raise APIError(404, "order not found")
        return self.by_client_id[client_id]

    def get_open_position(self, symbol):
        if symbol not in self.positions:
            raise APIError(404, "position does not exist")
        if self.positions[symbol] == "boom":
            raise APIError(500, "server error")
        return SimpleNamespace(qty=self.positions[symbol])


async def _no_sleep(_):
    return None


def _broker(client, **kw):
    return AlpacaBroker(client=client, sleep=_no_sleep, poll_s=0.5, fill_wait_s=kw.pop("fill_wait_s", 5), **kw)


@pytest.mark.parametrize("pair,expected", [("BTC/USDT", "BTC/USD"), ("XBT/USD", "BTC/USD"),
                                           ("ETH-USD", "ETH/USD"), ("SOL/USDC", "SOL/USD"), ("aapl", "AAPL")])
def test_symbol_mapping(pair, expected):
    assert alpaca_symbol(pair) == expected


def test_market_buy_waits_for_the_fill():
    client = FakeAlpaca(fills=[_order("new"), _order("filled", "0.0512", "84000.5")])
    report = run(_broker(client).submit_market("BTC/USDT", "buy", 0.05123, "cid-1"))
    [req] = client.submitted
    assert (req.symbol, req.side.value, req.time_in_force.value, req.client_order_id) == ("BTC/USD", "buy", "gtc", "cid-1")
    assert req.qty == pytest.approx(0.0512)  # floored to the 0.0001 increment
    assert (report.status, report.filled_volume, report.avg_price) == ("filled", 0.0512, 84000.5)
    assert report.filled


def test_below_minimum_is_rejected_without_sending():
    client = FakeAlpaca()
    report = run(_broker(client).submit_market("BTC/USDT", "buy", 0.00005, "cid"))
    assert report.status == "rejected" and "minimum" in report.message and client.submitted == []


def test_submit_error_is_a_rejection():
    report = run(_broker(FakeAlpaca(submit_error=APIError(403, "insufficient buying power")))
                 .submit_market("BTC/USDT", "buy", 1.0, "cid"))
    assert report.status == "rejected" and "insufficient buying power" in report.message


def test_partial_fill_and_still_open():
    partial = run(_broker(FakeAlpaca(fills=[_order("canceled", "0.02", "84000")])).submit_market("BTC/USDT", "buy", 0.05, "c1"))
    assert (partial.status, partial.filled_volume) == ("partial", 0.02) and partial.filled
    still_open = run(_broker(FakeAlpaca(fills=[_order("new")]), fill_wait_s=2).submit_market("BTC/USDT", "buy", 0.05, "c2"))
    assert still_open.status == "open" and not still_open.filled


def test_position_volume():
    broker = _broker(FakeAlpaca(positions={"BTCUSD": "0.0512", "ETHUSD": "boom"}))
    assert run(broker.position_volume("BTC/USDT")) == pytest.approx(0.0512)
    assert run(broker.position_volume("SOL/USDT")) == 0.0   # 404: no position
    with pytest.raises(APIError):                            # other errors: can't tell, don't trade
        run(broker.position_volume("ETH/USDT"))


def test_find_orders_by_client_id():
    client = FakeAlpaca(fills=[_order("filled", "0.05", "84000", side="sell")])
    broker = _broker(client)
    run(broker.submit_market("BTC/USDT", "sell", 0.05, "cid-9"))
    assert run(broker.find_orders("cid-9")) == [{"status": "closed", "side": "sell", "vol_exec": 0.05}]
    assert run(broker.find_orders("never-sent")) == []


def test_live_mode_is_refused():
    with pytest.raises(ValueError, match="paper"):
        AlpacaBroker(client=FakeAlpaca(), paper=False)


# ── wiring ──

class RecordingBus:
    def __init__(self):
        self.published = []

    def subscribe(self, channel, handler):
        pass

    def publish(self, channel, payload, source):
        self.published.append(payload)

    def of_type(self, t):
        return [p["data"] for p in self.published if p.get("type") == t]


def test_execution_agent_reports_the_brokers_fill():
    from hybrid.config import HybridConfig
    from hybrid.execution_agent import ExecutionAgent
    bus = RecordingBus()
    agent = ExecutionAgent(bus, HybridConfig(), dry_run=True, broker=_broker(FakeAlpaca(fills=[_order("filled", "0.05", "84010")])))
    run(agent._on_execution_request({"type": "execute_order", "data": {
        "symbol": "BTC/USDT", "side": "buy", "volume": 0.05, "order_type": "market",
        "asset_type": "crypto", "client_order_id": "cid-1", "userref": 7}}))
    [result] = bus.of_type("execution_result")
    assert (result["success"], result["status"], result["filled_volume"], result["avg_price"], result["exchange"]) == (
        True, "filled", 0.05, 84010.0, "alpaca_paper")


def _orch_with(broker):
    from hybrid.config import HybridConfig
    from hybrid.orchestrator import Orchestrator
    bus = RecordingBus()
    orch = Orchestrator(bus, HybridConfig(), broker=broker)
    run(orch.start())
    return bus, orch


def _approve_buy(orch, shares=0.05):
    run(orch._on_signal({"type": "risk_assessment", "data": {
        "ticker": "BTC/USDT", "action": "BUY", "approved": True, "shares": shares,
        "price": 84000.0, "stop_loss": 80000.0, "rejection_reason": ""}}))


def _result(orch, bus, **fields):
    order = bus.of_type("execute_order")[-1]
    run(orch._on_order({"type": "execution_result", "data": {
        "symbol": "BTC/USDT", "message": "", "client_order_id": order["client_order_id"], **fields}}))


def test_orchestrator_books_partial_fills():
    bus, orch = _orch_with(_broker(FakeAlpaca()))
    _approve_buy(orch, shares=0.05)
    _result(orch, bus, success=True, status="partial", filled_volume=0.02, avg_price=84000.0)
    assert orch.holdings == {"BTC/USDT": 0.02}
    assert orch.portfolio.positions["BTC/USDT"]["volume"] == pytest.approx(0.02)


def test_orchestrator_keeps_an_open_order_pending():
    bus, orch = _orch_with(_broker(FakeAlpaca()))
    _approve_buy(orch)
    _result(orch, bus, success=False, status="open", filled_volume=0.0, avg_price=None)
    assert orch.holdings == {} and len(orch.pending) == 1  # not lost, not assumed filled
    _approve_buy(orch)
    assert len(bus.of_type("execute_order")) == 1  # the coin stays blocked while it's unresolved


def test_orchestrator_uses_alpaca_for_live_balance_checks():
    bus, orch = _orch_with(_broker(FakeAlpaca(positions={"BTCUSD": "0.5"})))  # paper account already holds BTC
    _approve_buy(orch)
    assert bus.of_type("execute_order") == []


def test_build_broker_from_env(monkeypatch):
    from hybrid.main_async import build_broker
    monkeypatch.setenv("BROKER", "alpaca")
    monkeypatch.setenv("LIVE_TRADING", "true")
    with pytest.raises(SystemExit, match="paper"):
        build_broker()
    monkeypatch.delenv("LIVE_TRADING")
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    with pytest.raises(SystemExit, match="ALPACA_API_KEY"):
        build_broker()
    monkeypatch.setenv("ALPACA_API_KEY", "PKTEST")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "secret")
    broker = build_broker()
    assert isinstance(broker, AlpacaBroker) and broker.name == "alpaca_paper"
    assert OrderReport  # interface exported


# ── fees taken in the coin, and dust ──

def _fill_last(orch, bus, client, held_after, price=84000.0, filled=None):
    """Fill the last order; the paper account then holds `held_after` BTC."""
    order = bus.of_type("execute_order")[-1]
    client.positions.pop("BTCUSD", None)
    if held_after > 0:
        client.positions["BTCUSD"] = str(held_after)
    _result(orch, bus, success=True, status="filled", filled_volume=filled or order["volume"], avg_price=price)
    return order


def _approve_sell(orch):
    run(orch._on_signal({"type": "risk_assessment", "data": {
        "ticker": "BTC/USDT", "action": "SELL", "approved": True, "shares": 0.0005,
        "price": 84000.0, "stop_loss": 0, "rejection_reason": ""}}))


def test_buy_holds_what_alpaca_credited_after_the_fee():
    client = FakeAlpaca()
    bus, orch = _orch_with(_broker(client))
    _approve_buy(orch, shares=0.0005)
    _fill_last(orch, bus, client, held_after=0.000498748)  # Alpaca took its fee in BTC
    assert orch.holdings["BTC/USDT"] == pytest.approx(0.000498748)
    pos = orch.portfolio.positions["BTC/USDT"]
    assert pos["volume"] == pytest.approx(0.000498748)
    assert pos["volume"] * pos["avg_price"] == pytest.approx(0.0005 * 84000.0)  # fee in the cost basis


def test_selling_everything_leaves_no_position_to_block_the_next_buy():
    client = FakeAlpaca()
    bus, orch = _orch_with(_broker(client))
    _approve_buy(orch, shares=0.0005)
    _fill_last(orch, bus, client, held_after=0.000498748)
    _approve_sell(orch)
    sell = _fill_last(orch, bus, client, held_after=0)
    assert (sell["side"], sell["volume"]) == ("sell", pytest.approx(0.000498748))
    assert orch.holdings == {} and orch.stops == {} and orch.portfolio.positions == {}
    _approve_buy(orch, shares=0.0005)
    assert bus.of_type("execute_order")[-1]["side"] == "buy"  # not skipped as "already long"


def test_sub_minimum_leftover_after_a_sell_is_dropped():
    client = FakeAlpaca(positions={"BTCUSD": "0.0005"})
    bus, orch = _orch_with(_broker(FakeAlpaca()))
    orch.broker = _broker(client)
    orch.holdings["BTC/USDT"] = 0.0005  # booked before the fee sync existed
    _approve_sell(orch)
    _fill_last(orch, bus, client, held_after=0, filled=0.000498748)
    assert orch.holdings == {}  # 1.25e-6 BTC ≈ $0.10 left: dust, not a position


def test_partial_sell_above_the_minimum_keeps_the_rest():
    client = FakeAlpaca()
    bus, orch = _orch_with(_broker(client))
    _approve_buy(orch, shares=0.05)
    _fill_last(orch, bus, client, held_after=0.05)
    _approve_sell(orch)
    _fill_last(orch, bus, client, held_after=0.02, filled=0.03)
    assert orch.holdings["BTC/USDT"] == pytest.approx(0.02)  # $1,680 left: a real position
