"""Periodic settlement must not race submission or execution callbacks (no broker calls)."""
import asyncio
import time
from types import SimpleNamespace

import pytest

from hybrid.config import HybridConfig
from hybrid.orchestrator import Orchestrator


class Bus:
    def __init__(self):
        self.messages = []

    def subscribe(self, *args):
        pass

    def publish(self, channel, payload, source):
        self.messages.append(payload)


class Broker:
    name = "test_paper"

    def __init__(self, found):
        self.found = found
        self.lookups = 0

    async def find_orders(self, *args):
        self.lookups += 1
        return self.found

    async def position_volume(self, symbol):
        return 0.05


def make_orchestrator(found):
    bus = Bus()
    broker = Broker(found)
    orch = Orchestrator(bus, HybridConfig(), broker=broker)
    orch.pending["cid"] = {"ticker": "BTC/USDT", "side": "buy", "volume": 0.05,
                           "price": 84000.0, "stop": 80000.0, "sent_at": time.time(),
                           "awaiting_settlement": True}
    return orch, bus, broker


def test_submission_is_not_polled_before_execution_returns():
    orch, _, broker = make_orchestrator([])
    orch.pending["cid"].pop("awaiting_settlement")
    asyncio.run(orch.reconcile_pending_orders())
    assert broker.lookups == 0 and "cid" in orch.pending


def test_runtime_not_found_remains_pending():
    orch, _, _ = make_orchestrator([])
    asyncio.run(orch.reconcile_pending_orders())
    assert "cid" in orch.pending and orch.portfolio.cash == 100000


def test_periodic_reconciliation_never_drops_simulated_order():
    orch, _, _ = make_orchestrator([])
    orch.broker = None
    asyncio.run(orch.reconcile_pending_orders())
    assert "cid" in orch.pending


@pytest.mark.parametrize("rows", [
    [{"status": "filled", "side": "buy", "vol_exec": 0.05, "avg_price": None}],
    [{"status": "filled", "side": "buy", "vol_exec": 0.05, "avg_price": float("nan")}],
    [{"status": "filled", "side": "sell", "vol_exec": 0.05, "avg_price": 85000}],
    [{"status": "filled", "side": "buy", "vol_exec": 0.1, "avg_price": 85000}],
    [{"status": "unknown", "side": "buy", "vol_exec": 0.05, "avg_price": 85000}],
])
def test_invalid_or_unknown_broker_results_do_not_change_books(rows):
    orch, _, _ = make_orchestrator(rows)
    asyncio.run(orch.reconcile_pending_orders())
    assert "cid" in orch.pending and orch.holdings == {} and orch.portfolio.cash == 100000


def test_multiple_fills_use_volume_weighted_broker_price():
    orch, bus, _ = make_orchestrator([
        {"status": "closed", "side": "buy", "vol_exec": 0.02, "avg_price": 85000},
        {"status": "closed", "side": "buy", "vol_exec": 0.03, "avg_price": 86000},
    ])
    asyncio.run(orch.reconcile_pending_orders())
    assert orch.portfolio.cash == pytest.approx(95720)
    assert orch.portfolio.positions["BTC/USDT"]["avg_price"] == pytest.approx(85600)
    assert len([m for m in bus.messages if m["type"] == "trade_filled"]) == 1


def test_execution_callback_and_reconciler_cannot_book_twice():
    async def scenario():
        orch, bus, broker = make_orchestrator([
            {"status": "filled", "side": "buy", "vol_exec": 0.05, "avg_price": 85000}])
        entered, release = asyncio.Event(), asyncio.Event()

        async def find_orders(*args):
            entered.set()
            await release.wait()
            return broker.found

        broker.find_orders = find_orders
        reconciliation = asyncio.create_task(orch.reconcile_pending_orders())
        await entered.wait()
        callback = asyncio.create_task(orch._on_order({"type": "execution_result", "data": {
            "success": True, "symbol": "BTC/USDT", "message": "filled", "client_order_id": "cid",
            "status": "filled", "filled_volume": 0.05, "avg_price": 85000}}))
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(reconciliation, callback)
        assert orch.portfolio.cash == pytest.approx(95750)
        assert orch.holdings["BTC/USDT"] == pytest.approx(0.05)
        assert len([m for m in bus.messages if m["type"] == "trade_filled"]) == 1

    asyncio.run(scenario())


def test_runtime_does_not_reload_settled_intents_from_store():
    orch, _, _ = make_orchestrator([
        {"status": "filled", "side": "buy", "vol_exec": 0.05, "avg_price": 85000}])

    async def forbidden_reload():
        raise AssertionError("pending intents are restored at startup only")

    async def no_op(*args):
        pass

    orch.store = SimpleNamespace(load_pending=forbidden_reload, save=no_op, save_stop=no_op,
                                 save_portfolio=no_op, remove_pending=no_op)
    asyncio.run(orch.reconcile_pending_orders())
    asyncio.run(orch.reconcile_pending_orders())
    assert orch.pending == {} and orch.portfolio.cash == pytest.approx(95750)
