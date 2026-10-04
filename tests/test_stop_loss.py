"""Stop-loss enforcement: each bought position keeps its stop price, live prices are
watched, and reaching the stop sells the position at once (one order at a time,
retried with a delay if it fails), across restarts and in-flight crashes."""
import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace

from hybrid.config import HybridConfig
from hybrid.messaging import Channel
from hybrid.orchestrator import STOP_RETRY_S, Orchestrator, RedisHoldingsStore


class RecordingBus:
    def __init__(self):
        self.published, self.handlers = [], {}

    def subscribe(self, channel, handler):
        self.handlers.setdefault(channel, []).append(handler)

    def publish(self, channel, payload, source):
        self.published.append((channel, payload))

    def orders(self):
        return [p["data"] for _, p in self.published if p.get("type") == "execute_order"]

    def logs(self):
        return [p["data"] for c, p in self.published if p.get("type") == "log"]


class FakeRedis:
    def __init__(self):
        self.hashes = {}

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = str(value)

    async def hdel(self, key, field):
        self.hashes.get(key, {}).pop(field, None)


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


KEY = "orchestrator:holdings:dry_run"
run = asyncio.run


def _orch(redis=None, exchange=None, key=KEY):
    bus, clock = RecordingBus(), Clock()
    store = RedisHoldingsStore(redis, key) if redis is not None else None
    orch = Orchestrator(bus, HybridConfig(), exchange=exchange, store=store, clock=clock)
    run(orch.start())
    return bus, orch, clock


def _fill(orch, bus, success=True):
    order = bus.orders()[-1]
    run(orch._on_order({"type": "execution_result", "data": {
        "success": success, "symbol": order["symbol"], "message": "", "client_order_id": order["client_order_id"]}}))


def _buy(orch, bus, stop=80000.0, ticker="BTC/USDT", shares=0.05):
    run(orch._on_signal({"type": "risk_assessment", "data": {
        "ticker": ticker, "action": "BUY", "approved": True, "shares": shares,
        "price": 84000.0, "stop_loss": stop, "rejection_reason": ""}}))
    _fill(orch, bus)


def _tick(orch, price, bid=None, ticker="BTC/USDT"):
    run(orch._on_market_data({"symbol": ticker, "price": price, "bid": bid, "ask": None}))


def test_orchestrator_listens_to_market_data():
    bus, orch, _ = _orch()
    assert orch._on_market_data in bus.handlers[Channel.MARKET_DATA]


def test_stop_recorded_with_the_position():
    redis = FakeRedis()
    bus, orch, _ = _orch(redis)
    _buy(orch, bus, stop=80000.0)
    assert orch.stops == {"BTC/USDT": 80000.0}
    assert redis.hashes[f"{KEY}:stops"] == {"BTC/USDT": "80000.0"}


def test_price_above_stop_does_nothing():
    bus, orch, _ = _orch()
    _buy(orch, bus)
    _tick(orch, 80000.01)
    assert len(bus.orders()) == 1  # just the buy


def test_reaching_the_stop_sells_once():
    bus, orch, _ = _orch()
    _buy(orch, bus, stop=80000.0)
    _tick(orch, 79990.0)
    _tick(orch, 79500.0)  # exit still in flight: no second order
    orders = bus.orders()
    assert len(orders) == 2
    assert (orders[1]["side"], orders[1]["volume"], orders[1]["symbol"]) == ("sell", 0.05, "BTC/USDT")
    assert any("stop" in entry["message"].lower() for entry in bus.logs())

    _fill(orch, bus)
    assert orch.holdings == {} and orch.stops == {}
    _tick(orch, 70000.0)
    assert len(bus.orders()) == 2  # position closed: nothing more


def test_uses_the_bid_when_there_is_one():
    bus, orch, _ = _orch()
    _buy(orch, bus, stop=80000.0)
    _tick(orch, price=80100.0, bid=79950.0)  # last trade above, but a sell would fill at the bid
    assert bus.orders()[-1]["side"] == "sell"


def test_failed_exit_retries_after_a_delay():
    bus, orch, clock = _orch()
    _buy(orch, bus)
    _tick(orch, 79000.0)
    _fill(orch, bus, success=False)
    _tick(orch, 78900.0)
    assert len(bus.orders()) == 2          # no hammering the exchange every tick
    clock.now += STOP_RETRY_S + 1
    _tick(orch, 78800.0)
    assert len(bus.orders()) == 3 and bus.orders()[-1]["side"] == "sell"


def test_no_stop_no_exit():
    bus, orch, _ = _orch()
    _buy(orch, bus, stop=None)
    _tick(orch, 1.0)
    assert len(bus.orders()) == 1


class FakeKraken:
    def __init__(self, btc):
        self.btc, self.error = Decimal(btc), None

    async def get_balances(self):
        if self.error:
            raise self.error
        return {"XXBT": SimpleNamespace(total=self.btc)}

    async def find_orders_by_userref(self, userref):
        return []


def test_live_stop_exit_is_capped_at_the_balance():
    kraken = FakeKraken("0")  # empty before the buy, or the live balance check skips it
    bus, orch, _ = _orch(exchange=kraken)
    _buy(orch, bus, stop=80000.0)
    kraken.btc = Decimal("0.03")  # the fill arrived, then part was sold by hand
    _tick(orch, 79000.0)
    assert (bus.orders()[-1]["side"], bus.orders()[-1]["volume"]) == ("sell", 0.03)


def test_live_stop_exit_waits_when_balance_check_fails():
    kraken = FakeKraken("0")
    bus, orch, clock = _orch(exchange=kraken)
    _buy(orch, bus)
    kraken.btc = Decimal("0.05")
    kraken.error = RuntimeError("Kraken API error: EService:Unavailable")
    _tick(orch, 79000.0)
    assert len(bus.orders()) == 1
    kraken.error = None
    clock.now += STOP_RETRY_S + 1
    _tick(orch, 79000.0)
    assert bus.orders()[-1]["side"] == "sell"


def test_stop_survives_a_restart():
    redis = FakeRedis()
    bus, orch, _ = _orch(redis)
    _buy(orch, bus, stop=80000.0)
    bus2, restarted, _ = _orch(redis)
    assert restarted.stops == {"BTC/USDT": 80000.0}
    _tick(restarted, 79000.0)
    assert bus2.orders()[-1]["side"] == "sell"


def test_stop_restored_for_a_buy_that_filled_during_a_crash():
    redis = FakeRedis()
    key = "orchestrator:holdings:live"
    redis.hashes[f"{key}:pending"] = {"cid1": json.dumps(
        {"ticker": "BTC/USDT", "side": "buy", "volume": 0.05, "userref": 42,
         "stop": 80000.0, "price": 84000.0})}

    class Filled(FakeKraken):
        async def find_orders_by_userref(self, userref):
            return [{"status": "closed", "side": "buy", "vol_exec": 0.05}]

    bus, orch, _ = _orch(redis, exchange=Filled("0.05"), key=key)
    assert orch.holdings == {"BTC/USDT": 0.05} and orch.stops == {"BTC/USDT": 80000.0}


def test_dashboard_relays_log_messages():
    from hybrid.dashboard.app import relay_message
    msg = relay_message(json.dumps({"payload": {"type": "log", "data": {"source": "Orchestrator", "message": "Stop hit"}}}))
    assert msg == {"type": "log", "data": {"source": "Orchestrator", "message": "Stop hit"}}
