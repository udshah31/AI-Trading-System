"""The bot's own positions survive a restart, so it can still close them; dry-run and live
positions are kept apart, and a store outage never stops trading."""
import asyncio
from decimal import Decimal
from types import SimpleNamespace

from hybrid.config import HybridConfig
from hybrid.orchestrator import Orchestrator, RedisHoldingsStore, holdings_key


class RecordingBus:
    def __init__(self):
        self.published = []

    def subscribe(self, channel, handler):
        pass

    def publish(self, channel, payload, source):
        self.published.append(payload)

    def orders(self):
        return [p["data"] for p in self.published if p.get("type") == "execute_order"]


class FakeRedis:
    """The three async hash commands RedisHoldingsStore uses (decode_responses=True)."""

    def __init__(self, fail=False):
        self.hashes, self.fail = {}, fail

    async def hgetall(self, key):
        if self.fail:
            raise ConnectionError("Redis down")
        return dict(self.hashes.get(key, {}))

    async def hset(self, key, field, value):
        if self.fail:
            raise ConnectionError("Redis down")
        self.hashes.setdefault(key, {})[field] = str(value)

    async def hdel(self, key, field):
        if self.fail:
            raise ConnectionError("Redis down")
        self.hashes.get(key, {}).pop(field, None)


class FakeKraken:
    def __init__(self, balances):
        self.balances = {k: Decimal(v) for k, v in balances.items()}

    async def get_balances(self):
        return {k: SimpleNamespace(total=v) for k, v in self.balances.items()}


def _approved(action, ticker="BTC/USDT", shares=0.05):
    return {"type": "risk_assessment", "data": {
        "ticker": ticker, "action": action, "approved": True, "shares": shares,
        "price": 84000.0, "stop_loss": 80000.0, "rejection_reason": ""}}


def _started(redis, key="orchestrator:holdings:dry_run", exchange=None):
    bus = RecordingBus()
    orch = Orchestrator(bus, HybridConfig(), exchange=exchange, store=RedisHoldingsStore(redis, key))
    asyncio.run(orch.start())
    return bus, orch


def _fill_last(orch, bus, success=True):
    order = bus.orders()[-1]
    asyncio.run(orch._on_order({"type": "execution_result", "data": {
        "success": success, "symbol": order["symbol"], "message": "",
        "client_order_id": order["client_order_id"]}}))


def test_holdings_key_separates_dry_run_and_live():
    assert holdings_key(dry_run=True) == "orchestrator:holdings:dry_run"
    assert holdings_key(dry_run=False) == "orchestrator:holdings:live"


def test_fills_are_saved_and_removed():
    redis = FakeRedis()
    bus, orch = _started(redis)
    asyncio.run(orch._on_signal(_approved("BUY", shares=0.05)))
    _fill_last(orch, bus)
    assert redis.hashes["orchestrator:holdings:dry_run"] == {"BTC/USDT": "0.05"}

    asyncio.run(orch._on_signal(_approved("SELL")))
    _fill_last(orch, bus)
    assert redis.hashes["orchestrator:holdings:dry_run"] == {}


def test_failed_fill_is_not_saved():
    redis = FakeRedis()
    bus, orch = _started(redis)
    asyncio.run(orch._on_signal(_approved("BUY")))
    _fill_last(orch, bus, success=False)
    assert redis.hashes.get("orchestrator:holdings:dry_run", {}) == {}


def test_restarted_bot_can_sell_what_it_bought():
    redis = FakeRedis()
    bus, orch = _started(redis)
    asyncio.run(orch._on_signal(_approved("BUY", shares=0.05)))
    _fill_last(orch, bus)

    bus2, restarted = _started(redis)  # new process, same Redis
    assert restarted.holdings == {"BTC/USDT": 0.05}
    asyncio.run(restarted._on_signal(_approved("SELL")))
    assert [(o["side"], o["volume"]) for o in bus2.orders()] == [("sell", 0.05)]


def test_live_restart_sells_only_the_bots_share():
    redis = FakeRedis()
    redis.hashes["orchestrator:holdings:live"] = {"BTC/USDT": "0.05"}
    bus, orch = _started(redis, key="orchestrator:holdings:live",
                         exchange=FakeKraken({"XXBT": "1.05"}))  # user's own 1 BTC + the bot's 0.05
    asyncio.run(orch._on_signal(_approved("SELL")))
    assert [(o["side"], o["volume"]) for o in bus.orders()] == [("sell", 0.05)]


def test_dry_run_positions_never_load_in_live_mode():
    redis = FakeRedis()
    redis.hashes["orchestrator:holdings:dry_run"] = {"BTC/USDT": "0.05"}  # simulated
    bus, orch = _started(redis, key="orchestrator:holdings:live", exchange=FakeKraken({"XXBT": "1.0"}))
    assert orch.holdings == {}
    asyncio.run(orch._on_signal(_approved("SELL")))
    assert bus.orders() == []  # the user's real BTC is untouched


def test_store_outage_does_not_stop_trading():
    redis = FakeRedis(fail=True)
    bus, orch = _started(redis)  # load fails: start empty, keep running
    assert orch.holdings == {}
    asyncio.run(orch._on_signal(_approved("BUY")))
    _fill_last(orch, bus)          # save fails: still tracked in memory
    assert orch.holdings == {"BTC/USDT": 0.05}
