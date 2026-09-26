"""Orders in flight when the bot crashes: they're written down before being sent, tagged
with a Kraken userref, and settled against Kraken's order history on the next start."""
import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace

from hybrid.config import HybridConfig
from hybrid.orchestrator import Orchestrator, RedisHoldingsStore

KEY = "orchestrator:holdings:live"


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
    def __init__(self):
        self.hashes, self.fail_writes = {}, False

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def hset(self, key, field, value):
        if self.fail_writes:
            raise ConnectionError("Redis down")
        self.hashes.setdefault(key, {})[field] = str(value)

    async def hdel(self, key, field):
        self.hashes.get(key, {}).pop(field, None)


class FakeKraken:
    """Live spot exchange: balances plus Kraken's order history keyed by userref."""

    def __init__(self, orders=None, balances=None, error=None):
        self.orders, self.error = orders or [], error
        self.balances = {k: Decimal(v) for k, v in (balances or {}).items()}

    async def get_balances(self):
        return {k: SimpleNamespace(total=v) for k, v in self.balances.items()}

    async def find_orders_by_userref(self, userref):
        if self.error:
            raise self.error
        return [o for o in self.orders if o["userref"] == userref]


def _approved(action, ticker="BTC/USDT", shares=0.05):
    return {"type": "risk_assessment", "data": {
        "ticker": ticker, "action": action, "approved": True, "shares": shares,
        "price": 84000.0, "stop_loss": 80000.0, "rejection_reason": ""}}


def _orch(redis, exchange=None, key=KEY):
    bus = RecordingBus()
    orch = Orchestrator(bus, HybridConfig(), exchange=exchange, store=RedisHoldingsStore(redis, key))
    asyncio.run(orch.start())
    return bus, orch


def _crash_after_send(action="BUY", holdings=None):
    """Bot sends an order, then dies before the fill arrives. Returns (redis, sent order)."""
    redis = FakeRedis()
    if holdings:
        redis.hashes[KEY] = dict(holdings)
    bus, orch = _orch(redis, FakeKraken(balances={"XXBT": "1.0"} if action == "SELL" else {}))
    asyncio.run(orch._on_signal(_approved(action)))
    return redis, bus.orders()[0]


def test_order_is_saved_before_it_is_sent_with_an_int32_userref():
    redis, order = _crash_after_send()
    [saved] = [json.loads(v) for v in redis.hashes[f"{KEY}:pending"].values()]
    assert (saved["ticker"], saved["side"], saved["volume"]) == ("BTC/USDT", "buy", 0.05)
    assert saved["userref"] == order["userref"]
    assert isinstance(order["userref"], int) and 0 < order["userref"] < 2**31


def test_fill_clears_the_saved_pending_order():
    redis = FakeRedis()
    bus, orch = _orch(redis, FakeKraken())
    asyncio.run(orch._on_signal(_approved("BUY")))
    order = bus.orders()[0]
    asyncio.run(orch._on_order({"type": "execution_result", "data": {
        "success": True, "symbol": "BTC/USDT", "message": "", "client_order_id": order["client_order_id"]}}))
    assert redis.hashes[f"{KEY}:pending"] == {}
    assert redis.hashes[KEY] == {"BTC/USDT": "0.05"}


def test_restart_records_a_buy_that_filled_during_the_crash():
    redis, order = _crash_after_send("BUY")
    kraken = FakeKraken(orders=[{"userref": order["userref"], "status": "closed", "side": "buy", "vol_exec": 0.05}],
                        balances={"XXBT": "0.05"})  # the fill shows up on the account
    bus, orch = _orch(redis, kraken)
    assert orch.holdings == {"BTC/USDT": 0.05}
    assert redis.hashes[KEY] == {"BTC/USDT": "0.05"} and redis.hashes[f"{KEY}:pending"] == {}
    asyncio.run(orch._on_signal(_approved("SELL")))  # and can close it
    assert [(o["side"], o["volume"]) for o in bus.orders()] == [("sell", 0.05)]


def test_restart_removes_a_position_whose_sell_filled_during_the_crash():
    redis, order = _crash_after_send("SELL", holdings={"BTC/USDT": "0.05"})
    kraken = FakeKraken(orders=[{"userref": order["userref"], "status": "closed", "side": "sell", "vol_exec": 0.05}])
    _, orch = _orch(redis, kraken)
    assert orch.holdings == {} and redis.hashes[KEY] == {}


def test_restart_drops_an_order_that_never_reached_kraken():
    redis, _ = _crash_after_send("BUY")
    bus, orch = _orch(redis, FakeKraken(orders=[]))
    assert orch.holdings == {} and orch.pending == {} and redis.hashes[f"{KEY}:pending"] == {}
    asyncio.run(orch._on_signal(_approved("BUY")))
    assert len(bus.orders()) == 1  # free to trade again


def test_restart_keeps_unresolved_orders_pending_and_blocks_the_ticker():
    for kraken in (FakeKraken(error=RuntimeError("Kraken API error: EGeneral:Temporary lockout")),
                   FakeKraken(orders=[])):
        redis, order = _crash_after_send("BUY")
        if not kraken.error:  # still working on the exchange
            kraken.orders = [{"userref": order["userref"], "status": "open", "side": "buy", "vol_exec": 0.0}]
        bus, orch = _orch(redis, kraken)
        assert len(orch.pending) == 1 and len(redis.hashes[f"{KEY}:pending"]) == 1
        asyncio.run(orch._on_signal(_approved("BUY")))
        assert bus.orders() == []  # outcome unknown: don't stack another order on top


def test_dry_run_restart_drops_simulated_pending_orders():
    redis = FakeRedis()
    key = "orchestrator:holdings:dry_run"
    redis.hashes[f"{key}:pending"] = {"abc": json.dumps({"ticker": "BTC/USDT", "side": "buy",
                                                           "volume": 0.05, "userref": 7})}
    _, orch = _orch(redis, exchange=None, key=key)
    assert orch.pending == {} and redis.hashes[f"{key}:pending"] == {}


def test_live_order_not_sent_if_it_cannot_be_saved_first():
    redis = FakeRedis()
    bus, orch = _orch(redis, FakeKraken())
    redis.fail_writes = True
    asyncio.run(orch._on_signal(_approved("BUY")))
    assert bus.orders() == [] and orch.pending == {}


def test_execution_agent_forwards_userref_to_kraken():
    from hybrid.execution_agent import ExecutionAgent

    sent = []

    async def place_order(order):
        sent.append(order)
        return SimpleNamespace(success=True, order_id="O1", status="open", error=None)

    agent = ExecutionAgent(RecordingBus(), HybridConfig(), dry_run=True)
    agent.kraken_spot = SimpleNamespace(place_order=place_order)
    asyncio.run(agent._on_execution_request({"type": "execute_order", "data": {
        "symbol": "BTC/USDT", "side": "buy", "volume": 0.05, "order_type": "market",
        "asset_type": "crypto", "client_order_id": "abc", "userref": 123456789}}))
    assert sent[0].client_order_id == "123456789"  # sent to Kraken as userref


def test_find_orders_by_userref_reads_open_and_closed_orders():
    from hybrid.kraken_executor import KrakenConfig, KrakenExecutor

    calls = []

    async def get_open_orders(userref=None):
        calls.append(("open", userref))
        return {"open": {"O1": {"userref": 42, "status": "open", "vol_exec": "0.0",
                                "descr": {"type": "buy"}}}}

    async def get_closed_orders(start=None, end=None, userref=None):
        calls.append(("closed", userref))
        return {"closed": {"O2": {"userref": 42, "status": "closed", "vol_exec": "0.05000000",
                                  "descr": {"type": "sell"}},
                           "O3": {"userref": 99, "status": "closed", "vol_exec": "1.0",
                                  "descr": {"type": "buy"}}}, "count": 2}

    executor = KrakenExecutor(KrakenConfig(api_key="k", api_secret="cw=="))
    executor.rest = SimpleNamespace(get_open_orders=get_open_orders, get_closed_orders=get_closed_orders)
    found = asyncio.run(executor.find_orders_by_userref(42))
    assert sorted((o["status"], o["side"], o["vol_exec"]) for o in found) == [
        ("closed", "sell", 0.05), ("open", "buy", 0.0)]  # O3 (another userref) excluded
    assert calls == [("open", 42), ("closed", 42)]
