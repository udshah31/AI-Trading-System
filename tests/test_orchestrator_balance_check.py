"""Live mode: the orchestrator checks Kraken spot balances before each order, so it never
stacks on an existing holding (e.g. after a restart) and never sells coins it didn't buy."""
import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from hybrid.config import HybridConfig
from hybrid.orchestrator import Orchestrator, kraken_balance_codes


class RecordingBus:
    def __init__(self):
        self.published = []

    def subscribe(self, channel, handler):
        pass

    def publish(self, channel, payload, source):
        self.published.append(payload)

    def orders(self):
        return [p["data"] for p in self.published if p.get("type") == "execute_order"]


class FakeKraken:
    def __init__(self, balances=None, error=None):
        self.balances = {k: Decimal(v) for k, v in (balances or {}).items()}
        self.error = error
        self.calls = 0

    async def get_balances(self):
        self.calls += 1
        if self.error:
            raise self.error
        return {k: SimpleNamespace(asset=k, free=v, used=Decimal("0"), total=v)
                for k, v in self.balances.items()}


def _approved(action, ticker="BTC/USDT", shares=0.05, price=84000.0):
    return {"type": "risk_assessment", "data": {
        "ticker": ticker, "action": action, "approved": True, "shares": shares,
        "price": price, "stop_loss": price * 0.95, "rejection_reason": ""}}


def _live(balances=None, error=None):
    bus, kraken = RecordingBus(), FakeKraken(balances, error)
    return bus, kraken, Orchestrator(bus, HybridConfig(), exchange=kraken)


def _fill(orch, bus, success=True):
    order = bus.orders()[-1]
    asyncio.run(orch._on_order({"type": "execution_result", "data": {
        "success": success, "symbol": order["symbol"], "message": "",
        "client_order_id": order["client_order_id"]}}))


@pytest.mark.parametrize("pair,codes", [
    ("BTC/USDT", {"XBT", "XXBT"}), ("XBT/USD", {"XBT", "XXBT"}),
    ("ETH/USDT", {"ETH", "XETH"}), ("SOL/USDT", {"SOL", "XSOL"}),
])
def test_kraken_balance_codes(pair, codes):
    assert kraken_balance_codes(pair) == codes


def test_buy_skipped_when_account_already_holds_coin():
    bus, kraken, orch = _live({"XXBT": "0.5", "ZUSD": "1000"})  # e.g. bought before a restart
    asyncio.run(orch._on_signal(_approved("BUY")))
    assert bus.orders() == [] and kraken.calls == 1


def test_buy_allowed_over_dust_and_earn_balances():
    bus, _, orch = _live({"XXBT": "0.00001", "XBT.F": "2.0"})  # ~$0.84 spot dust; earn isn't spot
    asyncio.run(orch._on_signal(_approved("BUY")))
    assert [(o["side"], o["volume"]) for o in bus.orders()] == [("buy", 0.05)]


def test_no_order_when_balance_check_fails():
    bus, _, orch = _live(error=RuntimeError("Kraken API error: EAPI:Invalid nonce"))
    asyncio.run(orch._on_signal(_approved("BUY")))
    assert bus.orders() == []


def test_never_sells_coins_the_bot_did_not_buy():
    bus, _, orch = _live({"XXBT": "1.0"})
    asyncio.run(orch._on_signal(_approved("SELL")))
    assert bus.orders() == []


def test_sell_capped_at_actual_balance():
    bus, kraken, orch = _live({})
    asyncio.run(orch._on_signal(_approved("BUY", shares=0.05)))
    _fill(orch, bus)
    kraken.balances = {"XXBT": Decimal("0.03")}  # part sold by hand since
    asyncio.run(orch._on_signal(_approved("SELL")))
    assert (bus.orders()[-1]["side"], bus.orders()[-1]["volume"]) == ("sell", 0.03)


def test_position_dropped_when_coin_already_gone():
    bus, kraken, orch = _live({})
    asyncio.run(orch._on_signal(_approved("BUY")))
    _fill(orch, bus)
    kraken.balances = {}
    asyncio.run(orch._on_signal(_approved("SELL")))
    assert len(bus.orders()) == 1 and orch.holdings == {}


def test_dry_run_skips_exchange_checks():
    bus = RecordingBus()
    orch = Orchestrator(bus, HybridConfig())  # no exchange: simulated positions aren't on Kraken
    asyncio.run(orch._on_signal(_approved("BUY")))
    assert len(bus.orders()) == 1


def test_get_balances_drops_assets_no_longer_held():
    from hybrid.kraken_executor import KrakenConfig, KrakenExecutor

    responses = [{"XXBT": Decimal("0.5"), "ZUSD": Decimal("10")}, {"ZUSD": Decimal("42000")}]

    async def get_account_balance():
        return responses.pop(0)

    executor = KrakenExecutor(KrakenConfig(api_key="k", api_secret="cw=="))
    executor.rest = SimpleNamespace(get_account_balance=get_account_balance)
    assert "XXBT" in asyncio.run(executor.get_balances())
    assert set(asyncio.run(executor.get_balances())) == {"ZUSD"}  # BTC sold: gone, not stale
