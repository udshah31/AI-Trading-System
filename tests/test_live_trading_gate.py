"""Regression tests: paper mode never sends real orders, and agent updates reach the dashboard."""
import ast
import asyncio
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from hybrid.messaging import Channel


class RecordingBus:
    def __init__(self):
        self.published = []

    def subscribe(self, channel, handler):
        pass

    def publish(self, channel, payload, source):
        json.dumps(payload)  # MessageBus.publish serializes; Decimals etc. would fail here
        self.published.append((channel, payload, source))


class ExplodingRest:
    """Any attribute access means the executor tried to talk to Kraken."""

    def __getattr__(self, name):
        raise AssertionError(f"dry-run executor called Kraken REST: {name}")


# ── #1: live-trading gate ──

def test_kraken_config_is_dry_run_unless_live_trading(monkeypatch):
    from hybrid.kraken_executor import KrakenConfig

    monkeypatch.delenv("LIVE_TRADING", raising=False)
    assert KrakenConfig(api_key="k", api_secret="s").dry_run is True

    monkeypatch.setenv("LIVE_TRADING", "true")
    assert KrakenConfig(api_key="k", api_secret="s").dry_run is False


@pytest.mark.parametrize("environment", ["SPOT", "FUTURES"])
def test_dry_run_orders_never_hit_kraken(monkeypatch, environment):
    from hybrid.kraken_executor import KrakenConfig, KrakenEnvironment, KrakenExecutor, OrderRequest

    monkeypatch.delenv("LIVE_TRADING", raising=False)
    executor = KrakenExecutor(KrakenConfig(
        api_key="k", api_secret="s", environment=KrakenEnvironment[environment]))
    executor.rest = ExplodingRest()

    order = OrderRequest(symbol="XBT/USD", side="buy", order_type="market", volume=Decimal("0.1"))
    result = asyncio.run(executor.place_order(order))

    assert result.success
    assert result.order_id.startswith("DRYRUN-")
    assert result.filled_volume == Decimal("0.1")
    assert asyncio.run(executor.cancel_order(result.order_id)).success
    assert asyncio.run(executor.cancel_all_orders())["dry_run"] is True


def test_spot_crypto_orders_never_route_to_futures():
    from hybrid.config import HybridConfig
    from hybrid.execution_agent import ExecutionAgent

    agent = ExecutionAgent(RecordingBus(), HybridConfig(), dry_run=True)
    agent.kraken_spot, agent.kraken_futures = object(), object()
    assert agent._route_exchange("BTC/USD", "auto") == "kraken_spot"
    assert agent._route_exchange("BTC-PERP", "auto") == "kraken_futures"

    agent.kraken_spot = None
    assert agent._route_exchange("BTC/USD", "auto") is None


def test_btc_funding_requires_separate_futures_client_and_honours_limits():
    from hybrid.config import HybridConfig
    from hybrid.strategies.btc_funding import create_btc_funding_strategy

    spot, futures = object(), object()
    for bad_futures in (None, spot):
        with pytest.raises(ValueError):
            asyncio.run(create_btc_funding_strategy(RecordingBus(), HybridConfig(), spot, bad_futures))

    strategy = asyncio.run(create_btc_funding_strategy(
        RecordingBus(), HybridConfig(), spot, futures, max_position_usd=5000.0))
    assert strategy.max_position_usd == Decimal("5000.0")
    assert strategy.running is False  # caller starts it, so only one monitor loop runs


def test_sniper_does_not_swap_without_live_trading(monkeypatch):
    from hybrid.dex_executor import Chain
    from hybrid.sniper_bot import SniperBot, TokenInfo

    class NoSwapEngine:
        active_positions = {}

        async def execute_on_signal(self, token):
            raise AssertionError("sniper sent a swap in dry-run mode")

    async def no_alert(*args, **kwargs):
        pass

    monkeypatch.delenv("LIVE_TRADING", raising=False)
    bot = SniperBot.__new__(SniperBot)
    bot.sniper_engine = NoSwapEngine()
    bot.executor = type("Exec", (), {"executors": {Chain.SOLANA: object()}})()
    bot.max_position_usd = 100
    bot._emit_sniper_alert = no_alert

    token = TokenInfo(address="addr", symbol="NEW", name="New", chain="solana",
                      liquidity_usd=50_000.0, volume_24h=10_000.0, price_usd=0.01, age_minutes=5,
                      created_at=datetime.now(), dex="raydium", pair_address="pair")
    asyncio.run(bot._analyze_and_snipe(token))


# ── #3: agent updates reach the dashboard ──

def test_no_string_channel_publishes():
    offenders = []
    for path in Path("hybrid").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "publish" and node.args
                    and isinstance(node.args[0], ast.Constant)):
                offenders.append(f"{path}:{node.lineno}")
    assert not offenders, f"publish() takes a Channel enum: {offenders}"


def test_btc_funding_strategy_update_is_published():
    from hybrid.config import HybridConfig
    from hybrid.strategies.btc_funding import BTCFundingStrategy

    bus = RecordingBus()
    strategy = BTCFundingStrategy(bus, HybridConfig(), object(), object())

    async def funding_data():
        return {"spot_price": Decimal("60000"), "perp_price": Decimal("60030"),
                "basis_bps": Decimal("5"), "funding_rate": Decimal("0.0001")}

    strategy._get_funding_data = funding_data
    asyncio.run(strategy._emit_strategy_update())

    [(channel, payload, _)] = bus.published
    assert channel is Channel.SIGNALS
    assert payload["type"] == "strategy_update"
    assert payload["data"]["funding_rate_bps"] == 1.0


def test_dashboard_relays_agent_updates():
    from hybrid.dashboard.app import relay_message

    envelope = {"channel": "signals", "timestamp": 0, "source": "risk_agent",
                "payload": {"type": "risk_update", "data": {"drawdown_pct": 1.5}}}
    assert relay_message(json.dumps(envelope)) == {"type": "risk_update", "data": {"drawdown_pct": 1.5}}

    envelope["payload"]["type"] = "quant_decision"  # internal traffic stays off the websocket
    assert relay_message(json.dumps(envelope)) is None
    assert relay_message("not json") is None


# ── #5: containers reach Redis via REDIS_URL ──

def test_trading_system_uses_redis_url(monkeypatch):
    import importlib

    import hybrid.main_async as main_async

    monkeypatch.setenv("REDIS_URL", "redis://redis:6379")
    main_async = importlib.reload(main_async)
    try:
        system = main_async.PaperTradingSystem()
        assert system.bus.client.connection_pool.connection_kwargs["host"] == "redis"
    finally:
        monkeypatch.delenv("REDIS_URL")
        importlib.reload(main_async)
