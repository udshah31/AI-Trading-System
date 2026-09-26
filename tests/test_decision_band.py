"""Decision band (dashboard hero): each coin's latest quant score against the buy/sell
thresholds, plus whether the bot trades real money and when it checks next."""
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from hybrid.storage import StorageService


class FakeRedis:
    def __init__(self, values):
        self.values = values

    async def get(self, key):
        return self.values.get(key)


@pytest.fixture
def storage(tmp_path):
    svc = StorageService(f"sqlite+aiosqlite:///{tmp_path / 'band.db'}")
    asyncio.run(svc.initialize())
    yield svc
    asyncio.run(svc.close())


def _quant(storage, symbol, score, action, minutes_ago):
    asyncio.run(storage.record_signal(
        timestamp=datetime.now(timezone.utc) - timedelta(minutes=minutes_ago), symbol=symbol,
        agent="quant_agent", signal_type="quant", action=action,
        strength=Decimal(str(score)), confidence=Decimal("0.2")))


@pytest.fixture
def http(storage, monkeypatch):
    from hybrid.dashboard import app as dashboard
    monkeypatch.setenv("DASHBOARD_PASSWORD", "pw")
    monkeypatch.setattr(dashboard, "storage", storage)
    return dashboard, TestClient(dashboard.app, raise_server_exceptions=False)


def test_band_shows_latest_score_per_coin_and_thresholds(http, storage, monkeypatch):
    dashboard, client = http
    _quant(storage, "BTC/USDT", 0.30, "sell", minutes_ago=20)
    _quant(storage, "BTC/USDT", 0.49, "hold", minutes_ago=5)   # newer wins
    _quant(storage, "ETH/USDT", 0.66, "buy", minutes_ago=5)
    last = datetime.now(timezone.utc).timestamp() - 60
    monkeypatch.setattr(dashboard, "redis_client", FakeRedis({
        "system:mode": "dry_run", "system:last_analysis": str(last), "system:analysis_interval": "300"}))

    band = client.get("/api/decision-band", auth=("admin", "pw")).json()
    assert (band["sell_threshold"], band["buy_threshold"]) == (0.35, 0.65)
    assert band["mode"] == "dry_run"
    assert band["last_analysis"] == pytest.approx(last)
    assert band["next_analysis"] == pytest.approx(last + 300)
    assert [(c["symbol"], c["score"], c["action"]) for c in band["coins"]] == [
        ("BTC/USDT", 0.49, "hold"), ("ETH/USDT", 0.66, "buy")]


def test_band_without_redis_status(http, monkeypatch):
    dashboard, client = http
    monkeypatch.setattr(dashboard, "redis_client", FakeRedis({}))
    band = client.get("/api/decision-band", auth=("admin", "pw")).json()
    assert band["mode"] == "unknown" and band["coins"] == [] and band["next_analysis"] is None


def test_quant_decisions_are_relayed_live():
    from hybrid.dashboard.app import relay_message
    import json
    msg = relay_message(json.dumps({"payload": {"type": "quant_decision",
                                                "data": {"ticker": "SOL/USDT", "action": "BUY", "score": 0.7}}}))
    assert msg == {"type": "quant_decision", "data": {"ticker": "SOL/USDT", "action": "BUY", "score": 0.7}}


def test_trading_system_publishes_mode_and_schedule(monkeypatch):
    import hybrid.main_async as main_async

    class Recorder:
        def __init__(self):
            self.values = {}

        async def set(self, key, value):
            self.values[key] = value

    monkeypatch.delenv("LIVE_TRADING", raising=False)
    system = main_async.PaperTradingSystem()
    system.bus.client = Recorder()
    asyncio.run(system._publish_status())
    assert system.bus.client.values["system:mode"] == "dry_run"
    assert system.bus.client.values["system:analysis_interval"] == str(main_async.ANALYSIS_INTERVAL_S)

    monkeypatch.setenv("LIVE_TRADING", "true")
    asyncio.run(system._publish_status())
    assert system.bus.client.values["system:mode"] == "live"
