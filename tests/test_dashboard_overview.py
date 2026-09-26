"""/api/overview serializes the ORM rows it loads (trades, signals, equity curve)."""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from hybrid.dashboard import app as dashboard
from hybrid.storage import StorageService

AUTH = ("admin", "s3cret")


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_USER", AUTH[0])
    monkeypatch.setenv("DASHBOARD_PASSWORD", AUTH[1])
    svc = StorageService(f"sqlite+aiosqlite:///{tmp_path / 'overview.db'}")
    asyncio.run(svc.initialize())
    now = datetime.now(timezone.utc)

    async def seed():
        await svc.record_trade(timestamp=now, symbol="BTC/USDT", side="sell", volume=0.00049875,
                               price=84229.4, pnl=-0.11, strategy="quant", exchange="alpaca_paper",
                               status="filled")
        await svc.record_signal(timestamp=now, symbol="BTC/USDT", agent="quant", action="hold",
                                strength=0.4, confidence=0.6)
        await svc.record_equity(timestamp=now - timedelta(minutes=5), account="alpaca_paper",
                                equity=99999.79, drawdown_pct=0)

    asyncio.run(seed())
    monkeypatch.setattr(dashboard, "storage", svc)
    # no `with`: lifespan would replace storage; Redis is absent so agents come back empty
    yield TestClient(dashboard.app, raise_server_exceptions=False)
    asyncio.run(svc.close())


def test_overview_returns_recorded_rows(client):
    resp = client.get("/api/overview", auth=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [t["side"] for t in body["recent_trades"]] == ["sell"]
    assert [s["action"] for s in body["recent_signals"]] == ["hold"]
    assert [p["equity"] for p in body["equity_curve"]] == [99999.79]
    assert body["metrics"]["total_equity"] == 99999.79
