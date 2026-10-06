"""Dashboard exposure: routing, auth on every endpoint but /health, and no wildcard CORS."""
import asyncio

import pytest
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient
from starlette.routing import Match
from starlette.websockets import WebSocketDisconnect

from hybrid.dashboard import app as dashboard

AUTH = ("admin", "s3cret")


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("DASHBOARD_USER", AUTH[0])
    monkeypatch.setenv("DASHBOARD_PASSWORD", AUTH[1])
    # no `with`: lifespan (Postgres/Redis) isn't needed to exercise the middleware
    return TestClient(dashboard.app, raise_server_exceptions=False)


def _first_route(path):
    scope = {"type": "http", "path": path, "method": "GET"}
    for route in dashboard.app.router.routes:
        match, _ = route.matches(scope)
        if match is Match.FULL:
            return route.endpoint.__name__


def test_equity_history_is_not_shadowed_by_account_route():
    assert _first_route("/api/equity/history") == "get_equity_history"
    assert _first_route("/api/equity/default") == "get_equity"


def test_health_is_public(client):
    assert client.get("/health").status_code == 200


@pytest.mark.parametrize("path", ["/", "/metrics", "/api/risk", "/api/equity/history"])
def test_endpoints_require_credentials(client, path):
    for auth in (None, ("admin", "wrong"), ("intruder", AUTH[1])):
        resp = client.get(path, auth=auth)
        assert resp.status_code == 401, (path, auth)
        assert resp.headers["www-authenticate"].startswith("Basic")


def test_valid_credentials_pass_auth(client):
    assert client.get("/api/risk", auth=AUTH).status_code != 401


def test_risk_failure_is_not_reported_as_success(client, monkeypatch):
    monkeypatch.setattr(dashboard, "storage", None)
    response = client.get("/api/risk", auth=AUTH)
    assert response.status_code == 503
    assert response.json()["detail"] == "Risk data unavailable"


def test_websocket_rejected_without_credentials(client):
    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/ws"):
            pass
    assert exc.value.code == 1008


def test_websocket_accepted_with_credentials(monkeypatch):
    monkeypatch.setenv("DASHBOARD_USER", AUTH[0])
    monkeypatch.setenv("DASHBOARD_PASSWORD", AUTH[1])
    inner = FastAPI()

    @inner.websocket("/ws")
    async def ws(websocket: WebSocket):
        await websocket.accept()
        await websocket.send_text("ok")
        await websocket.close()

    client = TestClient(dashboard.BasicAuthMiddleware(inner))
    headers = {"Authorization": "Basic YWRtaW46czNjcmV0"}  # admin:s3cret
    with client.websocket_connect("/ws", headers=headers) as ws:
        assert ws.receive_text() == "ok"


def test_fails_closed_without_password(monkeypatch):
    monkeypatch.delenv("DASHBOARD_PASSWORD", raising=False)
    client = TestClient(dashboard.app, raise_server_exceptions=False)
    assert client.get("/api/risk", auth=("admin", "")).status_code == 503
    assert client.get("/health").status_code == 200
    with pytest.raises(RuntimeError, match="DASHBOARD_PASSWORD"):
        asyncio.run(dashboard.lifespan(dashboard.app).__aenter__())


def test_no_wildcard_cors(client):
    resp = client.get("/health", headers={"Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in resp.headers
