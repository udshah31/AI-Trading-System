"""Learning loop: decisions get their outcome (next-day return) recorded, a daily re-fit
proposes small weight changes only when they beat the current weights on held-out data,
and nothing is applied until a person approves it."""
import asyncio
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from hybrid.config import HybridConfig
from hybrid.learning import (
    MAX_STEP,
    MIN_SAMPLES,
    daily_samples,
    information_coefficient,
    label_outcomes,
    propose_weights,
    run_learning_cycle,
)
from hybrid.storage import StorageService

EQUAL = {"rsi": 0.25, "ema": 0.25, "bollinger": 0.25, "volume": 0.25}
T0 = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)


def _sample(day, ema, noise=0.0, symbol="BTC/USDT", rng=None):
    rng = rng or random.Random(day)
    scores = {"rsi": rng.random(), "ema": ema, "bollinger": rng.random(), "volume": rng.random()}
    # tomorrow's move follows the EMA score; the other signals are noise
    return {"symbol": symbol, "timestamp": T0 + timedelta(days=day), "scores": scores,
            "forward_return": (ema - 0.5) * 0.04 + noise}


def _ema_driven(n, seed=7):
    rng = random.Random(seed)
    return [_sample(d, rng.random(), rng.gauss(0, 0.004), rng=rng) for d in range(n)]


# ── config ──

def test_tech_weights_round_trip_through_config():
    cfg = HybridConfig()
    cfg.set_tech_weights({"rsi": 0.1, "ema": 0.5, "bollinger": 0.2, "volume": 0.2})
    assert cfg.tech_weights() == pytest.approx({"rsi": 0.1, "ema": 0.5, "bollinger": 0.2, "volume": 0.2})
    assert cfg.weight_ema_crossover == pytest.approx(0.125)  # 0.5 of the 0.25 technical share
    cfg.validate()


# ── samples and scoring ──

def test_one_sample_per_coin_per_day_keeps_the_first():
    rows = [
        {"symbol": "BTC/USDT", "timestamp": T0 + timedelta(minutes=m), "scores": EQUAL, "forward_return": r}
        for m, r in ((0, 0.01), (5, 0.99), (10, 0.99))
    ] + [{"symbol": "ETH/USDT", "timestamp": T0, "scores": EQUAL, "forward_return": 0.02}]
    kept = daily_samples(rows)
    assert sorted((s["symbol"], s["forward_return"]) for s in kept) == [("BTC/USDT", 0.01), ("ETH/USDT", 0.02)]


def test_information_coefficient():
    samples = _ema_driven(80)
    assert information_coefficient({"rsi": 0, "ema": 1, "bollinger": 0, "volume": 0}, samples) > 0.8
    assert information_coefficient(EQUAL, samples[:1]) is None


# ── proposals ──

def test_learns_toward_the_predictive_signal_within_the_step_limit():
    result = propose_weights(EQUAL, _ema_driven(90))
    assert result.accepted, result.reason
    assert result.weights["ema"] > EQUAL["ema"]
    assert all(abs(result.weights[k] - EQUAL[k]) <= MAX_STEP + 1e-9 for k in EQUAL)
    assert sum(result.weights.values()) == pytest.approx(1.0) and min(result.weights.values()) >= 0
    assert result.metrics["test_ic_proposed"] > result.metrics["test_ic_current"]
    assert result.metrics["n_test"] >= 20


def test_no_proposal_without_enough_data():
    result = propose_weights(EQUAL, _ema_driven(MIN_SAMPLES - 1))
    assert not result.accepted and "60" in result.reason


def test_no_proposal_when_nothing_predicts_returns():
    rng = random.Random(3)
    noise = [{"symbol": "BTC/USDT", "timestamp": T0 + timedelta(days=d),
              "scores": {k: rng.random() for k in EQUAL}, "forward_return": rng.gauss(0, 0.02)} for d in range(90)]
    result = propose_weights(EQUAL, noise)
    assert not result.accepted


# ── storage, outcome labelling, full cycle ──

@pytest.fixture
def storage(tmp_path):
    svc = StorageService(f"sqlite+aiosqlite:///{tmp_path / 'learn.db'}")
    asyncio.run(svc.initialize())
    yield svc
    asyncio.run(svc.close())


def _record_decision(storage, when, ema, price=100.0, symbol="BTC/USDT", others=(0.5, 0.5, 0.5)):
    features = {"rsi_score": others[0], "ema_crossover_score": ema, "bollinger_score": others[1],
                "volume_score": others[2], "current_price": price}
    asyncio.run(storage.record_signal(timestamp=when, symbol=symbol, agent="quant_agent", signal_type="quant",
                                      action="hold", strength=Decimal("0.5"), confidence=Decimal("0.1"),
                                      features=features))


def test_outcomes_labelled_from_the_price_a_day_later(storage):
    now = T0 + timedelta(days=3)
    _record_decision(storage, T0, ema=0.8, price=100.0)
    _record_decision(storage, now - timedelta(hours=2), ema=0.8)   # too recent to label
    requested = []

    def prices(symbol, start, end):
        requested.append(symbol)
        return [(T0, 100.0), (T0 + timedelta(hours=23), 101.0), (T0 + timedelta(hours=24, minutes=30), 103.0)]

    assert asyncio.run(label_outcomes(storage, prices, now=now)) == 1
    [sample] = asyncio.run(storage.learning.labelled_samples())
    assert sample["forward_return"] == pytest.approx(0.03)  # entry bar 100 -> first bar at or after +24h, 103
    assert sample["scores"]["ema"] == 0.8
    assert requested == ["BTC-USD"]  # exchange pair mapped to the price-data symbol
    assert asyncio.run(label_outcomes(storage, prices, now=now)) == 0  # already labelled


def _seed_history(storage, days=90):
    rng = random.Random(11)
    closes = {}
    for d in range(days):
        ema = rng.random()
        when = T0 + timedelta(days=2 * d)  # 2 days apart: an exit bar never doubles as the next entry
        _record_decision(storage, when, ema=ema, price=100.0, others=(rng.random(), rng.random(), rng.random()))
        closes[when] = 100.0
        closes[when + timedelta(hours=24)] = 100.0 * (1 + (ema - 0.5) * 0.04 + rng.gauss(0, 0.004))
    return lambda symbol, start, end: sorted(closes.items())


def test_cycle_creates_a_pending_proposal_and_nothing_changes_until_approved(storage):
    prices = _seed_history(storage)
    cfg = HybridConfig()
    cfg.set_tech_weights(EQUAL)
    result = asyncio.run(run_learning_cycle(storage, cfg, prices, now=T0 + timedelta(days=185)))
    assert result.accepted
    assert cfg.tech_weights() == pytest.approx(EQUAL)  # not applied by the cycle itself

    [proposal] = asyncio.run(storage.learning.list_proposals())
    assert proposal.status == "pending"
    assert asyncio.run(storage.learning.active_weights()) is None
    asyncio.run(storage.learning.set_status(proposal.id, "approved"))
    assert asyncio.run(storage.learning.active_weights()) == pytest.approx(proposal.proposed)


def test_new_proposal_supersedes_the_pending_one(storage):
    for _ in range(2):
        asyncio.run(storage.learning.save_proposal(EQUAL, {**EQUAL, "ema": 0.3, "rsi": 0.2}, {"n": 1}))
    statuses = sorted(p.status for p in asyncio.run(storage.learning.list_proposals()))
    assert statuses == ["pending", "superseded"]


def test_trading_system_applies_approved_weights(storage):
    import hybrid.main_async as main_async
    pid = asyncio.run(storage.learning.save_proposal(EQUAL, {"rsi": 0.2, "ema": 0.4, "bollinger": 0.2, "volume": 0.2}, {}))
    asyncio.run(storage.learning.set_status(pid, "approved"))
    system = main_async.PaperTradingSystem()
    system.storage = storage
    asyncio.run(system._apply_active_weights())
    assert system.config.tech_weights()["ema"] == pytest.approx(0.4)


# ── API ──

@pytest.fixture
def http(storage, monkeypatch):
    from hybrid.dashboard import app as dashboard
    monkeypatch.setenv("DASHBOARD_PASSWORD", "pw")
    monkeypatch.setattr(dashboard, "storage", storage)
    return TestClient(dashboard.app, raise_server_exceptions=False), ("admin", "pw")


def test_api_shows_progress_and_approves(http, storage):
    client, auth = http
    state = client.get("/api/learning", auth=auth).json()
    assert state["active"]["source"] == "starting"
    assert state["progress"] == {"labelled_days": 0, "needed": MIN_SAMPLES}
    assert state["pending"] is None

    pid = asyncio.run(storage.learning.save_proposal(EQUAL, {**EQUAL, "ema": 0.3, "rsi": 0.2},
                                                     {"n_train": 70, "n_test": 30, "test_ic_current": 0.05,
                                                      "test_ic_proposed": 0.12}))
    assert client.get("/api/learning", auth=auth).json()["pending"]["id"] == pid
    assert client.post(f"/api/learning/proposals/{pid}/approve", auth=auth).status_code == 200
    state = client.get("/api/learning", auth=auth).json()
    assert state["active"]["source"] == "approved" and state["active"]["weights"]["ema"] == 0.3
    assert client.post(f"/api/learning/proposals/{pid}/reject", auth=auth).status_code == 409  # not pending
    assert client.post("/api/learning/proposals/missing/approve", auth=auth).status_code == 404
