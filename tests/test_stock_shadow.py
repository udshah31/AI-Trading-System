"""Stock shadow research persists separately and cannot reach order routing."""
import asyncio
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from hybrid.config import HybridConfig
from hybrid.stock_shadow import RANKING, SECTORS, SYMBOLS, WATCHLIST, StockShadow, enabled, load_ranking, stock_shadow_loop
from hybrid.storage import StockShadowDecision, StorageService, Signal, Trade
from hybrid.technical_indicators import TechnicalSignals


@pytest.fixture
def storage(tmp_path):
    service = StorageService(f"sqlite+aiosqlite:///{tmp_path / 'shadow.db'}")
    asyncio.run(service.initialize())
    yield service
    asyncio.run(service.close())


def session(day="2026-11-27", close="13:00"):
    return SimpleNamespace(date=date.fromisoformat(day),
                           close=datetime.fromisoformat(f"{day}T{close}:00"))


class Calendar:
    def __init__(self, sessions):
        self.sessions = sessions
        self.requests = []

    def get_calendar(self, request):
        self.requests.append(request)
        return self.sessions


def indicators(symbol, trade_date):
    return TechnicalSignals(ticker=symbol, current_price=500, data_points=100,
                            last_bar_date=trade_date, rsi_score=0.8, ema_crossover_score=0.8,
                            bollinger_score=0.8, volume_score=0.8)


def rows(storage, model=StockShadowDecision):
    async def read():
        async with storage.db.session() as db:
            return list((await db.scalars(select(model))).all())
    return asyncio.run(read())


def test_watchlist_has_ten_ranked_stocks_in_each_sector_plus_etfs():
    stocks = [asset for asset in WATCHLIST if asset["kind"] == "stock"]
    assert len(stocks) == 100
    assert len(SYMBOLS) == len(set(SYMBOLS)) == 102
    assert {asset["symbol"] for asset in WATCHLIST if asset["kind"] == "etf"} == {"SPY", "QQQ"}
    for sector in SECTORS:
        ranked = sorted((asset for asset in stocks if asset["sector"] == sector), key=lambda a: a["rank"])
        assert [asset["rank"] for asset in ranked] == list(range(1, 11))
        caps = [asset["market_cap_usd"] for asset in ranked]
        assert caps == sorted(caps, reverse=True)
        assert all(asset["source_url"].startswith("https://") for asset in ranked)
    assert RANKING["retrieved_at"] and len(RANKING["sources"]) == 10


@pytest.mark.parametrize("invalid", ["missing", "duplicate", "rank", "cap", "source"])
def test_invalid_ranking_snapshot_rejected(tmp_path, invalid):
    import copy
    import json
    data = copy.deepcopy(RANKING)
    if invalid == "missing":
        data["stocks"].pop()
    elif invalid == "duplicate":
        data["stocks"][1]["symbol"] = data["stocks"][0]["symbol"]
    elif invalid == "rank":
        data["stocks"][0]["rank"] = 0
    elif invalid == "cap":
        data["stocks"][0]["market_cap_usd"] = -1
    else:
        data["stocks"][0]["source_url"] = "javascript:alert(1)"
    path = tmp_path / "ranking.json"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        load_ranking(path)


def test_one_failed_symbol_does_not_block_other_sectors(storage):
    def sometimes_bad(symbol, trade_date):
        if symbol == "JNJ":
            raise ConnectionError("offline")
        return indicators(symbol, trade_date)
    service = StockShadow(storage, HybridConfig(), Calendar([session()]), sometimes_bad)
    now = datetime(2026, 11, 27, 22, tzinfo=timezone.utc)
    assert asyncio.run(service.run_once(now)) == len(SYMBOLS) - 1
    assert {row.symbol for row in rows(storage)} == set(SYMBOLS) - {"JNJ"}
    assert rows(storage, Signal) == [] and rows(storage, Trade) == []
    recovered = StockShadow(storage, HybridConfig(), Calendar([session()]), indicators)
    assert asyncio.run(recovered.run_once(now)) == 1
    assert len(rows(storage)) == len(SYMBOLS)


def test_default_disabled_and_explicit_enable(monkeypatch):
    monkeypatch.delenv("STOCK_SHADOW_ENABLED", raising=False)
    assert not enabled()
    monkeypatch.setenv("STOCK_SHADOW_ENABLED", "true")
    assert enabled()


def test_early_close_waits_for_settling_and_persists_once_per_session(storage):
    service = StockShadow(storage, HybridConfig(), Calendar([session()]), indicators)
    # NY is UTC-5 in November: early close 13:00 = 18:00 UTC.
    assert asyncio.run(service.run_once(datetime(2026, 11, 27, 18, 19, tzinfo=timezone.utc))) == 0
    assert asyncio.run(service.run_once(datetime(2026, 11, 27, 18, 20, tzinfo=timezone.utc))) == len(SYMBOLS)
    assert sorted(r.symbol for r in rows(storage)) == sorted(SYMBOLS)
    assert all(r.action == "buy" for r in rows(storage))
    # Restart uses persisted unique session keys, not an in-memory deduplication set.
    restarted = StockShadow(storage, HybridConfig(), Calendar([session()]), indicators)
    assert asyncio.run(restarted.run_once(datetime(2026, 11, 28, 15, tzinfo=timezone.utc))) == 0
    assert len(rows(storage)) == len(SYMBOLS)
    assert rows(storage, Signal) == [] and rows(storage, Trade) == []


def test_database_enforces_unique_symbol_session(storage):
    from sqlalchemy.exc import IntegrityError
    service = StockShadow(storage, HybridConfig(), Calendar([session()]), indicators)
    asyncio.run(service.run_once(datetime(2026, 11, 27, 22, tzinfo=timezone.utc)))
    existing = rows(storage)[0]
    assert existing.features["buy_threshold"] == HybridConfig().buy_threshold
    assert existing.features["sell_threshold"] == HybridConfig().sell_threshold
    assert existing.features["watchlist_retrieved_at"] == RANKING["retrieved_at"]

    async def duplicate():
        async with storage.db.session() as db:
            db.add(StockShadowDecision(
                symbol=existing.symbol, session_date=existing.session_date,
                session_close=existing.session_close, analyzed_at=existing.analyzed_at,
                action=existing.action, score=existing.score, confidence=existing.confidence,
                close_price=existing.close_price, features=existing.features))
    with pytest.raises(IntegrityError):
        asyncio.run(duplicate())
    assert len(rows(storage)) == len(SYMBOLS)


def test_empty_calendar_does_not_run_analysis(storage):
    def forbidden(*args, **kwargs):
        raise AssertionError("No session should mean no analysis")
    service = StockShadow(storage, HybridConfig(), Calendar([]), forbidden)
    assert asyncio.run(service.run_once(datetime(2026, 11, 27, 22, tzinfo=timezone.utc))) == 0
    assert rows(storage) == []


def test_dst_close_conversion(storage):
    service = StockShadow(storage, HybridConfig(), Calendar([session("2026-07-01", "16:00")]), indicators)
    assert asyncio.run(service.run_once(datetime(2026, 7, 1, 20, 19, tzinfo=timezone.utc))) == 0
    assert asyncio.run(service.run_once(datetime(2026, 7, 1, 20, 20, tzinfo=timezone.utc))) == len(SYMBOLS)


def test_holiday_and_weekend_choose_last_completed_session(storage):
    calendar = Calendar([session("2026-11-25", "16:00")])
    service = StockShadow(storage, HybridConfig(), calendar, indicators)
    assert asyncio.run(service.run_once(datetime(2026, 11, 26, 22, tzinfo=timezone.utc))) == len(SYMBOLS)
    assert {r.session_date for r in rows(storage)} == {date(2026, 11, 25)}


@pytest.mark.parametrize("field,value", [("last_bar_date", "2026-11-25"),
                                        ("current_price", float("nan")), ("current_price", 0),
                                        ("rsi_score", float("inf")), ("volume_score", 2),
                                        ("data_points", 0)])
def test_missing_stale_invalid_data_not_recorded(storage, field, value):
    def bad(symbol, trade_date):
        result = indicators(symbol, trade_date)
        setattr(result, field, value)
        return result
    service = StockShadow(storage, HybridConfig(), Calendar([session()]), bad)
    assert asyncio.run(service.run_once(datetime(2026, 11, 27, 22, tzinfo=timezone.utc))) == 0
    assert rows(storage) == []


def test_calendar_failure_retries_without_running_indicators(storage):
    class Offline:
        def get_calendar(self, request):
            raise ConnectionError("offline")
    calls = []
    def should_not_run(*args, **kwargs):
        calls.append(1)
        raise AssertionError("no calendar, no analysis")
    service = StockShadow(storage, HybridConfig(), Offline(), should_not_run)
    class Stop(Exception):
        pass
    async def stop_sleep(seconds):
        assert seconds == 300
        raise Stop()
    with pytest.raises(Stop):
        asyncio.run(stock_shadow_loop(service, sleep=stop_sleep))
    assert calls == [] and rows(storage) == []


def test_actual_indicator_path_records_final_bar_and_excludes_future_data(monkeypatch):
    import pandas as pd
    import hybrid.technical_indicators as module
    index = pd.date_range("2026-09-01", periods=100)
    frame = pd.DataFrame({"Open": 100.0, "High": 102.0, "Low": 98.0,
                          "Close": 100.0, "Volume": 10000.0}, index=index)
    frame.loc[frame.index > "2026-11-27", "Close"] = 9999.0
    monkeypatch.setattr(module.yf, "download", lambda *args, **kwargs: frame)
    result = module.compute_technical_signals("SPY", trade_date="2026-11-27")
    assert result.last_bar_date == "2026-11-27" and result.current_price == 100.0
    assert result.data_points == len(frame.loc[:"2026-11-27"])


def test_dashboard_shadow_is_authenticated_and_separate_from_crypto(storage, monkeypatch):
    from hybrid.dashboard import app as dashboard
    service = StockShadow(storage, HybridConfig(), Calendar([session()]), indicators)
    asyncio.run(service.run_once(datetime(2026, 11, 27, 22, tzinfo=timezone.utc)))
    # More than 60 newer SPY rows must not hide other sectors' older snapshots.
    async def add_newer_spy_rows():
        async with storage.db.session() as db:
            for offset in range(1, 71):
                db.add(StockShadowDecision(
                    symbol="SPY", session_date=date(2026, 11, 27) + timedelta(days=offset),
                    session_close=datetime(2026, 11, 27, 18, tzinfo=timezone.utc),
                    analyzed_at=datetime(2026, 11, 27, 22, tzinfo=timezone.utc),
                    action="hold", score=0.5, confidence=0, close_price=500, features={}))
    asyncio.run(add_newer_spy_rows())
    monkeypatch.setattr(dashboard, "storage", storage)
    monkeypatch.setenv("DASHBOARD_PASSWORD", "pw")
    monkeypatch.setenv("STOCK_SHADOW_ENABLED", "true")
    client = TestClient(dashboard.app)
    assert client.get("/api/stocks/shadow").status_code == 401
    result = client.get("/api/stocks/shadow", auth=("admin", "pw")).json()
    assert result["mode"] == "shadow" and result["orders_enabled"] is False
    assert len(result["decisions"]) == len(SYMBOLS)
    assert result["watchlist"] == list(WATCHLIST)
    assert result["sectors"] == list(SECTORS)
    assert result["ranking"]["retrieved_at"] == RANKING["retrieved_at"]
    assert {row["symbol"] for row in result["decisions"]} == set(SYMBOLS)
    by_symbol = {row["symbol"]: row for row in result["decisions"]}
    assert by_symbol["JNJ"]["session_date"] == "2026-11-27"
    assert by_symbol["SPY"]["session_date"] == (date(2026, 11, 27) + timedelta(days=70)).isoformat()
    assert client.get("/api/decision-band", auth=("admin", "pw")).json()["coins"] == []
