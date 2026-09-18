# tests/test_p0_fixes.py
def test_learned_weights_applied(tmp_path, monkeypatch):
    from hybrid.config import HybridConfig
    import json
    # learned_weights.json has tech_weight_rsi=0.16 etc.; config currently creates new attrs
    cfg = HybridConfig()
    # after fix, weight_rsi should reflect learned value, not default 0.10
    assert cfg.weight_rsi != 0.10 or cfg.weight_ema_crossover != 0.08  # fails now


def test_storage_pg_only(monkeypatch, tmp_path):
    import pathlib
    text = pathlib.Path("hybrid/storage.py").read_text()
    # guard check: OrderRepository must be above StorageService and sqlite guard present
    assert text.index("class OrderRepository") < text.index("class StorageService"), "OrderRepository must be defined before StorageService"
    assert "sqlite" in text.lower(), "sqlite guard missing for PG types"
    # runtime: sqlite compat or clear error mentioning sqlite/PostgreSQL
    from hybrid.storage import StorageService
    import asyncio
    db_path = tmp_path / "test.db"
    svc = StorageService(f"sqlite+aiosqlite:///{db_path}")
    try:
        asyncio.run(svc.initialize())
        asyncio.run(svc.close())
    except Exception as e:
        assert "sqlite" in str(e).lower() or "postgresql" in str(e).lower()


def test_jupiter_url():
    import pathlib
    text = pathlib.Path("hybrid/dex_executor.py").read_text()
    assert "lite-api.jup.ag" in text  # fails now, still has quote-api.jup.ag


def test_uniswap_deadline_uses_epoch():
    import pathlib
    text = pathlib.Path("hybrid/dex_executor.py").read_text()
    assert "get_event_loop().time()" not in text
    assert "time.time()" in text


def test_funding_not_hardcoded():
    import pathlib
    text = pathlib.Path("hybrid/strategies/btc_funding.py").read_text()
    # _get_funding_rate should not return literal 0.0001 without API call
    assert text.count("0.0001") <= 1  # only in comment/test, not in return
    assert "get_funding" in text or "funding_rate" in text.lower()
