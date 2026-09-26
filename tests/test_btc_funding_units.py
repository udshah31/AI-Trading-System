"""BTC funding strategy: Kraken's absolute hourly funding rate is converted to the
relative 8h rate the thresholds, APR and accrual math are written in."""
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from hybrid.config import HybridConfig
from hybrid.strategies.btc_funding import (
    BTCFundingStrategy,
    PositionState,
    funding_apr_pct,
    relative_funding_rate_8h,
)

# Captured from GET https://futures.kraken.com/derivatives/api/v3/tickers (2026-09-26).
# Kraken's historical funding endpoint reported relativeFundingRate 1.98329875e-05 for
# the same hour, i.e. fundingRate * price with price ~84,000.
KRAKEN_TICKERS = {
    "result": "success",
    "tickers": [
        {"symbol": "PF_XBTUSD", "last": 84040.0, "markPrice": 84110.0, "fundingRate": 0.0123},
        {"symbol": "PI_XBTUSD", "tag": "perpetual", "last": 84036.5,
         "markPrice": 84111.6779649108, "fundingRate": 2.36106713e-10},
    ],
}


def _strategy(tickers=KRAKEN_TICKERS, spot_last="84000"):
    async def get_ticker(symbol):
        return SimpleNamespace(last=Decimal(spot_last))

    async def get_futures_tickers():
        return tickers

    spot = SimpleNamespace(get_ticker=get_ticker)
    futures = SimpleNamespace(rest=SimpleNamespace(get_futures_tickers=get_futures_tickers))
    bus = SimpleNamespace(subscribe=lambda *a: None, publish=lambda *a: None)
    return BTCFundingStrategy(bus, HybridConfig(), spot, futures)


def test_absolute_hourly_rate_converts_to_relative_8h():
    # Kraken's own relative figure for this hour, scaled to 8h. The exact settlement mark
    # price isn't published, so ~84,000 matches it to within 0.01%.
    rate = relative_funding_rate_8h(Decimal("2.36106713e-10"), Decimal("84000"))
    assert float(rate) == pytest.approx(8 * 1.98329875e-05, rel=1e-4)


def test_apr_is_a_percentage():
    assert float(funding_apr_pct(Decimal("0.0001"))) == pytest.approx(10.95)  # 1 bp per 8h


def test_funding_data_uses_pi_xbtusd_ticker():
    data = asyncio.run(_strategy()._get_funding_data())
    assert data["perp_price"] == Decimal("84111.6779649108")  # markPrice; last (84036.5) is stale
    assert float(data["funding_rate"]) * 10_000 == pytest.approx(1.589, abs=0.001)  # bps per 8h


def test_enters_when_live_funding_clears_thresholds():
    strategy = _strategy()
    entered = []

    async def fake_enter(funding_data):
        entered.append(funding_data)

    strategy._enter_position = fake_enter
    asyncio.run(strategy._check_entry())  # ~1.6 bps/8h, ~17% APR vs 1 bp / 5% thresholds
    assert len(entered) == 1


def test_does_not_enter_on_negative_funding():
    tickers = {"tickers": [dict(KRAKEN_TICKERS["tickers"][1], fundingRate=-2.36106713e-10)]}
    strategy = _strategy(tickers)

    async def fail_enter(funding_data):
        raise AssertionError("entered on negative funding")

    strategy._enter_position = fail_enter
    asyncio.run(strategy._check_entry())


def test_missing_perp_ticker_yields_no_data():
    assert asyncio.run(_strategy({"tickers": []})._get_funding_data()) is None


def test_funding_accrues_per_8h_period():
    strategy = _strategy()
    strategy.position.state = PositionState.OPEN
    strategy.position.spot_volume = strategy.position.perp_volume = Decimal("1")
    strategy.position.entry_spot_price = strategy.position.entry_perp_price = Decimal("60000")
    strategy.position.entry_funding_rate = Decimal("0.0001")  # 1 bp per 8h
    strategy.position.entry_time = datetime.utcnow() - timedelta(hours=8)

    async def steady():
        return {"spot_price": Decimal("60000"), "perp_price": Decimal("60000"),
                "basis_bps": Decimal("0"), "funding_rate": Decimal("0.0001")}

    strategy._get_funding_data = steady
    asyncio.run(strategy._manage_position())
    assert float(strategy.position.total_funding_collected) == pytest.approx(6.0, rel=1e-3)
