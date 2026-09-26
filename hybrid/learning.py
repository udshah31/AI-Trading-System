"""
Learning loop: record outcomes, re-fit, propose
================================================
1. Label outcomes: once a quant decision is HORIZON_HOURS old, record the price move
   over that horizon (hourly bars, same series for entry and exit).
2. Re-fit: search technical-indicator weights, within MAX_STEP of the current ones, for
   the best correlation between the composite score and the next-day return.
3. Propose only if it holds up on data it didn't learn from: the newest ~30% of samples
   are held out; there the proposal must beat the current weights by MIN_GAIN and its
   correlation must be significant (above 2/sqrt(n), roughly p < 0.05), because with a
   few dozen days a correlation of +/-0.2 arises by chance. Nothing is applied until a
   person approves it.

Decisions run every few minutes on daily indicators, so only the first decision per
coin per day becomes a training sample (the rest are near-duplicates).
"""
import asyncio
import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Sequence, Tuple

HORIZON_HOURS = 24
MIN_SAMPLES = 60          # coin-days with a known outcome before any proposal
MIN_TEST = 20             # held-out samples, at least
TEST_FRACTION = 0.3       # newest share of samples held out
MAX_STEP = 0.05           # largest change to any one weight per proposal
MIN_GAIN = 0.02           # held-out correlation must improve by at least this
CANDIDATES = 4000         # weight vectors tried per re-fit
PRICE_TOLERANCE = timedelta(hours=6)  # how late a bar may be and still count
WEIGHTS = ("rsi", "ema", "bollinger", "volume")

# (symbol, start, end) -> [(time, close)] ascending; symbol is a price-data symbol like BTC-USD
PriceFetcher = Callable[[str, datetime, datetime], Sequence[Tuple[datetime, float]]]


@dataclass
class Proposal:
    accepted: bool
    reason: str
    weights: Dict[str, float]
    metrics: Dict = field(default_factory=dict)


def daily_samples(rows: List[Dict]) -> List[Dict]:
    """First decision per coin per UTC day, oldest first."""
    kept: Dict[Tuple[str, object], Dict] = {}
    for row in sorted(rows, key=lambda r: r["timestamp"]):
        kept.setdefault((row["symbol"], row["timestamp"].date()), row)
    return list(kept.values())


def information_coefficient(weights: Dict[str, float], samples: List[Dict]) -> Optional[float]:
    """Pearson correlation between the weighted composite score and the forward return."""
    if len(samples) < 3:
        return None
    xs = [sum(weights[k] * s["scores"][k] for k in WEIGHTS) for s in samples]
    ys = [s["forward_return"] for s in samples]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx == 0 or syy == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(sxx * syy)


def _candidates(current: Dict[str, float], rng: random.Random):
    """Weight vectors within MAX_STEP of current that still sum to 1 and stay >= 0."""
    for _ in range(CANDIDATES):
        deltas = [rng.uniform(-MAX_STEP, MAX_STEP) for _ in WEIGHTS]
        mean = sum(deltas) / len(deltas)
        deltas = [d - mean for d in deltas]  # zero-sum keeps the total at 1
        cand = {k: current[k] + d for k, d in zip(WEIGHTS, deltas)}
        if all(abs(d) <= MAX_STEP for d in deltas) and min(cand.values()) >= 0:
            yield cand


def propose_weights(current: Dict[str, float], rows: List[Dict], seed: int = 0) -> Proposal:
    samples = daily_samples(rows)
    n = len(samples)
    if n < MIN_SAMPLES:
        return Proposal(False, f"Needs {MIN_SAMPLES} coin-days of decisions with known outcomes; has {n}.",
                        dict(current), {"n": n})
    n_test = max(MIN_TEST, round(n * TEST_FRACTION))
    train, test = samples[:-n_test], samples[-n_test:]

    best, best_ic = dict(current), information_coefficient(current, train)
    for cand in _candidates(current, random.Random(seed)):
        ic = information_coefficient(cand, train)
        if ic is not None and (best_ic is None or ic > best_ic):
            best, best_ic = cand, ic

    test_current = information_coefficient(current, test)
    test_proposed = information_coefficient(best, test)
    metrics = {
        "n_train": len(train), "n_test": len(test),
        "train_ic_current": information_coefficient(current, train), "train_ic_proposed": best_ic,
        "test_ic_current": test_current, "test_ic_proposed": test_proposed,
        "test_from": test[0]["timestamp"].isoformat(), "test_to": test[-1]["timestamp"].isoformat(),
    }
    if best == current or test_proposed is None:
        return Proposal(False, "No weights near the current ones did better on the training data.", dict(current), metrics)
    significant = 2 / math.sqrt(len(test))  # ~p < 0.05 for a correlation on len(test) samples
    metrics["test_ic_needed"] = significant
    if test_proposed < significant or test_proposed < (test_current or 0.0) + MIN_GAIN:
        return Proposal(False, "The best weights on training data didn't clearly hold up on the held-out days.",
                        dict(current), metrics)
    return Proposal(True, "Beat the current weights on held-out days.", best, metrics)


def _first_at_or_after(prices: Sequence[Tuple[datetime, float]], when: datetime) -> Optional[float]:
    for ts, price in prices:
        ts = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        if when <= ts <= when + PRICE_TOLERANCE:
            return price
    return None


async def label_outcomes(storage, fetch_prices: PriceFetcher, now: Optional[datetime] = None,
                         horizon_hours: int = HORIZON_HOURS) -> int:
    """Record the forward return of every quant decision old enough to have one."""
    from hybrid.quant_agent import market_data_symbol

    now = now or datetime.now(timezone.utc)
    horizon = timedelta(hours=horizon_hours)
    decisions = await storage.learning.unlabeled_decisions(now - horizon)
    by_symbol: Dict[str, list] = {}
    for d in decisions:
        by_symbol.setdefault(d.symbol, []).append(d)

    labelled = 0
    for symbol, group in by_symbol.items():
        times = [(d.timestamp if d.timestamp.tzinfo else d.timestamp.replace(tzinfo=timezone.utc)) for d in group]
        prices = await asyncio.to_thread(fetch_prices, market_data_symbol(symbol),
                                         min(times) - PRICE_TOLERANCE, max(times) + horizon + PRICE_TOLERANCE)
        prices = sorted(prices, key=lambda p: p[0] if p[0].tzinfo else p[0].replace(tzinfo=timezone.utc))
        for decision, at in zip(group, times):
            entry = _first_at_or_after(prices, at)
            exit_ = _first_at_or_after(prices, at + horizon)
            if entry and exit_:
                await storage.learning.save_outcome(decision.id, horizon_hours, entry, exit_)
                labelled += 1
    return labelled


def fetch_hourly_closes(symbol: str, start: datetime, end: datetime) -> List[Tuple[datetime, float]]:
    """Hourly closes from Yahoo Finance (same source as the quant indicators)."""
    import yfinance as yf

    df = yf.download(symbol, start=start, end=end + timedelta(hours=1), interval="1h", progress=False)
    if df is None or df.empty:
        return []
    closes = df["Close"]
    if hasattr(closes, "columns"):  # multi-ticker frame shape
        closes = closes.iloc[:, 0]
    return [(ts.to_pydatetime().astimezone(timezone.utc), float(v)) for ts, v in closes.dropna().items()]


async def run_learning_cycle(storage, config, fetch_prices: PriceFetcher = fetch_hourly_closes,
                             now: Optional[datetime] = None) -> Proposal:
    """Label new outcomes, re-fit, and store a pending proposal if one holds up."""
    labelled = await label_outcomes(storage, fetch_prices, now=now)
    samples = await storage.learning.labelled_samples()
    current = await storage.learning.active_weights() or config.tech_weights()
    result = propose_weights(current, samples)
    if result.accepted:
        await storage.learning.save_proposal(current, result.weights, result.metrics)
    print(f"[Learning] +{labelled} outcomes; {len(daily_samples(samples))} coin-days; "
          f"{'proposal saved for approval' if result.accepted else result.reason}")
    return result
