"""
Technical Indicators
====================
Pure mathematical technical analysis using yfinance + stockstats.
NO LLM involvement — this is Track B's data source.

These indicators feed directly into the Quant Engine alongside
the LLM-derived scores from Track A.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf
from stockstats import StockDataFrame


@dataclass
class TechnicalSignals:
    """Technical indicator values computed from price data.

    All scores are normalized to [0.0, 1.0] where applicable:
    - 0.0 = extremely bearish signal
    - 0.5 = neutral
    - 1.0 = extremely bullish signal
    """

    # Raw indicator values
    rsi: float = 50.0
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    bollinger_upper: float = 0.0
    bollinger_lower: float = 0.0
    bollinger_mid: float = 0.0
    atr: float = 0.0
    current_price: float = 0.0
    volume_sma_20: float = 0.0
    current_volume: float = 0.0

    # Normalized scores (0.0 to 1.0)
    rsi_score: float = 0.5
    ema_crossover_score: float = 0.5
    bollinger_score: float = 0.5
    volume_score: float = 0.5

    # Metadata
    ticker: str = ""
    as_of_date: str = ""
    data_points: int = 0

    def summary(self) -> str:
        """Human-readable summary of technical signals."""
        return (
            f"Technical Signals for {self.ticker} (as of {self.as_of_date}):\n"
            f"  Price:           ${self.current_price:.2f}\n"
            f"  RSI(14):         {self.rsi:.1f} → score: {self.rsi_score:.2f}\n"
            f"  EMA(12/26):      {self.ema_fast:.2f}/{self.ema_slow:.2f} → "
            f"score: {self.ema_crossover_score:.2f}\n"
            f"  Bollinger:       [{self.bollinger_lower:.2f} | "
            f"{self.bollinger_mid:.2f} | {self.bollinger_upper:.2f}] → "
            f"score: {self.bollinger_score:.2f}\n"
            f"  ATR(14):         {self.atr:.2f}\n"
            f"  Volume:          {self.current_volume:,.0f} vs "
            f"SMA(20): {self.volume_sma_20:,.0f} → "
            f"score: {self.volume_score:.2f}\n"
            f"  Data points:     {self.data_points}\n"
        )


def compute_technical_signals(
    ticker: str,
    trade_date: Optional[str] = None,
    rsi_period: int = 14,
    ema_fast: int = 12,
    ema_slow: int = 26,
    bollinger_period: int = 20,
    bollinger_std: float = 2.0,
    atr_period: int = 14,
    lookback_days: int = 365,
) -> TechnicalSignals:
    """Compute technical indicators for a given ticker.

    Downloads price data via yfinance (already installed in TradingAgents),
    computes indicators via stockstats (also installed), and returns
    normalized scores for the Quant Engine.

    Args:
        ticker: Stock/crypto ticker symbol (e.g., "AAPL", "BTC-USD").
        trade_date: Date to compute indicators for (YYYY-MM-DD).
                    If None, uses today.
        rsi_period: RSI lookback period.
        ema_fast: Fast EMA period.
        ema_slow: Slow EMA period.
        bollinger_period: Bollinger Band SMA period.
        bollinger_std: Bollinger Band standard deviation multiplier.
        atr_period: ATR lookback period.
        lookback_days: Days of historical data to download.

    Returns:
        TechnicalSignals with raw values and normalized scores.
    """
    signals = TechnicalSignals(ticker=ticker)

    # ── Download Price Data ──
    if trade_date:
        end_date = datetime.strptime(trade_date, "%Y-%m-%d")
    else:
        end_date = datetime.now()

    signals.as_of_date = end_date.strftime("%Y-%m-%d")
    start_date = end_date - timedelta(days=lookback_days)

    try:
        df = yf.download(
            ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )
    except Exception as e:
        print(f"[TechnicalIndicators] Failed to download data for {ticker}: {e}")
        return signals

    if df.empty or len(df) < ema_slow + 10:
        print(
            f"[TechnicalIndicators] Insufficient data for {ticker}: "
            f"{len(df)} rows (need {ema_slow + 10}+)"
        )
        return signals

    # Flatten multi-level columns if present (yfinance >= 0.2.28)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Clamp to trade_date to prevent look-ahead bias
    df = df[df.index <= pd.Timestamp(end_date)]
    signals.data_points = len(df)

    if df.empty:
        return signals

    # ── Current Price ──
    signals.current_price = float(df["Close"].iloc[-1])

    # ── RSI ──
    try:
        sdf = StockDataFrame.retype(df.copy())
        rsi_col = f"rsi_{rsi_period}"
        signals.rsi = float(sdf[rsi_col].iloc[-1])
    except Exception:
        # Manual RSI computation as fallback
        delta = df["Close"].diff()
        gain = delta.where(delta > 0, 0.0).rolling(window=rsi_period).mean()
        loss = (-delta.where(delta < 0, 0.0)).rolling(window=rsi_period).mean()
        rs = gain / loss.replace(0, float("inf"))
        rsi_series = 100 - (100 / (1 + rs))
        signals.rsi = float(rsi_series.iloc[-1]) if not rsi_series.empty else 50.0

    # Normalize RSI to score:
    # RSI < 30 (oversold) → bullish (score ~1.0)
    # RSI > 70 (overbought) → bearish (score ~0.0)
    # RSI 50 → neutral (score 0.5)
    signals.rsi_score = max(0.0, min(1.0, 1.0 - (signals.rsi / 100.0)))

    # ── EMA Crossover ──
    ema_fast_series = df["Close"].ewm(span=ema_fast, adjust=False).mean()
    ema_slow_series = df["Close"].ewm(span=ema_slow, adjust=False).mean()
    signals.ema_fast = float(ema_fast_series.iloc[-1])
    signals.ema_slow = float(ema_slow_series.iloc[-1])

    # Normalize: positive spread = bullish, negative = bearish
    if signals.ema_slow > 0:
        spread_pct = (signals.ema_fast - signals.ema_slow) / signals.ema_slow
        # Clamp spread to [-5%, +5%] range and normalize to [0, 1]
        signals.ema_crossover_score = max(0.0, min(1.0, 0.5 + spread_pct * 10))
    else:
        signals.ema_crossover_score = 0.5

    # ── Bollinger Bands ──
    sma = df["Close"].rolling(window=bollinger_period).mean()
    std = df["Close"].rolling(window=bollinger_period).std()
    signals.bollinger_mid = float(sma.iloc[-1])
    signals.bollinger_upper = float(sma.iloc[-1] + bollinger_std * std.iloc[-1])
    signals.bollinger_lower = float(sma.iloc[-1] - bollinger_std * std.iloc[-1])

    # Normalize: price near lower band = bullish, near upper = bearish
    bb_range = signals.bollinger_upper - signals.bollinger_lower
    if bb_range > 0:
        bb_position = (
            signals.current_price - signals.bollinger_lower
        ) / bb_range
        # Invert: low position (near lower band) = bullish
        signals.bollinger_score = max(0.0, min(1.0, 1.0 - bb_position))
    else:
        signals.bollinger_score = 0.5

    # ── ATR (Average True Range) ──
    high = df["High"]
    low = df["Low"]
    close_prev = df["Close"].shift(1)
    tr = pd.concat(
        [high - low, (high - close_prev).abs(), (low - close_prev).abs()],
        axis=1,
    ).max(axis=1)
    atr_series = tr.rolling(window=atr_period).mean()
    signals.atr = float(atr_series.iloc[-1]) if not atr_series.empty else 0.0

    # ── Volume Analysis ──
    if "Volume" in df.columns:
        vol = df["Volume"]
        signals.volume_sma_20 = float(vol.rolling(window=20).mean().iloc[-1])
        signals.current_volume = float(vol.iloc[-1])

        # Volume Z-score: how unusual is today's volume vs 20-day average
        if signals.volume_sma_20 > 0:
            vol_ratio = signals.current_volume / signals.volume_sma_20
            # High volume confirms trend; normalize to [0, 1]
            # vol_ratio > 1 = above average volume = stronger signal
            signals.volume_score = max(0.0, min(1.0, vol_ratio / 3.0))
        else:
            signals.volume_score = 0.5

    return signals
