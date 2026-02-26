"""1m indicator calculations."""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_indicators_1m(
    bars: pd.DataFrame,
    *,
    session_vwap_col: str = "session_vwap",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """Compute ATR_14_1m, RSI_2_1m, Dist_t, Sigma_t on 1m bars."""
    _require_columns(bars, [high_col, low_col, close_col, session_vwap_col])

    close = bars[close_col].astype(float)
    high = bars[high_col].astype(float)
    low = bars[low_col].astype(float)
    session_vwap = bars[session_vwap_col].astype(float)

    tr = _true_range(high=high, low=low, close=close)
    atr_14 = tr.rolling(window=14, min_periods=14).mean()

    rsi_2 = _rsi(close=close, period=2)

    dist_t = close - session_vwap
    sigma_t = dist_t.rolling(window=60, min_periods=60).std(ddof=0)

    return pd.DataFrame(
        {
            "ATR_14_1m": atr_14,
            "RSI_2_1m": rsi_2,
            "Dist_t": dist_t,
            "Sigma_t": sigma_t,
        },
        index=bars.index,
    )


def _true_range(*, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr_1 = high - low
    tr_2 = (high - prev_close).abs()
    tr_3 = (low - prev_close).abs()
    return pd.concat([tr_1, tr_2, tr_3], axis=1).max(axis=1)


def _rsi(*, close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))

    both_zero = (avg_gain == 0.0) & (avg_loss == 0.0)
    only_loss_zero = (avg_loss == 0.0) & (avg_gain > 0.0)
    rsi = rsi.mask(both_zero, 50.0)
    rsi = rsi.mask(only_loss_zero, 100.0)
    return rsi


def _require_columns(frame: pd.DataFrame, columns: list[str]) -> None:
    missing = [col for col in columns if col not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
