"""5m indicator calculations."""

from __future__ import annotations

import numpy as np
import pandas as pd

from futures_bot.features.indicator_primitives import adx, ema, kaufman_efficiency_ratio, normalized_slope, true_range


def compute_indicators_5m(
    bars: pd.DataFrame,
    *,
    vwap_col: str = "session_vwap",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """Compute ATR, ADX, EMAs, ER, and normalized VWAP slope on 5m bars."""
    _require_columns(bars, [high_col, low_col, close_col, vwap_col])

    high = bars[high_col].astype(float)
    low = bars[low_col].astype(float)
    close = bars[close_col].astype(float)
    session_vwap = bars[vwap_col].astype(float)

    tr = true_range(high=high, low=low, close=close)
    atr_14 = tr.rolling(window=14, min_periods=14).mean()

    adx_14 = adx(high=high, low=low, close=close, period=14)

    ema9 = ema(close, span=9)
    ema21 = ema(close, span=21)
    ema20 = ema(close, span=20)

    er_20 = kaufman_efficiency_ratio(close=close, period=20)

    vwap_slope_norm = normalized_slope(session_vwap, periods=6, normalizer=atr_14)

    return pd.DataFrame(
        {
            "ATR_14_5m": atr_14,
            "ADX_14_5m": adx_14,
            "EMA9_5m": ema9,
            "EMA21_5m": ema21,
            "EMA20_5m": ema20,
            "ER_20": er_20,
            "VWAP_SLOPE_NORM_6": vwap_slope_norm,
        },
        index=bars.index,
    )


def _require_columns(frame: pd.DataFrame, columns: list[str]) -> None:
    missing = [col for col in columns if col not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
