"""5m indicator calculations."""

from __future__ import annotations

import numpy as np
import pandas as pd


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

    tr = _true_range(high=high, low=low, close=close)
    atr_14 = tr.rolling(window=14, min_periods=14).mean()

    adx_14 = _adx(high=high, low=low, close=close, period=14)

    ema9 = close.ewm(span=9, adjust=False, min_periods=9).mean()
    ema21 = close.ewm(span=21, adjust=False, min_periods=21).mean()
    ema20 = close.ewm(span=20, adjust=False, min_periods=20).mean()

    er_20 = _kaufman_er(close=close, period=20)

    slope_6 = (session_vwap - session_vwap.shift(6)) / 6.0
    vwap_slope_norm = slope_6 / atr_14.replace(0.0, np.nan)

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


def _true_range(*, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr_1 = high - low
    tr_2 = (high - prev_close).abs()
    tr_3 = (low - prev_close).abs()
    return pd.concat([tr_1, tr_2, tr_3], axis=1).max(axis=1)


def _adx(*, high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0), index=high.index)
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0), index=high.index
    )

    tr = _true_range(high=high, low=low, close=close)
    tr_n = tr.rolling(window=period, min_periods=period).sum()
    plus_di = 100.0 * plus_dm.rolling(window=period, min_periods=period).sum() / tr_n
    minus_di = 100.0 * minus_dm.rolling(window=period, min_periods=period).sum() / tr_n

    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    return dx.rolling(window=period, min_periods=period).mean()


def _kaufman_er(*, close: pd.Series, period: int) -> pd.Series:
    direction = (close - close.shift(period)).abs()
    volatility = close.diff().abs().rolling(window=period, min_periods=period).sum()
    return direction / volatility.replace(0.0, np.nan)


def _require_columns(frame: pd.DataFrame, columns: list[str]) -> None:
    missing = [col for col in columns if col not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
