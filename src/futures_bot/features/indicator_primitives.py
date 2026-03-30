"""Pure indicator primitives shared by higher-level feature builders."""

from __future__ import annotations

import numpy as np
import pandas as pd


def true_range(*, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr_1 = high - low
    tr_2 = (high - prev_close).abs()
    tr_3 = (low - prev_close).abs()
    return pd.concat([tr_1, tr_2, tr_3], axis=1).max(axis=1)


def rolling_rsi(*, close: pd.Series, period: int) -> pd.Series:
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


def rolling_std(series: pd.Series, *, window: int, min_periods: int | None = None) -> pd.Series:
    required = window if min_periods is None else min_periods
    return series.rolling(window=window, min_periods=required).std(ddof=0)


def ema(series: pd.Series, *, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False, min_periods=span).mean()


def adx(*, high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0), index=high.index)
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0),
        index=high.index,
    )

    tr = true_range(high=high, low=low, close=close)
    tr_n = tr.rolling(window=period, min_periods=period).sum()
    plus_di = 100.0 * plus_dm.rolling(window=period, min_periods=period).sum() / tr_n
    minus_di = 100.0 * minus_dm.rolling(window=period, min_periods=period).sum() / tr_n

    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    return dx.rolling(window=period, min_periods=period).mean()


def kaufman_efficiency_ratio(*, close: pd.Series, period: int) -> pd.Series:
    direction = (close - close.shift(period)).abs()
    volatility = close.diff().abs().rolling(window=period, min_periods=period).sum()
    return direction / volatility.replace(0.0, np.nan)


def normalized_slope(series: pd.Series, *, periods: int, normalizer: pd.Series) -> pd.Series:
    slope = (series - series.shift(periods)) / float(periods)
    return slope / normalizer.replace(0.0, np.nan)
