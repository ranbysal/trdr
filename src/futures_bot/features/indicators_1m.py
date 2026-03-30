"""1m indicator calculations."""

from __future__ import annotations

import numpy as np
import pandas as pd

from futures_bot.features.indicator_primitives import rolling_rsi, rolling_std, true_range


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

    tr = true_range(high=high, low=low, close=close)
    atr_14 = tr.rolling(window=14, min_periods=14).mean()

    rsi_2 = rolling_rsi(close=close, period=2)

    dist_t = close - session_vwap
    sigma_t = rolling_std(dist_t, window=60, min_periods=60)

    return pd.DataFrame(
        {
            "ATR_14_1m": atr_14,
            "RSI_2_1m": rsi_2,
            "Dist_t": dist_t,
            "Sigma_t": sigma_t,
        },
        index=bars.index,
    )


def _require_columns(frame: pd.DataFrame, columns: list[str]) -> None:
    missing = [col for col in columns if col not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
