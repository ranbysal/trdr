from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pandas.testing as pdt

from futures_bot.features.indicators_1m import compute_indicators_1m
from futures_bot.features.indicators_5m import compute_indicators_5m

ET = ZoneInfo("America/New_York")


def test_sigma_60_alignment_includes_current_bar() -> None:
    start = datetime(2026, 1, 5, 9, 30, tzinfo=ET)
    rows = []
    for i in range(60):
        close = 100.0 + float(i)
        rows.append(
            {
                "ts": start + timedelta(minutes=i),
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "session_vwap": 100.0,
            }
        )
    bars = pd.DataFrame(rows)

    out = compute_indicators_1m(bars)

    assert pd.isna(out.loc[58, "Sigma_t"])
    assert pd.notna(out.loc[59, "Sigma_t"])
    assert out.loc[59, "Dist_t"] == 59.0


def test_indicators_5m_columns_populate() -> None:
    start = datetime(2026, 1, 5, 9, 30, tzinfo=ET)
    rows = []
    for i in range(40):
        close = 100.0 + i * 0.5
        rows.append(
            {
                "ts": start + timedelta(minutes=5 * i),
                "high": close + 0.8,
                "low": close - 0.6,
                "close": close,
                "session_vwap": 99.0 + i * 0.4,
            }
        )
    bars = pd.DataFrame(rows)

    out = compute_indicators_5m(bars)

    assert "ATR_14_5m" in out.columns
    assert "ADX_14_5m" in out.columns
    assert "EMA9_5m" in out.columns
    assert "EMA21_5m" in out.columns
    assert "EMA20_5m" in out.columns
    assert "ER_20" in out.columns
    assert "VWAP_SLOPE_NORM_6" in out.columns
    assert pd.notna(out.iloc[-1]["EMA9_5m"])


def test_indicator_primitives_are_deterministic() -> None:
    start = datetime(2026, 1, 5, 9, 30, tzinfo=ET)
    rows_1m = []
    rows_5m = []
    for i in range(80):
        close_1m = 100.0 + i * 0.25
        rows_1m.append(
            {
                "ts": start + timedelta(minutes=i),
                "high": close_1m + 0.8,
                "low": close_1m - 0.6,
                "close": close_1m,
                "session_vwap": 99.5 + i * 0.2,
            }
        )
    for i in range(40):
        close_5m = 100.0 + i * 0.5
        rows_5m.append(
            {
                "ts": start + timedelta(minutes=5 * i),
                "high": close_5m + 0.8,
                "low": close_5m - 0.6,
                "close": close_5m,
                "session_vwap": 99.0 + i * 0.4,
            }
        )

    bars_1m = pd.DataFrame(rows_1m)
    bars_5m = pd.DataFrame(rows_5m)

    first_1m = compute_indicators_1m(bars_1m)
    second_1m = compute_indicators_1m(bars_1m.copy())
    first_5m = compute_indicators_5m(bars_5m)
    second_5m = compute_indicators_5m(bars_5m.copy())

    pdt.assert_frame_equal(first_1m, second_1m)
    pdt.assert_frame_equal(first_5m, second_5m)
