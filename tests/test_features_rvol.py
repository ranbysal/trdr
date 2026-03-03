from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from futures_bot.features.rvol import compute_rvol_tod, median_rvol_3bar

ET = ZoneInfo("America/New_York")


def test_rvol_same_bucket_uses_prior_session_median() -> None:
    bars = pd.DataFrame(
        {
            "ts": [
                datetime(2026, 1, 5, 9, 30, tzinfo=ET),
                datetime(2026, 1, 6, 9, 30, tzinfo=ET),
                datetime(2026, 1, 7, 9, 30, tzinfo=ET),
            ],
            "volume": [100.0, 200.0, 300.0],
        }
    )

    out = compute_rvol_tod(bars, timeframe="1m")

    assert pd.isna(out.loc[0, "RVOL_TOD_1m"])
    assert out.loc[1, "RVOL_TOD_1m"] == 2.0
    assert out.loc[2, "same_bucket_median"] == 150.0
    assert out.loc[2, "RVOL_TOD_1m"] == 2.0


def test_median_rvol_3bar_helper() -> None:
    rvol = pd.Series([1.0, 2.0, 4.0, 3.0], dtype=float)
    med = median_rvol_3bar(rvol)

    assert pd.isna(med.iloc[1])
    assert med.iloc[2] == 2.0
    assert med.iloc[3] == 3.0


def test_rvol_vol_ok_and_insufficient_history_logging() -> None:
    bars = pd.DataFrame(
        {
            "ts": [
                datetime(2026, 1, 1, 9, 30, tzinfo=ET),
                datetime(2026, 1, 2, 9, 30, tzinfo=ET),
                datetime(2026, 1, 3, 9, 30, tzinfo=ET),
                datetime(2026, 1, 4, 9, 30, tzinfo=ET),
                datetime(2026, 1, 5, 9, 30, tzinfo=ET),
                datetime(2026, 1, 6, 9, 30, tzinfo=ET),
            ],
            "volume": [100.0, 110.0, 120.0, 130.0, 140.0, 200.0],
        }
    )

    out = compute_rvol_tod(bars, timeframe="1m")

    assert not out.loc[0, "VOL_OK"]
    assert not out.loc[4, "VOL_OK"]
    assert out.loc[5, "VOL_OK"]
    assert out.loc[5, "history_log_code"] == "INSUFFICIENT_HISTORY"
