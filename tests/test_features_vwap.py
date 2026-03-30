from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from futures_bot.core.enums import Family
from futures_bot.features.vwap import compute_anchored_vwap_1m, compute_session_vwap_1m

ET = ZoneInfo("America/New_York")


def test_vwap_start_equities_0930() -> None:
    bars = pd.DataFrame(
        {
            "ts": [
                datetime(2026, 1, 5, 9, 29, tzinfo=ET),
                datetime(2026, 1, 5, 9, 30, tzinfo=ET),
                datetime(2026, 1, 5, 9, 31, tzinfo=ET),
            ],
            "close": [99.0, 100.0, 102.0],
            "volume": [10.0, 20.0, 20.0],
        }
    )

    vwap = compute_session_vwap_1m(bars, family=Family.EQUITIES)

    assert pd.isna(vwap.iloc[0])
    assert vwap.iloc[1] == 100.0
    assert vwap.iloc[2] == 101.0


def test_vwap_start_metals_0800() -> None:
    bars = pd.DataFrame(
        {
            "ts": [
                datetime(2026, 1, 5, 7, 59, tzinfo=ET),
                datetime(2026, 1, 5, 8, 0, tzinfo=ET),
            ],
            "close": [2500.0, 2502.0],
            "volume": [5.0, 10.0],
        }
    )

    vwap = compute_session_vwap_1m(bars, family=Family.METALS)

    assert pd.isna(vwap.iloc[0])
    assert vwap.iloc[1] == 2502.0


def test_anchored_vwap_uses_fixed_anchor_timestamp() -> None:
    bars = pd.DataFrame(
        {
            "ts": [
                datetime(2026, 1, 5, 9, 29, tzinfo=ET),
                datetime(2026, 1, 5, 9, 30, tzinfo=ET),
                datetime(2026, 1, 5, 9, 31, tzinfo=ET),
            ],
            "close": [99.0, 100.0, 102.0],
            "volume": [10.0, 20.0, 20.0],
        }
    )

    vwap = compute_anchored_vwap_1m(bars, anchor_ts=datetime(2026, 1, 5, 9, 30, tzinfo=ET))

    assert pd.isna(vwap.iloc[0])
    assert vwap.iloc[1] == 100.0
    assert vwap.iloc[2] == 101.0
