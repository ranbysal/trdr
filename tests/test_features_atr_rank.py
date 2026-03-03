from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from futures_bot.features.atr_rank import compute_atr_pct_rank

ET = ZoneInfo("America/New_York")


def test_atr_pct_rank_readiness_states() -> None:
    start = datetime(2026, 1, 1, 10, 0, tzinfo=ET)
    rows = []
    for i in range(22):
        rows.append(
            {
                "ts": start + timedelta(days=i),
                "ATR_14_5m": float(i + 1),
            }
        )
    bars = pd.DataFrame(rows)

    out = compute_atr_pct_rank(bars)

    assert out.loc[0, "readiness"] == "unavailable"
    assert out.loc[5, "readiness"] == "partial"
    assert out.loc[5, "same_bucket_samples"] == 5
    assert out.loc[20, "readiness"] == "ready"
    assert out.loc[20, "same_bucket_samples"] == 20
    assert out.loc[21, "same_bucket_samples"] == 20
    assert out.loc[21, "ATR_pct_rank"] == 100.0


def test_atr_pct_rank_low_volume_regime_handling() -> None:
    start = datetime(2026, 1, 1, 10, 0, tzinfo=ET)
    bars = pd.DataFrame(
        {
            "ts": [start + timedelta(days=i) for i in range(6)],
            "ATR_14_5m": [1.0, 1.1, 1.2, 1.3, 1.4, 1.5],
        }
    )

    out = compute_atr_pct_rank(bars)

    assert out.loc[0, "low_volume_regime"]
    assert pd.isna(out.loc[0, "ATR_pct_rank"])
    assert not out.loc[0, "ATR_RANK_OK"]

    assert not out.loc[5, "low_volume_regime"]
    assert out.loc[5, "ATR_RANK_OK"]
    assert out.loc[5, "history_log_code"] == "INSUFFICIENT_HISTORY"
