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
    assert out.loc[21, "ATR_pct_rank"] == 1.0
