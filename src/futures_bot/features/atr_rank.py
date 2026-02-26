"""ATR percentile rank calculations by 5m time bucket."""

from __future__ import annotations

import numpy as np
import pandas as pd

from futures_bot.features.history_readiness import classify_sample_count


def compute_atr_pct_rank(
    bars: pd.DataFrame,
    *,
    ts_col: str = "ts",
    atr_col: str = "ATR_14_5m",
) -> pd.DataFrame:
    """Compute ATR percentile rank vs same 5m bucket over prior up to 20 sessions."""
    _require_columns(bars, [ts_col, atr_col])

    df = bars.copy()
    ts = pd.to_datetime(df[ts_col], errors="raise")
    df["_date"] = ts.dt.date
    df["_bucket"] = ts.dt.strftime("%H:%M")

    pct_ranks: list[float] = []
    counts: list[int] = []
    states: list[str] = []
    avail_flags: list[bool] = []
    partial_flags: list[bool] = []

    for idx, row in df.iterrows():
        prior = df.loc[
            (df.index != idx) & (df["_bucket"] == row["_bucket"]) & (df["_date"] < row["_date"]),
            atr_col,
        ].astype(float)

        if prior.shape[0] > 20:
            prior = prior.iloc[-20:]

        sample_count = int(prior.shape[0])
        readiness = classify_sample_count(sample_count)

        current_atr = float(row[atr_col])
        if sample_count > 0:
            pct_rank = float(np.count_nonzero(prior.to_numpy() <= current_atr) / sample_count)
        else:
            pct_rank = np.nan

        pct_ranks.append(pct_rank)
        counts.append(sample_count)
        states.append(readiness.state)
        avail_flags.append(readiness.is_available)
        partial_flags.append(readiness.is_partial)

    return pd.DataFrame(
        {
            "ATR_pct_rank": pd.Series(pct_ranks, index=df.index, dtype=float),
            "same_bucket_samples": pd.Series(counts, index=df.index, dtype=int),
            "readiness": pd.Series(states, index=df.index, dtype=str),
            "is_available": pd.Series(avail_flags, index=df.index, dtype=bool),
            "is_partial": pd.Series(partial_flags, index=df.index, dtype=bool),
        },
        index=df.index,
    )


def _require_columns(frame: pd.DataFrame, columns: list[str]) -> None:
    missing = [col for col in columns if col not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
