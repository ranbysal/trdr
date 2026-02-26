"""Time-of-day normalized RVOL calculations."""

from __future__ import annotations


import numpy as np
import pandas as pd

from futures_bot.features.history_readiness import classify_sample_count

VOL_STRONG_THRESHOLD_1M = 1.5
VOL_STRONG_THRESHOLD_5M = 1.3


def compute_rvol_tod(
    bars: pd.DataFrame,
    *,
    timeframe: str,
    ts_col: str = "ts",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """Compute same-bucket RVOL using prior-session median for each timestamp bucket."""
    if timeframe not in {"1m", "5m"}:
        raise ValueError("timeframe must be '1m' or '5m'")

    _require_columns(bars, [ts_col, volume_col])

    df = bars.copy()
    ts = pd.to_datetime(df[ts_col], errors="raise")
    df["_date"] = ts.dt.date
    df["_bucket"] = ts.dt.strftime("%H:%M")

    medians: list[float] = []
    counts: list[int] = []
    rvols: list[float] = []
    states: list[str] = []
    avail_flags: list[bool] = []
    partial_flags: list[bool] = []

    for idx, row in df.iterrows():
        prior = df.loc[
            (df.index != idx) & (df["_bucket"] == row["_bucket"]) & (df["_date"] < row["_date"]),
            volume_col,
        ].astype(float)
        sample_count = int(prior.shape[0])
        readiness = classify_sample_count(sample_count)

        bucket_median = float(prior.median()) if sample_count > 0 else np.nan
        vol = float(row[volume_col])
        rvol = vol / bucket_median if sample_count > 0 and bucket_median > 0.0 else np.nan

        medians.append(bucket_median)
        counts.append(sample_count)
        rvols.append(rvol)
        states.append(readiness.state)
        avail_flags.append(readiness.is_available)
        partial_flags.append(readiness.is_partial)

    col_prefix = "RVOL_TOD_1m" if timeframe == "1m" else "RVOL_TOD_5m"
    threshold = VOL_STRONG_THRESHOLD_1M if timeframe == "1m" else VOL_STRONG_THRESHOLD_5M
    rvol_series = pd.Series(rvols, index=df.index, dtype=float)

    return pd.DataFrame(
        {
            f"{col_prefix}": rvol_series,
            "same_bucket_median": pd.Series(medians, index=df.index, dtype=float),
            "same_bucket_samples": pd.Series(counts, index=df.index, dtype=int),
            "readiness": pd.Series(states, index=df.index, dtype=str),
            "is_available": pd.Series(avail_flags, index=df.index, dtype=bool),
            "is_partial": pd.Series(partial_flags, index=df.index, dtype=bool),
            "VOL_STRONG": rvol_series >= threshold,
        },
        index=df.index,
    )


def median_rvol_3bar(rvol: pd.Series) -> pd.Series:
    """Return rolling 3-bar median RVOL (5m helper)."""
    return rvol.astype(float).rolling(window=3, min_periods=3).median()


def _require_columns(frame: pd.DataFrame, columns: list[str]) -> None:
    missing = [col for col in columns if col not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
