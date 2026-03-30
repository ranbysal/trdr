"""Session VWAP calculations for 1m bars."""

from __future__ import annotations

from datetime import time

import numpy as np
import pandas as pd

from futures_bot.core.enums import Family

_ET = "America/New_York"


def session_start_time(family: Family) -> time:
    """Return session VWAP start time in ET by instrument family."""
    if family is Family.EQUITIES:
        return time(9, 30)
    if family is Family.METALS:
        return time(8, 0)
    raise ValueError(f"Unsupported family: {family}")


def compute_session_vwap_1m(
    bars: pd.DataFrame,
    *,
    family: Family,
    ts_col: str = "ts",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.Series:
    """Compute cumulative session VWAP on 1m bars from family-specific ET start."""
    _require_columns(bars, [ts_col, close_col, volume_col])

    df = bars.copy()
    ts_et = _to_et(pd.to_datetime(df[ts_col], errors="raise"))
    df["_session_date"] = ts_et.dt.date
    df["_session_time"] = ts_et.dt.time

    start = session_start_time(family)
    in_session = df["_session_time"] >= start

    out = pd.Series(np.nan, index=df.index, dtype=float)
    for _, idx in df[in_session].groupby("_session_date").groups.items():
        prices = df.loc[idx, close_col].astype(float)
        vols = df.loc[idx, volume_col].astype(float)
        cum_pv = (prices * vols).cumsum()
        cum_v = vols.cumsum()
        vwap = np.where(cum_v.to_numpy() > 0.0, cum_pv.to_numpy() / cum_v.to_numpy(), np.nan)
        out.loc[idx] = vwap

    return out


def compute_anchored_vwap_1m(
    bars: pd.DataFrame,
    *,
    anchor_ts: pd.Timestamp | object,
    ts_col: str = "ts",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.Series:
    """Compute cumulative VWAP from a fixed anchor timestamp."""
    _require_columns(bars, [ts_col, close_col, volume_col])

    if bars.empty:
        return pd.Series(dtype=float, index=bars.index)

    df = bars.copy()
    ts_et = _to_et(pd.to_datetime(df[ts_col], errors="raise"))
    anchor = _normalize_anchor(pd.Timestamp(anchor_ts))

    df["_ts_et"] = ts_et
    df = df.sort_values("_ts_et", kind="mergesort")
    out = pd.Series(np.nan, index=df.index, dtype=float)

    active = df["_ts_et"] >= anchor
    if active.any():
        prices = df.loc[active, close_col].astype(float)
        vols = df.loc[active, volume_col].astype(float)
        cum_pv = (prices * vols).cumsum()
        cum_v = vols.cumsum()
        out.loc[active] = np.where(cum_v.to_numpy() > 0.0, cum_pv.to_numpy() / cum_v.to_numpy(), np.nan)

    return out.reindex(bars.index)


def _to_et(ts: pd.Series) -> pd.Series:
    if ts.dt.tz is None:
        return ts.dt.tz_localize(_ET)
    return ts.dt.tz_convert(_ET)


def _normalize_anchor(anchor_ts: pd.Timestamp) -> pd.Timestamp:
    if anchor_ts.tzinfo is None:
        return anchor_ts.tz_localize(_ET)
    return anchor_ts.tz_convert(_ET)


def _require_columns(frame: pd.DataFrame, columns: list[str]) -> None:
    missing = [col for col in columns if col not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
