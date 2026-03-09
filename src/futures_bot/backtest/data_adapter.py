"""Replay data adapter from raw 1m OHLCV to paper-engine rows."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from futures_bot.core.enums import Family, Regime
from futures_bot.core.types import InstrumentMeta
from futures_bot.data.session_windows import is_active_session
from futures_bot.features.atr_rank import compute_atr_pct_rank
from futures_bot.features.data_quality import evaluate_bar_timing
from futures_bot.features.indicators_1m import compute_indicators_1m
from futures_bot.features.indicators_5m import compute_indicators_5m
from futures_bot.features.rvol import compute_rvol_tod, median_rvol_3bar
from futures_bot.features.vwap import compute_session_vwap_1m
from futures_bot.regime.engine import FAMILY_SYMBOLS, RegimeEngine
from futures_bot.regime.models import SymbolFeatureSnapshot

REQUIRED_COLUMNS = {"timestamp_et", "symbol", "open", "high", "low", "close", "volume"}

_FINAL_COLUMNS = [
    "ts",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "session_vwap",
    "ema9_5m",
    "ema21_5m",
    "ema20_5m_slope",
    "atr_14_5m",
    "atr_14_1m_price",
    "rvol_3bar_aggregate_5m",
    "low_volume_trend_streak_5m",
    "vol_strong_1m",
    "data_ok",
    "quote_ok",
    "trade_eligible",
    "lockout",
    "family_freeze",
    "raw_regime",
    "is_weak_neutral",
    "confidence",
]

_FALLBACK_FAMILY_BY_SYMBOL: dict[str, Family] = {
    "NQ": Family.EQUITIES,
    "MNQ": Family.EQUITIES,
    "YM": Family.EQUITIES,
    "MYM": Family.EQUITIES,
    "GC": Family.METALS,
    "MGC": Family.METALS,
    "SIL": Family.METALS,
}


@dataclass(frozen=True, slots=True)
class PreparedReplayData:
    rows: pd.DataFrame
    start_ts: pd.Timestamp
    end_ts: pd.Timestamp
    symbols: tuple[str, ...]


def prepare_replay_data(
    *,
    data_path: str | Path,
    instruments_by_symbol: Mapping[str, InstrumentMeta],
) -> PreparedReplayData:
    raw = pd.read_csv(data_path)
    missing = REQUIRED_COLUMNS - set(raw.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    if raw.empty:
        raise ValueError("Replay CSV is empty")

    df = raw.copy()
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["ts"] = _normalize_timestamps(df["timestamp_et"])
    for column in ("open", "high", "low", "close", "volume"):
        df[column] = pd.to_numeric(df[column], errors="raise")
    df = df.sort_values(["ts", "symbol"]).reset_index(drop=True)

    duplicate_mask = df.duplicated(subset=["ts", "symbol"], keep=False)
    if duplicate_mask.any():
        sample = (
            df.loc[duplicate_mask, ["timestamp_et", "symbol"]]
            .head(5)
            .to_dict(orient="records")
        )
        raise ValueError(f"Duplicate timestamp/symbol rows found: {sample}")

    families = {
        symbol: _resolve_family(symbol=symbol, instruments_by_symbol=instruments_by_symbol)
        for symbol in sorted(df["symbol"].unique())
    }

    symbol_rows: list[pd.DataFrame] = []
    five_minute_by_symbol: dict[str, pd.DataFrame] = {}
    for symbol, frame in df.groupby("symbol", sort=True):
        prepared_rows, prepared_5m = _prepare_symbol_rows(frame=frame.reset_index(drop=True), family=families[symbol])
        symbol_rows.append(prepared_rows)
        if not prepared_5m.empty:
            five_minute_by_symbol[symbol] = prepared_5m

    combined = pd.concat(symbol_rows, ignore_index=True).sort_values(["ts", "symbol"]).reset_index(drop=True)
    combined["family"] = combined["symbol"].map(lambda symbol: families[symbol].value)

    regime_rows = _build_family_regime_rows(
        five_minute_by_symbol=five_minute_by_symbol,
        families_by_symbol=families,
    )
    combined = _apply_family_regimes(rows=combined, family_regimes=regime_rows)

    final_rows = _finalize_rows(combined)
    return PreparedReplayData(
        rows=final_rows,
        start_ts=final_rows["ts"].iloc[0],
        end_ts=final_rows["ts"].iloc[-1],
        symbols=tuple(sorted(final_rows["symbol"].unique())),
    )


def _prepare_symbol_rows(*, frame: pd.DataFrame, family: Family) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = frame.copy()
    rows["session_vwap"] = compute_session_vwap_1m(rows, family=family, ts_col="ts")

    indicator_input = rows.copy()
    indicator_input["session_vwap"] = indicator_input["session_vwap"].fillna(indicator_input["close"])
    indicators_1m = compute_indicators_1m(indicator_input)
    rows["atr_14_1m_price"] = indicators_1m["ATR_14_1m"].fillna(0.0)

    rvol_1m = compute_rvol_tod(rows[["ts", "volume"]], timeframe="1m")
    rows["vol_strong_1m"] = rvol_1m["VOL_STRONG"].fillna(False).astype(bool)
    rows["data_ok"] = _evaluate_data_ok(ts_values=rows["ts"], family=family)
    rows["quote_ok"] = True
    rows["lockout"] = False
    rows["family_freeze"] = False

    five_minute = _build_5m_feature_frame(rows=rows, family=family)
    five_for_merge = five_minute[
        [
            "ts_5m",
            "ema9_5m",
            "ema21_5m",
            "ema20_5m_slope",
            "atr_14_5m",
            "rvol_3bar_aggregate_5m",
            "fallback_low_volume_trend_streak_5m",
        ]
    ]
    merged = pd.merge_asof(
        rows.sort_values("ts"),
        five_for_merge.sort_values("ts_5m"),
        left_on="ts",
        right_on="ts_5m",
        direction="backward",
    )

    merged["session_vwap"] = merged["session_vwap"].fillna(merged["close"])
    merged["ema9_5m"] = merged["ema9_5m"].astype(float)
    merged["ema21_5m"] = merged["ema21_5m"].astype(float)
    merged["ema20_5m_slope"] = merged["ema20_5m_slope"].fillna(0.0)
    merged["atr_14_5m"] = merged["atr_14_5m"].astype(float)
    merged["rvol_3bar_aggregate_5m"] = merged["rvol_3bar_aggregate_5m"].astype(float)
    merged["fallback_low_volume_trend_streak_5m"] = (
        merged["fallback_low_volume_trend_streak_5m"].fillna(0).astype(int)
    )

    merged["raw_regime_fallback"] = _fallback_regime_from_emas(
        ema9=merged["ema9_5m"],
        ema21=merged["ema21_5m"],
    )
    dist_vwap = (merged["close"] - merged["session_vwap"]).abs()
    merged["is_weak_neutral_fallback"] = (
        (merged["raw_regime_fallback"] == Regime.NEUTRAL.value)
        & merged["atr_14_5m"].fillna(0.0).gt(0.0)
        & dist_vwap.le(0.3 * merged["atr_14_5m"].fillna(0.0))
    )
    merged["confidence_fallback"] = np.where(
        merged["is_weak_neutral_fallback"],
        0.5,
        np.where(merged["raw_regime_fallback"] == Regime.NEUTRAL.value, 0.6, 1.0),
    )

    merged["trade_eligible"] = (
        merged["data_ok"]
        & merged["quote_ok"]
        & merged["ts_5m"].notna()
        & merged["ema21_5m"].notna()
        & merged["atr_14_5m"].notna()
        & merged["session_vwap"].notna()
        & merged["atr_14_1m_price"].gt(0.0)
    )
    return merged, five_minute


def _build_5m_feature_frame(*, rows: pd.DataFrame, family: Family) -> pd.DataFrame:
    grouped = (
        rows.assign(bucket_5m=rows["ts"].dt.floor("5min"))
        .groupby("bucket_5m", as_index=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            session_vwap=("session_vwap", "last"),
            data_ok_5m=("data_ok", "all"),
        )
    )
    if grouped.empty:
        return pd.DataFrame()

    grouped["symbol"] = rows["symbol"].iloc[0]
    grouped["family"] = family.value
    grouped["ts_5m"] = grouped["bucket_5m"] + pd.Timedelta(minutes=4)

    ind_input = grouped[["ts_5m", "high", "low", "close", "session_vwap"]].rename(columns={"ts_5m": "ts"})
    ind_input["session_vwap"] = ind_input["session_vwap"].fillna(ind_input["close"])
    indicators_5m = compute_indicators_5m(ind_input)

    rvol_input = grouped[["ts_5m", "volume"]].rename(columns={"ts_5m": "ts"})
    rvol_5m = compute_rvol_tod(rvol_input, timeframe="5m")
    atr_rank = compute_atr_pct_rank(
        pd.DataFrame(
            {
                "ts": grouped["ts_5m"],
                "ATR_14_5m": indicators_5m["ATR_14_5m"],
            }
        )
    )

    out = grouped.copy()
    out["atr_14_5m"] = indicators_5m["ATR_14_5m"].to_numpy()
    out["adx_14"] = indicators_5m["ADX_14_5m"].to_numpy()
    out["ema9_5m"] = indicators_5m["EMA9_5m"].to_numpy()
    out["ema21_5m"] = indicators_5m["EMA21_5m"].to_numpy()
    out["ema20_5m"] = indicators_5m["EMA20_5m"].to_numpy()
    out["er_20"] = indicators_5m["ER_20"].to_numpy()
    out["vwap_slope_norm"] = indicators_5m["VWAP_SLOPE_NORM_6"].to_numpy()
    out["rvol_tod_5m"] = rvol_5m["RVOL_TOD_5m"].to_numpy()
    out["rvol_3bar_aggregate_5m"] = median_rvol_3bar(rvol_5m["RVOL_TOD_5m"]).to_numpy()
    out["atr_pct_rank"] = atr_rank["ATR_pct_rank"].to_numpy()
    out["ema20_5m_slope"] = out["ema20_5m"].diff().fillna(0.0)
    out["fallback_raw_regime"] = _fallback_regime_from_emas(
        ema9=out["ema9_5m"],
        ema21=out["ema21_5m"],
    )
    out["fallback_low_volume_trend_streak_5m"] = _build_low_volume_trend_streak(
        raw_regimes=out["fallback_raw_regime"],
        rvol_tod_5m=out["rvol_tod_5m"],
    )
    return out


def _build_family_regime_rows(
    *,
    five_minute_by_symbol: Mapping[str, pd.DataFrame],
    families_by_symbol: Mapping[str, Family],
) -> pd.DataFrame:
    available_symbols_by_family: dict[Family, set[str]] = defaultdict(set)
    for symbol, family in families_by_symbol.items():
        available_symbols_by_family[family].add(symbol)

    records: list[dict[str, object]] = []
    for family, members in FAMILY_SYMBOLS.items():
        if not set(members).issubset(available_symbols_by_family.get(family, set())):
            continue

        engine = RegimeEngine(family_symbols={family: members})
        state = engine.initialize_state()
        frames = {symbol: five_minute_by_symbol[symbol].set_index("ts_5m") for symbol in members}
        timestamps = sorted(set().union(*(frame.index for frame in frames.values())))
        for ts in timestamps:
            snapshots: dict[str, SymbolFeatureSnapshot] = {}
            for symbol in members:
                frame = frames[symbol]
                if ts not in frame.index:
                    continue
                row = frame.loc[ts]
                snapshots[symbol] = _symbol_snapshot_from_row(symbol=symbol, row=row)

            if not snapshots:
                continue
            state, _ = engine.step(state=state, snapshots=snapshots)
            family_state = state.family_states[family]
            records.append(
                {
                    "family": family.value,
                    "ts_5m": ts,
                    "raw_regime": family_state.raw_regime.value,
                    "confidence": family_state.confidence,
                    "is_weak_neutral": family_state.is_weak_neutral,
                    "low_volume_trend_streak_5m": family_state.low_volume_trend_streak_5m,
                }
            )

    if not records:
        return pd.DataFrame()
    return pd.DataFrame.from_records(records).sort_values(["family", "ts_5m"]).reset_index(drop=True)


def _apply_family_regimes(*, rows: pd.DataFrame, family_regimes: pd.DataFrame) -> pd.DataFrame:
    if family_regimes.empty or "family" not in family_regimes.columns:
        out = rows.copy()
        out["raw_regime"] = out["raw_regime_fallback"]
        out["confidence"] = out["confidence_fallback"]
        out["is_weak_neutral"] = out["is_weak_neutral_fallback"]
        out["low_volume_trend_streak_5m"] = out["fallback_low_volume_trend_streak_5m"]
        return out.sort_values(["ts", "symbol"]).reset_index(drop=True)

    outputs: list[pd.DataFrame] = []
    for family_name, frame in rows.groupby("family", sort=False):
        part = frame.sort_values("ts").copy()
        regime_part = family_regimes.loc[family_regimes["family"] == family_name].copy()
        if regime_part.empty:
            part["raw_regime"] = part["raw_regime_fallback"]
            part["confidence"] = part["confidence_fallback"]
            part["is_weak_neutral"] = part["is_weak_neutral_fallback"]
            part["low_volume_trend_streak_5m"] = part["fallback_low_volume_trend_streak_5m"]
            outputs.append(part)
            continue

        merged = pd.merge_asof(
            part,
            regime_part.drop(columns=["family"]).sort_values("ts_5m"),
            left_on="ts",
            right_on="ts_5m",
            direction="backward",
        )
        merged["raw_regime"] = merged["raw_regime"].fillna(merged["raw_regime_fallback"])
        merged["confidence"] = merged["confidence"].fillna(merged["confidence_fallback"])
        merged["is_weak_neutral"] = merged["is_weak_neutral"].fillna(merged["is_weak_neutral_fallback"])
        merged["low_volume_trend_streak_5m"] = merged["low_volume_trend_streak_5m"].fillna(
            merged["fallback_low_volume_trend_streak_5m"]
        )
        outputs.append(merged)

    return pd.concat(outputs, ignore_index=True).sort_values(["ts", "symbol"]).reset_index(drop=True)


def _finalize_rows(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["session_vwap"] = out["session_vwap"].fillna(out["close"])
    out["ema9_5m"] = out["ema9_5m"].fillna(out["close"])
    out["ema21_5m"] = out["ema21_5m"].fillna(out["close"])
    out["ema20_5m_slope"] = out["ema20_5m_slope"].fillna(0.0)
    out["atr_14_5m"] = out["atr_14_5m"].fillna(0.0)
    out["atr_14_1m_price"] = out["atr_14_1m_price"].fillna(0.0)
    out["low_volume_trend_streak_5m"] = out["low_volume_trend_streak_5m"].fillna(0).astype(int)
    out["vol_strong_1m"] = out["vol_strong_1m"].fillna(False).astype(bool)
    out["data_ok"] = out["data_ok"].fillna(False).astype(bool)
    out["quote_ok"] = out["quote_ok"].fillna(True).astype(bool)
    out["trade_eligible"] = out["trade_eligible"].fillna(False).astype(bool)
    out["lockout"] = out["lockout"].fillna(False).astype(bool)
    out["family_freeze"] = out["family_freeze"].fillna(False).astype(bool)
    out["raw_regime"] = out["raw_regime"].fillna(Regime.NEUTRAL.value)
    out["is_weak_neutral"] = out["is_weak_neutral"].fillna(False).astype(bool)
    out["confidence"] = out["confidence"].fillna(0.6).astype(float)
    return out[_FINAL_COLUMNS].sort_values(["ts", "symbol"]).reset_index(drop=True)


def _symbol_snapshot_from_row(*, symbol: str, row: pd.Series) -> SymbolFeatureSnapshot:
    required_ready = (
        pd.notna(row["adx_14"])
        and pd.notna(row["er_20"])
        and pd.notna(row["vwap_slope_norm"])
        and pd.notna(row["atr_pct_rank"])
    )
    return SymbolFeatureSnapshot(
        symbol=symbol,
        adx_14=float(row["adx_14"]) if pd.notna(row["adx_14"]) else 0.0,
        er_20=float(row["er_20"]) if pd.notna(row["er_20"]) else 0.0,
        vwap_slope_norm=float(row["vwap_slope_norm"]) if pd.notna(row["vwap_slope_norm"]) else 0.0,
        atr_pct_rank=float(row["atr_pct_rank"]) if pd.notna(row["atr_pct_rank"]) else 0.0,
        rvol_tod_5m=float(row["rvol_tod_5m"]) if pd.notna(row["rvol_tod_5m"]) else None,
        data_ok=bool(row["data_ok_5m"]) and required_ready,
    )


def _evaluate_data_ok(*, ts_values: pd.Series, family: Family) -> pd.Series:
    previous_ts = None
    flags: list[bool] = []
    for ts in ts_values:
        bar_ts = ts.to_pydatetime() if isinstance(ts, pd.Timestamp) else ts
        result = evaluate_bar_timing(
            family=family,
            current_bar_ts=bar_ts,
            previous_bar_ts=previous_ts,
            is_active_session=is_active_session(bar_ts, family),
        )
        flags.append(result.data_ok)
        previous_ts = bar_ts
    return pd.Series(flags, index=ts_values.index, dtype=bool)


def _fallback_regime_from_emas(*, ema9: pd.Series, ema21: pd.Series) -> pd.Series:
    return pd.Series(
        np.where(
            ema9 > ema21,
            Regime.TREND.value,
            np.where(ema9 < ema21, Regime.CHOP.value, Regime.NEUTRAL.value),
        ),
        index=ema9.index,
        dtype=object,
    )


def _build_low_volume_trend_streak(*, raw_regimes: pd.Series, rvol_tod_5m: pd.Series) -> pd.Series:
    streaks: list[int] = []
    streak = 0
    for raw_regime, rvol in zip(raw_regimes, rvol_tod_5m, strict=False):
        low_volume = pd.notna(rvol) and float(rvol) < 0.70
        if raw_regime == Regime.TREND.value and low_volume:
            streak += 1
        else:
            streak = 0
        streaks.append(streak)
    return pd.Series(streaks, index=raw_regimes.index, dtype=int)


def _normalize_timestamps(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="raise")
    if ts.dt.tz is None:
        return ts.dt.tz_localize("America/New_York")
    return ts.dt.tz_convert("America/New_York")


def _resolve_family(*, symbol: str, instruments_by_symbol: Mapping[str, InstrumentMeta]) -> Family:
    instrument = instruments_by_symbol.get(symbol)
    if instrument is not None:
        return instrument.family
    family = _FALLBACK_FAMILY_BY_SYMBOL.get(symbol)
    if family is None:
        raise ValueError(f"Unsupported replay symbol without instrument metadata: {symbol}")
    return family
