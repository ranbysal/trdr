"""Historical 1m replay runner and report emitter."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd

from futures_bot.backtest.metrics import compute_backtest_metrics
from futures_bot.core.enums import StrategyModule
from futures_bot.core.types import InstrumentMeta
from futures_bot.pipeline.multistrategy_paper import run_multistrategy_paper_loop

_REQUIRED_COLUMNS = {"timestamp_et", "symbol", "open", "high", "low", "close", "volume"}


def run_replay_backtest(
    *,
    data_path: str | Path,
    out_dir: str | Path,
    instruments_by_symbol: dict[str, InstrumentMeta],
    enabled_strategies: set[StrategyModule],
    initial_equity: float = 100_000.0,
) -> dict[str, Any]:
    df = pd.read_csv(data_path)
    missing = _REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    enriched = _build_feature_frame(df)
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory() as tmp:
        tmp_csv = Path(tmp) / "enriched.csv"
        enriched.to_csv(tmp_csv, index=False)
        log_path = run_multistrategy_paper_loop(
            data_path=tmp_csv,
            out_dir=out_path,
            instruments_by_symbol=instruments_by_symbol,
            enabled_strategies=enabled_strategies,
        )

    events = _read_ndjson(log_path)
    trades = _extract_trades(events)
    equity_curve = _build_equity_curve(trades, initial_equity=initial_equity)
    summary = compute_backtest_metrics(trades, equity_curve)

    trades_path = out_path / "trades.csv"
    equity_path = out_path / "equity_curve.csv"
    summary_path = out_path / "summary.json"

    trades.to_csv(trades_path, index=False)
    equity_curve.to_csv(equity_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "trades_path": trades_path,
        "equity_curve_path": equity_path,
        "summary_path": summary_path,
        "summary": summary,
    }


def _build_feature_frame(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df["timestamp_et"] = pd.to_datetime(df["timestamp_et"], utc=False)
    df = df.sort_values(["timestamp_et", "symbol"]).reset_index(drop=True)
    df["bucket_5m"] = df["timestamp_et"].dt.floor("5min")

    df["date"] = df["timestamp_et"].dt.date
    df["pv"] = df["close"] * df["volume"]
    df["cum_pv"] = df.groupby(["symbol", "date"])["pv"].cumsum()
    df["cum_vol"] = df.groupby(["symbol", "date"])["volume"].cumsum().replace(0.0, 1.0)
    df["session_vwap"] = df["cum_pv"] / df["cum_vol"]

    prev_close = df.groupby("symbol")["close"].shift(1).fillna(df["close"])
    tr_1m = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr_14_1m_price"] = tr_1m.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=1).mean())

    five = _build_5m_features(df)
    df = df.merge(
        five,
        how="left",
        on=["symbol", "bucket_5m"],
    )

    vol_med = df.groupby("symbol")["volume"].transform(lambda s: s.rolling(20, min_periods=1).median())
    df["vol_strong_1m"] = df["volume"] >= vol_med

    df["raw_regime"] = "neutral"
    df.loc[df["ema9_5m"] > df["ema21_5m"], "raw_regime"] = "trend"
    df.loc[df["ema9_5m"] < df["ema21_5m"], "raw_regime"] = "chop"

    dist_vwap = (df["close"] - df["session_vwap"]).abs()
    weak_neutral = (df["raw_regime"] == "neutral") & (dist_vwap <= (0.3 * df["atr_14_5m"]))
    df["is_weak_neutral"] = weak_neutral
    df["confidence"] = 1.0
    df.loc[weak_neutral, "confidence"] = 0.5

    df["data_ok"] = True
    df["quote_ok"] = True
    df["trade_eligible"] = True
    df["lockout"] = False
    df["family_freeze"] = False

    return df[
        [
            "timestamp_et",
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
    ].rename(columns={"timestamp_et": "ts"})


def _build_5m_features(df: pd.DataFrame) -> pd.DataFrame:
    base = df[["timestamp_et", "symbol", "open", "high", "low", "close", "volume"]].copy()
    base["bucket_5m"] = base["timestamp_et"].dt.floor("5min")

    agg = (
        base.groupby(["symbol", "bucket_5m"], as_index=False)
        .agg(
            open_5m=("open", "first"),
            high_5m=("high", "max"),
            low_5m=("low", "min"),
            close_5m=("close", "last"),
            volume_5m=("volume", "sum"),
        )
        .sort_values(["symbol", "bucket_5m"])
    )

    agg["ema9_5m"] = agg.groupby("symbol")["close_5m"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    agg["ema21_5m"] = agg.groupby("symbol")["close_5m"].transform(lambda s: s.ewm(span=21, adjust=False).mean())
    ema20 = agg.groupby("symbol")["close_5m"].transform(lambda s: s.ewm(span=20, adjust=False).mean())
    agg["ema20_5m_slope"] = ema20.groupby(agg["symbol"]).diff().fillna(0.0)

    prev_close_5m = agg.groupby("symbol")["close_5m"].shift(1).fillna(agg["close_5m"])
    tr_5m = pd.concat(
        [
            (agg["high_5m"] - agg["low_5m"]).abs(),
            (agg["high_5m"] - prev_close_5m).abs(),
            (agg["low_5m"] - prev_close_5m).abs(),
        ],
        axis=1,
    ).max(axis=1)
    agg["atr_14_5m"] = tr_5m.groupby(agg["symbol"]).transform(lambda s: s.rolling(14, min_periods=1).mean())

    rolling_vol = agg.groupby("symbol")["volume_5m"].transform(lambda s: s.rolling(20, min_periods=1).mean())
    agg["rvol_3bar_aggregate_5m"] = (agg["volume_5m"] / rolling_vol.replace(0.0, 1.0)).clip(lower=0.0)

    return agg[["symbol", "bucket_5m", "ema9_5m", "ema21_5m", "ema20_5m_slope", "atr_14_5m", "rvol_3bar_aggregate_5m"]]


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        events.append(json.loads(text))
    return events


def _extract_trades(events: list[dict[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for event in events:
        kind = event.get("event")
        if kind == "position_closed":
            risk = float(event.get("initial_risk_dollars", 0.0))
            pnl = float(event.get("realized_pnl", 0.0))
            records.append(
                {
                    "timestamp_et": event.get("ts"),
                    "strategy": str(event.get("strategy", "unknown")),
                    "symbol": str(event.get("symbol", "unknown")),
                    "pnl_net": pnl,
                    "initial_risk_dollars": risk,
                    "r_multiple": (pnl / risk) if risk > 0.0 else 0.0,
                }
            )
        if kind == "pair_position_closed":
            risk = float(event.get("initial_risk_dollars", 0.0))
            pnl = float(event.get("realized_pnl", 0.0))
            lead = str(event.get("lead_symbol", "MGC"))
            hedge = str(event.get("hedge_symbol", "SIL"))
            records.append(
                {
                    "timestamp_et": event.get("ts"),
                    "strategy": str(event.get("strategy", StrategyModule.STRAT_D_PAIR.value)),
                    "symbol": f"{lead}-{hedge}",
                    "pnl_net": pnl,
                    "initial_risk_dollars": risk,
                    "r_multiple": (pnl / risk) if risk > 0.0 else 0.0,
                }
            )
    if not records:
        return pd.DataFrame(
            columns=["timestamp_et", "strategy", "symbol", "pnl_net", "initial_risk_dollars", "r_multiple"]
        )
    out = pd.DataFrame.from_records(records)
    out["timestamp_et"] = pd.to_datetime(out["timestamp_et"])
    out = out.sort_values("timestamp_et").reset_index(drop=True)
    return out


def _build_equity_curve(trades: pd.DataFrame, *, initial_equity: float) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            [
                {
                    "timestamp_et": pd.Timestamp("1970-01-01T00:00:00"),
                    "equity": float(initial_equity),
                    "pnl_cum": 0.0,
                }
            ]
        )

    curve = trades[["timestamp_et", "pnl_net"]].copy()
    curve["pnl_cum"] = curve["pnl_net"].cumsum()
    curve["equity"] = float(initial_equity) + curve["pnl_cum"]
    return curve[["timestamp_et", "equity", "pnl_cum"]]
