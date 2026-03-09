"""Backtest report extraction and file emission."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

_TRADE_COLUMNS = [
    "trade_id",
    "strategy",
    "symbol",
    "side",
    "entry_time_et",
    "exit_time_et",
    "entry_price",
    "exit_price",
    "qty",
    "gross_pnl",
    "net_pnl",
    "realized_R",
    "exit_reason",
]

_EQUITY_COLUMNS = [
    "timestamp_et",
    "equity",
    "realized_pnl",
    "unrealized_pnl",
    "drawdown",
]


def read_ndjson_events(path: str | Path) -> list[dict[str, Any]]:
    event_path = Path(path)
    if not event_path.exists():
        return []

    events: list[dict[str, Any]] = []
    for line in event_path.read_text(encoding="utf-8").splitlines():
        payload = line.strip()
        if payload:
            events.append(json.loads(payload))
    return events


def build_trades_report(events: list[dict[str, Any]]) -> pd.DataFrame:
    singles: dict[str, dict[str, Any]] = {}
    pair_entries: dict[str, dict[str, Any]] = {}
    closed_records: list[dict[str, Any]] = []

    for event in events:
        kind = str(event.get("event", ""))
        position_id = str(event.get("position_id", ""))
        if not position_id:
            continue

        if kind == "entry_filled":
            singles[position_id] = {
                "strategy": str(event.get("strategy", "unknown")),
                "symbol": str(event.get("symbol", "unknown")),
                "side": str(event.get("side", "unknown")),
                "entry_time_et": event.get("ts"),
                "entry_price": float(event.get("fill_price", 0.0) or 0.0),
                "qty": int(event.get("qty", 0) or 0),
                "gross_pnl": 0.0,
                "net_pnl": 0.0,
                "exit_price_weighted_sum": 0.0,
                "exit_qty_sum": 0,
                "exit_time_et": event.get("ts"),
                "exit_reason": None,
            }
            continue

        if kind == "position_update":
            state = singles.get(position_id)
            if state is None:
                continue
            qty_closed = int(event.get("qty_closed", 0) or 0)
            exit_price = float(event.get("exit_price", state["entry_price"]) or state["entry_price"])
            state["gross_pnl"] += float(event.get("gross_pnl_delta", event.get("realized_pnl_delta", 0.0)) or 0.0)
            state["net_pnl"] += float(event.get("realized_pnl_delta", 0.0) or 0.0)
            state["exit_price_weighted_sum"] += exit_price * qty_closed
            state["exit_qty_sum"] += qty_closed
            state["exit_time_et"] = event.get("ts", state["exit_time_et"])
            state["exit_reason"] = event.get("exit_reason", state["exit_reason"])
            continue

        if kind == "position_closed":
            state = singles.pop(position_id, None)
            if state is None:
                continue

            qty = int(event.get("contracts_initial", state["qty"]) or state["qty"])
            exit_qty_sum = int(state["exit_qty_sum"])
            if exit_qty_sum > 0:
                exit_price = float(state["exit_price_weighted_sum"]) / exit_qty_sum
            else:
                exit_price = float(event.get("entry_price", state["entry_price"]) or state["entry_price"])
            net_pnl = float(event.get("realized_pnl", state["net_pnl"]) or state["net_pnl"])
            gross_pnl = float(state["gross_pnl"])
            if exit_qty_sum == 0 and gross_pnl == 0.0:
                gross_pnl = net_pnl

            initial_risk = float(event.get("initial_risk_dollars", 0.0) or 0.0)
            closed_records.append(
                {
                    "strategy": str(event.get("strategy", state["strategy"])),
                    "symbol": str(event.get("symbol", state["symbol"])),
                    "side": str(event.get("side", state["side"])),
                    "entry_time_et": state["entry_time_et"],
                    "exit_time_et": event.get("ts", state["exit_time_et"]),
                    "entry_price": float(event.get("entry_price", state["entry_price"]) or state["entry_price"]),
                    "exit_price": exit_price,
                    "qty": qty,
                    "gross_pnl": gross_pnl,
                    "net_pnl": net_pnl,
                    "realized_R": (net_pnl / initial_risk) if initial_risk > 0.0 else 0.0,
                    "exit_reason": str(event.get("exit_reason", state["exit_reason"]) or ""),
                }
            )
            continue

        if kind == "pair_entry_filled":
            lead_symbol = str(event.get("lead_symbol", "lead"))
            hedge_symbol = str(event.get("hedge_symbol", "hedge"))
            pair_entries[position_id] = {
                "strategy": str(event.get("strategy", "unknown")),
                "symbol": f"{lead_symbol}-{hedge_symbol}",
                "side": str(event.get("side", "unknown")),
                "entry_time_et": event.get("ts"),
                "entry_price": float(event.get("entry_spread", 0.0) or 0.0),
                "qty": int(event.get("lead_qty", 0) or 0),
            }
            continue

        if kind == "pair_position_closed":
            lead_symbol = str(event.get("lead_symbol", "lead"))
            hedge_symbol = str(event.get("hedge_symbol", "hedge"))
            state = pair_entries.pop(position_id, None)
            initial_risk = float(event.get("initial_risk_dollars", 0.0) or 0.0)
            net_pnl = float(event.get("realized_pnl", 0.0) or 0.0)
            closed_records.append(
                {
                    "strategy": str(event.get("strategy", state["strategy"] if state else "unknown")),
                    "symbol": state["symbol"] if state else f"{lead_symbol}-{hedge_symbol}",
                    "side": str(event.get("side", state["side"] if state else "unknown")),
                    "entry_time_et": state["entry_time_et"] if state else event.get("ts"),
                    "exit_time_et": event.get("ts"),
                    "entry_price": float(event.get("entry_spread", state["entry_price"] if state else 0.0) or 0.0),
                    "exit_price": float(event.get("exit_spread", state["entry_price"] if state else 0.0) or 0.0),
                    "qty": int(event.get("lead_qty", state["qty"] if state else 0) or 0),
                    "gross_pnl": float(event.get("gross_pnl", net_pnl) or net_pnl),
                    "net_pnl": net_pnl,
                    "realized_R": (net_pnl / initial_risk) if initial_risk > 0.0 else 0.0,
                    "exit_reason": str(event.get("exit_reason", event.get("reason", "")) or ""),
                }
            )

    if not closed_records:
        return pd.DataFrame(columns=_TRADE_COLUMNS)

    trades = pd.DataFrame.from_records(closed_records)
    trades["entry_time_et"] = pd.to_datetime(trades["entry_time_et"], errors="raise")
    trades["exit_time_et"] = pd.to_datetime(trades["exit_time_et"], errors="raise")
    trades = trades.sort_values(["exit_time_et", "entry_time_et", "strategy", "symbol"]).reset_index(drop=True)
    trades.insert(0, "trade_id", [f"trade_{idx:06d}" for idx in range(1, len(trades) + 1)])
    return trades[_TRADE_COLUMNS]


def build_equity_curve(
    *,
    trades: pd.DataFrame,
    initial_equity: float,
    start_ts: pd.Timestamp | None = None,
) -> pd.DataFrame:
    if trades.empty:
        anchor = start_ts if start_ts is not None else pd.Timestamp("1970-01-01T00:00:00", tz="America/New_York")
        return pd.DataFrame(
            [
                {
                    "timestamp_et": anchor,
                    "equity": float(initial_equity),
                    "realized_pnl": 0.0,
                    "unrealized_pnl": 0.0,
                    "drawdown": 0.0,
                }
            ]
        )

    ordered = trades.sort_values(["exit_time_et", "entry_time_et", "trade_id"]).reset_index(drop=True)
    anchor = start_ts if start_ts is not None else ordered["entry_time_et"].iloc[0]

    records = [
        {
            "timestamp_et": anchor,
            "equity": float(initial_equity),
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "drawdown": 0.0,
        }
    ]
    realized_pnl = 0.0
    peak_equity = float(initial_equity)

    for trade in ordered.itertuples(index=False):
        realized_pnl += float(trade.net_pnl)
        equity = float(initial_equity) + realized_pnl
        peak_equity = max(peak_equity, equity)
        records.append(
            {
                "timestamp_et": trade.exit_time_et,
                "equity": equity,
                "realized_pnl": realized_pnl,
                "unrealized_pnl": 0.0,
                "drawdown": equity - peak_equity,
            }
        )

    return pd.DataFrame.from_records(records, columns=_EQUITY_COLUMNS)


def write_backtest_reports(
    *,
    out_dir: str | Path,
    trades: pd.DataFrame,
    equity_curve: pd.DataFrame,
    summary: dict[str, Any],
) -> dict[str, Path]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    trades_path = output_dir / "trades.csv"
    equity_curve_path = output_dir / "equity_curve.csv"
    summary_path = output_dir / "summary.json"

    _write_frame_csv(path=trades_path, frame=trades)
    _write_frame_csv(path=equity_curve_path, frame=equity_curve)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True, default=str), encoding="utf-8")

    return {
        "trades_path": trades_path,
        "equity_curve_path": equity_curve_path,
        "summary_path": summary_path,
    }


def _write_frame_csv(*, path: Path, frame: pd.DataFrame) -> None:
    output = frame.copy()
    for column in output.columns:
        if pd.api.types.is_datetime64_any_dtype(output[column]):
            output[column] = output[column].map(_isoformat)
    output.to_csv(path, index=False, float_format="%.6f")


def _isoformat(value: Any) -> str:
    if pd.isna(value):
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
