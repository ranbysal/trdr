"""Deterministic historical replay runner."""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from futures_bot.backtest.data_adapter import PreparedReplayData, prepare_replay_data
from futures_bot.backtest.metrics import compute_backtest_metrics
from futures_bot.backtest.reports import (
    build_equity_curve,
    build_trades_report,
    read_ndjson_events,
    write_backtest_reports,
)
from futures_bot.core.enums import StrategyModule
from futures_bot.core.types import InstrumentMeta
from futures_bot.pipeline.multistrategy_paper import MultiStrategyPaperEngine


def run_replay_backtest(
    *,
    data_path: str | Path,
    out_dir: str | Path,
    instruments_by_symbol: dict[str, InstrumentMeta],
    enabled_strategies: set[StrategyModule],
    initial_equity: float = 100_000.0,
    config_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    prepared = prepare_replay_data(
        data_path=data_path,
        instruments_by_symbol=instruments_by_symbol,
    )

    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "trade_logs.json"

    engine = MultiStrategyPaperEngine(
        log_path=log_path,
        instruments_by_symbol=instruments_by_symbol,
        enabled_strategies=enabled_strategies,
    )
    for row in prepared.rows.to_dict(orient="records"):
        engine.process_row(row)
    engine.flush()

    events = read_ndjson_events(log_path)
    trades = build_trades_report(events)
    if trades.empty:
        raise RuntimeError("Backtest completed without any closed trades")

    equity_curve = build_equity_curve(
        trades=trades,
        initial_equity=initial_equity,
        start_ts=prepared.start_ts,
    )
    summary = compute_backtest_metrics(trades=trades, equity_curve=equity_curve)
    summary["date_range"] = {
        "start": prepared.start_ts.isoformat(),
        "end": prepared.end_ts.isoformat(),
    }
    summary["strategies_requested"] = sorted(strategy.value for strategy in enabled_strategies)
    if config_snapshot is not None:
        summary["config_hash"] = _config_hash(config_snapshot)

    paths = write_backtest_reports(
        out_dir=output_dir,
        trades=trades,
        equity_curve=equity_curve,
        summary=summary,
    )
    return {
        **paths,
        "log_path": log_path,
        "summary": summary,
        "prepared": PreparedReplayData(
            rows=prepared.rows,
            start_ts=prepared.start_ts,
            end_ts=prepared.end_ts,
            symbols=prepared.symbols,
        ),
    }


def _config_hash(config_snapshot: Mapping[str, Any]) -> str:
    payload = json.dumps(config_snapshot, sort_keys=True, default=str, separators=(",", ":"))
    return sha256(payload.encode("utf-8")).hexdigest()
