"""Replay harness for validating the corrected futures signal architecture."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence, TextIO

import pandas as pd

from futures_bot.backtest.validation_reports import write_validation_reports
from futures_bot.config.models import GoldStrategyConfig, NQStrategyConfig, YMStrategyConfig
from futures_bot.core.enums import Family
from futures_bot.core.types import InstrumentMeta
from futures_bot.features import effective_anchor_timestamp
from futures_bot.pipeline.corrected_orchestrator import (
    AcceptedSignalOutput,
    CorrectedSignalOrchestrator,
    DailyHaltRejectedSignalOutput,
    GoldEvaluationRequest,
    LiquidityNewsRejectedSignalOutput,
    NQEvaluationRequest,
    RejectedSignalOutput,
    RiskRejectedSignalOutput,
    StageEvent,
    YMEvaluationRequest,
)
from futures_bot.risk.models import OpenPositionMtmSnapshot

_REQUIRED_COLUMNS = {"timestamp_et", "symbol", "open", "high", "low", "close", "volume"}


class ReplayOutcome(str, Enum):
    ACCEPTED_SIGNAL = "accepted_signal"
    REJECTED_SIGNAL = "rejected_signal"
    REJECTED_RISK = "rejected_due_to_risk"
    REJECTED_DAILY_HALT = "rejected_due_to_daily_halt"
    REJECTED_LIQUIDITY_NEWS = "rejected_due_to_liquidity_news"


@dataclass(frozen=True, slots=True)
class ReplayValidationRecord:
    ts: datetime
    symbol: str
    outcome: ReplayOutcome
    rejection_reason: str | None
    strategy: str | None
    setup: str | None
    side: str | None
    score: float | None
    contracts: int | None
    state_metric_name: str | None
    state_metric_value: float | None
    effective_anchor_ts: datetime | None
    current_session_date: str
    stage_events: tuple[StageEvent, ...]


@dataclass(frozen=True, slots=True)
class ReplayValidationResult:
    records: tuple[ReplayValidationRecord, ...]
    summary: dict[str, Any]
    paths: dict[str, Path]


@dataclass(frozen=True, slots=True)
class AcceptedSignalDiagnostics:
    unique_accepted_records: tuple[ReplayValidationRecord, ...]
    repeated_accepted_signals: pd.DataFrame
    repeated_accepted_signals_by_instrument: pd.DataFrame


@dataclass(slots=True)
class _AcceptedSignalRepeatState:
    signature: tuple[object, ...]
    last_ts: datetime
    last_unique_ts: datetime
    run_id: int
    consecutive_repeat_count: int = 0


def run_corrected_validation_replay(
    *,
    data_path: str | Path,
    out_dir: str | Path,
    instruments_by_symbol: Mapping[str, InstrumentMeta],
    nq_config: NQStrategyConfig,
    ym_config: YMStrategyConfig,
    gold_config: GoldStrategyConfig,
    progress_every: int | None = None,
    progress_stream: TextIO | None = None,
) -> ReplayValidationResult:
    replay_rows = _load_replay_rows(data_path)
    return _run_corrected_validation_replay_rows(
        replay_rows=replay_rows,
        out_dir=out_dir,
        instruments_by_symbol=instruments_by_symbol,
        nq_config=nq_config,
        ym_config=ym_config,
        gold_config=gold_config,
        progress_every=progress_every,
        progress_stream=progress_stream,
    )


def _run_corrected_validation_replay_rows(
    *,
    replay_rows: pd.DataFrame,
    out_dir: str | Path,
    instruments_by_symbol: Mapping[str, InstrumentMeta],
    nq_config: NQStrategyConfig,
    ym_config: YMStrategyConfig,
    gold_config: GoldStrategyConfig,
    progress_every: int | None = None,
    progress_stream: TextIO | None = None,
) -> ReplayValidationResult:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    orchestrator = CorrectedSignalOrchestrator(
        nq_config=nq_config,
        ym_config=ym_config,
        gold_config=gold_config,
    )
    rows_by_symbol = {
        symbol: frame.reset_index(drop=True)
        for symbol, frame in replay_rows.groupby("symbol", sort=False)
    }
    processed_counts_by_symbol: dict[str, int] = {}
    records: list[ReplayValidationRecord] = []
    total_rows = len(replay_rows.index)
    interval = _progress_interval(total_rows=total_rows, requested=progress_every)

    try:
        _log_progress(
            stream=progress_stream,
            message=f"starting corrected replay for {total_rows} rows -> {output_dir}",
        )
        for row_number, row in enumerate(replay_rows.itertuples(index=False), start=1):
            symbol = str(row.symbol)
            instrument = _resolve_replay_instrument(symbol=symbol, instruments_by_symbol=instruments_by_symbol)

            processed_count = processed_counts_by_symbol.get(symbol, 0) + 1
            processed_counts_by_symbol[symbol] = processed_count
            bars_1m = rows_by_symbol[symbol].iloc[:processed_count]
            request = _build_request(
                replay_symbol=symbol,
                row=row,
                bars_1m=bars_1m,
                instrument=instrument,
                nq_symbol=nq_config.symbol,
                ym_symbol=ym_config.symbol,
                gold_symbol=gold_config.symbol,
            )
            if request is None:
                continue

            if isinstance(request, NQEvaluationRequest):
                output = orchestrator.evaluate_nq(request)
            elif isinstance(request, YMEvaluationRequest):
                output = orchestrator.evaluate_ym(request)
            else:
                output = orchestrator.evaluate_gold(request)

            session_state = output.session_state
            records.append(
                ReplayValidationRecord(
                    ts=row.ts.to_pydatetime(),
                    symbol=symbol,
                    outcome=_outcome_for_output(output),
                    rejection_reason=_reason_for_output(output),
                    strategy=_strategy_for_output(output),
                    setup=_setup_for_output(output),
                    side=_side_for_output(output),
                    score=_score_for_output(output),
                    contracts=_contracts_for_output(output),
                    state_metric_name=_state_metric_name_for_output(output),
                    state_metric_value=_state_metric_value_for_output(output),
                    effective_anchor_ts=effective_anchor_timestamp(session_state, ts=row.ts.to_pydatetime()),
                    current_session_date=session_state.current_session.session_date.isoformat(),
                    stage_events=output.stage_events,
                )
            )

            if interval is not None and (row_number % interval == 0 or row_number == total_rows):
                _log_progress(
                    stream=progress_stream,
                    message=(
                        f"processed {row_number}/{total_rows} rows "
                        f"({(100.0 * row_number) / float(total_rows):.1f}%)"
                    ),
                )
    except Exception as exc:
        raise RuntimeError(
            f"Corrected replay failed while processing rows into '{output_dir}'"
        ) from exc

    if not records:
        raise ValueError("Replay produced no evaluation records for the supplied input.")

    events_path = _write_events(out_dir=output_dir, records=records)
    accepted_signal_diagnostics = build_accepted_signal_diagnostics(records)
    accepted_signals = build_accepted_signals(accepted_signal_diagnostics.unique_accepted_records)
    rejected_signals = build_rejected_signals(records)
    signals_by_instrument = build_signals_by_instrument(accepted_signal_diagnostics.unique_accepted_records)
    rejection_reason_counts = build_rejections_by_reason(records)
    rejection_reason_counts_by_instrument = build_rejections_by_reason_by_instrument(records)
    daily_halt_events = build_daily_halt_events(records)
    daily_halt_occurrences = build_daily_halt_occurrences(records)
    signal_frequency_by_instrument = build_signal_frequency_by_instrument(
        accepted_signal_diagnostics.unique_accepted_records,
        replay_rows,
    )
    instrument_diagnostics = build_instrument_diagnostics(
        records=records,
        unique_accepted_records=accepted_signal_diagnostics.unique_accepted_records,
        rejection_reason_counts_by_instrument=rejection_reason_counts_by_instrument,
        repeated_accepted_signals_by_instrument=accepted_signal_diagnostics.repeated_accepted_signals_by_instrument,
    )
    summary = build_validation_summary(
        records=records,
        unique_accepted_records=accepted_signal_diagnostics.unique_accepted_records,
        signals_by_instrument=signals_by_instrument,
        rejection_reason_counts=rejection_reason_counts,
        rejection_reason_counts_by_instrument=rejection_reason_counts_by_instrument,
        daily_halt_events=daily_halt_events,
        daily_halt_occurrences=daily_halt_occurrences,
        signal_frequency_by_instrument=signal_frequency_by_instrument,
        repeated_accepted_signals=accepted_signal_diagnostics.repeated_accepted_signals,
        repeated_accepted_signals_by_instrument=accepted_signal_diagnostics.repeated_accepted_signals_by_instrument,
        instrument_diagnostics=instrument_diagnostics,
    )
    paths = {"events_path": events_path}
    paths.update(
        write_validation_reports(
            out_dir=output_dir,
            accepted_signals=accepted_signals,
            rejected_signals=rejected_signals,
            rejection_reason_counts=rejection_reason_counts,
            rejection_reason_counts_by_instrument=rejection_reason_counts_by_instrument,
            signal_frequency_by_instrument=signal_frequency_by_instrument,
            daily_halt_events=daily_halt_events,
            repeated_accepted_signals=accepted_signal_diagnostics.repeated_accepted_signals,
            repeated_accepted_signals_by_instrument=accepted_signal_diagnostics.repeated_accepted_signals_by_instrument,
            instrument_diagnostics=instrument_diagnostics,
            summary=summary,
        )
    )
    _log_progress(
        stream=progress_stream,
        message=f"wrote corrected replay reports to {output_dir}",
    )
    return ReplayValidationResult(records=tuple(records), summary=summary, paths=paths)


def build_signals_by_instrument(records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]) -> pd.DataFrame:
    accepted = [record for record in records if record.outcome is ReplayOutcome.ACCEPTED_SIGNAL]
    if not accepted:
        return pd.DataFrame(columns=["instrument", "accepted_signal_count"])
    frame = pd.DataFrame({"instrument": [record.symbol for record in accepted]})
    out = frame.value_counts().rename("accepted_signal_count").reset_index()
    return out.sort_values("instrument").reset_index(drop=True)


def build_accepted_signals(records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]) -> pd.DataFrame:
    rows = [
        {
            "ts": record.ts.isoformat(),
            "instrument": record.symbol,
            "outcome": record.outcome.value,
            "strategy": record.strategy,
            "setup": record.setup,
            "side": record.side,
            "score": record.score,
            "contracts": record.contracts,
            "state_metric_name": record.state_metric_name,
            "state_metric_value": record.state_metric_value,
            "effective_anchor_ts": _isoformat_or_none(record.effective_anchor_ts),
            "current_session_date": record.current_session_date,
            "stage_events_json": _stage_events_json(record.stage_events),
        }
        for record in records
        if record.outcome is ReplayOutcome.ACCEPTED_SIGNAL
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "ts",
            "instrument",
            "outcome",
            "strategy",
            "setup",
            "side",
            "score",
            "contracts",
            "state_metric_name",
            "state_metric_value",
            "effective_anchor_ts",
            "current_session_date",
            "stage_events_json",
        ],
    )


def build_rejected_signals(records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]) -> pd.DataFrame:
    rows = [
        {
            "ts": record.ts.isoformat(),
            "instrument": record.symbol,
            "outcome": record.outcome.value,
            "rejection_reason": record.rejection_reason,
            "effective_anchor_ts": _isoformat_or_none(record.effective_anchor_ts),
            "current_session_date": record.current_session_date,
            "stage_events_json": _stage_events_json(record.stage_events),
        }
        for record in records
        if record.outcome is not ReplayOutcome.ACCEPTED_SIGNAL
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "ts",
            "instrument",
            "outcome",
            "rejection_reason",
            "effective_anchor_ts",
            "current_session_date",
            "stage_events_json",
        ],
    )


def build_rejections_by_reason(records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]) -> pd.DataFrame:
    rejected = [record for record in records if record.rejection_reason is not None]
    if not rejected:
        return pd.DataFrame(columns=["rejection_reason", "count"])
    frame = pd.DataFrame({"rejection_reason": [record.rejection_reason for record in rejected]})
    out = frame.value_counts().rename("count").reset_index()
    return out.sort_values("rejection_reason").reset_index(drop=True)


def build_rejections_by_reason_by_instrument(
    records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]
) -> pd.DataFrame:
    rejected = [record for record in records if record.rejection_reason is not None]
    if not rejected:
        return pd.DataFrame(columns=["instrument", "rejection_reason", "count"])
    frame = pd.DataFrame(
        {
            "instrument": [record.symbol for record in rejected],
            "rejection_reason": [record.rejection_reason for record in rejected],
        }
    )
    out = frame.value_counts().rename("count").reset_index()
    return out.sort_values(["instrument", "rejection_reason"]).reset_index(drop=True)


def build_daily_halt_events(records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]) -> pd.DataFrame:
    rows = [
        {
            "ts": record.ts.isoformat(),
            "instrument": record.symbol,
            "rejection_reason": record.rejection_reason,
            "effective_anchor_ts": _isoformat_or_none(record.effective_anchor_ts),
            "current_session_date": record.current_session_date,
        }
        for record in records
        if record.outcome is ReplayOutcome.REJECTED_DAILY_HALT
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "ts",
            "instrument",
            "rejection_reason",
            "effective_anchor_ts",
            "current_session_date",
        ],
    )


def build_daily_halt_occurrences(records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]) -> pd.DataFrame:
    halted = [record for record in records if record.outcome is ReplayOutcome.REJECTED_DAILY_HALT]
    if not halted:
        return pd.DataFrame([{"daily_halt_occurrences": 0}])
    return pd.DataFrame([{"daily_halt_occurrences": len(halted)}])


def build_signal_frequency_by_instrument(
    records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...],
    replay_rows: pd.DataFrame,
) -> pd.DataFrame:
    active_days = (
        replay_rows.assign(session_date=replay_rows["ts"].dt.date)
        .groupby("symbol")["session_date"]
        .nunique()
        .rename("active_days")
        .reset_index()
        .rename(columns={"symbol": "instrument"})
    )
    signals = build_signals_by_instrument(records)
    merged = active_days.merge(signals, how="left", on="instrument").fillna({"accepted_signal_count": 0})
    merged["accepted_signal_count"] = merged["accepted_signal_count"].astype(int)
    merged["accepted_signals_per_active_day"] = merged["accepted_signal_count"] / merged["active_days"]
    return merged.sort_values("instrument").reset_index(drop=True)


def build_accepted_signal_diagnostics(
    records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]
) -> AcceptedSignalDiagnostics:
    unique_accepted_records: list[ReplayValidationRecord] = []
    repeated_rows: list[dict[str, object]] = []
    states: dict[str, _AcceptedSignalRepeatState] = {}
    next_run_id = 1

    for record in records:
        state = states.get(record.symbol)
        if record.outcome is not ReplayOutcome.ACCEPTED_SIGNAL:
            if state is not None:
                states.pop(record.symbol, None)
            continue

        signature = _accepted_signal_signature(record)
        is_repeat = (
            state is not None
            and record.ts - state.last_ts == timedelta(minutes=1)
            and signature == state.signature
        )
        if not is_repeat:
            unique_accepted_records.append(record)
            states[record.symbol] = _AcceptedSignalRepeatState(
                signature=signature,
                last_ts=record.ts,
                last_unique_ts=record.ts,
                run_id=next_run_id,
            )
            next_run_id += 1
            continue

        state.last_ts = record.ts
        state.consecutive_repeat_count += 1
        repeated_rows.append(
            {
                "ts": record.ts.isoformat(),
                "instrument": record.symbol,
                "strategy": record.strategy,
                "setup": record.setup,
                "side": record.side,
                "score": record.score,
                "contracts": record.contracts,
                "state_metric_name": record.state_metric_name,
                "state_metric_value": record.state_metric_value,
                "previous_unique_signal_ts": state.last_unique_ts.isoformat(),
                "repeat_run_id": state.run_id,
                "consecutive_repeat_count": state.consecutive_repeat_count,
                "repeat_reason": "same_strategy_setup_on_consecutive_bar_without_state_change",
            }
        )

    repeated = pd.DataFrame(
        repeated_rows,
        columns=[
            "ts",
            "instrument",
            "strategy",
            "setup",
            "side",
            "score",
            "contracts",
            "state_metric_name",
            "state_metric_value",
            "previous_unique_signal_ts",
            "repeat_run_id",
            "consecutive_repeat_count",
            "repeat_reason",
        ],
    )
    if repeated.empty:
        repeated_by_instrument = pd.DataFrame(
            columns=[
                "instrument",
                "repeated_accepted_signal_count",
                "repeat_run_count",
                "max_consecutive_repeat_count",
            ]
        )
    else:
        repeated_by_instrument = (
            repeated.groupby("instrument", dropna=False)
            .agg(
                repeated_accepted_signal_count=("instrument", "size"),
                repeat_run_count=("repeat_run_id", "nunique"),
                max_consecutive_repeat_count=("consecutive_repeat_count", "max"),
            )
            .reset_index()
            .sort_values("instrument")
            .reset_index(drop=True)
        )
    return AcceptedSignalDiagnostics(
        unique_accepted_records=tuple(unique_accepted_records),
        repeated_accepted_signals=repeated,
        repeated_accepted_signals_by_instrument=repeated_by_instrument,
    )


def build_instrument_diagnostics(
    *,
    records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...],
    unique_accepted_records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...],
    rejection_reason_counts_by_instrument: pd.DataFrame,
    repeated_accepted_signals_by_instrument: pd.DataFrame,
) -> pd.DataFrame:
    instruments = sorted({record.symbol for record in records})
    accepted_counts = (
        build_signals_by_instrument(unique_accepted_records)
        .rename(columns={"accepted_signal_count": "accepted_signal_count"})
        .set_index("instrument")
    )
    repeated_counts = (
        repeated_accepted_signals_by_instrument.set_index("instrument")
        if not repeated_accepted_signals_by_instrument.empty
        else pd.DataFrame(
            columns=[
                "repeated_accepted_signal_count",
                "repeat_run_count",
                "max_consecutive_repeat_count",
            ]
        )
    )
    rejected_counts: dict[str, int] = {}
    for record in records:
        if record.rejection_reason is not None:
            rejected_counts[record.symbol] = rejected_counts.get(record.symbol, 0) + 1

    top_reason_rows: dict[str, tuple[str | None, int]] = {}
    if not rejection_reason_counts_by_instrument.empty:
        ordered = rejection_reason_counts_by_instrument.sort_values(
            ["instrument", "count", "rejection_reason"],
            ascending=[True, False, True],
        )
        for instrument, frame in ordered.groupby("instrument", sort=False):
            row = frame.iloc[0]
            top_reason_rows[str(instrument)] = (str(row["rejection_reason"]), int(row["count"]))

    rows: list[dict[str, object]] = []
    for instrument in instruments:
        accepted_signal_count = int(accepted_counts.loc[instrument, "accepted_signal_count"]) if instrument in accepted_counts.index else 0
        repeated_signal_count = (
            int(repeated_counts.loc[instrument, "repeated_accepted_signal_count"])
            if instrument in repeated_counts.index
            else 0
        )
        repeat_run_count = int(repeated_counts.loc[instrument, "repeat_run_count"]) if instrument in repeated_counts.index else 0
        max_repeat_count = (
            int(repeated_counts.loc[instrument, "max_consecutive_repeat_count"])
            if instrument in repeated_counts.index
            else 0
        )
        top_reason, top_reason_count = top_reason_rows.get(instrument, (None, 0))
        rows.append(
            {
                "instrument": instrument,
                "accepted_signal_count": accepted_signal_count,
                "rejected_signal_count": rejected_counts.get(instrument, 0),
                "repeated_accepted_signal_count": repeated_signal_count,
                "repeat_run_count": repeat_run_count,
                "max_consecutive_repeat_count": max_repeat_count,
                "top_rejection_reason": top_reason,
                "top_rejection_count": top_reason_count,
                "probable_zero_signal_cause": _probable_zero_signal_cause(
                    accepted_signal_count=accepted_signal_count,
                    top_rejection_reason=top_reason,
                ),
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "instrument",
            "accepted_signal_count",
            "rejected_signal_count",
            "repeated_accepted_signal_count",
            "repeat_run_count",
            "max_consecutive_repeat_count",
            "top_rejection_reason",
            "top_rejection_count",
            "probable_zero_signal_cause",
        ],
    )


def build_validation_summary(
    *,
    records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...],
    unique_accepted_records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...],
    signals_by_instrument: pd.DataFrame,
    rejection_reason_counts: pd.DataFrame,
    rejection_reason_counts_by_instrument: pd.DataFrame,
    daily_halt_events: pd.DataFrame,
    daily_halt_occurrences: pd.DataFrame,
    signal_frequency_by_instrument: pd.DataFrame,
    repeated_accepted_signals: pd.DataFrame,
    repeated_accepted_signals_by_instrument: pd.DataFrame,
    instrument_diagnostics: pd.DataFrame,
) -> dict[str, Any]:
    accepted_signal_count = len(unique_accepted_records)
    rejected_signal_count = int(sum(record.rejection_reason is not None for record in records))
    risk_skip_count = int(sum(record.outcome is ReplayOutcome.REJECTED_RISK for record in records))
    daily_halt_event_count = int(sum(record.outcome is ReplayOutcome.REJECTED_DAILY_HALT for record in records))
    return {
        "record_count": len(records),
        "accepted_signal_count": accepted_signal_count,
        "rejected_signal_count": rejected_signal_count,
        "risk_skip_count": risk_skip_count,
        "daily_halt_event_count": daily_halt_event_count,
        "accepted_signals_by_instrument": signals_by_instrument.to_dict(orient="records"),
        "signals_by_instrument": signals_by_instrument.to_dict(orient="records"),
        "rejection_reason_counts": rejection_reason_counts.to_dict(orient="records"),
        "rejections_by_reason": rejection_reason_counts.to_dict(orient="records"),
        "rejection_reason_counts_by_instrument": rejection_reason_counts_by_instrument.to_dict(orient="records"),
        "daily_halt_events": daily_halt_events.to_dict(orient="records"),
        "daily_halt_occurrences": daily_halt_occurrences.to_dict(orient="records"),
        "signal_frequency_by_instrument": signal_frequency_by_instrument.to_dict(orient="records"),
        "average_signal_frequency_by_instrument": signal_frequency_by_instrument.to_dict(orient="records"),
        "repeated_accepted_signal_count": int(len(repeated_accepted_signals.index)),
        "repeated_accepted_signals_by_instrument": repeated_accepted_signals_by_instrument.to_dict(orient="records"),
        "instrument_diagnostics": instrument_diagnostics.to_dict(orient="records"),
    }


def _progress_interval(*, total_rows: int, requested: int | None) -> int | None:
    if total_rows <= 0:
        return None
    if requested is not None:
        return max(1, requested)
    return max(1_000, total_rows // 20)


def _log_progress(*, stream: TextIO | None, message: str) -> None:
    if stream is None:
        return
    print(f"[corrected_replay] {message}", file=stream, flush=True)


def _load_replay_rows(data_path: str | Path) -> pd.DataFrame:
    rows = pd.read_csv(data_path)
    missing = _REQUIRED_COLUMNS.difference(rows.columns)
    if missing:
        raise ValueError(f"Replay CSV missing required columns: {sorted(missing)}")
    rows = rows.copy()
    rows["symbol"] = rows["symbol"].astype(str).str.strip().str.upper().map(_normalize_replay_symbol)
    rows["ts"] = pd.to_datetime(rows["timestamp_et"], errors="raise")
    for column in ("open", "high", "low", "close", "volume"):
        rows[column] = pd.to_numeric(rows[column], errors="raise")
    rows = rows.sort_values(["ts", "symbol"], kind="mergesort").reset_index(drop=True)
    return rows


def _normalize_replay_symbol(symbol: str) -> str:
    normalized = str(symbol).strip().upper()
    if normalized in {"GOLD", "GC", "MGC"}:
        return "GOLD"
    return normalized


def _build_request(
    *,
    replay_symbol: str,
    row: Any,
    bars_1m: pd.DataFrame,
    instrument: InstrumentMeta,
    nq_symbol: str,
    ym_symbol: str,
    gold_symbol: str,
) -> NQEvaluationRequest | YMEvaluationRequest | GoldEvaluationRequest | None:
    base_kwargs = {
        "bars_1m": bars_1m,
        "instrument": instrument,
        "session_start_equity": _row_float(row, "session_start_equity", default=100_000.0),
        "realized_pnl": _row_float(row, "realized_pnl", default=0.0),
        "open_positions": _open_positions_from_row(row),
        "liquidity_ok": _row_bool(row, "liquidity_ok", default=True),
        "macro_blocked": _row_bool(row, "macro_blocked", default=False),
        "choch_confirmed": _row_bool(row, "choch_confirmed", default=False),
        "fvg_present": _row_bool(row, "fvg_present", default=False),
        "intermarket_confirmed": _row_optional_bool(row, "intermarket_confirmed"),
    }
    if replay_symbol == nq_symbol:
        pullback_price, structure_break_price, order_block_low, order_block_high = _nq_structural_levels(row)
        return NQEvaluationRequest(
            **base_kwargs,
            pullback_price=pullback_price,
            structure_break_price=structure_break_price,
            order_block_low=order_block_low,
            order_block_high=order_block_high,
        )
    if replay_symbol == ym_symbol:
        return YMEvaluationRequest(**base_kwargs)
    if replay_symbol == "GOLD" or instrument.symbol == gold_symbol:
        return GoldEvaluationRequest(
            **base_kwargs,
            pullback_price=_row_optional_float(row, "pullback_price"),
            structure_break_price=_row_optional_float(row, "structure_break_price"),
            order_block_low=_row_optional_float(row, "order_block_low"),
            order_block_high=_row_optional_float(row, "order_block_high"),
        )
    return None


def _open_positions_from_row(row: Any) -> tuple[OpenPositionMtmSnapshot, ...]:
    symbol = _row_optional_str(row, "open_position_symbol")
    if symbol is None:
        return ()
    return (
        OpenPositionMtmSnapshot(
            ts=row.ts.to_pydatetime(),
            symbol=symbol,
            quantity=int(_row_float(row, "open_position_quantity", default=0.0)),
            avg_entry_price=_row_float(row, "open_position_avg_entry_price", default=0.0),
            mark_price=_row_float(row, "open_position_mark_price", default=0.0),
            point_value=_row_float(row, "open_position_point_value", default=0.0),
        ),
    )


def _outcome_for_output(
    output: AcceptedSignalOutput
    | RejectedSignalOutput
    | RiskRejectedSignalOutput
    | DailyHaltRejectedSignalOutput
    | LiquidityNewsRejectedSignalOutput
    | Any,
) -> ReplayOutcome:
    if isinstance(output, AcceptedSignalOutput):
        return ReplayOutcome.ACCEPTED_SIGNAL
    if isinstance(output, RiskRejectedSignalOutput):
        return ReplayOutcome.REJECTED_RISK
    if isinstance(output, LiquidityNewsRejectedSignalOutput):
        return ReplayOutcome.REJECTED_LIQUIDITY_NEWS
    if isinstance(output, DailyHaltRejectedSignalOutput):
        return ReplayOutcome.REJECTED_DAILY_HALT
    return ReplayOutcome.REJECTED_SIGNAL


def _reason_for_output(output: Any) -> str | None:
    if isinstance(output, AcceptedSignalOutput):
        return None
    return getattr(output, "rejection_reason", None)


def _strategy_for_output(output: Any) -> str | None:
    if isinstance(output, AcceptedSignalOutput):
        return output.signal.strategy.value
    return None


def _setup_for_output(output: Any) -> str | None:
    if isinstance(output, AcceptedSignalOutput):
        return output.candidate.setup.value
    return None


def _side_for_output(output: Any) -> str | None:
    if isinstance(output, AcceptedSignalOutput):
        return output.signal.side.value
    return None


def _score_for_output(output: Any) -> float | None:
    if isinstance(output, AcceptedSignalOutput):
        return float(output.signal.score)
    return None


def _contracts_for_output(output: Any) -> int | None:
    if isinstance(output, AcceptedSignalOutput):
        return int(output.sizing.contracts)
    return None


def _state_metric_name_for_output(output: Any) -> str | None:
    if not isinstance(output, AcceptedSignalOutput):
        return None
    if hasattr(output.candidate, "vwap_distance_atr"):
        return "vwap_distance_atr"
    return None


def _state_metric_value_for_output(output: Any) -> float | None:
    if not isinstance(output, AcceptedSignalOutput):
        return None
    if hasattr(output.candidate, "vwap_distance_atr"):
        return float(output.candidate.vwap_distance_atr)
    return None


def _write_events(*, out_dir: str | Path, records: list[ReplayValidationRecord]) -> Path:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "validation_events.ndjson"
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(_record_to_dict(record), sort_keys=True, default=str))
            handle.write("\n")
    return path


def _record_to_dict(record: ReplayValidationRecord) -> dict[str, Any]:
    payload = asdict(record)
    payload["outcome"] = record.outcome.value
    payload["stage_events"] = _stage_events_payload(record.stage_events)
    return payload


def _stage_events_payload(stage_events: Sequence[StageEvent]) -> list[dict[str, str]]:
    return [
        {
            "stage": event.stage.value,
            "status": event.status.value,
            "reason": event.reason,
        }
        for event in stage_events
    ]


def _stage_events_json(stage_events: Sequence[StageEvent]) -> str:
    return json.dumps(_stage_events_payload(stage_events), sort_keys=True)


def _isoformat_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _row_bool(row: Any, field: str, *, default: bool) -> bool:
    raw = getattr(row, field, default)
    if pd.isna(raw):
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "y"}


def _row_optional_bool(row: Any, field: str) -> bool | None:
    raw = getattr(row, field, None)
    if raw is None or pd.isna(raw):
        return None
    if isinstance(raw, bool):
        return raw
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "y"}:
        return True
    if value in {"0", "false", "no", "n"}:
        return False
    return None


def _row_float(row: Any, field: str, *, default: float) -> float:
    raw = getattr(row, field, default)
    if raw is None or pd.isna(raw):
        return float(default)
    return float(raw)


def _row_optional_float(row: Any, field: str) -> float | None:
    raw = getattr(row, field, None)
    if raw is None or pd.isna(raw):
        return None
    return float(raw)


def _row_optional_str(row: Any, field: str) -> str | None:
    raw = getattr(row, field, None)
    if raw is None or pd.isna(raw):
        return None
    return str(raw).strip()


def _resolve_replay_instrument(
    *,
    symbol: str,
    instruments_by_symbol: Mapping[str, InstrumentMeta],
) -> InstrumentMeta:
    candidate_symbols = [symbol]
    if symbol == "GOLD":
        candidate_symbols.extend(["MGC", "GC"])
    elif symbol in {"MGC", "GC"}:
        candidate_symbols.append("GOLD")
    for candidate_symbol in candidate_symbols:
        instrument = instruments_by_symbol.get(candidate_symbol)
        if instrument is not None:
            return instrument
    raise ValueError(f"Replay symbol '{symbol}' is not configured in instruments_by_symbol.")


def _nq_structural_levels(row: Any) -> tuple[float, float, float, float]:
    order_block_low = _row_optional_float(row, "order_block_low")
    order_block_high = _row_optional_float(row, "order_block_high")
    pullback_price = _row_optional_float(row, "pullback_price")
    structure_break_price = _row_optional_float(row, "structure_break_price")
    candle_low = min(float(row.open), float(row.close), float(row.low))
    candle_high = max(float(row.open), float(row.close), float(row.high))
    if order_block_low is None:
        order_block_low = candle_low
    if order_block_high is None:
        order_block_high = candle_high
    if pullback_price is None:
        pullback_price = (order_block_low + order_block_high) / 2.0
    if structure_break_price is None:
        structure_break_price = float(row.open)
    return pullback_price, structure_break_price, order_block_low, order_block_high


def _accepted_signal_signature(record: ReplayValidationRecord) -> tuple[object, ...]:
    return (
        record.strategy,
        record.setup,
        record.side,
        record.current_session_date,
        _isoformat_or_none(record.effective_anchor_ts),
        record.contracts,
        _round_or_none(record.score, digits=1),
        record.state_metric_name,
        _round_or_none(record.state_metric_value, digits=1),
    )


def _round_or_none(value: float | None, *, digits: int) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _probable_zero_signal_cause(*, accepted_signal_count: int, top_rejection_reason: str | None) -> str | None:
    if accepted_signal_count > 0 or top_rejection_reason is None:
        return None
    if top_rejection_reason == "indicator_data_unavailable":
        return "indicator_availability"
    if top_rejection_reason == "symbol_not_in_scope":
        return "symbol_routing_bug"
    if top_rejection_reason in {"HARD_RISK_CAP_EXCEEDED", "SIZE_LT_ONE", "SIZE_LT_ONE_AFTER_MICRO_ROUTING"}:
        return "risk_sizing_mismatch"
    return "over_restrictive_gates"


def _default_instrument(symbol: str) -> InstrumentMeta:
    if symbol == "NQ":
        return InstrumentMeta(
            symbol="NQ",
            root_symbol="NQ",
            family=Family.EQUITIES,
            tick_size=0.25,
            tick_value=5.0,
            point_value=20.0,
            commission_rt=4.8,
            symbol_type="future",
            micro_equivalent="MNQ",
            contract_units=1.0,
        )
    if symbol == "YM":
        return InstrumentMeta(
            symbol="YM",
            root_symbol="YM",
            family=Family.EQUITIES,
            tick_size=1.0,
            tick_value=5.0,
            point_value=5.0,
            commission_rt=4.8,
            symbol_type="future",
            micro_equivalent="MYM",
            contract_units=1.0,
        )
    if symbol in {"GC", "MGC", "GOLD"}:
        return InstrumentMeta(
            symbol="MGC",
            root_symbol="GC",
            family=Family.METALS,
            tick_size=0.1,
            tick_value=1.0,
            point_value=10.0,
            commission_rt=4.8,
            symbol_type="future",
            micro_equivalent="MGC",
            contract_units=1.0,
        )
    raise ValueError(f"Unsupported corrected replay symbol: {symbol}")


def _build_default_runtime(
    replay_rows: pd.DataFrame,
) -> tuple[dict[str, InstrumentMeta], NQStrategyConfig, YMStrategyConfig, GoldStrategyConfig]:
    symbols = {str(symbol).upper() for symbol in replay_rows["symbol"].unique()}
    instruments = {
        "NQ": _default_instrument("NQ"),
        "YM": _default_instrument("YM"),
    }
    if "GOLD" in symbols:
        instruments["GOLD"] = _default_instrument("GOLD")
    return (
        instruments,
        NQStrategyConfig(hard_risk_per_trade_dollars=750.0, daily_halt_loss_dollars=1_500.0),
        YMStrategyConfig(hard_risk_per_trade_dollars=500.0, daily_halt_loss_dollars=1_500.0),
        GoldStrategyConfig(
            hard_risk_per_trade_dollars=400.0,
            daily_halt_loss_dollars=1_200.0,
            symbol="MGC",
        ),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic corrected replay validation over historical 1m OHLCV bars."
    )
    parser.add_argument("--data", required=True, help="Path to the historical 1m OHLCV CSV input.")
    parser.add_argument("--out", required=True, help="Directory where factual replay reports will be written.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    output_dir = Path(args.out)
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        replay_rows = _load_replay_rows(args.data)
        instruments_by_symbol, nq_config, ym_config, gold_config = _build_default_runtime(replay_rows)
        _run_corrected_validation_replay_rows(
            replay_rows=replay_rows,
            out_dir=output_dir,
            instruments_by_symbol=instruments_by_symbol,
            nq_config=nq_config,
            ym_config=ym_config,
            gold_config=gold_config,
            progress_stream=sys.stderr,
        )
    except ValueError as exc:
        parser.exit(2, f"error: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
