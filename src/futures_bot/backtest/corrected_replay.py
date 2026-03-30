"""Replay harness for validating the corrected futures signal architecture."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from futures_bot.backtest.validation_reports import write_validation_reports
from futures_bot.config.models import GoldStrategyConfig, NQStrategyConfig, YMStrategyConfig
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
    effective_anchor_ts: datetime | None
    current_session_date: str
    stage_events: tuple[StageEvent, ...]


@dataclass(frozen=True, slots=True)
class ReplayValidationResult:
    records: tuple[ReplayValidationRecord, ...]
    summary: dict[str, Any]
    paths: dict[str, Path]


def run_corrected_validation_replay(
    *,
    data_path: str | Path,
    out_dir: str | Path,
    instruments_by_symbol: Mapping[str, InstrumentMeta],
    nq_config: NQStrategyConfig,
    ym_config: YMStrategyConfig,
    gold_config: GoldStrategyConfig,
) -> ReplayValidationResult:
    replay_rows = _load_replay_rows(data_path)
    orchestrator = CorrectedSignalOrchestrator(
        nq_config=nq_config,
        ym_config=ym_config,
        gold_config=gold_config,
    )
    history_by_symbol: dict[str, list[dict[str, object]]] = {}
    records: list[ReplayValidationRecord] = []

    for row in replay_rows.itertuples(index=False):
        symbol = str(row.symbol)
        if symbol not in instruments_by_symbol:
            continue

        history = history_by_symbol.setdefault(symbol, [])
        history.append(
            {
                "ts": row.ts,
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
                "volume": float(row.volume),
            }
        )
        bars_1m = pd.DataFrame(history)
        request = _build_request(
            row=row,
            bars_1m=bars_1m,
            instrument=instruments_by_symbol[symbol],
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
                effective_anchor_ts=effective_anchor_timestamp(session_state, ts=row.ts.to_pydatetime()),
                current_session_date=session_state.current_session.session_date.isoformat(),
                stage_events=output.stage_events,
            )
        )

    events_path = _write_events(out_dir=out_dir, records=records)
    signals_by_instrument = build_signals_by_instrument(records)
    rejections_by_reason = build_rejections_by_reason(records)
    daily_halt_occurrences = build_daily_halt_occurrences(records)
    signal_frequency_by_instrument = build_signal_frequency_by_instrument(records, replay_rows)
    summary = build_validation_summary(
        records=records,
        signals_by_instrument=signals_by_instrument,
        rejections_by_reason=rejections_by_reason,
        daily_halt_occurrences=daily_halt_occurrences,
        signal_frequency_by_instrument=signal_frequency_by_instrument,
    )
    paths = {"events_path": events_path}
    paths.update(
        write_validation_reports(
            out_dir=out_dir,
            signals_by_instrument=signals_by_instrument,
            rejections_by_reason=rejections_by_reason,
            daily_halt_occurrences=daily_halt_occurrences,
            signal_frequency_by_instrument=signal_frequency_by_instrument,
            summary=summary,
        )
    )
    return ReplayValidationResult(records=tuple(records), summary=summary, paths=paths)


def build_signals_by_instrument(records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]) -> pd.DataFrame:
    accepted = [record for record in records if record.outcome is ReplayOutcome.ACCEPTED_SIGNAL]
    if not accepted:
        return pd.DataFrame(columns=["symbol", "accepted_signal_count"])
    frame = pd.DataFrame({"symbol": [record.symbol for record in accepted]})
    out = frame.value_counts().rename("accepted_signal_count").reset_index()
    return out.sort_values("symbol").reset_index(drop=True)


def build_rejections_by_reason(records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]) -> pd.DataFrame:
    rejected = [record for record in records if record.rejection_reason is not None]
    if not rejected:
        return pd.DataFrame(columns=["rejection_reason", "count"])
    frame = pd.DataFrame({"rejection_reason": [record.rejection_reason for record in rejected]})
    out = frame.value_counts().rename("count").reset_index()
    return out.sort_values("rejection_reason").reset_index(drop=True)


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
    )
    signals = build_signals_by_instrument(records).rename(columns={"accepted_signal_count": "accepted_signals"})
    merged = active_days.merge(signals, how="left", on="symbol").fillna({"accepted_signals": 0})
    merged["average_signal_frequency"] = merged["accepted_signals"] / merged["active_days"]
    return merged.sort_values("symbol").reset_index(drop=True)


def build_validation_summary(
    *,
    records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...],
    signals_by_instrument: pd.DataFrame,
    rejections_by_reason: pd.DataFrame,
    daily_halt_occurrences: pd.DataFrame,
    signal_frequency_by_instrument: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "record_count": len(records),
        "accepted_signal_count": int(sum(record.outcome is ReplayOutcome.ACCEPTED_SIGNAL for record in records)),
        "signals_by_instrument": signals_by_instrument.to_dict(orient="records"),
        "rejections_by_reason": rejections_by_reason.to_dict(orient="records"),
        "daily_halt_occurrences": daily_halt_occurrences.to_dict(orient="records"),
        "average_signal_frequency_by_instrument": signal_frequency_by_instrument.to_dict(orient="records"),
        "note": "Validation counts only. This report does not prove win rate or profitability.",
    }


def _load_replay_rows(data_path: str | Path) -> pd.DataFrame:
    rows = pd.read_csv(data_path)
    missing = _REQUIRED_COLUMNS.difference(rows.columns)
    if missing:
        raise ValueError(f"Replay CSV missing required columns: {sorted(missing)}")
    rows = rows.copy()
    rows["symbol"] = rows["symbol"].astype(str).str.strip().str.upper()
    rows["ts"] = pd.to_datetime(rows["timestamp_et"], errors="raise")
    rows = rows.sort_values(["ts", "symbol"], kind="mergesort").reset_index(drop=True)
    return rows


def _build_request(
    *,
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
    if instrument.symbol == nq_symbol:
        return NQEvaluationRequest(
            **base_kwargs,
            pullback_price=_row_float(row, "pullback_price", default=float(row.close)),
            structure_break_price=_row_float(row, "structure_break_price", default=float(row.close)),
            order_block_low=_row_float(row, "order_block_low", default=float(row.close)),
            order_block_high=_row_float(row, "order_block_high", default=float(row.close)),
        )
    if instrument.symbol == ym_symbol:
        return YMEvaluationRequest(**base_kwargs)
    if instrument.symbol == gold_symbol:
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
    payload["stage_events"] = [
        {
            "stage": event.stage.value,
            "status": event.status.value,
            "reason": event.reason,
        }
        for event in record.stage_events
    ]
    return payload


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
