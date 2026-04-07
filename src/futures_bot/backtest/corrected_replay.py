"""Replay harness for validating the corrected futures signal architecture."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

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
    return _run_corrected_validation_replay_rows(
        replay_rows=replay_rows,
        out_dir=out_dir,
        instruments_by_symbol=instruments_by_symbol,
        nq_config=nq_config,
        ym_config=ym_config,
        gold_config=gold_config,
    )


def _run_corrected_validation_replay_rows(
    *,
    replay_rows: pd.DataFrame,
    out_dir: str | Path,
    instruments_by_symbol: Mapping[str, InstrumentMeta],
    nq_config: NQStrategyConfig,
    ym_config: YMStrategyConfig,
    gold_config: GoldStrategyConfig,
) -> ReplayValidationResult:
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

    if not records:
        raise ValueError("Replay produced no evaluation records for the supplied input.")

    events_path = _write_events(out_dir=out_dir, records=records)
    accepted_signals = build_accepted_signals(records)
    rejected_signals = build_rejected_signals(records)
    signals_by_instrument = build_signals_by_instrument(records)
    rejection_reason_counts = build_rejections_by_reason(records)
    daily_halt_events = build_daily_halt_events(records)
    daily_halt_occurrences = build_daily_halt_occurrences(records)
    signal_frequency_by_instrument = build_signal_frequency_by_instrument(records, replay_rows)
    summary = build_validation_summary(
        records=records,
        signals_by_instrument=signals_by_instrument,
        rejection_reason_counts=rejection_reason_counts,
        daily_halt_events=daily_halt_events,
        daily_halt_occurrences=daily_halt_occurrences,
        signal_frequency_by_instrument=signal_frequency_by_instrument,
    )
    paths = {"events_path": events_path}
    paths.update(
        write_validation_reports(
            out_dir=out_dir,
            accepted_signals=accepted_signals,
            rejected_signals=rejected_signals,
            rejection_reason_counts=rejection_reason_counts,
            signal_frequency_by_instrument=signal_frequency_by_instrument,
            daily_halt_events=daily_halt_events,
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


def build_accepted_signals(records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]) -> pd.DataFrame:
    rows = [
        {
            "ts": record.ts.isoformat(),
            "symbol": record.symbol,
            "outcome": record.outcome.value,
            "strategy": record.strategy,
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
            "symbol",
            "outcome",
            "strategy",
            "effective_anchor_ts",
            "current_session_date",
            "stage_events_json",
        ],
    )


def build_rejected_signals(records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]) -> pd.DataFrame:
    rows = [
        {
            "ts": record.ts.isoformat(),
            "symbol": record.symbol,
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
            "symbol",
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


def build_daily_halt_events(records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...]) -> pd.DataFrame:
    rows = [
        {
            "ts": record.ts.isoformat(),
            "symbol": record.symbol,
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
            "symbol",
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
    )
    signals = build_signals_by_instrument(records)
    merged = active_days.merge(signals, how="left", on="symbol").fillna({"accepted_signal_count": 0})
    merged["accepted_signal_count"] = merged["accepted_signal_count"].astype(int)
    merged["accepted_signals_per_active_day"] = merged["accepted_signal_count"] / merged["active_days"]
    return merged.sort_values("symbol").reset_index(drop=True)


def build_validation_summary(
    *,
    records: list[ReplayValidationRecord] | tuple[ReplayValidationRecord, ...],
    signals_by_instrument: pd.DataFrame,
    rejection_reason_counts: pd.DataFrame,
    daily_halt_events: pd.DataFrame,
    daily_halt_occurrences: pd.DataFrame,
    signal_frequency_by_instrument: pd.DataFrame,
) -> dict[str, Any]:
    accepted_signal_count = int(sum(record.outcome is ReplayOutcome.ACCEPTED_SIGNAL for record in records))
    rejected_signal_count = len(records) - accepted_signal_count
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
        "daily_halt_events": daily_halt_events.to_dict(orient="records"),
        "daily_halt_occurrences": daily_halt_occurrences.to_dict(orient="records"),
        "signal_frequency_by_instrument": signal_frequency_by_instrument.to_dict(orient="records"),
        "average_signal_frequency_by_instrument": signal_frequency_by_instrument.to_dict(orient="records"),
    }


def _load_replay_rows(data_path: str | Path) -> pd.DataFrame:
    rows = pd.read_csv(data_path)
    missing = _REQUIRED_COLUMNS.difference(rows.columns)
    if missing:
        raise ValueError(f"Replay CSV missing required columns: {sorted(missing)}")
    rows = rows.copy()
    rows["symbol"] = rows["symbol"].astype(str).str.strip().str.upper()
    rows["ts"] = pd.to_datetime(rows["timestamp_et"], errors="raise")
    for column in ("open", "high", "low", "close", "volume"):
        rows[column] = pd.to_numeric(rows[column], errors="raise")
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
    if symbol == "GC":
        return InstrumentMeta(
            symbol="GC",
            root_symbol="GC",
            family=Family.METALS,
            tick_size=0.1,
            tick_value=10.0,
            point_value=100.0,
            commission_rt=4.8,
            symbol_type="future",
            micro_equivalent="MGC",
            contract_units=1.0,
        )
    if symbol == "MGC":
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
    if "GC" in symbols and "MGC" in symbols:
        raise ValueError("Replay input cannot mix GC and MGC in the standalone corrected replay CLI.")

    gold_symbol = "MGC" if "MGC" in symbols else "GC"
    instruments = {
        "NQ": _default_instrument("NQ"),
        "YM": _default_instrument("YM"),
        gold_symbol: _default_instrument(gold_symbol),
    }
    return (
        instruments,
        NQStrategyConfig(hard_risk_per_trade_dollars=750.0, daily_halt_loss_dollars=1_500.0),
        YMStrategyConfig(hard_risk_per_trade_dollars=5.0, daily_halt_loss_dollars=1_500.0),
        GoldStrategyConfig(
            hard_risk_per_trade_dollars=400.0,
            daily_halt_loss_dollars=1_200.0,
            symbol=gold_symbol,
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
    try:
        replay_rows = _load_replay_rows(args.data)
        instruments_by_symbol, nq_config, ym_config, gold_config = _build_default_runtime(replay_rows)
        _run_corrected_validation_replay_rows(
            replay_rows=replay_rows,
            out_dir=args.out,
            instruments_by_symbol=instruments_by_symbol,
            nq_config=nq_config,
            ym_config=ym_config,
            gold_config=gold_config,
        )
    except ValueError as exc:
        parser.exit(2, f"error: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
