"""Stateful paper executor for CORR-V4 execution queue events."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from bot_corrected_paper.config import CorrectedPaperConfig
from shared.runtime.ndjson_writer import NdjsonWriter
from shared.runtime.state_store import JsonStateStore


@dataclass(slots=True)
class CorrectedPaperPosition:
    position_id: str
    signal_id: str
    symbol: str
    strategy: str
    setup: str
    direction: str
    quantity: int
    entry_price: float
    stop_price: float
    tp1_price: float
    point_value: float
    opened_at_et: str
    score: float | None = None


@dataclass(slots=True)
class CorrectedPaperTrade:
    position_id: str
    signal_id: str
    symbol: str
    strategy: str
    setup: str
    direction: str
    quantity: int
    entry_price: float
    stop_price: float
    tp1_price: float
    exit_price: float
    closure_outcome: str
    realized_pnl: float
    win_loss: str
    opened_at_et: str
    closed_at_et: str


@dataclass(slots=True)
class CorrectedPaperState:
    source_line_count: int = 0
    realized_pnl: float = 0.0
    processed_signal_ids: set[str] = field(default_factory=set)
    open_positions: dict[str, CorrectedPaperPosition] = field(default_factory=dict)
    closed_trades: list[CorrectedPaperTrade] = field(default_factory=list)
    instrument_stats: dict[str, dict[str, float | int]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ProcessQueueResult:
    processed_records: int
    opened_positions: int
    closed_positions: int
    skipped_records: int


class CorrectedPaperEngine:
    def __init__(self, *, config: CorrectedPaperConfig) -> None:
        self._config = config
        self._state_store = JsonStateStore(config.state_path)
        self._journal = NdjsonWriter(config.journal_path)
        self._events = NdjsonWriter(config.events_path)
        config.state_path.parent.mkdir(parents=True, exist_ok=True)
        config.journal_path.parent.mkdir(parents=True, exist_ok=True)
        config.events_path.parent.mkdir(parents=True, exist_ok=True)
        config.reports_dir.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    def process_queue(self) -> ProcessQueueResult:
        if not self._config.enabled:
            return ProcessQueueResult(0, 0, 0, 0)
        if not self._config.source_queue_path.exists():
            self._save_state()
            self.write_reports()
            return ProcessQueueResult(0, 0, 0, 0)

        raw_lines = self._config.source_queue_path.read_text(encoding="utf-8").splitlines()
        start_index = min(self.state.source_line_count, len(raw_lines))
        if self.state.source_line_count > len(raw_lines):
            start_index = 0
        opened = 0
        closed = 0
        skipped = 0
        processed = 0
        for line in raw_lines[start_index:]:
            processed += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            before_open = len(self.state.open_positions)
            before_closed = len(self.state.closed_trades)
            if not self.process_record(record):
                skipped += 1
            opened += max(0, len(self.state.open_positions) - before_open)
            closed += max(0, len(self.state.closed_trades) - before_closed)
        self.state.source_line_count = len(raw_lines)
        self._save_state()
        self.write_reports()
        self._journal.flush()
        self._events.flush()
        return ProcessQueueResult(processed, opened, closed, skipped)

    def process_record(self, record: Mapping[str, Any]) -> bool:
        event = str(record.get("event", ""))
        if event == "CORRECTED_EXECUTION_SIGNAL":
            return self._open_position(record)
        if event == "CORRECTED_MARKET_BAR":
            return self._mark_positions(record)
        return False

    def open_positions(self) -> tuple[CorrectedPaperPosition, ...]:
        return tuple(self.state.open_positions.values())

    def recent_trades(self, *, limit: int = 10) -> tuple[CorrectedPaperTrade, ...]:
        return tuple(self.state.closed_trades[-limit:])

    def summary(self) -> dict[str, object]:
        wins = sum(1 for trade in self.state.closed_trades if trade.win_loss == "win")
        losses = sum(1 for trade in self.state.closed_trades if trade.win_loss == "loss")
        return {
            "realized_pnl": round(self.state.realized_pnl, 2),
            "open_positions": len(self.state.open_positions),
            "closed_trades": len(self.state.closed_trades),
            "wins": wins,
            "losses": losses,
            "instrument_stats": self.state.instrument_stats,
        }

    def write_reports(self) -> None:
        summary_path = self._config.reports_dir / "summary.json"
        summary_path.write_text(json.dumps(self.summary(), indent=2, sort_keys=True), encoding="utf-8")
        trades_path = self._config.reports_dir / "closed_trades.csv"
        with trades_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "position_id",
                    "signal_id",
                    "symbol",
                    "strategy",
                    "setup",
                    "direction",
                    "quantity",
                    "entry_price",
                    "stop_price",
                    "tp1_price",
                    "exit_price",
                    "closure_outcome",
                    "realized_pnl",
                    "win_loss",
                    "opened_at_et",
                    "closed_at_et",
                ],
            )
            writer.writeheader()
            for trade in self.state.closed_trades:
                writer.writerow(asdict(trade))

    def write_runtime_event(self, event: str, payload: Mapping[str, Any] | None = None) -> None:
        self._write_event(event, payload or {})
        self._events.flush()

    def _open_position(self, record: Mapping[str, Any]) -> bool:
        signal_id = str(record.get("signal_id", "")).strip()
        symbol = str(record.get("symbol", "")).strip().upper()
        if not signal_id or not symbol:
            return False
        if signal_id in self.state.processed_signal_ids:
            return False
        if any(position.symbol == symbol for position in self.state.open_positions.values()):
            self.state.processed_signal_ids.add(signal_id)
            self._write_event("CORRECTED_PAPER_SIGNAL_SKIPPED", record, reason="symbol_already_open")
            return True

        direction = str(record.get("direction", "")).strip().upper()
        if direction not in {"LONG", "SHORT"}:
            return False
        quantity = int(record.get("contracts", record.get("quantity", 0)))
        entry_price = float(record.get("entry_price", 0.0))
        stop_price = float(record.get("stop_price", 0.0))
        tp1_price = float(record.get("tp1_price", _default_tp1(entry_price, stop_price, direction)))
        point_value = float(record.get("point_value", self._config.default_point_values.get(symbol, 1.0)))
        timestamp = str(record.get("timestamp_et", ""))
        if quantity <= 0 or entry_price <= 0.0 or stop_price <= 0.0 or tp1_price <= 0.0 or not timestamp:
            return False

        position = CorrectedPaperPosition(
            position_id=f"corr-paper-{signal_id}",
            signal_id=signal_id,
            symbol=symbol,
            strategy=str(record.get("strategy", "")),
            setup=str(record.get("setup", "")),
            direction=direction,
            quantity=quantity,
            entry_price=entry_price,
            stop_price=stop_price,
            tp1_price=tp1_price,
            point_value=point_value,
            opened_at_et=timestamp,
            score=None if record.get("score") is None else float(record["score"]),
        )
        self.state.processed_signal_ids.add(signal_id)
        self.state.open_positions[position.position_id] = position
        self._bump_stat(symbol, "opened", 1)
        self._write_journal("POSITION_OPENED", asdict(position))
        self._write_event("CORRECTED_PAPER_POSITION_OPENED", asdict(position))
        return True

    def _mark_positions(self, record: Mapping[str, Any]) -> bool:
        symbol = str(record.get("symbol", "")).strip().upper()
        if not symbol:
            return False
        high = float(record.get("high", 0.0))
        low = float(record.get("low", 0.0))
        timestamp = str(record.get("timestamp_et", ""))
        closed_position_ids: list[str] = []
        for position in list(self.state.open_positions.values()):
            if position.symbol != symbol:
                continue
            exit_price: float | None = None
            outcome: str | None = None
            if position.direction == "LONG":
                if low <= position.stop_price:
                    exit_price = position.stop_price
                    outcome = "stop"
                elif high >= position.tp1_price:
                    exit_price = position.tp1_price
                    outcome = "tp1"
            else:
                if high >= position.stop_price:
                    exit_price = position.stop_price
                    outcome = "stop"
                elif low <= position.tp1_price:
                    exit_price = position.tp1_price
                    outcome = "tp1"
            if exit_price is None or outcome is None:
                continue
            closed_position_ids.append(position.position_id)
            self._close_position(position=position, exit_price=exit_price, outcome=outcome, closed_at_et=timestamp)
        return bool(closed_position_ids) or any(position.symbol == symbol for position in self.state.open_positions.values())

    def _close_position(
        self,
        *,
        position: CorrectedPaperPosition,
        exit_price: float,
        outcome: str,
        closed_at_et: str,
    ) -> None:
        signed_qty = position.quantity if position.direction == "LONG" else -position.quantity
        realized_pnl = (exit_price - position.entry_price) * signed_qty * position.point_value
        win_loss = "win" if realized_pnl > 0.0 else "loss" if realized_pnl < 0.0 else "flat"
        trade = CorrectedPaperTrade(
            position_id=position.position_id,
            signal_id=position.signal_id,
            symbol=position.symbol,
            strategy=position.strategy,
            setup=position.setup,
            direction=position.direction,
            quantity=position.quantity,
            entry_price=position.entry_price,
            stop_price=position.stop_price,
            tp1_price=position.tp1_price,
            exit_price=exit_price,
            closure_outcome=outcome,
            realized_pnl=round(realized_pnl, 2),
            win_loss=win_loss,
            opened_at_et=position.opened_at_et,
            closed_at_et=closed_at_et,
        )
        self.state.open_positions.pop(position.position_id, None)
        self.state.closed_trades.append(trade)
        self.state.realized_pnl = round(self.state.realized_pnl + trade.realized_pnl, 2)
        self._bump_stat(position.symbol, "closed", 1)
        self._bump_stat(position.symbol, "realized_pnl", trade.realized_pnl)
        if win_loss == "win":
            self._bump_stat(position.symbol, "wins", 1)
        elif win_loss == "loss":
            self._bump_stat(position.symbol, "losses", 1)
        self._write_journal("POSITION_CLOSED", asdict(trade))
        self._write_event("CORRECTED_PAPER_POSITION_CLOSED", asdict(trade))

    def _bump_stat(self, symbol: str, field_name: str, delta: float | int) -> None:
        stats = self.state.instrument_stats.setdefault(
            symbol,
            {"opened": 0, "closed": 0, "wins": 0, "losses": 0, "realized_pnl": 0.0},
        )
        stats[field_name] = stats.get(field_name, 0) + delta
        if field_name == "realized_pnl":
            stats[field_name] = round(float(stats[field_name]), 2)

    def _write_journal(self, event: str, payload: Mapping[str, Any]) -> None:
        self._journal.write({"event": event, **dict(payload)})

    def _write_event(self, event: str, payload: Mapping[str, Any], *, reason: str | None = None) -> None:
        record = {"event": event, **dict(payload)}
        if reason is not None:
            record["reason"] = reason
        self._events.write(record)

    def _load_state(self) -> CorrectedPaperState:
        raw = self._state_store.load()
        state = CorrectedPaperState()
        state.source_line_count = int(raw.get("source_line_count", 0))
        state.realized_pnl = float(raw.get("realized_pnl", 0.0))
        state.processed_signal_ids = {str(item) for item in raw.get("processed_signal_ids", [])}
        state.instrument_stats = {
            str(symbol): dict(stats)
            for symbol, stats in raw.get("instrument_stats", {}).items()
            if isinstance(stats, dict)
        }
        for item in raw.get("open_positions", []):
            if isinstance(item, dict):
                position = CorrectedPaperPosition(**item)
                state.open_positions[position.position_id] = position
        for item in raw.get("closed_trades", []):
            if isinstance(item, dict):
                state.closed_trades.append(CorrectedPaperTrade(**item))
        return state

    def _save_state(self) -> None:
        self._state_store.save(
            {
                "source_line_count": self.state.source_line_count,
                "realized_pnl": self.state.realized_pnl,
                "processed_signal_ids": sorted(self.state.processed_signal_ids),
                "open_positions": [asdict(position) for position in self.state.open_positions.values()],
                "closed_trades": [asdict(trade) for trade in self.state.closed_trades],
                "instrument_stats": self.state.instrument_stats,
                "updated_at_et": datetime.now().astimezone().isoformat(),
            }
        )


def _default_tp1(entry_price: float, stop_price: float, direction: str) -> float:
    risk = abs(entry_price - stop_price)
    return entry_price + risk if direction == "LONG" else entry_price - risk
