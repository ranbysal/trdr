"""SQLite-backed paper trade journal for Bot 3."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

from bot_exec_v3.models import (
    Direction,
    FillType,
    OpenPositionRecord,
    OrderStatus,
    PendingOrderRecord,
    PositionStatus,
    SignalEvent,
    SignalStatus,
)


class PaperTradeJournal:
    def __init__(self, sqlite_path: str | Path) -> None:
        self.path = Path(sqlite_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    signal_id TEXT PRIMARY KEY,
                    source_bot TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    setup_type TEXT NOT NULL,
                    session TEXT NOT NULL,
                    confluence REAL NOT NULL,
                    formed_timestamp_et TEXT NOT NULL,
                    entry REAL NOT NULL,
                    stop REAL NOT NULL,
                    tp1 REAL NOT NULL,
                    tp2 REAL NOT NULL,
                    tp3 REAL NOT NULL,
                    notes TEXT NOT NULL,
                    freshness_seconds INTEGER NOT NULL,
                    received_at_et TEXT NOT NULL,
                    status TEXT NOT NULL,
                    rejection_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS orders (
                    order_id TEXT PRIMARY KEY,
                    signal_id TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    stop_price REAL NOT NULL,
                    tp1_price REAL NOT NULL,
                    tp2_price REAL NOT NULL,
                    tp3_price REAL NOT NULL,
                    quantity INTEGER NOT NULL,
                    tp1_quantity INTEGER NOT NULL,
                    tp2_quantity INTEGER NOT NULL,
                    tp3_quantity INTEGER NOT NULL,
                    point_value REAL NOT NULL,
                    submitted_at_et TEXT NOT NULL,
                    filled_at_et TEXT,
                    closed_at_et TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(signal_id) REFERENCES signals(signal_id)
                );

                CREATE TABLE IF NOT EXISTS positions (
                    position_id TEXT PRIMARY KEY,
                    signal_id TEXT NOT NULL,
                    order_id TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    status TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    stop_price REAL NOT NULL,
                    tp1_price REAL NOT NULL,
                    tp2_price REAL NOT NULL,
                    tp3_price REAL NOT NULL,
                    quantity_initial INTEGER NOT NULL,
                    quantity_open INTEGER NOT NULL,
                    tp1_quantity INTEGER NOT NULL,
                    tp2_quantity INTEGER NOT NULL,
                    tp3_quantity INTEGER NOT NULL,
                    tp1_filled_quantity INTEGER NOT NULL,
                    tp2_filled_quantity INTEGER NOT NULL,
                    tp3_filled_quantity INTEGER NOT NULL,
                    point_value REAL NOT NULL,
                    realized_pnl REAL NOT NULL,
                    opened_at_et TEXT NOT NULL,
                    closed_at_et TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(signal_id) REFERENCES signals(signal_id),
                    FOREIGN KEY(order_id) REFERENCES orders(order_id)
                );

                CREATE TABLE IF NOT EXISTS fills (
                    fill_id TEXT PRIMARY KEY,
                    signal_id TEXT NOT NULL,
                    order_id TEXT NOT NULL,
                    position_id TEXT,
                    fill_type TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    price REAL NOT NULL,
                    realized_pnl REAL NOT NULL,
                    fill_timestamp_et TEXT NOT NULL,
                    notes TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(signal_id) REFERENCES signals(signal_id),
                    FOREIGN KEY(order_id) REFERENCES orders(order_id),
                    FOREIGN KEY(position_id) REFERENCES positions(position_id)
                );

                CREATE TABLE IF NOT EXISTS pnl_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    signal_id TEXT NOT NULL,
                    position_id TEXT,
                    status TEXT NOT NULL,
                    realized_pnl REAL NOT NULL,
                    unrealized_pnl REAL NOT NULL,
                    as_of_timestamp_et TEXT NOT NULL,
                    note TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(signal_id) REFERENCES signals(signal_id),
                    FOREIGN KEY(position_id) REFERENCES positions(position_id)
                );
                """
            )

    def signal_exists(self, signal_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM signals WHERE signal_id = ?", (signal_id,)).fetchone()
        return row is not None

    def record_signal(
        self,
        *,
        signal: SignalEvent,
        received_at_et: datetime,
        status: SignalStatus,
        rejection_reason: str | None = None,
    ) -> None:
        now = received_at_et.isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO signals (
                    signal_id, source_bot, instrument, direction, setup_type, session, confluence,
                    formed_timestamp_et, entry, stop, tp1, tp2, tp3, notes, freshness_seconds,
                    received_at_et, status, rejection_reason, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.signal_id,
                    signal.source_bot,
                    signal.instrument,
                    signal.direction.value,
                    signal.setup_type,
                    signal.session,
                    signal.confluence,
                    signal.formed_timestamp_et.isoformat(),
                    signal.entry,
                    signal.stop,
                    signal.tp1,
                    signal.tp2,
                    signal.tp3,
                    signal.notes,
                    signal.freshness_seconds,
                    received_at_et.isoformat(),
                    status.value,
                    rejection_reason,
                    now,
                    now,
                ),
            )

    def update_signal_status(self, *, signal_id: str, status: SignalStatus, updated_at_et: datetime) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE signals SET status = ?, updated_at = ? WHERE signal_id = ?",
                (status.value, updated_at_et.isoformat(), signal_id),
            )

    def create_order(
        self,
        *,
        order_id: str,
        signal: SignalEvent,
        quantity: int,
        tp1_quantity: int,
        tp2_quantity: int,
        tp3_quantity: int,
        point_value: float,
        submitted_at_et: datetime,
    ) -> None:
        now = submitted_at_et.isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO orders (
                    order_id, signal_id, instrument, direction, order_type, status,
                    entry_price, stop_price, tp1_price, tp2_price, tp3_price,
                    quantity, tp1_quantity, tp2_quantity, tp3_quantity, point_value,
                    submitted_at_et, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    signal.signal_id,
                    signal.instrument,
                    signal.direction.value,
                    "pending_bracket",
                    OrderStatus.PENDING.value,
                    signal.entry,
                    signal.stop,
                    signal.tp1,
                    signal.tp2,
                    signal.tp3,
                    quantity,
                    tp1_quantity,
                    tp2_quantity,
                    tp3_quantity,
                    point_value,
                    submitted_at_et.isoformat(),
                    now,
                    now,
                ),
            )

    def get_pending_orders(self, instrument: str) -> tuple[PendingOrderRecord, ...]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT order_id, signal_id, instrument, direction, entry_price, stop_price,
                       tp1_price, tp2_price, tp3_price, quantity, tp1_quantity, tp2_quantity,
                       tp3_quantity, point_value, status
                FROM orders
                WHERE instrument = ? AND status = ?
                ORDER BY created_at ASC
                """,
                (instrument, OrderStatus.PENDING.value),
            ).fetchall()
        return tuple(self._pending_order_from_row(row) for row in rows)

    def mark_order_filled(self, *, order_id: str, filled_at_et: datetime) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE orders
                SET status = ?, filled_at_et = ?, updated_at = ?
                WHERE order_id = ?
                """,
                (OrderStatus.FILLED.value, filled_at_et.isoformat(), filled_at_et.isoformat(), order_id),
            )

    def mark_order_closed(self, *, order_id: str, closed_at_et: datetime) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE orders
                SET status = ?, closed_at_et = ?, updated_at = ?
                WHERE order_id = ?
                """,
                (OrderStatus.CLOSED.value, closed_at_et.isoformat(), closed_at_et.isoformat(), order_id),
            )

    def create_position_from_order(self, *, order: PendingOrderRecord, opened_at_et: datetime) -> str:
        position_id = f"pos_{order.signal_id}"
        now = opened_at_et.isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO positions (
                    position_id, signal_id, order_id, instrument, direction, status,
                    entry_price, stop_price, tp1_price, tp2_price, tp3_price,
                    quantity_initial, quantity_open, tp1_quantity, tp2_quantity, tp3_quantity,
                    tp1_filled_quantity, tp2_filled_quantity, tp3_filled_quantity,
                    point_value, realized_pnl, opened_at_et, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position_id,
                    order.signal_id,
                    order.order_id,
                    order.instrument,
                    order.direction.value,
                    PositionStatus.OPEN.value,
                    order.entry_price,
                    order.stop_price,
                    order.tp1_price,
                    order.tp2_price,
                    order.tp3_price,
                    order.quantity,
                    order.quantity,
                    order.tp1_quantity,
                    order.tp2_quantity,
                    order.tp3_quantity,
                    0,
                    0,
                    0,
                    order.point_value,
                    0.0,
                    opened_at_et.isoformat(),
                    now,
                    now,
                ),
            )
        return position_id

    def get_open_positions(self, instrument: str) -> tuple[OpenPositionRecord, ...]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT position_id, signal_id, order_id, instrument, direction, status,
                       entry_price, stop_price, tp1_price, tp2_price, tp3_price,
                       quantity_initial, quantity_open, tp1_quantity, tp2_quantity, tp3_quantity,
                       tp1_filled_quantity, tp2_filled_quantity, tp3_filled_quantity,
                       point_value, realized_pnl
                FROM positions
                WHERE instrument = ? AND status = ?
                ORDER BY created_at ASC
                """,
                (instrument, PositionStatus.OPEN.value),
            ).fetchall()
        return tuple(self._open_position_from_row(row) for row in rows)

    def get_open_position(self, position_id: str) -> OpenPositionRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT position_id, signal_id, order_id, instrument, direction, status,
                       entry_price, stop_price, tp1_price, tp2_price, tp3_price,
                       quantity_initial, quantity_open, tp1_quantity, tp2_quantity, tp3_quantity,
                       tp1_filled_quantity, tp2_filled_quantity, tp3_filled_quantity,
                       point_value, realized_pnl
                FROM positions
                WHERE position_id = ? AND status = ?
                """,
                (position_id, PositionStatus.OPEN.value),
            ).fetchone()
        return None if row is None else self._open_position_from_row(row)

    def apply_position_fill(
        self,
        *,
        position_id: str,
        fill_type: FillType,
        quantity: int,
        realized_pnl_delta: float,
        filled_at_et: datetime,
    ) -> None:
        field_name = {
            FillType.TP1: "tp1_filled_quantity",
            FillType.TP2: "tp2_filled_quantity",
            FillType.TP3: "tp3_filled_quantity",
            FillType.STOP: None,
            FillType.CLOSE: None,
            FillType.ENTRY: None,
        }[fill_type]

        with self._connect() as conn:
            if field_name is not None:
                conn.execute(
                    f"""
                    UPDATE positions
                    SET quantity_open = quantity_open - ?,
                        {field_name} = {field_name} + ?,
                        realized_pnl = realized_pnl + ?,
                        updated_at = ?
                    WHERE position_id = ?
                    """,
                    (quantity, quantity, realized_pnl_delta, filled_at_et.isoformat(), position_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE positions
                    SET quantity_open = quantity_open - ?,
                        realized_pnl = realized_pnl + ?,
                        updated_at = ?
                    WHERE position_id = ?
                    """,
                    (quantity, realized_pnl_delta, filled_at_et.isoformat(), position_id),
                )

    def close_position(self, *, position_id: str, closed_at_et: datetime) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE positions
                SET status = ?, closed_at_et = ?, updated_at = ?
                WHERE position_id = ?
                """,
                (PositionStatus.CLOSED.value, closed_at_et.isoformat(), closed_at_et.isoformat(), position_id),
            )

    def record_fill(
        self,
        *,
        fill_id: str,
        signal_id: str,
        order_id: str,
        position_id: str | None,
        fill_type: FillType,
        quantity: int,
        price: float,
        realized_pnl: float,
        fill_timestamp_et: datetime,
        notes: str,
    ) -> None:
        now = fill_timestamp_et.isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fills (
                    fill_id, signal_id, order_id, position_id, fill_type, quantity, price,
                    realized_pnl, fill_timestamp_et, notes, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill_id,
                    signal_id,
                    order_id,
                    position_id,
                    fill_type.value,
                    quantity,
                    price,
                    realized_pnl,
                    fill_timestamp_et.isoformat(),
                    notes,
                    now,
                    now,
                ),
            )

    def record_pnl_snapshot(
        self,
        *,
        snapshot_id: str,
        signal_id: str,
        position_id: str | None,
        status: str,
        realized_pnl: float,
        unrealized_pnl: float,
        as_of_timestamp_et: datetime,
        note: str,
    ) -> None:
        now = as_of_timestamp_et.isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO pnl_snapshots (
                    snapshot_id, signal_id, position_id, status, realized_pnl, unrealized_pnl,
                    as_of_timestamp_et, note, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    signal_id,
                    position_id,
                    status,
                    realized_pnl,
                    unrealized_pnl,
                    as_of_timestamp_et.isoformat(),
                    note,
                    now,
                    now,
                ),
            )

    def get_signal_status(self, signal_id: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute("SELECT status FROM signals WHERE signal_id = ?", (signal_id,)).fetchone()
        return None if row is None else str(row["status"])

    def _pending_order_from_row(self, row: sqlite3.Row) -> PendingOrderRecord:
        return PendingOrderRecord(
            order_id=str(row["order_id"]),
            signal_id=str(row["signal_id"]),
            instrument=str(row["instrument"]),
            direction=Direction(str(row["direction"])),
            entry_price=float(row["entry_price"]),
            stop_price=float(row["stop_price"]),
            tp1_price=float(row["tp1_price"]),
            tp2_price=float(row["tp2_price"]),
            tp3_price=float(row["tp3_price"]),
            quantity=int(row["quantity"]),
            tp1_quantity=int(row["tp1_quantity"]),
            tp2_quantity=int(row["tp2_quantity"]),
            tp3_quantity=int(row["tp3_quantity"]),
            point_value=float(row["point_value"]),
            status=OrderStatus(str(row["status"])),
        )

    def _open_position_from_row(self, row: sqlite3.Row) -> OpenPositionRecord:
        return OpenPositionRecord(
            position_id=str(row["position_id"]),
            signal_id=str(row["signal_id"]),
            order_id=str(row["order_id"]),
            instrument=str(row["instrument"]),
            direction=Direction(str(row["direction"])),
            entry_price=float(row["entry_price"]),
            stop_price=float(row["stop_price"]),
            tp1_price=float(row["tp1_price"]),
            tp2_price=float(row["tp2_price"]),
            tp3_price=float(row["tp3_price"]),
            quantity_initial=int(row["quantity_initial"]),
            quantity_open=int(row["quantity_open"]),
            tp1_quantity=int(row["tp1_quantity"]),
            tp2_quantity=int(row["tp2_quantity"]),
            tp3_quantity=int(row["tp3_quantity"]),
            tp1_filled_quantity=int(row["tp1_filled_quantity"]),
            tp2_filled_quantity=int(row["tp2_filled_quantity"]),
            tp3_filled_quantity=int(row["tp3_filled_quantity"]),
            point_value=float(row["point_value"]),
            realized_pnl=float(row["realized_pnl"]),
            status=PositionStatus(str(row["status"])),
        )
