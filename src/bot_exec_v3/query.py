"""Typed query helpers over the Bot 3 paper ledger."""

from __future__ import annotations

import csv
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class LedgerTradeRecord:
    trade_id: str
    signal_id: str
    order_id: str
    instrument: str
    direction: str
    entry_price: float
    exit_price: float | None
    size: int
    open_quantity: int
    realized_pnl: float
    status: str
    opened_at_et: datetime
    closed_at_et: datetime | None


@dataclass(frozen=True, slots=True)
class TradeFillRecord:
    fill_id: str
    fill_type: str
    quantity: int
    price: float
    realized_pnl: float
    fill_timestamp_et: datetime
    notes: str


@dataclass(frozen=True, slots=True)
class TradeDetail:
    trade: LedgerTradeRecord
    fills: tuple[TradeFillRecord, ...]


@dataclass(frozen=True, slots=True)
class PnlSummary:
    label: str
    realized_pnl: float
    trades_opened: int
    trades_closed: int
    win_count: int
    loss_count: int
    open_positions: int
    period_start_et: datetime
    period_end_et: datetime


class PaperTradeQueries:
    def __init__(self, sqlite_path: str | Path) -> None:
        self.path = Path(sqlite_path)

    def open_positions(self) -> tuple[LedgerTradeRecord, ...]:
        return self._fetch_trades(where_clause="WHERE p.status = 'open'")

    def closed_trades(self, *, limit: int = 10) -> tuple[LedgerTradeRecord, ...]:
        return self._fetch_trades(
            where_clause="WHERE p.status = 'closed'",
            order_clause="ORDER BY p.closed_at_et DESC, p.opened_at_et DESC",
            limit=limit,
        )

    def last_trades(self, *, limit: int = 10) -> tuple[LedgerTradeRecord, ...]:
        return self._fetch_trades(
            where_clause="",
            order_clause="ORDER BY COALESCE(p.closed_at_et, p.opened_at_et) DESC, p.opened_at_et DESC",
            limit=limit,
        )

    def trade_by_id(self, trade_id: str) -> TradeDetail | None:
        rows = self._fetch_trades(
            where_clause="WHERE p.position_id = ? OR p.signal_id = ? OR p.order_id = ?",
            order_clause="ORDER BY p.opened_at_et DESC",
            limit=1,
            params=(trade_id, trade_id, trade_id),
        )
        if not rows:
            return None
        trade = rows[0]
        return TradeDetail(trade=trade, fills=self._fetch_fills(trade.trade_id))

    def pnl_today(self, *, now_et: datetime) -> PnlSummary:
        today = now_et.astimezone(ET).date()
        start = datetime(today.year, today.month, today.day, tzinfo=ET)
        end = start + timedelta(days=1)
        return self._pnl_for_range(label="today", start_et=start, end_et=end)

    def pnl_week(self, *, now_et: datetime) -> PnlSummary:
        current = now_et.astimezone(ET)
        start_date = current.date() - timedelta(days=current.weekday())
        start = datetime(start_date.year, start_date.month, start_date.day, tzinfo=ET)
        end = start + timedelta(days=7)
        return self._pnl_for_range(label="week", start_et=start, end_et=end)

    def export_recent_trades_csv(self, *, out_dir: str | Path, limit: int = 50) -> Path:
        reports_dir = Path(out_dir)
        reports_dir.mkdir(parents=True, exist_ok=True)
        export_path = reports_dir / f"paper_trades_{datetime.now(tz=ET).strftime('%Y%m%d_%H%M%S')}.csv"
        trades = self.last_trades(limit=limit)
        with export_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "trade_id",
                    "signal_id",
                    "order_id",
                    "instrument",
                    "direction",
                    "entry",
                    "exit",
                    "size",
                    "open_quantity",
                    "realized_pnl",
                    "status",
                    "opened_at_et",
                    "closed_at_et",
                ]
            )
            for trade in trades:
                writer.writerow(
                    [
                        trade.trade_id,
                        trade.signal_id,
                        trade.order_id,
                        trade.instrument,
                        trade.direction,
                        f"{trade.entry_price:.2f}",
                        "" if trade.exit_price is None else f"{trade.exit_price:.2f}",
                        trade.size,
                        trade.open_quantity,
                        f"{trade.realized_pnl:.2f}",
                        trade.status,
                        trade.opened_at_et.isoformat(),
                        "" if trade.closed_at_et is None else trade.closed_at_et.isoformat(),
                    ]
                )
        return export_path

    def _pnl_for_range(self, *, label: str, start_et: datetime, end_et: datetime) -> PnlSummary:
        trades = self._fetch_trades()
        closed_in_range = [
            trade
            for trade in trades
            if trade.closed_at_et is not None and start_et <= trade.closed_at_et < end_et
        ]
        opened_in_range = [trade for trade in trades if start_et <= trade.opened_at_et < end_et]
        fills = self._fetch_fills_for_range(start_et=start_et, end_et=end_et)
        realized_pnl = sum(fill.realized_pnl for fill in fills if fill.fill_type != "entry")
        return PnlSummary(
            label=label,
            realized_pnl=realized_pnl,
            trades_opened=len(opened_in_range),
            trades_closed=len(closed_in_range),
            win_count=sum(1 for trade in closed_in_range if trade.realized_pnl > 0.0),
            loss_count=sum(1 for trade in closed_in_range if trade.realized_pnl < 0.0),
            open_positions=len([trade for trade in trades if trade.status == "open"]),
            period_start_et=start_et,
            period_end_et=end_et,
        )

    def _fetch_trades(
        self,
        *,
        where_clause: str = "",
        order_clause: str = "ORDER BY p.opened_at_et DESC",
        limit: int | None = None,
        params: tuple[object, ...] = (),
    ) -> tuple[LedgerTradeRecord, ...]:
        sql = f"""
            SELECT
                p.position_id,
                p.signal_id,
                p.order_id,
                p.instrument,
                p.direction,
                p.entry_price,
                p.quantity_initial,
                p.quantity_open,
                p.realized_pnl,
                p.status,
                p.opened_at_et,
                p.closed_at_et,
                (
                    SELECT
                        CASE
                            WHEN SUM(CASE WHEN f.fill_type != 'entry' THEN f.quantity ELSE 0 END) > 0
                            THEN
                                SUM(CASE WHEN f.fill_type != 'entry' THEN f.price * f.quantity ELSE 0 END) * 1.0 /
                                SUM(CASE WHEN f.fill_type != 'entry' THEN f.quantity ELSE 0 END)
                            ELSE NULL
                        END
                    FROM fills f
                    WHERE f.position_id = p.position_id
                ) AS exit_price
            FROM positions p
            {where_clause}
            {order_clause}
        """
        if limit is not None:
            sql = f"{sql} LIMIT ?"
            params = params + (limit,)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return tuple(self._trade_from_row(row) for row in rows)

    def _fetch_fills(self, trade_id: str) -> tuple[TradeFillRecord, ...]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT fill_id, fill_type, quantity, price, realized_pnl, fill_timestamp_et, notes
                FROM fills
                WHERE position_id = ?
                ORDER BY fill_timestamp_et ASC, fill_id ASC
                """,
                (trade_id,),
            ).fetchall()
        return tuple(self._fill_from_row(row) for row in rows)

    def _fetch_fills_for_range(self, *, start_et: datetime, end_et: datetime) -> tuple[TradeFillRecord, ...]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT fill_id, fill_type, quantity, price, realized_pnl, fill_timestamp_et, notes
                FROM fills
                ORDER BY fill_timestamp_et ASC, fill_id ASC
                """
            ).fetchall()
        fills = tuple(self._fill_from_row(row) for row in rows)
        return tuple(fill for fill in fills if start_et <= fill.fill_timestamp_et < end_et)

    def _trade_from_row(self, row: sqlite3.Row) -> LedgerTradeRecord:
        return LedgerTradeRecord(
            trade_id=str(row["position_id"]),
            signal_id=str(row["signal_id"]),
            order_id=str(row["order_id"]),
            instrument=str(row["instrument"]),
            direction=str(row["direction"]),
            entry_price=float(row["entry_price"]),
            exit_price=None if row["exit_price"] is None else float(row["exit_price"]),
            size=int(row["quantity_initial"]),
            open_quantity=int(row["quantity_open"]),
            realized_pnl=float(row["realized_pnl"]),
            status=str(row["status"]),
            opened_at_et=datetime.fromisoformat(str(row["opened_at_et"])),
            closed_at_et=None if row["closed_at_et"] is None else datetime.fromisoformat(str(row["closed_at_et"])),
        )

    def _fill_from_row(self, row: sqlite3.Row) -> TradeFillRecord:
        return TradeFillRecord(
            fill_id=str(row["fill_id"]),
            fill_type=str(row["fill_type"]),
            quantity=int(row["quantity"]),
            price=float(row["price"]),
            realized_pnl=float(row["realized_pnl"]),
            fill_timestamp_et=datetime.fromisoformat(str(row["fill_timestamp_et"])),
            notes=str(row["notes"]),
        )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
