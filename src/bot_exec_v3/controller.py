"""Telegram-facing command controller for Bot 3 paper journal queries."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

from bot_exec_v3.query import LedgerTradeRecord, PaperTradeQueries, PnlSummary, TradeDetail

ET = ZoneInfo("America/New_York")


class PaperCommandController:
    def __init__(
        self,
        *,
        sqlite_path: str | Path = "state/executor_v3/paper_ledger.db",
        reports_dir: str | Path = "out/executor_v3/reports",
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._queries = PaperTradeQueries(sqlite_path)
        self._reports_dir = Path(reports_dir)
        self._now_provider = now_provider or (lambda: datetime.now(tz=ET))

    def handle_command(self, text: str) -> str | None:
        parts = text.strip().split()
        if not parts:
            return None
        command = parts[0].lower()
        try:
            if command == "/paper_open":
                return self._format_trade_list("Open Paper Positions", self._queries.open_positions())
            if command == "/paper_closed":
                return self._format_trade_list("Closed Paper Trades", self._queries.closed_trades(limit=10))
            if command == "/paper_last":
                limit = self._parse_limit(parts[1] if len(parts) > 1 else "10")
                return self._format_trade_list(f"Last {limit} Paper Trades", self._queries.last_trades(limit=limit))
            if command == "/paper_pnl":
                now_et = self._now_provider()
                today = self._queries.pnl_today(now_et=now_et)
                week = self._queries.pnl_week(now_et=now_et)
                return self._format_pnl(today=today, week=week)
            if command == "/paper_trade" and len(parts) > 1:
                detail = self._queries.trade_by_id(parts[1])
                return self._format_trade_detail(detail, parts[1])
            if command == "/paper_export":
                limit = self._parse_limit(parts[1] if len(parts) > 1 else "50")
                export_path = self._queries.export_recent_trades_csv(out_dir=self._reports_dir, limit=limit)
                return f"[EXEC-V3] CSV export created: {export_path}"
            return None
        except sqlite3.Error:
            return "[EXEC-V3] Paper ledger unavailable."

    def export_recent_trades_csv(self, *, limit: int = 50) -> Path:
        return self._queries.export_recent_trades_csv(out_dir=self._reports_dir, limit=limit)

    def _format_trade_list(self, title: str, trades: tuple[LedgerTradeRecord, ...]) -> str:
        if not trades:
            return f"[EXEC-V3] {title}\nNo matching trades."
        lines = [f"[EXEC-V3] {title}"]
        for trade in trades:
            lines.append(self._trade_line(trade))
        return "\n".join(lines)

    def _format_trade_detail(self, detail: TradeDetail | None, trade_id: str) -> str:
        if detail is None:
            return f"[EXEC-V3] Trade not found: {trade_id}"
        trade = detail.trade
        lines = [
            f"[EXEC-V3] Trade {trade.trade_id}",
            self._trade_line(trade),
            "Fills:",
        ]
        if not detail.fills:
            lines.append("none")
        else:
            for fill in detail.fills:
                lines.append(
                    f"{fill.fill_type.upper()} qty={fill.quantity} px={fill.price:.2f} pnl={fill.realized_pnl:.2f} ts={fill.fill_timestamp_et.isoformat()}"
                )
        return "\n".join(lines)

    def _format_pnl(self, *, today: PnlSummary, week: PnlSummary) -> str:
        return "\n".join(
            [
                "[EXEC-V3] Paper PnL",
                self._pnl_line("Today", today),
                self._pnl_line("Week", week),
            ]
        )

    def _trade_line(self, trade: LedgerTradeRecord) -> str:
        exit_text = "n/a" if trade.exit_price is None else f"{trade.exit_price:.2f}"
        closed_text = "open" if trade.closed_at_et is None else trade.closed_at_et.isoformat()
        return (
            f"{trade.trade_id} | {trade.instrument} {trade.direction} | "
            f"entry={trade.entry_price:.2f} exit={exit_text} | "
            f"size={trade.size} open={trade.open_quantity} | "
            f"pnl={trade.realized_pnl:.2f} | status={trade.status} | "
            f"opened={trade.opened_at_et.isoformat()} | closed={closed_text}"
        )

    def _pnl_line(self, label: str, summary: PnlSummary) -> str:
        return (
            f"{label}: realized={summary.realized_pnl:.2f} | opened={summary.trades_opened} | "
            f"closed={summary.trades_closed} | win/loss={summary.win_count}/{summary.loss_count} | "
            f"open_positions={summary.open_positions}"
        )

    def _parse_limit(self, raw: str) -> int:
        try:
            value = int(raw)
        except ValueError:
            return 10
        return max(1, min(value, 200))
