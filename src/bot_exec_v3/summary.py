"""Daily recap for Bot 3 paper execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from bot_exec_v3.query import PaperTradeQueries, PnlSummary
from futures_bot.alerts.telegram import TelegramDelivery, TelegramNotifier
from futures_bot.runtime.schedule import in_daily_halt, market_is_open


@dataclass(frozen=True, slots=True)
class PaperDailySummary:
    summary_date: date
    trades_opened: int
    trades_closed: int
    win_count: int
    loss_count: int
    realized_pnl: float
    open_positions: int


class PaperDailySummaryManager:
    def __init__(self, *, queries: PaperTradeQueries, last_sent_date: date | None = None) -> None:
        self._queries = queries
        self._last_sent_date = last_sent_date

    @property
    def last_sent_date(self) -> date | None:
        return self._last_sent_date

    def maybe_send(self, *, now_et: datetime, notifier: TelegramNotifier) -> TelegramDelivery | None:
        today = now_et.date()
        if self._last_sent_date == today:
            return None
        if market_is_open(now_et) and not in_daily_halt(now_et):
            return None
        if now_et.hour < 17:
            return None
        summary = self.build_summary(now_et=now_et)
        delivery = notifier.send_text(text=self.format_message(summary))
        if delivery.delivered:
            self._last_sent_date = today
        return delivery

    def build_summary(self, *, now_et: datetime) -> PaperDailySummary:
        pnl = self._queries.pnl_today(now_et=now_et)
        return PaperDailySummary(
            summary_date=now_et.date(),
            trades_opened=pnl.trades_opened,
            trades_closed=pnl.trades_closed,
            win_count=pnl.win_count,
            loss_count=pnl.loss_count,
            realized_pnl=pnl.realized_pnl,
            open_positions=pnl.open_positions,
        )

    def format_message(self, summary: PaperDailySummary) -> str:
        return "\n".join(
            [
                "<b>PAPER DAILY SUMMARY</b>",
                f"<b>Date:</b> {summary.summary_date.isoformat()}",
                f"<b>Trades Opened:</b> {summary.trades_opened}",
                f"<b>Trades Closed:</b> {summary.trades_closed}",
                f"<b>Win/Loss:</b> {summary.win_count}/{summary.loss_count}",
                f"<b>Realized PnL:</b> {summary.realized_pnl:.2f}",
                f"<b>Open Positions:</b> {summary.open_positions}",
            ]
        )
