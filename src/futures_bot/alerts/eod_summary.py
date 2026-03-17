"""End-of-day summary tracking and delivery."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime

from futures_bot.alerts.telegram import TelegramDelivery, TelegramNotifier
from futures_bot.runtime.schedule import in_daily_halt, market_is_open


@dataclass(slots=True)
class DailySummaryState:
    trade_date: date | None = None
    total_signals: int = 0
    invalidated_or_closed: int = 0
    feed_uptime_issues: int = 0


class EodSummaryManager:
    def __init__(self, *, last_sent_date: date | None = None, state: DailySummaryState | None = None) -> None:
        self._last_sent_date = last_sent_date
        self._state = state or DailySummaryState()
        self._by_strategy: Counter[str] = Counter()
        self._by_symbol: Counter[str] = Counter()

    @property
    def last_sent_date(self) -> date | None:
        return self._last_sent_date

    def restore_counters(self, payload: dict[str, object]) -> None:
        raw_date = payload.get("trade_date")
        self._state.trade_date = date.fromisoformat(str(raw_date)) if raw_date else None
        self._state.total_signals = int(payload.get("total_signals", 0))
        self._state.invalidated_or_closed = int(payload.get("invalidated_or_closed", 0))
        self._state.feed_uptime_issues = int(payload.get("feed_uptime_issues", 0))
        self._by_strategy = Counter({str(k): int(v) for k, v in dict(payload.get("by_strategy", {})).items()})
        self._by_symbol = Counter({str(k): int(v) for k, v in dict(payload.get("by_symbol", {})).items()})

    def snapshot(self) -> dict[str, object]:
        return {
            "trade_date": self._state.trade_date.isoformat() if self._state.trade_date else None,
            "total_signals": self._state.total_signals,
            "invalidated_or_closed": self._state.invalidated_or_closed,
            "feed_uptime_issues": self._state.feed_uptime_issues,
            "by_strategy": dict(self._by_strategy),
            "by_symbol": dict(self._by_symbol),
        }

    def record_signal(self, *, ts_et: datetime, strategy: str, symbol: str) -> None:
        self._roll_day(ts_et.date())
        self._state.total_signals += 1
        self._by_strategy[strategy] += 1
        self._by_symbol[symbol] += 1

    def record_closed_signal(self, *, ts_et: datetime) -> None:
        self._roll_day(ts_et.date())
        self._state.invalidated_or_closed += 1

    def record_feed_issue(self, *, ts_et: datetime) -> None:
        self._roll_day(ts_et.date())
        self._state.feed_uptime_issues += 1

    def maybe_send(self, *, now_et: datetime, notifier: TelegramNotifier) -> TelegramDelivery | None:
        today = now_et.date()
        if self._last_sent_date == today:
            return None
        if market_is_open(now_et) and not in_daily_halt(now_et):
            return None
        if now_et.hour < 17:
            return None
        delivery = notifier.send_text(text=self.format_message(summary_date=today))
        if delivery.delivered:
            self._last_sent_date = today
            self._reset()
        return delivery

    def format_message(self, *, summary_date: date) -> str:
        by_strategy = ", ".join(f"{key}={value}" for key, value in sorted(self._by_strategy.items())) or "none"
        by_symbol = ", ".join(f"{key}={value}" for key, value in sorted(self._by_symbol.items())) or "none"
        return "\n".join(
            [
                "<b>EOD SUMMARY</b>",
                f"<b>Date:</b> {summary_date.isoformat()}",
                f"<b>Total Signals:</b> {self._state.total_signals}",
                f"<b>By Strategy:</b> {by_strategy}",
                f"<b>By Symbol:</b> {by_symbol}",
                f"<b>Invalidated/Closed:</b> {self._state.invalidated_or_closed}",
                f"<b>Feed Uptime Issues:</b> {self._state.feed_uptime_issues}",
            ]
        )

    def _roll_day(self, trade_date: date) -> None:
        if self._state.trade_date is None:
            self._state.trade_date = trade_date
            return
        if self._state.trade_date != trade_date:
            self._reset()
            self._state.trade_date = trade_date

    def _reset(self) -> None:
        self._state = DailySummaryState()
        self._by_strategy.clear()
        self._by_symbol.clear()
