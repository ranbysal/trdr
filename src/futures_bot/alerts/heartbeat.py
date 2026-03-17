"""Periodic operational heartbeat alerts."""

from __future__ import annotations

from datetime import datetime, timedelta

from futures_bot.alerts.telegram import TelegramDelivery, TelegramNotifier
from futures_bot.runtime.health import RuntimeStatus


class HeartbeatManager:
    def __init__(self, *, interval_hours: float = 4.0, last_sent_at: datetime | None = None) -> None:
        self._interval = timedelta(hours=interval_hours)
        self._last_sent_at = last_sent_at

    @property
    def last_sent_at(self) -> datetime | None:
        return self._last_sent_at

    def maybe_send(self, *, now_et: datetime, status: RuntimeStatus, notifier: TelegramNotifier) -> TelegramDelivery | None:
        if self._last_sent_at is not None and (now_et - self._last_sent_at) < self._interval:
            return None
        delivery = notifier.send_text(text=self.format_message(status=status))
        if delivery.delivered:
            self._last_sent_at = now_et
        return delivery

    def format_message(self, *, status: RuntimeStatus) -> str:
        market = "open" if status.market_open else "closed"
        feed = "connected" if status.feed_connected else "disconnected"
        last_bar = status.last_bar_timestamp or "none"
        signals = "true" if status.signals_active else "false"
        return "\n".join(
            [
                "<b>HEARTBEAT</b>",
                "<b>Bot:</b> online",
                f"<b>Market:</b> {market}",
                f"<b>Feed:</b> {feed}",
                f"<b>Last Bar:</b> {last_bar}",
                f"<b>Signals Active:</b> {signals}",
                f"<b>Output:</b> {status.output_path}",
            ]
        )
