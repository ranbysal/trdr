"""Fatal runtime error formatting and forwarding."""

from __future__ import annotations

from datetime import datetime

from futures_bot.alerts.telegram import TelegramDelivery, TelegramNotifier


def format_fatal_error_message(
    *,
    error_type: str,
    message: str,
    timestamp_et: datetime,
    component: str | None = None,
) -> str:
    lines = [
        "<b>FATAL ERROR</b>",
        f"<b>Type:</b> {error_type}",
        f"<b>Message:</b> {message}",
        f"<b>Timestamp:</b> {timestamp_et.isoformat()}",
    ]
    if component:
        lines.append(f"<b>Component:</b> {component}")
    return "\n".join(lines)


class ErrorForwarder:
    def __init__(self, notifier: TelegramNotifier | None = None) -> None:
        self._notifier = notifier or TelegramNotifier()
        self._sent_keys: set[str] = set()

    def send(
        self,
        *,
        error_type: str,
        message: str,
        timestamp_et: datetime,
        component: str | None = None,
        dedupe_key: str | None = None,
    ) -> TelegramDelivery:
        key = dedupe_key or f"{error_type}:{component}:{message}"
        if key in self._sent_keys:
            return TelegramDelivery(delivered=False, message="", error="duplicate_error_suppressed")
        delivery = self._notifier.send_text(
            text=format_fatal_error_message(
                error_type=error_type,
                message=message,
                timestamp_et=timestamp_et,
                component=component,
            )
        )
        if delivery.delivered:
            self._sent_keys.add(key)
        return delivery

    def clear(self, dedupe_key: str) -> None:
        self._sent_keys.discard(dedupe_key)
