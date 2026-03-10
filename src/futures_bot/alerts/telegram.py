"""Telegram alert transport and message formatting."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass

from futures_bot.signals.models import AlertKind, SignalIdea, SignalLifecycleState


@dataclass(frozen=True, slots=True)
class TelegramDelivery:
    delivered: bool
    message: str
    response_code: int | None = None
    error: str | None = None


class TelegramNotifier:
    def __init__(
        self,
        *,
        token: str | None = None,
        chat_id: str | None = None,
        parse_mode: str = "HTML",
        timeout_s: float = 10.0,
    ) -> None:
        self._token = token
        self._chat_id = chat_id
        self._parse_mode = parse_mode
        self._timeout_s = timeout_s

    @property
    def enabled(self) -> bool:
        return bool(self._token and self._chat_id)

    def send(
        self,
        *,
        kind: AlertKind,
        idea: SignalIdea,
        state: SignalLifecycleState,
        note: str | None = None,
    ) -> TelegramDelivery:
        message = self.format(kind=kind, idea=idea, state=state, note=note)
        if not self.enabled:
            return TelegramDelivery(delivered=False, message=message, error="telegram_not_configured")

        payload = json.dumps(
            {
                "chat_id": self._chat_id,
                "text": message,
                "parse_mode": self._parse_mode,
                "disable_web_page_preview": True,
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            url=f"https://api.telegram.org/bot{self._token}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_s) as response:
                return TelegramDelivery(
                    delivered=True,
                    message=message,
                    response_code=getattr(response, "status", None),
                )
        except urllib.error.URLError as exc:
            return TelegramDelivery(delivered=False, message=message, error=str(exc))

    def format(
        self,
        *,
        kind: AlertKind,
        idea: SignalIdea,
        state: SignalLifecycleState,
        note: str | None = None,
    ) -> str:
        if kind is AlertKind.NEW_SIGNAL:
            title = "NEW SIGNAL"
        elif kind is AlertKind.INVALIDATION:
            title = "SETUP INVALIDATED"
        elif kind is AlertKind.CLOSE:
            title = "SIGNAL CLOSED"
        else:
            title = "SIGNAL UPDATE"

        lines = [
            f"<b>{title}</b>",
            f"<b>Symbol:</b> {idea.symbol_display}",
            f"<b>Side:</b> {idea.side}",
            f"<b>Entry:</b> {idea.entry_low:.2f} - {idea.entry_high:.2f}",
            f"<b>Stop Loss:</b> {idea.stop_loss:.2f}",
            f"<b>TP1:</b> {idea.tp1:.2f}",
            f"<b>TP2:</b> {idea.tp2:.2f}",
            f"<b>Partial:</b> {idea.partial_profit_guidance}",
            f"<b>Invalidation:</b> {idea.invalidation}",
            f"<b>Timestamp:</b> {idea.timestamp.isoformat()}",
            f"<b>Strategy:</b> {idea.strategy.value}",
            f"<b>Context:</b> {idea.strategy_context}",
            f"<b>Regime/Confidence:</b> {idea.regime} / {idea.confidence:.2f}",
            f"<b>State:</b> {state.value}",
            "<b>Position Assumption:</b> false",
        ]
        if note:
            lines.append(f"<b>Note:</b> {note}")
        return "\n".join(lines)
