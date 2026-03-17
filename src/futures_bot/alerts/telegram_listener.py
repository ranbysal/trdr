"""Telegram polling listener for remote signal controls."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from futures_bot.alerts.telegram import TelegramNotifier


class TelegramCommandListener:
    def __init__(
        self,
        *,
        notifier: TelegramNotifier,
        status_provider: Callable[[], str],
        set_signals_active: Callable[[bool], Awaitable[None] | None],
        poll_interval_s: float = 2.0,
    ) -> None:
        self._notifier = notifier
        self._status_provider = status_provider
        self._set_signals_active = set_signals_active
        self._poll_interval_s = poll_interval_s
        self._offset: int | None = None

    async def run(self, stop_event: asyncio.Event) -> None:
        if not self._notifier.enabled:
            return
        while not stop_event.is_set():
            updates = await asyncio.to_thread(
                self._notifier.fetch_updates,
                offset=self._offset,
                timeout_s=max(1, int(self._poll_interval_s)),
            )
            for update in updates:
                self._offset = max(self._offset or 0, int(update.get("update_id", 0)) + 1)
                await self._handle_update(update)
            await asyncio.sleep(self._poll_interval_s)

    async def _handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message") or {}
        text = str(message.get("text", "")).strip()
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id", ""))
        if not text.startswith("/") or chat_id != self._notifier.chat_id:
            return
        command = text.split()[0].lower()
        if command == "/start":
            await _maybe_await(self._set_signals_active(True))
            self._notifier.send_text(text="<b>Signals:</b> active")
            return
        if command == "/stop":
            await _maybe_await(self._set_signals_active(False))
            self._notifier.send_text(text="<b>Signals:</b> paused")
            return
        if command == "/status":
            self._notifier.send_text(text=self._status_provider())


async def _maybe_await(result: Awaitable[None] | None) -> None:
    if result is None:
        return
    await result
