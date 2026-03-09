"""Heartbeat and timeout tracking."""

from __future__ import annotations

import time


class HeartbeatMonitor:
    def __init__(self, *, ping_interval_s: float = 10.0, pong_timeout_s: float = 5.0, message_timeout_s: float = 30.0) -> None:
        self.ping_interval_s = ping_interval_s
        self.pong_timeout_s = pong_timeout_s
        self.message_timeout_s = message_timeout_s
        self._last_message = time.monotonic()
        self._last_ping = 0.0

    def mark_message(self) -> None:
        self._last_message = time.monotonic()

    def should_ping(self) -> bool:
        now = time.monotonic()
        return (now - self._last_ping) >= self.ping_interval_s

    def mark_ping(self) -> None:
        self._last_ping = time.monotonic()

    def message_timed_out(self) -> bool:
        now = time.monotonic()
        return (now - self._last_message) > self.message_timeout_s
