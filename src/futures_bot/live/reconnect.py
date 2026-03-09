"""Reconnect backoff policy."""

from __future__ import annotations

import random


class ReconnectPolicy:
    def __init__(self, *, base_delay: float = 0.5, max_delay: float = 15.0, jitter: float = 0.2) -> None:
        self._base = base_delay
        self._max = max_delay
        self._jitter = jitter
        self._attempt = 0

    def next_delay(self) -> float:
        delay = min(self._max, self._base * (2**self._attempt))
        self._attempt += 1
        if self._jitter <= 0.0:
            return delay
        scale = 1.0 + random.uniform(-self._jitter, self._jitter)
        return max(0.0, delay * scale)

    def reset(self) -> None:
        self._attempt = 0
