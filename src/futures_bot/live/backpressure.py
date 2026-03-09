"""Inbound backpressure queue policies."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass

from futures_bot.live.feed_models import FeedMessage


@dataclass(frozen=True, slots=True)
class BackpressureResult:
    accepted: bool
    dropped_quotes: int = 0
    freeze_trading: bool = False


class BackpressureQueue:
    def __init__(self, *, maxsize: int) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be >=1")
        self._maxsize = maxsize
        self._queue: deque[FeedMessage] = deque()
        self._cv = asyncio.Condition()
        self._closed = False

    async def put(self, message: FeedMessage) -> BackpressureResult:
        async with self._cv:
            if self._closed:
                return BackpressureResult(accepted=False, freeze_trading=True)
            if len(self._queue) < self._maxsize:
                self._queue.append(message)
                self._cv.notify()
                return BackpressureResult(accepted=True)

            if message.type == "quote_1s":
                dropped = self._drop_oldest_quote_locked()
                if dropped > 0:
                    self._queue.append(message)
                    self._cv.notify()
                    return BackpressureResult(accepted=True, dropped_quotes=dropped)
                return BackpressureResult(accepted=False, freeze_trading=True)

            # Bars/events are never dropped. Signal freeze/overload.
            return BackpressureResult(accepted=False, freeze_trading=True)

    async def get(self) -> FeedMessage:
        async with self._cv:
            while not self._queue and not self._closed:
                await self._cv.wait()
            if not self._queue and self._closed:
                raise RuntimeError("queue closed")
            return self._queue.popleft()

    async def qsize(self) -> int:
        async with self._cv:
            return len(self._queue)

    async def close(self) -> None:
        async with self._cv:
            self._closed = True
            self._cv.notify_all()

    def _drop_oldest_quote_locked(self) -> int:
        for idx, item in enumerate(self._queue):
            if item.type == "quote_1s":
                del self._queue[idx]
                return 1
        return 0
