"""Async websocket live feed client with reconnect and backpressure."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable
from typing import Any

from futures_bot.live.backpressure import BackpressureQueue
from futures_bot.live.feed_models import FeedMessage, FeedSchemaError, parse_feed_message
from futures_bot.live.heartbeat import HeartbeatMonitor
from futures_bot.live.reconnect import ReconnectPolicy

logger = logging.getLogger(__name__)

try:
    import websockets
except ImportError:  # pragma: no cover - handled by runtime checks
    websockets = None


class LiveWsClient:
    def __init__(
        self,
        *,
        ws_url: str,
        queue_maxsize: int = 2000,
        reconnect_policy: ReconnectPolicy | None = None,
        heartbeat: HeartbeatMonitor | None = None,
        on_overload: Callable[[str], None] | None = None,
    ) -> None:
        self._ws_url = ws_url
        self._queue = BackpressureQueue(maxsize=queue_maxsize)
        self._reconnect = reconnect_policy or ReconnectPolicy()
        self._heartbeat = heartbeat or HeartbeatMonitor()
        self._on_overload = on_overload
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if websockets is None:
            raise RuntimeError("websockets dependency is required for live mode")
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="live-ws-client")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._queue.close()

    async def messages(self) -> AsyncIterator[FeedMessage]:
        while True:
            try:
                msg = await self._queue.get()
            except RuntimeError:
                break
            yield msg

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._connect_and_consume()
                self._reconnect.reset()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("websocket loop error: %s", exc)

            if self._stop_event.is_set():
                break
            await asyncio.sleep(self._reconnect.next_delay())

    async def _connect_and_consume(self) -> None:
        assert websockets is not None
        async with websockets.connect(self._ws_url, ping_interval=None) as ws:
            self._heartbeat.mark_message()
            while not self._stop_event.is_set():
                if self._heartbeat.should_ping():
                    pong_waiter = await ws.ping()
                    self._heartbeat.mark_ping()
                    await asyncio.wait_for(pong_waiter, timeout=self._heartbeat.pong_timeout_s)

                recv_task = asyncio.create_task(ws.recv())
                done, pending = await asyncio.wait(
                    {recv_task},
                    timeout=min(self._heartbeat.ping_interval_s, self._heartbeat.message_timeout_s),
                )
                if not done:
                    recv_task.cancel()
                    if self._heartbeat.message_timed_out():
                        raise TimeoutError("message heartbeat timeout")
                    continue

                raw = recv_task.result()
                self._heartbeat.mark_message()
                message = self._decode_message(raw)
                if message is None:
                    continue

                result = await self._queue.put(message)
                if result.freeze_trading and self._on_overload is not None:
                    self._on_overload("FEED_QUEUE_OVERLOAD")

    def _decode_message(self, raw: Any) -> FeedMessage | None:
        try:
            if isinstance(raw, bytes):
                payload = json.loads(raw.decode("utf-8"))
            else:
                payload = json.loads(str(raw))
            if not isinstance(payload, dict):
                return None
            return parse_feed_message(payload)
        except (json.JSONDecodeError, FeedSchemaError) as exc:
            logger.warning("dropping malformed feed message: %s", exc)
            return None
