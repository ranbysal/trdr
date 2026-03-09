from __future__ import annotations

import asyncio
from datetime import datetime

from futures_bot.live.backpressure import BackpressureQueue
from futures_bot.live.feed_models import FeedMessage


def _msg(kind: str, symbol: str = "NQ") -> FeedMessage:
    payload = {"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}
    if kind == "quote_1s":
        payload = {"bid": 1, "ask": 2, "bid_size": 1, "ask_size": 1}
    return FeedMessage(type=kind, timestamp_et=datetime(2026, 1, 1), symbol=symbol, payload=payload)


def test_backpressure_drops_oldest_quote_when_full() -> None:
    async def scenario() -> None:
        queue = BackpressureQueue(maxsize=2)
        await queue.put(_msg("quote_1s", "NQ"))
        await queue.put(_msg("quote_1s", "YM"))

        result = await queue.put(_msg("quote_1s", "ES"))

        assert result.accepted is True
        assert result.dropped_quotes == 1
        first = await queue.get()
        second = await queue.get()
        assert {first.symbol, second.symbol} == {"YM", "ES"}

    asyncio.run(scenario())


def test_backpressure_never_drops_bar_and_requests_freeze() -> None:
    async def scenario() -> None:
        queue = BackpressureQueue(maxsize=1)
        await queue.put(_msg("bar_1m", "NQ"))
        result = await queue.put(_msg("bar_1m", "YM"))
        assert result.accepted is False
        assert result.freeze_trading is True

    asyncio.run(scenario())
