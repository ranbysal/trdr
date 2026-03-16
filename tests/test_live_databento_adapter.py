from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import databento as db
import databento_dbn as dbn
import pytest

from futures_bot.live.databento_adapter import (
    DEFAULT_DATABENTO_DATASET,
    DEFAULT_DATABENTO_SCHEMA,
    DEFAULT_DATABENTO_STYPE_IN,
    DEFAULT_DATABENTO_SYMBOLS,
    DatabentoLiveClient,
)


@dataclass
class FakeLiveSession:
    def __post_init__(self) -> None:
        self.callbacks: list[tuple[object, object | None]] = []
        self.reconnect_callbacks: list[tuple[object, object | None]] = []
        self.subscribe_calls: list[dict[str, object]] = []
        self.started = False
        self.stopped = False
        self.symbology_map: dict[int, str] = {}
        self._closed = asyncio.Event()

    def add_callback(self, callback, exception_callback=None) -> None:
        self.callbacks.append((callback, exception_callback))

    def add_reconnect_callback(self, callback, exception_callback=None) -> None:
        self.reconnect_callbacks.append((callback, exception_callback))

    def subscribe(self, **kwargs) -> None:
        self.subscribe_calls.append(kwargs)

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True
        self._closed.set()

    async def wait_for_close(self) -> None:
        await self._closed.wait()


def test_databento_client_uses_continuous_bars_only_defaults_and_normalizes_records(caplog: pytest.LogCaptureFixture) -> None:
    async def scenario() -> None:
        session = FakeLiveSession()
        client = DatabentoLiveClient(
            api_key="db-key",
            client_factory=lambda **_: session,
        )

        with caplog.at_level(logging.INFO):
            await client.start()

        assert session.started is True
        assert session.subscribe_calls == [
            {
                "dataset": DEFAULT_DATABENTO_DATASET,
                "schema": DEFAULT_DATABENTO_SCHEMA,
                "symbols": list(DEFAULT_DATABENTO_SYMBOLS),
                "stype_in": DEFAULT_DATABENTO_STYPE_IN,
            }
        ]
        assert "dataset=GLBX.MDP3 schema=ohlcv-1m stype_in=continuous symbols=['YM.v.0', 'NQ.v.0']" in caplog.text

        callback, _ = session.callbacks[0]
        ts_event = 1_736_688_600_000_000_000
        callback(
            dbn.SymbolMappingMsg(
                1,
                101,
                ts_event,
                db.SType.CONTINUOUS,
                "YM.v.0",
                db.SType.RAW_SYMBOL,
                "YMH6",
                ts_event,
                ts_event,
            )
        )
        callback(
            dbn.OHLCVMsg(
                0,
                1,
                101,
                ts_event,
                5_000_000_000,
                5_100_000_000,
                4_900_000_000,
                5_050_000_000,
                12,
            )
        )
        callback(
            dbn.BBOMsg(
                0,
                1,
                101,
                ts_event,
                0,
                0,
                dbn.Side.NONE,
                ts_event,
                levels=dbn.BidAskPair(5_000_000_000, 5_025_000_000, 7, 9, 1, 1),
            )
        )
        await asyncio.sleep(0)

        messages = client.messages()
        bar = await anext(messages)

        assert bar.type == "bar_1m"
        assert bar.symbol == "YM"
        assert bar.payload["open"] == 5.0
        assert bar.payload["close"] == 5.05

        await client.stop()
        assert session.stopped is True

    asyncio.run(scenario())


def test_databento_client_classifies_symbol_resolution_errors() -> None:
    async def scenario() -> None:
        session = FakeLiveSession()
        client = DatabentoLiveClient(
            api_key="db-key",
            client_factory=lambda **_: session,
        )

        await client.start()

        callback, _ = session.callbacks[0]
        callback(
            dbn.ErrorMsg(
                1_736_688_600_000_000_000,
                "symbol resolution failed for subscription",
            )
        )
        await asyncio.sleep(0)

        messages = client.messages()
        event = await anext(messages)
        assert event.type == "event"
        assert event.payload["code"] == "DATABENTO_SYMBOL_RESOLUTION_FAILURE"
        assert client.quote_schema_enabled is False

        await client.stop()

    asyncio.run(scenario())
