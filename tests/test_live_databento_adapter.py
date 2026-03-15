from __future__ import annotations

import asyncio
from dataclasses import dataclass

import databento as db
import databento_dbn as dbn

from futures_bot.live.databento_adapter import DatabentoLiveClient


@dataclass
class FakeLiveSession:
    fail_quote_schema: bool = False

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
        if self.fail_quote_schema and kwargs.get("schema") == "bbo-1s":
            raise ValueError("quote schema unavailable")
        self.subscribe_calls.append(kwargs)

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True
        self._closed.set()

    async def wait_for_close(self) -> None:
        await self._closed.wait()


def test_databento_client_initializes_and_normalizes_records() -> None:
    async def scenario() -> None:
        session = FakeLiveSession()
        client = DatabentoLiveClient(
            api_key="db-key",
            dataset="GLBX.MDP3",
            symbols=["ES"],
            client_factory=lambda **_: session,
        )

        await client.start()

        assert session.started is True
        assert [call["schema"] for call in session.subscribe_calls] == ["ohlcv-1m", "bbo-1s"]
        assert all(call["stype_in"] == "parent" for call in session.subscribe_calls)

        callback, _ = session.callbacks[0]
        ts_event = 1_736_688_600_000_000_000
        callback(
            dbn.SymbolMappingMsg(
                1,
                101,
                ts_event,
                db.SType.PARENT,
                "ES",
                db.SType.RAW_SYMBOL,
                "ESH6",
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
        quote = await anext(messages)

        assert bar.type == "bar_1m"
        assert bar.symbol == "ES"
        assert bar.payload["open"] == 5.0
        assert bar.payload["close"] == 5.05

        assert quote.type == "quote_1s"
        assert quote.symbol == "ES"
        assert quote.payload["bid"] == 5.0
        assert quote.payload["ask"] == 5.025
        assert quote.payload["bid_size"] == 7.0
        assert quote.payload["ask_size"] == 9.0

        await client.stop()
        assert session.stopped is True

    asyncio.run(scenario())


def test_databento_client_degrades_gracefully_without_quote_schema() -> None:
    async def scenario() -> None:
        session = FakeLiveSession(fail_quote_schema=True)
        client = DatabentoLiveClient(
            api_key="db-key",
            dataset="GLBX.MDP3",
            symbols=["ES"],
            client_factory=lambda **_: session,
        )

        await client.start()

        messages = client.messages()
        event = await anext(messages)
        assert event.type == "event"
        assert event.payload["code"] == "QUOTE_SCHEMA_UNAVAILABLE"
        assert client.quote_schema_enabled is False

        await client.stop()

    asyncio.run(scenario())
