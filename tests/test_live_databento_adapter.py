from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import databento as db
import databento_dbn as dbn
import pytest

from futures_bot.config.loader import load_instruments
from futures_bot.core.enums import StrategyModule
from futures_bot.live.databento_adapter import (
    DEFAULT_DATABENTO_DATASET,
    DEFAULT_DATABENTO_SCHEMA,
    DEFAULT_DATABENTO_STYPE_IN,
    DEFAULT_DATABENTO_SYMBOLS,
    DatabentoLiveClient,
)
from futures_bot.live.feed_models import FeedMessage
from futures_bot.live.live_runner import run_live_signals
from futures_bot.alerts.telegram import TelegramDelivery, TelegramNotifier
from futures_bot.runtime.schedule import ET


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


def test_databento_client_classifies_subscription_ack_as_info() -> None:
    async def scenario() -> None:
        session = FakeLiveSession()
        client = DatabentoLiveClient(
            api_key="db-key",
            client_factory=lambda **_: session,
        )

        await client.start()

        callback, _ = session.callbacks[0]
        callback(
            dbn.SystemMsg(
                1_736_688_600_000_000_000,
                "Subscription request 0 for ohlcv-1m data succeeded",
            )
        )
        await asyncio.sleep(0)

        messages = client.messages()
        event = await anext(messages)
        assert event.type == "event"
        assert event.payload["code"] == "DATABENTO_SUBSCRIPTION_ACK"

        await client.stop()

    asyncio.run(scenario())


def test_databento_client_classifies_entitlement_failures() -> None:
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
                "Subscription request rejected: user not entitled for requested dataset",
            )
        )
        await asyncio.sleep(0)

        messages = client.messages()
        event = await anext(messages)
        assert event.type == "event"
        assert event.payload["code"] == "DATABENTO_ENTITLEMENT_FAILURE"

        await client.stop()

    asyncio.run(scenario())


def test_databento_client_preserves_heartbeat_and_interval_system_events() -> None:
    async def scenario() -> None:
        session = FakeLiveSession()
        client = DatabentoLiveClient(
            api_key="db-key",
            client_factory=lambda **_: session,
        )

        await client.start()

        callback, _ = session.callbacks[0]
        callback(dbn.SystemMsg(1_736_688_600_000_000_000, "heartbeat", code=dbn.SystemCode.HEARTBEAT))
        callback(
            dbn.SystemMsg(
                1_736_688_660_000_000_000,
                "end_of_interval",
                code=dbn.SystemCode.END_OF_INTERVAL,
            )
        )
        await asyncio.sleep(0)

        messages = client.messages()
        heartbeat = await anext(messages)
        interval = await anext(messages)
        assert heartbeat.payload["code"] == "DATABENTO_SYSTEM_heartbeat"
        assert interval.payload["code"] == "DATABENTO_SYSTEM_end_of_interval"

        await client.stop()

    asyncio.run(scenario())


class _FakeTelegramNotifier(TelegramNotifier):
    def __init__(self) -> None:
        super().__init__(token="token", chat_id="12345")
        self.texts: list[str] = []

    def send_text(self, *, text: str) -> TelegramDelivery:
        prepared = self.prepare_text(text=text)
        self.texts.append(prepared)
        return TelegramDelivery(delivered=True, message=prepared, response_code=200)

    def fetch_updates(self, *, offset: int | None = None, timeout_s: int = 1) -> list[dict[str, object]]:
        return []


def test_live_runner_does_not_forward_subscription_ack_as_fatal(tmp_path: Path) -> None:
    class FakeFeedClient:
        quote_schema_enabled = False

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def messages(self):
            yield FeedMessage(
                type="event",
                timestamp_et=datetime(2026, 1, 12, 9, 30, tzinfo=ET),
                symbol="*",
                payload={
                    "code": "DATABENTO_SUBSCRIPTION_ACK",
                    "detail": "Subscription request 0 for ohlcv-1m data succeeded",
                },
            )

    async def scenario() -> None:
        notifier = _FakeTelegramNotifier()
        await run_live_signals(
            out_dir=tmp_path / "live_out",
            instruments_by_symbol=load_instruments("configs"),
            enabled_strategies={StrategyModule.STRAT_A_ORB},
            notifier=notifier,
            feed_client=FakeFeedClient(),
            max_messages=1,
        )
        assert notifier.texts == []

    asyncio.run(scenario())
