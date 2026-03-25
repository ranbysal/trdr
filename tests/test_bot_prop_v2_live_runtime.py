from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from bot_prop_v2.config import load_prop_v2_config
from bot_prop_v2.live import PropV2LiveRunner
from bot_prop_v2.pipeline import build_pipeline
from bot_prop_v2.pipeline.signal_engine import Direction, Instrument, SessionWindow, Signal, SignalType
from futures_bot.alerts.telegram import TelegramDelivery, TelegramNotifier
from futures_bot.live.feed_models import FeedMessage

ET = ZoneInfo("America/New_York")


class FakeTelegramNotifier(TelegramNotifier):
    def __init__(self) -> None:
        super().__init__(token="tg-token", chat_id="12345", alert_tag="[PROP-V2]")
        self.texts: list[str] = []

    def send_text(self, *, text: str) -> TelegramDelivery:
        prepared = self.prepare_text(text=text)
        self.texts.append(prepared)
        return TelegramDelivery(delivered=True, message=prepared, response_code=200)

    def fetch_updates(self, *, offset: int | None = None, timeout_s: int = 1) -> list[dict[str, object]]:
        return []


class FakeDatabentoClient:
    quote_schema_enabled = False

    def __init__(self, **_: object) -> None:
        self._messages: list[FeedMessage] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def messages(self):
        for message in self._messages:
            yield message


def _build_runner(tmp_path: Path) -> tuple[PropV2LiveRunner, FakeTelegramNotifier]:
    config = load_prop_v2_config("configs/prop_v2")
    pipeline = build_pipeline(config, out_dir=tmp_path / "out")
    notifier = FakeTelegramNotifier()
    runner = PropV2LiveRunner(
        config=config,
        engine=pipeline.engine,
        out_dir=tmp_path / "out",
        state_dir=tmp_path / "state",
        notifier=notifier,
        databento_api_key="db-key",
        databento_dataset=config.dataset,
        databento_schema=config.schema,
        databento_stype_in=config.stype_in,
        databento_symbols=config.databento_symbols,
    )
    runner._client = FakeDatabentoClient()
    return runner, notifier


def test_prop_v2_live_runner_initializes_ndjson_writer(tmp_path: Path) -> None:
    runner, notifier = _build_runner(tmp_path)

    assert runner._events_log_path == tmp_path / "out" / "live_events.ndjson"
    assert runner._events_log_path.parent.exists()
    assert runner._state_store.path == tmp_path / "state" / "prop_v2_live_state.json"
    assert runner._notifier is notifier
    assert not hasattr(runner, "_listener")
    assert runner._listener_task is None


def test_prop_v2_live_runner_writes_feed_event_ndjson(tmp_path: Path) -> None:
    runner, _ = _build_runner(tmp_path)
    runner._client._messages = [
        FeedMessage(
            type="event",
            timestamp_et=datetime(2026, 1, 12, 9, 30, tzinfo=ET),
            symbol="*",
            payload={"code": "DATABENTO_SYSTEM_0", "detail": "heartbeat"},
        )
    ]

    asyncio.run(runner.run())

    lines = (tmp_path / "out" / "live_events.ndjson").read_text(encoding="utf-8").strip().splitlines()
    records = [json.loads(line) for line in lines]

    assert any(
        record["event"] == "TELEGRAM_SEND_SUCCESS" and record["reason_code"] == "STARTUP_OK"
        for record in records
    )
    assert any(
        record["event"] == "feed_event"
        and record["reason_code"] == "DATABENTO_SYSTEM_0"
        and record["detail"] == "heartbeat"
        for record in records
    )


def test_prop_v2_live_runner_sends_startup_ok(tmp_path: Path) -> None:
    runner, notifier = _build_runner(tmp_path)

    asyncio.run(runner.run())

    assert notifier.texts[0].startswith("[PROP-V2] STARTUP_OK")
    assert "feed_status: connected" in notifier.texts[0]
    assert "market: " in notifier.texts[0]
    assert f"output_dir: {tmp_path / 'out'}" in notifier.texts[0]
    assert f"state_dir: {tmp_path / 'state'}" in notifier.texts[0]


def test_prop_v2_live_runner_does_not_start_telegram_polling_listener(caplog, tmp_path: Path) -> None:
    runner, _ = _build_runner(tmp_path)

    with caplog.at_level("INFO"):
        asyncio.run(runner.run())

    assert "Telegram command polling disabled; Telegram control is centralized in Trader V1" in caplog.text
    assert not hasattr(runner, "_listener")
    assert runner._listener_task is None


def test_prop_v2_live_runner_writes_execution_signal_queue(tmp_path: Path) -> None:
    runner, _ = _build_runner(tmp_path)
    runner._client._messages = [
        FeedMessage(
            type="bar_1m",
            timestamp_et=datetime(2026, 1, 12, 9, 30, tzinfo=ET),
            symbol="NQ",
            payload={"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000.0},
        )
    ]
    runner._engine.on_candle = lambda **_: Signal(
        instrument=Instrument.NQ,
        direction=Direction.LONG,
        signal_type=SignalType.ICT_SMC,
        entry_price=100.5,
        stop_loss=98.0,
        take_profit_1=103.0,
        take_profit_2=105.5,
        take_profit_3=108.0,
        risk_amount_usd=250.0,
        position_size=1.0,
        confluence_score=4,
        signal_type_name=SignalType.ICT_SMC.value,
        session=SessionWindow.NY_OPEN,
        formed_at=datetime(2026, 1, 12, 9, 30, tzinfo=ET),
        notes="execution queue test",
    )

    asyncio.run(runner.run())

    lines = (tmp_path / "out" / "execution_signals.ndjson").read_text(encoding="utf-8").strip().splitlines()
    record = json.loads(lines[0])

    assert record["source_bot"] == "prop_v2"
    assert record["instrument"] == "NQ"
    assert record["direction"] == "LONG"
    assert record["signal_id"].startswith("prop_v2-")
