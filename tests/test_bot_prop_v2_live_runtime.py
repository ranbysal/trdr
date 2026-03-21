from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from bot_prop_v2.config import load_prop_v2_config
from bot_prop_v2.live import PropV2LiveRunner
from bot_prop_v2.pipeline import build_pipeline
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
    assert runner._listener is not None


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

    assert records[0]["event"] == "feed_event"
    assert records[0]["reason_code"] == "DATABENTO_SYSTEM_0"
    assert records[0]["detail"] == "heartbeat"


def test_prop_v2_status_reply_is_tagged(tmp_path: Path) -> None:
    runner, notifier = _build_runner(tmp_path)

    async def scenario() -> None:
        await runner._listener._handle_update(
            {
                "update_id": 1,
                "message": {
                    "chat": {"id": "12345"},
                    "text": "/status",
                },
            }
        )

    asyncio.run(scenario())

    assert notifier.texts[0].startswith("[PROP-V2] <b>STATUS</b>")
    assert "signals_active" in notifier.texts[0]
    assert "PROP_V2_SIGNAL_ENGINE" in notifier.texts[0]
