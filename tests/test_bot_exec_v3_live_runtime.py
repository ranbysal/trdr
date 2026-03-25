from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from bot_exec_v3.executor import PaperExecutor
from bot_exec_v3.journal import PaperTradeJournal
from bot_exec_v3.live import ExecutorV3LiveRunner
from bot_exec_v3.models import Direction, ExecutorConfig, SignalEvent, SizingConfig
from bot_exec_v3.risk import PaperRiskSizer
from futures_bot.alerts.telegram import TelegramDelivery, TelegramNotifier
from futures_bot.live.feed_models import FeedMessage

ET = ZoneInfo("America/New_York")


class FakeTelegramNotifier(TelegramNotifier):
    def __init__(self) -> None:
        super().__init__(token="tg-token", chat_id="12345", alert_tag="[EXEC-V3]")
        self.texts: list[str] = []

    def send_text(self, *, text: str) -> TelegramDelivery:
        prepared = self.prepare_text(text=text)
        self.texts.append(prepared)
        return TelegramDelivery(delivered=True, message=prepared, response_code=200)


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


def _signal(signal_id: str = "prop_v2-test-sig") -> SignalEvent:
    return SignalEvent(
        signal_id=signal_id,
        source_bot="prop_v2",
        instrument="NQ",
        direction=Direction.LONG,
        setup_type="ict_smc",
        session="ny_open",
        confluence=4.0,
        formed_timestamp_et=datetime.now(tz=ET),
        entry=100.0,
        stop=95.0,
        tp1=105.0,
        tp2=110.0,
        tp3=115.0,
        notes="queue consumer test",
        freshness_seconds=180,
    )


def _build_runner(tmp_path: Path) -> tuple[ExecutorV3LiveRunner, FakeTelegramNotifier, Path]:
    queue_path = tmp_path / "prop_v2" / "execution_signals.ndjson"
    db_path = tmp_path / "state" / "paper_ledger.db"
    config = ExecutorConfig(
        enabled=True,
        source_bot="prop_v2",
        signal_queue_path=queue_path,
        sqlite_path=db_path,
        freshness_seconds=180,
        paper_mode=True,
        sizing=SizingConfig(
            default_contracts=1,
            contracts_by_instrument={"NQ": 1},
            point_value_by_instrument={"NQ": 1.0},
        ),
        alert_tag="[EXEC-V3]",
        databento_symbols=("NQ.v.0",),
    )
    journal = PaperTradeJournal(config.sqlite_path)
    executor = PaperExecutor(config=config, journal=journal, risk_sizer=PaperRiskSizer(config.sizing))
    notifier = FakeTelegramNotifier()
    runner = ExecutorV3LiveRunner(
        config=config,
        executor=executor,
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
    return runner, notifier, queue_path


def _write_signal(queue_path: Path, signal: SignalEvent) -> None:
    queue_path.parent.mkdir(parents=True, exist_ok=True)
    queue_path.write_text(json.dumps(signal.to_record()) + "\n", encoding="utf-8")


def test_exec_v3_live_runner_consumes_signal_once(tmp_path: Path) -> None:
    runner, _, queue_path = _build_runner(tmp_path)
    _write_signal(queue_path, _signal())

    asyncio.run(runner.run())

    runner2, _, _ = _build_runner(tmp_path)
    asyncio.run(runner2.run())

    with sqlite3.connect(tmp_path / "state" / "paper_ledger.db") as conn:
        count = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    assert count == 1


def test_exec_v3_live_runner_writes_runtime_events_ndjson(tmp_path: Path) -> None:
    runner, _, queue_path = _build_runner(tmp_path)
    _write_signal(queue_path, _signal())
    runner._client._messages = [
        FeedMessage(
            type="bar_1m",
            timestamp_et=datetime(2026, 1, 12, 9, 31, tzinfo=ET),
            symbol="NQ",
            payload={"open": 99.5, "high": 100.5, "low": 99.0, "close": 100.0, "volume": 100.0},
        ),
        FeedMessage(
            type="bar_1m",
            timestamp_et=datetime(2026, 1, 12, 9, 32, tzinfo=ET),
            symbol="NQ",
            payload={"open": 100.0, "high": 106.0, "low": 99.5, "close": 105.0, "volume": 120.0},
        ),
    ]

    asyncio.run(runner.run())

    lines = (tmp_path / "out" / "live_events.ndjson").read_text(encoding="utf-8").strip().splitlines()
    records = [json.loads(line) for line in lines]

    assert any(record["event"] == "SIGNAL_RECEIVED" for record in records)
    assert any(record["event"] == "ORDER_FILLED" for record in records)
    assert any(record["event"] == "TP_HIT" for record in records)
    assert any(record["event"] == "POSITION_CLOSED" for record in records)


def test_exec_v3_live_runner_sends_execution_notifications(tmp_path: Path) -> None:
    runner, notifier, queue_path = _build_runner(tmp_path)
    _write_signal(queue_path, _signal())
    runner._client._messages = [
        FeedMessage(
            type="bar_1m",
            timestamp_et=datetime(2026, 1, 12, 9, 31, tzinfo=ET),
            symbol="NQ",
            payload={"open": 99.5, "high": 100.5, "low": 99.0, "close": 100.0, "volume": 100.0},
        ),
        FeedMessage(
            type="bar_1m",
            timestamp_et=datetime(2026, 1, 12, 9, 32, tzinfo=ET),
            symbol="NQ",
            payload={"open": 100.0, "high": 106.0, "low": 99.5, "close": 105.0, "volume": 120.0},
        ),
    ]

    asyncio.run(runner.run())

    assert any(text.startswith("[EXEC-V3] SIGNAL_RECEIVED") for text in notifier.texts)
    assert any(text.startswith("[EXEC-V3] ORDER_FILLED") for text in notifier.texts)
    assert any(text.startswith("[EXEC-V3] TP_HIT") for text in notifier.texts)
    assert any(text.startswith("[EXEC-V3] POSITION_CLOSED") for text in notifier.texts)
