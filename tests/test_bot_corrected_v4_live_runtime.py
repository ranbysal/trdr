from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from bot_corrected_v4.config import load_corrected_v4_config
from bot_corrected_v4.live import CorrectedV4LiveRunner
from futures_bot.alerts.heartbeat import HeartbeatManager
from futures_bot.live.feed_models import FeedMessage
from futures_bot.pipeline.corrected_orchestrator import (
    AcceptedSignalOutput,
    GoldEvaluationRequest,
    NQEvaluationRequest,
    RejectedSignalOutput,
)
from shared.alerts.telegram import TelegramDelivery, TelegramNotifier

ET = ZoneInfo("America/New_York")


class FakeTelegramNotifier(TelegramNotifier):
    def __init__(self) -> None:
        super().__init__(token="tg-token", chat_id="12345", alert_tag="[CORR-V4]")
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


def _build_runner(tmp_path: Path) -> tuple[CorrectedV4LiveRunner, FakeTelegramNotifier]:
    config = load_corrected_v4_config("configs/corrected_v4")
    notifier = FakeTelegramNotifier()
    runner = CorrectedV4LiveRunner(
        config=config,
        orchestrator=config.build_orchestrator(),
        instruments_by_symbol=config.load_instruments("configs/corrected_v4"),
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


def _nq_messages(*, count: int = 130) -> list[FeedMessage]:
    start = datetime(2026, 1, 5, 9, 30, tzinfo=ET)
    messages: list[FeedMessage] = []
    for index in range(count):
        close = 20_480.0 + (0.45 * index)
        messages.append(
            FeedMessage(
                type="bar_1m",
                timestamp_et=start + timedelta(minutes=index),
                symbol="NQ",
                payload={
                    "open": close - 0.2,
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "volume": 1_000.0 + index,
                },
            )
        )
    return messages


def _gold_messages(*, count: int = 2) -> list[FeedMessage]:
    start = datetime(2026, 1, 5, 9, 30, tzinfo=ET)
    messages: list[FeedMessage] = []
    for index in range(count):
        close = 2_640.0
        messages.append(
            FeedMessage(
                type="bar_1m",
                timestamp_et=start + timedelta(minutes=index),
                symbol="GC",
                payload={
                    "open": close - 0.2,
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "volume": 1_000.0 + index,
                },
            )
        )
    return messages


def _gold_bars(*, count: int) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts": message.timestamp_et,
                "open": message.payload["open"],
                "high": message.payload["high"],
                "low": message.payload["low"],
                "close": message.payload["close"],
                "volume": message.payload["volume"],
            }
            for message in _gold_messages(count=count)
        ]
    )


def test_corrected_v4_live_runner_sends_startup_ok(tmp_path: Path) -> None:
    runner, notifier = _build_runner(tmp_path)

    asyncio.run(runner.run())

    assert notifier.texts[0].startswith("[CORR-V4] STARTUP_OK")
    assert "feed_status: connected" in notifier.texts[0]
    assert f"output_dir: {tmp_path / 'out'}" in notifier.texts[0]
    assert f"state_dir: {tmp_path / 'state'}" in notifier.texts[0]


def test_corrected_v4_heartbeat_and_status_plumbing(tmp_path: Path) -> None:
    runner, notifier = _build_runner(tmp_path)
    runner._feed_connected = True
    runner._stale_monitor.mark_bar("NQ", datetime(2026, 1, 5, 10, 0, tzinfo=ET))
    runner._heartbeat = HeartbeatManager(interval_hours=0.0)

    status = runner._status_message()
    runner._maybe_send_heartbeat(datetime(2026, 1, 5, 10, 1, tzinfo=ET))
    runner._events_log.flush()

    records = [
        json.loads(line)
        for line in (tmp_path / "out" / "live_events.ndjson").read_text(encoding="utf-8").strip().splitlines()
    ]

    assert "<b>STATUS</b>" in status
    assert "STRAT_NQ_SIGNAL" in status
    assert "last_bar_timestamp" in status
    assert any(text.startswith("[CORR-V4] <b>HEARTBEAT</b>") for text in notifier.texts)
    assert any(record["event"] == "TELEGRAM_SEND_SUCCESS" and record["reason_code"] == "HEARTBEAT" for record in records)


def test_corrected_v4_live_runner_logs_feed_events(tmp_path: Path) -> None:
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

    records = [
        json.loads(line)
        for line in (tmp_path / "out" / "live_events.ndjson").read_text(encoding="utf-8").strip().splitlines()
    ]

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


def test_corrected_v4_live_runner_emits_corrected_orchestrator_signal(tmp_path: Path) -> None:
    runner, notifier = _build_runner(tmp_path)
    bars = pd.DataFrame(
        [
            {
                "ts": message.timestamp_et,
                "open": message.payload["open"],
                "high": message.payload["high"],
                "low": message.payload["low"],
                "close": message.payload["close"],
                "volume": message.payload["volume"],
            }
            for message in _nq_messages()
        ]
    )
    accepted_output = runner._orchestrator.evaluate_nq(
        NQEvaluationRequest(
            bars_1m=bars,
            instrument=runner._instruments_by_symbol["NQ"],
            session_start_equity=100_000.0,
            realized_pnl=0.0,
            open_positions=(),
            liquidity_ok=True,
            macro_blocked=False,
            pullback_price=20_520.0,
            structure_break_price=20_525.0,
            order_block_low=20_518.0,
            order_block_high=20_525.0,
        )
    )
    assert isinstance(accepted_output, AcceptedSignalOutput)
    runner._evaluate_request = lambda request: accepted_output
    runner._client._messages = _nq_messages(count=1)

    asyncio.run(runner.run())

    records = [
        json.loads(line)
        for line in (tmp_path / "out" / "live_events.ndjson").read_text(encoding="utf-8").strip().splitlines()
    ]
    execution_records = [
        json.loads(line)
        for line in (tmp_path / "out" / "execution_events.ndjson").read_text(encoding="utf-8").strip().splitlines()
    ]

    assert any(text.startswith("[CORR-V4] SIGNAL") for text in notifier.texts)
    assert any("instrument: NQ" in text for text in notifier.texts)
    assert any("strategy: strat_nq_signal" in text for text in notifier.texts)
    assert any(record["event"] == "BAR_RECEIVED" and record["symbol"] == "NQ" for record in records)
    assert any(record["event"] == "SIGNAL_ACCEPTED" and record["symbol"] == "NQ" for record in records)
    assert any(record["event"] == "SIGNAL_EMITTED" and record["symbol"] == "NQ" for record in records)
    assert any(record["event"] == "TELEGRAM_SEND_SUCCESS" and record["strategy"] == "strat_nq_signal" for record in records)
    signal_record = next(record for record in execution_records if record["event"] == "CORRECTED_EXECUTION_SIGNAL")
    assert signal_record["paper_only"] is True
    assert signal_record["symbol"] == "NQ"
    assert signal_record["entry_price"] > 0.0
    assert signal_record["stop_price"] > 0.0
    assert signal_record["tp1_price"] > 0.0
    assert any(record["event"] == "CORRECTED_MARKET_BAR" and record["symbol"] == "NQ" for record in execution_records)


def test_corrected_v4_live_runner_canonicalizes_gold_symbol_across_live_events_and_state(tmp_path: Path) -> None:
    state_path = tmp_path / "state" / "corrected_v4_live_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "last_bar_by_symbol": {
                    "GC": datetime(2026, 1, 5, 9, 29, tzinfo=ET).isoformat(),
                },
                "stale_alert_active_flags": {
                    "bar:GC": True,
                },
                "last_signal_keys": {
                    "GC": "legacy-gold-key",
                },
                "open_positions": [
                    {
                        "ts": datetime(2026, 1, 5, 9, 0, tzinfo=ET).isoformat(),
                        "symbol": "GC",
                        "quantity": 1,
                        "avg_entry_price": 2640.0,
                        "mark_price": 2641.0,
                        "point_value": 10.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    runner, notifier = _build_runner(tmp_path)
    gold_instrument = runner._instruments_by_symbol["MGC"]
    rejected_output = runner._orchestrator.evaluate_gold(
        GoldEvaluationRequest(
            bars_1m=_gold_bars(count=1),
            instrument=gold_instrument,
            session_start_equity=100_000.0,
            realized_pnl=0.0,
            open_positions=(),
            liquidity_ok=True,
            macro_blocked=False,
            pullback_price=2_640.0,
            structure_break_price=2_640.0,
            order_block_low=2_639.8,
            order_block_high=2_640.2,
        )
    )
    accepted_output = runner._orchestrator.evaluate_gold(
        GoldEvaluationRequest(
            bars_1m=_gold_bars(count=130),
            instrument=gold_instrument,
            session_start_equity=100_000.0,
            realized_pnl=0.0,
            open_positions=(),
            liquidity_ok=True,
            macro_blocked=False,
            pullback_price=2_640.0,
            structure_break_price=2_640.0,
            order_block_low=2_639.8,
            order_block_high=2_640.2,
        )
    )
    assert isinstance(rejected_output, RejectedSignalOutput)
    assert isinstance(accepted_output, AcceptedSignalOutput)
    assert runner._monitored_symbols == {"NQ", "YM", "MGC"}
    assert set(runner._stale_monitor.last_bar_by_symbol()) == {"MGC"}
    assert set(runner._open_positions) == {"MGC"}
    assert set(runner._last_signal_keys) == {"MGC"}

    outputs = iter([rejected_output, accepted_output])
    runner._evaluate_request = lambda request: next(outputs)
    runner._client._messages = _gold_messages(count=2)

    asyncio.run(runner.run())

    records = [
        json.loads(line)
        for line in (tmp_path / "out" / "live_events.ndjson").read_text(encoding="utf-8").strip().splitlines()
    ]
    execution_records = [
        json.loads(line)
        for line in (tmp_path / "out" / "execution_events.ndjson").read_text(encoding="utf-8").strip().splitlines()
    ]
    state = json.loads(state_path.read_text(encoding="utf-8"))

    assert any(text.startswith("[CORR-V4] SIGNAL") for text in notifier.texts)
    assert any("instrument: MGC" in text for text in notifier.texts)
    assert any(record["event"] == "BAR_RECEIVED" and record["symbol"] == "MGC" for record in records)
    assert any(record["event"] == "SIGNAL_REJECTED" and record["symbol"] == "MGC" for record in records)
    assert any(record["event"] == "SIGNAL_ACCEPTED" and record["symbol"] == "MGC" for record in records)
    assert any(record["event"] == "SIGNAL_EMITTED" and record["symbol"] == "MGC" for record in records)
    assert any(
        record["event"] == "TELEGRAM_SEND_SUCCESS"
        and record["strategy"] == "strat_gold_signal"
        and record["symbol"] == "MGC"
        for record in records
    )
    assert all(
        record["symbol"] != "GC"
        for record in records
        if record["event"]
        in {"BAR_RECEIVED", "SIGNAL_REJECTED", "SIGNAL_ACCEPTED", "SIGNAL_EMITTED", "TELEGRAM_SEND_SUCCESS"}
    )
    assert set(state["last_bar_by_symbol"]) == {"MGC"}
    assert set(state["last_signal_keys"]) == {"MGC"}
    assert set(state["stale_alert_active_flags"]) == {"bar:MGC"}
    assert state["open_positions"][0]["symbol"] == "MGC"
    assert any(
        record["event"] == "CORRECTED_EXECUTION_SIGNAL" and record["symbol"] == "MGC"
        for record in execution_records
    )
