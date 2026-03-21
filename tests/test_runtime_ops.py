from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from futures_bot.alerts.eod_summary import EodSummaryManager
from futures_bot.alerts.error_forwarder import format_fatal_error_message
from futures_bot.alerts.heartbeat import HeartbeatManager
from futures_bot.alerts.telegram import TelegramDelivery, TelegramNotifier
from futures_bot.alerts.telegram_listener import TelegramCommandListener
from futures_bot.core.enums import StrategyModule
from futures_bot.runtime.health import RuntimeStatus
from futures_bot.runtime.schedule import ET, in_daily_halt, market_is_open, next_halt_time, next_open_time
from futures_bot.runtime.state_store import JsonStateStore
from futures_bot.runtime.stale_data import StaleDataMonitor
from futures_bot.signals.models import SignalIdea
from futures_bot.signals.state import AlertStateManager


class FakeTelegramNotifier(TelegramNotifier):
    def __init__(self) -> None:
        super().__init__(token="token", chat_id="12345")
        self.texts: list[str] = []

    def send_text(self, *, text: str) -> TelegramDelivery:
        prepared = self.prepare_text(text=text)
        self.texts.append(prepared)
        return TelegramDelivery(delivered=True, message=prepared, response_code=200)


def test_cme_schedule_helpers_cover_open_halt_and_next_times() -> None:
    sunday_preopen = datetime(2026, 1, 11, 17, 30, tzinfo=ET)
    sunday_open = datetime(2026, 1, 11, 18, 0, tzinfo=ET)
    monday_halt = datetime(2026, 1, 12, 17, 15, tzinfo=ET)
    friday_close = datetime(2026, 1, 16, 17, 1, tzinfo=ET)

    assert market_is_open(sunday_preopen) is False
    assert market_is_open(sunday_open) is True
    assert in_daily_halt(monday_halt) is True
    assert market_is_open(monday_halt) is False
    assert market_is_open(friday_close) is False
    assert next_open_time(sunday_preopen) == sunday_open
    assert next_open_time(monday_halt) == datetime(2026, 1, 12, 18, 0, tzinfo=ET)
    assert next_halt_time(datetime(2026, 1, 12, 10, 0, tzinfo=ET)) == datetime(2026, 1, 12, 17, 0, tzinfo=ET)
    assert next_halt_time(friday_close) == datetime(2026, 1, 19, 17, 0, tzinfo=ET)


def test_telegram_notifier_uses_env_alert_tag(monkeypatch) -> None:
    monkeypatch.setenv("BOT_ALERT_TAG", "[BOT-2]")

    notifier = TelegramNotifier(token="token", chat_id="12345")

    assert notifier.prepare_text(text="<b>HEARTBEAT</b>").startswith("[BOT-2] ")


def test_telegram_listener_handles_start_stop_and_status() -> None:
    notifier = FakeTelegramNotifier()
    states: list[bool] = []

    async def set_signals_active(value: bool) -> None:
        states.append(value)

    listener = TelegramCommandListener(
        notifier=notifier,
        status_provider=lambda: "<b>STATUS</b>\n<b>signals_active:</b> true",
        set_signals_active=set_signals_active,
    )

    async def scenario() -> None:
        await listener._handle_update({"update_id": 1, "message": {"chat": {"id": "12345"}, "text": "/stop"}})
        await listener._handle_update({"update_id": 2, "message": {"chat": {"id": "12345"}, "text": "/start"}})
        await listener._handle_update({"update_id": 3, "message": {"chat": {"id": "12345"}, "text": "/status"}})
        await listener._handle_update({"update_id": 4, "message": {"chat": {"id": "99999"}, "text": "/stop"}})

    asyncio.run(scenario())

    assert states == [False, True]
    assert notifier.texts[0] == "[TRADER-V1] <b>Signals:</b> paused"
    assert notifier.texts[1] == "[TRADER-V1] <b>Signals:</b> active"
    assert notifier.texts[2].startswith("[TRADER-V1] ")
    assert "signals_active" in notifier.texts[2]
    assert len(notifier.texts) == 3


def test_stale_data_monitor_triggers_and_recovers() -> None:
    monitor = StaleDataMonitor(bars_timeout_s=120.0)
    first_ts = datetime(2026, 1, 12, 10, 0, tzinfo=ET)

    assert monitor.mark_bar("NQ", first_ts) == []
    stale_events = monitor.check(
        now_et=first_ts + timedelta(seconds=121),
        market_open=True,
        symbols={"NQ"},
        quote_stream_enabled=False,
    )
    assert [event.kind for event in stale_events] == ["stale"]
    recovered = monitor.mark_bar("NQ", first_ts + timedelta(seconds=122))
    assert [event.kind for event in recovered] == ["recovered"]


def test_eod_summary_sends_once_per_day() -> None:
    notifier = FakeTelegramNotifier()
    manager = EodSummaryManager()
    ts = datetime(2026, 1, 12, 10, 0, tzinfo=ET)
    halt = datetime(2026, 1, 12, 17, 0, tzinfo=ET)

    manager.record_signal(ts_et=ts, strategy="strat_a_orb", symbol="NQ")
    manager.record_closed_signal(ts_et=ts)
    manager.record_feed_issue(ts_et=ts)

    first = manager.maybe_send(now_et=halt, notifier=notifier)
    second = manager.maybe_send(now_et=halt + timedelta(minutes=10), notifier=notifier)

    assert first is not None and first.delivered is True
    assert second is None
    assert notifier.texts[0].startswith("[TRADER-V1] ")
    assert "EOD SUMMARY" in notifier.texts[0]
    assert "Total Signals" in notifier.texts[0]


def test_state_store_and_alert_state_recover_from_disk(tmp_path: Path) -> None:
    notifier = FakeTelegramNotifier()
    manager = AlertStateManager(out_dir=tmp_path, notifier=notifier)
    idea = SignalIdea(
        idea_id="strat_a_orb:NQ:BUY",
        strategy=StrategyModule.STRAT_A_ORB,
        symbol="NQ",
        symbol_display="NQ",
        side="BUY",
        entry_low=100.0,
        entry_high=100.5,
        stop_loss=99.0,
        tp1=101.0,
        tp2=102.0,
        invalidation="thesis fails below 99.00",
        partial_profit_guidance="trim into strength at 101.00",
        timestamp=datetime(2026, 1, 12, 10, 0, tzinfo=ZoneInfo("UTC")),
        flatten_by=datetime(2026, 1, 12, 11, 30, tzinfo=ZoneInfo("UTC")),
        regime="trend",
        confidence=0.9,
        strategy_context="strat_a_orb",
        last_price=100.2,
    )
    manager.register(idea)
    store = JsonStateStore(tmp_path / "runtime_state.json")
    store.save({"signals_active": False, "active_ideas": manager.snapshot_records()})

    loaded = store.load()
    recovered = AlertStateManager(out_dir=tmp_path / "restored", notifier=notifier)
    recovered.restore(list(loaded["active_ideas"]))

    assert loaded["signals_active"] is False
    assert recovered.active_count() == 1

    broken = JsonStateStore(tmp_path / "broken_state.json")
    broken.path.write_text("{bad json", encoding="utf-8")
    assert broken.load() == {}


def test_heartbeat_deduplicates_after_restart() -> None:
    notifier = FakeTelegramNotifier()
    status = RuntimeStatus(
        signals_active=True,
        market_open=True,
        in_daily_halt=False,
        feed_connected=True,
        last_bar_timestamp="2026-01-12T10:00:00-05:00",
        active_ideas=1,
        strategies_enabled=["strat_a_orb"],
        output_path="out/signals_live",
    )
    last_sent = datetime(2026, 1, 12, 8, 0, tzinfo=ET)
    manager = HeartbeatManager(last_sent_at=last_sent)

    assert manager.maybe_send(now_et=last_sent + timedelta(hours=2), status=status, notifier=notifier) is None
    sent = manager.maybe_send(now_et=last_sent + timedelta(hours=4, minutes=1), status=status, notifier=notifier)

    assert sent is not None and sent.delivered is True
    assert len(notifier.texts) == 1
    assert notifier.texts[0].startswith("[TRADER-V1] ")
    assert "HEARTBEAT" in notifier.texts[0]


def test_fatal_error_forwarding_formatting() -> None:
    ts = datetime(2026, 1, 12, 10, 5, tzinfo=ET)
    message = format_fatal_error_message(
        error_type="RuntimeError",
        message="feed subscription failed",
        timestamp_et=ts,
        component="databento_adapter",
    )

    assert "FATAL ERROR" in message
    assert "RuntimeError" in message
    assert "feed subscription failed" in message
    assert "databento_adapter" in message


def test_error_forwarder_sends_tagged_outbound_message() -> None:
    notifier = FakeTelegramNotifier()
    ts = datetime(2026, 1, 12, 10, 5, tzinfo=ET)

    delivery = notifier.send_text(
        text=format_fatal_error_message(
            error_type="RuntimeError",
            message="feed subscription failed",
            timestamp_et=ts,
            component="databento_adapter",
        )
    )

    assert delivery.delivered is True
    assert notifier.texts[0].startswith("[TRADER-V1] ")
    assert "FATAL ERROR" in notifier.texts[0]
