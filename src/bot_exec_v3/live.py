"""Live paper execution runtime for Bot 3."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from bot_exec_v3.executor import PaperExecutor
from bot_exec_v3.models import ExecutionEventType, ExecutionRuntimeEvent, ExecutorConfig, MarketBar, SignalEvent
from bot_exec_v3.query import PaperTradeQueries
from bot_exec_v3.summary import PaperDailySummaryManager
from futures_bot.runtime.health import RuntimeStatus
from shared.alerts.heartbeat import HeartbeatManager
from shared.alerts.telegram import TelegramDelivery, TelegramNotifier
from shared.live.databento_adapter import DatabentoLiveClient
from shared.live.feed_models import FeedMessage
from shared.runtime.ndjson_writer import NdjsonWriter
from shared.runtime.schedule import in_daily_halt, market_is_open
from shared.runtime.stale_data import StaleDataEvent, StaleDataMonitor
from shared.runtime.state_store import JsonStateStore

ET = ZoneInfo("America/New_York")
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ExecutorV3RuntimeStatus:
    signals_active: bool
    market_open: bool
    in_daily_halt: bool
    feed_connected: bool
    last_bar_timestamp: str | None
    active_ideas: int
    strategies_enabled: list[str]
    output_path: str


class ExecutorV3LiveRunner:
    def __init__(
        self,
        *,
        config: ExecutorConfig,
        executor: PaperExecutor,
        out_dir: str | Path,
        state_dir: str | Path,
        notifier: TelegramNotifier,
        databento_api_key: str,
        databento_dataset: str,
        databento_schema: str,
        databento_stype_in: str,
        databento_symbols: tuple[str, ...],
        client_factory: Any | None = None,
    ) -> None:
        self._config = config
        self._executor = executor
        self._out_dir = Path(out_dir)
        self._state_dir = Path(state_dir)
        self._out_dir.mkdir(parents=True, exist_ok=True)
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._notifier = notifier
        self._symbols = tuple(databento_symbols)
        self._monitored_symbols = {_normalize_stream_symbol(symbol) for symbol in self._symbols}
        self._events_log_path = self._out_dir / "live_events.ndjson"
        self._events_log = NdjsonWriter(self._events_log_path)
        self._state_store = JsonStateStore(self._state_dir / "executor_v3_live_state.json")
        restored = self._state_store.load()
        self._signals_active = bool(restored.get("signals_active", True))
        self._signal_queue_path = Path(self._config.signal_queue_path)
        self._signal_queue_offset = int(restored.get("signal_queue_offset", 0))
        self._consumed_signal_ids = {
            str(signal_id)
            for signal_id in list(restored.get("consumed_signal_ids", []))
            if str(signal_id)
        }
        self._heartbeat = HeartbeatManager(
            interval_hours=config.heartbeat_interval_hours,
            last_sent_at=self._parse_datetime(restored.get("last_heartbeat_timestamp")),
        )
        self._daily_summary = PaperDailySummaryManager(
            queries=PaperTradeQueries(config.sqlite_path),
            last_sent_date=self._parse_date(restored.get("last_paper_summary_date_sent")),
        )
        self._stale_monitor = StaleDataMonitor(
            bars_timeout_s=config.bars_stale_after_s,
            last_bar_by_symbol=self._parse_last_bars(restored.get("last_bar_by_symbol", {})),
            stale_flags={str(k): bool(v) for k, v in dict(restored.get("stale_alert_active_flags", {})).items()},
        )
        self._feed_connected = False
        self._stop_event = asyncio.Event()
        self._listener_task: asyncio.Task[None] | None = None
        self._client = DatabentoLiveClient(
            api_key=databento_api_key,
            dataset=databento_dataset,
            schema=databento_schema,
            stype_in=databento_stype_in,
            symbols=self._symbols,
            client_factory=client_factory,
        )

    async def run(self) -> None:
        maintenance_task: asyncio.Task[None] | None = None
        startup_complete = False
        try:
            logger.info("Bot 3 Telegram command polling disabled; Bot 3 is send-only in this pass")
            await self._client.start()
            self._feed_connected = True
            await self._poll_signal_queue()
            self._save_state()
            started_at = datetime.now(tz=ET)
            self._write_telegram_event(
                self._notifier.send_text(text=self._runtime_message(title="STARTUP_OK", now_et=started_at)),
                timestamp_et=started_at,
                symbol="*",
                strategy="runtime",
                reason_code="STARTUP_OK",
            )
            startup_complete = True
            maintenance_task = asyncio.create_task(self._maintenance_loop(), name="executor-v3-live-maintenance")
            async for message in self._client.messages():
                await self._poll_signal_queue()
                await self._handle_message(message)
            await self._poll_signal_queue()
        except Exception as exc:
            failed_at = datetime.now(tz=ET)
            reason_code = "STARTUP_FAILED" if not startup_complete else "SHUTDOWN_ERROR"
            self._write_telegram_event(
                self._notifier.send_text(text=self._runtime_message(title=reason_code, now_et=failed_at, error=exc)),
                timestamp_et=failed_at,
                symbol="*",
                strategy="runtime",
                reason_code=reason_code,
            )
            logger.exception("Executor V3 live runner failed")
            raise
        finally:
            self._stop_event.set()
            self._feed_connected = False
            if maintenance_task is not None:
                maintenance_task.cancel()
                try:
                    await maintenance_task
                except asyncio.CancelledError:
                    pass
            await self._client.stop()
            self._save_state()
            self._events_log.flush()

    async def _maintenance_loop(self) -> None:
        while True:
            await asyncio.sleep(1)
            now_et = datetime.now(tz=ET)
            await self._poll_signal_queue()
            self._check_stale(now_et)
            self._maybe_send_heartbeat(now_et)
            self._maybe_send_daily_summary(now_et)
            self._save_state()

    async def _poll_signal_queue(self) -> None:
        if not self._signal_queue_path.exists():
            return
        file_size = self._signal_queue_path.stat().st_size
        if file_size < self._signal_queue_offset:
            self._signal_queue_offset = 0
        with self._signal_queue_path.open("r", encoding="utf-8") as handle:
            handle.seek(self._signal_queue_offset)
            while True:
                line_start = handle.tell()
                line = handle.readline()
                if not line:
                    break
                if not line.endswith("\n"):
                    handle.seek(line_start)
                    break
                self._signal_queue_offset = handle.tell()
                payload_text = line.strip()
                if not payload_text:
                    continue
                try:
                    payload = json.loads(payload_text)
                except json.JSONDecodeError as exc:
                    self._write_event(
                        event="SIGNAL_PARSE_ERROR",
                        timestamp_et=datetime.now(tz=ET),
                        symbol="*",
                        reason_code="INVALID_JSON",
                        detail=str(exc),
                    )
                    continue
                if not isinstance(payload, dict):
                    self._write_event(
                        event="SIGNAL_PARSE_ERROR",
                        timestamp_et=datetime.now(tz=ET),
                        symbol="*",
                        reason_code="INVALID_SIGNAL_RECORD",
                        detail="queue payload must be a JSON object",
                    )
                    continue
                try:
                    signal = SignalEvent.from_mapping(payload)
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_event(
                        event="SIGNAL_PARSE_ERROR",
                        timestamp_et=datetime.now(tz=ET),
                        symbol=str(payload.get("instrument", "*")),
                        reason_code="INVALID_SIGNAL_SCHEMA",
                        detail=str(exc),
                    )
                    continue
                if signal.signal_id in self._consumed_signal_ids:
                    self._write_event(
                        event="SIGNAL_SKIPPED_DUPLICATE",
                        timestamp_et=datetime.now(tz=ET),
                        symbol=signal.instrument,
                        reason_code=signal.signal_id,
                    )
                    continue
                received_at = datetime.now(tz=ET)
                result = self._executor.submit_signal(signal, received_at_et=received_at)
                self._consumed_signal_ids.add(signal.signal_id)
                self._emit_execution_events(result.events)
                if not result.accepted:
                    self._write_event(
                        event="SIGNAL_REJECTED",
                        timestamp_et=received_at,
                        symbol=signal.instrument,
                        reason_code=result.reason,
                        detail=signal.signal_id,
                    )
        self._save_state()

    async def _handle_message(self, message: FeedMessage) -> None:
        if message.type == "event":
            self._handle_event(message)
            return
        if message.type == "quote_1s":
            self._feed_connected = True
            for event in self._stale_monitor.mark_quote(message.symbol, message.timestamp_et):
                self._handle_stale_event(event)
            return
        if message.type != "bar_1m":
            return

        self._feed_connected = True
        for event in self._stale_monitor.mark_bar(message.symbol, message.timestamp_et):
            self._handle_stale_event(event)
        self._write_event(
            event="BAR_RECEIVED",
            timestamp_et=message.timestamp_et,
            symbol=message.symbol,
            reason_code=None,
        )
        instrument = _instrument_from_symbol(message.symbol)
        if instrument is None:
            logger.debug("Ignoring unsupported Bot 3 symbol: %s", message.symbol)
            return
        payload = message.payload
        result = self._executor.on_market_bar(
            MarketBar(
                instrument=instrument,
                timestamp_et=message.timestamp_et.astimezone(ET),
                open=float(payload["open"]),
                high=float(payload["high"]),
                low=float(payload["low"]),
                close=float(payload["close"]),
            )
        )
        self._emit_execution_events(result.events)
        self._save_state()

    def _emit_execution_events(self, events: tuple[ExecutionRuntimeEvent, ...]) -> None:
        for event in events:
            self._write_execution_event(event)
            self._notify_execution_event(event)

    def _notify_execution_event(self, event: ExecutionRuntimeEvent) -> None:
        if event.event_type not in {
            ExecutionEventType.SIGNAL_RECEIVED,
            ExecutionEventType.ORDER_FILLED,
            ExecutionEventType.TP_HIT,
            ExecutionEventType.STOP_HIT,
            ExecutionEventType.POSITION_CLOSED,
        }:
            return
        lines = [
            event.event_type.value,
            f"signal_id: {event.signal_id}",
            f"instrument: {event.instrument}",
        ]
        if event.order_id is not None:
            lines.append(f"order_id: {event.order_id}")
        if event.position_id is not None:
            lines.append(f"position_id: {event.position_id}")
        if event.fill_type is not None:
            lines.append(f"fill_type: {event.fill_type.value}")
        if event.price is not None:
            lines.append(f"price: {event.price:.2f}")
        if event.quantity is not None:
            lines.append(f"quantity: {event.quantity}")
        if event.realized_pnl is not None:
            lines.append(f"realized_pnl: {event.realized_pnl:.2f}")
        lines.append(f"detail: {event.message}")
        delivery = self._notifier.send_text(text="\n".join(lines))
        self._write_telegram_event(
            delivery,
            timestamp_et=event.timestamp_et,
            symbol=event.instrument,
            strategy="executor_v3",
            reason_code=event.event_type.value,
        )

    def _write_execution_event(self, event: ExecutionRuntimeEvent) -> None:
        payload = {
            "event": event.event_type.value,
            "timestamp_et": event.timestamp_et.isoformat(),
            "symbol": event.instrument,
            "strategy": "executor_v3",
            "reason_code": event.fill_type.value if event.fill_type is not None else event.event_type.value,
            "signal_id": event.signal_id,
            "order_id": event.order_id,
            "position_id": event.position_id,
            "price": event.price,
            "quantity": event.quantity,
            "realized_pnl": event.realized_pnl,
            "detail": event.message,
        }
        self._events_log.write(payload)

    def _check_stale(self, now_et: datetime) -> None:
        events = self._stale_monitor.check(
            now_et=now_et,
            market_open=market_is_open(now_et),
            symbols=set(self._monitored_symbols),
            quote_stream_enabled=self._client.quote_schema_enabled,
        )
        for event in events:
            self._handle_stale_event(event)

    def _handle_event(self, message: FeedMessage) -> None:
        code = str(message.payload.get("code", "EVENT"))
        detail = message.payload.get("detail")
        logger.info("Bot 3 live event: symbol=%s payload=%s", message.symbol, message.payload)
        if code in {"DATABENTO_RECONNECTED", "DATABENTO_SUBSCRIPTION_ACK"} or code.startswith("DATABENTO_SYSTEM_"):
            self._feed_connected = True
        elif code.startswith("DATABENTO_"):
            self._feed_connected = False
        self._write_event(
            event="feed_event",
            timestamp_et=message.timestamp_et,
            symbol=message.symbol,
            reason_code=code,
            detail=str(detail) if detail is not None else None,
        )
        self._save_state()

    def _handle_stale_event(self, event: StaleDataEvent) -> None:
        if event.kind == "recovered":
            message = (
                "<b>DATA RECOVERED</b>\n"
                f"<b>Symbol:</b> {event.symbol}\n"
                f"<b>Stream:</b> {event.stream}\n"
                f"<b>Timestamp:</b> {event.last_timestamp.isoformat() if event.last_timestamp else 'none'}"
            )
        elif event.kind == "persistent":
            message = (
                "<b>PERSISTENT STALE DATA</b>\n"
                f"<b>Symbol:</b> {event.symbol}\n"
                f"<b>Stream:</b> {event.stream}\n"
                f"<b>Lag Seconds:</b> {event.lag_seconds:.1f}"
            )
        else:
            message = (
                "<b>STALE DATA</b>\n"
                f"<b>Symbol:</b> {event.symbol}\n"
                f"<b>Stream:</b> {event.stream}\n"
                f"<b>Lag Seconds:</b> {event.lag_seconds:.1f}"
            )
        delivery = self._notifier.send_text(text=message)
        self._write_telegram_event(
            delivery,
            timestamp_et=datetime.now(tz=ET),
            symbol=event.symbol,
            strategy="runtime",
            reason_code=f"STALE_{event.stream.upper()}_{event.kind.upper()}",
        )
        self._save_state()

    def _maybe_send_heartbeat(self, now_et: datetime) -> None:
        delivery = self._heartbeat.maybe_send(
            now_et=now_et,
            status=self._status(now_et),
            notifier=self._notifier,
        )
        if delivery is not None:
            self._write_telegram_event(
                delivery,
                timestamp_et=now_et,
                symbol="*",
                strategy="runtime",
                reason_code="HEARTBEAT",
            )

    def _maybe_send_daily_summary(self, now_et: datetime) -> None:
        delivery = self._daily_summary.maybe_send(now_et=now_et, notifier=self._notifier)
        if delivery is not None:
            self._write_telegram_event(
                delivery,
                timestamp_et=now_et,
                symbol="*",
                strategy="runtime",
                reason_code="PAPER_DAILY_SUMMARY",
            )

    def _status(self, now_et: datetime) -> RuntimeStatus:
        last_bar = max(self._stale_monitor.last_bar_by_symbol().values(), default=None)
        return RuntimeStatus(
            signals_active=self._signals_active,
            market_open=market_is_open(now_et),
            in_daily_halt=in_daily_halt(now_et),
            feed_connected=self._feed_connected,
            last_bar_timestamp=last_bar.isoformat() if last_bar is not None else None,
            active_ideas=0,
            strategies_enabled=["EXECUTOR_V3_PAPER"],
            output_path=str(self._events_log_path),
        )

    def _runtime_message(self, *, title: str, now_et: datetime, error: Exception | None = None) -> str:
        status = self._status(now_et)
        lines = [
            title,
            f"feed_status: {'connected' if status.feed_connected else 'disconnected'}",
            f"market: {'open' if status.market_open else 'closed'}",
            f"signal_queue: {self._signal_queue_path}",
            f"output_dir: {self._out_dir}",
            f"state_dir: {self._state_dir}",
        ]
        if error is not None:
            lines.insert(1, f"error_type: {type(error).__name__}")
            lines.insert(2, f"error: {str(error) or type(error).__name__}")
        return "\n".join(lines)

    def _write_event(
        self,
        *,
        event: str,
        timestamp_et: datetime,
        symbol: str,
        reason_code: str | None,
        detail: str | None = None,
    ) -> None:
        payload = {
            "event": event,
            "timestamp_et": timestamp_et.isoformat(),
            "symbol": symbol,
            "strategy": "runtime",
            "reason_code": reason_code,
            "score": None,
        }
        if detail is not None:
            payload["detail"] = detail
        self._events_log.write(payload)

    def _write_telegram_event(
        self,
        delivery: TelegramDelivery,
        *,
        timestamp_et: datetime,
        symbol: str,
        strategy: str,
        reason_code: str | None,
    ) -> None:
        self._events_log.write(
            {
                "event": "TELEGRAM_SEND_SUCCESS" if delivery.delivered else "TELEGRAM_SEND_FAILURE",
                "timestamp_et": timestamp_et.isoformat(),
                "symbol": symbol,
                "strategy": strategy,
                "reason_code": reason_code,
                "score": None,
                "delivery_error": delivery.error,
            }
        )

    def _save_state(self) -> None:
        self._state_store.save(
            {
                "signals_active": self._signals_active,
                "signal_queue_offset": self._signal_queue_offset,
                "consumed_signal_ids": sorted(self._consumed_signal_ids),
                "last_heartbeat_timestamp": self._format_datetime(self._heartbeat.last_sent_at),
                "last_paper_summary_date_sent": self._format_date(self._daily_summary.last_sent_date),
                "last_bar_by_symbol": {
                    symbol: ts.isoformat()
                    for symbol, ts in self._stale_monitor.last_bar_by_symbol().items()
                },
                "stale_alert_active_flags": self._stale_monitor.stale_flags(),
            }
        )

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if not value or not isinstance(value, str):
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _parse_last_bars(payload: Any) -> dict[str, datetime]:
        if not isinstance(payload, dict):
            return {}
        parsed: dict[str, datetime] = {}
        for symbol, raw_ts in payload.items():
            if not isinstance(raw_ts, str):
                continue
            try:
                parsed[str(symbol)] = datetime.fromisoformat(raw_ts)
            except ValueError:
                continue
        return parsed

    @staticmethod
    def _format_datetime(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    @staticmethod
    def _parse_date(value: Any):
        if not value or not isinstance(value, str):
            return None
        try:
            return datetime.fromisoformat(value).date() if "T" in value else datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None

    @staticmethod
    def _format_date(value) -> str | None:
        return value.isoformat() if value is not None else None


async def run_live_signals(
    *,
    config: ExecutorConfig,
    executor: PaperExecutor,
    out_dir: str | Path,
    state_dir: str | Path,
    notifier: TelegramNotifier,
    databento_api_key: str,
    databento_dataset: str,
    databento_schema: str,
    databento_stype_in: str,
    databento_symbols: tuple[str, ...],
    client_factory: Any | None = None,
) -> None:
    runner = ExecutorV3LiveRunner(
        config=config,
        executor=executor,
        out_dir=out_dir,
        state_dir=state_dir,
        notifier=notifier,
        databento_api_key=databento_api_key,
        databento_dataset=databento_dataset,
        databento_schema=databento_schema,
        databento_stype_in=databento_stype_in,
        databento_symbols=databento_symbols,
        client_factory=client_factory,
    )
    await runner.run()


def _instrument_from_symbol(symbol: str) -> str | None:
    normalized = _normalize_stream_symbol(symbol)
    if normalized.startswith("NQ"):
        return "NQ"
    if normalized.startswith("YM"):
        return "YM"
    if normalized.startswith(("GC", "MGC", "GOLD")):
        return "GC"
    if normalized.startswith(("SI", "SILVER")):
        return "SI"
    return None


def _normalize_stream_symbol(symbol: str) -> str:
    return str(symbol).split(".", 1)[0].upper()
