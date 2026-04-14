"""Live corrected signal runtime for the isolated V4 path."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pandas as pd

from bot_corrected_v4.config import CorrectedV4Config
from futures_bot.core.enums import OrderSide
from futures_bot.core.types import InstrumentMeta
from futures_bot.pipeline.corrected_orchestrator import (
    AcceptedSignalOutput,
    CorrectedSignalOrchestrator,
    CorrectedOrchestratorOutput,
    GoldEvaluationRequest,
    NQEvaluationRequest,
    YMEvaluationRequest,
)
from futures_bot.risk.models import OpenPositionMtmSnapshot
from shared.alerts.heartbeat import HeartbeatManager
from shared.alerts.telegram import TelegramDelivery, TelegramNotifier
from shared.runtime.health import RuntimeStatus
from shared.runtime.ndjson_writer import NdjsonWriter
from shared.runtime.schedule import ET, in_daily_halt, market_is_open
from shared.runtime.stale_data import StaleDataEvent, StaleDataMonitor
from shared.runtime.state_store import JsonStateStore

if TYPE_CHECKING:
    from shared.live.feed_models import FeedMessage

logger = logging.getLogger(__name__)
UTC = ZoneInfo("UTC")


@dataclass(slots=True)
class SymbolHistory:
    bars_1m: list[dict[str, object]] = field(default_factory=list)
    last_signal_key: str | None = None


class CorrectedV4LiveRunner:
    def __init__(
        self,
        *,
        config: CorrectedV4Config,
        orchestrator: CorrectedSignalOrchestrator,
        instruments_by_symbol: dict[str, InstrumentMeta],
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
        self._orchestrator = orchestrator
        self._instruments_by_symbol = dict(instruments_by_symbol)
        self._out_dir = Path(out_dir)
        self._state_dir = Path(state_dir)
        self._out_dir.mkdir(parents=True, exist_ok=True)
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._notifier = notifier
        self._symbols = tuple(databento_symbols)
        self._monitored_symbols = {
            _canonical_runtime_symbol(symbol, self._config.gold_symbol) for symbol in self._symbols
        }
        self._events_log_path = self._out_dir / "live_events.ndjson"
        self._events_log = NdjsonWriter(self._events_log_path)
        self._state_store = JsonStateStore(self._state_dir / "corrected_v4_live_state.json")
        restored = self._state_store.load()
        self._signals_active = bool(restored.get("signals_active", True))
        self._heartbeat = HeartbeatManager(
            interval_hours=config.heartbeat_interval_hours,
            last_sent_at=self._parse_datetime(restored.get("last_heartbeat_timestamp")),
        )
        self._stale_monitor = StaleDataMonitor(
            bars_timeout_s=config.bars_stale_after_s,
            last_bar_by_symbol=self._parse_last_bars(restored.get("last_bar_by_symbol", {})),
            stale_flags=self._parse_stale_flags(restored.get("stale_alert_active_flags", {})),
        )
        self._last_signal_keys = self._parse_last_signal_keys(restored.get("last_signal_keys", {}))
        self._histories: dict[str, SymbolHistory] = {}
        self._session_start_equity = float(restored.get("session_start_equity", config.starting_equity))
        self._realized_pnl = float(restored.get("realized_pnl", 0.0))
        self._open_positions = self._parse_open_positions(restored.get("open_positions", []))
        self._feed_connected = False
        self._stop_event = asyncio.Event()
        self._listener_task: asyncio.Task[None] | None = None
        from shared.live.databento_adapter import DatabentoLiveClient

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
            logger.info(
                "Corrected V4 Telegram command polling disabled; Telegram control is centralized in Trader V1"
            )
            await self._client.start()
            self._feed_connected = True
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
            maintenance_task = asyncio.create_task(
                self._maintenance_loop(),
                name="corrected-v4-live-maintenance",
            )
            async for message in self._client.messages():
                await self._handle_message(message)
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
            logger.exception("Corrected V4 live runner failed")
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
            await asyncio.sleep(5)
            now_et = datetime.now(tz=ET)
            self._check_stale(now_et)
            self._maybe_send_heartbeat(now_et)
            self._save_state()

    async def _handle_message(self, message: FeedMessage) -> None:
        if message.type == "event":
            self._handle_event(message)
            return
        canonical_symbol = _canonical_runtime_symbol(message.symbol, self._config.gold_symbol)
        if message.type == "quote_1s":
            self._feed_connected = True
            for event in self._stale_monitor.mark_quote(canonical_symbol, message.timestamp_et):
                self._handle_stale_event(event)
            return
        if message.type != "bar_1m":
            return

        self._feed_connected = True
        for event in self._stale_monitor.mark_bar(canonical_symbol, message.timestamp_et):
            self._handle_stale_event(event)
        self._write_event(
            event="BAR_RECEIVED",
            timestamp_et=message.timestamp_et,
            symbol=canonical_symbol,
            strategy="runtime",
            reason_code=None,
        )

        instrument = _resolve_live_instrument(
            symbol=canonical_symbol,
            instruments_by_symbol=self._instruments_by_symbol,
            gold_symbol=self._config.gold_symbol,
        )
        if instrument is None:
            logger.debug("Ignoring unsupported Corrected V4 symbol: %s", message.symbol)
            self._save_state()
            return

        payload = message.payload
        bar_ts = message.timestamp_et.astimezone(ET)
        history = self._histories.setdefault(
            canonical_symbol,
            SymbolHistory(last_signal_key=self._last_signal_keys.get(canonical_symbol)),
        )
        _upsert_bar(
            history.bars_1m,
            {
                "ts": bar_ts,
                "open": float(payload["open"]),
                "high": float(payload["high"]),
                "low": float(payload["low"]),
                "close": float(payload["close"]),
                "volume": float(payload["volume"]),
            },
            max_keep=20_000,
        )
        request = _build_live_request(
            symbol=canonical_symbol,
            gold_symbol=self._config.gold_symbol,
            bars_1m=pd.DataFrame(history.bars_1m),
            instrument=instrument,
            session_start_equity=self._session_start_equity,
            realized_pnl=self._realized_pnl,
            open_positions=tuple(self._open_positions.values()),
        )
        if request is None:
            self._save_state()
            return
        output = self._evaluate_request(request)
        self._handle_orchestrator_output(output=output, history=history, instrument=instrument)
        self._save_state()

    def _evaluate_request(
        self,
        request: NQEvaluationRequest | YMEvaluationRequest | GoldEvaluationRequest,
    ) -> CorrectedOrchestratorOutput:
        if isinstance(request, NQEvaluationRequest):
            return self._orchestrator.evaluate_nq(request)
        if isinstance(request, YMEvaluationRequest):
            return self._orchestrator.evaluate_ym(request)
        return self._orchestrator.evaluate_gold(request)

    def _handle_orchestrator_output(
        self,
        *,
        output: CorrectedOrchestratorOutput,
        history: SymbolHistory,
        instrument: InstrumentMeta,
    ) -> None:
        if not isinstance(output, AcceptedSignalOutput):
            self._write_event(
                event="SIGNAL_REJECTED",
                timestamp_et=datetime.now(tz=ET),
                symbol=instrument.symbol,
                strategy="runtime",
                reason_code=getattr(output, "rejection_reason", "UNKNOWN_REJECTION"),
            )
            return

        signal = output.signal
        signal_key = f"{signal.symbol}:{signal.strategy.value}:{signal.side.value}:{signal.ts.isoformat()}"
        if not self._signals_active:
            self._write_event(
                event="SIGNAL_SKIPPED_PAUSED",
                timestamp_et=signal.ts.astimezone(ET),
                symbol=signal.symbol,
                strategy=signal.strategy.value,
                reason_code=signal.side.value,
                detail=signal_key,
            )
            return
        if signal_key == history.last_signal_key:
            self._write_event(
                event="SIGNAL_SUPPRESSED_DUPLICATE",
                timestamp_et=signal.ts.astimezone(ET),
                symbol=signal.symbol,
                strategy=signal.strategy.value,
                reason_code=signal.side.value,
                detail=signal_key,
            )
            return

        self._write_signal_event(event="SIGNAL_EMITTED", output=output)
        delivery = self._notifier.send_text(text=_format_signal_message(output))
        self._write_telegram_event(
            delivery,
            timestamp_et=signal.ts.astimezone(ET),
            symbol=signal.symbol,
            strategy=signal.strategy.value,
            reason_code=signal.side.value,
        )
        history.last_signal_key = signal_key
        self._last_signal_keys[_canonical_runtime_symbol(signal.symbol, self._config.gold_symbol)] = signal_key
        logger.info(
            "Corrected V4 signal alert delivered=%s instrument=%s strategy=%s setup=%s",
            delivery.delivered,
            signal.symbol,
            signal.strategy.value,
            output.candidate.setup.value,
        )

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
        if code in {"DATABENTO_RECONNECTED", "DATABENTO_SUBSCRIPTION_ACK"} or code.startswith("DATABENTO_SYSTEM_"):
            self._feed_connected = True
        elif code.startswith("DATABENTO_"):
            self._feed_connected = False
        self._write_event(
            event="feed_event",
            timestamp_et=message.timestamp_et,
            symbol="*" if message.symbol == "*" else _canonical_runtime_symbol(message.symbol, self._config.gold_symbol),
            strategy="runtime",
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

    def _status(self, now_et: datetime) -> RuntimeStatus:
        last_bar = max(self._stale_monitor.last_bar_by_symbol().values(), default=None)
        return RuntimeStatus(
            signals_active=self._signals_active,
            market_open=market_is_open(now_et),
            in_daily_halt=in_daily_halt(now_et),
            feed_connected=self._feed_connected,
            last_bar_timestamp=last_bar.isoformat() if last_bar is not None else None,
            active_ideas=0,
            strategies_enabled=[
                "STRAT_NQ_SIGNAL",
                "STRAT_YM_SIGNAL",
                "STRAT_GOLD_SIGNAL",
            ],
            output_path=str(self._events_log_path),
        )

    def _status_message(self) -> str:
        status = self._status(datetime.now(tz=ET))
        market = "open" if status.market_open else "closed"
        feed = "connected" if status.feed_connected else "disconnected"
        signals = "true" if status.signals_active else "false"
        last_bar = status.last_bar_timestamp or "none"
        strategies = ", ".join(status.strategies_enabled) or "none"
        return "\n".join(
            [
                "<b>STATUS</b>",
                f"<b>signals_active:</b> {signals}",
                f"<b>market:</b> {market}",
                f"<b>feed:</b> {feed}",
                f"<b>last_bar_timestamp:</b> {last_bar}",
                f"<b>active_ideas:</b> {status.active_ideas}",
                f"<b>strategies_enabled:</b> {strategies}",
            ]
        )

    def _runtime_message(self, *, title: str, now_et: datetime, error: Exception | None = None) -> str:
        status = self._status(now_et)
        lines = [
            title,
            f"feed_status: {'connected' if status.feed_connected else 'disconnected'}",
            f"market: {'open' if status.market_open else 'closed'}",
            f"output_dir: {self._out_dir}",
            f"state_dir: {self._state_dir}",
        ]
        if error is not None:
            lines.insert(1, f"error_type: {type(error).__name__}")
            lines.insert(2, f"error: {str(error) or type(error).__name__}")
        return "\n".join(lines)

    async def _set_signals_active(self, value: bool) -> None:
        self._signals_active = value
        self._save_state()

    def _write_signal_event(self, *, event: str, output: AcceptedSignalOutput) -> None:
        detail = f"setup={output.candidate.setup.value};contracts={output.sizing.contracts}"
        self._write_event(
            event=event,
            timestamp_et=output.signal.ts.astimezone(ET),
            symbol=output.signal.symbol,
            strategy=output.signal.strategy.value,
            reason_code=output.signal.side.value,
            detail=detail,
            score=output.signal.score,
            contracts=output.sizing.contracts,
        )

    def _write_event(
        self,
        *,
        event: str,
        timestamp_et: datetime,
        symbol: str,
        strategy: str,
        reason_code: str | None,
        detail: str | None = None,
        score: float | None = None,
        contracts: int | None = None,
    ) -> None:
        payload: dict[str, object] = {
            "event": event,
            "timestamp_et": timestamp_et.isoformat(),
            "symbol": symbol,
            "strategy": strategy,
            "reason_code": reason_code,
            "score": score,
        }
        if detail is not None:
            payload["detail"] = detail
        if contracts is not None:
            payload["contracts"] = contracts
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
                "session_start_equity": self._session_start_equity,
                "realized_pnl": self._realized_pnl,
                "open_positions": [self._serialize_open_position(position) for position in self._open_positions.values()],
                "last_heartbeat_timestamp": self._format_datetime(self._heartbeat.last_sent_at),
                "last_bar_by_symbol": {
                    symbol: ts.isoformat()
                    for symbol, ts in self._stale_monitor.last_bar_by_symbol().items()
                },
                "stale_alert_active_flags": self._stale_monitor.stale_flags(),
                "last_signal_keys": self._last_signal_keys,
            }
        )

    @staticmethod
    def _serialize_open_position(position: OpenPositionMtmSnapshot) -> dict[str, object]:
        return {
            "ts": position.ts.isoformat(),
            "symbol": position.symbol,
            "quantity": position.quantity,
            "avg_entry_price": position.avg_entry_price,
            "mark_price": position.mark_price,
            "point_value": position.point_value,
        }

    def _parse_open_positions(self, payload: Any) -> dict[str, OpenPositionMtmSnapshot]:
        if not isinstance(payload, list):
            return {}
        positions: dict[str, OpenPositionMtmSnapshot] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            raw_ts = item.get("ts")
            if not isinstance(raw_ts, str):
                continue
            try:
                ts = datetime.fromisoformat(raw_ts)
            except ValueError:
                continue
            symbol = _canonical_runtime_symbol(str(item.get("symbol", "")).strip(), self._config.gold_symbol)
            if not symbol:
                continue
            positions[symbol] = OpenPositionMtmSnapshot(
                ts=ts,
                symbol=symbol,
                quantity=int(item.get("quantity", 0)),
                avg_entry_price=float(item.get("avg_entry_price", 0.0)),
                mark_price=float(item.get("mark_price", 0.0)),
                point_value=float(item.get("point_value", 0.0)),
            )
        return positions

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if not value or not isinstance(value, str):
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _parse_last_bars(self, payload: Any) -> dict[str, datetime]:
        if not isinstance(payload, dict):
            return {}
        parsed: dict[str, datetime] = {}
        for symbol, raw_ts in payload.items():
            if not isinstance(raw_ts, str):
                continue
            try:
                timestamp = datetime.fromisoformat(raw_ts)
            except ValueError:
                continue
            canonical_symbol = _canonical_runtime_symbol(str(symbol), self._config.gold_symbol)
            previous = parsed.get(canonical_symbol)
            if previous is None or timestamp > previous:
                parsed[canonical_symbol] = timestamp
        return parsed

    def _parse_stale_flags(self, payload: Any) -> dict[str, bool]:
        if not isinstance(payload, dict):
            return {}
        parsed: dict[str, bool] = {}
        for key, value in payload.items():
            raw_key = str(key)
            stream, sep, symbol = raw_key.partition(":")
            if not sep:
                parsed[raw_key] = bool(value)
                continue
            canonical_key = f"{stream}:{_canonical_runtime_symbol(symbol, self._config.gold_symbol)}"
            parsed[canonical_key] = bool(value) or parsed.get(canonical_key, False)
        return parsed

    def _parse_last_signal_keys(self, payload: Any) -> dict[str, str]:
        if not isinstance(payload, dict):
            return {}
        parsed: dict[str, str] = {}
        for symbol, signal_key in payload.items():
            signal_key_text = str(signal_key)
            if not signal_key_text:
                continue
            canonical_symbol = _canonical_runtime_symbol(str(symbol), self._config.gold_symbol)
            parsed[canonical_symbol] = signal_key_text
        return parsed

    @staticmethod
    def _format_datetime(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None


async def run_live_signals(
    *,
    config: CorrectedV4Config,
    orchestrator: CorrectedSignalOrchestrator,
    instruments_by_symbol: dict[str, InstrumentMeta],
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
    runner = CorrectedV4LiveRunner(
        config=config,
        orchestrator=orchestrator,
        instruments_by_symbol=instruments_by_symbol,
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


def _build_live_request(
    *,
    symbol: str,
    gold_symbol: str,
    bars_1m: pd.DataFrame,
    instrument: InstrumentMeta,
    session_start_equity: float,
    realized_pnl: float,
    open_positions: tuple[OpenPositionMtmSnapshot, ...],
) -> NQEvaluationRequest | YMEvaluationRequest | GoldEvaluationRequest | None:
    if bars_1m.empty:
        return None
    latest = bars_1m.iloc[-1]
    candle_low = min(float(latest["open"]), float(latest["close"]), float(latest["low"]))
    candle_high = max(float(latest["open"]), float(latest["close"]), float(latest["high"]))
    base_kwargs = {
        "bars_1m": bars_1m,
        "instrument": instrument,
        "session_start_equity": session_start_equity,
        "realized_pnl": realized_pnl,
        "open_positions": open_positions,
        "liquidity_ok": True,
        "macro_blocked": False,
        "choch_confirmed": False,
        "fvg_present": False,
        "intermarket_confirmed": None,
    }
    if symbol == "NQ":
        return NQEvaluationRequest(
            **base_kwargs,
            pullback_price=(candle_low + candle_high) / 2.0,
            structure_break_price=candle_high,
            order_block_low=candle_low,
            order_block_high=candle_high,
        )
    if symbol == "YM":
        return YMEvaluationRequest(**base_kwargs)
    if symbol == gold_symbol.upper():
        return GoldEvaluationRequest(
            **base_kwargs,
            pullback_price=(candle_low + candle_high) / 2.0,
            structure_break_price=candle_high,
            order_block_low=candle_low,
            order_block_high=candle_high,
        )
    return None


def _canonical_runtime_symbol(symbol: str, gold_symbol: str) -> str:
    normalized = _normalize_stream_symbol(symbol)
    if normalized.startswith("NQ"):
        return "NQ"
    if normalized.startswith("YM"):
        return "YM"
    if _is_gold_symbol(normalized):
        return gold_symbol.upper()
    return normalized


def _resolve_live_instrument(
    *,
    symbol: str,
    instruments_by_symbol: dict[str, InstrumentMeta],
    gold_symbol: str,
) -> InstrumentMeta | None:
    canonical_symbol = _canonical_runtime_symbol(symbol, gold_symbol)
    if canonical_symbol == gold_symbol.upper():
        for candidate in (gold_symbol, "MGC", "GC"):
            instrument = instruments_by_symbol.get(candidate)
            if instrument is not None:
                return instrument
        return None
    return instruments_by_symbol.get(canonical_symbol)


def _normalize_stream_symbol(symbol: str) -> str:
    raw = str(symbol).strip().upper()
    if "." in raw:
        raw = raw.split(".", 1)[0]
    return raw


def _is_gold_symbol(symbol: str) -> bool:
    normalized = _normalize_stream_symbol(symbol)
    return normalized.startswith(("GC", "MGC", "GOLD"))


def _upsert_bar(bars: list[dict[str, object]], bar: dict[str, object], *, max_keep: int) -> None:
    if bars and bars[-1].get("ts") == bar.get("ts"):
        bars[-1] = bar
    else:
        bars.append(bar)
    if len(bars) > max_keep:
        del bars[:-max_keep]


def _format_signal_message(output: AcceptedSignalOutput) -> str:
    signal = output.signal
    direction = "LONG" if signal.side is OrderSide.BUY else "SHORT"
    return "\n".join(
        [
            "SIGNAL",
            f"instrument: {signal.symbol}",
            f"strategy: {signal.strategy.value}",
            f"setup: {output.candidate.setup.value}",
            f"direction: {direction}",
            f"score: {signal.score:.2f}",
            f"contracts: {output.sizing.contracts}",
            f"risk_dollars: {output.sizing.risk_dollars:.2f}",
            f"signal_ts: {signal.ts.astimezone(ET).isoformat()}",
        ]
    )
