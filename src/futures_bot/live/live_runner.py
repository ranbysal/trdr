"""Live feed runner that drives signal generation from live bars/quotes."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
import logging
import os
from pathlib import Path
from typing import Any

from futures_bot.alerts.eod_summary import EodSummaryManager
from futures_bot.alerts.error_forwarder import ErrorForwarder
from futures_bot.alerts.heartbeat import HeartbeatManager
from futures_bot.alerts.telegram import TelegramDelivery, TelegramNotifier
from futures_bot.alerts.telegram_listener import TelegramCommandListener
from futures_bot.core.enums import Regime, StrategyModule
from futures_bot.core.types import InstrumentMeta
from futures_bot.live.databento_adapter import (
    DEFAULT_DATABENTO_DATASET,
    DEFAULT_DATABENTO_SCHEMA,
    DEFAULT_DATABENTO_STYPE_IN,
    DEFAULT_DATABENTO_SYMBOLS,
    DatabentoLiveClient,
)
from futures_bot.live.feed_models import FeedMessage
from futures_bot.live.ws_client import LiveWsClient
from futures_bot.pipeline.multistrategy_signals import MultiStrategySignalEngine
from futures_bot.runtime.health import RuntimeHealth
from futures_bot.runtime.ndjson_writer import NdjsonWriter
from futures_bot.runtime.schedule import ET, schedule_state
from futures_bot.runtime.state_store import JsonStateStore
from futures_bot.runtime.stale_data import StaleDataEvent, StaleDataMonitor

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _SymbolState:
    session_date: datetime.date | None = None
    cum_pv: float = 0.0
    cum_vol: float = 0.0
    last_quote_ts: datetime | None = None
    last_bar_ts: datetime | None = None
    current_5m_bucket: datetime | None = None
    current_5m_open: float = 0.0
    current_5m_high: float = 0.0
    current_5m_low: float = 0.0
    current_5m_close: float = 0.0
    current_5m_vol: float = 0.0
    prev_5m_close: float | None = None
    ema9_5m: float | None = None
    ema21_5m: float | None = None
    ema20_5m: float | None = None
    ema20_5m_prev: float | None = None
    tr_5m: deque[float] = field(default_factory=lambda: deque(maxlen=14))
    vol_5m: deque[float] = field(default_factory=lambda: deque(maxlen=20))


class LiveSignalRunner:
    def __init__(
        self,
        *,
        out_dir: str | Path,
        state_dir: str | Path | None = None,
        instruments_by_symbol: dict[str, InstrumentMeta],
        enabled_strategies: set[StrategyModule],
        feed_client: Any | None = None,
        notifier: TelegramNotifier | None = None,
        queue_maxsize: int = 2000,
        databento_api_key: str | None = None,
        databento_dataset: str = DEFAULT_DATABENTO_DATASET,
        databento_schema: str = DEFAULT_DATABENTO_SCHEMA,
        databento_stype_in: str = DEFAULT_DATABENTO_STYPE_IN,
        databento_symbols: tuple[str, ...] = DEFAULT_DATABENTO_SYMBOLS,
        ws_url: str | None = None,
    ) -> None:
        out_path = Path(out_dir)
        state_path = Path(state_dir) if state_dir is not None else out_path
        out_path.mkdir(parents=True, exist_ok=True)
        state_path.mkdir(parents=True, exist_ok=True)
        self._out_path = out_path
        self._notifier = notifier or TelegramNotifier()
        self._events_log = NdjsonWriter(out_path / "live_events.ndjson")
        self._state_store = JsonStateStore(state_path / "runtime_state.json")
        restored_state = self._state_store.load()
        restored_last_bars = self._load_last_bar_state(restored_state)
        self._health = RuntimeHealth(
            out_dir=out_path,
            enabled_strategies={item.value for item in enabled_strategies},
            signals_active=bool(restored_state.get("signals_active", True)),
        )
        self._health.load_last_bars(restored_last_bars)
        self._heartbeat = HeartbeatManager(
            interval_hours=float(os.getenv("FUTURES_BOT_HEARTBEAT_HOURS", "4")),
            last_sent_at=self._parse_datetime(restored_state.get("last_heartbeat_timestamp")),
        )
        self._eod = EodSummaryManager(
            last_sent_date=self._parse_date(restored_state.get("last_eod_summary_date_sent")),
        )
        self._eod.restore_counters(dict(restored_state.get("daily_summary", {})))
        self._stale_monitor = StaleDataMonitor(
            bars_timeout_s=float(os.getenv("FUTURES_BOT_BARS_STALE_TIMEOUT_S", "180")),
            quote_timeout_s=float(os.getenv("FUTURES_BOT_QUOTE_STALE_TIMEOUT_S", "30")),
            last_bar_by_symbol=restored_last_bars,
            stale_flags={str(k): bool(v) for k, v in dict(restored_state.get("stale_alert_active_flags", {})).items()},
        )
        self._error_forwarder = ErrorForwarder(self._notifier)
        self._last_schedule_state: str | None = None
        self._disconnect_events: deque[datetime] = deque(maxlen=10)
        self._stop_event = asyncio.Event()
        self._supervisor_task: asyncio.Task[None] | None = None
        self._listener_task: asyncio.Task[None] | None = None
        self._engine = MultiStrategySignalEngine(
            out_dir=out_path,
            state_dir=state_path,
            instruments_by_symbol=instruments_by_symbol,
            enabled_strategies=enabled_strategies,
            notifier=self._notifier,
            alert_emit_callback=self._on_alert_emitted,
            signal_register_callback=self._on_signal_registered,
        )
        self._engine.restore(list(restored_state.get("active_ideas", [])))
        self._states: dict[str, _SymbolState] = {}
        self._global_freeze = False
        self._lockout = False
        self._client = feed_client or self._build_client(
            databento_api_key=databento_api_key,
            databento_dataset=databento_dataset,
            databento_schema=databento_schema,
            databento_stype_in=databento_stype_in,
            databento_symbols=databento_symbols,
            instruments_by_symbol=instruments_by_symbol,
            queue_maxsize=queue_maxsize,
            ws_url=ws_url,
        )
        self._listener = TelegramCommandListener(
            notifier=self._notifier,
            status_provider=self._status_message,
            set_signals_active=self._set_signals_active,
            poll_interval_s=float(os.getenv("FUTURES_BOT_TELEGRAM_POLL_INTERVAL_S", "2")),
        )

    async def run(self, *, max_messages: int | None = None, max_runtime_s: float | None = None) -> None:
        try:
            await self._client.start()
            self._health.set_feed_connected(True)
            self._persist_state()
            self._supervisor_task = asyncio.create_task(self._supervise(), name="live-signal-supervisor")
            self._listener_task = asyncio.create_task(self._listener.run(self._stop_event), name="telegram-command-listener")
            processed = 0
            started = asyncio.get_event_loop().time()
            async for message in self._client.messages():
                await self._handle_message(message)
                processed += 1
                if max_messages is not None and processed >= max_messages:
                    break
                if max_runtime_s is not None and (asyncio.get_event_loop().time() - started) >= max_runtime_s:
                    break
        except Exception as exc:
            logger.exception("Live signal runner failed")
            self._write_telegram_event(
                self._error_forwarder.send(
                    error_type=type(exc).__name__,
                    message=str(exc),
                    timestamp_et=datetime.now(tz=ET),
                    component=__name__,
                    dedupe_key=f"fatal:{type(exc).__name__}:{str(exc)}",
                ),
                timestamp_et=datetime.now(tz=ET),
                symbol="*",
                strategy="runtime",
                reason_code="FATAL_ERROR",
            )
            raise
        finally:
            self._stop_event.set()
            if self._listener_task is not None:
                await asyncio.gather(self._listener_task, return_exceptions=True)
            if self._supervisor_task is not None:
                await asyncio.gather(self._supervisor_task, return_exceptions=True)
            await self._client.stop()
            self._health.set_feed_connected(False)
            self._persist_state()
            self._engine.flush()
            self._events_log.flush()

    async def _handle_message(self, message: FeedMessage) -> None:
        if message.type == "quote_1s":
            self._handle_quote(message)
            return
        if message.type == "event":
            self._handle_event(message)
            return
        self._handle_bar(message)

    def _handle_quote(self, message: FeedMessage) -> None:
        state = self._state_for(message.symbol)
        state.last_quote_ts = message.timestamp_et
        self._health.set_feed_connected(True)
        for event in self._stale_monitor.mark_quote(message.symbol, message.timestamp_et):
            self._handle_stale_event(event, now_et=message.timestamp_et)

    def _handle_event(self, message: FeedMessage) -> None:
        code = str(message.payload.get("code", "EVENT"))
        if code == "LOCKOUT_ON":
            self._lockout = True
        elif code == "LOCKOUT_OFF":
            self._lockout = False
        elif code == "FREEZE_ON":
            self._global_freeze = True
        elif code == "FREEZE_OFF":
            self._global_freeze = False
        if code == "DATABENTO_RECONNECTED":
            self._health.set_feed_connected(True)
            self._error_forwarder.clear("disconnect-loop")
        elif code.startswith("DATABENTO_"):
            self._health.set_feed_connected(False)
            self._record_feed_issue(message.timestamp_et)
            if code in {"DATABENTO_CONNECTION_RESET", "DATABENTO_CALLBACK_ERROR"}:
                self._disconnect_events.append(message.timestamp_et)
                self._check_disconnect_loop(now_et=message.timestamp_et)
            if code in {"DATABENTO_AUTH_FAILURE", "DATABENTO_ENTITLEMENT_FAILURE", "DATABENTO_SYMBOL_RESOLUTION_FAILURE"}:
                self._write_telegram_event(
                    self._error_forwarder.send(
                        error_type=code,
                        message=str(message.payload.get("detail", code)),
                        timestamp_et=message.timestamp_et,
                        component="databento_adapter",
                        dedupe_key=f"feed-startup:{code}",
                    ),
                    timestamp_et=message.timestamp_et,
                    symbol=message.symbol,
                    strategy="runtime",
                    reason_code=code,
                )
        self._events_log.write(
            {
                "timestamp_et": message.timestamp_et.isoformat(),
                "event": "feed_event",
                "reason_code": code,
                "symbol": message.symbol,
                "strategy": "runtime",
                "score": None,
            }
        )
        self._persist_state()

    def _handle_bar(self, message: FeedMessage) -> None:
        symbol = message.symbol
        payload = message.payload
        state = self._state_for(symbol)

        bar_ts = message.timestamp_et
        bar_open = float(payload["open"])
        bar_high = float(payload["high"])
        bar_low = float(payload["low"])
        bar_close = float(payload["close"])
        bar_volume = float(payload["volume"])

        session_date = bar_ts.date()
        if state.session_date != session_date:
            self._reset_session(state, session_date)

        data_ok = True
        if state.last_bar_ts is not None:
            gap_s = (bar_ts - state.last_bar_ts).total_seconds()
            if gap_s > 90.0:
                data_ok = False
                self._global_freeze = True
                self._events_log.write(
                    {
                        "timestamp_et": bar_ts.isoformat(),
                        "event": "risk_event",
                        "reason_code": "DATA_GAP_DETECTED",
                        "symbol": symbol,
                        "strategy": "runtime",
                        "score": None,
                        "gap_seconds": gap_s,
                    }
                )
                self._record_feed_issue(bar_ts)
        state.last_bar_ts = bar_ts
        self._health.mark_bar(symbol, bar_ts)
        self._health.set_feed_connected(True)
        for event in self._stale_monitor.mark_bar(symbol, bar_ts):
            self._handle_stale_event(event, now_et=bar_ts)
        self._events_log.write(
            {
                "event": "BAR_RECEIVED",
                "timestamp_et": bar_ts.isoformat(),
                "symbol": symbol,
                "strategy": "runtime",
                "reason_code": None,
                "score": None,
            }
        )

        quote_required = bool(getattr(self._client, "quote_schema_enabled", True))
        quote_ok = (not quote_required) or (
            state.last_quote_ts is not None and (bar_ts - state.last_quote_ts).total_seconds() <= 15.0
        )

        state.cum_pv += bar_close * bar_volume
        state.cum_vol += bar_volume
        session_vwap = state.cum_pv / state.cum_vol if state.cum_vol > 0.0 else bar_close

        atr_14_1m = abs(bar_high - bar_low)
        atr_14_5m, ema9_5m, ema21_5m, ema20_slope, rvol_3 = self._update_5m(state, bar_ts, bar_open, bar_high, bar_low, bar_close, bar_volume)

        raw_regime = Regime.NEUTRAL
        if ema9_5m > ema21_5m:
            raw_regime = Regime.TREND
        elif ema9_5m < ema21_5m:
            raw_regime = Regime.CHOP
        is_weak_neutral = raw_regime is Regime.NEUTRAL and abs(bar_close - session_vwap) <= (0.3 * atr_14_5m)
        state_name = schedule_state(bar_ts)

        row = {
            "ts": bar_ts,
            "symbol": symbol,
            "open": bar_open,
            "high": bar_high,
            "low": bar_low,
            "close": bar_close,
            "volume": bar_volume,
            "session_vwap": session_vwap,
            "ema9_5m": ema9_5m,
            "ema21_5m": ema21_5m,
            "ema20_5m_slope": ema20_slope,
            "atr_14_5m": atr_14_5m,
            "atr_14_1m_price": atr_14_1m,
            "rvol_3bar_aggregate_5m": rvol_3,
            "vol_strong_1m": bool(bar_volume >= 1.0),
            "data_ok": data_ok,
            "quote_ok": quote_ok,
            "trade_eligible": True,
            "lockout": self._lockout,
            "family_freeze": self._global_freeze,
            "raw_regime": raw_regime,
            "is_weak_neutral": is_weak_neutral,
            "confidence": 0.5 if is_weak_neutral else 1.0,
            "market_open": state_name == "open",
            "signals_active": self._health.signals_active,
            "schedule_state": state_name,
        }
        self._engine.process_row(row)
        self._persist_state()

    def _state_for(self, symbol: str) -> _SymbolState:
        state = self._states.get(symbol)
        if state is None:
            state = _SymbolState()
            self._states[symbol] = state
        return state

    def _reset_session(self, state: _SymbolState, session_date: datetime.date) -> None:
        state.session_date = session_date
        state.cum_pv = 0.0
        state.cum_vol = 0.0

    def _update_5m(
        self,
        state: _SymbolState,
        ts: datetime,
        o: float,
        h: float,
        l: float,
        c: float,
        v: float,
    ) -> tuple[float, float, float, float, float | None]:
        bucket = ts.replace(minute=(ts.minute // 5) * 5, second=0, microsecond=0)
        if state.current_5m_bucket is None:
            state.current_5m_bucket = bucket
            state.current_5m_open = o
            state.current_5m_high = h
            state.current_5m_low = l
            state.current_5m_close = c
            state.current_5m_vol = v
        elif bucket != state.current_5m_bucket:
            self._finalize_5m(state)
            state.current_5m_bucket = bucket
            state.current_5m_open = o
            state.current_5m_high = h
            state.current_5m_low = l
            state.current_5m_close = c
            state.current_5m_vol = v
        else:
            state.current_5m_high = max(state.current_5m_high, h)
            state.current_5m_low = min(state.current_5m_low, l)
            state.current_5m_close = c
            state.current_5m_vol += v

        atr_14_5m = sum(state.tr_5m) / len(state.tr_5m) if state.tr_5m else max(abs(h - l), 1.0)
        ema9 = state.ema9_5m if state.ema9_5m is not None else c
        ema21 = state.ema21_5m if state.ema21_5m is not None else c
        ema20 = state.ema20_5m if state.ema20_5m is not None else c
        slope = 0.0 if state.ema20_5m_prev is None else ema20 - state.ema20_5m_prev
        rvol = None
        if state.vol_5m:
            avg_vol = sum(state.vol_5m) / len(state.vol_5m)
            if avg_vol > 0.0:
                rvol = state.current_5m_vol / avg_vol
        return atr_14_5m, ema9, ema21, slope, rvol

    def _finalize_5m(self, state: _SymbolState) -> None:
        close_5m = state.current_5m_close
        high_5m = state.current_5m_high
        low_5m = state.current_5m_low
        prev_close = state.prev_5m_close if state.prev_5m_close is not None else close_5m
        tr = max(abs(high_5m - low_5m), abs(high_5m - prev_close), abs(low_5m - prev_close))
        state.tr_5m.append(tr)
        state.vol_5m.append(state.current_5m_vol)

        if state.ema9_5m is None:
            state.ema9_5m = close_5m
        else:
            alpha9 = 2.0 / (9.0 + 1.0)
            state.ema9_5m = (alpha9 * close_5m) + ((1.0 - alpha9) * state.ema9_5m)

        if state.ema21_5m is None:
            state.ema21_5m = close_5m
        else:
            alpha21 = 2.0 / (21.0 + 1.0)
            state.ema21_5m = (alpha21 * close_5m) + ((1.0 - alpha21) * state.ema21_5m)

        state.ema20_5m_prev = state.ema20_5m
        if state.ema20_5m is None:
            state.ema20_5m = close_5m
        else:
            alpha20 = 2.0 / (20.0 + 1.0)
            state.ema20_5m = (alpha20 * close_5m) + ((1.0 - alpha20) * state.ema20_5m)

        state.prev_5m_close = close_5m

    def _on_overload(self, code: str) -> None:
        self._global_freeze = True
        self._events_log.write(
            {
                "timestamp_et": datetime.now(tz=ET).isoformat(),
                "event": "risk_event",
                "reason_code": code,
                "symbol": "*",
                "strategy": "runtime",
                "score": None,
            }
        )
        self._record_feed_issue(datetime.now(tz=ET))

    def _build_client(
        self,
        *,
        databento_api_key: str | None,
        databento_dataset: str,
        databento_schema: str,
        databento_stype_in: str,
        databento_symbols: tuple[str, ...],
        instruments_by_symbol: dict[str, InstrumentMeta],
        queue_maxsize: int,
        ws_url: str | None,
    ) -> Any:
        if databento_api_key:
            logger.info(
                "Configuring Databento live client dataset=%s schema=%s stype_in=%s symbols=%s",
                databento_dataset,
                databento_schema,
                databento_stype_in,
                list(databento_symbols),
            )
            return DatabentoLiveClient(
                api_key=databento_api_key,
                dataset=databento_dataset,
                schema=databento_schema,
                stype_in=databento_stype_in,
                symbols=databento_symbols,
                queue_maxsize=queue_maxsize,
                on_overload=self._on_overload,
            )
        if ws_url:
            return LiveWsClient(
                ws_url=ws_url,
                queue_maxsize=queue_maxsize,
                on_overload=self._on_overload,
            )
        raise ValueError("live feed client configuration is required")

    async def _supervise(self) -> None:
        while not self._stop_event.is_set():
            now_et = datetime.now(tz=ET)
            self._log_schedule_transition(now_et)
            await self._check_stale(now_et)
            self._maybe_send_heartbeat(now_et)
            self._maybe_send_eod(now_et)
            self._persist_state()
            await asyncio.sleep(5.0)

    async def _check_stale(self, now_et: datetime) -> None:
        symbols = set(self._health.last_bar_by_symbol())
        events = self._stale_monitor.check(
            now_et=now_et,
            market_open=self._health.snapshot(now_et=now_et, active_ideas=self._engine.active_count()).market_open,
            symbols=symbols,
            quote_stream_enabled=bool(getattr(self._client, "quote_schema_enabled", False)),
        )
        for event in events:
            self._handle_stale_event(event, now_et=now_et)

    def _handle_stale_event(self, event: StaleDataEvent, *, now_et: datetime) -> None:
        last_seen = event.last_timestamp.isoformat() if event.last_timestamp is not None else "none"
        if event.kind == "recovered":
            message = (
                f"<b>RECOVERED</b>\n<b>Symbol:</b> {event.symbol}\n<b>Stream:</b> {event.stream}\n"
                f"<b>Timestamp:</b> {now_et.isoformat()}\n<b>Last Seen:</b> {last_seen}"
            )
            delivery = self._notifier.send_text(text=message)
            self._write_telegram_event(delivery, timestamp_et=now_et, symbol=event.symbol, strategy="runtime", reason_code="RECOVERED")
            self._error_forwarder.clear(f"persistent-stale:{event.stream}:{event.symbol}")
        elif event.kind == "stale":
            logger.warning("Stale %s data detected for %s lag=%.1fs last=%s", event.stream, event.symbol, event.lag_seconds, last_seen)
            self._events_log.write(
                {
                    "event": "REJECTION_REASON",
                    "timestamp_et": now_et.isoformat(),
                    "symbol": event.symbol,
                    "strategy": "runtime",
                    "reason_code": f"STALE_{event.stream.upper()}_DATA",
                    "score": None,
                }
            )
            delivery = self._notifier.send_text(
                text=(
                    f"<b>STALE DATA</b>\n<b>Symbol:</b> {event.symbol}\n<b>Stream:</b> {event.stream}\n"
                    f"<b>Lag Seconds:</b> {event.lag_seconds:.1f}\n<b>Last Seen:</b> {last_seen}"
                )
            )
            self._write_telegram_event(
                delivery,
                timestamp_et=now_et,
                symbol=event.symbol,
                strategy="runtime",
                reason_code=f"STALE_{event.stream.upper()}_DATA",
            )
            self._record_feed_issue(now_et)
        elif event.kind == "persistent":
            logger.warning("Persistent stale %s data for %s lag=%.1fs", event.stream, event.symbol, event.lag_seconds)
            self._write_telegram_event(
                self._error_forwarder.send(
                    error_type="PERSISTENT_STALE_DATA",
                    message=f"{event.symbol} {event.stream} stale for {event.lag_seconds:.1f}s",
                    timestamp_et=now_et,
                    component="stale_data_monitor",
                    dedupe_key=f"persistent-stale:{event.stream}:{event.symbol}",
                ),
                timestamp_et=now_et,
                symbol=event.symbol,
                strategy="runtime",
                reason_code="PERSISTENT_STALE_DATA",
            )

    def _maybe_send_heartbeat(self, now_et: datetime) -> None:
        status = self._health.snapshot(now_et=now_et, active_ideas=self._engine.active_count())
        delivery = self._heartbeat.maybe_send(now_et=now_et, status=status, notifier=self._notifier)
        if delivery is not None:
            self._write_telegram_event(delivery, timestamp_et=now_et, symbol="*", strategy="runtime", reason_code="HEARTBEAT")

    def _maybe_send_eod(self, now_et: datetime) -> None:
        delivery = self._eod.maybe_send(now_et=now_et, notifier=self._notifier)
        if delivery is not None:
            self._write_telegram_event(delivery, timestamp_et=now_et, symbol="*", strategy="runtime", reason_code="EOD_SUMMARY")

    def _status_message(self) -> str:
        status = self._health.snapshot(now_et=datetime.now(tz=ET), active_ideas=self._engine.active_count())
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

    async def _set_signals_active(self, value: bool) -> None:
        self._health.set_signals_active(value)
        self._persist_state()

    def _log_schedule_transition(self, now_et: datetime) -> None:
        current = schedule_state(now_et)
        if current == self._last_schedule_state:
            return
        self._last_schedule_state = current
        self._events_log.write(
            {
                "event": "SCHEDULE_STATE_TRANSITION",
                "timestamp_et": now_et.isoformat(),
                "symbol": "*",
                "strategy": "runtime",
                "reason_code": current.upper(),
                "score": None,
            }
        )

    def _on_signal_registered(self, idea) -> None:
        self._eod.record_signal(ts_et=idea.timestamp.astimezone(ET), strategy=idea.strategy.value, symbol=idea.symbol_display)
        self._persist_state()

    def _on_alert_emitted(self, idea, _kind, state, delivery: TelegramDelivery) -> None:
        if state.value in {"THESIS_INVALIDATED", "CLOSE_SIGNAL"}:
            self._eod.record_closed_signal(ts_et=datetime.now(tz=ET))
        self._write_telegram_event(
            delivery,
            timestamp_et=datetime.now(tz=ET),
            symbol=idea.symbol_display,
            strategy=idea.strategy.value,
            reason_code=state.value,
        )
        self._persist_state()

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

    def _check_disconnect_loop(self, *, now_et: datetime) -> None:
        cutoff = now_et.timestamp() - 300.0
        recent = [item for item in self._disconnect_events if item.timestamp() >= cutoff]
        self._disconnect_events = deque(recent, maxlen=10)
        if len(recent) < 3:
            return
        self._write_telegram_event(
            self._error_forwarder.send(
                error_type="REPEATED_DATABENTO_DISCONNECT",
                message=f"{len(recent)} disconnects within 5 minutes",
                timestamp_et=now_et,
                component="databento_adapter",
                dedupe_key="disconnect-loop",
            ),
            timestamp_et=now_et,
            symbol="*",
            strategy="runtime",
            reason_code="REPEATED_DATABENTO_DISCONNECT",
        )

    def _record_feed_issue(self, ts_et: datetime) -> None:
        self._eod.record_feed_issue(ts_et=ts_et.astimezone(ET))

    def _persist_state(self) -> None:
        self._state_store.save(
            {
                "signals_active": self._health.signals_active,
                "active_ideas": self._engine.snapshot_records(),
                "last_heartbeat_timestamp": self._format_datetime(self._heartbeat.last_sent_at),
                "last_eod_summary_date_sent": self._eod.last_sent_date.isoformat() if self._eod.last_sent_date else None,
                "last_seen_bar_timestamp_by_symbol": {
                    key: value.isoformat() for key, value in self._health.last_bar_by_symbol().items()
                },
                "stale_alert_active_flags": self._stale_monitor.stale_flags(),
                "daily_summary": self._eod.snapshot(),
            }
        )

    def _load_last_bar_state(self, payload: dict[str, Any]) -> dict[str, datetime]:
        last_bars: dict[str, datetime] = {}
        for symbol, raw_ts in dict(payload.get("last_seen_bar_timestamp_by_symbol", {})).items():
            parsed = self._parse_datetime(raw_ts)
            if parsed is not None:
                last_bars[str(symbol)] = parsed
        return last_bars

    def _parse_datetime(self, value: Any) -> datetime | None:
        if not value:
            return None
        return datetime.fromisoformat(str(value))

    def _parse_date(self, value: Any):
        if not value:
            return None
        return datetime.fromisoformat(f"{value}T00:00:00").date()

    def _format_datetime(self, value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None


async def run_live_signals(
    *,
    out_dir: str | Path,
    state_dir: str | Path | None = None,
    instruments_by_symbol: dict[str, InstrumentMeta],
    enabled_strategies: set[StrategyModule],
    notifier: TelegramNotifier | None = None,
    max_messages: int | None = None,
    max_runtime_s: float | None = None,
    databento_api_key: str | None = None,
    databento_dataset: str = DEFAULT_DATABENTO_DATASET,
    databento_schema: str = DEFAULT_DATABENTO_SCHEMA,
    databento_stype_in: str = DEFAULT_DATABENTO_STYPE_IN,
    databento_symbols: tuple[str, ...] = DEFAULT_DATABENTO_SYMBOLS,
    feed_client: Any | None = None,
    ws_url: str | None = None,
) -> None:
    runner = LiveSignalRunner(
        out_dir=out_dir,
        state_dir=state_dir,
        instruments_by_symbol=instruments_by_symbol,
        enabled_strategies=enabled_strategies,
        feed_client=feed_client,
        notifier=notifier,
        databento_api_key=databento_api_key,
        databento_dataset=databento_dataset,
        databento_schema=databento_schema,
        databento_stype_in=databento_stype_in,
        databento_symbols=databento_symbols,
        ws_url=ws_url,
    )
    await runner.run(max_messages=max_messages, max_runtime_s=max_runtime_s)
