"""Live signal runtime for Bot 2 using shared transport/runtime infrastructure."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from bot_prop_v2.config import PropV2Config
from bot_prop_v2.pipeline.signal_engine import Candle, Direction, Instrument, Signal, SignalEngine
from shared.alerts.heartbeat import HeartbeatManager
from shared.alerts.telegram import TelegramNotifier
from shared.live.databento_adapter import DatabentoLiveClient
from shared.live.feed_models import FeedMessage
from shared.runtime.schedule import in_daily_halt, market_is_open
from shared.runtime.stale_data import StaleDataEvent, StaleDataMonitor
from shared.runtime.state_store import JsonStateStore

ET = ZoneInfo("America/New_York")
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PropV2RuntimeStatus:
    signals_active: bool
    market_open: bool
    in_daily_halt: bool
    feed_connected: bool
    last_bar_timestamp: str | None
    active_ideas: int
    strategies_enabled: list[str]
    output_path: str


@dataclass(slots=True)
class SymbolHistory:
    bars_1m: list[Candle] = field(default_factory=list)
    last_signal_key: str | None = None


class PropV2LiveRunner:
    def __init__(
        self,
        *,
        config: PropV2Config,
        engine: SignalEngine,
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
        self._engine = engine
        self._out_dir = Path(out_dir)
        self._state_dir = Path(state_dir)
        self._notifier = notifier
        self._symbols = tuple(databento_symbols)
        self._monitored_symbols = {_normalize_stream_symbol(symbol) for symbol in self._symbols}
        self._state_store = JsonStateStore(self._state_dir / "prop_v2_live_state.json")
        restored = self._state_store.load()

        self._heartbeat = HeartbeatManager(
            interval_hours=config.heartbeat_interval_hours,
            last_sent_at=self._parse_datetime(restored.get("last_heartbeat_timestamp")),
        )
        self._stale_monitor = StaleDataMonitor(
            bars_timeout_s=config.bars_stale_after_s,
            last_bar_by_symbol=self._parse_last_bars(restored.get("last_bar_by_symbol", {})),
            stale_flags={str(k): bool(v) for k, v in dict(restored.get("stale_alert_active_flags", {})).items()},
        )
        self._history: dict[Instrument, SymbolHistory] = {}
        self._last_signal_keys: dict[str, str] = {
            str(k): str(v) for k, v in dict(restored.get("last_signal_keys", {})).items()
        }
        self._feed_connected = False
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
        await self._client.start()
        self._feed_connected = True
        maintenance_task = asyncio.create_task(self._maintenance_loop(), name="prop-v2-live-maintenance")
        try:
            async for message in self._client.messages():
                await self._handle_message(message)
        finally:
            self._feed_connected = False
            if maintenance_task is not None:
                maintenance_task.cancel()
                try:
                    await maintenance_task
                except asyncio.CancelledError:
                    pass
            await self._client.stop()
            self._save_state()

    async def _maintenance_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            now_et = datetime.now(tz=ET)
            self._check_stale(now_et)
            self._maybe_send_heartbeat(now_et)
            self._save_state()

    async def _handle_message(self, message: FeedMessage) -> None:
        if message.type == "event":
            logger.info("Bot 2 live event: symbol=%s payload=%s", message.symbol, message.payload)
            return
        if message.type == "quote_1s":
            for event in self._stale_monitor.mark_quote(message.symbol, message.timestamp_et):
                self._handle_stale_event(event)
            return
        if message.type != "bar_1m":
            return

        for event in self._stale_monitor.mark_bar(message.symbol, message.timestamp_et):
            self._handle_stale_event(event)

        instrument = _instrument_from_symbol(message.symbol)
        if instrument is None:
            logger.debug("Ignoring unsupported Bot 2 symbol: %s", message.symbol)
            return

        payload = message.payload
        candle = Candle(
            timestamp=message.timestamp_et.astimezone(ET),
            open=float(payload["open"]),
            high=float(payload["high"]),
            low=float(payload["low"]),
            close=float(payload["close"]),
            volume=float(payload["volume"]),
            instrument=instrument,
        )
        history = self._history.setdefault(
            instrument,
            SymbolHistory(last_signal_key=self._last_signal_keys.get(instrument.value)),
        )
        _upsert_candle(history.bars_1m, candle, max_keep=20_000)

        candles_1m = list(history.bars_1m)
        candles_5m = _aggregate_candles(candles_1m, timeframe="5m")
        candles_15m = _aggregate_candles(candles_1m, timeframe="15m")
        daily_candles = _aggregate_candles(candles_1m, timeframe="1d")
        weekly_candles = _aggregate_candles(candles_1m, timeframe="1w")

        signal = self._engine.on_candle(
            candle=candle,
            candles_1m=candles_1m,
            candles_5m=candles_5m,
            candles_15m=candles_15m,
            daily_candles=daily_candles,
            weekly_candles=weekly_candles,
        )
        if signal is None:
            self._save_state()
            return

        signal_key = f"{signal.instrument.value}:{signal.signal_type.value}:{signal.direction.name}:{signal.formed_at.isoformat()}"
        if signal_key != history.last_signal_key:
            delivery = self._notifier.send_text(text=_format_signal_message(signal))
            logger.info(
                "Bot 2 signal alert %s delivered=%s instrument=%s type=%s",
                signal.direction.name,
                delivery.delivered,
                signal.instrument.value,
                signal.signal_type.value,
            )
            history.last_signal_key = signal_key
            self._last_signal_keys[instrument.value] = signal_key

        # Signal-only mode: do not keep synthetic positions open when no broker exists.
        self._engine.open_trades.pop(instrument, None)
        self._engine.risk.open_positions.pop(instrument, None)
        self._save_state()

    def _check_stale(self, now_et: datetime) -> None:
        events = self._stale_monitor.check(
            now_et=now_et,
            market_open=market_is_open(now_et),
            symbols=set(self._monitored_symbols),
            quote_stream_enabled=self._client.quote_schema_enabled,
        )
        for event in events:
            self._handle_stale_event(event)

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
        self._notifier.send_text(text=message)
        self._save_state()

    def _maybe_send_heartbeat(self, now_et: datetime) -> None:
        self._heartbeat.maybe_send(
            now_et=now_et,
            status=self._status(now_et),
            notifier=self._notifier,
        )

    def _status(self, now_et: datetime) -> PropV2RuntimeStatus:
        last_bar = max(self._stale_monitor.last_bar_by_symbol().values(), default=None)
        return PropV2RuntimeStatus(
            signals_active=True,
            market_open=market_is_open(now_et),
            in_daily_halt=in_daily_halt(now_et),
            feed_connected=self._feed_connected,
            last_bar_timestamp=last_bar.isoformat() if last_bar is not None else None,
            active_ideas=0,
            strategies_enabled=["PROP_V2_SIGNAL_ENGINE"],
            output_path=str(self._out_dir),
        )

    def _save_state(self) -> None:
        self._state_store.save(
            {
                "signals_active": True,
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


async def run_live_signals(
    *,
    config: PropV2Config,
    engine: SignalEngine,
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
    runner = PropV2LiveRunner(
        config=config,
        engine=engine,
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


def _instrument_from_symbol(symbol: str) -> Instrument | None:
    normalized = _normalize_stream_symbol(symbol)
    if normalized.startswith("NQ"):
        return Instrument.NQ
    if normalized.startswith("YM"):
        return Instrument.YM
    if normalized.startswith(("GC", "MGC", "GOLD")):
        return Instrument.GOLD
    if normalized.startswith(("SI", "SILVER")):
        return Instrument.SILVER
    return None


def _upsert_candle(candles: list[Candle], candle: Candle, *, max_keep: int) -> None:
    if candles and candles[-1].timestamp == candle.timestamp:
        candles[-1] = candle
    else:
        candles.append(candle)
    if len(candles) > max_keep:
        del candles[:-max_keep]


def _aggregate_candles(candles: list[Candle], *, timeframe: str) -> list[Candle]:
    if not candles:
        return []
    buckets: dict[datetime, Candle] = {}
    ordered_keys: list[datetime] = []
    for candle in candles:
        bucket = _bucket_start(candle.timestamp, timeframe)
        existing = buckets.get(bucket)
        if existing is None:
            buckets[bucket] = Candle(
                timestamp=bucket,
                open=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close,
                volume=candle.volume,
                instrument=candle.instrument,
            )
            ordered_keys.append(bucket)
        else:
            existing.high = max(existing.high, candle.high)
            existing.low = min(existing.low, candle.low)
            existing.close = candle.close
            existing.volume += candle.volume
    return [buckets[key] for key in ordered_keys]


def _bucket_start(timestamp: datetime, timeframe: str) -> datetime:
    ts = timestamp.astimezone(ET)
    if timeframe == "5m":
        minute = ts.minute - (ts.minute % 5)
        return ts.replace(minute=minute, second=0, microsecond=0)
    if timeframe == "15m":
        minute = ts.minute - (ts.minute % 15)
        return ts.replace(minute=minute, second=0, microsecond=0)
    if timeframe == "1d":
        return ts.replace(hour=0, minute=0, second=0, microsecond=0)
    if timeframe == "1w":
        week_start = ts.replace(hour=0, minute=0, second=0, microsecond=0)
        return week_start - timedelta(days=week_start.weekday())
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _format_signal_message(signal: Signal) -> str:
    side = "LONG" if signal.direction == Direction.LONG else "SHORT"
    return "\n".join(
        [
            "<b>BOT 2 SIGNAL</b>",
            f"<b>Instrument:</b> {signal.instrument.value}",
            f"<b>Direction:</b> {side}",
            f"<b>Type:</b> {signal.signal_type_name}",
            f"<b>Entry:</b> {signal.entry_price:.2f}",
            f"<b>Stop:</b> {signal.stop_loss:.2f}",
            f"<b>TP1:</b> {signal.take_profit_1:.2f}",
            f"<b>TP2:</b> {signal.take_profit_2:.2f}",
            f"<b>TP3:</b> {signal.take_profit_3:.2f}",
            f"<b>Session:</b> {signal.session.value}",
            f"<b>Confluence:</b> {signal.confluence_score}",
            f"<b>Formed:</b> {signal.formed_at.isoformat()}",
            f"<b>Notes:</b> {signal.notes or 'n/a'}",
        ]
    )


def _normalize_stream_symbol(symbol: str) -> str:
    return str(symbol).split(".", 1)[0].upper()
