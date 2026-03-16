"""Databento live feed adapter for the signal runner."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from futures_bot.live.backpressure import BackpressureQueue
from futures_bot.live.feed_models import FeedMessage

if TYPE_CHECKING:
    import databento as db
    import databento_dbn as dbn

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
_PRICE_SCALE = 1_000_000_000.0
DEFAULT_DATABENTO_DATASET = "GLBX.MDP3"
DEFAULT_DATABENTO_SCHEMA = "ohlcv-1m"
DEFAULT_DATABENTO_STYPE_IN = "continuous"
DEFAULT_DATABENTO_SYMBOLS = ("YM.v.0", "NQ.v.0")


class DatabentoLiveClient:
    def __init__(
        self,
        *,
        api_key: str,
        dataset: str = DEFAULT_DATABENTO_DATASET,
        schema: str = DEFAULT_DATABENTO_SCHEMA,
        stype_in: str = DEFAULT_DATABENTO_STYPE_IN,
        symbols: Sequence[str] = DEFAULT_DATABENTO_SYMBOLS,
        queue_maxsize: int = 2000,
        on_overload: Callable[[str], None] | None = None,
        client_factory: Callable[..., Any] | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        if not dataset:
            raise ValueError("dataset is required")
        if not schema:
            raise ValueError("schema is required")
        if not stype_in:
            raise ValueError("stype_in is required")
        if not symbols:
            raise ValueError("at least one symbol is required")
        self._api_key = api_key
        self._dataset = dataset
        self._schema = schema
        self._stype_in = stype_in
        self._symbols = tuple(symbols)
        self._queue = BackpressureQueue(maxsize=queue_maxsize)
        self._on_overload = on_overload
        self._client_factory = client_factory
        self._client: Any | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._stop_requested = False
        self._quote_schema_enabled = False
        self._symbol_map: dict[int, str] = {}

    async def start(self) -> None:
        if self._close_task is not None and not self._close_task.done():
            return
        self._loop = asyncio.get_running_loop()
        self._stop_requested = False

        logger.info(
            "Starting Databento live feed dataset=%s schema=%s stype_in=%s symbols=%s",
            self._dataset,
            self._schema,
            self._stype_in,
            list(self._symbols),
        )
        client = self._build_client()
        client.add_callback(self._handle_record, self._handle_callback_error)
        client.add_reconnect_callback(self._handle_reconnect, self._handle_callback_error)
        try:
            self._subscribe(client)
            self._client = client
            client.start()
        except Exception as exc:
            raise RuntimeError(_format_startup_error(exc)) from exc
        self._close_task = asyncio.create_task(self._wait_for_close(), name="databento-live-close")

    async def stop(self) -> None:
        self._stop_requested = True
        if self._client is not None:
            try:
                self._client.stop()
            except ValueError:
                pass
        if self._close_task is not None:
            try:
                await self._close_task
            except asyncio.CancelledError:
                pass
        await self._queue.close()

    async def messages(self) -> AsyncIterator[FeedMessage]:
        while True:
            try:
                msg = await self._queue.get()
            except RuntimeError:
                break
            yield msg

    @property
    def quote_schema_enabled(self) -> bool:
        return self._quote_schema_enabled

    def _build_client(self) -> Any:
        if self._client_factory is not None:
            return self._client_factory(
                key=self._api_key,
                reconnect_policy="reconnect",
            )
        try:
            import databento as db
        except ImportError as exc:  # pragma: no cover - depends on runtime env
            raise RuntimeError("databento dependency is required for live mode") from exc
        return db.Live(
            key=self._api_key,
            reconnect_policy=db.ReconnectPolicy.RECONNECT,
        )

    def _subscribe(self, client: Any) -> None:
        stype_in = self._resolve_stype_in()
        client.subscribe(
            dataset=self._dataset,
            schema=self._schema,
            symbols=list(self._symbols),
            stype_in=stype_in,
        )
        self._quote_schema_enabled = False

    def _resolve_stype_in(self) -> Any:
        if self._client_factory is not None:
            return self._stype_in
        import databento as db

        try:
            return getattr(db.SType, self._stype_in.upper())
        except AttributeError:
            return self._stype_in

    async def _wait_for_close(self) -> None:
        if self._client is None:
            await self._queue.close()
            return
        try:
            await self._client.wait_for_close()
        except Exception as exc:
            if not self._stop_requested:
                code = _classify_databento_issue(str(exc), default="DATABENTO_CONNECTION_RESET")
                logger.warning("Databento live session closed code=%s detail=%s", code, exc)
                self._schedule_message(
                    FeedMessage(
                        type="event",
                        timestamp_et=datetime.now(tz=ET),
                        symbol="*",
                        payload={"code": code, "detail": str(exc)},
                    )
                )
        finally:
            await self._queue.close()

    def _handle_record(self, record: Any) -> None:
        message = self._normalize_record(record)
        if message is not None:
            self._schedule_message(message)

    def _handle_reconnect(self, last_ts: Any, new_start: Any) -> None:
        logger.info("Databento reconnect last_ts=%s new_start=%s", last_ts, new_start)
        self._schedule_message(
            FeedMessage(
                type="event",
                timestamp_et=datetime.now(tz=ET),
                symbol="*",
                payload={
                    "code": "DATABENTO_RECONNECTED",
                    "last_ts": str(last_ts),
                    "new_start": str(new_start),
                },
            )
        )

    def _handle_callback_error(self, exc: Exception) -> None:
        code = _classify_databento_issue(str(exc), default="DATABENTO_CALLBACK_ERROR")
        logger.warning("Databento callback error code=%s detail=%s", code, exc)
        self._schedule_message(
            FeedMessage(
                type="event",
                timestamp_et=datetime.now(tz=ET),
                symbol="*",
                payload={"code": code, "detail": str(exc)},
            )
        )

    def _normalize_record(self, record: Any) -> FeedMessage | None:
        import databento_dbn as dbn

        if isinstance(record, dbn.SymbolMappingMsg):
            self._symbol_map[int(record.instrument_id)] = _normalize_strategy_symbol(str(record.stype_in_symbol))
            return None
        if isinstance(record, dbn.OHLCVMsg):
            symbol = self._resolve_symbol(record.instrument_id)
            if symbol is None:
                return None
            return FeedMessage(
                type="bar_1m",
                timestamp_et=_ts_to_et(record.ts_event),
                symbol=symbol,
                payload={
                    "open": _price_to_float(record.open),
                    "high": _price_to_float(record.high),
                    "low": _price_to_float(record.low),
                    "close": _price_to_float(record.close),
                    "volume": float(record.volume),
                },
            )
        if isinstance(record, (dbn.BBOMsg, dbn.MBP1Msg)):
            symbol = self._resolve_symbol(record.instrument_id)
            if symbol is None:
                return None
            quote = _normalize_quote_payload(record)
            if quote is None:
                return None
            return FeedMessage(
                type="quote_1s",
                timestamp_et=_ts_to_et(record.ts_event),
                symbol=symbol,
                payload=quote,
            )
        if isinstance(record, dbn.SystemMsg):
            detail = str(record.msg)
            return FeedMessage(
                type="event",
                timestamp_et=_ts_to_et(record.ts_event),
                symbol="*",
                payload={
                    "code": _classify_databento_issue(detail, default=f"DATABENTO_SYSTEM_{record.code}"),
                    "detail": detail,
                },
            )
        if isinstance(record, dbn.ErrorMsg):
            detail = str(record.err)
            return FeedMessage(
                type="event",
                timestamp_et=_ts_to_et(record.ts_event),
                symbol="*",
                payload={
                    "code": _classify_databento_issue(detail, default=f"DATABENTO_ERROR_{record.code}"),
                    "detail": detail,
                },
            )
        return None

    def _resolve_symbol(self, instrument_id: int) -> str | None:
        symbol = self._symbol_map.get(int(instrument_id))
        if symbol is not None:
            return symbol
        if self._client is not None:
            mapped = self._client.symbology_map.get(int(instrument_id))
            if mapped is not None:
                symbol = _normalize_strategy_symbol(str(mapped))
                self._symbol_map[int(instrument_id)] = symbol
                return symbol
        logger.debug("Dropping Databento record for unknown instrument_id=%s", instrument_id)
        return None

    def _schedule_message(self, message: FeedMessage) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        self._loop.call_soon_threadsafe(self._start_enqueue_task, message)

    def _start_enqueue_task(self, message: FeedMessage) -> None:
        task = asyncio.create_task(self._enqueue(message))
        task.add_done_callback(self._log_enqueue_error)

    async def _enqueue(self, message: FeedMessage) -> None:
        result = await self._queue.put(message)
        if result.freeze_trading and self._on_overload is not None:
            self._on_overload("FEED_QUEUE_OVERLOAD")

    def _log_enqueue_error(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.warning("Databento enqueue error: %s", exc)


def _normalize_quote_payload(record: Any) -> dict[str, float] | None:
    levels = getattr(record, "levels", None)
    if not levels:
        return None
    level = levels[0]
    bid = _price_to_float(level.bid_px)
    ask = _price_to_float(level.ask_px)
    if bid is None or ask is None:
        return None
    return {
        "bid": bid,
        "ask": ask,
        "bid_size": float(level.bid_sz),
        "ask_size": float(level.ask_sz),
    }


def _price_to_float(value: int) -> float | None:
    import databento_dbn as dbn

    if int(value) == int(dbn.UNDEF_PRICE):
        return None
    return float(value) / _PRICE_SCALE


def _ts_to_et(ts_event_ns: int) -> datetime:
    return datetime.fromtimestamp(int(ts_event_ns) / 1_000_000_000, tz=timezone.utc).astimezone(ET)


def _normalize_strategy_symbol(symbol: str) -> str:
    head, dot, _tail = symbol.partition(".")
    if dot and head:
        return head
    return symbol


def _format_startup_error(exc: Exception) -> str:
    code = _classify_databento_issue(str(exc), default="DATABENTO_STARTUP_FAILURE")
    if code == "DATABENTO_AUTH_FAILURE":
        return f"Databento authentication failed: {exc}"
    if code == "DATABENTO_SYMBOL_RESOLUTION_FAILURE":
        return f"Databento symbol resolution failed: {exc}"
    if code == "DATABENTO_ENTITLEMENT_FAILURE":
        return f"Databento entitlement or subscription failed: {exc}"
    if code == "DATABENTO_CONNECTION_RESET":
        return f"Databento connection reset during startup: {exc}"
    return f"Databento live startup failed: {exc}"


def _classify_databento_issue(detail: str, *, default: str) -> str:
    text = detail.lower()
    if any(token in text for token in ("auth", "unauthorized", "forbidden", "api key", "credential", "invalid key")):
        return "DATABENTO_AUTH_FAILURE"
    if any(
        token in text
        for token in (
            "symbol resolution",
            "resolve symbol",
            "unknown symbol",
            "symbol not found",
            "invalid symbol",
            "symbology",
            "stype_in",
        )
    ):
        return "DATABENTO_SYMBOL_RESOLUTION_FAILURE"
    if any(token in text for token in ("entitlement", "not entitled", "subscription", "schema", "dataset permission")):
        return "DATABENTO_ENTITLEMENT_FAILURE"
    if any(
        token in text
        for token in ("connection reset", "connection closed", "broken pipe", "econnreset", "disconnected", "reconnect")
    ):
        return "DATABENTO_CONNECTION_RESET"
    return default
