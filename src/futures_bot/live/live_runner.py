"""Live feed runner that drives signal generation from live bars/quotes."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from futures_bot.core.enums import Regime, StrategyModule
from futures_bot.core.types import InstrumentMeta
from futures_bot.live.databento_adapter import DatabentoLiveClient
from futures_bot.live.feed_models import FeedMessage
from futures_bot.live.ws_client import LiveWsClient
from futures_bot.alerts.telegram import TelegramNotifier
from futures_bot.pipeline.multistrategy_signals import MultiStrategySignalEngine
from futures_bot.runtime.ndjson_writer import NdjsonWriter


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
        instruments_by_symbol: dict[str, InstrumentMeta],
        enabled_strategies: set[StrategyModule],
        feed_client: Any | None = None,
        notifier: TelegramNotifier | None = None,
        queue_maxsize: int = 2000,
        databento_api_key: str | None = None,
        databento_dataset: str = "GLBX.MDP3",
        ws_url: str | None = None,
    ) -> None:
        out_path = Path(out_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        self._events_log = NdjsonWriter(out_path / "live_events.ndjson")
        self._engine = MultiStrategySignalEngine(
            out_dir=out_path,
            instruments_by_symbol=instruments_by_symbol,
            enabled_strategies=enabled_strategies,
            notifier=notifier,
        )
        self._states: dict[str, _SymbolState] = {}
        self._global_freeze = False
        self._lockout = False
        self._client = feed_client or self._build_client(
            databento_api_key=databento_api_key,
            databento_dataset=databento_dataset,
            instruments_by_symbol=instruments_by_symbol,
            queue_maxsize=queue_maxsize,
            ws_url=ws_url,
        )

    async def run(self, *, max_messages: int | None = None, max_runtime_s: float | None = None) -> None:
        await self._client.start()
        processed = 0
        started = asyncio.get_event_loop().time()
        try:
            async for message in self._client.messages():
                await self._handle_message(message)
                processed += 1
                if max_messages is not None and processed >= max_messages:
                    break
                if max_runtime_s is not None and (asyncio.get_event_loop().time() - started) >= max_runtime_s:
                    break
        finally:
            await self._client.stop()
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
        self._events_log.write(
            {
                "ts": message.timestamp_et.isoformat(),
                "event": "feed_event",
                "code": code,
                "symbol": message.symbol,
            }
        )

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
                        "ts": bar_ts.isoformat(),
                        "event": "risk_event",
                        "code": "DATA_GAP_DETECTED",
                        "symbol": symbol,
                        "gap_seconds": gap_s,
                    }
                )
        state.last_bar_ts = bar_ts

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
        }
        self._engine.process_row(row)

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
                "ts": datetime.utcnow().isoformat(),
                "event": "risk_event",
                "code": code,
                "symbol": "*",
            }
        )

    def _build_client(
        self,
        *,
        databento_api_key: str | None,
        databento_dataset: str,
        instruments_by_symbol: dict[str, InstrumentMeta],
        queue_maxsize: int,
        ws_url: str | None,
    ) -> Any:
        if databento_api_key:
            return DatabentoLiveClient(
                api_key=databento_api_key,
                dataset=databento_dataset,
                symbols=sorted(instruments_by_symbol),
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


async def run_live_signals(
    *,
    out_dir: str | Path,
    instruments_by_symbol: dict[str, InstrumentMeta],
    enabled_strategies: set[StrategyModule],
    notifier: TelegramNotifier | None = None,
    max_messages: int | None = None,
    max_runtime_s: float | None = None,
    databento_api_key: str | None = None,
    databento_dataset: str = "GLBX.MDP3",
    feed_client: Any | None = None,
    ws_url: str | None = None,
) -> None:
    runner = LiveSignalRunner(
        out_dir=out_dir,
        instruments_by_symbol=instruments_by_symbol,
        enabled_strategies=enabled_strategies,
        feed_client=feed_client,
        notifier=notifier,
        databento_api_key=databento_api_key,
        databento_dataset=databento_dataset,
        ws_url=ws_url,
    )
    await runner.run(max_messages=max_messages, max_runtime_s=max_runtime_s)
