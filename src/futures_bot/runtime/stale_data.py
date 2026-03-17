"""Stale data detection for live runtime monitoring."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class StaleDataEvent:
    kind: str
    symbol: str
    stream: str
    lag_seconds: float
    last_timestamp: datetime | None


class StaleDataMonitor:
    def __init__(
        self,
        *,
        bars_timeout_s: float = 180.0,
        quote_timeout_s: float = 30.0,
        last_bar_by_symbol: dict[str, datetime] | None = None,
        stale_flags: dict[str, bool] | None = None,
    ) -> None:
        self._bars_timeout_s = bars_timeout_s
        self._quote_timeout_s = quote_timeout_s
        self._last_bar_by_symbol = dict(last_bar_by_symbol or {})
        self._last_quote_by_symbol: dict[str, datetime] = {}
        self._stale_flags = dict(stale_flags or {})
        self._persistent_sent: dict[str, bool] = {}

    def mark_bar(self, symbol: str, ts: datetime) -> list[StaleDataEvent]:
        self._last_bar_by_symbol[symbol] = ts
        return self._recover(symbol=symbol, stream="bar", ts=ts)

    def mark_quote(self, symbol: str, ts: datetime) -> list[StaleDataEvent]:
        self._last_quote_by_symbol[symbol] = ts
        return self._recover(symbol=symbol, stream="quote", ts=ts)

    def check(
        self,
        *,
        now_et: datetime,
        market_open: bool,
        symbols: set[str],
        quote_stream_enabled: bool,
    ) -> list[StaleDataEvent]:
        if not market_open:
            return []
        now_et = now_et.astimezone(ET)
        events: list[StaleDataEvent] = []
        for symbol in sorted(symbols):
            events.extend(self._check_stream(now_et=now_et, symbol=symbol, stream="bar", timeout_s=self._bars_timeout_s))
            if quote_stream_enabled:
                events.extend(
                    self._check_stream(now_et=now_et, symbol=symbol, stream="quote", timeout_s=self._quote_timeout_s)
                )
        return events

    def stale_flags(self) -> dict[str, bool]:
        return dict(self._stale_flags)

    def last_bar_by_symbol(self) -> dict[str, datetime]:
        return dict(self._last_bar_by_symbol)

    def _check_stream(self, *, now_et: datetime, symbol: str, stream: str, timeout_s: float) -> list[StaleDataEvent]:
        key = f"{stream}:{symbol}"
        last_seen = self._last_seen(symbol=symbol, stream=stream)
        if last_seen is None:
            return []
        lag_s = max(0.0, (now_et - last_seen).total_seconds())
        if lag_s <= timeout_s:
            return []

        events: list[StaleDataEvent] = []
        if not self._stale_flags.get(key, False):
            self._stale_flags[key] = True
            self._persistent_sent[key] = False
            events.append(
                StaleDataEvent(
                    kind="stale",
                    symbol=symbol,
                    stream=stream,
                    lag_seconds=lag_s,
                    last_timestamp=last_seen,
                )
            )
        elif lag_s > (timeout_s * 2.0) and not self._persistent_sent.get(key, False):
            self._persistent_sent[key] = True
            events.append(
                StaleDataEvent(
                    kind="persistent",
                    symbol=symbol,
                    stream=stream,
                    lag_seconds=lag_s,
                    last_timestamp=last_seen,
                )
            )
        return events

    def _recover(self, *, symbol: str, stream: str, ts: datetime) -> list[StaleDataEvent]:
        key = f"{stream}:{symbol}"
        if not self._stale_flags.get(key, False):
            return []
        self._stale_flags[key] = False
        self._persistent_sent[key] = False
        return [
            StaleDataEvent(
                kind="recovered",
                symbol=symbol,
                stream=stream,
                lag_seconds=0.0,
                last_timestamp=ts,
            )
        ]

    def _last_seen(self, *, symbol: str, stream: str) -> datetime | None:
        if stream == "bar":
            return self._last_bar_by_symbol.get(symbol)
        return self._last_quote_by_symbol.get(symbol)
