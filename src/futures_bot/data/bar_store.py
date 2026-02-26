"""Deterministic in-memory 1m bar ingestion store."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from futures_bot.core.types import Bar1m
from futures_bot.data.integrity import IntegrityError, LogEvent, bars_ohlcv_equal


@dataclass(slots=True)
class _StoredBar:
    bar: Bar1m
    finalized: bool


@dataclass(frozen=True, slots=True)
class BarIngestResult:
    accepted: bool
    ignored: bool
    overwritten: bool
    data_ok: bool


class SymbolBarStore:
    """In-memory bar store that enforces spec v2.0 ingestion rules per symbol."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.data_ok = True
        self._bars: dict[datetime, _StoredBar] = {}
        self.logs: list[LogEvent] = []

    def ingest(
        self,
        bar: Bar1m,
        *,
        provisional: bool,
        is_active_session: bool,
        opening_gap: bool = False,
        wide_first_or_bar: bool = False,
    ) -> BarIngestResult:
        if bar.symbol != self.symbol:
            raise IntegrityError(f"Bar symbol mismatch for store {self.symbol}: {bar.symbol}")

        existing = self._bars.get(bar.ts)
        if existing is not None:
            return self._handle_duplicate(existing, bar, provisional)

        self._detect_gap(
            ts=bar.ts,
            is_active_session=is_active_session,
            opening_gap=opening_gap,
            wide_first_or_bar=wide_first_or_bar,
        )

        self._bars[bar.ts] = _StoredBar(bar=bar, finalized=not provisional)
        return BarIngestResult(accepted=True, ignored=False, overwritten=False, data_ok=self.data_ok)

    def get_bar(self, ts: datetime) -> Bar1m | None:
        item = self._bars.get(ts)
        return item.bar if item is not None else None

    def _handle_duplicate(self, existing: _StoredBar, incoming: Bar1m, provisional: bool) -> BarIngestResult:
        if bars_ohlcv_equal(existing.bar, incoming):
            if not existing.finalized and not provisional:
                # Promote the existing minute from provisional to finalized without data mutation.
                existing.finalized = True
                return BarIngestResult(accepted=True, ignored=False, overwritten=False, data_ok=self.data_ok)

            self.logs.append(
                LogEvent(ts=incoming.ts, code="DUPLICATE_BAR_IGNORED", symbol=self.symbol)
            )
            return BarIngestResult(accepted=False, ignored=True, overwritten=False, data_ok=self.data_ok)

        if not existing.finalized:
            existing.bar = incoming
            existing.finalized = not provisional
            self.logs.append(LogEvent(ts=incoming.ts, code="BAR_REVISION_ACCEPTED", symbol=self.symbol))
            return BarIngestResult(accepted=True, ignored=False, overwritten=True, data_ok=self.data_ok)

        self.data_ok = False
        self.logs.append(LogEvent(ts=incoming.ts, code="BAR_REVISION_AFTER_FINAL", symbol=self.symbol))
        return BarIngestResult(accepted=False, ignored=False, overwritten=False, data_ok=self.data_ok)

    def _detect_gap(
        self,
        *,
        ts: datetime,
        is_active_session: bool,
        opening_gap: bool,
        wide_first_or_bar: bool,
    ) -> None:
        if not is_active_session or not self._bars:
            return

        last_ts = max(self._bars)
        delta_minutes = int((ts - last_ts).total_seconds() // 60)
        if delta_minutes <= 2:
            return

        self.logs.append(
            LogEvent(
                ts=ts,
                code="GAP_FLAG",
                symbol=self.symbol,
                details={"delta_minutes": delta_minutes},
            )
        )
        if not (opening_gap or wide_first_or_bar):
            self.data_ok = False
