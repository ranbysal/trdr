"""Deterministic in-memory 1s quote store."""

from __future__ import annotations

from datetime import datetime

from futures_bot.core.types import Quote1s
from futures_bot.data.integrity import IntegrityError, LogEvent


class SymbolQuoteStore:
    """In-memory quote store keyed by timestamp for a single symbol."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._quotes: dict[datetime, Quote1s] = {}
        self.logs: list[LogEvent] = []

    def upsert(self, quote: Quote1s) -> None:
        if quote.symbol != self.symbol:
            raise IntegrityError(f"Quote symbol mismatch for store {self.symbol}: {quote.symbol}")

        if quote.ts in self._quotes and self._quotes[quote.ts] == quote:
            self.logs.append(LogEvent(ts=quote.ts, code="DUPLICATE_QUOTE_IGNORED", symbol=self.symbol))
            return

        self._quotes[quote.ts] = quote

    def get(self, ts: datetime) -> Quote1s | None:
        return self._quotes.get(ts)
