"""Inbound live feed message models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class Bar1mPayload:
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True, slots=True)
class Quote1sPayload:
    bid: float
    ask: float
    bid_size: float
    ask_size: float


@dataclass(frozen=True, slots=True)
class FeedMessage:
    type: Literal["bar_1m", "quote_1s", "event"]
    timestamp_et: datetime
    symbol: str
    payload: dict[str, Any]


class FeedSchemaError(ValueError):
    pass


def parse_feed_message(raw: dict[str, Any]) -> FeedMessage:
    msg_type = raw.get("type")
    if msg_type not in {"bar_1m", "quote_1s", "event"}:
        raise FeedSchemaError("invalid type")

    ts_raw = raw.get("timestamp_et")
    symbol = raw.get("symbol")
    payload = raw.get("payload", {})

    if not isinstance(ts_raw, str):
        raise FeedSchemaError("timestamp_et must be string")
    if not isinstance(symbol, str) or not symbol:
        raise FeedSchemaError("symbol must be non-empty string")
    if not isinstance(payload, dict):
        raise FeedSchemaError("payload must be object")

    try:
        ts = datetime.fromisoformat(ts_raw)
    except ValueError as exc:
        raise FeedSchemaError("timestamp_et must be ISO-8601") from exc

    if msg_type == "bar_1m":
        _parse_bar_payload(payload)
    elif msg_type == "quote_1s":
        _parse_quote_payload(payload)

    return FeedMessage(
        type=msg_type,
        timestamp_et=ts,
        symbol=symbol,
        payload=payload,
    )


def _parse_bar_payload(payload: dict[str, Any]) -> Bar1mPayload:
    try:
        return Bar1mPayload(
            open=float(payload["open"]),
            high=float(payload["high"]),
            low=float(payload["low"]),
            close=float(payload["close"]),
            volume=float(payload["volume"]),
        )
    except KeyError as exc:
        raise FeedSchemaError(f"missing bar payload field: {exc.args[0]}") from exc


def _parse_quote_payload(payload: dict[str, Any]) -> Quote1sPayload:
    try:
        return Quote1sPayload(
            bid=float(payload["bid"]),
            ask=float(payload["ask"]),
            bid_size=float(payload["bid_size"]),
            ask_size=float(payload["ask_size"]),
        )
    except KeyError as exc:
        raise FeedSchemaError(f"missing quote payload field: {exc.args[0]}") from exc
