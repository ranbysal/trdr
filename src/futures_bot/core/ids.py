"""Deterministic ID helpers."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_timestamp_id(prefix: str) -> str:
    """Return a deterministic UTC timestamp ID string (second precision)."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{ts}"
