"""Clock utilities used to preserve deterministic time handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True)
class Clock:
    """Simple injectable clock interface for deterministic tests."""

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)
