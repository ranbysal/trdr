"""Append-only newline-delimited JSON writer."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any


class NdjsonWriter:
    """Write one JSON object per line with append-safe semantics."""

    def __init__(self, path: str | Path, flush_every_n: int = 1, fsync: bool = False) -> None:
        if flush_every_n < 1:
            raise ValueError("flush_every_n must be >= 1")
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._flush_every_n = flush_every_n
        self._fsync = fsync
        self._pending: list[str] = []
        self._writes_since_flush = 0
        self._lock = threading.Lock()

        self._needs_leading_newline = False
        if self._path.exists() and self._path.stat().st_size > 0:
            with self._path.open("rb") as handle:
                handle.seek(-1, 2)
                self._needs_leading_newline = handle.read(1) != b"\n"

    def write(self, event_dict: dict[str, Any]) -> None:
        line = json.dumps(event_dict, sort_keys=True, default=str)
        with self._lock:
            if self._needs_leading_newline:
                self._pending.append("\n")
                self._needs_leading_newline = False
            self._pending.append(f"{line}\n")
            self._writes_since_flush += 1
            if self._writes_since_flush >= self._flush_every_n:
                self._flush_locked()

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        if not self._pending:
            return
        payload = "".join(self._pending)
        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            if self._fsync:
                os.fsync(handle.fileno())
        self._pending.clear()
        self._writes_since_flush = 0

    def __del__(self) -> None:
        try:
            self.flush()
        except Exception:
            pass
