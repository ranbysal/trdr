from __future__ import annotations

import json
from pathlib import Path

from futures_bot.runtime.ndjson_writer import NdjsonWriter


def _read_lines(path: Path) -> list[str]:
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_ndjson_writer_appends_multiple_events(tmp_path: Path) -> None:
    path = tmp_path / "trade_logs.json"
    writer = NdjsonWriter(path)

    writer.write({"event": "a", "value": 1})
    writer.write({"event": "b", "value": 2})
    writer.flush()

    lines = _read_lines(path)
    assert len(lines) == 2
    assert json.loads(lines[0])["event"] == "a"
    assert json.loads(lines[1])["event"] == "b"


def test_ndjson_writer_does_not_overwrite_existing_lines(tmp_path: Path) -> None:
    path = tmp_path / "trade_logs.json"
    first = NdjsonWriter(path)
    first.write({"event": "first", "n": 1})
    first.flush()

    second = NdjsonWriter(path)
    second.write({"event": "second", "n": 2})
    second.flush()

    lines = _read_lines(path)
    assert len(lines) == 2
    assert json.loads(lines[0])["event"] == "first"
    assert json.loads(lines[1])["event"] == "second"


def test_ndjson_writer_flush_every_n(tmp_path: Path) -> None:
    path = tmp_path / "batch" / "trade_logs.json"
    writer = NdjsonWriter(path, flush_every_n=2)

    writer.write({"event": "a"})
    assert not path.exists()

    writer.write({"event": "b"})
    assert path.exists()
    assert len(_read_lines(path)) == 2

    writer.write({"event": "c"})
    assert len(_read_lines(path)) == 2

    writer.flush()
    assert len(_read_lines(path)) == 3
    assert all(isinstance(json.loads(line), dict) for line in _read_lines(path))
