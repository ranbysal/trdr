"""Config loading for the isolated CORR-V4 paper executor."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True, slots=True)
class CorrectedPaperConfig:
    enabled: bool
    source_queue_path: Path
    state_path: Path
    journal_path: Path
    events_path: Path
    reports_dir: Path
    default_point_values: dict[str, float] = field(default_factory=dict)


def load_corrected_paper_config(config_dir: str | Path) -> CorrectedPaperConfig:
    config_path = Path(config_dir) / "bot.yaml"
    if not config_path.exists():
        raise ValueError(f"Corrected paper config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Corrected paper config root must be a mapping: {config_path}")

    bot = payload.get("bot") or {}
    storage = payload.get("storage") or {}
    point_values = payload.get("point_values") or {}
    if not isinstance(bot, dict):
        raise ValueError("bot.yaml: key 'bot' must be a mapping when present")
    if not isinstance(storage, dict):
        raise ValueError("bot.yaml: key 'storage' must be a mapping when present")
    if not isinstance(point_values, dict):
        raise ValueError("bot.yaml: key 'point_values' must be a mapping when present")

    return CorrectedPaperConfig(
        enabled=bool(bot.get("enabled", True)),
        source_queue_path=Path(str(bot.get("source_queue_path", "out/corrected_v4/execution_events.ndjson"))),
        state_path=Path(str(storage.get("state_path", "state/corrected_paper/corrected_paper_state.json"))),
        journal_path=Path(str(storage.get("journal_path", "out/corrected_paper/corrected_paper_journal.ndjson"))),
        events_path=Path(str(storage.get("events_path", "out/corrected_paper/events.ndjson"))),
        reports_dir=Path(str(storage.get("reports_dir", "out/corrected_paper/reports"))),
        default_point_values={str(key).upper(): float(value) for key, value in point_values.items()},
    )
