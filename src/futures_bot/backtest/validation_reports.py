"""Validation report builders for the corrected replay harness."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def write_validation_reports(
    *,
    out_dir: str | Path,
    signals_by_instrument: pd.DataFrame,
    rejections_by_reason: pd.DataFrame,
    daily_halt_occurrences: pd.DataFrame,
    signal_frequency_by_instrument: pd.DataFrame,
    summary: dict[str, Any],
) -> dict[str, Path]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    signals_path = output_dir / "signals_by_instrument.csv"
    rejections_path = output_dir / "rejections_by_reason.csv"
    halts_path = output_dir / "daily_halt_occurrences.csv"
    frequency_path = output_dir / "signal_frequency_by_instrument.csv"
    summary_path = output_dir / "validation_summary.json"

    signals_by_instrument.to_csv(signals_path, index=False)
    rejections_by_reason.to_csv(rejections_path, index=False)
    daily_halt_occurrences.to_csv(halts_path, index=False)
    signal_frequency_by_instrument.to_csv(frequency_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True, default=str), encoding="utf-8")

    return {
        "signals_by_instrument_path": signals_path,
        "rejections_by_reason_path": rejections_path,
        "daily_halt_occurrences_path": halts_path,
        "signal_frequency_by_instrument_path": frequency_path,
        "summary_path": summary_path,
    }
