"""Validation report builders for the corrected replay harness."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def write_validation_reports(
    *,
    out_dir: str | Path,
    accepted_signals: pd.DataFrame,
    rejected_signals: pd.DataFrame,
    rejection_reason_counts: pd.DataFrame,
    signal_frequency_by_instrument: pd.DataFrame,
    daily_halt_events: pd.DataFrame,
    summary: dict[str, Any],
) -> dict[str, Path]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    accepted_signals_path = output_dir / "accepted_signals.csv"
    rejected_signals_path = output_dir / "rejected_signals.csv"
    rejection_reason_counts_path = output_dir / "rejection_reason_counts.csv"
    frequency_path = output_dir / "signal_frequency_by_instrument.csv"
    daily_halt_events_path = output_dir / "daily_halt_events.csv"
    summary_path = output_dir / "summary.json"

    accepted_signals.to_csv(accepted_signals_path, index=False)
    rejected_signals.to_csv(rejected_signals_path, index=False)
    rejection_reason_counts.to_csv(rejection_reason_counts_path, index=False)
    signal_frequency_by_instrument.to_csv(frequency_path, index=False)
    daily_halt_events.to_csv(daily_halt_events_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True, default=str), encoding="utf-8")

    return {
        "accepted_signals_path": accepted_signals_path,
        "rejected_signals_path": rejected_signals_path,
        "rejection_reason_counts_path": rejection_reason_counts_path,
        "signal_frequency_by_instrument_path": frequency_path,
        "daily_halt_events_path": daily_halt_events_path,
        "summary_path": summary_path,
    }
