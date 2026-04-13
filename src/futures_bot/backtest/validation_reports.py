"""Validation report builders for the corrected replay harness."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def write_validation_reports(
    *,
    out_dir: str | Path,
    accepted_bars: pd.DataFrame,
    accepted_signals: pd.DataFrame,
    actionable_signal_breakdown: pd.DataFrame,
    actionable_setup_fingerprints: pd.DataFrame,
    actionable_signals_by_instrument_and_session: pd.DataFrame,
    actionable_signals_by_instrument_and_strategy: pd.DataFrame,
    actionable_signals_by_instrument_and_hour: pd.DataFrame,
    actionable_signals_by_date: pd.DataFrame,
    common_actionable_setup_fingerprints: pd.DataFrame,
    rejected_signals: pd.DataFrame,
    rejection_reason_counts: pd.DataFrame,
    rejection_reason_counts_by_instrument: pd.DataFrame,
    signal_frequency_by_instrument: pd.DataFrame,
    daily_halt_events: pd.DataFrame,
    repeated_accepted_signals: pd.DataFrame,
    repeated_accepted_signals_by_instrument: pd.DataFrame,
    instrument_diagnostics: pd.DataFrame,
    summary: dict[str, Any],
) -> dict[str, Path]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    accepted_bars_path = output_dir / "accepted_bars.csv"
    accepted_signals_path = output_dir / "accepted_signals.csv"
    actionable_signal_breakdown_path = output_dir / "actionable_signal_breakdown.csv"
    actionable_setup_fingerprints_path = output_dir / "actionable_setup_fingerprints.csv"
    actionable_signals_by_instrument_and_session_path = output_dir / "actionable_signals_by_instrument_and_session.csv"
    actionable_signals_by_instrument_and_strategy_path = output_dir / "actionable_signals_by_instrument_and_strategy.csv"
    actionable_signals_by_instrument_and_hour_path = output_dir / "actionable_signals_by_instrument_and_hour.csv"
    actionable_signals_by_date_path = output_dir / "actionable_signals_by_date.csv"
    common_actionable_setup_fingerprints_path = output_dir / "common_actionable_setup_fingerprints.csv"
    rejected_signals_path = output_dir / "rejected_signals.csv"
    rejection_reason_counts_path = output_dir / "rejection_reason_counts.csv"
    rejection_reason_counts_by_instrument_path = output_dir / "rejection_reason_counts_by_instrument.csv"
    frequency_path = output_dir / "signal_frequency_by_instrument.csv"
    daily_halt_events_path = output_dir / "daily_halt_events.csv"
    repeated_accepted_signals_path = output_dir / "repeated_accepted_signals.csv"
    repeated_accepted_signals_by_instrument_path = output_dir / "accepted_signal_repetition_by_instrument.csv"
    instrument_diagnostics_path = output_dir / "instrument_diagnostics.csv"
    summary_path = output_dir / "summary.json"

    accepted_bars.to_csv(accepted_bars_path, index=False)
    accepted_signals.to_csv(accepted_signals_path, index=False)
    actionable_signal_breakdown.to_csv(actionable_signal_breakdown_path, index=False)
    actionable_setup_fingerprints.to_csv(actionable_setup_fingerprints_path, index=False)
    actionable_signals_by_instrument_and_session.to_csv(actionable_signals_by_instrument_and_session_path, index=False)
    actionable_signals_by_instrument_and_strategy.to_csv(actionable_signals_by_instrument_and_strategy_path, index=False)
    actionable_signals_by_instrument_and_hour.to_csv(actionable_signals_by_instrument_and_hour_path, index=False)
    actionable_signals_by_date.to_csv(actionable_signals_by_date_path, index=False)
    common_actionable_setup_fingerprints.to_csv(common_actionable_setup_fingerprints_path, index=False)
    rejected_signals.to_csv(rejected_signals_path, index=False)
    rejection_reason_counts.to_csv(rejection_reason_counts_path, index=False)
    rejection_reason_counts_by_instrument.to_csv(rejection_reason_counts_by_instrument_path, index=False)
    signal_frequency_by_instrument.to_csv(frequency_path, index=False)
    daily_halt_events.to_csv(daily_halt_events_path, index=False)
    repeated_accepted_signals.to_csv(repeated_accepted_signals_path, index=False)
    repeated_accepted_signals_by_instrument.to_csv(repeated_accepted_signals_by_instrument_path, index=False)
    instrument_diagnostics.to_csv(instrument_diagnostics_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True, default=str), encoding="utf-8")

    return {
        "accepted_bars_path": accepted_bars_path,
        "accepted_signals_path": accepted_signals_path,
        "actionable_signal_breakdown_path": actionable_signal_breakdown_path,
        "actionable_setup_fingerprints_path": actionable_setup_fingerprints_path,
        "actionable_signals_by_instrument_and_session_path": actionable_signals_by_instrument_and_session_path,
        "actionable_signals_by_instrument_and_strategy_path": actionable_signals_by_instrument_and_strategy_path,
        "actionable_signals_by_instrument_and_hour_path": actionable_signals_by_instrument_and_hour_path,
        "actionable_signals_by_date_path": actionable_signals_by_date_path,
        "common_actionable_setup_fingerprints_path": common_actionable_setup_fingerprints_path,
        "rejected_signals_path": rejected_signals_path,
        "rejection_reason_counts_path": rejection_reason_counts_path,
        "rejection_reason_counts_by_instrument_path": rejection_reason_counts_by_instrument_path,
        "signal_frequency_by_instrument_path": frequency_path,
        "daily_halt_events_path": daily_halt_events_path,
        "repeated_accepted_signals_path": repeated_accepted_signals_path,
        "repeated_accepted_signals_by_instrument_path": repeated_accepted_signals_by_instrument_path,
        "instrument_diagnostics_path": instrument_diagnostics_path,
        "summary_path": summary_path,
    }
