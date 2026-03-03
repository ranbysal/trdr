"""Locked CRO policy constants for deterministic risk controls."""

from __future__ import annotations

risk_pct_default = 0.0030
risk_pct_cap = 0.0050
family_open_risk_cap = 0.0075
total_open_risk_cap = 0.0120
daily_loss_limit = 0.0150
max_positions_per_symbol = 1
cooldown_losses_trigger = 3
cooldown_minutes = 30

tier1_lockout_pre_minutes = 15
tier1_lockout_post_minutes = 20
tier1_cancel_resting_entries_pre_minutes = 2

slippage_k_by_symbol: dict[str, float] = {
    "NQ": 0.08,
    "MNQ": 0.08,
    "YM": 0.06,
    "MYM": 0.06,
    "MGC": 0.10,
    "SIL": 0.15,
}

slippage_base_ticks_by_symbol: dict[str, float] = {
    "NQ": 1.0,
    "MNQ": 1.0,
    "YM": 1.0,
    "MYM": 1.0,
    "MGC": 1.0,
    "SIL": 2.0,
}

