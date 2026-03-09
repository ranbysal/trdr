from __future__ import annotations

import pytest

from futures_bot.config.policy_guard import validate_config_matches_policy
from futures_bot.policy import cro_policy


def _matching_risk_config() -> dict[str, object]:
    return {
        "version": 1,
        "risk": {
            "risk_pct_default": 0.0030,
            "risk_pct_cap": 0.0050,
            "family_open_risk_cap": 0.0075,
            "total_open_risk_cap": 0.0120,
            "daily_loss_limit": 0.0150,
            "max_positions_per_symbol": 1,
            "cooldown_losses_trigger": 3,
            "cooldown_minutes": 30,
            "tier1": {
                "lockout_pre_minutes": 15,
                "lockout_post_minutes": 20,
                "cancel_resting_entries_pre_minutes": 2,
            },
            "slippage_k_by_symbol": {
                "NQ": 0.08,
                "MNQ": 0.08,
                "YM": 0.06,
                "MYM": 0.06,
                "MGC": 0.10,
                "SIL": 0.15,
            },
            "slippage_base_ticks_by_symbol": {
                "NQ": 1.0,
                "MNQ": 1.0,
                "YM": 1.0,
                "MYM": 1.0,
                "MGC": 1.0,
                "SIL": 2.0,
            },
        },
    }


def test_validate_config_matches_policy_passes_on_match() -> None:
    validate_config_matches_policy(_matching_risk_config(), cro_policy)


def test_validate_config_matches_policy_raises_on_mismatch() -> None:
    cfg = _matching_risk_config()
    risk = cfg["risk"]
    assert isinstance(risk, dict)
    risk["risk_pct_cap"] = 0.0099
    tier1 = risk["tier1"]
    assert isinstance(tier1, dict)
    tier1["lockout_pre_minutes"] = 99

    with pytest.raises(ValueError) as exc_info:
        validate_config_matches_policy(cfg, cro_policy)

    message = str(exc_info.value)
    assert "risk_pct_cap" in message
    assert "tier1.lockout_pre_minutes" in message
