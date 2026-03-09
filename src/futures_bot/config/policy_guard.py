"""Guard risk config against locked CRO policy defaults."""

from __future__ import annotations

import os
import sys
from typing import Any

OVERRIDE_ENV_VAR = "FUTURES_BOT_ALLOW_POLICY_DRIFT"
_TRUTHY = {"1", "true", "yes", "on"}


def _expected_policy_config(cro_policy: Any) -> dict[str, Any]:
    return {
        "risk_pct_default": cro_policy.risk_pct_default,
        "risk_pct_cap": cro_policy.risk_pct_cap,
        "family_open_risk_cap": cro_policy.family_open_risk_cap,
        "total_open_risk_cap": cro_policy.total_open_risk_cap,
        "daily_loss_limit": cro_policy.daily_loss_limit,
        "max_positions_per_symbol": cro_policy.max_positions_per_symbol,
        "cooldown_losses_trigger": cro_policy.cooldown_losses_trigger,
        "cooldown_minutes": cro_policy.cooldown_minutes,
        "tier1": {
            "lockout_pre_minutes": cro_policy.tier1_lockout_pre_minutes,
            "lockout_post_minutes": cro_policy.tier1_lockout_post_minutes,
            "cancel_resting_entries_pre_minutes": cro_policy.tier1_cancel_resting_entries_pre_minutes,
        },
        "slippage_k_by_symbol": dict(cro_policy.slippage_k_by_symbol),
        "slippage_base_ticks_by_symbol": dict(cro_policy.slippage_base_ticks_by_symbol),
    }


def _effective_risk_section(risk_cfg: Any) -> dict[str, Any]:
    if not isinstance(risk_cfg, dict):
        return {}
    nested = risk_cfg.get("risk")
    if isinstance(nested, dict):
        return nested
    return risk_cfg


def _diff_expected_vs_actual(
    expected: dict[str, Any],
    actual: dict[str, Any],
    *,
    prefix: str = "",
) -> list[str]:
    diffs: list[str] = []
    for key, expected_value in expected.items():
        path = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"
        if key not in actual:
            diffs.append(f"{path}: expected {expected_value!r}, got <missing>")
            continue
        actual_value = actual[key]
        if isinstance(expected_value, dict):
            if not isinstance(actual_value, dict):
                diffs.append(f"{path}: expected mapping, got {type(actual_value).__name__}")
                continue
            diffs.extend(_diff_expected_vs_actual(expected_value, actual_value, prefix=path))
            continue
        if actual_value != expected_value:
            diffs.append(f"{path}: expected {expected_value!r}, got {actual_value!r}")
    return diffs


def validate_config_matches_policy(risk_cfg: Any, cro_policy: Any) -> None:
    """Raise ValueError when risk config drifts from locked policy constants."""
    expected = _expected_policy_config(cro_policy)
    actual = _effective_risk_section(risk_cfg)
    diffs = _diff_expected_vs_actual(expected, actual)
    if diffs:
        formatted = "\n".join(f"- {item}" for item in diffs)
        raise ValueError(
            "Risk config does not match locked CRO policy constants.\n"
            f"Mismatches:\n{formatted}"
        )


def is_policy_drift_override_enabled(allow_policy_drift: bool = False) -> bool:
    """Return True when strict policy guard should be bypassed."""
    if allow_policy_drift:
        return True
    value = os.getenv(OVERRIDE_ENV_VAR, "")
    return value.strip().lower() in _TRUTHY


def enforce_policy_guard(
    risk_cfg: Any,
    cro_policy: Any,
    *,
    allow_policy_drift: bool = False,
) -> None:
    """Validate or bypass with explicit operator override and loud warning."""
    try:
        validate_config_matches_policy(risk_cfg, cro_policy)
    except ValueError as exc:
        if not is_policy_drift_override_enabled(allow_policy_drift):
            raise
        print(
            "WARNING: Policy guard override enabled; continuing with config drift. "
            f"Set `{OVERRIDE_ENV_VAR}=0` or omit `--allow-policy-drift` to enforce strict mode.",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
