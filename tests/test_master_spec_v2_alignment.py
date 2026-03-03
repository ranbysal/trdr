from __future__ import annotations

import dataclasses
import re
from pathlib import Path

from futures_bot.core.reason_codes import ALL_REASON_CODES
from futures_bot.core.types import InstrumentMeta
from futures_bot.policy import cro_policy


def test_cro_policy_constants() -> None:
    assert cro_policy.risk_pct_default == 0.0030
    assert cro_policy.risk_pct_cap == 0.0050
    assert cro_policy.family_open_risk_cap == 0.0075
    assert cro_policy.total_open_risk_cap == 0.0120
    assert cro_policy.daily_loss_limit == 0.0150
    assert cro_policy.max_positions_per_symbol == 1
    assert cro_policy.cooldown_losses_trigger == 3
    assert cro_policy.cooldown_minutes == 30
    assert cro_policy.tier1_lockout_pre_minutes == 15
    assert cro_policy.tier1_lockout_post_minutes == 20
    assert cro_policy.tier1_cancel_resting_entries_pre_minutes == 2

    assert cro_policy.slippage_k_by_symbol == {
        "NQ": 0.08,
        "MNQ": 0.08,
        "YM": 0.06,
        "MYM": 0.06,
        "MGC": 0.10,
        "SIL": 0.15,
    }
    assert cro_policy.slippage_base_ticks_by_symbol == {
        "NQ": 1.0,
        "MNQ": 1.0,
        "YM": 1.0,
        "MYM": 1.0,
        "MGC": 1.0,
        "SIL": 2.0,
    }


def test_instrumentmeta_schema() -> None:
    fields = {f.name for f in dataclasses.fields(InstrumentMeta)}
    required = {
        "tick_size",
        "tick_value",
        "point_value",
        "commission_rt",
        "micro_equivalent",
        "family",
        "contract_units",
    }
    assert required.issubset(fields)


def test_reason_codes_smoke() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    code_files = [
        repo_root / "src/futures_bot/risk/portfolio_caps.py",
        repo_root / "src/futures_bot/risk/daily_halt.py",
        repo_root / "src/futures_bot/risk/sizing_single.py",
        repo_root / "src/futures_bot/pipeline/orb_pipeline.py",
        repo_root / "src/futures_bot/strategies/strategy_a_orb.py",
        repo_root / "src/futures_bot/data/calendar_store.py",
    ]

    discovered: set[str] = set()
    patterns = (
        r'reason_code="([A-Z0-9_]+)"',
        r'_reject\("([A-Z0-9_]+)"\)',
        r'code="([A-Z0-9_]+)"',
    )
    for path in code_files:
        text = path.read_text(encoding="utf-8")
        for pattern in patterns:
            discovered.update(re.findall(pattern, text))

    assert discovered <= ALL_REASON_CODES
    assert {
        "COOLDOWN_ACTIVE",
        "DAILY_LOSS_HALT",
        "TIER1_LOCKOUT_ACTIVE",
        "CALENDAR_LOCKOUT",
        "FAMILY_OPEN_RISK_CAP",
        "TOTAL_OPEN_RISK_CAP",
    } <= discovered

