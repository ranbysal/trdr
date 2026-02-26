from __future__ import annotations

from datetime import datetime, timedelta, timezone

from futures_bot.risk.cooldowns import ConsecutiveLossCooldownManager


def test_cooldown_trigger_after_three_losses() -> None:
    manager = ConsecutiveLossCooldownManager()
    t0 = datetime(2026, 1, 5, 14, 0, tzinfo=timezone.utc)

    manager.record_closed_trade(
        module_id="strat_a_orb",
        symbol="NQ",
        net_realized_pnl_after_costs=-10.0,
        closed_at=t0,
    )
    manager.record_closed_trade(
        module_id="strat_a_orb",
        symbol="NQ",
        net_realized_pnl_after_costs=-5.0,
        closed_at=t0 + timedelta(minutes=1),
    )
    state = manager.record_closed_trade(
        module_id="strat_a_orb",
        symbol="NQ",
        net_realized_pnl_after_costs=-1.0,
        closed_at=t0 + timedelta(minutes=2),
    )

    assert state.consecutive_losses == 3
    assert state.cooldown_until == t0 + timedelta(minutes=32)
    assert manager.is_in_cooldown(
        module_id="strat_a_orb",
        symbol="NQ",
        now=t0 + timedelta(minutes=10),
    )


def test_cooldown_reset_on_win_or_flat() -> None:
    manager = ConsecutiveLossCooldownManager()
    t0 = datetime(2026, 1, 5, 14, 0, tzinfo=timezone.utc)

    manager.record_closed_trade(
        module_id="strat_b_vwap_rev",
        symbol="MGC",
        net_realized_pnl_after_costs=-10.0,
        closed_at=t0,
    )
    manager.record_closed_trade(
        module_id="strat_b_vwap_rev",
        symbol="MGC",
        net_realized_pnl_after_costs=-5.0,
        closed_at=t0 + timedelta(minutes=1),
    )
    state = manager.record_closed_trade(
        module_id="strat_b_vwap_rev",
        symbol="MGC",
        net_realized_pnl_after_costs=0.0,
        closed_at=t0 + timedelta(minutes=2),
    )

    assert state.consecutive_losses == 0
    assert state.cooldown_until is None
