"""Consecutive-loss cooldown tracking keyed by (module_id, symbol)."""

from __future__ import annotations

from datetime import datetime, timedelta

from futures_bot.policy import cro_policy
from futures_bot.risk.models import CooldownState


class ConsecutiveLossCooldownManager:
    """Tracks closed-trade loss streak and cooldown windows."""

    def __init__(
        self,
        *,
        threshold_losses: int = cro_policy.cooldown_losses_trigger,
        cooldown_minutes: int = cro_policy.cooldown_minutes,
    ) -> None:
        self._threshold_losses = threshold_losses
        self._cooldown_minutes = cooldown_minutes
        self._states: dict[tuple[str, str], CooldownState] = {}

    def is_in_cooldown(self, *, module_id: str, symbol: str, now: datetime) -> bool:
        state = self._states.get((module_id, symbol))
        if state is None or state.cooldown_until is None:
            return False
        return now < state.cooldown_until

    def get_state(self, *, module_id: str, symbol: str) -> CooldownState:
        return self._states.get((module_id, symbol), CooldownState(consecutive_losses=0, cooldown_until=None))

    def record_closed_trade(
        self,
        *,
        module_id: str,
        symbol: str,
        net_realized_pnl_after_costs: float,
        closed_at: datetime,
    ) -> CooldownState:
        key = (module_id, symbol)
        prev = self._states.get(key, CooldownState(consecutive_losses=0, cooldown_until=None))

        if net_realized_pnl_after_costs < 0.0:
            losses = prev.consecutive_losses + 1
            cooldown_until = prev.cooldown_until
            if losses >= self._threshold_losses:
                cooldown_until = closed_at + timedelta(minutes=self._cooldown_minutes)
            next_state = CooldownState(consecutive_losses=losses, cooldown_until=cooldown_until)
        else:
            next_state = CooldownState(consecutive_losses=0, cooldown_until=None)

        self._states[key] = next_state
        return next_state
