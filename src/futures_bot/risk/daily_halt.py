"""Daily realized-loss halt manager for new entries."""

from __future__ import annotations

from futures_bot.policy import cro_policy
from futures_bot.risk.models import RiskDecision


class DailyHaltManager:
    """Halts new entries when realized session loss exceeds threshold."""

    def __init__(self, *, realized_loss_halt_pct: float = cro_policy.daily_loss_limit) -> None:
        self._realized_loss_halt_pct = realized_loss_halt_pct
        self._session_start_equity: float | None = None
        self._realized_pnl: float = 0.0

    def reset_session(self, *, session_start_equity: float) -> None:
        self._session_start_equity = float(session_start_equity)
        self._realized_pnl = 0.0

    def update_realized_pnl(self, *, realized_pnl: float) -> None:
        self._realized_pnl = float(realized_pnl)

    def can_open_new_entry(self) -> RiskDecision:
        if self._session_start_equity is None:
            return RiskDecision(approved=True, reason_code="APPROVED")

        loss_limit = -self._session_start_equity * self._realized_loss_halt_pct
        if self._realized_pnl <= loss_limit:
            return RiskDecision(
                approved=False,
                reason_code="DAILY_LOSS_HALT",
                details={"realized_pnl": self._realized_pnl, "loss_limit": loss_limit},
            )
        return RiskDecision(approved=True, reason_code="APPROVED")
