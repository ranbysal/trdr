"""Daily realized-loss halt manager for new entries."""

from __future__ import annotations

from collections.abc import Sequence

from futures_bot.policy import cro_policy
from futures_bot.risk.models import AccountRiskState, OpenPositionMtmSnapshot, RiskDecision


def compute_daily_pnl(
    *,
    realized_pnl: float,
    open_positions: Sequence[OpenPositionMtmSnapshot],
) -> tuple[float, float, float]:
    unrealized_pnl = float(sum(position.unrealized_pnl for position in open_positions))
    realized_total = float(realized_pnl)
    return realized_total, unrealized_pnl, realized_total + unrealized_pnl


def build_account_risk_state(
    *,
    session_start_equity: float,
    realized_pnl: float,
    open_positions: Sequence[OpenPositionMtmSnapshot],
    daily_loss_halt_pct: float = cro_policy.daily_loss_limit,
) -> AccountRiskState:
    realized_total, unrealized_total, daily_total = compute_daily_pnl(
        realized_pnl=realized_pnl,
        open_positions=open_positions,
    )
    daily_loss_limit = float(session_start_equity * daily_loss_halt_pct)
    return AccountRiskState(
        session_start_equity=float(session_start_equity),
        realized_pnl=realized_total,
        unrealized_pnl=unrealized_total,
        daily_pnl=daily_total,
        daily_loss_limit=daily_loss_limit,
        is_daily_halt=daily_total <= -daily_loss_limit,
    )


class DailyHaltManager:
    """Halts new entries when mark-to-market daily loss exceeds threshold."""

    def __init__(self, *, realized_loss_halt_pct: float = cro_policy.daily_loss_limit) -> None:
        self._daily_loss_halt_pct = realized_loss_halt_pct
        self._session_start_equity: float | None = None
        self._realized_pnl: float = 0.0
        self._open_positions: tuple[OpenPositionMtmSnapshot, ...] = ()

    def reset_session(self, *, session_start_equity: float) -> None:
        self._session_start_equity = float(session_start_equity)
        self._realized_pnl = 0.0
        self._open_positions = ()

    def update_realized_pnl(self, *, realized_pnl: float) -> None:
        self._realized_pnl = float(realized_pnl)

    def update_open_positions(self, *, open_positions: Sequence[OpenPositionMtmSnapshot]) -> None:
        self._open_positions = tuple(open_positions)

    def mark_to_market_state(self) -> AccountRiskState | None:
        if self._session_start_equity is None:
            return None
        return build_account_risk_state(
            session_start_equity=self._session_start_equity,
            realized_pnl=self._realized_pnl,
            open_positions=self._open_positions,
            daily_loss_halt_pct=self._daily_loss_halt_pct,
        )

    def can_open_new_entry(self) -> RiskDecision:
        if self._session_start_equity is None:
            return RiskDecision(approved=True, reason_code="APPROVED")

        state = self.mark_to_market_state()
        if state is None:
            return RiskDecision(approved=True, reason_code="APPROVED")
        if state.is_daily_halt:
            return RiskDecision(
                approved=False,
                reason_code="DAILY_LOSS_HALT",
                details={
                    "realized_pnl": state.realized_pnl,
                    "unrealized_pnl": state.unrealized_pnl,
                    "daily_pnl": state.daily_pnl,
                    "loss_limit": -state.daily_loss_limit,
                },
            )
        return RiskDecision(approved=True, reason_code="APPROVED")
