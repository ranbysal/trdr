"""Config-driven paper sizing for Bot 3."""

from __future__ import annotations

import math

from bot_exec_v3.models import PositionPlan, SignalEvent, SizingConfig


class PaperRiskSizer:
    def __init__(self, config: SizingConfig) -> None:
        self._config = config

    def size_signal(self, signal: SignalEvent) -> PositionPlan:
        instrument = signal.instrument.upper()
        fixed_contracts = self._config.contracts_by_instrument.get(instrument, self._config.default_contracts)
        quantity = max(0, int(fixed_contracts))
        point_value = float(self._config.point_value_by_instrument.get(instrument, 1.0))

        if (
            self._config.risk_per_trade_percent is not None
            and self._config.account_size is not None
            and point_value > 0.0
        ):
            risk_budget = self._config.account_size * self._config.risk_per_trade_percent
            per_contract_risk = abs(signal.entry - signal.stop) * point_value
            if per_contract_risk > 0.0:
                risk_limited = math.floor(risk_budget / per_contract_risk)
                quantity = min(quantity, risk_limited)

        if quantity <= 0:
            raise ValueError(f"configured sizing yields zero contracts for {signal.instrument}")

        tp1_quantity = max(1, quantity // 2)
        remaining_after_tp1 = quantity - tp1_quantity
        tp2_quantity = remaining_after_tp1 // 2
        tp3_quantity = remaining_after_tp1 - tp2_quantity
        return PositionPlan(
            quantity=quantity,
            tp1_quantity=tp1_quantity,
            tp2_quantity=tp2_quantity,
            tp3_quantity=tp3_quantity,
            point_value=point_value,
        )
