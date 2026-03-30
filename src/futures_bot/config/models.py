"""Typed instrument-specific strategy configuration models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import time


@dataclass(frozen=True, slots=True)
class OptionalContextConfig:
    use_choch: bool = True
    require_choch: bool = False
    use_fvg: bool = True
    require_fvg: bool = False

    def __post_init__(self) -> None:
        if self.require_choch:
            raise ValueError("CHoCH must remain optional context, not a hard requirement")
        if self.require_fvg:
            raise ValueError("FVG must remain optional context, not a hard requirement")


@dataclass(frozen=True, slots=True)
class IntermarketConfirmationConfig:
    confirm_with_symbol: str | None = None
    enabled: bool = False
    required: bool = False

    def __post_init__(self) -> None:
        if self.required:
            raise ValueError("Intermarket confirmation must remain optional")
        if self.enabled and self.confirm_with_symbol is None:
            raise ValueError("enabled confirmation requires confirm_with_symbol")


@dataclass(frozen=True, slots=True)
class NQStrategyConfig:
    hard_risk_per_trade_dollars: float
    daily_halt_loss_dollars: float
    symbol: str = "NQ"
    timezone: str = "America/New_York"
    anchor_time: time = time(9, 30)
    context: OptionalContextConfig = field(default_factory=OptionalContextConfig)
    confirmation: IntermarketConfirmationConfig = field(
        default_factory=lambda: IntermarketConfirmationConfig(confirm_with_symbol="YM")
    )

    def __post_init__(self) -> None:
        if self.symbol != "NQ":
            raise ValueError("NQStrategyConfig must remain instrument-specific to NQ")
        _validate_hard_risk(self.hard_risk_per_trade_dollars, self.daily_halt_loss_dollars)


@dataclass(frozen=True, slots=True)
class YMStrategyConfig:
    hard_risk_per_trade_dollars: float
    daily_halt_loss_dollars: float
    symbol: str = "YM"
    timezone: str = "America/New_York"
    anchor_time: time = time(9, 30)
    context: OptionalContextConfig = field(default_factory=OptionalContextConfig)
    confirmation: IntermarketConfirmationConfig = field(
        default_factory=lambda: IntermarketConfirmationConfig(confirm_with_symbol="NQ")
    )

    def __post_init__(self) -> None:
        if self.symbol != "YM":
            raise ValueError("YMStrategyConfig must remain instrument-specific to YM")
        _validate_hard_risk(self.hard_risk_per_trade_dollars, self.daily_halt_loss_dollars)


@dataclass(frozen=True, slots=True)
class GoldStrategyConfig:
    hard_risk_per_trade_dollars: float
    daily_halt_loss_dollars: float
    symbol: str = "GC"
    timezone: str = "America/New_York"
    anchor_time: time = time(8, 0)
    context: OptionalContextConfig = field(default_factory=OptionalContextConfig)
    confirmation: IntermarketConfirmationConfig = field(default_factory=IntermarketConfirmationConfig)

    def __post_init__(self) -> None:
        if self.symbol not in {"GC", "MGC"}:
            raise ValueError("GoldStrategyConfig must remain instrument-specific to GC or MGC")
        _validate_hard_risk(self.hard_risk_per_trade_dollars, self.daily_halt_loss_dollars)


def _validate_hard_risk(hard_risk_per_trade_dollars: float, daily_halt_loss_dollars: float) -> None:
    if hard_risk_per_trade_dollars <= 0.0:
        raise ValueError("hard_risk_per_trade_dollars must be positive")
    if daily_halt_loss_dollars <= 0.0:
        raise ValueError("daily_halt_loss_dollars must be positive")
