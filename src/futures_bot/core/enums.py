"""Core enums for deterministic bot state and orchestration."""

from __future__ import annotations

from enum import Enum


class Family(str, Enum):
    """Supported instrument families."""

    EQUITIES = "equities"
    METALS = "metals"


class StrategyModule(str, Enum):
    """Strategy module identifiers."""

    STRAT_A_ORB = "strat_a_orb"
    STRAT_B_VWAP_REV = "strat_b_vwap_rev"
    STRAT_C_METALS_ORB = "strat_c_metals_orb"
    STRAT_D_PAIR = "strat_d_pair"


class Regime(str, Enum):
    """High-level market regime states."""

    TREND = "trend"
    CHOP = "chop"
    NEUTRAL = "neutral"


class OrderSide(str, Enum):
    """Order direction."""

    BUY = "buy"
    SELL = "sell"


class ApprovalStatus(str, Enum):
    """Risk approval status."""

    APPROVED = "approved"
    REJECTED = "rejected"
