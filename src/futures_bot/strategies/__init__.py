"""Strategy package."""

from futures_bot.strategies.strategy_a_models import (
    ORSessionState,
    StrategyAEntryPlan,
    StrategyAEvaluation,
    StrategyAFeatureSnapshot,
)
from futures_bot.strategies.strategy_a_orb import StrategyAORB
from futures_bot.strategies.strategy_b_models import StrategyBEvaluation, StrategyBFeatureSnapshot
from futures_bot.strategies.strategy_b_vwap_rev import StrategyBVWAPReversion
from futures_bot.strategies.strategy_c_metals_orb import StrategyCMetalsORB
from futures_bot.strategies.strategy_c_models import (
    MetalsORSessionState,
    StrategyCEntryPlan,
    StrategyCEvaluation,
    StrategyCFeatureSnapshot,
)
from futures_bot.strategies.strategy_d_pair import (
    PairSignal,
    ar1_half_life,
    evaluate_pair_signal,
    ewls_beta,
    fit_ar1_phi,
    spread_zscore,
)

__all__ = [
    "ORSessionState",
    "StrategyAEntryPlan",
    "StrategyAEvaluation",
    "StrategyAFeatureSnapshot",
    "StrategyAORB",
    "StrategyBEvaluation",
    "StrategyBFeatureSnapshot",
    "StrategyBVWAPReversion",
    "MetalsORSessionState",
    "StrategyCEntryPlan",
    "StrategyCEvaluation",
    "StrategyCFeatureSnapshot",
    "StrategyCMetalsORB",
    "PairSignal",
    "ar1_half_life",
    "evaluate_pair_signal",
    "ewls_beta",
    "fit_ar1_phi",
    "spread_zscore",
]
