"""Strategy package."""

from futures_bot.strategies.gold_models import (
    GoldSignalEvaluation,
    GoldSignalFeatures,
    GoldSignalRejection,
    GoldSignalResult,
    GoldSignalSetup,
)
from futures_bot.strategies.gold_signal import GoldSignalStrategy
from futures_bot.strategies.nq_models import (
    NQSignalEvaluation,
    NQSignalFeatures,
    NQSignalRejection,
    NQSignalResult,
    NQSignalSetup,
)
from futures_bot.strategies.nq_signal import NQSignalStrategy
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
from futures_bot.strategies.ym_models import (
    YMSignalEvaluation,
    YMSignalFeatures,
    YMSignalRejection,
    YMSignalResult,
    YMSignalSetup,
)
from futures_bot.strategies.ym_signal import YMSignalStrategy

__all__ = [
    "GoldSignalEvaluation",
    "GoldSignalFeatures",
    "GoldSignalRejection",
    "GoldSignalResult",
    "GoldSignalSetup",
    "GoldSignalStrategy",
    "NQSignalEvaluation",
    "NQSignalFeatures",
    "NQSignalRejection",
    "NQSignalResult",
    "NQSignalSetup",
    "NQSignalStrategy",
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
    "YMSignalEvaluation",
    "YMSignalFeatures",
    "YMSignalRejection",
    "YMSignalResult",
    "YMSignalSetup",
    "YMSignalStrategy",
]
