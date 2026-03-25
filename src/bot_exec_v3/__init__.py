"""Bot 3 paper execution companion for Prop V2."""

from bot_exec_v3.executor import PaperExecutor
from bot_exec_v3.journal import PaperTradeJournal
from bot_exec_v3.live import ExecutorV3LiveRunner
from bot_exec_v3.models import ExecutorConfig, MarketBar, SignalEvent, SubmitSignalResult, build_signal_id
from bot_exec_v3.risk import PaperRiskSizer

__all__ = [
    "ExecutorConfig",
    "ExecutorV3LiveRunner",
    "MarketBar",
    "PaperExecutor",
    "PaperRiskSizer",
    "PaperTradeJournal",
    "SignalEvent",
    "SubmitSignalResult",
    "build_signal_id",
]
