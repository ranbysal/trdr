"""Bot 3 paper execution companion for Prop V2."""

from bot_exec_v3.executor import PaperExecutor
from bot_exec_v3.journal import PaperTradeJournal
from bot_exec_v3.models import ExecutorConfig, MarketBar, SignalEvent, SubmitSignalResult
from bot_exec_v3.risk import PaperRiskSizer

__all__ = [
    "ExecutorConfig",
    "MarketBar",
    "PaperExecutor",
    "PaperRiskSizer",
    "PaperTradeJournal",
    "SignalEvent",
    "SubmitSignalResult",
]
