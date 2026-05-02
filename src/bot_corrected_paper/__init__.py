"""Isolated corrected paper executor for CORR-V4 accepted signals."""

from __future__ import annotations

__all__ = [
    "CorrectedPaperCommandController",
    "CorrectedPaperConfig",
    "CorrectedPaperEngine",
    "load_corrected_paper_config",
]


def __getattr__(name: str) -> object:
    if name == "CorrectedPaperCommandController":
        from bot_corrected_paper.controller import CorrectedPaperCommandController

        return CorrectedPaperCommandController
    if name in {"CorrectedPaperConfig", "load_corrected_paper_config"}:
        from bot_corrected_paper.config import CorrectedPaperConfig, load_corrected_paper_config

        return {"CorrectedPaperConfig": CorrectedPaperConfig, "load_corrected_paper_config": load_corrected_paper_config}[
            name
        ]
    if name == "CorrectedPaperEngine":
        from bot_corrected_paper.engine import CorrectedPaperEngine

        return CorrectedPaperEngine
    raise AttributeError(name)
