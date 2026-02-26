"""History readiness helpers for same-bucket feature availability."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Readiness:
    sample_count: int
    state: str
    is_available: bool
    is_partial: bool


def classify_sample_count(sample_count: int) -> Readiness:
    """Classify readiness from same-bucket sample count."""
    if sample_count < 5:
        return Readiness(
            sample_count=sample_count,
            state="unavailable",
            is_available=False,
            is_partial=False,
        )
    if sample_count < 20:
        return Readiness(
            sample_count=sample_count,
            state="partial",
            is_available=True,
            is_partial=True,
        )
    return Readiness(
        sample_count=sample_count,
        state="ready",
        is_available=True,
        is_partial=False,
    )
