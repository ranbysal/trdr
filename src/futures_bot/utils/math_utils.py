"""Deterministic mathematical helper functions."""

from __future__ import annotations

import math

import numpy as np


def clip01(x: float) -> float:
    """Clamp a numeric input into the inclusive [0, 1] interval."""
    return float(min(1.0, max(0.0, x)))


def safe_div(num: float, den: float, default: float = 0.0) -> float:
    """Divide `num` by `den`, returning `default` for non-finite or zero denominator."""
    if den == 0.0 or not math.isfinite(den):
        return float(default)
    result = num / den
    if not math.isfinite(result):
        return float(default)
    return float(result)


def percentile_rank(value: float, series: np.ndarray) -> float:
    """Return percentile rank of `value` within `series` as [0, 1]."""
    arr = np.asarray(series, dtype=float)
    finite = arr[np.isfinite(arr)]
    if finite.size == 0:
        return 0.0
    less_or_equal = np.count_nonzero(finite <= value)
    return float(less_or_equal / finite.size)
