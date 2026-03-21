"""Shared statistics helpers for detectors — replaces PostgreSQL-specific PERCENTILE_CONT."""

from __future__ import annotations

from statistics import median as _median
from statistics import quantiles as _quantiles


def quartiles(data: list[float]) -> tuple[float, float, float]:
    """Return (Q1, median, Q3) for *data* (must have >= 3 elements)."""
    q1, med, q3 = _quantiles(data, n=4)
    return q1, med, q3


def median(data: list[float]) -> float:
    """Return the median of *data*."""
    return _median(data)


def fmt_duration(secs: float) -> str:
    """Format a duration in seconds to a human-readable string."""
    if secs < 60:
        return f"{secs:.0f}s"
    if secs < 3600:
        return f"{secs / 60:.1f}m"
    return f"{secs / 3600:.1f}h"
