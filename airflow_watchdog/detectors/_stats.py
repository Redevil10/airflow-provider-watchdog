"""Shared statistics helpers for detectors — replaces PostgreSQL-specific PERCENTILE_CONT."""

from __future__ import annotations

from statistics import median as _median
from statistics import quantiles as _quantiles


def quartiles(data: list[float]) -> tuple[float, float, float]:
    """Return (Q1, median, Q3) for *data* (must have >= 2 elements)."""
    q1, med, q3 = _quantiles(data, n=4)
    return q1, med, q3


def median(data: list[float]) -> float:
    """Return the median of *data*."""
    return _median(data)
