"""Shared statistics helpers for detectors — replaces PostgreSQL-specific PERCENTILE_CONT."""

from __future__ import annotations

from datetime import datetime, timezone
from statistics import median as _median
from statistics import quantiles as _quantiles


def quartiles(data: list[float]) -> tuple[float, float, float]:
    """Return (Q1, median, Q3) for *data* (must have >= 2 elements)."""
    q1, med, q3 = _quantiles(data, n=4)
    return q1, med, q3


def median(data: list[float]) -> float:
    """Return the median of *data*."""
    return _median(data)


def ensure_tz(dt: datetime) -> datetime:
    """Return *dt* as a timezone-aware datetime in UTC.

    Airflow stores all metadata timestamps in UTC, but some database backends
    (e.g. SQLite) return naive datetimes. A naive value is therefore interpreted
    as UTC — *not* the configured ``airflow.settings.TIMEZONE``, which would
    misread a UTC instant whenever the deployment runs in a non-UTC timezone.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def as_datetime(value: datetime | str) -> datetime:
    """Coerce a raw SQL timestamp to a tz-aware UTC datetime.

    Raw ``text()`` queries bypass SQLAlchemy's type handling, so SQLite returns
    timestamps as ISO strings while PostgreSQL/MySQL return ``datetime`` objects.
    This normalizes both to a tz-aware UTC datetime so duration arithmetic works
    on every supported backend.
    """
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return ensure_tz(value)


def fmt_duration(secs: float) -> str:
    """Format a duration in seconds to a human-readable string."""
    if secs < 60:
        return f"{secs:.0f}s"
    if secs < 3600:
        return f"{secs / 60:.1f}m"
    return f"{secs / 3600:.1f}h"
