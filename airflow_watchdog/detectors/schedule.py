"""
Schedule anomaly detector.

Flags tasks whose most recent start or end time-of-day falls outside the
IQR fence computed from historical runs.  This catches upstream delays,
scheduler lag, and schedule drift — problems that the runtime duration
detector would miss because the task *duration* may be normal even when
the *wall-clock timing* is not.

Handles midnight wraparound: if historical times span midnight (e.g.
23:30–00:30), the detector shifts values before computing IQR.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from airflow_watchdog.config import WatchdogConfig
from airflow_watchdog.detectors import Alert, AlertType, Severity
from airflow_watchdog.detectors._stats import as_datetime, quartiles

logger = logging.getLogger(__name__)

_SQL = text(
    """\
WITH recent_tasks AS (
    SELECT
        dag_id,
        task_id,
        start_date,
        end_date,
        ROW_NUMBER() OVER (
            PARTITION BY dag_id, task_id
            ORDER BY start_date DESC
        ) AS rn
    FROM task_instance
    WHERE state = 'success'
      AND start_date IS NOT NULL
      AND end_date IS NOT NULL
      AND dag_id NOT IN :exclude_dags
)
SELECT dag_id, task_id, start_date, end_date, rn
FROM recent_tasks
WHERE rn <= :lookback
ORDER BY dag_id, task_id, rn
"""
)

# Minutes in a day
_MINUTES_IN_DAY = 1440


def _to_minutes(dt) -> float:
    """Convert a datetime to minutes since midnight (UTC)."""
    dt = as_datetime(dt)
    return dt.hour * 60 + dt.minute + dt.second / 60


def _handle_wraparound(minutes_list: list[float]) -> list[float]:
    """Shift times if they span midnight to allow correct IQR computation.

    If the range exceeds 12 hours, values below 720 are shifted up by 1440
    so that e.g. [1410, 1430, 10, 20] becomes [1410, 1430, 1450, 1460].
    """
    if not minutes_list:
        return minutes_list
    if max(minutes_list) - min(minutes_list) > _MINUTES_IN_DAY / 2:
        return [m + _MINUTES_IN_DAY if m < _MINUTES_IN_DAY / 2 else m for m in minutes_list]
    return minutes_list


def _normalize(minutes: float) -> float:
    """Wrap minutes back into [0, 1440)."""
    return minutes % _MINUTES_IN_DAY


def _fmt_time(minutes: float) -> str:
    """Format minutes-since-midnight as HH:MM."""
    minutes = _normalize(minutes)
    h = int(minutes) // 60
    m = int(minutes) % 60
    return f"{h:02d}:{m:02d}"


def detect(session: Session, config: WatchdogConfig) -> list[Alert]:
    """Return alerts for tasks with anomalous start or end times."""
    alerts: list[Alert] = []

    exclude = list(config.exclude_dags) or ["__none__"]

    try:
        stmt = _SQL.bindparams(bindparam("exclude_dags", expanding=True))
        rows = session.execute(
            stmt,
            {
                "exclude_dags": exclude,
                "lookback": config.lookback_runs,
            },
        ).fetchall()
    except Exception:
        logger.exception("Schedule anomaly query failed")
        return alerts

    # Group by (dag_id, task_id) — collect start/end minutes and track latest
    start_times: dict[tuple[str, str], list[float]] = defaultdict(list)
    end_times: dict[tuple[str, str], list[float]] = defaultdict(list)
    latest_start: dict[tuple[str, str], float] = {}
    latest_end: dict[tuple[str, str], float] = {}

    for row in rows:
        key = (row.dag_id, row.task_id)
        sm = _to_minutes(row.start_date)
        em = _to_minutes(row.end_date)
        start_times[key].append(sm)
        end_times[key].append(em)
        if row.rn == 1:
            latest_start[key] = sm
            latest_end[key] = em

    multiplier = config.schedule_iqr_multiplier
    results: list[tuple[float, Alert]] = []

    for key in start_times:
        dag_id, task_id = key
        if len(start_times[key]) < 5:
            continue

        for label, times, latest_val in [
            ("start", start_times[key], latest_start.get(key)),
            ("end", end_times[key], latest_end.get(key)),
        ]:
            if latest_val is None:
                continue

            shifted = _handle_wraparound(list(times))
            latest_shifted = latest_val
            # Apply same shift to latest if wraparound was detected
            if max(times) - min(times) > _MINUTES_IN_DAY / 2 and latest_val < _MINUTES_IN_DAY / 2:
                latest_shifted = latest_val + _MINUTES_IN_DAY

            q1, med, q3 = quartiles(shifted)
            iqr = q3 - q1
            lower = q1 - multiplier * iqr
            upper = q3 + multiplier * iqr

            if lower <= latest_shifted <= upper:
                continue

            deviation = abs(latest_shifted - med)
            # Ignore sub-minute jitter — a collapsed IQR fence (near-zero
            # historical variance) otherwise flags a task that always starts at
            # the same time as "later/earlier than expected".
            if deviation < config.schedule_min_deviation_minutes:
                continue
            direction = "later" if latest_shifted > upper else "earlier"
            severity = Severity.CRITICAL if deviation > 3 * iqr else Severity.WARNING

            alert = Alert(
                alert_type=AlertType.SCHEDULE_ANOMALY,
                severity=severity,
                dag_id=dag_id,
                task_id=task_id,
                message=(
                    f"Task {label} time {_fmt_time(latest_val)} is {direction} than expected "
                    f"(median {_fmt_time(med)}, "
                    f"fence [{_fmt_time(lower)}, {_fmt_time(upper)}])"
                ),
                details={
                    "type": label,
                    "latest_time": _fmt_time(latest_val),
                    "median_time": _fmt_time(med),
                    "lower_fence": _fmt_time(lower),
                    "upper_fence": _fmt_time(upper),
                    "iqr_minutes": round(iqr, 1),
                    "sample_size": len(times),
                },
            )
            results.append((deviation, alert))

    results.sort(key=lambda x: x[0], reverse=True)
    alerts = [a for _, a in results]

    logger.info("Schedule anomaly detector found %d alerts", len(alerts))
    return alerts
