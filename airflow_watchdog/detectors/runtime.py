"""
Runtime anomaly detector.

Flags tasks whose most recent duration falls outside the IQR fence:
    lower = Q1 - multiplier * IQR
    upper = Q3 + multiplier * IQR

Queries only the ``task_instance`` table — no external dependencies.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from airflow_watchdog.config import WatchdogConfig
from airflow_watchdog.detectors import Alert, AlertType, Severity
from airflow_watchdog.detectors._stats import quartiles

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# SQL: fetch recent successful durations per (dag_id, task_id).
# Stats are computed in Python for DB-engine portability.
# ──────────────────────────────────────────────────────────────────────────────

_SQL = text(
    """\
WITH recent_tasks AS (
    SELECT
        dag_id,
        task_id,
        duration,
        ROW_NUMBER() OVER (
            PARTITION BY dag_id, task_id
            ORDER BY start_date DESC
        ) AS rn
    FROM task_instance
    WHERE state = 'success'
      AND duration IS NOT NULL
      AND dag_id NOT IN :exclude_dags
)
SELECT dag_id, task_id, duration, rn
FROM recent_tasks
WHERE rn <= :lookback
ORDER BY dag_id, task_id, rn
"""
)


def detect(session: Session, config: WatchdogConfig) -> list[Alert]:
    """Return alerts for tasks with anomalous durations."""
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
        logger.exception("Runtime anomaly query failed")
        return alerts

    # Group durations by (dag_id, task_id) and track the latest run
    groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    latest: dict[tuple[str, str], float] = {}

    for row in rows:
        key = (row.dag_id, row.task_id)
        groups[key].append(row.duration)
        if row.rn == 1:
            latest[key] = row.duration

    # Compute IQR fences and check for anomalies
    results: list[tuple[float, Alert]] = []  # (deviation, alert) for sorting

    for (dag_id, task_id), durations in groups.items():
        if len(durations) < 5:
            continue

        latest_duration = latest.get((dag_id, task_id))
        if latest_duration is None:
            continue

        q1, med, q3 = quartiles(durations)
        iqr = q3 - q1
        multiplier = config.runtime_iqr_multiplier
        lower_fence = q1 - multiplier * iqr
        upper_fence = q3 + multiplier * iqr

        if lower_fence <= latest_duration <= upper_fence:
            continue

        deviation = abs(latest_duration - med)
        # Ignore trivial deltas — a collapsed IQR fence (near-zero historical
        # variance) otherwise flags sub-second noise as an anomaly.
        if deviation < config.runtime_min_deviation_secs:
            continue
        direction = "slower" if latest_duration > upper_fence else "faster"
        severity = Severity.CRITICAL if deviation > 3 * iqr else Severity.WARNING

        alert = Alert(
            alert_type=AlertType.RUNTIME_ANOMALY,
            severity=severity,
            dag_id=dag_id,
            task_id=task_id,
            message=(
                f"Latest run {latest_duration:.1f}s is {direction} than expected "
                f"(median {med:.1f}s, IQR fence "
                f"[{lower_fence:.1f}s, {upper_fence:.1f}s])"
            ),
            details={
                "latest_duration": latest_duration,
                "median": med,
                "q1": q1,
                "q3": q3,
                "iqr": iqr,
                "lower_fence": lower_fence,
                "upper_fence": upper_fence,
                "sample_size": len(durations),
            },
        )
        results.append((deviation, alert))

    # Sort by deviation descending (most anomalous first)
    results.sort(key=lambda x: x[0], reverse=True)
    alerts = [a for _, a in results]

    logger.info("Runtime detector found %d anomalies", len(alerts))
    return alerts
