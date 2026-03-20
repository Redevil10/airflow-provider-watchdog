"""
Runtime anomaly detector.

Flags tasks whose most recent duration falls outside the IQR fence:
    lower = Q1 - multiplier × IQR
    upper = Q3 + multiplier × IQR

Queries only the ``task_instance`` table — no external dependencies.
"""

from __future__ import annotations

import logging

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from airflow_watchdog.config import WatchdogConfig
from airflow_watchdog.detectors import Alert, AlertType, Severity

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# SQL: for each (dag_id, task_id), compute IQR stats over the last N successful
# runs and compare the most recent duration.
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
),
stats AS (
    SELECT
        dag_id,
        task_id,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY duration) AS q1,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY duration) AS q3,
        COUNT(*) AS sample_size
    FROM recent_tasks
    WHERE rn <= :lookback
    GROUP BY dag_id, task_id
    HAVING COUNT(*) >= 5  -- need minimum sample
),
latest AS (
    SELECT dag_id, task_id, duration AS latest_duration
    FROM recent_tasks
    WHERE rn = 1
)
SELECT
    s.dag_id,
    s.task_id,
    s.q1,
    s.median,
    s.q3,
    s.sample_size,
    l.latest_duration,
    (s.q3 - s.q1) AS iqr,
    s.q1 - :multiplier * (s.q3 - s.q1) AS lower_fence,
    s.q3 + :multiplier * (s.q3 - s.q1) AS upper_fence
FROM stats s
JOIN latest l ON s.dag_id = l.dag_id AND s.task_id = l.task_id
WHERE l.latest_duration < s.q1 - :multiplier * (s.q3 - s.q1)
   OR l.latest_duration > s.q3 + :multiplier * (s.q3 - s.q1)
ORDER BY ABS(l.latest_duration - s.median) DESC
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
                "multiplier": config.runtime_iqr_multiplier,
            },
        ).fetchall()
    except Exception:
        logger.exception("Runtime anomaly query failed")
        return alerts

    for row in rows:
        direction = "slower" if row.latest_duration > row.upper_fence else "faster"
        severity = (
            Severity.CRITICAL
            if abs(row.latest_duration - row.median) > 3 * row.iqr
            else Severity.WARNING
        )

        alerts.append(
            Alert(
                alert_type=AlertType.RUNTIME_ANOMALY,
                severity=severity,
                dag_id=row.dag_id,
                task_id=row.task_id,
                message=(
                    f"Latest run {row.latest_duration:.1f}s is {direction} than expected "
                    f"(median {row.median:.1f}s, IQR fence "
                    f"[{row.lower_fence:.1f}s, {row.upper_fence:.1f}s])"
                ),
                details={
                    "latest_duration": row.latest_duration,
                    "median": row.median,
                    "q1": row.q1,
                    "q3": row.q3,
                    "iqr": row.iqr,
                    "lower_fence": row.lower_fence,
                    "upper_fence": row.upper_fence,
                    "sample_size": row.sample_size,
                },
            )
        )

    logger.info("Runtime detector found %d anomalies", len(alerts))
    return alerts
