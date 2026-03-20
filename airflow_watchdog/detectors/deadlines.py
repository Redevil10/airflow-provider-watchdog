"""
Missed deadline detector.

Flags DAG runs that are currently in ``running`` state and have been running
longer than ``deadline_multiplier × historical_median_duration``.

This catches DAGs that are "stuck" at the DAG level — perhaps waiting on
an external dependency or running an unexpectedly slow task.
"""

from __future__ import annotations

import logging

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from airflow_watchdog.config import WatchdogConfig
from airflow_watchdog.detectors import Alert, AlertType, Severity

logger = logging.getLogger(__name__)

_SQL = text(
    """\
WITH completed_runs AS (
    SELECT
        dag_id,
        EXTRACT(EPOCH FROM (end_date - start_date)) AS duration_secs,
        ROW_NUMBER() OVER (
            PARTITION BY dag_id
            ORDER BY start_date DESC
        ) AS rn
    FROM dag_run
    WHERE state = 'success'
      AND end_date IS NOT NULL
      AND start_date IS NOT NULL
      AND dag_id NOT IN :exclude_dags
),
dag_stats AS (
    SELECT
        dag_id,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_secs) AS median_duration,
        MAX(duration_secs) AS max_duration,
        COUNT(*) AS sample_size
    FROM completed_runs
    WHERE rn <= :lookback
    GROUP BY dag_id
    HAVING COUNT(*) >= 3
),
running_now AS (
    SELECT
        dag_id,
        run_id,
        start_date,
        EXTRACT(EPOCH FROM (NOW() - start_date)) AS elapsed_secs
    FROM dag_run
    WHERE state = 'running'
      AND start_date IS NOT NULL
      AND dag_id NOT IN :exclude_dags
)
SELECT
    r.dag_id,
    r.run_id,
    r.elapsed_secs,
    s.median_duration,
    s.max_duration,
    s.sample_size,
    :multiplier * s.median_duration AS deadline_secs
FROM running_now r
JOIN dag_stats s ON r.dag_id = s.dag_id
WHERE r.elapsed_secs > :multiplier * s.median_duration
ORDER BY (r.elapsed_secs / NULLIF(s.median_duration, 0)) DESC
"""
)


def detect(session: Session, config: WatchdogConfig) -> list[Alert]:
    """Return alerts for DAG runs that have exceeded their expected duration."""
    alerts: list[Alert] = []

    exclude = list(config.exclude_dags) or ["__none__"]

    try:
        stmt = _SQL.bindparams(bindparam("exclude_dags", expanding=True))
        rows = session.execute(
            stmt,
            {
                "exclude_dags": exclude,
                "lookback": config.lookback_runs,
                "multiplier": config.deadline_multiplier,
            },
        ).fetchall()
    except Exception:
        logger.exception("Missed deadline query failed")
        return alerts

    def _fmt(secs: float) -> str:
        if secs < 60:
            return f"{secs:.0f}s"
        if secs < 3600:
            return f"{secs / 60:.1f}m"
        return f"{secs / 3600:.1f}h"

    for row in rows:
        ratio = row.elapsed_secs / row.median_duration if row.median_duration else 0
        severity = Severity.CRITICAL if ratio > 3 else Severity.WARNING

        alerts.append(
            Alert(
                alert_type=AlertType.MISSED_DEADLINE,
                severity=severity,
                dag_id=row.dag_id,
                message=(
                    f"Run '{row.run_id}' has been running for {_fmt(row.elapsed_secs)}, "
                    f"exceeding deadline of {_fmt(row.deadline_secs)} "
                    f"({ratio:.1f}× median {_fmt(row.median_duration)})"
                ),
                details={
                    "run_id": row.run_id,
                    "elapsed_secs": row.elapsed_secs,
                    "median_duration": row.median_duration,
                    "max_duration": row.max_duration,
                    "deadline_secs": row.deadline_secs,
                    "ratio": ratio,
                },
            )
        )

    logger.info("Deadline detector found %d alerts", len(alerts))
    return alerts
