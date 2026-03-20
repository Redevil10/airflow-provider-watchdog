"""
Stuck task detector.

Flags task instances currently in ``running`` state that have been running
longer than ``stuck_multiplier × historical_max_duration`` for that
(dag_id, task_id) combination.

This catches zombie tasks, hung database queries, tasks waiting on
unresponsive external services, etc.
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
WITH historical AS (
    SELECT
        dag_id,
        task_id,
        MAX(duration) AS max_duration,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration) AS median_duration,
        COUNT(*) AS sample_size
    FROM (
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
    ) ranked
    WHERE rn <= :lookback
    GROUP BY dag_id, task_id
    HAVING COUNT(*) >= 3
),
running_now AS (
    SELECT
        dag_id,
        task_id,
        run_id,
        start_date,
        EXTRACT(EPOCH FROM (NOW() - start_date)) AS elapsed_secs
    FROM task_instance
    WHERE state = 'running'
      AND start_date IS NOT NULL
      AND dag_id NOT IN :exclude_dags
)
SELECT
    r.dag_id,
    r.task_id,
    r.run_id,
    r.elapsed_secs,
    h.max_duration,
    h.median_duration,
    h.sample_size,
    :multiplier * h.max_duration AS stuck_threshold
FROM running_now r
JOIN historical h ON r.dag_id = h.dag_id AND r.task_id = h.task_id
WHERE r.elapsed_secs > :multiplier * h.max_duration
ORDER BY (r.elapsed_secs / NULLIF(h.max_duration, 0)) DESC
"""
)


def detect(session: Session, config: WatchdogConfig) -> list[Alert]:
    """Return alerts for tasks stuck in running state."""
    alerts: list[Alert] = []

    exclude = list(config.exclude_dags) or ["__none__"]

    try:
        stmt = _SQL.bindparams(bindparam("exclude_dags", expanding=True))
        rows = session.execute(
            stmt,
            {
                "exclude_dags": exclude,
                "lookback": config.lookback_runs,
                "multiplier": config.stuck_multiplier,
            },
        ).fetchall()
    except Exception:
        logger.exception("Stuck task query failed")
        return alerts

    def _fmt(secs: float) -> str:
        if secs < 60:
            return f"{secs:.0f}s"
        if secs < 3600:
            return f"{secs / 60:.1f}m"
        return f"{secs / 3600:.1f}h"

    for row in rows:
        ratio = row.elapsed_secs / row.max_duration if row.max_duration else 0

        alerts.append(
            Alert(
                alert_type=AlertType.STUCK_TASK,
                severity=Severity.CRITICAL,  # stuck tasks are always critical
                dag_id=row.dag_id,
                task_id=row.task_id,
                message=(
                    f"Task running for {_fmt(row.elapsed_secs)}, "
                    f"exceeds {config.stuck_multiplier}× "
                    f"historical max ({_fmt(row.max_duration)}). "
                    f"Possibly stuck or zombie."
                ),
                details={
                    "run_id": row.run_id,
                    "elapsed_secs": row.elapsed_secs,
                    "max_duration": row.max_duration,
                    "median_duration": row.median_duration,
                    "stuck_threshold": row.stuck_threshold,
                    "ratio": ratio,
                },
            )
        )

    logger.info("Stuck task detector found %d alerts", len(alerts))
    return alerts
