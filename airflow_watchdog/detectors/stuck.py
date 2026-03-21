"""
Stuck task detector.

Flags task instances currently in ``running`` state that have been running
longer than ``stuck_multiplier * historical_max_duration`` for that
(dag_id, task_id) combination.

This catches zombie tasks, hung database queries, tasks waiting on
unresponsive external services, etc.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from airflow_watchdog.config import WatchdogConfig
from airflow_watchdog.detectors import Alert, AlertType, Severity
from airflow_watchdog.detectors._stats import ensure_tz, median
from airflow_watchdog.detectors._stats import fmt_duration as _fmt

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# SQL: two queries for DB portability — avoids PostgreSQL-specific
# EXTRACT(EPOCH FROM ...), PERCENTILE_CONT, and NOW().
# ──────────────────────────────────────────────────────────────────────────────

_HISTORY_SQL = text(
    """\
WITH ranked AS (
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
SELECT dag_id, task_id, duration
FROM ranked
WHERE rn <= :lookback
ORDER BY dag_id, task_id
"""
)

_RUNNING_SQL = text(
    """\
SELECT dag_id, task_id, run_id, start_date
FROM task_instance
WHERE state = 'running'
  AND start_date IS NOT NULL
  AND dag_id NOT IN :exclude_dags
"""
)


def detect(session: Session, config: WatchdogConfig) -> list[Alert]:
    """Return alerts for tasks stuck in running state."""
    alerts: list[Alert] = []

    exclude = list(config.exclude_dags) or ["__none__"]
    params = {"exclude_dags": exclude, "lookback": config.lookback_runs}

    try:
        hist_stmt = _HISTORY_SQL.bindparams(bindparam("exclude_dags", expanding=True))
        hist_rows = session.execute(hist_stmt, params).fetchall()

        run_stmt = _RUNNING_SQL.bindparams(bindparam("exclude_dags", expanding=True))
        run_rows = session.execute(run_stmt, {"exclude_dags": exclude}).fetchall()
    except Exception:
        logger.exception("Stuck task query failed")
        return alerts

    # Compute historical stats per (dag_id, task_id)
    durations_by_task: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in hist_rows:
        durations_by_task[(row.dag_id, row.task_id)].append(row.duration)

    task_stats: dict[tuple[str, str], tuple[float, float]] = {}  # -> (max, median)
    for key, durations in durations_by_task.items():
        if len(durations) < 3:
            continue
        task_stats[key] = (max(durations), median(durations))

    # Check running tasks against stuck threshold
    now = datetime.now(timezone.utc)
    results: list[tuple[float, Alert]] = []

    for row in run_rows:
        key = (row.dag_id, row.task_id)
        stats = task_stats.get(key)
        if stats is None:
            continue

        max_duration, median_duration = stats
        elapsed_secs = (now - ensure_tz(row.start_date)).total_seconds()
        stuck_threshold = config.stuck_multiplier * max_duration

        if elapsed_secs <= stuck_threshold:
            continue

        ratio = elapsed_secs / max_duration if max_duration else 0

        alert = Alert(
            alert_type=AlertType.STUCK_TASK,
            severity=Severity.CRITICAL,  # stuck tasks are always critical
            dag_id=row.dag_id,
            task_id=row.task_id,
            message=(
                f"Task running for {_fmt(elapsed_secs)}, "
                f"exceeds {config.stuck_multiplier}\u00d7 "
                f"historical max ({_fmt(max_duration)}). "
                f"Possibly stuck or zombie."
            ),
            details={
                "run_id": row.run_id,
                "elapsed_secs": elapsed_secs,
                "max_duration": max_duration,
                "median_duration": median_duration,
                "stuck_threshold": stuck_threshold,
                "ratio": ratio,
            },
        )
        results.append((ratio, alert))

    # Sort by ratio descending (most stuck first)
    results.sort(key=lambda x: x[0], reverse=True)
    alerts = [a for _, a in results]

    logger.info("Stuck task detector found %d alerts", len(alerts))
    return alerts
