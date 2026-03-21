"""
Missed deadline detector.

Flags DAG runs that are currently in ``running`` state and have been running
longer than ``deadline_multiplier * historical_median_duration``.

This catches DAGs that are "stuck" at the DAG level — perhaps waiting on
an external dependency or running an unexpectedly slow task.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from airflow_watchdog.config import WatchdogConfig
from airflow_watchdog.detectors import Alert, AlertType, Severity
from airflow_watchdog.detectors._stats import fmt_duration as _fmt
from airflow_watchdog.detectors._stats import median

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# SQL: two queries for DB portability — avoids PostgreSQL-specific
# EXTRACT(EPOCH FROM ...), PERCENTILE_CONT, and NOW().
# ──────────────────────────────────────────────────────────────────────────────

_HISTORY_SQL = text(
    """\
WITH completed_runs AS (
    SELECT
        dag_id,
        start_date,
        end_date,
        ROW_NUMBER() OVER (
            PARTITION BY dag_id
            ORDER BY start_date DESC
        ) AS rn
    FROM dag_run
    WHERE state = 'success'
      AND end_date IS NOT NULL
      AND start_date IS NOT NULL
      AND dag_id NOT IN :exclude_dags
)
SELECT dag_id, start_date, end_date
FROM completed_runs
WHERE rn <= :lookback
ORDER BY dag_id
"""
)

_RUNNING_SQL = text(
    """\
SELECT dag_id, run_id, start_date
FROM dag_run
WHERE state = 'running'
  AND start_date IS NOT NULL
  AND dag_id NOT IN :exclude_dags
"""
)


def detect(session: Session, config: WatchdogConfig) -> list[Alert]:
    """Return alerts for DAG runs that have exceeded their expected duration."""
    alerts: list[Alert] = []

    exclude = list(config.exclude_dags) or ["__none__"]
    params = {"exclude_dags": exclude, "lookback": config.lookback_runs}

    try:
        hist_stmt = _HISTORY_SQL.bindparams(bindparam("exclude_dags", expanding=True))
        hist_rows = session.execute(hist_stmt, params).fetchall()

        run_stmt = _RUNNING_SQL.bindparams(bindparam("exclude_dags", expanding=True))
        run_rows = session.execute(run_stmt, {"exclude_dags": exclude}).fetchall()
    except Exception:
        logger.exception("Missed deadline query failed")
        return alerts

    # Compute historical durations per dag_id
    durations_by_dag: dict[str, list[float]] = defaultdict(list)
    for row in hist_rows:
        secs = (row.end_date - row.start_date).total_seconds()
        durations_by_dag[row.dag_id].append(secs)

    # Compute stats per dag_id
    dag_stats: dict[str, tuple[float, float]] = {}  # dag_id -> (median, max)
    for dag_id, durations in durations_by_dag.items():
        if len(durations) < 3:
            continue
        dag_stats[dag_id] = (median(durations), max(durations))

    # Check running DAGs against deadlines
    now = datetime.now(timezone.utc)
    results: list[tuple[float, Alert]] = []

    for row in run_rows:
        stats = dag_stats.get(row.dag_id)
        if stats is None:
            continue

        median_duration, max_duration = stats
        elapsed_secs = (now - row.start_date).total_seconds()
        deadline_secs = config.deadline_multiplier * median_duration

        if elapsed_secs <= deadline_secs:
            continue

        ratio = elapsed_secs / median_duration if median_duration else 0
        severity = Severity.CRITICAL if ratio > 3 else Severity.WARNING

        alert = Alert(
            alert_type=AlertType.MISSED_DEADLINE,
            severity=severity,
            dag_id=row.dag_id,
            message=(
                f"Run '{row.run_id}' has been running for {_fmt(elapsed_secs)}, "
                f"exceeding deadline of {_fmt(deadline_secs)} "
                f"({ratio:.1f}\u00d7 median {_fmt(median_duration)})"
            ),
            details={
                "run_id": row.run_id,
                "elapsed_secs": elapsed_secs,
                "median_duration": median_duration,
                "max_duration": max_duration,
                "deadline_secs": deadline_secs,
                "ratio": ratio,
            },
        )
        results.append((ratio, alert))

    # Sort by ratio descending (most overdue first)
    results.sort(key=lambda x: x[0], reverse=True)
    alerts = [a for _, a in results]

    logger.info("Deadline detector found %d alerts", len(alerts))
    return alerts
