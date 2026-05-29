"""
Failure spike detector.

Compares the failure rate in a recent window of runs against the failure
rate over the *preceding* historical baseline (the baseline excludes the
recent window so a fresh spike does not dilute its own reference point).
Fires when the recent rate exceeds ``failure_spike_ratio * baseline_rate``.

Example: if the prior baseline failure rate is 5 % and the recent window
shows 15 %, with a spike ratio of 2.0, this triggers because
15 % > 2.0 * 5 % = 10 %.
"""

from __future__ import annotations

import logging

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from airflow_watchdog.config import WatchdogConfig
from airflow_watchdog.detectors import Alert, AlertType, Severity

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# SQL: fetch per-DAG failure counts for the recent window and baseline.
# Rates and spike comparison are computed in Python for DB portability
# (avoids PostgreSQL-specific ``::numeric`` cast).
# ──────────────────────────────────────────────────────────────────────────────

_SQL = text(
    """\
WITH numbered_runs AS (
    SELECT
        dag_id,
        state,
        ROW_NUMBER() OVER (
            PARTITION BY dag_id
            ORDER BY start_date DESC
        ) AS rn
    FROM dag_run
    WHERE dag_id NOT IN :exclude_dags
      AND state IN ('success', 'failed')
),
recent AS (
    SELECT
        dag_id,
        COUNT(*) AS total,
        SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) AS failures
    FROM numbered_runs
    WHERE rn <= :window
    GROUP BY dag_id
    HAVING COUNT(*) >= 3
),
baseline AS (
    -- Prior history *excluding* the recent window, so a fresh wave of
    -- failures does not dilute the baseline it is being compared against.
    SELECT
        dag_id,
        COUNT(*) AS total,
        SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) AS failures
    FROM numbered_runs
    WHERE rn > :window AND rn <= :baseline_runs + :window
    GROUP BY dag_id
    HAVING COUNT(*) >= 10
)
SELECT
    r.dag_id,
    r.failures AS recent_failures,
    r.total AS recent_total,
    b.failures AS baseline_failures,
    b.total AS baseline_total
FROM recent r
JOIN baseline b ON r.dag_id = b.dag_id
"""
)


def detect(session: Session, config: WatchdogConfig) -> list[Alert]:
    """Return alerts for DAGs with failure rate spikes."""
    alerts: list[Alert] = []

    exclude = list(config.exclude_dags) or ["__none__"]

    try:
        stmt = _SQL.bindparams(bindparam("exclude_dags", expanding=True))
        rows = session.execute(
            stmt,
            {
                "exclude_dags": exclude,
                "window": config.failure_window_runs,
                "baseline_runs": config.failure_baseline_runs,
            },
        ).fetchall()
    except Exception:
        logger.exception("Failure spike query failed")
        return alerts

    results: list[tuple[float, Alert]] = []  # (recent_rate, alert) for sorting

    for row in rows:
        recent_rate = row.recent_failures / row.recent_total
        baseline_rate = row.baseline_failures / row.baseline_total if row.baseline_total else 0.0

        # Check spike condition
        is_spike = baseline_rate > 0 and recent_rate > config.failure_spike_ratio * baseline_rate
        is_new_failures = baseline_rate == 0 and row.recent_failures > 0

        if not is_spike and not is_new_failures:
            continue

        severity = (
            Severity.CRITICAL if baseline_rate == 0 or recent_rate > 0.5 else Severity.WARNING
        )

        alert = Alert(
            alert_type=AlertType.FAILURE_SPIKE,
            severity=severity,
            dag_id=row.dag_id,
            message=(
                f"Failure rate {recent_rate:.1%} in last {row.recent_total} runs "
                f"(baseline {baseline_rate:.1%} over {row.baseline_total} runs) — "
                f"{row.recent_failures}/{row.recent_total} recent runs failed"
            ),
            details={
                "recent_failures": row.recent_failures,
                "recent_total": row.recent_total,
                "recent_rate": round(recent_rate, 4),
                "baseline_failures": row.baseline_failures,
                "baseline_total": row.baseline_total,
                "baseline_rate": round(baseline_rate, 4),
            },
        )
        results.append((recent_rate, alert))

    # Sort by recent failure rate descending
    results.sort(key=lambda x: x[0], reverse=True)
    alerts = [a for _, a in results]

    logger.info("Failure spike detector found %d alerts", len(alerts))
    return alerts
