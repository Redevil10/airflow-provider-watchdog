"""
Failure spike detector.

Compares the failure rate in a recent window of runs against a longer
historical baseline.  Fires when the recent rate exceeds
``failure_spike_ratio × baseline_rate``.

Example: if baseline failure rate is 5 % and recent window shows 15 %,
with a spike ratio of 2.0, this triggers because 15 % > 2.0 × 5 % = 10 %.
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
    HAVING COUNT(*) >= 3  -- need minimum sample
),
baseline AS (
    SELECT
        dag_id,
        COUNT(*) AS total,
        SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) AS failures
    FROM numbered_runs
    WHERE rn <= :baseline_runs
    GROUP BY dag_id
    HAVING COUNT(*) >= 10
)
SELECT
    r.dag_id,
    r.failures AS recent_failures,
    r.total AS recent_total,
    ROUND(r.failures::numeric / r.total, 4) AS recent_rate,
    b.failures AS baseline_failures,
    b.total AS baseline_total,
    ROUND(b.failures::numeric / b.total, 4) AS baseline_rate
FROM recent r
JOIN baseline b ON r.dag_id = b.dag_id
WHERE b.failures > 0  -- baseline has some failures
  AND (r.failures::numeric / r.total) > :spike_ratio * (b.failures::numeric / b.total)

UNION ALL

-- Also flag DAGs with zero historical failures that now have failures
SELECT
    r.dag_id,
    r.failures AS recent_failures,
    r.total AS recent_total,
    ROUND(r.failures::numeric / r.total, 4) AS recent_rate,
    b.failures AS baseline_failures,
    b.total AS baseline_total,
    0.0 AS baseline_rate
FROM recent r
JOIN baseline b ON r.dag_id = b.dag_id
WHERE b.failures = 0
  AND r.failures > 0

ORDER BY recent_rate DESC
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
                "spike_ratio": config.failure_spike_ratio,
            },
        ).fetchall()
    except Exception:
        logger.exception("Failure spike query failed")
        return alerts

    for row in rows:
        # Zero-baseline failures are always critical
        severity = (
            Severity.CRITICAL
            if row.baseline_rate == 0 or row.recent_rate > 0.5
            else Severity.WARNING
        )

        alerts.append(
            Alert(
                alert_type=AlertType.FAILURE_SPIKE,
                severity=severity,
                dag_id=row.dag_id,
                message=(
                    f"Failure rate {row.recent_rate:.1%} in last {row.recent_total} runs "
                    f"(baseline {row.baseline_rate:.1%} over {row.baseline_total} runs) — "
                    f"{row.recent_failures}/{row.recent_total} recent runs failed"
                ),
                details={
                    "recent_failures": row.recent_failures,
                    "recent_total": row.recent_total,
                    "recent_rate": float(row.recent_rate),
                    "baseline_failures": row.baseline_failures,
                    "baseline_total": row.baseline_total,
                    "baseline_rate": float(row.baseline_rate),
                },
            )
        )

    logger.info("Failure spike detector found %d alerts", len(alerts))
    return alerts
