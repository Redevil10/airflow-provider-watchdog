"""
Auto-registered Watchdog monitoring DAG.

This DAG is automatically discovered by Airflow when the provider is installed.
It runs all four detectors on a configurable schedule and dispatches alerts.

Configuration via Airflow Variable ``watchdog_config`` (see config.py).
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

logger = logging.getLogger(__name__)


def _run_watchdog(**context) -> str:
    """Execute all detectors and dispatch alerts."""
    from airflow.settings import Session

    from airflow_watchdog.alerting import dispatch
    from airflow_watchdog.config import load_config
    from airflow_watchdog.detectors import Alert
    from airflow_watchdog.detectors.deadlines import detect as detect_deadlines
    from airflow_watchdog.detectors.failures import detect as detect_failures
    from airflow_watchdog.detectors.runtime import detect as detect_runtime
    from airflow_watchdog.detectors.stuck import detect as detect_stuck

    config = load_config()
    session = Session()

    all_alerts: list[Alert] = []

    detectors = [
        ("runtime", detect_runtime),
        ("failures", detect_failures),
        ("deadlines", detect_deadlines),
        ("stuck", detect_stuck),
    ]

    try:
        for name, detect_fn in detectors:
            try:
                all_alerts.extend(detect_fn(session, config))
            except Exception:
                logger.exception("Detector '%s' failed; continuing with remaining detectors", name)
    finally:
        session.close()

    dispatch(all_alerts, config)

    # Push summary to XCom for the dashboard
    summary = {
        "total_alerts": len(all_alerts),
        "by_type": {},
        "alerts": [],
    }
    for alert in all_alerts:
        t = alert.alert_type.value
        summary["by_type"][t] = summary["by_type"].get(t, 0) + 1
        summary["alerts"].append(
            {
                "type": alert.alert_type.value,
                "severity": alert.severity.value,
                "dag_id": alert.dag_id,
                "task_id": alert.task_id,
                "message": alert.message,
                "detected_at": alert.detected_at.isoformat(),
                "details": alert.details,
            }
        )

    context["ti"].xcom_push(key="watchdog_results", value=json.dumps(summary))

    status = f"Watchdog complete: {len(all_alerts)} alert(s)"
    logger.info(status)
    return status


# ── DAG definition ───────────────────────────────────────────────────────────
# The schedule is read at parse time; changing it requires a webserver restart
# (or a Variable-aware approach).  Default: every 30 minutes.

with DAG(
    dag_id="watchdog_monitor",
    description=(
        "Monitors DAG/task health"
        " — runtime anomalies, failure spikes, missed deadlines, stuck tasks."
    ),
    schedule=timedelta(minutes=30),
    start_date=None,  # Airflow 3: no fixed start_date needed for timedelta schedules
    catchup=False,
    max_active_runs=1,
    tags=["watchdog", "monitoring"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:
    run_watchdog = PythonOperator(
        task_id="run_detectors",
        python_callable=_run_watchdog,
    )
