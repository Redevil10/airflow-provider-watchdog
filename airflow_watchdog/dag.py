"""
Auto-registered Watchdog monitoring DAG.

This DAG is automatically discovered by Airflow when the provider is installed.
It runs all five detectors on a configurable schedule and dispatches alerts.

Configuration via Airflow Variable ``watchdog_config`` (see config.py).
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

logger = logging.getLogger(__name__)

# Cap the number of alerts serialized into XCom so a pathological run can't
# produce an oversized payload. The most severe alerts are kept.
_MAX_XCOM_ALERTS = 200


def _run_watchdog(**context) -> str:
    """Execute all detectors and dispatch alerts."""
    from airflow.settings import Session

    from airflow_watchdog.alerting import dispatch
    from airflow_watchdog.config import load_config
    from airflow_watchdog.detectors import Alert
    from airflow_watchdog.detectors.deadlines import detect as detect_deadlines
    from airflow_watchdog.detectors.failures import detect as detect_failures
    from airflow_watchdog.detectors.runtime import detect as detect_runtime
    from airflow_watchdog.detectors.schedule import detect as detect_schedule
    from airflow_watchdog.detectors.stuck import detect as detect_stuck

    config = load_config()
    session = Session()

    all_alerts: list[Alert] = []

    detectors = [
        ("runtime_anomaly", detect_runtime),
        ("failure_spike", detect_failures),
        ("missed_deadline", detect_deadlines),
        ("stuck_task", detect_stuck),
        ("schedule_anomaly", detect_schedule),
    ]

    try:
        for name, detect_fn in detectors:
            # Skip globally-disabled detectors up front to avoid the DB work.
            # Per-DAG overrides are applied to the produced alerts below.
            if name in config.disable_detectors:
                logger.info("Detector '%s' is globally disabled; skipping", name)
                continue
            try:
                all_alerts.extend(detect_fn(session, config))
            except Exception:
                logger.exception("Detector '%s' failed; continuing with remaining detectors", name)
    finally:
        session.close()

    # Apply per-DAG detector filtering
    all_alerts = [
        a for a in all_alerts if config.is_detector_enabled(a.alert_type.value, a.dag_id)
    ]

    dispatch(all_alerts, config)

    # Push summary to XCom for the dashboard. by_type counts reflect every
    # alert; the serialized alert list is capped to keep the XCom bounded.
    summary = {
        "total_alerts": len(all_alerts),
        "by_type": {},
        "alerts": [],
    }
    for alert in all_alerts:
        t = alert.alert_type.value
        summary["by_type"][t] = summary["by_type"].get(t, 0) + 1

    # Keep the most severe alerts when capping (Severity values sort
    # "critical" before "warning").
    ranked = sorted(all_alerts, key=lambda a: a.severity.value)
    for alert in ranked[:_MAX_XCOM_ALERTS]:
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

    # Push the dict directly — XCom serializes it to JSON. Passing a pre-dumped
    # string would be double-encoded and the dashboard could not parse it back.
    context["ti"].xcom_push(key="watchdog_results", value=summary)

    status = f"Watchdog complete: {len(all_alerts)} alert(s)"
    logger.info(status)
    return status


# ── DAG definition ───────────────────────────────────────────────────────────
# The schedule is read at parse time; changing it requires a scheduler restart.


def _get_schedule_minutes() -> int:
    """Read schedule_interval_minutes from the Airflow Variable at parse time."""
    try:
        import json as _json

        from airflow.models import Variable

        raw = Variable.get("watchdog_config", default_var="{}")
        cfg = _json.loads(raw) if isinstance(raw, str) else raw
        return int(cfg.get("schedule_interval_minutes", 30))
    except Exception:
        return 30


with DAG(
    dag_id="airflow_watchdog_monitor",
    description=(
        "Monitors DAG/task health"
        " — runtime anomalies, failure spikes, missed deadlines,"
        " stuck tasks, schedule anomalies."
    ),
    schedule=timedelta(minutes=_get_schedule_minutes()),
    start_date=None,  # Airflow 3: no fixed start_date needed for timedelta schedules
    catchup=False,
    max_active_runs=1,
    tags={"watchdog", "monitoring"},
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:
    run_watchdog = PythonOperator(
        task_id="run_detectors",
        python_callable=_run_watchdog,
    )
