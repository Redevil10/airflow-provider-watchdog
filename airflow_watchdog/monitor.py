"""
Detection runner.

Runs all five detectors against the metadata DB, dispatches alerts, and stores
a bounded summary of the results.

In Airflow 3, task/worker code may not access the metadata DB via the ORM
(AIP-72 task isolation). The detectors therefore run on the API-server side —
driven by the background scheduler started from the plugin's FastAPI lifespan
(see ``scheduler.py``) — where direct DB access is sanctioned. This is the same
place, and the same ``airflow.settings.Session``, the dashboard already uses.

The latest summary is persisted to the Airflow Variable ``watchdog_last_results``
so the dashboard can read it (and so it survives an API-server restart and is
shared across API-server replicas).
"""

from __future__ import annotations

import json
import logging

from airflow_watchdog.config import WatchdogConfig, load_config

logger = logging.getLogger(__name__)

# Variable holding the most recent detection summary, read by the dashboard.
RESULTS_VARIABLE_KEY = "watchdog_last_results"

# Cap the number of alerts persisted into the results Variable so a pathological
# run can't produce an oversized payload. The most severe alerts are kept. The
# by_type counts still reflect every alert.
_MAX_STORED_ALERTS = 50


def run_detection(config: WatchdogConfig | None = None) -> dict:
    """Execute all detectors, dispatch alerts, and return a bounded summary.

    Must run where ORM access is allowed (API server / scheduler), never inside
    a task. Persists the summary to the ``watchdog_last_results`` Variable.
    """
    from airflow.settings import Session

    from airflow_watchdog.alerting import dispatch
    from airflow_watchdog.detectors import Alert
    from airflow_watchdog.detectors.deadlines import detect as detect_deadlines
    from airflow_watchdog.detectors.failures import detect as detect_failures
    from airflow_watchdog.detectors.runtime import detect as detect_runtime
    from airflow_watchdog.detectors.schedule import detect as detect_schedule
    from airflow_watchdog.detectors.stuck import detect as detect_stuck

    if config is None:
        config = load_config()

    assert Session is not None  # configured by Airflow at runtime
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

    summary = _build_summary(all_alerts)
    _store_results(summary)

    logger.info("Watchdog complete: %d alert(s)", len(all_alerts))
    return summary


def _build_summary(all_alerts: list) -> dict:
    """Build the bounded summary dict the dashboard consumes.

    ``by_type`` counts reflect every alert; the serialized alert list is capped
    to keep the stored payload bounded, keeping the most severe alerts.
    """
    from datetime import datetime, timezone

    summary: dict = {
        "total_alerts": len(all_alerts),
        "by_type": {},
        "alerts": [],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    for alert in all_alerts:
        t = alert.alert_type.value
        summary["by_type"][t] = summary["by_type"].get(t, 0) + 1

    # Keep the most severe alerts when capping (Severity values sort
    # "critical" before "warning").
    ranked = sorted(all_alerts, key=lambda a: a.severity.value)
    for alert in ranked[:_MAX_STORED_ALERTS]:
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
    return summary


def _store_results(summary: dict) -> None:
    """Persist the summary to the ``watchdog_last_results`` Variable."""
    try:
        from airflow.models import Variable

        Variable.set(RESULTS_VARIABLE_KEY, json.dumps(summary))
    except Exception:
        logger.exception("Failed to persist watchdog results to Variable")


def load_results() -> dict:
    """Read the latest detection summary from the Variable.

    Returns an empty summary when nothing has been stored yet (e.g. before the
    first scheduler cycle completes).
    """
    empty: dict = {"total_alerts": 0, "by_type": {}, "alerts": [], "generated_at": None}
    try:
        from airflow.models import Variable

        raw = Variable.get(RESULTS_VARIABLE_KEY, default_var=None)
    except Exception:
        logger.info("Could not read Variable '%s'; returning empty results.", RESULTS_VARIABLE_KEY)
        return empty

    if not raw:
        return empty
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, ValueError):
        logger.warning("Stored watchdog results were not valid JSON; returning empty results.")
        return empty
    return parsed if isinstance(parsed, dict) else empty
