"""
Watchdog dashboard — FastAPI app.

Registers a ``/watchdog`` route in the Airflow webserver that shows an
overview of all DAGs with their health status and highlights problems.

Data comes from:
1. The latest XCom from the ``airflow_watchdog_monitor`` DAG (alert results)
2. Live queries against ``dag_run`` and ``task_instance`` for current status
"""

from __future__ import annotations

import json
import logging
import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

logger = logging.getLogger(__name__)

watchdog_app = FastAPI()


def _get_dashboard_data() -> dict:
    """Gather data for the dashboard from the metadata DB."""
    from datetime import datetime, timezone

    from airflow.settings import Session

    session = Session()
    data: dict = {
        "dags": [],
        "alerts": [],
        "summary": {"total_dags": 0, "healthy": 0, "warning": 0, "critical": 0},
    }

    try:
        from sqlalchemy import text

        # ── Get all active DAGs ────────────────────────────────────────
        dag_query = text(
            """\
            SELECT dag_id, is_paused
            FROM dag
            WHERE dag_id != 'airflow_watchdog_monitor'
            ORDER BY dag_id
        """
        )
        dag_rows = session.execute(dag_query).fetchall()

        # ── Get latest run per DAG ─────────────────────────────────────
        runs_query = text(
            """\
            SELECT dag_id, state, start_date, end_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY dag_id ORDER BY start_date DESC
                   ) AS rn
            FROM dag_run
            WHERE dag_id != 'airflow_watchdog_monitor'
        """
        )
        run_rows = session.execute(runs_query).fetchall()

        # Build lookup: dag_id -> latest run (rn=1)
        latest_runs: dict = {}
        for row in run_rows:
            if row.rn == 1:
                latest_runs[row.dag_id] = row

        # ── Get latest watchdog alerts from XCom ───────────────────────
        alerts_query = text(
            """\
            SELECT value
            FROM xcom
            WHERE dag_id = 'airflow_watchdog_monitor'
              AND task_id = 'run_detectors'
              AND key = 'watchdog_results'
            ORDER BY timestamp DESC
            LIMIT 1
        """
        )

        alert_row = session.execute(alerts_query).fetchone()
        alert_data: dict = {}
        if alert_row:
            try:
                alert_data = json.loads(alert_row.value)
            except (json.JSONDecodeError, TypeError):
                pass

        # ── Build alert lookup by dag_id ───────────────────────────────
        alerts_by_dag: dict[str, list[dict]] = {}
        for alert in alert_data.get("alerts", []):
            dag_id = alert.get("dag_id", "")
            alerts_by_dag.setdefault(dag_id, []).append(alert)

        # ── Assemble DAG list ──────────────────────────────────────────
        now = datetime.now(timezone.utc)

        for row in dag_rows:
            dag_alerts = alerts_by_dag.get(row.dag_id, [])
            has_critical = any(a["severity"] == "critical" for a in dag_alerts)
            has_warning = any(a["severity"] == "warning" for a in dag_alerts)

            if has_critical:
                status = "critical"
                data["summary"]["critical"] += 1
            elif has_warning:
                status = "warning"
                data["summary"]["warning"] += 1
            else:
                status = "healthy"
                data["summary"]["healthy"] += 1

            data["summary"]["total_dags"] += 1

            # Compute duration in Python (DB-agnostic)
            lr = latest_runs.get(row.dag_id)
            last_run_state = lr.state if lr else None
            last_run_start = lr.start_date if lr else None
            duration_secs = None
            if lr and lr.start_date:
                end = lr.end_date if lr.end_date else now
                duration_secs = (end - lr.start_date).total_seconds()

            data["dags"].append(
                {
                    "dag_id": row.dag_id,
                    "is_paused": row.is_paused,
                    "status": status,
                    "last_run_state": last_run_state,
                    "last_run_start": last_run_start.isoformat() if last_run_start else None,
                    "last_run_duration_secs": duration_secs,
                    "alerts": dag_alerts,
                }
            )

        data["alerts"] = alert_data.get("alerts", [])

        # Sort: critical first, then warning, then healthy
        status_order = {"critical": 0, "warning": 1, "healthy": 2}
        data["dags"].sort(key=lambda d: (status_order.get(d["status"], 3), d["dag_id"]))

    except Exception:
        logger.exception("Error loading watchdog dashboard data")
        data["error"] = "Failed to load dashboard data. Check the Airflow webserver logs."
    finally:
        session.close()

    return data


@watchdog_app.get("/")
async def dashboard() -> HTMLResponse:
    """Serve the watchdog dashboard."""
    data = _get_dashboard_data()
    template_path = os.path.join(os.path.dirname(__file__), "templates", "dashboard.html")

    with open(template_path) as f:
        html_template = f.read()

    # OWASP: escape HTML-significant chars using unicode escapes (valid in JS)
    safe_json = (
        json.dumps(data).replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")
    )
    html = html_template.replace("{{ DATA_JSON }}", safe_json)

    return HTMLResponse(content=html)


@watchdog_app.get("/api/data")
async def api_data() -> JSONResponse:
    """JSON API endpoint for dashboard data."""
    data = _get_dashboard_data()
    return JSONResponse(content=data)


# ── Config UI ────────────────────────────────────────────────────────────────


def _get_config_data() -> dict:
    """Load current config and DAG list for the config page."""
    from airflow.settings import Session

    session = Session()
    data: dict = {"dags": [], "config": {}, "detector_names": []}

    try:
        from airflow_watchdog.config import load_config
        from airflow_watchdog.detectors import AlertType

        config = load_config()
        data["detector_names"] = [e.value for e in AlertType]
        data["config"] = {
            "disable_detectors": config.disable_detectors,
            "dag_overrides": config.dag_overrides,
        }

        # Get all DAGs
        from sqlalchemy import text

        dag_query = text(
            "SELECT dag_id FROM dag WHERE dag_id != 'airflow_watchdog_monitor' ORDER BY dag_id"
        )
        dag_rows = session.execute(dag_query).fetchall()
        data["dags"] = [row.dag_id for row in dag_rows]

    except Exception:
        logger.exception("Error loading config data")
        data["error"] = "Failed to load configuration data."
    finally:
        session.close()

    return data


def _save_config(disable_detectors: list[str], dag_overrides: dict[str, dict]) -> dict:
    """Update the disable_detectors and dag_overrides fields in the Airflow Variable."""
    try:
        from airflow.models import Variable

        from airflow_watchdog.config import _VARIABLE_KEY
        from airflow_watchdog.detectors import AlertType

        valid_names = {e.value for e in AlertType}

        # Validate disable_detectors entries
        for name in disable_detectors:
            if name not in valid_names:
                return {
                    "success": False,
                    "error": f"Unknown detector: {name}",
                }

        # Validate dag_overrides structure
        for dag_id, cfg in dag_overrides.items():
            if not isinstance(cfg, dict):
                return {
                    "success": False,
                    "error": f"Invalid override for {dag_id}",
                }
            for name in cfg.get("disable_detectors", []):
                if name not in valid_names:
                    return {
                        "success": False,
                        "error": f"Unknown detector in {dag_id}: {name}",
                    }

        raw = Variable.get(_VARIABLE_KEY, default_var="{}")
        current = json.loads(raw) if isinstance(raw, str) else raw

        current["disable_detectors"] = disable_detectors
        # Remove empty overrides to keep the JSON clean
        current["dag_overrides"] = {
            dag_id: cfg for dag_id, cfg in dag_overrides.items() if cfg.get("disable_detectors")
        }

        Variable.set(_VARIABLE_KEY, json.dumps(current))
        return {"success": True}

    except Exception:
        logger.exception("Error saving watchdog config")
        return {"success": False, "error": "Failed to save configuration."}


@watchdog_app.get("/config")
async def config_page() -> HTMLResponse:
    """Serve the config UI page."""
    data = _get_config_data()
    template_path = os.path.join(os.path.dirname(__file__), "templates", "config.html")

    with open(template_path) as f:
        html_template = f.read()

    safe_json = (
        json.dumps(data).replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")
    )
    html = html_template.replace("{{ DATA_JSON }}", safe_json)
    return HTMLResponse(content=html)


@watchdog_app.get("/api/config")
async def api_config() -> JSONResponse:
    """JSON API endpoint for config data."""
    data = _get_config_data()
    return JSONResponse(content=data)


@watchdog_app.post("/api/config")
async def api_config_save(request: Request) -> JSONResponse:
    """Save config changes."""
    body = await request.json()
    result = _save_config(
        disable_detectors=body.get("disable_detectors", []),
        dag_overrides=body.get("dag_overrides", {}),
    )
    return JSONResponse(content=result)
