"""Render the real Watchdog UI templates with realistic fake data, for screenshots.

This produces standalone HTML copies of the dashboard and config pages — the
exact templates the plugin serves, fed the same ``DATA_JSON`` blob the FastAPI
app injects (see ``airflow_watchdog/ui/app.py``) — so the docs screenshots can be
taken without a running Airflow.

Usage (from anywhere; paths are resolved relative to the repo):

    python docs/gen_preview.py            # writes to ./preview/
    python docs/gen_preview.py /tmp/out   # writes to a custom dir

Open the generated ``dashboard.html`` / ``config.html`` in a browser and capture
the screenshots by hand into ``docs/`` (dashboard.png, and the three config tabs:
config_detectors.png, config_thresholds.png, config_alerts.png). The generated
HTML is a build artifact and is not committed.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Resolve paths relative to this file so the script runs from any cwd.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_TPL_DIR = _REPO_ROOT / "airflow_watchdog" / "ui" / "templates"

now = datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat()


# ── Dashboard data (matches _get_dashboard_data output) ──────────────────────
dags = [
    {
        "dag_id": "payment_reconciliation",
        "is_paused": False,
        "status": "critical",
        "last_run_state": "failed",
        "last_run_start": iso(now - timedelta(minutes=22)),
        "last_run_duration_secs": 418.0,
        "alerts": [
            {
                "type": "failure_spike",
                "severity": "critical",
                "dag_id": "payment_reconciliation",
                "task_id": None,
                "message": "Failure rate 40.0% in last 10 runs (baseline 2.0% over 50 runs) — 4/10 recent runs failed",  # noqa: E501
                "detected_at": iso(now),
                "details": {},
            },
            {
                "type": "stuck_task",
                "severity": "critical",
                "dag_id": "payment_reconciliation",
                "task_id": "settle_transactions",
                "message": "Task running 47.0m, over 2× its historical max of 6.0m",
                "detected_at": iso(now),
                "details": {},
            },
        ],
    },
    {
        "dag_id": "daily_sales_etl",
        "is_paused": False,
        "status": "warning",
        "last_run_state": "success",
        "last_run_start": iso(now - timedelta(minutes=35)),
        "last_run_duration_secs": 142.0,
        "alerts": [
            {
                "type": "runtime_anomaly",
                "severity": "warning",
                "dag_id": "daily_sales_etl",
                "task_id": "aggregate_revenue",
                "message": "Latest run 142.0s is slower than expected (median 38.0s, IQR fence [20.0s, 60.0s])",  # noqa: E501
                "detected_at": iso(now),
                "details": {},
            }
        ],
    },
    {
        "dag_id": "inventory_sync",
        "is_paused": False,
        "status": "warning",
        "last_run_state": "success",
        "last_run_start": iso(now - timedelta(hours=1, minutes=12)),
        "last_run_duration_secs": 73.0,
        "alerts": [
            {
                "type": "schedule_anomaly",
                "severity": "warning",
                "dag_id": "inventory_sync",
                "task_id": "pull_stock_levels",
                "message": "Task start time 09:47 is later than expected (median 06:00, fence [05:30, 06:30])",  # noqa: E501
                "detected_at": iso(now),
                "details": {},
            }
        ],
    },
    {
        "dag_id": "order_ingestion",
        "is_paused": False,
        "status": "healthy",
        "last_run_state": "success",
        "last_run_start": iso(now - timedelta(minutes=8)),
        "last_run_duration_secs": 54.0,
        "alerts": [],
    },
    {
        "dag_id": "customer_segmentation",
        "is_paused": False,
        "status": "healthy",
        "last_run_state": "success",
        "last_run_start": iso(now - timedelta(hours=3)),
        "last_run_duration_secs": 612.0,
        "alerts": [],
    },
    {
        "dag_id": "product_recommendations",
        "is_paused": False,
        "status": "healthy",
        "last_run_state": "running",
        "last_run_start": iso(now - timedelta(minutes=4)),
        "last_run_duration_secs": 240.0,
        "alerts": [],
    },
    {
        "dag_id": "marketing_attribution",
        "is_paused": True,
        "status": "healthy",
        "last_run_state": "success",
        "last_run_start": iso(now - timedelta(days=1)),
        "last_run_duration_secs": 1830.0,
        "alerts": [],
    },
    {
        "dag_id": "warehouse_load",
        "is_paused": False,
        "status": "healthy",
        "last_run_state": "success",
        "last_run_start": iso(now - timedelta(minutes=51)),
        "last_run_duration_secs": 305.0,
        "alerts": [],
    },
]

dashboard_data = {
    "dags": dags,
    "alerts": [a for d in dags for a in d["alerts"]],
    "summary": {"total_dags": 8, "healthy": 5, "warning": 2, "critical": 1},
    "base_url": "",
}

# ── Config data (matches _get_config_data output) ────────────────────────────
config_data = {
    "dags": [d["dag_id"] for d in dags],
    "detector_names": [
        "runtime_anomaly",
        "failure_spike",
        "missed_deadline",
        "stuck_task",
        "schedule_anomaly",
    ],
    "config": {
        "disable_detectors": [],
        "dag_overrides": {"marketing_attribution": {"disable_detectors": ["schedule_anomaly"]}},
        "schedule_interval_minutes": 30,
        "lookback_runs": 20,
        "runtime_iqr_multiplier": 1.5,
        "runtime_min_deviation_secs": 5.0,
        "failure_window_runs": 10,
        "failure_baseline_runs": 50,
        "failure_spike_ratio": 2.0,
        "deadline_multiplier": 2.0,
        "stuck_multiplier": 2.0,
        "schedule_iqr_multiplier": 1.5,
        "schedule_min_deviation_minutes": 5.0,
        "exclude_dags": ["legacy_orders_backfill"],
        "alert_emails": ["data-alerts@shop.example.com"],
        "alert_slack_webhook": "https://hooks.slack.com/services/T0XXXXXXX/B0XXXXXXX/************",
        "alert_teams_webhook": None,
        "alert_discord_webhook": None,
    },
}


def render(template_name: str, data: dict) -> str:
    tpl = (_TPL_DIR / template_name).read_text(encoding="utf-8")
    # Keep this escaping in sync with app._safe_json: escape the HTML-significant
    # chars (so the blob can't break out of the <script>) plus U+2028/U+2029
    # (valid in JSON but illegal raw in a JS string literal).
    safe = (
        json.dumps(data)
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace(" ", "\\u2028")
        .replace(" ", "\\u2029")
    )
    return tpl.replace("{{ DATA_JSON }}", safe)


def main() -> None:
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else _REPO_ROOT / "preview"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, data in [("dashboard.html", dashboard_data), ("config.html", config_data)]:
        path = out_dir / name
        path.write_text(render(name, data), encoding="utf-8")
        print("wrote", path)


if __name__ == "__main__":
    main()
