"""
Configuration for Watchdog detectors.

All thresholds are read from Airflow Variables (JSON) under the key
``watchdog_config``.  If the Variable is not set, sensible defaults apply.

Example Variable value (set via UI or CLI):

    {
        "schedule_interval_minutes": 30,
        "lookback_runs": 20,
        "runtime_iqr_multiplier": 1.5,
        "failure_window_runs": 10,
        "failure_baseline_runs": 50,
        "failure_spike_ratio": 2.0,
        "deadline_multiplier": 2.0,
        "stuck_multiplier": 2.0,
        "schedule_iqr_multiplier": 1.5,
        "exclude_dags": ["airflow_watchdog_monitor"],
        "disable_detectors": [],
        "dag_overrides": {"my_dag": {"disable_detectors": ["schedule_anomaly"]}},
        "alert_emails": [],
        "alert_slack_webhook": null,
        "alert_teams_webhook": null,
        "alert_discord_webhook": null
    }
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_VARIABLE_KEY = "watchdog_config"

# Defaults ────────────────────────────────────────────────────────────────────

_DEFAULTS: dict[str, Any] = {
    # How often the watchdog DAG runs (minutes)
    "schedule_interval_minutes": 30,
    # Number of recent completed runs to consider for statistics
    "lookback_runs": 20,
    # IQR multiplier — duration outside Q1-multiplier*IQR .. Q3+multiplier*IQR
    "runtime_iqr_multiplier": 1.5,
    # Failure-spike: compare recent window vs longer baseline
    "failure_window_runs": 10,
    "failure_baseline_runs": 50,
    "failure_spike_ratio": 2.0,
    # Deadline: flag if running longer than multiplier × historical median
    "deadline_multiplier": 2.0,
    # Stuck: flag if a running task exceeds multiplier × historical max
    "stuck_multiplier": 2.0,
    # Schedule anomaly: IQR multiplier for start/end time-of-day fences
    "schedule_iqr_multiplier": 1.5,
    # DAGs to skip (always excludes the watchdog DAG itself)
    "exclude_dags": ["airflow_watchdog_monitor"],
    # Detectors to disable globally (by AlertType value)
    "disable_detectors": [],
    # Per-DAG overrides: {"dag_id": {"disable_detectors": [...], ...}}
    "dag_overrides": {},
    # Alerting
    "alert_emails": [],
    "alert_slack_webhook": None,
    "alert_teams_webhook": None,
    "alert_discord_webhook": None,
}


@dataclass
class WatchdogConfig:
    """Typed configuration with defaults."""

    schedule_interval_minutes: int = _DEFAULTS["schedule_interval_minutes"]
    lookback_runs: int = _DEFAULTS["lookback_runs"]
    runtime_iqr_multiplier: float = _DEFAULTS["runtime_iqr_multiplier"]
    failure_window_runs: int = _DEFAULTS["failure_window_runs"]
    failure_baseline_runs: int = _DEFAULTS["failure_baseline_runs"]
    failure_spike_ratio: float = _DEFAULTS["failure_spike_ratio"]
    deadline_multiplier: float = _DEFAULTS["deadline_multiplier"]
    stuck_multiplier: float = _DEFAULTS["stuck_multiplier"]
    schedule_iqr_multiplier: float = _DEFAULTS["schedule_iqr_multiplier"]
    exclude_dags: list[str] = field(default_factory=lambda: list(_DEFAULTS["exclude_dags"]))
    disable_detectors: list[str] = field(
        default_factory=lambda: list(_DEFAULTS["disable_detectors"])
    )
    dag_overrides: dict[str, dict] = field(
        default_factory=lambda: dict(_DEFAULTS["dag_overrides"])
    )
    alert_emails: list[str] = field(default_factory=lambda: list(_DEFAULTS["alert_emails"]))
    alert_slack_webhook: str | None = _DEFAULTS["alert_slack_webhook"]
    alert_teams_webhook: str | None = _DEFAULTS["alert_teams_webhook"]
    alert_discord_webhook: str | None = _DEFAULTS["alert_discord_webhook"]

    def is_detector_enabled(self, detector_name: str, dag_id: str) -> bool:
        """Check if a detector is enabled for a given DAG."""
        if detector_name in self.disable_detectors:
            return False
        dag_cfg = self.dag_overrides.get(dag_id, {})
        if detector_name in dag_cfg.get("disable_detectors", []):
            return False
        return True


def load_config() -> WatchdogConfig:
    """Load config from the Airflow Variable, falling back to defaults."""
    try:
        from airflow.models import Variable

        raw = Variable.get(_VARIABLE_KEY, default_var="{}")
        overrides = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        logger.info("Could not read Airflow Variable '%s'; using defaults.", _VARIABLE_KEY)
        overrides = {}

    merged = {**_DEFAULTS, **overrides}

    # Ensure the watchdog's own DAG is always excluded
    excluded = set(merged["exclude_dags"])
    excluded.add("airflow_watchdog_monitor")
    merged["exclude_dags"] = sorted(excluded)

    return WatchdogConfig(
        **{k: v for k, v in merged.items() if k in WatchdogConfig.__dataclass_fields__}
    )
