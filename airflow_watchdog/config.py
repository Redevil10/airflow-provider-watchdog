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
        "exclude_dags": ["watchdog_monitor"],
        "alert_emails": [],
        "alert_slack_webhook": null
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
    # DAGs to skip (always excludes the watchdog DAG itself)
    "exclude_dags": ["watchdog_monitor"],
    # Alerting
    "alert_emails": [],
    "alert_slack_webhook": None,
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
    exclude_dags: list[str] = field(default_factory=lambda: list(_DEFAULTS["exclude_dags"]))
    alert_emails: list[str] = field(default_factory=lambda: list(_DEFAULTS["alert_emails"]))
    alert_slack_webhook: str | None = _DEFAULTS["alert_slack_webhook"]


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
    excluded.add("watchdog_monitor")
    merged["exclude_dags"] = sorted(excluded)

    return WatchdogConfig(
        **{k: v for k, v in merged.items() if k in WatchdogConfig.__dataclass_fields__}
    )
