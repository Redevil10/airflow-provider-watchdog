"""
Watchdog detectors.

Each detector module exposes a ``detect(session, config) -> list[Alert]``
function that queries the Airflow metadata DB and returns zero or more alerts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class Severity(str, Enum):
    WARNING = "warning"
    CRITICAL = "critical"


class AlertType(str, Enum):
    RUNTIME_ANOMALY = "runtime_anomaly"
    FAILURE_SPIKE = "failure_spike"
    MISSED_DEADLINE = "missed_deadline"
    STUCK_TASK = "stuck_task"
    SCHEDULE_ANOMALY = "schedule_anomaly"


@dataclass
class Alert:
    """A single watchdog alert."""

    alert_type: AlertType
    severity: Severity
    dag_id: str
    task_id: str | None = None
    message: str = ""
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    details: dict = field(default_factory=dict)

    def __str__(self) -> str:
        target = f"{self.dag_id}.{self.task_id}" if self.task_id else self.dag_id
        return (
            f"[{self.severity.value.upper()}] {self.alert_type.value}: {target} — {self.message}"
        )
