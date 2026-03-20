"""Tests for alert types."""

from __future__ import annotations

from airflow_watchdog.detectors import Alert, AlertType, Severity


def test_alert_str_with_task():
    a = Alert(
        alert_type=AlertType.RUNTIME_ANOMALY,
        severity=Severity.WARNING,
        dag_id="my_dag",
        task_id="my_task",
        message="Too slow",
    )
    s = str(a)
    assert "WARNING" in s
    assert "runtime_anomaly" in s
    assert "my_dag.my_task" in s
    assert "Too slow" in s


def test_alert_str_without_task():
    a = Alert(
        alert_type=AlertType.FAILURE_SPIKE,
        severity=Severity.CRITICAL,
        dag_id="my_dag",
        message="Spike detected",
    )
    s = str(a)
    assert "CRITICAL" in s
    assert "my_dag" in s
    assert ".None" not in s


def test_alert_types_enum():
    assert AlertType.RUNTIME_ANOMALY.value == "runtime_anomaly"
    assert AlertType.FAILURE_SPIKE.value == "failure_spike"
    assert AlertType.MISSED_DEADLINE.value == "missed_deadline"
    assert AlertType.STUCK_TASK.value == "stuck_task"
