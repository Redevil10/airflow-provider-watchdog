"""Tests for watchdog detectors.

Each detector is tested by mocking the SQLAlchemy session to return
controlled result rows, then verifying the Alert objects produced.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from airflow_watchdog.config import WatchdogConfig
from airflow_watchdog.detectors import AlertType, Severity

# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_session(rows: list[SimpleNamespace]) -> MagicMock:
    """Create a mock Session whose execute().fetchall() returns *rows*."""
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = rows
    return session


def _default_config(**overrides) -> WatchdogConfig:
    return WatchdogConfig(**overrides)


# ── Runtime anomaly detector ─────────────────────────────────────────────────


class TestRuntimeDetector:
    def test_slower_than_upper_fence_warning(self):
        from airflow_watchdog.detectors.runtime import detect

        row = SimpleNamespace(
            dag_id="etl_daily",
            task_id="extract",
            q1=10.0,
            median=15.0,
            q3=20.0,
            iqr=10.0,
            lower_fence=-5.0,
            upper_fence=35.0,
            latest_duration=40.0,  # outside upper fence but within 3*IQR
            sample_size=20,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert alerts[0].alert_type == AlertType.RUNTIME_ANOMALY
        assert alerts[0].severity == Severity.WARNING
        assert alerts[0].dag_id == "etl_daily"
        assert alerts[0].task_id == "extract"
        assert "slower" in alerts[0].message

    def test_faster_than_lower_fence(self):
        from airflow_watchdog.detectors.runtime import detect

        row = SimpleNamespace(
            dag_id="etl_daily",
            task_id="extract",
            q1=100.0,
            median=150.0,
            q3=200.0,
            iqr=100.0,
            lower_fence=-50.0,
            upper_fence=350.0,
            latest_duration=-60.0,  # below lower fence
            sample_size=20,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert "faster" in alerts[0].message

    def test_critical_when_deviation_exceeds_3x_iqr(self):
        from airflow_watchdog.detectors.runtime import detect

        row = SimpleNamespace(
            dag_id="etl_daily",
            task_id="load",
            q1=10.0,
            median=15.0,
            q3=20.0,
            iqr=10.0,
            lower_fence=-5.0,
            upper_fence=35.0,
            latest_duration=50.0,  # |50-15| = 35 > 3*10 = 30 → CRITICAL
            sample_size=20,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert alerts[0].severity == Severity.CRITICAL

    def test_no_anomalies_returns_empty(self):
        from airflow_watchdog.detectors.runtime import detect

        session = _make_session([])
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_query_failure_returns_empty(self):
        from airflow_watchdog.detectors.runtime import detect

        session = MagicMock()
        session.execute.side_effect = Exception("DB connection lost")
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_details_populated(self):
        from airflow_watchdog.detectors.runtime import detect

        row = SimpleNamespace(
            dag_id="dag1",
            task_id="task1",
            q1=10.0,
            median=15.0,
            q3=20.0,
            iqr=10.0,
            lower_fence=-5.0,
            upper_fence=35.0,
            latest_duration=40.0,
            sample_size=20,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert alerts[0].details["latest_duration"] == 40.0
        assert alerts[0].details["median"] == 15.0
        assert alerts[0].details["sample_size"] == 20


# ── Failure spike detector ───────────────────────────────────────────────────


class TestFailureSpikeDetector:
    def test_spike_detected_warning(self):
        from airflow_watchdog.detectors.failures import detect

        row = SimpleNamespace(
            dag_id="etl_daily",
            recent_failures=3,
            recent_total=10,
            recent_rate=0.3,
            baseline_failures=5,
            baseline_total=50,
            baseline_rate=0.1,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert alerts[0].alert_type == AlertType.FAILURE_SPIKE
        assert alerts[0].severity == Severity.WARNING
        assert alerts[0].dag_id == "etl_daily"

    def test_zero_baseline_is_critical(self):
        from airflow_watchdog.detectors.failures import detect

        row = SimpleNamespace(
            dag_id="new_dag",
            recent_failures=2,
            recent_total=5,
            recent_rate=0.4,
            baseline_failures=0,
            baseline_total=50,
            baseline_rate=0.0,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert alerts[0].severity == Severity.CRITICAL

    def test_high_recent_rate_is_critical(self):
        from airflow_watchdog.detectors.failures import detect

        row = SimpleNamespace(
            dag_id="broken_dag",
            recent_failures=6,
            recent_total=10,
            recent_rate=0.6,  # > 0.5 → CRITICAL
            baseline_failures=5,
            baseline_total=50,
            baseline_rate=0.1,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert alerts[0].severity == Severity.CRITICAL

    def test_no_spikes_returns_empty(self):
        from airflow_watchdog.detectors.failures import detect

        session = _make_session([])
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_query_failure_returns_empty(self):
        from airflow_watchdog.detectors.failures import detect

        session = MagicMock()
        session.execute.side_effect = Exception("DB error")
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_message_includes_rates(self):
        from airflow_watchdog.detectors.failures import detect

        row = SimpleNamespace(
            dag_id="dag1",
            recent_failures=3,
            recent_total=10,
            recent_rate=0.3,
            baseline_failures=5,
            baseline_total=50,
            baseline_rate=0.1,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert "30.0%" in alerts[0].message
        assert "10.0%" in alerts[0].message


# ── Missed deadline detector ─────────────────────────────────────────────────


class TestDeadlineDetector:
    def test_warning_when_over_deadline(self):
        from airflow_watchdog.detectors.deadlines import detect

        row = SimpleNamespace(
            dag_id="etl_daily",
            run_id="manual__2024-01-01",
            elapsed_secs=1800.0,
            median_duration=600.0,
            max_duration=900.0,
            sample_size=15,
            deadline_secs=1200.0,  # 2x median
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert alerts[0].alert_type == AlertType.MISSED_DEADLINE
        assert alerts[0].severity == Severity.WARNING  # ratio = 3.0, need > 3 for CRITICAL
        assert alerts[0].dag_id == "etl_daily"

    def test_warning_for_moderate_overrun(self):
        from airflow_watchdog.detectors.deadlines import detect

        row = SimpleNamespace(
            dag_id="etl_daily",
            run_id="manual__2024-01-01",
            elapsed_secs=900.0,
            median_duration=600.0,  # ratio = 1.5 < 3 → WARNING
            max_duration=800.0,
            sample_size=15,
            deadline_secs=1200.0,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert alerts[0].severity == Severity.WARNING

    def test_no_overruns_returns_empty(self):
        from airflow_watchdog.detectors.deadlines import detect

        session = _make_session([])
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_query_failure_returns_empty(self):
        from airflow_watchdog.detectors.deadlines import detect

        session = MagicMock()
        session.execute.side_effect = Exception("DB error")
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_details_include_run_id(self):
        from airflow_watchdog.detectors.deadlines import detect

        row = SimpleNamespace(
            dag_id="dag1",
            run_id="scheduled__2024-01-01",
            elapsed_secs=2000.0,
            median_duration=600.0,
            max_duration=900.0,
            sample_size=10,
            deadline_secs=1200.0,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert alerts[0].details["run_id"] == "scheduled__2024-01-01"
        assert alerts[0].details["elapsed_secs"] == 2000.0

    def test_zero_median_no_division_error(self):
        from airflow_watchdog.detectors.deadlines import detect

        row = SimpleNamespace(
            dag_id="dag1",
            run_id="run1",
            elapsed_secs=100.0,
            median_duration=0.0,
            max_duration=0.0,
            sample_size=5,
            deadline_secs=0.0,
        )
        session = _make_session([row])
        # Should not raise ZeroDivisionError
        alerts = detect(session, _default_config())
        assert len(alerts) == 1


# ── Stuck task detector ──────────────────────────────────────────────────────


class TestStuckTaskDetector:
    def test_stuck_task_always_critical(self):
        from airflow_watchdog.detectors.stuck import detect

        row = SimpleNamespace(
            dag_id="etl_daily",
            task_id="load_data",
            run_id="manual__2024-01-01",
            elapsed_secs=7200.0,
            max_duration=1800.0,
            median_duration=1200.0,
            sample_size=10,
            stuck_threshold=3600.0,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert alerts[0].alert_type == AlertType.STUCK_TASK
        assert alerts[0].severity == Severity.CRITICAL
        assert alerts[0].dag_id == "etl_daily"
        assert alerts[0].task_id == "load_data"
        assert "stuck" in alerts[0].message.lower() or "zombie" in alerts[0].message.lower()

    def test_no_stuck_tasks_returns_empty(self):
        from airflow_watchdog.detectors.stuck import detect

        session = _make_session([])
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_query_failure_returns_empty(self):
        from airflow_watchdog.detectors.stuck import detect

        session = MagicMock()
        session.execute.side_effect = Exception("DB error")
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_multiple_stuck_tasks(self):
        from airflow_watchdog.detectors.stuck import detect

        rows = [
            SimpleNamespace(
                dag_id="dag1",
                task_id="task_a",
                run_id="run1",
                elapsed_secs=5000.0,
                max_duration=1000.0,
                median_duration=800.0,
                sample_size=10,
                stuck_threshold=2000.0,
            ),
            SimpleNamespace(
                dag_id="dag2",
                task_id="task_b",
                run_id="run2",
                elapsed_secs=8000.0,
                max_duration=2000.0,
                median_duration=1500.0,
                sample_size=15,
                stuck_threshold=4000.0,
            ),
        ]
        session = _make_session(rows)
        alerts = detect(session, _default_config())

        assert len(alerts) == 2
        assert all(a.severity == Severity.CRITICAL for a in alerts)

    def test_zero_max_duration_no_division_error(self):
        from airflow_watchdog.detectors.stuck import detect

        row = SimpleNamespace(
            dag_id="dag1",
            task_id="task1",
            run_id="run1",
            elapsed_secs=100.0,
            max_duration=0.0,
            median_duration=0.0,
            sample_size=5,
            stuck_threshold=0.0,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())
        assert len(alerts) == 1
