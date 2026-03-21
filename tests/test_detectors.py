"""Tests for watchdog detectors.

Each detector is tested by mocking the SQLAlchemy session to return
controlled result rows, then verifying the Alert objects produced.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
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


def _make_multi_session(*row_lists: list[SimpleNamespace]) -> MagicMock:
    """Mock session returning different rows for successive execute() calls."""
    session = MagicMock()
    results = []
    for rows in row_lists:
        result = MagicMock()
        result.fetchall.return_value = rows
        results.append(result)
    session.execute.side_effect = results
    return session


def _default_config(**overrides) -> WatchdogConfig:
    return WatchdogConfig(**overrides)


# ── Runtime anomaly detector ─────────────────────────────────────────────────


class TestRuntimeDetector:
    def test_slower_than_upper_fence_warning(self):
        from airflow_watchdog.detectors.runtime import detect

        # Provide enough data points (rn 1-6), latest (rn=1) is the anomaly
        rows = [
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=40.0, rn=1),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=15.0, rn=2),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=12.0, rn=3),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=18.0, rn=4),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=14.0, rn=5),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=16.0, rn=6),
        ]
        session = _make_session(rows)
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert alerts[0].alert_type == AlertType.RUNTIME_ANOMALY
        assert alerts[0].severity == Severity.WARNING
        assert alerts[0].dag_id == "etl_daily"
        assert alerts[0].task_id == "extract"
        assert "slower" in alerts[0].message

    def test_faster_than_lower_fence(self):
        from airflow_watchdog.detectors.runtime import detect

        # Latest (rn=1) is far below the cluster of 100-140
        rows = [
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=10.0, rn=1),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=100.0, rn=2),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=105.0, rn=3),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=110.0, rn=4),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=115.0, rn=5),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=120.0, rn=6),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=125.0, rn=7),
            SimpleNamespace(dag_id="etl_daily", task_id="extract", duration=130.0, rn=8),
        ]
        session = _make_session(rows)
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert "faster" in alerts[0].message

    def test_critical_when_deviation_exceeds_3x_iqr(self):
        from airflow_watchdog.detectors.runtime import detect

        # Latest duration far outside IQR fences
        rows = [
            SimpleNamespace(dag_id="etl_daily", task_id="load", duration=200.0, rn=1),
            SimpleNamespace(dag_id="etl_daily", task_id="load", duration=10.0, rn=2),
            SimpleNamespace(dag_id="etl_daily", task_id="load", duration=12.0, rn=3),
            SimpleNamespace(dag_id="etl_daily", task_id="load", duration=14.0, rn=4),
            SimpleNamespace(dag_id="etl_daily", task_id="load", duration=16.0, rn=5),
            SimpleNamespace(dag_id="etl_daily", task_id="load", duration=18.0, rn=6),
        ]
        session = _make_session(rows)
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

        rows = [
            SimpleNamespace(dag_id="dag1", task_id="task1", duration=40.0, rn=1),
            SimpleNamespace(dag_id="dag1", task_id="task1", duration=15.0, rn=2),
            SimpleNamespace(dag_id="dag1", task_id="task1", duration=12.0, rn=3),
            SimpleNamespace(dag_id="dag1", task_id="task1", duration=18.0, rn=4),
            SimpleNamespace(dag_id="dag1", task_id="task1", duration=14.0, rn=5),
            SimpleNamespace(dag_id="dag1", task_id="task1", duration=16.0, rn=6),
        ]
        session = _make_session(rows)
        alerts = detect(session, _default_config())

        assert alerts[0].details["latest_duration"] == 40.0
        assert "median" in alerts[0].details
        assert "sample_size" in alerts[0].details

    def test_fewer_than_5_samples_skipped(self):
        from airflow_watchdog.detectors.runtime import detect

        rows = [
            SimpleNamespace(dag_id="dag1", task_id="task1", duration=100.0, rn=1),
            SimpleNamespace(dag_id="dag1", task_id="task1", duration=10.0, rn=2),
            SimpleNamespace(dag_id="dag1", task_id="task1", duration=12.0, rn=3),
        ]
        session = _make_session(rows)
        alerts = detect(session, _default_config())
        assert alerts == []


# ── Failure spike detector ───────────────────────────────────────────────────


class TestFailureSpikeDetector:
    def test_spike_detected_warning(self):
        from airflow_watchdog.detectors.failures import detect

        row = SimpleNamespace(
            dag_id="etl_daily",
            recent_failures=3,
            recent_total=10,
            baseline_failures=5,
            baseline_total=50,
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
            baseline_failures=0,
            baseline_total=50,
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
            baseline_failures=5,
            baseline_total=50,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert alerts[0].severity == Severity.CRITICAL

    def test_no_spikes_returns_empty(self):
        from airflow_watchdog.detectors.failures import detect

        session = _make_session([])
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_no_spike_when_below_threshold(self):
        from airflow_watchdog.detectors.failures import detect

        # Recent rate (10%) is NOT > 2x baseline (10%) — no spike
        row = SimpleNamespace(
            dag_id="stable_dag",
            recent_failures=1,
            recent_total=10,
            baseline_failures=5,
            baseline_total=50,
        )
        session = _make_session([row])
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
            baseline_failures=5,
            baseline_total=50,
        )
        session = _make_session([row])
        alerts = detect(session, _default_config())

        assert "30.0%" in alerts[0].message
        assert "10.0%" in alerts[0].message


# ── Missed deadline detector ─────────────────────────────────────────────────


class TestDeadlineDetector:
    def _now(self):
        return datetime.now(timezone.utc)

    def test_warning_when_over_deadline(self):
        from airflow_watchdog.detectors.deadlines import detect

        now = self._now()
        # Historical: 6 runs with ~600s duration
        hist_rows = [
            SimpleNamespace(
                dag_id="etl_daily",
                start_date=now - timedelta(days=i, seconds=600),
                end_date=now - timedelta(days=i),
            )
            for i in range(1, 7)
        ]
        # Running: started 1500s ago (2.5x median → WARNING since ratio < 3)
        run_rows = [
            SimpleNamespace(
                dag_id="etl_daily",
                run_id="manual__2024-01-01",
                start_date=now - timedelta(seconds=1500),
            )
        ]
        session = _make_multi_session(hist_rows, run_rows)
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert alerts[0].alert_type == AlertType.MISSED_DEADLINE
        assert alerts[0].severity == Severity.WARNING
        assert alerts[0].dag_id == "etl_daily"

    def test_critical_when_far_over_deadline(self):
        from airflow_watchdog.detectors.deadlines import detect

        now = self._now()
        hist_rows = [
            SimpleNamespace(
                dag_id="etl_daily",
                start_date=now - timedelta(days=i, seconds=600),
                end_date=now - timedelta(days=i),
            )
            for i in range(1, 7)
        ]
        # Running: started 2400s ago (4x median → CRITICAL)
        run_rows = [
            SimpleNamespace(
                dag_id="etl_daily",
                run_id="manual__2024-01-01",
                start_date=now - timedelta(seconds=2400),
            )
        ]
        session = _make_multi_session(hist_rows, run_rows)
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert alerts[0].severity == Severity.CRITICAL

    def test_no_overruns_returns_empty(self):
        from airflow_watchdog.detectors.deadlines import detect

        session = _make_multi_session([], [])
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

        now = self._now()
        hist_rows = [
            SimpleNamespace(
                dag_id="dag1",
                start_date=now - timedelta(days=i, seconds=600),
                end_date=now - timedelta(days=i),
            )
            for i in range(1, 7)
        ]
        run_rows = [
            SimpleNamespace(
                dag_id="dag1",
                run_id="scheduled__2024-01-01",
                start_date=now - timedelta(seconds=2000),
            )
        ]
        session = _make_multi_session(hist_rows, run_rows)
        alerts = detect(session, _default_config())

        assert alerts[0].details["run_id"] == "scheduled__2024-01-01"
        assert alerts[0].details["elapsed_secs"] > 0

    def test_zero_median_no_division_error(self):
        from airflow_watchdog.detectors.deadlines import detect

        now = self._now()
        # All runs have 0 duration
        hist_rows = [
            SimpleNamespace(
                dag_id="dag1",
                start_date=now - timedelta(days=i),
                end_date=now - timedelta(days=i),
            )
            for i in range(1, 7)
        ]
        run_rows = [
            SimpleNamespace(
                dag_id="dag1",
                run_id="run1",
                start_date=now - timedelta(seconds=100),
            )
        ]
        session = _make_multi_session(hist_rows, run_rows)
        # Should not raise ZeroDivisionError
        alerts = detect(session, _default_config())
        # With 0 median and multiplier 2.0, deadline is 0s — any elapsed time exceeds it
        assert len(alerts) == 1


# ── Stuck task detector ──────────────────────────────────────────────────────


class TestStuckTaskDetector:
    def _now(self):
        return datetime.now(timezone.utc)

    def test_stuck_task_always_critical(self):
        from airflow_watchdog.detectors.stuck import detect

        now = self._now()
        hist_rows = [
            SimpleNamespace(dag_id="etl_daily", task_id="load_data", duration=1800.0 - i * 100)
            for i in range(5)
        ]
        run_rows = [
            SimpleNamespace(
                dag_id="etl_daily",
                task_id="load_data",
                run_id="manual__2024-01-01",
                start_date=now - timedelta(seconds=7200),
            )
        ]
        session = _make_multi_session(hist_rows, run_rows)
        alerts = detect(session, _default_config())

        assert len(alerts) == 1
        assert alerts[0].alert_type == AlertType.STUCK_TASK
        assert alerts[0].severity == Severity.CRITICAL
        assert alerts[0].dag_id == "etl_daily"
        assert alerts[0].task_id == "load_data"
        assert "stuck" in alerts[0].message.lower() or "zombie" in alerts[0].message.lower()

    def test_no_stuck_tasks_returns_empty(self):
        from airflow_watchdog.detectors.stuck import detect

        session = _make_multi_session([], [])
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

        now = self._now()
        hist_rows = [
            SimpleNamespace(dag_id="dag1", task_id="task_a", duration=1000.0 - i * 50)
            for i in range(5)
        ] + [
            SimpleNamespace(dag_id="dag2", task_id="task_b", duration=2000.0 - i * 100)
            for i in range(5)
        ]
        run_rows = [
            SimpleNamespace(
                dag_id="dag1",
                task_id="task_a",
                run_id="run1",
                start_date=now - timedelta(seconds=5000),
            ),
            SimpleNamespace(
                dag_id="dag2",
                task_id="task_b",
                run_id="run2",
                start_date=now - timedelta(seconds=8000),
            ),
        ]
        session = _make_multi_session(hist_rows, run_rows)
        alerts = detect(session, _default_config())

        assert len(alerts) == 2
        assert all(a.severity == Severity.CRITICAL for a in alerts)

    def test_zero_max_duration_no_division_error(self):
        from airflow_watchdog.detectors.stuck import detect

        now = self._now()
        hist_rows = [
            SimpleNamespace(dag_id="dag1", task_id="task1", duration=0.0) for _ in range(5)
        ]
        run_rows = [
            SimpleNamespace(
                dag_id="dag1",
                task_id="task1",
                run_id="run1",
                start_date=now - timedelta(seconds=100),
            )
        ]
        session = _make_multi_session(hist_rows, run_rows)
        # Should not raise ZeroDivisionError
        alerts = detect(session, _default_config())
        assert len(alerts) == 1


# ── Schedule anomaly detector ────────────────────────────────────────────────


class TestScheduleAnomalyDetector:
    def _make_row(self, dag_id, task_id, hour, minute, rn, end_hour=None, end_minute=None):
        """Create a row with start/end datetimes at the given times."""
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        start = base.replace(hour=hour, minute=minute)
        if end_hour is not None or end_minute is not None:
            end = start.replace(
                hour=end_hour if end_hour is not None else hour,
                minute=end_minute if end_minute is not None else minute,
            )
        else:
            end = start + timedelta(minutes=10)
        return SimpleNamespace(
            dag_id=dag_id, task_id=task_id, start_date=start, end_date=end, rn=rn
        )

    def test_late_start_detected(self):
        from airflow_watchdog.detectors.schedule import detect

        # 7 runs normally starting at 02:00, latest (rn=1) starts at 10:00
        rows = [self._make_row("dag1", "task1", 10, 0, 1)]  # anomaly
        rows += [
            self._make_row("dag1", "task1", 2, i, rn)
            for rn, i in enumerate([0, 5, 10, 15, 20, 25, 30], start=2)
        ]
        session = _make_session(rows)
        alerts = detect(session, _default_config())

        assert len(alerts) >= 1
        start_alerts = [a for a in alerts if a.details.get("type") == "start"]
        assert len(start_alerts) == 1
        assert start_alerts[0].alert_type == AlertType.SCHEDULE_ANOMALY
        assert "later" in start_alerts[0].message

    def test_early_start_detected(self):
        from airflow_watchdog.detectors.schedule import detect

        # Normally starts at 14:00, latest starts at 06:00
        rows = [self._make_row("dag1", "task1", 6, 0, 1)]
        rows += [
            self._make_row("dag1", "task1", 14, i, rn)
            for rn, i in enumerate([0, 5, 10, 15, 20, 25, 30], start=2)
        ]
        session = _make_session(rows)
        alerts = detect(session, _default_config())

        start_alerts = [a for a in alerts if a.details.get("type") == "start"]
        assert len(start_alerts) == 1
        assert "earlier" in start_alerts[0].message

    def test_normal_times_no_alert(self):
        from airflow_watchdog.detectors.schedule import detect

        # All times cluster around 02:00-02:30
        rows = [
            self._make_row("dag1", "task1", 2, i, rn)
            for rn, i in enumerate([15, 0, 5, 10, 20, 25, 30], start=1)
        ]
        session = _make_session(rows)
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_midnight_wraparound(self):
        from airflow_watchdog.detectors.schedule import detect

        # Times around midnight: 23:50, 23:55, 00:00, 00:05, 00:10, 00:15, 00:20
        # Latest at 06:00 should be flagged
        rows = [self._make_row("dag1", "task1", 6, 0, 1)]  # anomaly
        rows += [
            self._make_row("dag1", "task1", 23, 50, 2),
            self._make_row("dag1", "task1", 23, 55, 3),
            self._make_row("dag1", "task1", 0, 0, 4),
            self._make_row("dag1", "task1", 0, 5, 5),
            self._make_row("dag1", "task1", 0, 10, 6),
            self._make_row("dag1", "task1", 0, 15, 7),
            self._make_row("dag1", "task1", 0, 20, 8),
        ]
        session = _make_session(rows)
        alerts = detect(session, _default_config())

        start_alerts = [a for a in alerts if a.details.get("type") == "start"]
        assert len(start_alerts) >= 1

    def test_fewer_than_5_samples_skipped(self):
        from airflow_watchdog.detectors.schedule import detect

        rows = [self._make_row("dag1", "task1", 10, 0, rn) for rn in range(1, 4)]
        session = _make_session(rows)
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_query_failure_returns_empty(self):
        from airflow_watchdog.detectors.schedule import detect

        session = MagicMock()
        session.execute.side_effect = Exception("DB error")
        alerts = detect(session, _default_config())
        assert alerts == []

    def test_end_time_anomaly_detected(self):
        from airflow_watchdog.detectors.schedule import detect

        # Normal end at 03:00, latest ends at 12:00
        rows = [self._make_row("dag1", "task1", 2, 0, 1, end_hour=12, end_minute=0)]
        rows += [
            self._make_row("dag1", "task1", 2, i, rn, end_hour=3, end_minute=i)
            for rn, i in enumerate([0, 5, 10, 15, 20, 25, 30], start=2)
        ]
        session = _make_session(rows)
        alerts = detect(session, _default_config())

        end_alerts = [a for a in alerts if a.details.get("type") == "end"]
        assert len(end_alerts) == 1
