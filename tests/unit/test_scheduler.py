"""Tests for the background detection scheduler."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch


class TestIntervalSeconds:
    def test_normal_interval(self):
        from airflow_watchdog import scheduler
        from airflow_watchdog.config import WatchdogConfig

        assert scheduler._interval_seconds(WatchdogConfig(schedule_interval_minutes=30)) == 1800.0

    def test_floored_at_minimum(self):
        from airflow_watchdog import scheduler
        from airflow_watchdog.config import WatchdogConfig

        # A tiny configured interval can't drop below the floor.
        assert (
            scheduler._interval_seconds(WatchdogConfig(schedule_interval_minutes=0))
            == scheduler._MIN_INTERVAL_SECONDS
        )


class TestRanRecently:
    def _cfg(self):
        from airflow_watchdog.config import WatchdogConfig

        return WatchdogConfig(schedule_interval_minutes=30)

    def test_false_when_no_prior_run(self):
        from airflow_watchdog import scheduler

        with patch.object(scheduler, "load_results", return_value={"generated_at": None}):
            assert scheduler._ran_recently(self._cfg()) is False

    def test_true_when_run_within_window(self):
        from airflow_watchdog import scheduler

        recent = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
        with patch.object(scheduler, "load_results", return_value={"generated_at": recent}):
            assert scheduler._ran_recently(self._cfg()) is True

    def test_false_when_run_is_old(self):
        from airflow_watchdog import scheduler

        old = (datetime.now(timezone.utc) - timedelta(minutes=40)).isoformat()
        with patch.object(scheduler, "load_results", return_value={"generated_at": old}):
            assert scheduler._ran_recently(self._cfg()) is False


class TestAdvisoryLock:
    def test_sqlite_is_noop_and_closes_session(self):
        from airflow_watchdog import scheduler

        session = MagicMock()
        session.bind.dialect.name = "sqlite"
        settings = MagicMock()
        settings.Session.return_value = session

        with patch.dict("sys.modules", {"airflow": MagicMock(), "airflow.settings": settings}):
            with scheduler._advisory_lock() as acquired:
                assert acquired is True
            # No lock/unlock queries on sqlite, but the session must be closed.
            session.execute.assert_not_called()
            session.close.assert_called_once()

    def test_postgres_acquires_and_releases(self):
        from airflow_watchdog import scheduler

        session = MagicMock()
        session.bind.dialect.name = "postgresql"
        session.execute.return_value.scalar.return_value = True
        settings = MagicMock()
        settings.Session.return_value = session

        with patch.dict("sys.modules", {"airflow": MagicMock(), "airflow.settings": settings}):
            with scheduler._advisory_lock() as acquired:
                assert acquired is True
            # One execute to acquire, one to release.
            assert session.execute.call_count == 2
            session.close.assert_called_once()


class TestLifecycle:
    def test_start_stop_idempotent(self):
        from airflow_watchdog import scheduler

        assert not scheduler.is_running()
        try:
            scheduler.start()
            assert scheduler.is_running()
            scheduler.start()  # idempotent: no second thread
            assert scheduler.is_running()
        finally:
            scheduler.stop()
        assert not scheduler.is_running()
