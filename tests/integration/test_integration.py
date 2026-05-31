"""End-to-end integration tests against a real Airflow metadata DB + auth.

These exercise the parts the unit tests cannot: the hand-written detector and
dashboard SQL against the real schema, the results Variable round trip that
feeds the dashboard, and that the dashboard endpoints actually enforce
authentication.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ._seed import seed_dag, seed_dag_run, seed_task_instance

pytestmark = pytest.mark.integration


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Detector SQL against the real schema ────────────────────────────────────────


class TestDetectorSQL:
    def test_runtime_anomaly_detected(self, clean_tables):
        session = clean_tables
        from airflow_watchdog.config import WatchdogConfig
        from airflow_watchdog.detectors.runtime import detect

        seed_dag(session, "etl")
        # Five normal runs (~10s) plus a latest run that is wildly slower.
        base = _now() - timedelta(hours=10)
        normals = [10.0, 12.0, 11.0, 13.0, 9.0]
        for i, dur in enumerate(normals):
            start = base + timedelta(hours=i)
            seed_dag_run(
                session, "etl", f"run_{i}", "success", start, start + timedelta(seconds=dur)
            )
            seed_task_instance(
                session,
                "etl",
                "extract",
                f"run_{i}",
                "success",
                start,
                start + timedelta(seconds=dur),
                dur,
            )
        latest = base + timedelta(hours=10)
        seed_dag_run(
            session, "etl", "run_latest", "success", latest, latest + timedelta(seconds=500)
        )
        seed_task_instance(
            session,
            "etl",
            "extract",
            "run_latest",
            "success",
            latest,
            latest + timedelta(seconds=500),
            500.0,
        )

        alerts = detect(session, WatchdogConfig())

        assert len(alerts) == 1
        assert alerts[0].dag_id == "etl"
        assert alerts[0].task_id == "extract"
        assert "slower" in alerts[0].message

    def test_failure_spike_detected(self, clean_tables):
        session = clean_tables
        from airflow_watchdog.config import WatchdogConfig
        from airflow_watchdog.detectors.failures import detect

        seed_dag(session, "flaky")
        base = _now() - timedelta(days=2)
        # 13 older runs, all successful (baseline rate 0%).
        for i in range(13):
            start = base + timedelta(hours=i)
            seed_dag_run(
                session, "flaky", f"old_{i}", "success", start, start + timedelta(minutes=1)
            )
        # 10 recent runs (most recent), 4 of them failed.
        recent_base = base + timedelta(hours=20)
        for i in range(10):
            state = "failed" if i < 4 else "success"
            start = recent_base + timedelta(hours=i)
            seed_dag_run(session, "flaky", f"new_{i}", state, start, start + timedelta(minutes=1))

        alerts = detect(session, WatchdogConfig())

        assert len(alerts) == 1
        assert alerts[0].dag_id == "flaky"
        # baseline rate was 0 → a fresh wave of failures is critical
        assert alerts[0].severity.value == "critical"

    def test_missed_deadline_uses_utc(self, clean_tables):
        session = clean_tables
        from airflow_watchdog.config import WatchdogConfig
        from airflow_watchdog.detectors.deadlines import detect

        seed_dag(session, "slowdag")
        base = _now() - timedelta(days=1)
        # History: median run ~60s.
        for i in range(5):
            start = base + timedelta(hours=i)
            seed_dag_run(
                session, "slowdag", f"h_{i}", "success", start, start + timedelta(seconds=60)
            )
        # A run that started 1 hour ago and is still running → way over 2× median.
        running_start = _now() - timedelta(hours=1)
        seed_dag_run(session, "slowdag", "stuck_run", "running", running_start, None)

        alerts = detect(session, WatchdogConfig())

        assert len(alerts) == 1
        assert alerts[0].dag_id == "slowdag"
        # ~3600s elapsed vs 120s deadline — comfortably over, proving the
        # naive timestamp was read as UTC (a non-UTC reading would be hours off).
        assert alerts[0].details["elapsed_secs"] == pytest.approx(3600, abs=120)


# ── Dashboard data round trip (real results Variable) ───────────────────────────


class TestDashboardRoundTrip:
    def test_alerts_flow_from_variable_to_dashboard(self, clean_tables):
        session = clean_tables
        import json

        from airflow.models import Variable

        from airflow_watchdog.monitor import RESULTS_VARIABLE_KEY
        from airflow_watchdog.ui.app import _get_dashboard_data

        seed_dag(session, "etl")
        start = _now() - timedelta(minutes=5)
        seed_dag_run(session, "etl", "etl_run", "success", start, start + timedelta(minutes=1))

        # Reproduce exactly what the scheduler persists: the detection summary
        # JSON in the watchdog_last_results Variable.
        summary = {
            "total_alerts": 1,
            "by_type": {"runtime_anomaly": 1},
            "alerts": [
                {
                    "type": "runtime_anomaly",
                    "severity": "critical",
                    "dag_id": "etl",
                    "task_id": "extract",
                    "message": "Latest run far slower than expected",
                    "detected_at": _now().isoformat(),
                    "details": {},
                }
            ],
            "generated_at": _now().isoformat(),
        }
        Variable.set(RESULTS_VARIABLE_KEY, json.dumps(summary))
        try:
            data = _get_dashboard_data()
        finally:
            Variable.delete(RESULTS_VARIABLE_KEY)

        assert "error" not in data
        etl = next(d for d in data["dags"] if d["dag_id"] == "etl")
        assert etl["status"] == "critical"
        assert len(etl["alerts"]) == 1
        assert etl["alerts"][0]["task_id"] == "extract"


# ── Stale DAG filtering (real is_stale column) ──────────────────────────────────


class TestStaleFiltering:
    def test_stale_dag_excluded_from_dashboard(self, clean_tables):
        session = clean_tables
        from airflow_watchdog.ui.app import _get_dashboard_data

        seed_dag(session, "active_dag")
        seed_dag(session, "removed_dag", is_stale=True)
        # The stale DAG still has leftover history that the UI hides.
        start = _now() - timedelta(minutes=5)
        seed_dag_run(session, "removed_dag", "r1", "success", start, start + timedelta(minutes=1))

        data = _get_dashboard_data()

        assert "error" not in data
        dag_ids = {d["dag_id"] for d in data["dags"]}
        assert "active_dag" in dag_ids
        assert "removed_dag" not in dag_ids

    def test_stale_dag_ids_reads_real_column(self, clean_tables):
        # Exercises the real ``WHERE is_stale = true`` SQL that feeds the
        # detector-side exclude list, against the actual schema.
        session = clean_tables
        from airflow_watchdog.monitor import _stale_dag_ids

        seed_dag(session, "active_dag")
        seed_dag(session, "removed_dag", is_stale=True)

        assert _stale_dag_ids(session) == {"removed_dag"}


# ── Authentication enforcement (real SimpleAuthManager) ──────────────────────────


class TestPluginWiring:
    """Asserts Airflow's plugin-discovery machinery actually surfaces the plugin.

    The other suites import ``watchdog_app`` / ``detect`` directly, so they
    verify the components work but never exercise the registration layer — the
    exact layer where a broken ``airflow.plugins`` entry point silently makes the
    dashboard and its scheduler disappear in a real deployment.
    """

    def test_plugin_is_discoverable_by_airflow(self):
        # Goes through plugins_manager (the ``airflow.plugins`` entry point), the
        # same path the API server uses to mount FastAPI apps and nav links.
        # Fails if the entry point is dropped or the plugin no longer exposes the
        # dashboard app.
        from airflow import plugins_manager

        plugins_manager.get_fastapi_plugins.cache_clear()
        apps, _ = plugins_manager.get_fastapi_plugins()

        prefixes = [a.get("url_prefix") for a in apps]
        assert "/watchdog" in prefixes, f"watchdog plugin was not discovered: {prefixes}"

    def test_navbar_link_surfaced_by_airflow(self):
        # external_views feeds the Airflow 3 navbar. Airflow validates each entry
        # and silently drops malformed ones, so go through the real UI-plugin path
        # to prove our nav link is actually accepted and surfaced.
        from airflow import plugins_manager

        plugins_manager._get_ui_plugins.cache_clear()
        external_views, _ = plugins_manager._get_ui_plugins()

        hrefs = [v.get("href") or "" for v in external_views]
        assert any(h.endswith("/watchdog/") for h in hrefs), (
            f"watchdog nav link not surfaced: {external_views}"
        )

    def test_scheduler_starts_with_app_lifespan(self):
        # The detection scheduler is started by the plugin app's FastAPI
        # lifespan, which Airflow runs only in the API server. Drive the lifespan
        # and assert the scheduler thread actually comes up (then shuts down).
        from fastapi.testclient import TestClient

        from airflow_watchdog import scheduler
        from airflow_watchdog.ui.app import watchdog_app

        assert not scheduler.is_running()
        with TestClient(watchdog_app):  # entering the context runs startup
            assert scheduler.is_running()
        assert not scheduler.is_running()


class TestAuthEnforcement:
    def test_api_data_requires_auth(self, clean_tables):
        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        watchdog_app.dependency_overrides.clear()
        client = TestClient(watchdog_app)
        resp = client.get("/api/data")
        assert resp.status_code == 401

    def test_api_data_allows_valid_token(self, clean_tables, auth_token):
        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        watchdog_app.dependency_overrides.clear()
        client = TestClient(watchdog_app)
        client.cookies.set("_token", auth_token)
        resp = client.get("/api/data")
        assert resp.status_code == 200
        assert "dags" in resp.json()

    def test_config_write_allows_admin(self, clean_tables, auth_token):
        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        watchdog_app.dependency_overrides.clear()
        client = TestClient(watchdog_app)
        client.cookies.set("_token", auth_token)
        resp = client.post(
            "/api/config",
            json={"disable_detectors": ["stuck_task"], "dag_overrides": {}},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True
