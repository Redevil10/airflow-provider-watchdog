"""End-to-end integration tests against a real Airflow metadata DB + auth.

These exercise the parts the unit tests cannot: the hand-written detector and
dashboard SQL against the real schema, the XCom serialize/deserialize round
trip, and that the dashboard endpoints actually enforce authentication.
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


# ── Dashboard data round trip (real XCom serialize/deserialize) ──────────────────


class TestDashboardRoundTrip:
    def test_alerts_flow_from_xcom_to_dashboard(self, clean_tables):
        session = clean_tables
        from airflow.models.xcom import XComModel

        from airflow_watchdog.ui.app import _get_dashboard_data

        seed_dag(session, "etl")
        start = _now() - timedelta(minutes=5)
        seed_dag_run(session, "etl", "etl_run", "success", start, start + timedelta(minutes=1))

        # Reproduce exactly what the (fixed) watchdog DAG pushes: a dict value.
        wd_start = _now() - timedelta(minutes=2)
        wd_run_id = "wd_run"
        seed_dag(session, "airflow_watchdog_monitor")
        seed_dag_run(
            session,
            "airflow_watchdog_monitor",
            wd_run_id,
            "success",
            wd_start,
            wd_start + timedelta(seconds=5),
        )
        # XCom has a FK to task_instance; the push happens from run_detectors.
        seed_task_instance(
            session,
            "airflow_watchdog_monitor",
            "run_detectors",
            wd_run_id,
            "success",
            wd_start,
            wd_start + timedelta(seconds=5),
            5.0,
        )
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
        }
        XComModel.set(
            key="watchdog_results",
            value=summary,
            dag_id="airflow_watchdog_monitor",
            task_id="run_detectors",
            run_id=wd_run_id,
            session=session,
        )
        session.commit()

        data = _get_dashboard_data()

        assert "error" not in data
        etl = next(d for d in data["dags"] if d["dag_id"] == "etl")
        assert etl["status"] == "critical"
        assert len(etl["alerts"]) == 1
        assert etl["alerts"][0]["task_id"] == "extract"


# ── Authentication enforcement (real SimpleAuthManager) ──────────────────────────


class TestProviderWiring:
    """Asserts Airflow's *discovery* machinery actually surfaces the provider.

    The other suites import ``watchdog_app`` / ``detect`` directly, so they
    verify the components work but never exercise the registration layer — the
    exact layer where a missing ``plugins`` key or an invalid DAG silently makes
    the dashboard and monitor DAG disappear in a real deployment.
    """

    def test_plugin_is_discoverable_by_airflow(self):
        # Goes through ProvidersManager (provider_info + entry points), the same
        # path the webserver uses to mount FastAPI apps and nav links. Fails if
        # get_provider_info() drops the ``plugins`` key.
        from airflow.providers_manager import ProvidersManager

        pm = ProvidersManager()
        pm.initialize_providers_plugins()

        watchdog = next((p for p in pm.plugins if p.name == "watchdog"), None)
        assert watchdog is not None, "WatchdogPlugin was not discovered by Airflow"
        assert watchdog.plugin_class == "airflow_watchdog.plugin.WatchdogPlugin"

    def test_monitor_dag_loads_in_dagbag(self):
        # Parses dag.py exactly as the scheduler's DagBag would. Fails if the DAG
        # has an import/parse error or the dag_id ever drifts.
        from airflow.dag_processing.dagbag import DagBag

        import airflow_watchdog.dag as dag_mod

        bag = DagBag(dag_folder=dag_mod.__file__, include_examples=False)

        assert bag.import_errors == {}, f"DAG failed to parse: {bag.import_errors}"
        assert "airflow_watchdog_monitor" in bag.dags


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
