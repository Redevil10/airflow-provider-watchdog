"""Tests for the watchdog dashboard."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture()
def _mock_airflow():
    """Patch Airflow imports so dashboard code can be loaded without Airflow."""
    mock_session_cls = MagicMock()
    mock_settings = MagicMock()
    mock_settings.Session = mock_session_cls
    with patch.dict(
        "sys.modules",
        {
            "airflow": MagicMock(),
            "airflow.settings": mock_settings,
        },
    ):
        yield mock_session_cls


def _make_session(dag_rows, run_rows, alert_row=None):
    """Build a mock session that returns controlled rows for each query."""
    session = MagicMock()
    results = []

    # Query 1: dag table
    r1 = MagicMock()
    r1.fetchall.return_value = dag_rows
    results.append(r1)

    # Query 2: dag_run with ROW_NUMBER
    r2 = MagicMock()
    r2.fetchall.return_value = run_rows
    results.append(r2)

    # Query 3: xcom alerts
    r3 = MagicMock()
    r3.fetchone.return_value = alert_row
    results.append(r3)

    session.execute.side_effect = results
    return session


class TestGetDashboardData:
    @pytest.mark.usefixtures("_mock_airflow")
    def test_returns_empty_structure_when_no_data(self, _mock_airflow):
        session = _make_session([], [], None)
        _mock_airflow.return_value = session

        from airflow_watchdog.ui.app import _get_dashboard_data

        data = _get_dashboard_data()

        assert data["dags"] == []
        assert data["alerts"] == []
        assert data["summary"]["total_dags"] == 0
        assert "error" not in data

    @pytest.mark.usefixtures("_mock_airflow")
    def test_assembles_dag_with_run_and_alerts(self, _mock_airflow):
        now = datetime.now(timezone.utc)

        dag_rows = [SimpleNamespace(dag_id="etl_daily", is_paused=False)]
        run_rows = [
            SimpleNamespace(
                dag_id="etl_daily",
                state="success",
                start_date=now - timedelta(seconds=300),
                end_date=now,
                rn=1,
            )
        ]
        alert_json = json.dumps(
            {
                "alerts": [
                    {
                        "dag_id": "etl_daily",
                        "severity": "warning",
                        "message": "slow",
                    }
                ]
            }
        )
        alert_row = SimpleNamespace(value=alert_json)

        session = _make_session(dag_rows, run_rows, alert_row)
        _mock_airflow.return_value = session

        from airflow_watchdog.ui.app import _get_dashboard_data

        data = _get_dashboard_data()

        assert len(data["dags"]) == 1
        dag = data["dags"][0]
        assert dag["dag_id"] == "etl_daily"
        assert dag["status"] == "warning"
        assert dag["last_run_state"] == "success"
        assert dag["last_run_duration_secs"] is not None
        assert dag["last_run_duration_secs"] == pytest.approx(300, abs=2)
        assert len(dag["alerts"]) == 1
        assert data["summary"]["warning"] == 1

    @pytest.mark.usefixtures("_mock_airflow")
    def test_critical_sorted_before_healthy(self, _mock_airflow):
        dag_rows = [
            SimpleNamespace(dag_id="aaa_healthy", is_paused=False),
            SimpleNamespace(dag_id="zzz_critical", is_paused=False),
        ]
        run_rows = []
        alert_json = json.dumps(
            {"alerts": [{"dag_id": "zzz_critical", "severity": "critical", "message": "bad"}]}
        )
        alert_row = SimpleNamespace(value=alert_json)

        session = _make_session(dag_rows, run_rows, alert_row)
        _mock_airflow.return_value = session

        from airflow_watchdog.ui.app import _get_dashboard_data

        data = _get_dashboard_data()

        assert data["dags"][0]["dag_id"] == "zzz_critical"
        assert data["dags"][1]["dag_id"] == "aaa_healthy"

    @pytest.mark.usefixtures("_mock_airflow")
    def test_db_error_sets_error_key(self, _mock_airflow):
        session = MagicMock()
        session.execute.side_effect = Exception("DB down")
        _mock_airflow.return_value = session

        from airflow_watchdog.ui.app import _get_dashboard_data

        data = _get_dashboard_data()

        assert "error" in data
        assert data["dags"] == []

    @pytest.mark.usefixtures("_mock_airflow")
    def test_dag_without_run_has_null_duration(self, _mock_airflow):
        dag_rows = [SimpleNamespace(dag_id="new_dag", is_paused=False)]
        run_rows = []

        session = _make_session(dag_rows, run_rows, None)
        _mock_airflow.return_value = session

        from airflow_watchdog.ui.app import _get_dashboard_data

        data = _get_dashboard_data()

        dag = data["dags"][0]
        assert dag["last_run_state"] is None
        assert dag["last_run_start"] is None
        assert dag["last_run_duration_secs"] is None


class TestDashboardEndpoints:
    @pytest.mark.usefixtures("_mock_airflow")
    def test_dashboard_returns_html(self, _mock_airflow):
        session = _make_session([], [], None)
        _mock_airflow.return_value = session

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        resp = client.get("/")

        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Watchdog Dashboard" in resp.text

    @pytest.mark.usefixtures("_mock_airflow")
    def test_dashboard_html_escapes_json(self, _mock_airflow):
        dag_rows = [SimpleNamespace(dag_id="<script>alert(1)</script>", is_paused=False)]
        session = _make_session(dag_rows, [], None)
        _mock_airflow.return_value = session

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        resp = client.get("/")

        assert "<script>alert(1)</script>" not in resp.text
        assert "\\u003cscript\\u003e" in resp.text

    @pytest.mark.usefixtures("_mock_airflow")
    def test_api_data_returns_json(self, _mock_airflow):
        session = _make_session([], [], None)
        _mock_airflow.return_value = session

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        resp = client.get("/api/data")

        assert resp.status_code == 200
        body = resp.json()
        assert "dags" in body
        assert "summary" in body


# ── Config UI tests ──────────────────────────────────────────────────────────


@pytest.fixture()
def _mock_airflow_with_config():
    """Patch Airflow imports for config endpoints."""
    mock_session_cls = MagicMock()
    mock_settings = MagicMock()
    mock_settings.Session = mock_session_cls

    mock_variable = MagicMock()
    mock_variable.get.return_value = json.dumps(
        {
            "disable_detectors": ["stuck_task"],
            "dag_overrides": {"etl": {"disable_detectors": ["runtime_anomaly"]}},
        }
    )
    mock_models = MagicMock()
    mock_models.Variable = mock_variable

    with patch.dict(
        "sys.modules",
        {
            "airflow": MagicMock(),
            "airflow.settings": mock_settings,
            "airflow.models": mock_models,
        },
    ):
        yield mock_session_cls, mock_variable


class TestConfigEndpoints:
    @pytest.mark.usefixtures("_mock_airflow_with_config")
    def test_config_page_returns_html(self, _mock_airflow_with_config):
        mock_session_cls, _ = _mock_airflow_with_config
        session = MagicMock()
        # DAG query returns one row
        r1 = MagicMock()
        r1.fetchall.return_value = [SimpleNamespace(dag_id="etl_daily")]
        session.execute.return_value = r1
        mock_session_cls.return_value = session

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        resp = client.get("/config")

        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Watchdog Configuration" in resp.text

    @pytest.mark.usefixtures("_mock_airflow_with_config")
    def test_api_config_get(self, _mock_airflow_with_config):
        mock_session_cls, _ = _mock_airflow_with_config
        session = MagicMock()
        r1 = MagicMock()
        r1.fetchall.return_value = [SimpleNamespace(dag_id="etl_daily")]
        session.execute.return_value = r1
        mock_session_cls.return_value = session

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        resp = client.get("/api/config")

        assert resp.status_code == 200
        body = resp.json()
        assert "detector_names" in body
        assert "config" in body
        assert "dags" in body

    @pytest.mark.usefixtures("_mock_airflow_with_config")
    def test_api_config_save(self, _mock_airflow_with_config):
        _, mock_variable = _mock_airflow_with_config

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        payload = {
            "disable_detectors": ["schedule_anomaly"],
            "dag_overrides": {"etl": {"disable_detectors": ["runtime_anomaly"]}},
        }
        resp = client.post("/api/config", json=payload)

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        # Verify Variable.set was called
        mock_variable.set.assert_called_once()

    @pytest.mark.usefixtures("_mock_airflow_with_config")
    def test_api_config_save_cleans_empty_overrides(self, _mock_airflow_with_config):
        _, mock_variable = _mock_airflow_with_config

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        payload = {
            "disable_detectors": [],
            "dag_overrides": {"etl": {"disable_detectors": []}},
        }
        resp = client.post("/api/config", json=payload)

        assert resp.status_code == 200
        # Check that empty overrides are stripped from saved JSON
        saved_json = mock_variable.set.call_args[0][1]
        saved = json.loads(saved_json)
        assert "etl" not in saved.get("dag_overrides", {})
