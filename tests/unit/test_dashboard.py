"""Tests for the watchdog dashboard."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


@contextmanager
def _auth_disabled():
    """Bypass Airflow auth dependencies for endpoint tests.

    The TestClient drives the standalone FastAPI app directly, so there is no
    Airflow API server (and no auth manager) to satisfy the real dependencies.
    """
    from airflow_watchdog.ui.app import (
        _require_variable_write,
        _require_view_access,
        watchdog_app,
    )

    watchdog_app.dependency_overrides[_require_view_access] = lambda: None
    watchdog_app.dependency_overrides[_require_variable_write] = lambda: None
    try:
        yield
    finally:
        watchdog_app.dependency_overrides.clear()


@pytest.fixture()
def _mock_airflow():
    """Patch Airflow imports so dashboard code can be loaded without Airflow.

    Yields ``(session_cls, variable)``: the mocked ``Session`` class (set
    ``.return_value`` to the per-test session) and the mocked ``Variable`` whose
    ``get`` returns the stored detection summary (``None`` by default).
    """
    mock_session_cls = MagicMock()
    mock_settings = MagicMock()
    mock_settings.Session = mock_session_cls

    mock_variable = MagicMock()
    mock_variable.get.return_value = None  # no stored results unless a test sets it
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
        with _auth_disabled():
            yield mock_session_cls, mock_variable


def _make_session(dag_rows, run_rows):
    """Build a mock session that returns controlled rows for each query.

    The dashboard issues two live queries (DAGs, then latest runs); alerts come
    from the ``watchdog_last_results`` Variable, not from a query.
    """
    session = MagicMock()

    r1 = MagicMock()
    r1.fetchall.return_value = dag_rows
    r2 = MagicMock()
    r2.fetchall.return_value = run_rows

    session.execute.side_effect = [r1, r2]
    return session


def _alerts_json(alerts: list[dict]) -> str:
    """Serialize an alert list the way the results Variable stores it."""
    return json.dumps({"total_alerts": len(alerts), "by_type": {}, "alerts": alerts})


class TestGetDashboardData:
    def test_returns_empty_structure_when_no_data(self, _mock_airflow):
        mock_session_cls, _ = _mock_airflow
        mock_session_cls.return_value = _make_session([], [])

        from airflow_watchdog.ui.app import _get_dashboard_data

        data = _get_dashboard_data()

        assert data["dags"] == []
        assert data["alerts"] == []
        assert data["summary"]["total_dags"] == 0
        assert "error" not in data

    def test_assembles_dag_with_run_and_alerts(self, _mock_airflow):
        mock_session_cls, mock_variable = _mock_airflow
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
        mock_session_cls.return_value = _make_session(dag_rows, run_rows)
        mock_variable.get.return_value = _alerts_json(
            [{"dag_id": "etl_daily", "severity": "warning", "message": "slow"}]
        )

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

    def test_critical_sorted_before_healthy(self, _mock_airflow):
        mock_session_cls, mock_variable = _mock_airflow
        dag_rows = [
            SimpleNamespace(dag_id="aaa_healthy", is_paused=False),
            SimpleNamespace(dag_id="zzz_critical", is_paused=False),
        ]
        mock_session_cls.return_value = _make_session(dag_rows, [])
        mock_variable.get.return_value = _alerts_json(
            [{"dag_id": "zzz_critical", "severity": "critical", "message": "bad"}]
        )

        from airflow_watchdog.ui.app import _get_dashboard_data

        data = _get_dashboard_data()

        assert data["dags"][0]["dag_id"] == "zzz_critical"
        assert data["dags"][1]["dag_id"] == "aaa_healthy"

    def test_db_error_sets_error_key(self, _mock_airflow):
        mock_session_cls, _ = _mock_airflow
        session = MagicMock()
        session.execute.side_effect = Exception("DB down")
        mock_session_cls.return_value = session

        from airflow_watchdog.ui.app import _get_dashboard_data

        data = _get_dashboard_data()

        assert "error" in data
        assert data["dags"] == []

    def test_dag_without_run_has_null_duration(self, _mock_airflow):
        mock_session_cls, _ = _mock_airflow
        dag_rows = [SimpleNamespace(dag_id="new_dag", is_paused=False)]
        mock_session_cls.return_value = _make_session(dag_rows, [])

        from airflow_watchdog.ui.app import _get_dashboard_data

        data = _get_dashboard_data()

        dag = data["dags"][0]
        assert dag["last_run_state"] is None
        assert dag["last_run_start"] is None
        assert dag["last_run_duration_secs"] is None


class TestDashboardEndpoints:
    def test_dashboard_returns_html(self, _mock_airflow):
        mock_session_cls, _ = _mock_airflow
        mock_session_cls.return_value = _make_session([], [])

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        resp = client.get("/")

        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Watchdog Dashboard" in resp.text

    def test_dashboard_html_escapes_json(self, _mock_airflow):
        mock_session_cls, _ = _mock_airflow
        dag_rows = [SimpleNamespace(dag_id="<script>alert(1)</script>", is_paused=False)]
        mock_session_cls.return_value = _make_session(dag_rows, [])

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        resp = client.get("/")

        assert "<script>alert(1)</script>" not in resp.text
        assert "\\u003cscript\\u003e" in resp.text

    def test_api_data_returns_json(self, _mock_airflow):
        mock_session_cls, _ = _mock_airflow
        mock_session_cls.return_value = _make_session([], [])

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
        with _auth_disabled():
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

    @pytest.mark.usefixtures("_mock_airflow_with_config")
    def test_api_config_save_persists_params(self, _mock_airflow_with_config):
        _, mock_variable = _mock_airflow_with_config

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        payload = {
            "disable_detectors": [],
            "dag_overrides": {},
            "params": {
                "runtime_min_deviation_secs": 12.5,
                "schedule_min_deviation_minutes": 8,
                "exclude_dags": ["legacy_etl"],
                "alert_emails": ["team@example.com"],
                "alert_slack_webhook": "https://hooks.slack.com/x",
                "alert_teams_webhook": None,
            },
        }
        resp = client.post("/api/config", json=payload)

        assert resp.status_code == 200
        assert resp.json()["success"] is True
        saved = json.loads(mock_variable.set.call_args[0][1])
        assert saved["runtime_min_deviation_secs"] == 12.5
        assert saved["schedule_min_deviation_minutes"] == 8
        assert saved["exclude_dags"] == ["legacy_etl"]
        assert saved["alert_emails"] == ["team@example.com"]
        assert saved["alert_slack_webhook"] == "https://hooks.slack.com/x"
        assert saved["alert_teams_webhook"] is None

    @pytest.mark.usefixtures("_mock_airflow_with_config")
    def test_api_config_save_rejects_negative_threshold(self, _mock_airflow_with_config):
        _, mock_variable = _mock_airflow_with_config

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        payload = {
            "disable_detectors": [],
            "dag_overrides": {},
            "params": {"runtime_min_deviation_secs": -1},
        }
        resp = client.post("/api/config", json=payload)

        assert resp.status_code == 200
        assert resp.json()["success"] is False
        mock_variable.set.assert_not_called()

    def test_validate_param_rejects_non_finite_numbers(self):
        # NaN/Infinity pass json.loads but would make json.dumps emit invalid
        # JSON the browser can't parse, so they must be rejected.
        from airflow_watchdog.ui.app import _validate_param

        assert _validate_param("runtime_min_deviation_secs", float("nan")) is not None
        assert _validate_param("runtime_min_deviation_secs", float("inf")) is not None
        assert _validate_param("runtime_min_deviation_secs", float("-inf")) is not None
        assert _validate_param("runtime_min_deviation_secs", 5.0) is None

    @pytest.mark.usefixtures("_mock_airflow_with_config")
    def test_api_config_save_rejects_unknown_param(self, _mock_airflow_with_config):
        _, mock_variable = _mock_airflow_with_config

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        client = TestClient(watchdog_app)
        payload = {
            "disable_detectors": [],
            "dag_overrides": {},
            "params": {"not_a_real_param": 1},
        }
        resp = client.post("/api/config", json=payload)

        assert resp.status_code == 200
        assert resp.json()["success"] is False
        mock_variable.set.assert_not_called()


# ── Authentication tests ───────────────────────────────────────────────────────


@contextmanager
def _patch_auth_modules(authorized: bool):
    """Install fake Airflow auth submodules so the auth dependencies resolve.

    *authorized* controls whether the fake auth manager grants access. The
    resolver treats a missing ``_token`` cookie as unauthenticated (401).
    """
    from fastapi import HTTPException

    auth_manager = MagicMock()
    auth_manager.is_authorized_view.return_value = authorized
    auth_manager.is_authorized_variable.return_value = authorized

    async def _resolve(token_str):
        if not token_str:
            raise HTTPException(status_code=401, detail="Not authenticated")
        return MagicMock(name="user")

    app_mod = MagicMock()
    app_mod.get_auth_manager.return_value = auth_manager

    security_mod = MagicMock()
    security_mod.resolve_user_from_token = _resolve

    base_mod = MagicMock()
    base_mod.COOKIE_NAME_JWT_TOKEN = "_token"

    modules = {
        "airflow.api_fastapi.app": app_mod,
        "airflow.api_fastapi.core_api.security": security_mod,
        "airflow.api_fastapi.auth.managers.base_auth_manager": base_mod,
        "airflow.api_fastapi.auth.managers.models.resource_details": MagicMock(),
    }
    with patch.dict("sys.modules", modules):
        yield


class TestAuth:
    def test_view_endpoint_rejects_unauthenticated(self, _mock_airflow):
        mock_session_cls, _ = _mock_airflow
        mock_session_cls.return_value = _make_session([], [])

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        # Drop the bypass installed by the fixture so the real dependency runs.
        watchdog_app.dependency_overrides.clear()

        client = TestClient(watchdog_app)
        with _patch_auth_modules(authorized=True):
            resp = client.get("/api/data")
        assert resp.status_code == 401

    def test_view_endpoint_rejects_unauthorized(self, _mock_airflow):
        mock_session_cls, _ = _mock_airflow
        mock_session_cls.return_value = _make_session([], [])

        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        watchdog_app.dependency_overrides.clear()

        client = TestClient(watchdog_app)
        client.cookies.set("_token", "abc")
        with _patch_auth_modules(authorized=False):
            resp = client.get("/api/data")
        assert resp.status_code == 403

    @pytest.mark.usefixtures("_mock_airflow_with_config")
    def test_config_write_rejects_unauthorized(self, _mock_airflow_with_config):
        from fastapi.testclient import TestClient

        from airflow_watchdog.ui.app import watchdog_app

        watchdog_app.dependency_overrides.clear()

        client = TestClient(watchdog_app)
        client.cookies.set("_token", "abc")
        with _patch_auth_modules(authorized=False):
            resp = client.post(
                "/api/config",
                json={"disable_detectors": [], "dag_overrides": {}},
            )
        assert resp.status_code == 403
