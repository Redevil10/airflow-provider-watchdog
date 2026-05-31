"""Tests for the detection runner (monitor.py)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


def _make_alert(severity: str, dag_id: str = "d", alert_type: str = "runtime_anomaly"):
    from airflow_watchdog.detectors import Alert, AlertType, Severity

    return Alert(
        alert_type=AlertType(alert_type),
        severity=Severity(severity),
        dag_id=dag_id,
        message="m",
    )


class TestBuildSummary:
    def test_counts_every_alert_but_caps_stored_list(self):
        from airflow_watchdog import monitor

        alerts = [_make_alert("warning", dag_id=f"d{i}") for i in range(120)]
        summary = monitor._build_summary(alerts)

        assert summary["total_alerts"] == 120
        assert summary["by_type"]["runtime_anomaly"] == 120  # counts are not capped
        assert len(summary["alerts"]) == monitor._MAX_STORED_ALERTS  # list is capped
        assert summary["generated_at"] is not None

    def test_keeps_most_severe_when_capping(self):
        from airflow_watchdog import monitor

        # One critical buried among many warnings; it must survive the cap.
        alerts = [_make_alert("warning", dag_id=f"w{i}") for i in range(60)]
        alerts.append(_make_alert("critical", dag_id="crit"))
        summary = monitor._build_summary(alerts)

        stored = {a["dag_id"] for a in summary["alerts"]}
        assert "crit" in stored
        assert summary["alerts"][0]["severity"] == "critical"


class TestLoadResults:
    def _patch_variable(self, get_return):
        mock_variable = MagicMock()
        mock_variable.get.return_value = get_return
        mock_models = MagicMock()
        mock_models.Variable = mock_variable
        return patch.dict("sys.modules", {"airflow": MagicMock(), "airflow.models": mock_models})

    def test_empty_when_variable_missing(self):
        from airflow_watchdog import monitor

        with self._patch_variable(None):
            results = monitor.load_results()
        assert results == {"total_alerts": 0, "by_type": {}, "alerts": [], "generated_at": None}

    def test_parses_stored_json(self):
        from airflow_watchdog import monitor

        stored = {"total_alerts": 2, "by_type": {}, "alerts": [], "generated_at": "x"}
        with self._patch_variable(json.dumps(stored)):
            results = monitor.load_results()
        assert results["total_alerts"] == 2

    def test_empty_on_invalid_json(self):
        from airflow_watchdog import monitor

        with self._patch_variable("not json{"):
            results = monitor.load_results()
        assert results["total_alerts"] == 0


class TestStaleDagIds:
    def test_returns_dag_ids_from_rows(self):
        from airflow_watchdog import monitor

        session = MagicMock()
        session.execute.return_value.fetchall.return_value = [
            SimpleNamespace(dag_id="old_etl"),
            SimpleNamespace(dag_id="deleted_dag"),
        ]
        assert monitor._stale_dag_ids(session) == {"old_etl", "deleted_dag"}

    def test_empty_set_on_query_error(self):
        from airflow_watchdog import monitor

        session = MagicMock()
        session.execute.side_effect = RuntimeError("db down")
        assert monitor._stale_dag_ids(session) == set()


class TestRunDetection:
    def test_stale_dags_are_added_to_exclude_dags(self):
        from airflow_watchdog import monitor
        from airflow_watchdog.config import WatchdogConfig

        cfg = WatchdogConfig(exclude_dags=["manually_excluded"])
        with (
            patch.object(monitor, "_store_results"),
            patch.object(monitor, "_stale_dag_ids", return_value={"stale_a", "stale_b"}),
            patch("airflow_watchdog.alerting.dispatch"),
            patch("airflow_watchdog.detectors.runtime.detect", return_value=[]) as runtime_detect,
            patch("airflow_watchdog.detectors.failures.detect", return_value=[]),
            patch("airflow_watchdog.detectors.deadlines.detect", return_value=[]),
            patch("airflow_watchdog.detectors.stuck.detect", return_value=[]),
            patch("airflow_watchdog.detectors.schedule.detect", return_value=[]),
            patch.dict(
                "sys.modules",
                {"airflow": MagicMock(), "airflow.settings": MagicMock(Session=MagicMock())},
            ),
        ):
            monitor.run_detection(cfg)

        # Detectors receive a config whose exclude_dags merges stale + manual.
        passed_config = runtime_detect.call_args.args[1]
        assert set(passed_config.exclude_dags) == {"manually_excluded", "stale_a", "stale_b"}
        # The caller's config is not mutated.
        assert cfg.exclude_dags == ["manually_excluded"]

    def test_dispatches_and_stores(self):
        from airflow_watchdog import monitor
        from airflow_watchdog.config import WatchdogConfig

        cfg = WatchdogConfig()
        crit = [_make_alert("critical")]
        # One detector yields a critical alert; the rest yield nothing.
        with (
            patch.object(monitor, "_store_results") as store,
            patch("airflow_watchdog.alerting.dispatch") as dispatch,
            patch("airflow_watchdog.detectors.runtime.detect", return_value=crit),
            patch("airflow_watchdog.detectors.failures.detect", return_value=[]),
            patch("airflow_watchdog.detectors.deadlines.detect", return_value=[]),
            patch("airflow_watchdog.detectors.stuck.detect", return_value=[]),
            patch("airflow_watchdog.detectors.schedule.detect", return_value=[]),
            patch.dict(
                "sys.modules",
                {"airflow": MagicMock(), "airflow.settings": MagicMock(Session=MagicMock())},
            ),
        ):
            summary = monitor.run_detection(cfg)

        assert summary["total_alerts"] == 1
        dispatch.assert_called_once()
        store.assert_called_once()

    def test_disabled_detector_is_skipped(self):
        from airflow_watchdog import monitor
        from airflow_watchdog.config import WatchdogConfig

        cfg = WatchdogConfig(disable_detectors=["runtime_anomaly"])
        with (
            patch.object(monitor, "_store_results"),
            patch("airflow_watchdog.alerting.dispatch"),
            patch("airflow_watchdog.detectors.runtime.detect") as runtime_detect,
            patch("airflow_watchdog.detectors.failures.detect", return_value=[]),
            patch("airflow_watchdog.detectors.deadlines.detect", return_value=[]),
            patch("airflow_watchdog.detectors.stuck.detect", return_value=[]),
            patch("airflow_watchdog.detectors.schedule.detect", return_value=[]),
            patch.dict(
                "sys.modules",
                {"airflow": MagicMock(), "airflow.settings": MagicMock(Session=MagicMock())},
            ),
        ):
            monitor.run_detection(cfg)

        runtime_detect.assert_not_called()
