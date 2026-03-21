"""Tests for watchdog config."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from airflow_watchdog.config import WatchdogConfig, load_config


def test_defaults():
    """Config loads sensible defaults when no Variable is set."""
    mock_models = MagicMock()
    mock_models.Variable.get.return_value = "{}"
    with patch.dict("sys.modules", {"airflow.models": mock_models, "airflow": MagicMock()}):
        config = load_config()

    assert isinstance(config, WatchdogConfig)
    assert config.lookback_runs == 20
    assert config.runtime_iqr_multiplier == 1.5
    assert config.failure_spike_ratio == 2.0
    assert "airflow_watchdog_monitor" in config.exclude_dags


def test_overrides():
    """Config merges Variable overrides with defaults."""
    import json

    overrides = json.dumps({"lookback_runs": 50, "runtime_iqr_multiplier": 2.0})

    mock_models = MagicMock()
    mock_models.Variable.get.return_value = overrides
    with patch.dict("sys.modules", {"airflow.models": mock_models, "airflow": MagicMock()}):
        config = load_config()

    assert config.lookback_runs == 50
    assert config.runtime_iqr_multiplier == 2.0
    # Unchanged defaults still apply
    assert config.failure_spike_ratio == 2.0


def test_watchdog_dag_always_excluded():
    """The watchdog's own DAG is always in exclude_dags."""
    import json

    overrides = json.dumps({"exclude_dags": ["my_dag"]})

    mock_models = MagicMock()
    mock_models.Variable.get.return_value = overrides
    with patch.dict("sys.modules", {"airflow.models": mock_models, "airflow": MagicMock()}):
        config = load_config()

    assert "airflow_watchdog_monitor" in config.exclude_dags
    assert "my_dag" in config.exclude_dags


def test_fallback_on_import_error():
    """Config returns defaults if Airflow Variable is unavailable."""
    # When import fails, load_config catches the exception and returns defaults
    with patch.dict("sys.modules", {"airflow.models": None}):
        config = load_config()

    assert isinstance(config, WatchdogConfig)
    assert config.lookback_runs == 20


def test_is_detector_enabled_default():
    """All detectors enabled by default."""
    config = WatchdogConfig()
    assert config.is_detector_enabled("runtime_anomaly", "any_dag")
    assert config.is_detector_enabled("schedule_anomaly", "any_dag")


def test_is_detector_disabled_globally():
    """Globally disabled detectors are blocked for all DAGs."""
    config = WatchdogConfig(disable_detectors=["schedule_anomaly"])
    assert not config.is_detector_enabled("schedule_anomaly", "any_dag")
    assert config.is_detector_enabled("runtime_anomaly", "any_dag")


def test_is_detector_disabled_per_dag():
    """Per-DAG overrides disable detectors only for specific DAGs."""
    config = WatchdogConfig(
        dag_overrides={"event_dag": {"disable_detectors": ["schedule_anomaly"]}}
    )
    assert not config.is_detector_enabled("schedule_anomaly", "event_dag")
    assert config.is_detector_enabled("schedule_anomaly", "other_dag")
    assert config.is_detector_enabled("runtime_anomaly", "event_dag")
