"""Tests for watchdog config."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from airflow_watchdog.config import WatchdogConfig, load_config


def _load_with(raw):
    """Run load_config with the Variable returning ``raw`` (str or already-parsed)."""
    mock_models = MagicMock()
    mock_models.Variable.get.return_value = raw if isinstance(raw, str) else json.dumps(raw)
    with patch.dict("sys.modules", {"airflow.models": mock_models, "airflow": MagicMock()}):
        return load_config()


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
    assert config.exclude_dags == []


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


def test_exclude_dags_normalized():
    """exclude_dags overrides are de-duplicated and sorted."""
    import json

    overrides = json.dumps({"exclude_dags": ["b_dag", "a_dag", "b_dag"]})

    mock_models = MagicMock()
    mock_models.Variable.get.return_value = overrides
    with patch.dict("sys.modules", {"airflow.models": mock_models, "airflow": MagicMock()}):
        config = load_config()

    assert config.exclude_dags == ["a_dag", "b_dag"]


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


# ── Sanitization of out-of-band Variable values ─────────────────────────────────


def test_non_object_json_falls_back_to_defaults():
    """A valid-but-non-object Variable (list/string/number) yields defaults."""
    for raw in ("[]", '"bad"', "42", "null"):
        config = _load_with(raw)
        assert isinstance(config, WatchdogConfig)
        assert config.lookback_runs == 20
        assert config.dag_overrides == {}


def test_invalid_numeric_falls_back_to_default():
    """Wrong-type, non-finite, and out-of-range numerics revert to defaults."""
    config = _load_with(
        {
            "lookback_runs": "twenty",  # wrong type
            "failure_spike_ratio": 0,  # must be > 0
            "deadline_multiplier": -1,  # negative
            "schedule_interval_minutes": True,  # bool is not a valid number
        }
    )
    assert config.lookback_runs == 20
    assert config.failure_spike_ratio == 2.0
    assert config.deadline_multiplier == 2.0
    assert config.schedule_interval_minutes == 30


def test_min_deviation_zero_is_allowed():
    """The min-deviation floors may legitimately be zero (disables the floor)."""
    config = _load_with({"runtime_min_deviation_secs": 0, "schedule_min_deviation_minutes": 0})
    assert config.runtime_min_deviation_secs == 0
    assert config.schedule_min_deviation_minutes == 0


def test_int_fields_coerced():
    """Float values for int fields are coerced to int."""
    config = _load_with({"lookback_runs": 25.0, "failure_window_runs": 8.0})
    assert config.lookback_runs == 25
    assert isinstance(config.lookback_runs, int)
    assert config.failure_window_runs == 8


def test_bad_list_and_string_fields_fall_back():
    """Non-list list-fields and non-string webhook fields revert to defaults."""
    config = _load_with(
        {
            "exclude_dags": "not_a_list",
            "alert_emails": [1, 2],
            "alert_slack_webhook": 123,
        }
    )
    assert config.exclude_dags == []
    assert config.alert_emails == []
    assert config.alert_slack_webhook is None


def test_malformed_dag_overrides_dropped():
    """Malformed per-DAG override entries are dropped; well-formed ones survive."""
    config = _load_with(
        {
            "dag_overrides": {
                "good_dag": {"disable_detectors": ["runtime_anomaly"]},
                "list_not_dict": [],  # not an object
                "bad_disabled": {"disable_detectors": "runtime_anomaly"},  # not a list
                "bad_items": {"disable_detectors": [1, 2]},  # not strings
            }
        }
    )
    assert "good_dag" in config.dag_overrides
    assert "list_not_dict" not in config.dag_overrides
    assert "bad_disabled" not in config.dag_overrides
    assert "bad_items" not in config.dag_overrides
    # The surviving override is intact and usable.
    assert not config.is_detector_enabled("runtime_anomaly", "good_dag")


def test_dag_overrides_not_object_falls_back():
    """A non-object dag_overrides reverts to an empty dict."""
    config = _load_with({"dag_overrides": ["nope"]})
    assert config.dag_overrides == {}
