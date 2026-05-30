"""Tests for provider registration."""

from __future__ import annotations

from airflow_watchdog import get_provider_info


def test_provider_info():
    info = get_provider_info()
    assert info["package-name"] == "airflow-provider-watchdog"
    assert info["name"] == "Watchdog"
    assert "versions" in info
    assert len(info["versions"]) > 0


def test_provider_info_registers_plugin():
    # The ``plugins`` key is the canonical way Airflow discovers the dashboard /
    # nav-link plugin (the ``airflow.plugins`` entry point in pyproject.toml is a
    # fallback that also survives LAZY_LOAD_PROVIDERS); dropping it means the UI
    # silently disappears on the provider-info path.
    info = get_provider_info()
    assert info["plugins"] == [
        {"name": "watchdog", "plugin-class": "airflow_watchdog.plugin.WatchdogPlugin"}
    ]
    # The old ``dags`` key was never a real Airflow mechanism — guard against it
    # being reintroduced as a false promise of DAG auto-discovery.
    assert "dags" not in info
