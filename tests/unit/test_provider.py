"""Tests for provider registration."""

from __future__ import annotations

from airflow_watchdog import get_provider_info


def test_provider_info():
    info = get_provider_info()
    assert info["package-name"] == "airflow-provider-watchdog"
    assert info["name"] == "Watchdog"
    assert "versions" in info
    assert len(info["versions"]) > 0
    assert info["dags"] == ["airflow_watchdog.dag"]
