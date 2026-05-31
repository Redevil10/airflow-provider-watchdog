"""Tests for the Airflow plugin registration."""

from __future__ import annotations


def test_plugin_registers_dashboard_app():
    """WatchdogPlugin exposes the dashboard FastAPI app under /watchdog."""
    from airflow_watchdog.plugin import WatchdogPlugin
    from airflow_watchdog.ui.app import watchdog_app

    assert WatchdogPlugin.name == "watchdog"

    apps = WatchdogPlugin.fastapi_apps
    assert len(apps) == 1
    assert apps[0]["url_prefix"] == "/watchdog"
    assert apps[0]["app"] is watchdog_app


def test_plugin_registers_navbar_link():
    """WatchdogPlugin adds an external view so the dashboard appears in the navbar."""
    from airflow_watchdog.plugin import WatchdogPlugin

    views = WatchdogPlugin.external_views
    assert len(views) == 1
    view = views[0]
    assert view["name"] == "Watchdog"
    # Must point at the mounted app and render as a nav entry.
    assert view["href"] == "/watchdog/"
    assert view["destination"] == "nav"
    # Airflow drops external views without a url_route, so it must be present.
    assert view.get("url_route")


def test_package_exposes_version_but_not_provider_info():
    """The package is a plain plugin: it carries a version, not provider info."""
    import airflow_watchdog

    assert isinstance(airflow_watchdog.__version__, str)
    # Dropped when the package became a pure plugin (0.6.0).
    assert not hasattr(airflow_watchdog, "get_provider_info")
