"""
Airflow plugin registration.

This module is discovered by Airflow's plugin manager and registers:
- The FastAPI app for the ``/watchdog`` dashboard and API
- A navigation link in the Airflow UI
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from airflow_watchdog.ui.app import watchdog_app


class WatchdogPlugin(AirflowPlugin):
    name = "watchdog"
    fastapi_apps = [
        {
            "app": watchdog_app,
            "url_prefix": "/watchdog",
            "name": "Watchdog",
        }
    ]
    # Navbar link to the mounted dashboard. ``fastapi_apps`` only mounts the app;
    # Airflow 3 builds nav entries from ``external_views``. Without this, the
    # dashboard is reachable by URL but has no menu item.
    external_views = [
        {
            "name": "Watchdog",
            "href": "/watchdog/",
            # ``url_route`` is required: Airflow's UI-plugin loader silently
            # drops any external view without one, so the nav link won't appear.
            "url_route": "watchdog",
            "destination": "nav",
            "category": "Browse",
        }
    ]
