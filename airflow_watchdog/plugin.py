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
