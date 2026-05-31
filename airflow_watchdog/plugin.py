"""
Airflow plugin registration.

This module is discovered by Airflow's plugin manager and registers:
- The FastAPI app for the ``/watchdog`` dashboard and API
- A navigation link in the Airflow UI
"""

from __future__ import annotations

from urllib.parse import urlsplit

from airflow.plugins_manager import AirflowPlugin

from airflow_watchdog.ui.app import watchdog_app


def _dashboard_href() -> str:
    """Build the navbar link's href, honoring Airflow's configured base path.

    Airflow embeds an external view by putting this href straight into an iframe
    ``src`` — it does NOT prepend the API base path. So a deployment served under
    a sub-path (e.g. behind ``/airflow``) must have that prefix baked in, or the
    iframe resolves to the wrong origin (and shows the Airflow home page). We read
    the path from ``[api] base_url`` so it works under any prefix, or none.
    """
    prefix = ""
    try:
        from airflow.configuration import conf

        base_url = conf.get("api", "base_url", fallback="") or ""
        prefix = urlsplit(base_url).path.rstrip("/")
    except Exception:
        prefix = ""
    return f"{prefix}/watchdog/"


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
            # Honors the deployment's base path (e.g. /airflow) — see _dashboard_href.
            "href": _dashboard_href(),
            # ``url_route`` is required: Airflow's UI-plugin loader silently
            # drops any external view without one, so the nav link won't appear.
            "url_route": "watchdog",
            "destination": "nav",
            "category": "Browse",
        }
    ]
