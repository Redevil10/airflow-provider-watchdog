"""Watchdog monitoring DAG — deployment shim.

Airflow does not auto-discover DAGs shipped inside provider packages, so copy
this file into your ``dags_folder`` (default ``$AIRFLOW_HOME/dags/``) to expose
the ``airflow_watchdog_monitor`` DAG to the scheduler. It re-exports the DAG
object defined by the provider; all behaviour and configuration live in
``airflow_watchdog.dag`` (see the ``watchdog_config`` Airflow Variable).
"""

from __future__ import annotations

from airflow_watchdog.dag import dag  # noqa: F401
