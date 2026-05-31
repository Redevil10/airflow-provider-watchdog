"""
Airflow Watchdog
~~~~~~~~~~~~~~~~

A lightweight, zero-dependency Airflow plugin that monitors DAG and task health
by querying the Airflow metadata database.

Detection runs on a background scheduler inside the API-server process (started
from the plugin's FastAPI lifespan), so it never accesses the metadata DB from a
task — staying within Airflow 3's task-isolation rules (AIP-72). No monitoring
DAG to deploy: installing the plugin is enough.

Detects:
- Runtime anomalies (IQR-based duration outliers)
- Failure spikes (sudden increase in failure rate)
- Missed deadlines (DAG runs exceeding expected duration)
- Stuck tasks (tasks in 'running' state beyond expected time)
- Schedule anomalies (start/end time-of-day outliers)

Install:
    pip install airflow-plugin-watchdog
"""

__version__ = "0.6.3"
