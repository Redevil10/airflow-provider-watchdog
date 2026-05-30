"""
Airflow Provider Watchdog
~~~~~~~~~~~~~~~~~~~~~~~~~

A lightweight, zero-dependency Airflow provider that monitors DAG and task
health by querying the Airflow metadata database.

Detects:
- Runtime anomalies (IQR-based duration outliers)
- Failure spikes (sudden increase in failure rate)
- Missed deadlines (DAG runs exceeding expected duration)
- Stuck tasks (tasks in 'running' state beyond expected time)

Install:
    pip install airflow-provider-watchdog
"""

__version__ = "0.5.0"


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-watchdog",
        "name": "Watchdog",
        "description": (
            "Monitors DAG/task health via metadata DB"
            " — runtime anomalies, failure spikes, missed deadlines, stuck tasks."
        ),
        "versions": [__version__],
        # Register the FastAPI dashboard / nav link. This is the only mechanism
        # by which Airflow 3 discovers a provider's AirflowPlugin — there is no
        # auto-discovery of plugin classes from installed packages.
        "plugins": [
            {
                "name": "watchdog",
                "plugin-class": "airflow_watchdog.plugin.WatchdogPlugin",
            }
        ],
    }
