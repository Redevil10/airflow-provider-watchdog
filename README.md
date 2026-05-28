# airflow-provider-watchdog

| Category    | Badges                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **License** | [![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)                                                                                                                                                                                                                                                                                                                                                                                                                          |
| **PyPI**    | [![python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/downloads/) [![airflow](https://img.shields.io/badge/airflow-3.0%2B-blue.svg)](https://airflow.apache.org/) [![PyPI](https://img.shields.io/pypi/v/airflow-provider-watchdog)](https://pypi.org/project/airflow-provider-watchdog/) [![Downloads](https://img.shields.io/pypi/dm/airflow-provider-watchdog)](https://pypi.org/project/airflow-provider-watchdog/)                                              |
| **CI**      | [![lint](https://github.com/Redevil10/airflow-provider-watchdog/actions/workflows/lint.yml/badge.svg)](https://github.com/Redevil10/airflow-provider-watchdog/actions/workflows/lint.yml) [![tests](https://github.com/Redevil10/airflow-provider-watchdog/actions/workflows/test.yml/badge.svg)](https://github.com/Redevil10/airflow-provider-watchdog/actions/workflows/test.yml) [![codecov](https://codecov.io/github/Redevil10/airflow-provider-watchdog/graph/badge.svg)](https://codecov.io/gh/Redevil10/airflow-provider-watchdog) |

A lightweight, zero-dependency Airflow provider that monitors DAG and task health by querying the metadata database.

No Prometheus. No Grafana. No Datadog. Just `pip install` and go.

## What it detects

| Detector | What it catches | How it works |
|---|---|---|
| **Runtime anomaly** | Tasks running unusually slow or fast | IQR-based outlier detection on task durations |
| **Failure spike** | Sudden increase in DAG failure rate | Compares recent failure rate vs historical baseline |
| **Missed deadline** | DAG runs taking too long | Flags running DAGs exceeding N× their median duration |
| **Stuck task** | Zombie or hung tasks | Flags tasks in `running` state beyond N× their historical max |
| **Schedule anomaly** | Tasks starting or ending at unusual times | IQR-based outlier detection on time-of-day (handles midnight wraparound) |

## Requirements

- Apache Airflow >= 3.0.0
- Python >= 3.10
- Any SQL metadata database supported by Airflow (PostgreSQL, MySQL, SQLite)

## Installation

```bash
pip install airflow-provider-watchdog
```

That's it. The provider auto-registers:

1. An **`airflow_watchdog_monitor` DAG** that runs every 30 minutes (configurable)
2. A **`/watchdog/` dashboard** accessible from the Airflow UI under Browse → Watchdog

## Configuration

Set an Airflow Variable called `watchdog_config` with a JSON object. All fields are optional — sensible defaults apply.

```json
{
    "schedule_interval_minutes": 30,
    "lookback_runs": 20,
    "runtime_iqr_multiplier": 1.5,
    "failure_window_runs": 10,
    "failure_baseline_runs": 50,
    "failure_spike_ratio": 2.0,
    "deadline_multiplier": 2.0,
    "stuck_multiplier": 2.0,
    "schedule_iqr_multiplier": 1.5,
    "exclude_dags": [],
    "disable_detectors": [],
    "dag_overrides": {
        "my_dag": {
            "disable_detectors": ["schedule_anomaly"]
        }
    },
    "alert_emails": ["team@example.com"],
    "alert_slack_webhook": "https://hooks.slack.com/services/...",
    "alert_teams_webhook": "https://outlook.office.com/webhook/...",
    "alert_discord_webhook": "https://discord.com/api/webhooks/..."
}
```

### Configuration reference

| Field | Default | Description |
|---|---|---|
| `schedule_interval_minutes` | `30` | How often the watchdog DAG runs |
| `lookback_runs` | `20` | Number of recent runs used for statistical baselines |
| `runtime_iqr_multiplier` | `1.5` | IQR multiplier for runtime anomaly fences |
| `failure_window_runs` | `10` | Recent window size for failure rate calculation |
| `failure_baseline_runs` | `50` | Historical baseline size for failure rate comparison |
| `failure_spike_ratio` | `2.0` | Alert when recent rate exceeds this × baseline rate |
| `deadline_multiplier` | `2.0` | Alert when DAG run exceeds this × median duration |
| `stuck_multiplier` | `2.0` | Alert when task exceeds this × historical max duration |
| `schedule_iqr_multiplier` | `1.5` | IQR multiplier for start/end time-of-day fences |
| `exclude_dags` | `[]` | DAG IDs to skip (`airflow_watchdog_monitor` is always excluded) |
| `disable_detectors` | `[]` | Detector names to disable globally (e.g. `["schedule_anomaly"]`) |
| `dag_overrides` | `{}` | Per-DAG overrides: `{"dag_id": {"disable_detectors": [...]}}` |
| `alert_emails` | `[]` | Email addresses for alert notifications |
| `alert_slack_webhook` | `null` | Slack incoming webhook URL |
| `alert_teams_webhook` | `null` | MS Teams incoming webhook URL |
| `alert_discord_webhook` | `null` | Discord incoming webhook URL |

## How it works

### Architecture

```
┌─────────────────────────────────────────────────┐
│  airflow_watchdog_monitor DAG                   │
│                                                 │
│  ┌─────────┐ ┌────────┐ ┌────────┐ ┌─────┐ ┌────────┐ │
│  │ Runtime │ │Failure │ │Deadline│ │Stuck│ │Schedule│ │
│  │Detector │ │Detector│ │Detector│ │Det. │ │Detector│ │
│  └────┬────┘ └───┬────┘ └───┬────┘ └──┬──┘ └───┬────┘ │
│       │          │          │         │        │      │
│       └──────────┴──────────┴─────────┴────────┘      │
│                       │                         │
│              ┌────────▼────────┐                │
│              │    Alerting     │                │
│              │ Log/Email/Slack │                │
│              └────────┬────────┘                │
│                       │                         │
│              ┌────────▼────────┐                │
│              │  XCom (results) │                │
│              └─────────────────┘                │
└─────────────────────────────────────────────────┘
                        │
               ┌────────▼────────┐
               │   /watchdog/    │
               │   Dashboard     │
               │   (FastAPI)     │
               └─────────────────┘
```

### Detection methods

**Runtime anomaly (IQR):** For each `(dag_id, task_id)`, the detector computes Q1, Q3, and IQR from the last N successful runs. If the most recent duration falls outside `[Q1 - 1.5×IQR, Q3 + 1.5×IQR]`, it's flagged. This is more robust than z-score because outliers don't skew the baseline.

**Failure spike:** Compares the failure rate in the last 10 runs against the rate over the *preceding* baseline runs (the baseline excludes the recent window, so a fresh spike doesn't dilute its own reference point). If the recent rate exceeds `2× baseline`, it fires. Also catches DAGs that suddenly start failing when they historically never did.

**Missed deadline:** Checks currently-running DAG runs and compares their elapsed time against `2× median` historical duration. Catches DAGs that are silently hanging.

**Stuck task:** Checks currently-running task instances against `2× historical max` duration for that specific task. Catches zombie tasks, hung queries, and unresponsive external calls.

**Schedule anomaly (IQR):** For each `(dag_id, task_id)`, converts start and end times to minutes-since-midnight and computes IQR fences. Flags tasks that started or ended at an unusual time-of-day. Handles midnight wraparound (e.g. tasks normally running between 23:30–00:30).

## Dashboard

The dashboard is available at `/watchdog/` in the Airflow webserver. It shows:

- Summary cards: total DAGs, healthy, warning, critical counts
- DAG health table: sorted with problems at the top
- Per-DAG alerts with severity indicators
- Auto-refreshes every 60 seconds

Access it via **Browse → Watchdog** in the Airflow UI navbar.

The dashboard and its API require an authenticated Airflow user. Reading the
dashboard needs website (view) access; saving configuration changes requires
permission to edit Airflow Variables — enforced through Airflow's auth manager.

## Alerting

Alerts are dispatched through five channels:

1. **Airflow task logs** — always on, visible in the `airflow_watchdog_monitor` DAG run logs
2. **Email** — via Airflow's built-in `send_email` (requires SMTP config in `airflow.cfg`)
3. **Slack** — via incoming webhook (set `alert_slack_webhook` in config)
4. **MS Teams** — via incoming webhook with Adaptive Card (set `alert_teams_webhook` in config)
5. **Discord** — via incoming webhook (set `alert_discord_webhook` in config)

## Development

```bash
git clone https://github.com/Redevil10/airflow-provider-watchdog.git
cd airflow-provider-watchdog
uv sync --extra dev
uv run pytest
```

## Known limitations

- **XCom-based dashboard** — alert history is limited to the latest watchdog run. A future version may store results in a dedicated table for historical trending.

## Roadmap

- [ ] Historical alert storage (dedicated table) for trend analysis
- [ ] Sparkline charts in the dashboard showing duration trends
- [x] Per-DAG detector enable/disable via `dag_overrides` config
- [x] Multi-database support (PostgreSQL, MySQL, SQLite)
- [x] GitHub Actions CI (lint, test, publish)
- [ ] Contribution to the [Airflow ecosystem page](https://airflow.apache.org/ecosystem/)

## License

Apache License 2.0 — see [LICENSE](LICENSE).
