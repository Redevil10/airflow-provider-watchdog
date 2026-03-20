# airflow-provider-watchdog 🐕

A lightweight, zero-dependency Airflow provider that monitors DAG and task health by querying the metadata database.

No Prometheus. No Grafana. No Datadog. Just `pip install` and go.

## What it detects

| Detector | What it catches | How it works |
|---|---|---|
| **Runtime anomaly** | Tasks running unusually slow or fast | IQR-based outlier detection on task durations |
| **Failure spike** | Sudden increase in DAG failure rate | Compares recent failure rate vs historical baseline |
| **Missed deadline** | DAG runs taking too long | Flags running DAGs exceeding N× their median duration |
| **Stuck task** | Zombie or hung tasks | Flags tasks in `running` state beyond N× their historical max |

## Requirements

- Apache Airflow >= 3.0.0
- PostgreSQL metadata database (uses `PERCENTILE_CONT`)
- Python >= 3.10

## Installation

```bash
pip install airflow-provider-watchdog
```

That's it. The provider auto-registers:

1. A **`watchdog_monitor` DAG** that runs every 30 minutes (configurable)
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
    "exclude_dags": [],
    "alert_emails": ["team@example.com"],
    "alert_slack_webhook": "https://hooks.slack.com/services/..."
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
| `exclude_dags` | `[]` | DAG IDs to skip (`watchdog_monitor` is always excluded) |
| `alert_emails` | `[]` | Email addresses for alert notifications |
| `alert_slack_webhook` | `null` | Slack incoming webhook URL |

## How it works

### Architecture

```
┌─────────────────────────────────────────────────┐
│  watchdog_monitor DAG  (runs every 30 min)      │
│                                                 │
│  ┌─────────┐ ┌──────────┐ ┌──────────┐ ┌─────┐ │
│  │ Runtime │ │ Failures │ │Deadlines │ │Stuck│ │
│  │Detector │ │ Detector │ │ Detector │ │Det. │ │
│  └────┬────┘ └────┬─────┘ └────┬─────┘ └──┬──┘ │
│       │           │            │           │    │
│       └───────────┴────────────┴───────────┘    │
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
               │  (Flask BP)     │
               └─────────────────┘
```

### Detection methods

**Runtime anomaly (IQR):** For each `(dag_id, task_id)`, the detector computes Q1, Q3, and IQR from the last N successful runs. If the most recent duration falls outside `[Q1 - 1.5×IQR, Q3 + 1.5×IQR]`, it's flagged. This is more robust than z-score because outliers don't skew the baseline.

**Failure spike:** Compares the failure rate in the last 10 runs against the rate in the last 50 runs. If the recent rate exceeds `2× baseline`, it fires. Also catches DAGs that suddenly start failing when they historically never did.

**Missed deadline:** Checks currently-running DAG runs and compares their elapsed time against `2× median` historical duration. Catches DAGs that are silently hanging.

**Stuck task:** Checks currently-running task instances against `2× historical max` duration for that specific task. Catches zombie tasks, hung queries, and unresponsive external calls.

## Dashboard

The dashboard is available at `/watchdog/` in the Airflow webserver. It shows:

- Summary cards: total DAGs, healthy, warning, critical counts
- DAG health table: sorted with problems at the top
- Per-DAG alerts with severity indicators
- Auto-refreshes every 60 seconds

Access it via **Browse → Watchdog** in the Airflow UI navbar.

## Alerting

Alerts are dispatched through three channels:

1. **Airflow task logs** — always on, visible in the `watchdog_monitor` DAG run logs
2. **Email** — via Airflow's built-in `send_email` (requires SMTP config in `airflow.cfg`)
3. **Slack** — via incoming webhook (set `alert_slack_webhook` in config)

## Development

```bash
git clone https://github.com/YOUR_USERNAME/airflow-provider-watchdog.git
cd airflow-provider-watchdog
pip install -e ".[dev]"
pytest
```

## Known limitations

- **PostgreSQL only** — the SQL uses `PERCENTILE_CONT` which is PostgreSQL-specific. SQLite and MySQL are not currently supported. This is intentional for v0.1 — PostgreSQL is the recommended Airflow metadata DB for production.
- **XCom-based dashboard** — alert history is limited to the latest watchdog run. A future version may store results in a dedicated table for historical trending.
- **Tuple binding** — the `IN :exclude_dags` syntax may behave differently across SQLAlchemy versions. Tested with SQLAlchemy 1.4+ and 2.0.

## Roadmap

- [ ] Historical alert storage (dedicated table) for trend analysis
- [ ] Sparkline charts in the dashboard showing duration trends
- [ ] Per-DAG threshold overrides via a separate Variable or JSON config
- [ ] MySQL compatibility (using `PERCENTILE_CONT` alternative)
- [ ] GitHub Actions CI
- [ ] Contribution to the [Airflow ecosystem page](https://airflow.apache.org/ecosystem/)

## License

Apache License 2.0 — see [LICENSE](LICENSE).
