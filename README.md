# airflow-plugin-watchdog

| Category    | Badges                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **License** | [![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)                                                                                                                                                                                                                                                                                                                                                                                                                          |
| **PyPI**    | [![python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue.svg)](https://www.python.org/downloads/) [![airflow](https://img.shields.io/badge/airflow-3.0%2B-blue.svg)](https://airflow.apache.org/) [![PyPI](https://img.shields.io/pypi/v/airflow-plugin-watchdog)](https://pypi.org/project/airflow-plugin-watchdog/) [![Downloads](https://img.shields.io/pypi/dm/airflow-plugin-watchdog)](https://pypi.org/project/airflow-plugin-watchdog/)                                              |
| **CI**      | [![lint](https://github.com/Redevil10/airflow-plugin-watchdog/actions/workflows/lint.yml/badge.svg)](https://github.com/Redevil10/airflow-plugin-watchdog/actions/workflows/lint.yml) [![unit tests](https://github.com/Redevil10/airflow-plugin-watchdog/actions/workflows/unit_test.yml/badge.svg)](https://github.com/Redevil10/airflow-plugin-watchdog/actions/workflows/unit_test.yml) [![integration](https://github.com/Redevil10/airflow-plugin-watchdog/actions/workflows/integration_test.yml/badge.svg)](https://github.com/Redevil10/airflow-plugin-watchdog/actions/workflows/integration_test.yml) [![codecov](https://codecov.io/github/Redevil10/airflow-plugin-watchdog/graph/badge.svg)](https://codecov.io/gh/Redevil10/airflow-plugin-watchdog) |

A lightweight, zero-dependency Airflow plugin that monitors DAG and task health by querying the metadata database.

No Prometheus. No Grafana. No Datadog. No DAG to deploy. Just `pip install` and go.

## Screenshots

> 📸 _Placeholder — add `docs/dashboard.png` and `docs/config.png`, then uncomment the block below._

<!--
| Dashboard | Configuration |
|---|---|
| ![Watchdog dashboard](docs/dashboard.png) | ![Watchdog configuration](docs/config.png) |
-->

## What it detects

| Detector | What it catches | How it works |
|---|---|---|
| **Runtime anomaly** | Tasks running unusually slow or fast | IQR-based outlier detection on task durations |
| **Failure spike** | Sudden increase in DAG failure rate | Compares recent failure rate vs historical baseline |
| **Missed deadline** | DAG runs taking too long | Flags running DAGs exceeding N× their median duration |
| **Stuck task** | Zombie or hung tasks | Flags tasks in `running` state beyond N× their historical max |
| **Schedule anomaly** | Tasks starting or ending at unusual times | IQR-based outlier detection on time-of-day (handles midnight wraparound) |

## Quick start

```bash
pip install airflow-plugin-watchdog
```

1. **Install** (above), then **restart the Airflow API server** so the plugin and its background scheduler load.
2. **Open the dashboard** — click **Watchdog** in the Airflow navbar (under Browse), or go to `/watchdog/`.
3. **Tune it (optional)** — click **Configuration** on the dashboard to toggle detectors, adjust thresholds, and add alert destinations.

That's it — no DAG to deploy, no extra services. Detection runs every 30 minutes by default and works out of the box.

## Requirements

- Apache Airflow >= 3.0.0
- Python >= 3.10
- A metadata database: **PostgreSQL** (recommended, integration-tested) or **SQLite** (tested). MySQL is not tested in CI but should work, as the code is backend-agnostic.

## Installation

```bash
pip install airflow-plugin-watchdog
```

This registers the **`/watchdog/` dashboard** (accessible from the Airflow UI
under Browse → Watchdog) **and** the detection scheduler. There is **no DAG to
deploy** — detection runs on a background scheduler inside the Airflow API-server
process, which is started automatically by the plugin.

> **Why no DAG?** Airflow 3 isolates task execution from the metadata database
> (AIP-72): a task may not access it via the ORM. Watchdog's detectors need that
> data, so detection runs on the API server — where direct metadata-DB access is
> sanctioned — instead of inside a worker task. See [How it works](#how-it-works).

After installing, **restart the API server** so the plugin and its scheduler are
picked up. By default detection runs every 30 minutes (configurable — see below).

## Configuration

All settings live in a single Airflow Variable, `watchdog_config` (a JSON object). All fields are optional — sensible defaults apply. There are two equivalent ways to edit it:

- **Watchdog Configuration page** (recommended) — open the dashboard and click **Configuration**. It's a structured, validated editor for the same Variable, organized into three tabs: **Detectors** (enable/disable per DAG, plus excluded DAGs), **Thresholds** (numeric tuning), and **Alerts** (emails and webhook URLs).
- **Admin → Variables → `watchdog_config`** — edit the raw JSON by hand. Same effect, no validation.

Either way the value is stored in Airflow's metadata DB (the `variable` table), read fresh each detection cycle and shared across API-server replicas. Editing either way takes effect on the next run.

> **Note:** Watchdog also writes a second Variable, `watchdog_last_results` — this is *output*, not config: the scheduler overwrites it every cycle with the latest alert summary that the dashboard reads. It shows up under Admin → Variables too, but don't edit it (your changes are clobbered on the next run).

The full set of `watchdog_config` fields:

```json
{
    "schedule_interval_minutes": 30,
    "lookback_runs": 20,
    "runtime_iqr_multiplier": 1.5,
    "runtime_min_deviation_secs": 5.0,
    "failure_window_runs": 10,
    "failure_baseline_runs": 50,
    "failure_spike_ratio": 2.0,
    "deadline_multiplier": 2.0,
    "stuck_multiplier": 2.0,
    "schedule_iqr_multiplier": 1.5,
    "schedule_min_deviation_minutes": 5.0,
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
| `schedule_interval_minutes` | `30` | How often detection runs (read each cycle — changes apply without a restart) |
| `lookback_runs` | `20` | Number of recent runs used for statistical baselines |
| `runtime_iqr_multiplier` | `1.5` | IQR multiplier for runtime anomaly fences |
| `runtime_min_deviation_secs` | `5.0` | Minimum absolute duration change before a runtime anomaly fires (suppresses noise from steady/very short tasks) |
| `failure_window_runs` | `10` | Recent window size for failure rate calculation |
| `failure_baseline_runs` | `50` | Historical baseline size for failure rate comparison |
| `failure_spike_ratio` | `2.0` | Alert when recent rate exceeds this × baseline rate |
| `deadline_multiplier` | `2.0` | Alert when DAG run exceeds this × median duration |
| `stuck_multiplier` | `2.0` | Alert when task exceeds this × historical max duration |
| `schedule_iqr_multiplier` | `1.5` | IQR multiplier for start/end time-of-day fences |
| `schedule_min_deviation_minutes` | `5.0` | Minimum deviation from the median time-of-day before a schedule anomaly fires (suppresses sub-minute jitter) |
| `exclude_dags` | `[]` | DAG IDs to skip during detection |
| `disable_detectors` | `[]` | Detector names to disable globally (e.g. `["schedule_anomaly"]`) |
| `dag_overrides` | `{}` | Per-DAG overrides: `{"dag_id": {"disable_detectors": [...]}}` |
| `alert_emails` | `[]` | Email addresses for alert notifications |
| `alert_slack_webhook` | `null` | Slack incoming webhook URL |
| `alert_teams_webhook` | `null` | MS Teams incoming webhook URL |
| `alert_discord_webhook` | `null` | Discord incoming webhook URL |

## How it works

### Architecture

Detection runs entirely inside the **Airflow API-server** process, started by the
plugin's FastAPI lifespan. A background scheduler fires every
`schedule_interval_minutes`; across multiple API-server replicas a database
advisory lock plus a last-run check ensures only one cycle runs per interval.
Both the detectors and the dashboard read the metadata DB directly here — the
sanctioned place for it in Airflow 3 — so nothing ever runs in a worker task.

```mermaid
flowchart TD
    subgraph api["Airflow API server (FastAPI plugin)"]
        direction TB
        timer["Background scheduler<br/><i>every N min · advisory-locked</i>"]

        subgraph det["Detectors"]
            runtime["Runtime"]
            failure["Failure"]
            deadline["Deadline"]
            stuck["Stuck"]
            schedule["Schedule"]
        end

        timer --> det
        det --> alerting["Alerting<br/><i>Log / Email / Slack / Teams / Discord</i>"]
        det --> var[("Variable<br/>watchdog_last_results")]
        var --> dashboard["/watchdog/ Dashboard"]
    end

    db[("Metadata DB<br/><i>dag_run · task_instance</i>")] -.read.-> det
    db -.read.-> dashboard
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

Alerts are dispatched through five channels. Only the task log is on by default —
every other channel stays silent until you set the matching field in
`watchdog_config`:

| Channel | Config field | Default |
|---|---|---|
| **API-server logs** | _(none)_ | **always on** — alerts are logged by the scheduler in the Airflow API-server logs |
| **Email** | `alert_emails` | off — also requires Airflow SMTP (see below) |
| **Slack** | `alert_slack_webhook` | off |
| **MS Teams** (Adaptive Card) | `alert_teams_webhook` | off |
| **Discord** | `alert_discord_webhook` | off |

Webhook channels need no extra setup — paste the incoming-webhook URL into the
corresponding field and you're done.

### Email requires Airflow SMTP

Email is **two-part**: Watchdog only decides *who* to notify (`alert_emails`); the
actual sending goes through Airflow's own `airflow.utils.email.send_email`, which
reads Airflow's `[smtp]` settings. So filling in `alert_emails` alone is not
enough — you must also configure SMTP on the Airflow side.

1. **Tell Watchdog who to email** — in the `watchdog_config` Variable:

   ```json
   { "alert_emails": ["team@example.com"] }
   ```

2. **Configure SMTP in Airflow** — via `airflow.cfg`:

   ```ini
   [smtp]
   smtp_host = smtp.example.com
   smtp_starttls = True
   smtp_port = 587
   smtp_user = alerts@example.com
   smtp_password = <app-password>
   smtp_mail_from = alerts@example.com
   ```

   …or the equivalent environment variables (`AIRFLOW__SMTP__SMTP_HOST`, etc.).
   Some Airflow 3 deployments use an SMTP **Connection** (`smtp_default`) instead
   of `airflow.cfg` — configure it under Admin → Connections in that case.

> **Fail-soft:** a delivery failure never breaks the detection cycle. If a
> channel is misconfigured (e.g. SMTP not set up, or a bad webhook URL), the
> cycle still completes and the error is logged in the API-server logs
> (`Failed to send watchdog email` / `… Slack notification`, etc.). If you
> configured a channel but see nothing, check that log first. Also note alerts
> are only dispatched when there's something to report — a clean run sends
> nothing.

## Development

```bash
git clone https://github.com/Redevil10/airflow-plugin-watchdog.git
cd airflow-plugin-watchdog
uv sync --extra dev
uv run pytest tests/unit   # fast unit tests (Airflow mocked)
```

### Integration tests

The unit suite mocks Airflow. A separate integration suite runs the detector and
dashboard SQL, the results-Variable round trip, and the auth dependencies against
a **real** Airflow metadata database. PostgreSQL is the supported production backend, so it
is the primary target; SQLite is also exercised because timestamps and JSON are
read via raw SQL and differ by driver.

```bash
# PostgreSQL (recommended — matches production)
docker run -d --rm --name wd_pg -e POSTGRES_USER=airflow \
    -e POSTGRES_PASSWORD=airflow -e POSTGRES_DB=airflow -p 5432:5432 postgres:16
WATCHDOG_IT_DB_URL="postgresql+psycopg2://airflow:airflow@localhost:5432/airflow" \
    uv run --extra dev pytest tests/integration -m integration

# SQLite (no service needed)
WATCHDOG_IT_DB_URL="sqlite:////tmp/watchdog_it.db" \
    uv run --extra dev pytest tests/integration -m integration
```

Integration tests are marked `integration` and skipped by default; CI runs them
against a PostgreSQL service container (see `.github/workflows/integration_test.yml`).

## Known limitations

- **Latest-run only** — the dashboard shows the most recent detection cycle (stored in the `watchdog_last_results` Variable); there is no alert history. A future version may store results in a dedicated table for historical trending.
- **Detection is not a DAG run** — because detection runs on the API server rather than as a task, it does not appear in Airflow's DAG/run list; its activity is visible in the dashboard and the API-server logs instead.

## Roadmap

- [ ] Historical alert storage (dedicated table) for trend analysis
- [ ] Sparkline charts in the dashboard showing duration trends
- [x] Per-DAG detector enable/disable via `dag_overrides` config
- [x] Multi-database support — PostgreSQL (primary, integration-tested) and SQLite (tested); MySQL should work but is not covered in CI
- [x] GitHub Actions CI (lint, unit, integration, publish)
- [ ] Contribution to the [Airflow ecosystem page](https://airflow.apache.org/ecosystem/)

## License

Apache License 2.0 — see [LICENSE](LICENSE).
