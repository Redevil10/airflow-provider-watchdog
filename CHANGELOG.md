# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2026-05-31

> **Renamed:** the distribution is now **`airflow-plugin-watchdog`** (was `airflow-provider-watchdog`). Install with `pip install airflow-plugin-watchdog`. The import path is unchanged (`import airflow_watchdog`).

### Fixed

- **`RuntimeError: Direct database access via the ORM is not allowed in Airflow 3.0`** — the monitor DAG's task ran the detector SQL through `airflow.settings.Session`, but Airflow 3 isolates task/worker execution from the metadata DB (AIP-72), so every run failed. Detection now runs on the API-server side, where direct metadata-DB access is sanctioned.

### Changed

- **Detection moved off the worker and onto the API server.** A background scheduler, started by the plugin's FastAPI lifespan, runs the detectors every `schedule_interval_minutes` inside the API-server process — the same place, and the same DB access, the dashboard already used. The synchronous detector SQL runs on a dedicated daemon thread so it never blocks the API server's event loop. Across multiple API-server replicas/workers, a database advisory lock (Postgres/MySQL) plus a last-run check ensures only one cycle runs per interval.
- **No monitoring DAG to deploy.** Installing the plugin is now sufficient; there is no `dags_folder` shim step. `schedule_interval_minutes` is read each cycle, so cadence changes apply without a restart (previously required a scheduler restart).
- Dashboard alert results now come from the `watchdog_last_results` Variable (written by the scheduler, capped at 50 most-severe alerts) instead of the monitor DAG's XCom.
- `exclude_dags` no longer force-includes the (now-removed) `airflow_watchdog_monitor` DAG; it defaults to `[]` and is normalized to a sorted, de-duplicated list.

### Removed

- The `airflow_watchdog_monitor` DAG (`airflow_watchdog/dag.py`) and the `example_dags/watchdog_monitor.py` shim. Detection no longer runs as a DAG task, so it no longer appears in Airflow's DAG/run list — its activity is visible in the dashboard and the API-server logs.
- **Provider registration.** The package is now a plain Airflow **plugin**, not a provider: `get_provider_info()` and the `apache_airflow_provider` entry point are gone. The plugin loads via the `airflow.plugins` entry point (the registration that already did the work). It no longer appears in Airflow's Providers list — look under Plugins instead.

## [0.5.0] - 2026-05-30

### Fixed

- **Dashboard / nav link never appeared** — `WatchdogPlugin` was never registered, so the `/watchdog/` dashboard and the Browse → Watchdog nav link were invisible in every deployment. The plugin is now registered both via the provider-info `plugins` key and an `airflow.plugins` entry point (the latter survives `LAZY_LOAD_PROVIDERS=True`). Airflow dedupes by plugin name, so the two registrations don't double-mount.
- **`"dags"` provider-info key did nothing** — the key added in 0.4.1 is not part of Airflow's provider-info schema and was silently ignored; Airflow has no mechanism to auto-discover DAGs shipped inside provider packages. Removed it.

### Added

- `example_dags/watchdog_monitor.py` — a one-line shim that re-exports the monitor DAG. Copy it into your `dags_folder` to expose `airflow_watchdog_monitor` to the scheduler (see README).
- Provider-wiring integration tests asserting the plugin is discoverable via `ProvidersManager` and the monitor DAG loads cleanly in a `DagBag`.

### Changed

- README installation steps now reflect reality: `pip install` registers the dashboard, and the monitor DAG requires dropping the shim into `dags_folder`.

## [0.4.1] - 2026-05-30

### Fixed

- **DAG not auto-discovered after install** — added missing `"dags"` key to provider info so Airflow's provider manager correctly discovers the `airflow_watchdog_monitor` DAG (superseded by 0.5.0 — this key is not a real Airflow mechanism and had no effect)

## [0.4.0] - 2026-05-29

### Added

- Python 3.14 support (added to classifiers and CI test matrix)
- Integration test suite that runs the detector/dashboard SQL, the XCom round trip, and the auth dependencies against a **real** Airflow metadata DB; CI exercises it on a PostgreSQL service container and on SQLite

### Security

- Dashboard and config endpoints now require an authenticated Airflow user. Reads require website (view) access; saving config requires permission to edit Airflow Variables. Previously these plugin endpoints — including the config-write endpoint that mutates the `watchdog_config` Variable — were reachable without authentication.

### Fixed

- **Deadline/stuck/schedule detectors and dashboard broken on SQLite** — raw SQL returns timestamps as strings on SQLite (vs `datetime` objects on PostgreSQL/MySQL), so duration arithmetic raised `TypeError`. Timestamps are now coerced via a shared `as_datetime` helper that works on every backend.
- **Dashboard showed no alerts** — the watchdog DAG pushed `json.dumps(summary)` to XCom, which XCom serialized again (double-encoding); the dashboard's `json.loads` then yielded a string instead of a dict. The DAG now pushes the dict directly, and the dashboard decodes defensively across backends (handles single/double-encoded strings and pre-decoded JSON).
- Failure-spike baseline now **excludes** the recent window (`rn > window`), so a fresh wave of failures no longer dilutes the baseline it's compared against
- Naive metadata timestamps are now interpreted as UTC (the value Airflow actually stores) instead of `airflow.settings.TIMEZONE`, which produced incorrect elapsed times for deadline/stuck detection on non-UTC deployments
- Dashboard DAG-link `base_url` is now passed through `encodeURI` for consistent escaping

### Changed

- Alerts serialized into the dashboard XCom are capped (most-severe first) to keep the payload bounded; `total_alerts`/`by_type` counts still reflect every alert

## [0.3.1] - 2026-03-25

### Fixed

- Dashboard and config UI now use relative URLs — works behind any URL prefix (e.g. `/airflow/watchdog/`)
- DAG links in dashboard use `base_url` from Airflow config instead of hardcoded `/dags/`

## [0.3.0] - 2026-03-21

### Added

- **Schedule anomaly detector** — flags tasks whose start or end time-of-day deviates from historical norms (IQR-based, handles midnight wraparound)
- **Per-DAG detector enable/disable** — `disable_detectors` (global) and `dag_overrides` (per-DAG) configuration fields
- **Configuration UI** at `/watchdog/config` — toggle detectors on/off globally or per DAG with a visual grid
- **MS Teams alerting** via Adaptive Card webhook (`alert_teams_webhook` config)
- **Discord alerting** via webhook (`alert_discord_webhook` config)
- `schedule_interval_minutes` config now wired to DAG schedule (read at parse time)

### Fixed

- Email alerts now HTML-escape DAG/task IDs to prevent XSS in email clients
- Config POST endpoint validates detector names against `AlertType` enum
- Config page dirty-check bug after saving (Save button now re-enables correctly)
- Consolidated duplicate `_fmt` duration helper into shared `_stats.fmt_duration`
- Naive datetimes now respect `airflow.settings.TIMEZONE` instead of assuming UTC

## [0.2.0] - 2026-03-21

### Added

- Multi-database support — detectors and dashboard now work with PostgreSQL, MySQL, and SQLite
- Dashboard error indicator — shows a visible error message instead of a blank page on DB failures
- Light/dark theme support — dashboard follows OS `prefers-color-scheme`
- Dashboard tests (8 new tests covering data assembly, endpoints, XSS escaping)
- `py.typed` marker for type checker support
- `CHANGELOG.md`

### Changed

- Renamed DAG from `watchdog_monitor` to `airflow_watchdog_monitor` to avoid collisions
- Detectors compute statistics in Python instead of PostgreSQL-specific `PERCENTILE_CONT`
- Dashboard SQL replaced `LEFT JOIN LATERAL`, `EXTRACT(EPOCH FROM)`, `NOW()` with standard SQL

### Fixed

- XSS vulnerability in dashboard — replaced incomplete `</` escaping with OWASP-recommended unicode escapes
- Type safety — replaced `type: ignore` with `cast()` in alerting module
- Airflow 3 compatibility — DAG `tags` changed from `list` to `set`

## [0.1.0] - 2026-03-20

### Added

- Four health detectors: runtime anomaly (IQR), failure spike, missed deadline, stuck task
- Auto-registered `airflow_watchdog_monitor` DAG with configurable schedule
- Dark-themed `/watchdog/` dashboard with auto-refresh (FastAPI + Airflow plugin)
- Alerting via Airflow logs, email, and Slack webhook
- JSON-based configuration via Airflow Variable `watchdog_config`
- Support for Python 3.10–3.13 and Apache Airflow 3.0+
