# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
