# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-20

### Added

- Four health detectors: runtime anomaly (IQR), failure spike, missed deadline, stuck task
- Auto-registered `airflow_watchdog_monitor` DAG with configurable schedule
- Dark-themed `/watchdog/` dashboard with auto-refresh (FastAPI + Airflow plugin)
- Alerting via Airflow logs, email, and Slack webhook
- JSON-based configuration via Airflow Variable `watchdog_config`
- Support for Python 3.10–3.13 and Apache Airflow 3.0+
