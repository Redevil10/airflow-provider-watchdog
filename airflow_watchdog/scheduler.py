"""
Background detection scheduler.

Runs :func:`airflow_watchdog.monitor.run_detection` on a fixed cadence from
inside the Airflow API-server process. It is started/stopped by the plugin's
FastAPI lifespan (see ``ui/app.py``), which Airflow runs only in the API server
— the one place ORM/metadata-DB access is sanctioned in Airflow 3.

The detection SQL is synchronous, so it runs on a dedicated daemon thread and
never touches the API server's async event loop.

Multiple API-server replicas (or uvicorn workers) each start this scheduler, so
every cycle is guarded twice to avoid duplicate work and duplicate alerts:

1. A database advisory lock ensures only one runs at a time.
2. A last-run timestamp check (from the stored results) skips a cycle if another
   replica already ran within the current interval.

The cadence is read from the ``watchdog_config`` Variable each cycle, so a
change to ``schedule_interval_minutes`` takes effect without a restart.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from datetime import datetime, timezone

from airflow_watchdog.config import WatchdogConfig, load_config
from airflow_watchdog.monitor import load_results, run_detection

logger = logging.getLogger(__name__)

# Seconds to wait after startup before the first detection run, so the API
# server finishes coming up first.
_INITIAL_DELAY = 15.0

# Floor on the cadence so a misconfigured tiny interval can't hammer the DB.
_MIN_INTERVAL_SECONDS = 60.0

# Advisory-lock identifiers (Postgres uses a bigint key, MySQL a name).
_PG_LOCK_KEY = 911_739_812
_MYSQL_LOCK_NAME = "airflow_watchdog_monitor"

_stop_event = threading.Event()
_thread: threading.Thread | None = None
_state_lock = threading.Lock()


# ── Public API ────────────────────────────────────────────────────────────────


def start() -> None:
    """Start the background scheduler thread (idempotent)."""
    global _thread
    with _state_lock:
        if _thread is not None and _thread.is_alive():
            return
        _stop_event.clear()
        _thread = threading.Thread(
            target=_run_loop, name="watchdog-scheduler", daemon=True
        )
        _thread.start()
        logger.info("Watchdog scheduler started")


def is_running() -> bool:
    """Return True if the scheduler thread is currently alive."""
    with _state_lock:
        return _thread is not None and _thread.is_alive()


def stop() -> None:
    """Signal the scheduler thread to stop and wait briefly for it to exit."""
    global _thread
    with _state_lock:
        thread = _thread
        if thread is None:
            return
        _stop_event.set()
    thread.join(timeout=10.0)
    with _state_lock:
        _thread = None
    logger.info("Watchdog scheduler stopped")


# ── Loop ──────────────────────────────────────────────────────────────────────


def _run_loop() -> None:
    if _stop_event.wait(_INITIAL_DELAY):
        return
    while True:
        try:
            _tick()
        except Exception:
            logger.exception("Watchdog scheduler tick failed")
        if _stop_event.wait(_interval_seconds()):
            return


def _tick() -> None:
    """Run one detection cycle, guarded by the advisory lock and last-run check."""
    config = load_config()
    with _advisory_lock() as acquired:
        if not acquired:
            logger.debug("Another replica holds the watchdog lock; skipping this tick")
            return
        if _ran_recently(config):
            logger.debug("Watchdog ran within the current interval; skipping this tick")
            return
        run_detection(config)


def _interval_seconds(config: WatchdogConfig | None = None) -> float:
    """Return the configured cadence in seconds, floored at the minimum."""
    try:
        cfg = config or load_config()
        minutes = float(cfg.schedule_interval_minutes)
    except Exception:
        minutes = 30.0
    return max(minutes * 60.0, _MIN_INTERVAL_SECONDS)


def _ran_recently(config: WatchdogConfig) -> bool:
    """True if a detection ran within ~90% of the current interval.

    Lets a slightly-offset replica skip a redundant run inside the same window
    while still allowing the next scheduled cycle through.
    """
    results = load_results()
    generated_at = results.get("generated_at")
    if not generated_at:
        return False
    try:
        last = datetime.fromisoformat(generated_at)
    except (TypeError, ValueError):
        return False
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    elapsed = (datetime.now(timezone.utc) - last).total_seconds()
    return elapsed < _interval_seconds(config) * 0.9


# ── Cross-replica advisory lock ─────────────────────────────────────────────────


@contextlib.contextmanager
def _advisory_lock():
    """Yield True if a DB advisory lock was acquired, else False.

    Uses the backend's native session-level advisory lock (held on a single
    connection for the duration). SQLite has no such primitive but is only used
    for single-node setups, so the lock is a no-op there.
    """
    from airflow.settings import Session

    if Session is None:  # pragma: no cover - configured by Airflow at runtime
        yield True
        return

    session = Session()
    dialect = ""
    acquired = True
    try:
        from sqlalchemy import text

        dialect = session.bind.dialect.name if session.bind is not None else ""

        if dialect == "postgresql":
            acquired = bool(
                session.execute(
                    text("SELECT pg_try_advisory_lock(:k)"), {"k": _PG_LOCK_KEY}
                ).scalar()
            )
        elif dialect in ("mysql", "mariadb"):
            acquired = (
                session.execute(
                    text("SELECT GET_LOCK(:n, 0)"), {"n": _MYSQL_LOCK_NAME}
                ).scalar()
                == 1
            )
        else:
            acquired = True  # sqlite / other: single-node, no lock needed

        yield acquired
    except Exception:
        logger.exception("Watchdog advisory lock failed; running without it")
        yield True
    finally:
        try:
            if acquired:
                from sqlalchemy import text

                if dialect == "postgresql":
                    session.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _PG_LOCK_KEY})
                elif dialect in ("mysql", "mariadb"):
                    session.execute(text("SELECT RELEASE_LOCK(:n)"), {"n": _MYSQL_LOCK_NAME})
        except Exception:
            logger.exception("Failed to release watchdog advisory lock")
        finally:
            session.close()
