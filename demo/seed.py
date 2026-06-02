"""Seed the demo metadata DB with history that makes every detector fire.

Run inside the demo's ``airflow-init`` container, *after* ``airflow db migrate``.
It inserts DAGs, DAG runs, and task instances straight into the metadata tables
(the same approach the integration tests use) so the Watchdog dashboard shows a
realistic, populated picture the moment you open it — no need to wait for real
DAGs to accumulate history.

Each DAG below is shaped to trigger exactly one kind of alert (plus a few that
stay healthy), so the dashboard demonstrates all five detectors at once:

    payment_reconciliation   -> failure spike   (critical)
    transaction_settlement   -> stuck task      (critical)
    daily_sales_etl          -> runtime anomaly (warning)
    inventory_sync           -> schedule anomaly(critical)
    nightly_warehouse_load   -> missed deadline (critical)
    order_ingestion / customer_segmentation / product_recommendations -> healthy
    marketing_attribution    -> healthy, paused

The script is idempotent: if the demo DAGs already exist it does nothing.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("watchdog-demo-seed")

BUNDLE = "demo-bundle"


# ── Low-level insert helpers (mirror tests/integration/_seed.py) ────────────────


def _seed_dag(session, dag_id, *, is_paused=False):
    from airflow.models.dag import DagModel

    session.execute(
        DagModel.__table__.insert().values(
            dag_id=dag_id,
            is_paused=is_paused,
            is_stale=False,
            bundle_name=BUNDLE,
            max_active_tasks=16,
            max_consecutive_failed_dag_runs=0,
            has_task_concurrency_limits=False,
        )
    )
    session.commit()


def _seed_run(session, dag_id, run_id, state, start, end):
    from airflow.models.dagrun import DagRun

    result = session.execute(
        DagRun.__table__.insert().values(
            dag_id=dag_id,
            run_id=run_id,
            run_type="scheduled",
            state=state,
            start_date=start,
            end_date=end,
            logical_date=start,
            run_after=start,
            span_status="not_started",
        )
    )
    session.commit()
    return result.inserted_primary_key[0]


def _seed_ti(session, dag_id, task_id, run_id, state, start, end, duration):
    from airflow.models.taskinstance import TaskInstance

    session.execute(
        TaskInstance.__table__.insert().values(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            map_index=-1,
            state=state,
            start_date=start,
            end_date=end,
            duration=duration,
            max_tries=0,
            pool="default_pool",
            span_status="not_started",
        )
    )
    session.commit()


# ── Scenario builders ───────────────────────────────────────────────────────────


def _healthy(session, now, dag_id, task_id, duration, *, is_paused=False):
    """Consistent, on-time runs — no detector should flag this DAG."""
    _seed_dag(session, dag_id, is_paused=is_paused)
    # Space runs 1h apart so start time-of-day spreads across the clock (no
    # schedule-anomaly), with steady durations (no runtime-anomaly).
    for i in range(8):
        start = now - timedelta(hours=8 - i)
        end = start + timedelta(seconds=duration)
        _seed_run(session, dag_id, f"{dag_id}_r{i}", "success", start, end)
        _seed_ti(
            session, dag_id, task_id, f"{dag_id}_r{i}", "success", start, end, float(duration)
        )


def _failure_spike(session, now):
    dag_id = "payment_reconciliation"
    _seed_dag(session, dag_id)
    base = now - timedelta(hours=30)
    # 13 older runs, all successful -> 0% baseline.
    for i in range(13):
        start = base + timedelta(hours=i)
        _seed_run(
            session, dag_id, f"{dag_id}_old{i}", "success", start, start + timedelta(minutes=1)
        )
    # 10 most-recent runs, 4 failed -> 40% recent vs 0% baseline = critical.
    recent = base + timedelta(hours=14)
    for i in range(10):
        state = "failed" if i < 4 else "success"
        start = recent + timedelta(hours=i)
        _seed_run(session, dag_id, f"{dag_id}_new{i}", state, start, start + timedelta(minutes=1))


def _stuck_task(session, now):
    dag_id = "transaction_settlement"
    task_id = "settle_transactions"
    _seed_dag(session, dag_id)
    # History: ~30-minute runs, each with a ~5-minute settle task. Long DAG-run
    # durations keep the missed-deadline detector quiet so only "stuck" fires.
    for i in range(6):
        start = now - timedelta(hours=12 - i)
        end = start + timedelta(minutes=30)
        _seed_run(session, dag_id, f"{dag_id}_r{i}", "success", start, end)
        t_start = start
        t_end = start + timedelta(minutes=5)
        _seed_ti(session, dag_id, task_id, f"{dag_id}_r{i}", "success", t_start, t_end, 300.0)
    # A run still going, whose settle task has been running ~47 min: 47m >> 2x5m.
    rstart = now - timedelta(minutes=47)
    _seed_run(session, dag_id, f"{dag_id}_running", "running", rstart, None)
    _seed_ti(session, dag_id, task_id, f"{dag_id}_running", "running", rstart, None, None)


def _runtime_anomaly(session, now):
    dag_id = "daily_sales_etl"
    task_id = "aggregate_revenue"
    _seed_dag(session, dag_id)
    normals = [30, 45, 35, 50, 40, 38, 42, 48, 33, 47, 36, 44]
    for i, dur in enumerate(normals):
        start = now - timedelta(hours=len(normals) - i + 1)
        end = start + timedelta(seconds=dur)
        _seed_run(session, dag_id, f"{dag_id}_r{i}", "success", start, end)
        _seed_ti(session, dag_id, task_id, f"{dag_id}_r{i}", "success", start, end, float(dur))
    # Latest run: 70s — past the upper IQR fence (~63s) but not far enough to be
    # critical, so it lands as a warning (mixing severities on the dashboard).
    start = now - timedelta(minutes=35)
    end = start + timedelta(seconds=70)
    _seed_run(session, dag_id, f"{dag_id}_latest", "success", start, end)
    _seed_ti(session, dag_id, task_id, f"{dag_id}_latest", "success", start, end, 70.0)


def _schedule_anomaly(session, now):
    dag_id = "inventory_sync"
    task_id = "pull_stock_levels"
    _seed_dag(session, dag_id)
    # 8 days of history, all starting ~06:00 UTC, steady ~70s duration. Anchored
    # to days in the past (not the current wall clock) so the anomaly is the same
    # whatever time of day you run the demo.
    for i in range(8):
        day = now - timedelta(days=9 - i)
        start = day.replace(hour=6, minute=0, second=0, microsecond=0)
        end = start + timedelta(seconds=70)
        _seed_run(session, dag_id, f"{dag_id}_r{i}", "success", start, end)
        _seed_ti(session, dag_id, task_id, f"{dag_id}_r{i}", "success", start, end, 70.0)
    # Latest run (most recent) started 09:47 — hours later than the 06:00 norm.
    day = now - timedelta(days=1)
    start = day.replace(hour=9, minute=47, second=0, microsecond=0)
    end = start + timedelta(seconds=72)
    _seed_run(session, dag_id, f"{dag_id}_latest", "success", start, end)
    _seed_ti(session, dag_id, task_id, f"{dag_id}_latest", "success", start, end, 72.0)


def _missed_deadline(session, now):
    dag_id = "nightly_warehouse_load"
    _seed_dag(session, dag_id)
    # History: ~10-minute runs (median 600s).
    for i in range(6):
        start = now - timedelta(hours=12 - i)
        _seed_run(
            session, dag_id, f"{dag_id}_r{i}", "success", start, start + timedelta(minutes=10)
        )
    # A run that has been going ~40 min: well past 2x the 10-minute median.
    rstart = now - timedelta(minutes=40)
    _seed_run(session, dag_id, f"{dag_id}_running", "running", rstart, None)


# ── Entry point ─────────────────────────────────────────────────────────────────


def main():
    from airflow.models.dagbundle import DagBundleModel
    from airflow.settings import Session

    session = Session()

    # The dag table FK-references dag_bundle; create the bundle our DAGs use.
    if not session.query(DagBundleModel).filter_by(name=BUNDLE).first():
        session.execute(DagBundleModel.__table__.insert().values(name=BUNDLE))
        session.commit()

    from airflow.models.dag import DagModel

    if session.query(DagModel).filter_by(dag_id="payment_reconciliation").first():
        log.info("Demo data already present; nothing to seed.")
        return

    now = datetime.now(timezone.utc)

    _failure_spike(session, now)
    _stuck_task(session, now)
    _runtime_anomaly(session, now)
    _schedule_anomaly(session, now)
    _missed_deadline(session, now)

    _healthy(session, now, "order_ingestion", "load_orders", 54)
    _healthy(session, now, "customer_segmentation", "cluster_customers", 180)
    _healthy(session, now, "product_recommendations", "rank_products", 95)
    _healthy(session, now, "marketing_attribution", "attribute_touchpoints", 220, is_paused=True)

    # Run detection every minute in the demo so edits show up quickly (the
    # scheduler floors this at 60s) and keep all alert channels off.
    from airflow.models import Variable

    Variable.set("watchdog_config", json.dumps({"schedule_interval_minutes": 1}))

    log.info("Seeded demo DAGs, runs, and task instances.")


if __name__ == "__main__":
    main()
