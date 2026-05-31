"""Helpers to seed the real metadata DB with controlled rows.

Uses SQLAlchemy Core inserts against the actual Airflow model tables, providing
only the columns the watchdog reads plus those the schema requires (NOT NULL
without a default). Python-side column defaults fill in the rest.
"""

from __future__ import annotations

from datetime import datetime


def seed_dag(session, dag_id: str, *, is_paused: bool = False, is_stale: bool = False) -> None:
    from airflow.models.dag import DagModel

    session.execute(
        DagModel.__table__.insert().values(
            dag_id=dag_id,
            is_paused=is_paused,
            is_stale=is_stale,
            bundle_name="test-bundle",
            max_active_tasks=16,
            max_consecutive_failed_dag_runs=0,
            has_task_concurrency_limits=False,
        )
    )
    session.commit()


def seed_dag_run(
    session,
    dag_id: str,
    run_id: str,
    state: str,
    start: datetime,
    end: datetime | None,
) -> int:
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


def seed_task_instance(
    session,
    dag_id: str,
    task_id: str,
    run_id: str,
    state: str,
    start: datetime,
    end: datetime | None,
    duration: float | None,
) -> None:
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


def seed_xcom(
    session,
    dag_run_id: int,
    dag_id: str,
    run_id: str,
    task_id: str,
    key: str,
    value: bytes,
) -> None:
    from airflow.models.xcom import XComModel

    session.execute(
        XComModel.__table__.insert().values(
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=-1,
            key=key,
            value=value,
        )
    )
    session.commit()
