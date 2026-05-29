"""Integration-test harness: a real Airflow metadata DB + auth manager.

Unlike the unit tests (which mock Airflow entirely), these tests run the
detector / dashboard SQL and the auth dependencies against a freshly migrated
database and a real ``SimpleAuthManager``. This catches schema/column drift and
verifies that authentication is actually enforced end to end.

PostgreSQL is the supported production backend, so that is what these tests
target. Point them at a database with ``WATCHDOG_IT_DB_URL`` (defaults to a
local throwaway Postgres); if no database is reachable the suite is skipped.

Environment is configured at import time — *before* any real Airflow import —
so ``airflow.settings`` binds its engine to the test database.
"""

from __future__ import annotations

import os
import tempfile

# ── Configure Airflow before it is imported anywhere ────────────────────────────
_DB_URL = os.environ.get(
    "WATCHDOG_IT_DB_URL",
    "postgresql+psycopg2://airflow:airflow@localhost:55432/airflow",
)
_TMP = tempfile.mkdtemp(prefix="watchdog_it_")

os.environ.setdefault("AIRFLOW_HOME", _TMP)
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = _DB_URL
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
# Deterministic JWT signing so minted tokens validate within the same process.
# 64+ bytes to satisfy the SHA-512 HMAC minimum-length recommendation.
os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] = "watchdog-integration-secret-" + ("x" * 48)

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def airflow_db():
    """Migrate the test DB and initialise the auth manager once."""
    from airflow.utils.db import initdb
    from airflow.utils.session import create_session
    from sqlalchemy.exc import OperationalError

    try:
        initdb()
    except OperationalError as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"No integration database reachable at {_DB_URL}: {exc}")

    from airflow.api_fastapi.app import init_auth_manager

    init_auth_manager()

    # The `dag` table has a FK to `dag_bundle`; seed the bundle the test DAGs use.
    from airflow.models.dagbundle import DagBundleModel

    with create_session() as session:
        if not session.query(DagBundleModel).filter_by(name="test-bundle").first():
            session.execute(DagBundleModel.__table__.insert().values(name="test-bundle"))
            session.commit()
        yield session


@pytest.fixture()
def clean_tables(airflow_db):
    """Truncate the tables the watchdog reads, so each test starts clean."""
    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.xcom import XComModel

    def _truncate():
        # Recover from any failed transaction a prior test may have left behind.
        airflow_db.rollback()
        for model in (XComModel, TaskInstance, DagRun, DagModel):
            airflow_db.query(model).delete()
        airflow_db.commit()

    _truncate()
    yield airflow_db
    _truncate()


@pytest.fixture()
def auth_token():
    """Mint a valid admin JWT for authenticated endpoint requests."""
    from airflow.api_fastapi.app import get_auth_manager
    from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser

    user = SimpleAuthManagerUser(username="admin", role="admin")
    return get_auth_manager().generate_jwt(user)
