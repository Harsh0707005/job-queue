"""Pytest fixtures: disposable Postgres via Docker (testcontainers).

A Postgres container is started for the test session and **force-removed**
in a ``finally`` block so nothing is left running after tests exit.
Ryuk is disabled; we own lifecycle cleanup ourselves.
"""

from __future__ import annotations

import os

import pytest

# We clean up containers ourselves in fixture teardown — no need for Ryuk.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")
# Unit/soak tests deliver webhooks to local HTTP sinks (127.0.0.1). Production
# defaults block private destinations; tests opt in explicitly.
os.environ.setdefault("JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS", "1")

from testcontainers.postgres import PostgresContainer  # noqa: E402

from job_queue import JobQueue  # noqa: E402


def _psycopg_dsn(sqlalchemy_url: str) -> str:
    """Convert SQLAlchemy-style URL to a plain psycopg3 DSN."""
    return (
        sqlalchemy_url.replace("postgresql+psycopg://", "postgresql://", 1)
        .replace("postgresql+psycopg2://", "postgresql://", 1)
        .replace("postgres+psycopg://", "postgresql://", 1)
    )


@pytest.fixture(scope="session")
def postgres_url():
    """Start a throwaway Postgres container; delete it after all tests."""
    postgres = PostgresContainer("postgres:16-alpine")
    postgres.start()
    try:
        yield _psycopg_dsn(postgres.get_connection_url(driver="psycopg"))
    finally:
        # stop() removes the container (not just stop) when remove=True (default).
        postgres.stop()


@pytest.fixture
def queue(postgres_url: str):
    """Fresh JobQueue against the session Postgres, with a clean jobs table."""
    q = JobQueue.connect(postgres_url, min_size=1, max_size=8)
    with q._pool.connection() as conn:
        conn.execute("TRUNCATE jobs CASCADE")
        conn.execute("TRUNCATE tenant_settings")
        conn.execute("TRUNCATE tenant_fairness")
        conn.execute("TRUNCATE schedules")
        conn.execute("TRUNCATE webhook_deliveries")
        conn.execute("TRUNCATE webhook_endpoints CASCADE")
        conn.commit()
    yield q
    q.close()
