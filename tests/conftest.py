"""Pytest fixtures: disposable Postgres via Docker (testcontainers).

A Postgres container is started for the test session and **force-removed**
in a ``finally`` block so nothing is left running after tests exit.
Ryuk is disabled; we own lifecycle cleanup ourselves.
"""

from __future__ import annotations

import base64
import os

import pytest

# We clean up containers ourselves in fixture teardown — no need for Ryuk.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")
# Unit/soak tests deliver webhooks to local HTTP sinks (127.0.0.1). Production
# defaults block private destinations; tests opt in explicitly.
os.environ.setdefault("JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS", "1")
# Per-tenant payload encryption KEK (32 bytes). Shared by all tests / reconnects.
os.environ.setdefault(
    "JOB_QUEUE_PAYLOAD_KEK",
    base64.b64encode(b"0123456789abcdef0123456789abcdef").decode(),
)
os.environ.setdefault("JOB_QUEUE_PAYLOAD_KEK_ID", "test")

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
    """Start a throwaway Postgres container; delete it after all tests.

    Set ``JOB_QUEUE_TEST_DSN`` to reuse an already-running database (when
    Docker Desktop is unavailable).
    """
    existing = os.environ.get("JOB_QUEUE_TEST_DSN")
    if existing:
        yield existing
        return
    postgres = PostgresContainer("postgres:16-alpine")
    postgres.start()
    try:
        yield _psycopg_dsn(postgres.get_connection_url(driver="psycopg"))
    finally:
        # stop() removes the container (not just stop) when remove=True (default).
        postgres.stop()


@pytest.fixture
def queue(postgres_url: str):
    """Fresh JobQueue against the session Postgres, with a clean jobs table.

    Most unit suites never register workers for partition membership. They opt
    out of strict registration so lease paths stay free-for-all. Partition
    tests re-enable ``require_worker_registration`` explicitly.
    """
    q = JobQueue.connect(
        postgres_url,
        min_size=1,
        max_size=8,
        require_worker_registration=False,
    )
    with q._pool.connection() as conn:
        conn.execute("TRUNCATE jobs CASCADE")
        conn.execute("TRUNCATE job_dag_batches")
        # Audit is intentionally not FK-cascaded from jobs (rebuild survives row loss);
        # clear it each test so list_orphaned_audit_jobs stays order-independent.
        conn.execute("TRUNCATE job_audit_events")
        conn.execute("TRUNCATE tenant_settings")
        conn.execute("TRUNCATE tenant_fairness")
        conn.execute("TRUNCATE schedule_generations, schedule_blackouts, schedules")
        conn.execute("TRUNCATE webhook_deliveries")
        conn.execute("TRUNCATE webhook_endpoints CASCADE")
        conn.execute("TRUNCATE queue_metric_buckets")
        conn.execute("TRUNCATE queue_metric_snapshots")
        conn.execute("TRUNCATE tenant_usage_buckets")
        conn.execute("TRUNCATE duration_estimates")
        conn.execute("TRUNCATE job_worker_affinity")
        conn.execute("TRUNCATE alert_events CASCADE")
        conn.execute("TRUNCATE alert_rules CASCADE")
        # Partition ownership must not leak across tests.
        conn.execute("TRUNCATE partition_assignments")
        conn.execute("TRUNCATE worker_nodes")
        conn.execute("TRUNCATE payload_key_rotations")
        conn.execute("TRUNCATE tenant_payload_keys")
        conn.execute("TRUNCATE workflow_signals CASCADE")
        conn.execute("TRUNCATE workflow_step_runs CASCADE")
        conn.execute("TRUNCATE workflow_runs CASCADE")
        conn.execute("TRUNCATE tenant_restores CASCADE")
        conn.execute("TRUNCATE tenant_snapshots CASCADE")
        conn.execute("TRUNCATE coordinator_leader")
        conn.execute("TRUNCATE coordinator_nodes")
        conn.execute(
            """
            UPDATE queue_settings
            SET partition_count = 32,
                worker_drain_release_held = FALSE,
                updated_at = NOW()
            WHERE id = 1
            """
        )
        conn.commit()
    yield q
    q.close()
