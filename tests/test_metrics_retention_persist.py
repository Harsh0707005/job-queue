"""Metrics retention must survive schema re-init (reconnect / coordinator restart)."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer


from job_queue import JobQueue
from job_queue.db import init_schema
from job_queue.coordinator.app import Coordinator, _make_handler


def _reinit_schema(queue: JobQueue) -> None:
    """Simulate reconnect / multi-coordinator boot: run init_schema again."""
    with queue._pool.connection() as conn:
        init_schema(conn)


def test_null_metrics_retention_survives_repeated_init_schema(queue: JobQueue):
    """Intentional keep-forever (NULL) must not be rewritten to 7d on restart."""
    queue.set_metrics_retention(None)
    assert queue.get_metrics_retention() is None

    for _ in range(3):
        _reinit_schema(queue)
        assert queue.get_metrics_retention() is None

    # Finite value also sticks across re-init.
    queue.set_metrics_retention(3600)
    assert queue.get_metrics_retention() == 3600
    _reinit_schema(queue)
    assert queue.get_metrics_retention() == 3600

    queue.set_metrics_retention(None)
    _reinit_schema(queue)
    assert queue.get_metrics_retention() is None


def test_reconnect_connect_preserves_null_retention(postgres_url: str):
    """Fresh JobQueue.connect (new pool) after set NULL keeps forever."""
    q1 = JobQueue.connect(postgres_url, min_size=1, max_size=2)
    try:
        with q1._pool.connection() as conn:
            conn.execute("TRUNCATE queue_settings")
            # Ensure clean singleton with explicit forever.
            conn.execute(
                """
                INSERT INTO queue_settings (id, metrics_retention_seconds)
                VALUES (1, NULL)
                """
            )
            conn.commit()
        assert q1.get_metrics_retention() is None
    finally:
        q1.close()

    q2 = JobQueue.connect(postgres_url, min_size=1, max_size=2)
    try:
        # connect() always runs init_schema — must not clobber NULL.
        assert q2.get_metrics_retention() is None
        q2.set_metrics_retention(7200)
        assert q2.get_metrics_retention() == 7200
    finally:
        q2.close()

    q3 = JobQueue.connect(postgres_url, min_size=1, max_size=2)
    try:
        assert q3.get_metrics_retention() == 7200
        q3.set_metrics_retention(None)
    finally:
        q3.close()

    q4 = JobQueue.connect(postgres_url, min_size=1, max_size=2)
    try:
        assert q4.get_metrics_retention() is None
    finally:
        q4.close()


def test_new_install_default_is_seven_days_once(postgres_url: str):
    """Brand-new settings row gets 7d; migration does not re-fire."""
    q = JobQueue.connect(postgres_url, min_size=1, max_size=2)
    try:
        with q._pool.connection() as conn:
            conn.execute("TRUNCATE queue_settings")
            conn.execute(
                "DELETE FROM schema_migrations WHERE name = %s",
                ("metrics_retention_default_7d_v1",),
            )
            conn.commit()
            # Re-run schema as a new install would (migration may re-apply once).
            init_schema(conn)
        # Either INSERT default 7d or one-time migration → 7 days, not forever.
        # After truncate of settings, INSERT ON CONFLICT inserts 604800.
        got = q.get_metrics_retention()
        assert got == 604800

        q.set_metrics_retention(None)
        with q._pool.connection() as conn:
            init_schema(conn)
        assert q.get_metrics_retention() is None
    finally:
        q.close()


def test_coordinator_http_retention_survives_schema_reinit(postgres_url: str):
    q = JobQueue.connect(postgres_url, min_size=1, max_size=4)
    try:
        with q._pool.connection() as conn:
            conn.execute("TRUNCATE queue_settings")
            conn.execute(
                """
                INSERT INTO queue_settings (id, metrics_retention_seconds)
                VALUES (1, NULL)
                """
            )
            conn.commit()

        coord = Coordinator(q)
        server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{port}"

        def get_ret() -> object:
            with urllib.request.urlopen(f"{base}/v1/metrics/retention", timeout=5) as r:
                return json.loads(r.read().decode())["metrics_retention_seconds"]

        def set_ret(val: object) -> object:
            req = urllib.request.Request(
                f"{base}/v1/metrics/retention",
                data=json.dumps({"metrics_retention_seconds": val}).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as r:
                return json.loads(r.read().decode())["metrics_retention_seconds"]

        assert get_ret() is None
        with q._pool.connection() as conn:
            init_schema(conn)
        assert get_ret() is None
        assert set_ret(86400) == 86400
        with q._pool.connection() as conn:
            init_schema(conn)
        assert get_ret() == 86400
        assert set_ret(None) is None
        with q._pool.connection() as conn:
            init_schema(conn)
        assert get_ret() is None
        server.shutdown()
    finally:
        q.close()
