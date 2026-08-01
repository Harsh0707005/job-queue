"""Shared helpers for job queue functional tests (moved from test_queue.py)."""

from __future__ import annotations

from job_queue import Job, JobQueue, JobStatus


def _ack(queue: JobQueue, job: Job, worker_id: str, result=None) -> Job:
    assert job.lease_token is not None
    return queue.ack(
        job.id,
        tenant_id=job.tenant_id,
        worker_id=worker_id,
        lease_token=job.lease_token,
        result=result,
    )


def _hb(queue: JobQueue, job: Job, worker_id: str, **kw) -> Job:
    assert job.lease_token is not None
    return queue.heartbeat(
        job.id,
        tenant_id=job.tenant_id,
        worker_id=worker_id,
        lease_token=job.lease_token,
        **kw,
    )


def _nack(queue: JobQueue, job: Job, worker_id: str, **kw) -> Job:
    assert job.lease_token is not None
    return queue.nack(
        job.id,
        tenant_id=job.tenant_id,
        worker_id=worker_id,
        lease_token=job.lease_token,
        **kw,
    )


def _drive_to_dead(
    queue: JobQueue, *, tenant_id: str, payload: dict, max_attempts: int = 1, **enq_kw
) -> Job:
    """Enqueue a job and force it into dead via repeated lease+nack."""
    enq_kw.setdefault("retry_backoff_base_seconds", 0)
    job = queue.enqueue(
        tenant_id=tenant_id,
        payload=payload,
        max_attempts=max_attempts,
        **enq_kw,
    )
    for i in range(max_attempts):
        leased = queue.lease(
            tenant_id=tenant_id, worker_id=f"w-dead-{i}", queue_name=job.queue_name
        )
        assert leased is not None
        assert leased.id == job.id
        result = _nack(queue, leased, f"w-dead-{i}", error=f"fail-{i}", requeue=True)
        if i + 1 < max_attempts:
            assert result.status == JobStatus.PENDING
        else:
            assert result.status == JobStatus.DEAD
            return result
    raise AssertionError("job never reached dead")


class _WebhookCapture:
    """Minimal HTTP server that records POSTs for tests."""

    def __init__(self, *, fail_times: int = 0, status: int = 200):
        from http.server import BaseHTTPRequestHandler, HTTPServer
        import threading

        self.received: list[dict] = []
        self.fail_times = fail_times
        self._fails_left = fail_times
        self.status = status
        self._lock = threading.Lock()
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                import json

                with parent._lock:
                    should_fail = parent._fails_left > 0
                    if should_fail:
                        parent._fails_left -= 1
                    parent.received.append(
                        {
                            "path": self.path,
                            "body": json.loads(body.decode("utf-8")),
                            "signature": self.headers.get("X-Job-Queue-Signature"),
                        }
                    )
                if should_fail:
                    self.send_response(500)
                    self.end_headers()
                    self.wfile.write(b"fail")
                else:
                    self.send_response(parent.status)
                    self.end_headers()
                    self.wfile.write(b"ok")

            def log_message(self, format, *args):  # noqa: A003
                return

        self._httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}/hook"
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._thread.join(timeout=5)


def _utcnow():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)
