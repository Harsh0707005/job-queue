#!/usr/bin/env python3
"""Full-stack soak: disposable Postgres + schedules + fair workers + webhooks.

Spins up a throwaway Postgres container, registers one recurring schedule per
tenant (default: 10 tenants, fire every 30s) with job.completed webhooks,
then runs:

  * fair multi-tenant workers (tenant_id=None)
  * ScheduleRunner
  * WebhookDispatcher
  * a local HTTP capture server for webhook POSTs

Keeps the stack up until every schedule has at least TARGET completed jobs
and the job/webhook backlog is fully drained. On component death, restarts
workers / runners. On hard failure, tears down and restarts the whole stack.

Env overrides (optional):
  TENANTS=10
  INTERVAL_SECONDS=30
  TARGET_COMPLETED=60
  WORKERS=4
  MAX_WALL_SECONDS=3600
  POLL_SECONDS=2
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from uuid import UUID

# Own container lifecycle (no Ryuk left behind).
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

from testcontainers.postgres import PostgresContainer

from job_queue import (
    Job,
    JobQueue,
    JobStatus,
    ScheduleRunner,
    WebhookDispatcher,
    Worker,
)
from job_queue.webhooks import WebhookDeliveryResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("stack_soak")

TENANTS = int(os.environ.get("TENANTS", "10"))
INTERVAL_SECONDS = int(os.environ.get("INTERVAL_SECONDS", "30"))
TARGET_COMPLETED = int(os.environ.get("TARGET_COMPLETED", "60"))
NUM_WORKERS = int(os.environ.get("WORKERS", "4"))
MAX_WALL_SECONDS = int(os.environ.get("MAX_WALL_SECONDS", "3600"))
POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "2"))
FULL_STACK_RESTARTS = int(os.environ.get("FULL_STACK_RESTARTS", "3"))


def _psycopg_dsn(sqlalchemy_url: str) -> str:
    return (
        sqlalchemy_url.replace("postgresql+psycopg://", "postgresql://", 1)
        .replace("postgresql+psycopg2://", "postgresql://", 1)
        .replace("postgres+psycopg://", "postgresql://", 1)
    )


# ---------------------------------------------------------------------------
# Local webhook sink
# ---------------------------------------------------------------------------


class WebhookSink:
    """Minimal HTTP server that accepts webhook POSTs (always 200)."""

    def __init__(self) -> None:
        self.received = 0
        self.by_tenant: dict[str, int] = {}
        self._lock = threading.Lock()
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                try:
                    payload = json.loads(body.decode("utf-8") or "{}")
                except json.JSONDecodeError:
                    payload = {}
                tenant = str(payload.get("tenant_id") or "?")
                with parent._lock:
                    parent.received += 1
                    parent.by_tenant[tenant] = parent.by_tenant.get(tenant, 0) + 1
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")

            def log_message(self, format, *args):  # noqa: A003
                return

        self._httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}/hook"
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="webhook-sink",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        try:
            self._httpd.shutdown()
        except Exception:  # noqa: BLE001
            pass
        self._thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Component supervision
# ---------------------------------------------------------------------------


@dataclass
class Component:
    name: str
    thread: threading.Thread
    stop: Any  # object with .stop()
    restarts: int = 0
    last_error: str | None = None


@dataclass
class StackState:
    schedule_ids: dict[str, UUID] = field(default_factory=dict)  # tenant -> id
    components: list[Component] = field(default_factory=list)
    stop_all: threading.Event = field(default_factory=threading.Event)
    fatal: str | None = None


def _run_worker(queue: JobQueue, worker_id: str, stop: threading.Event) -> None:
    worker = Worker(
        queue,
        tenant_id=None,  # fair multi-tenant lease
        worker_id=worker_id,
        poll_interval_seconds=0.2,
        heartbeat_interval_seconds=2.0,
        visibility_timeout_seconds=30,
    )

    @worker.handler
    def process(job: Job) -> dict[str, Any]:
        # Fast handler: emulate light work, return a result payload.
        time.sleep(0.01)
        return {
            "ok": True,
            "tenant_id": job.tenant_id,
            "schedule_id": str(job.schedule_id) if job.schedule_id else None,
            "worker_id": worker_id,
        }

    # Bridge external stop Event into worker.stop()
    def watch() -> None:
        stop.wait()
        worker.stop()

    threading.Thread(target=watch, name=f"{worker_id}-watch", daemon=True).start()
    worker.run()


def _run_schedule_runner(queue: JobQueue, stop: threading.Event) -> None:
    runner = ScheduleRunner(queue, poll_interval_seconds=0.5, batch_limit=100)

    def watch() -> None:
        stop.wait()
        runner.stop()

    threading.Thread(target=watch, name="sched-watch", daemon=True).start()
    runner.run()


def _run_webhook_dispatcher(queue: JobQueue, stop: threading.Event) -> None:
    def on_result(r: WebhookDeliveryResult) -> None:
        if not r.success:
            logger.warning(
                "webhook fail delivery=%s status=%s err=%s",
                r.delivery.id,
                r.status_code,
                r.error,
            )

    dispatcher = WebhookDispatcher(
        queue,
        poll_interval_seconds=0.5,
        batch_limit=100,
        http_timeout_seconds=5.0,
        on_result=on_result,
    )

    def watch() -> None:
        stop.wait()
        dispatcher.stop()

    threading.Thread(target=watch, name="wh-watch", daemon=True).start()
    dispatcher.run()


def _start_component(
    state: StackState,
    name: str,
    target,
    *args,
) -> Component:
    stop = threading.Event()

    def wrapper() -> None:
        try:
            target(*args, stop)
        except Exception as exc:  # noqa: BLE001
            logger.exception("component %s crashed: %s", name, exc)
            for c in state.components:
                if c.name == name:
                    c.last_error = f"{type(exc).__name__}: {exc}"
                    break

    t = threading.Thread(target=wrapper, name=name, daemon=True)
    comp = Component(name=name, thread=t, stop=stop)
    state.components.append(comp)
    t.start()
    return comp


def start_runtime(queue: JobQueue, state: StackState) -> None:
    state.components.clear()
    for i in range(NUM_WORKERS):
        _start_component(state, f"worker-{i}", _run_worker, queue, f"fair-w{i}")
    _start_component(state, "schedule-runner", _run_schedule_runner, queue)
    _start_component(state, "webhook-dispatcher", _run_webhook_dispatcher, queue)
    logger.info(
        "started runtime: %d fair workers + schedule runner + webhook dispatcher",
        NUM_WORKERS,
    )


def stop_runtime(state: StackState, *, join_timeout: float = 10.0) -> None:
    for c in state.components:
        c.stop.set()
    deadline = time.time() + join_timeout
    for c in state.components:
        remaining = max(0.1, deadline - time.time())
        c.thread.join(timeout=remaining)
    state.components.clear()


def supervise_components(queue: JobQueue, state: StackState) -> None:
    """Restart any dead component threads (unless global stop)."""
    if state.stop_all.is_set():
        return
    for c in list(state.components):
        if c.thread.is_alive():
            continue
        logger.error(
            "component %s died (error=%s); restarting",
            c.name,
            c.last_error,
        )
        c.restarts += 1
        if c.restarts > 20:
            state.fatal = f"component {c.name} restarted too many times"
            state.stop_all.set()
            return
        # Rebuild the same kind of component.
        name = c.name
        state.components.remove(c)
        if name.startswith("worker-"):
            wid = name.replace("worker-", "fair-w")
            # name is worker-0 → worker id fair-w0 was used with i; map carefully
            idx = name.split("-", 1)[1]
            _start_component(state, name, _run_worker, queue, f"fair-w{idx}")
        elif name == "schedule-runner":
            _start_component(state, name, _run_schedule_runner, queue)
        elif name == "webhook-dispatcher":
            _start_component(state, name, _run_webhook_dispatcher, queue)
        else:
            state.fatal = f"unknown component {name}"
            state.stop_all.set()


# ---------------------------------------------------------------------------
# Progress queries
# ---------------------------------------------------------------------------


def completed_per_schedule(queue: JobQueue, schedule_ids: list[UUID]) -> dict[UUID, int]:
    if not schedule_ids:
        return {}
    with queue._pool.connection() as conn:
        rows = conn.execute(
            """
            SELECT schedule_id, COUNT(*)::int AS n
            FROM jobs
            WHERE schedule_id = ANY(%s)
              AND status = %s
            GROUP BY schedule_id
            """,
            (list(schedule_ids), JobStatus.COMPLETED.value),
        ).fetchall()
    out = {sid: 0 for sid in schedule_ids}
    for r in rows:
        out[r["schedule_id"]] = int(r["n"])
    return out


def backlog_counts(queue: JobQueue) -> dict[str, int]:
    with queue._pool.connection() as conn:
        job_row = conn.execute(
            """
            SELECT
              COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
              COUNT(*) FILTER (WHERE status = 'leased')::int AS leased,
              COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
              COUNT(*) FILTER (WHERE status = 'dead')::int AS dead,
              COUNT(*) FILTER (WHERE status = 'completed')::int AS completed
            FROM jobs
            """
        ).fetchone()
        wh_row = conn.execute(
            """
            SELECT
              COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
              COUNT(*) FILTER (WHERE status = 'delivering')::int AS delivering,
              COUNT(*) FILTER (WHERE status = 'dead')::int AS dead,
              COUNT(*) FILTER (WHERE status = 'succeeded')::int AS succeeded
            FROM webhook_deliveries
            """
        ).fetchone()
    assert job_row is not None and wh_row is not None
    return {
        "job_pending": int(job_row["pending"]),
        "job_leased": int(job_row["leased"]),
        "job_failed": int(job_row["failed"]),
        "job_dead": int(job_row["dead"]),
        "job_completed": int(job_row["completed"]),
        "wh_pending": int(wh_row["pending"]),
        "wh_delivering": int(wh_row["delivering"]),
        "wh_dead": int(wh_row["dead"]),
        "wh_succeeded": int(wh_row["succeeded"]),
    }


def disable_schedules_at_target(
    queue: JobQueue,
    tenant_to_sid: dict[str, UUID],
    counts: dict[UUID, int],
    disabled: set[UUID],
) -> None:
    for tenant_id, sid in tenant_to_sid.items():
        if sid in disabled:
            continue
        if counts.get(sid, 0) >= TARGET_COMPLETED:
            try:
                queue.pause_schedule(sid, tenant_id=tenant_id)
                disabled.add(sid)
                logger.info(
                    "schedule %s tenant=%s reached %d completed — paused",
                    sid,
                    tenant_id,
                    counts[sid],
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("pause_schedule failed: %s", exc)


def goal_met(
    counts: dict[UUID, int],
    backlog: dict[str, int],
    n_schedules: int,
) -> bool:
    if len(counts) < n_schedules:
        return False
    if any(counts.get(sid, 0) < TARGET_COMPLETED for sid in counts):
        return False
    # Zero pending / leased jobs and no open webhook rows.
    if backlog["job_pending"] != 0 or backlog["job_leased"] != 0:
        return False
    if backlog["wh_pending"] != 0 or backlog["wh_delivering"] != 0:
        return False
    # No dead jobs or dead webhooks — treat as failure path.
    if backlog["job_dead"] != 0 or backlog["wh_dead"] != 0:
        return False
    return True


def register_tenants(queue: JobQueue, webhook_url: str) -> dict[str, UUID]:
    tenant_to_sid: dict[str, UUID] = {}
    for i in range(TENANTS):
        tenant_id = f"tenant-{i:02d}"
        queue.register_webhook(
            tenant_id=tenant_id,
            url=webhook_url,
            events=["job.completed"],
            secret=f"secret-{tenant_id}",
            max_attempts=8,
            backoff_base_seconds=1,
            backoff_max_seconds=30,
        )
        sched = queue.create_schedule(
            tenant_id=tenant_id,
            name=f"soak-{tenant_id}",
            interval_seconds=INTERVAL_SECONDS,
            payload={"tenant": tenant_id, "kind": "soak"},
            visibility_timeout_seconds=30,
            max_attempts=3,
        )
        tenant_to_sid[tenant_id] = sched.id
        logger.info("registered %s schedule=%s webhook=%s", tenant_id, sched.id, webhook_url)
    return tenant_to_sid


# ---------------------------------------------------------------------------
# One full stack attempt
# ---------------------------------------------------------------------------


def run_one_attempt(attempt: int) -> bool:
    logger.info(
        "=== stack attempt %d: tenants=%d interval=%ds target=%d workers=%d ===",
        attempt,
        TENANTS,
        INTERVAL_SECONDS,
        TARGET_COMPLETED,
        NUM_WORKERS,
    )
    sink = WebhookSink()
    postgres = PostgresContainer("postgres:16-alpine")
    postgres.start()
    dsn = _psycopg_dsn(postgres.get_connection_url(driver="psycopg"))
    logger.info("postgres up dsn=%s webhook_sink=%s", dsn, sink.url)

    queue: JobQueue | None = None
    state = StackState()
    started = time.monotonic()
    disabled: set[UUID] = set()

    try:
        queue = JobQueue.connect(dsn, min_size=2, max_size=16)
        tenant_to_sid = register_tenants(queue, sink.url)
        state.schedule_ids = tenant_to_sid
        sids = list(tenant_to_sid.values())

        start_runtime(queue, state)
        last_log = 0.0

        while not state.stop_all.is_set():
            elapsed = time.monotonic() - started
            if elapsed > MAX_WALL_SECONDS:
                state.fatal = f"wall clock exceeded {MAX_WALL_SECONDS}s"
                break

            supervise_components(queue, state)
            if state.fatal:
                break

            counts = completed_per_schedule(queue, sids)
            backlog = backlog_counts(queue)
            disable_schedules_at_target(queue, tenant_to_sid, counts, disabled)

            # Dead jobs / webhooks mean the stack failed the soak criteria.
            if backlog["job_dead"] > 0 or backlog["wh_dead"] > 0:
                state.fatal = (
                    f"dead backlog: jobs={backlog['job_dead']} "
                    f"webhooks={backlog['wh_dead']}"
                )
                break

            if goal_met(counts, backlog, len(sids)):
                mins = [counts[s] for s in sids]
                logger.info(
                    "GOAL MET in %.1fs: min_completed=%d all=%s backlog=%s "
                    "webhooks_received=%d",
                    elapsed,
                    min(mins) if mins else 0,
                    {str(k)[:8]: v for k, v in counts.items()},
                    backlog,
                    sink.received,
                )
                # Sanity: every completed job should have produced a webhook.
                if sink.received < TARGET_COMPLETED * TENANTS:
                    # Allow small lag then re-check once more.
                    time.sleep(2)
                    backlog = backlog_counts(queue)
                    if backlog["wh_succeeded"] < TARGET_COMPLETED * TENANTS:
                        state.fatal = (
                            f"webhook under-delivery: sink={sink.received} "
                            f"succeeded={backlog['wh_succeeded']} "
                            f"expected>={TARGET_COMPLETED * TENANTS}"
                        )
                        break
                return True

            now = time.monotonic()
            if now - last_log >= 10.0:
                last_log = now
                min_c = min(counts.values()) if counts else 0
                max_c = max(counts.values()) if counts else 0
                logger.info(
                    "progress t=%.0fs completed[min=%d max=%d sum=%d] "
                    "paused_sched=%d/%d backlog=%s sink=%d",
                    elapsed,
                    min_c,
                    max_c,
                    sum(counts.values()),
                    len(disabled),
                    len(sids),
                    backlog,
                    sink.received,
                )

            time.sleep(POLL_SECONDS)

        logger.error("attempt %d failed: %s", attempt, state.fatal)
        return False
    except Exception as exc:  # noqa: BLE001
        logger.error("attempt %d exception: %s\n%s", attempt, exc, traceback.format_exc())
        return False
    finally:
        stop_runtime(state)
        if queue is not None:
            try:
                queue.close()
            except Exception:  # noqa: BLE001
                pass
        try:
            postgres.stop()
        except Exception:  # noqa: BLE001
            logger.exception("postgres cleanup failed")
        sink.stop()
        logger.info("attempt %d cleaned up", attempt)


def main() -> int:
    logger.info(
        "stack soak config: tenants=%d interval=%ds target=%d workers=%d "
        "max_wall=%ds full_restarts=%d",
        TENANTS,
        INTERVAL_SECONDS,
        TARGET_COMPLETED,
        NUM_WORKERS,
        MAX_WALL_SECONDS,
        FULL_STACK_RESTARTS,
    )
    # Rough ETA: no-overlap + interval means ~TARGET * INTERVAL wall time.
    eta = TARGET_COMPLETED * INTERVAL_SECONDS
    logger.info("expected minimum wall time ~%ds (%.1f min)", eta, eta / 60.0)

    for attempt in range(1, FULL_STACK_RESTARTS + 1):
        ok = run_one_attempt(attempt)
        if ok:
            logger.info("STACK SOAK PASSED on attempt %d", attempt)
            return 0
        logger.warning(
            "STACK SOAK failed attempt %d/%d — restarting full stack",
            attempt,
            FULL_STACK_RESTARTS,
        )
        time.sleep(2)

    logger.error("STACK SOAK FAILED after %d attempts", FULL_STACK_RESTARTS)
    return 1


if __name__ == "__main__":
    # Allow Ctrl-C to exit cleanly.
    signal.signal(signal.SIGINT, lambda *_: sys.exit(130))
    sys.exit(main())
