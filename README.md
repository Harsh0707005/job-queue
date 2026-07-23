# Multi-tenant Job Queue

A PostgreSQL-backed job queue in Python with **lease / heartbeat / ack** semantics and **visibility-timeout recovery**.

## Features

| Feature | Behavior |
|---|---|
| **Multi-tenant** | Every operation is scoped by `tenant_id`. Tenants never see each other's jobs. |
| **Lease** | Workers claim jobs with `SELECT … FOR UPDATE SKIP LOCKED`. Only one worker can hold a job. |
| **Heartbeat** | Extend `lease_expires_at` while work is in progress. |
| **Ack** | Mark the job completed; it will never run again. |
| **Visibility timeout** | If a worker dies (no heartbeat/ack), the lease expires and another worker can reclaim the job. Work is not lost and not run by two live workers at once. |
| **Nack / dead-letter** | Failed jobs requeue until `max_attempts`, then move to `dead`. |
| **Delayed jobs** | `available_at` / `delay_seconds` — pending jobs are not claimable until that time. |
| **Deduplication** | `dedupe_key` — second enqueue of an active key returns the existing job. |
| **Tenant concurrency** | `set_concurrency_limit(tenant, n)` — at most n leased jobs per tenant. |
| **Cancel** | `cancel(job_id, tenant_id=...)` — cancels **pending** jobs only; leased jobs are left alone. |
| **Dead letter inspect/replay** | `list_dead_letters` + `replay_dead_letter` / `replay_dead_letters` re-queue exhausted jobs. |
| **Recurring schedules** | `create_schedule` every N seconds; `process_due_schedules` / `ScheduleRunner`; pause/resume/delete. |

## Install

```bash
# runtime
uv pip install -e .

# with test deps
uv pip install -e ".[dev]"
```

Requires Docker for integration tests (testcontainers spins up Postgres and deletes the container when tests finish).

## Quick start

```python
from job_queue import JobQueue, Worker, Job

queue = JobQueue.connect("postgresql://user:pass@localhost:5432/jobs")

# Enqueue
queue.enqueue(
    tenant_id="acme",
    payload={"email": "user@example.com"},
    queue_name="email",
    visibility_timeout_seconds=30,
    delay_seconds=60,              # optional: not claimable for 60s
    dedupe_key="welcome-user-42",  # optional: collapse duplicate enqueues
)

# Worker
worker = Worker(queue, tenant_id="acme", queue_name="email", worker_id="w1")

@worker.handler
def process(job: Job) -> None:
    send_email(job.payload["email"])

worker.run(stop_when_idle=True)
queue.close()
```

### Manual lease API

```python
job = queue.lease(tenant_id="acme", worker_id="w1", queue_name="email")
if job:
    try:
        do_work(job.payload)
        queue.heartbeat(
            job.id,
            tenant_id="acme",
            worker_id="w1",
            lease_token=job.lease_token,
        )
        queue.ack(
            job.id,
            tenant_id="acme",
            worker_id="w1",
            lease_token=job.lease_token,
        )
    except Exception as e:
        queue.nack(
            job.id,
            tenant_id="acme",
            worker_id="w1",
            lease_token=job.lease_token,
            error=str(e),
            requeue=True,
        )
```

## Why jobs don't double-run

1. `lease()` locks one row with `FOR UPDATE SKIP LOCKED`, sets `status=leased` / `leased_by` / `lease_expires_at`, and mints a **fencing `lease_token`**.
2. Other workers skip that row until the lease expires.
3. `heartbeat` / `ack` / `nack` require `tenant_id` + `worker_id` + `lease_token`. A zombie worker whose lease was reclaimed cannot complete the job.
4. The `Worker` helper heartbeats automatically and signals cancel when the lease is lost so handlers can abort before side effects.
5. If the process dies (no heartbeat), the lease expires; the next `lease()` reclaims it with a **new** token (attempt counter increments). Only the current holder can ack.
6. Exhausted expired leases are bulk dead-lettered on claim — no skip-limit that can hide remaining work.

Handlers with external side effects should call `lease.ensure_held()` (or check `lease.lease_lost`) before committing them, or make effects idempotent on `job.id`.

## Tests

```bash
uv pip install -e ".[dev]"
pytest -v
```

Tests use [testcontainers](https://testcontainers.com/) to start a disposable `postgres:16-alpine` container for the session and **remove it on teardown**. No leftover containers.

## Layout

```
src/job_queue/
  models.py    # Job, JobStatus
  schema.py    # DDL
  db.py        # pool + transactions
  queue.py     # enqueue / lease / heartbeat / ack / nack
  worker.py    # polling worker with background heartbeat
tests/
  conftest.py  # Docker Postgres fixture (auto-cleanup)
  test_queue.py
```
