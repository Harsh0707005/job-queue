"""Worker helper that leases jobs and heartbeats while processing.

Double-run protection
---------------------
Visibility-timeout reclaim is how dead workers free jobs. That means a
*zombie* worker (still running after its lease expired) can overlap with a
new holder. We prevent incorrect completion and shrink the overlap window:

1. Each lease mints a unique ``lease_token`` (fencing token).
2. Heartbeat / ack / nack require that token; after reclaim they fail.
3. The worker heartbeats on a short interval and sets a cancel flag as soon
   as the lease is lost so the handler can abort before side effects.
4. Ack only runs if the lease is still held after the handler returns.
5. If the lease was lost, we do **not** nack (another worker may already own
   the job) — we raise ``LeaseError`` instead of re-queueing a duplicate.

Handlers that perform external side effects should call
``lease_lost.is_set()`` / ``ctx.ensure_held()`` before committing them, or
use idempotent writes keyed by ``job.id``.
"""

from __future__ import annotations

from job_queue.lease_context import LeaseContext, _ActiveJob
from job_queue.worker_loop import WorkerLoopMixin
from job_queue.worker_process import WorkerProcessMixin


class Worker(WorkerLoopMixin, WorkerProcessMixin):
    """Poll the queue, run a handler, ack/nack with fencing-token heartbeats."""

    pass


__all__ = ["Worker", "LeaseContext", "_ActiveJob"]
