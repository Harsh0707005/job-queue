"""Remote worker agent (composed from loop + work mixins)."""
from __future__ import annotations

from job_queue.remote.worker_agent_loop import RemoteWorkerLoopMixin
from job_queue.remote.worker_agent_work import RemoteWorkerWorkMixin


class RemoteWorkerAgent(RemoteWorkerLoopMixin, RemoteWorkerWorkMixin):
    """Remote worker: leases/heartbeats/acks only via coordinator HTTP.

    Graceful drain on deploy/shutdown::

        agent.drain(timeout_seconds=60)
        # no new leases; finish in-flight with heartbeats; on timeout
        # release remaining jobs for immediate reclaim by others
    """

    pass


__all__ = ["RemoteWorkerAgent"]
