"""Compatibility re-export: RemoteWorkerWorkMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.remote.worker_agent_do_work import RemoteWorkerDoWorkMixin
from job_queue.remote.worker_agent_spawn_work import RemoteWorkerSpawnWorkMixin
from job_queue.remote.worker_agent_effects_work import RemoteWorkerEffectsWorkMixin
from job_queue.remote.worker_agent_workflow_work import RemoteWorkerWorkflowWorkMixin


class RemoteWorkerWorkMixin(
    RemoteWorkerDoWorkMixin,
    RemoteWorkerSpawnWorkMixin,
    RemoteWorkerEffectsWorkMixin,
    RemoteWorkerWorkflowWorkMixin,
):
    """Aggregated RemoteWorkerWorkMixin (split for maintainability)."""

    pass
