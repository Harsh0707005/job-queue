"""Compatibility re-export: ApprovalSpawnMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.approval import ApprovalMixin
from job_queue.queue.spawn import SpawnMixin


class ApprovalSpawnMixin(ApprovalMixin, SpawnMixin):
    """Aggregated ApprovalSpawnMixin (split for maintainability)."""

    pass
