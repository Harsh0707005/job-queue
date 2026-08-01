"""Compatibility re-export: SpawnMixin composed from sub-mixins."""
from __future__ import annotations

from job_queue.queue.spawn_api import SpawnApiMixin
from job_queue.queue.spawn_join import SpawnJoinMixin

class SpawnMixin(SpawnApiMixin, SpawnJoinMixin):
    """Aggregated SpawnMixin (split for maintainability)."""

    pass
