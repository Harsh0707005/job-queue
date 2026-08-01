"""Compatibility re-export: SpawnApiMixin composed from sub-mixins."""
from __future__ import annotations

from job_queue.queue.spawn_children import SpawnChildrenMixin
from job_queue.queue.spawn_query import SpawnQueryMixin

class SpawnApiMixin(SpawnChildrenMixin, SpawnQueryMixin):
    """Aggregated SpawnApiMixin (split for maintainability)."""

    pass
