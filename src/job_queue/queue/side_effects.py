"""Compatibility re-export: SideEffectsMixin composed from sub-mixins."""
from __future__ import annotations

from job_queue.queue.side_effects_query import SideEffectsQueryMixin
from job_queue.queue.side_effects_lifecycle import SideEffectsLifecycleMixin

class SideEffectsMixin(SideEffectsQueryMixin, SideEffectsLifecycleMixin):
    """Aggregated SideEffectsMixin (split for maintainability)."""

    pass
