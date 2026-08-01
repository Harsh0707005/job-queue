"""Compatibility re-export: LeaseDeadlinesMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.lease_max_runtime import LeaseMaxRuntimeMixin
from job_queue.queue.lease_reap import LeaseReapMixin


class LeaseDeadlinesMixin(LeaseMaxRuntimeMixin, LeaseReapMixin):
    """Aggregated LeaseDeadlinesMixin (split for maintainability)."""

    pass
