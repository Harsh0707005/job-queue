"""Compatibility re-export: LeasePreemptMixin composed from sub-mixins."""
from __future__ import annotations

from job_queue.queue.lease_estimates import LeaseEstimatesMixin
from job_queue.queue.lease_preempt_ops import LeasePreemptOpsMixin

class LeasePreemptMixin(LeaseEstimatesMixin, LeasePreemptOpsMixin):
    """Aggregated LeasePreemptMixin (split for maintainability)."""

    pass
