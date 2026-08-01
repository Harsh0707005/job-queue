"""Compatibility re-export: LeaseClaimMixin composed from sub-mixins."""
from __future__ import annotations

from job_queue.queue.lease_rate import LeaseRateMixin
from job_queue.queue.lease_claim_scoped import LeaseClaimScopedMixin
from job_queue.queue.lease_claim_fair import LeaseClaimFairMixin

class LeaseClaimMixin(LeaseRateMixin, LeaseClaimScopedMixin, LeaseClaimFairMixin):
    """Aggregated LeaseClaimMixin (split for maintainability)."""

    pass
