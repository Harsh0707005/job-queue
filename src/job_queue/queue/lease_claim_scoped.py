"""Compatibility re-export: LeaseClaimScopedMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.lease_claim_public import LeaseClaimPublicMixin
from job_queue.queue.lease_claim_tenant import LeaseClaimTenantMixin


class LeaseClaimScopedMixin(LeaseClaimPublicMixin, LeaseClaimTenantMixin):
    """Aggregated LeaseClaimScopedMixin (split for maintainability)."""

    pass
