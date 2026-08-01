"""Compatibility re-export: TenantsLimitsMixin composed from sub-mixins."""
from __future__ import annotations

from job_queue.queue.tenants_concurrency import TenantsConcurrencyMixin
from job_queue.queue.tenants_rate import TenantsRateMixin
from job_queue.queue.tenants_retention import TenantsRetentionMixin

class TenantsLimitsMixin(TenantsConcurrencyMixin, TenantsRateMixin, TenantsRetentionMixin):
    """Aggregated TenantsLimitsMixin (split for maintainability)."""

    pass
