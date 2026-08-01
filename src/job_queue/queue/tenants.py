"""Compatibility re-export: TenantsMixin composed from sub-mixins."""
from __future__ import annotations

from job_queue.queue.tenants_limits import TenantsLimitsMixin
from job_queue.queue.tenants_lifecycle import TenantsLifecycleMixin
from job_queue.queue.tenants_stats import TenantsStatsMixin

class TenantsMixin(TenantsLimitsMixin, TenantsLifecycleMixin, TenantsStatsMixin):
    """Aggregated TenantsMixin (split for maintainability)."""

    pass
