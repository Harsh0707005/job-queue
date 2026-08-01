"""Compatibility re-export: TenantsLifecycleMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.tenants_pause import TenantsPauseMixin
from job_queue.queue.tenants_offboard import TenantsOffboardMixin
from job_queue.queue.tenants_merge import TenantsMergeMixin


class TenantsLifecycleMixin(TenantsPauseMixin, TenantsOffboardMixin, TenantsMergeMixin):
    """Aggregated TenantsLifecycleMixin (split for maintainability)."""

    pass
