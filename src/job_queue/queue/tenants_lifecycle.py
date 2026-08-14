"""Compatibility re-export: TenantsLifecycleMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.tenants_pause import TenantsPauseMixin
from job_queue.queue.tenants_drain import TenantsDrainMixin
from job_queue.queue.tenants_offboard import TenantsOffboardMixin
from job_queue.queue.tenants_merge import TenantsMergeMixin
from job_queue.queue.tenants_export import TenantsExportMixin
from job_queue.queue.tenants_restore import TenantsRestoreMixin


class TenantsLifecycleMixin(
    TenantsPauseMixin,
    TenantsDrainMixin,
    TenantsOffboardMixin,
    TenantsMergeMixin,
    TenantsExportMixin,
    TenantsRestoreMixin,
):
    """Aggregated TenantsLifecycleMixin (split for maintainability)."""

    pass
