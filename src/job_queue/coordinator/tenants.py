"""Compatibility re-export: TenantsApiMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.coordinator.tenants_api_limits import TenantsApiLimitsMixin
from job_queue.coordinator.tenants_api_lifecycle import TenantsApiLifecycleMixin


class TenantsApiMixin(TenantsApiLimitsMixin, TenantsApiLifecycleMixin):
    """Aggregated TenantsApiMixin (split for maintainability)."""

    pass
