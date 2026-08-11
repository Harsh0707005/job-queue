"""Compatibility re-export: TenantsLimitsMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.tenants_concurrency import TenantsConcurrencyMixin
from job_queue.queue.tenants_rate import TenantsRateMixin
from job_queue.queue.tenants_retention import TenantsRetentionMixin
from job_queue.queue.tenants_spend import TenantsSpendMixin
from job_queue.queue.tenants_ttl import TenantsTtlMixin
from job_queue.queue.tenants_quarantine import TenantsQuarantineMixin


class TenantsLimitsMixin(
    TenantsConcurrencyMixin,
    TenantsRateMixin,
    TenantsRetentionMixin,
    TenantsTtlMixin,
    TenantsSpendMixin,
    TenantsQuarantineMixin,
):
    """Aggregated TenantsLimitsMixin (split for maintainability)."""

    pass
