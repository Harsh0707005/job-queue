"""Compatibility re-export: LeaseMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.lease_claim import LeaseClaimMixin
from job_queue.queue.lease_ops import LeaseOpsMixin
from job_queue.queue.lease_deadlines import LeaseDeadlinesMixin
from job_queue.queue.lease_preempt import LeasePreemptMixin
from job_queue.queue.lease_quarantine import LeaseQuarantineMixin


class LeaseMixin(
    LeaseClaimMixin,
    LeaseOpsMixin,
    LeaseDeadlinesMixin,
    LeasePreemptMixin,
    LeaseQuarantineMixin,
):
    """Aggregated LeaseMixin (split for maintainability)."""

    pass
