"""Compatibility re-export: LeaseCompleteMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.lease_ack import LeaseAckMixin
from job_queue.queue.lease_nack import LeaseNackMixin
from job_queue.queue.lease_checkpoint import LeaseCheckpointMixin


class LeaseCompleteMixin(LeaseAckMixin, LeaseNackMixin, LeaseCheckpointMixin):
    """Aggregated LeaseCompleteMixin (split for maintainability)."""

    pass
