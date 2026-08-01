"""Compatibility re-export: LeaseOpsMixin composed from sub-mixins."""
from __future__ import annotations

from job_queue.queue.lease_heartbeat import LeaseHeartbeatMixin
from job_queue.queue.lease_complete import LeaseCompleteMixin

class LeaseOpsMixin(LeaseHeartbeatMixin, LeaseCompleteMixin):
    """Aggregated LeaseOpsMixin (split for maintainability)."""

    pass
