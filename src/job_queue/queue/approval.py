"""Compatibility re-export: ApprovalMixin composed from sub-mixins."""
from __future__ import annotations

from job_queue.queue.approval_api import ApprovalApiMixin
from job_queue.queue.approval_timeouts import ApprovalTimeoutsMixin

class ApprovalMixin(ApprovalApiMixin, ApprovalTimeoutsMixin):
    """Aggregated ApprovalMixin (split for maintainability)."""

    pass
