"""Compatibility re-export: ApprovalApiMixin composed from sub-mixins."""
from __future__ import annotations

from job_queue.queue.approval_request import ApprovalRequestMixin
from job_queue.queue.approval_decide import ApprovalDecideMixin

class ApprovalApiMixin(ApprovalRequestMixin, ApprovalDecideMixin):
    """Aggregated ApprovalApiMixin (split for maintainability)."""

    pass
