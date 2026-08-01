"""Compatibility re-export: AuditMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.audit_write import AuditWriteMixin
from job_queue.queue.audit_read import AuditReadMixin
from job_queue.queue.audit_rebuild import AuditRebuildMixin


class AuditMixin(AuditWriteMixin, AuditReadMixin, AuditRebuildMixin):
    """Aggregated AuditMixin (split for maintainability)."""

    pass
