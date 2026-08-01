"""Compatibility re-export: AuditWriteMixin composed from sub-mixins."""
from __future__ import annotations

from job_queue.queue.audit_append import AuditAppendMixin
from job_queue.queue.audit_stream import AuditStreamMixin

class AuditWriteMixin(AuditAppendMixin, AuditStreamMixin):
    """Aggregated AuditWriteMixin (split for maintainability)."""

    pass
