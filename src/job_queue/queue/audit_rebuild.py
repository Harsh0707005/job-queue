"""Compatibility re-export: AuditRebuildMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.audit_rebuild_one import AuditRebuildOneMixin
from job_queue.queue.audit_orphans import AuditOrphansMixin


class AuditRebuildMixin(AuditRebuildOneMixin, AuditOrphansMixin):
    """Aggregated AuditRebuildMixin (split for maintainability)."""

    pass
