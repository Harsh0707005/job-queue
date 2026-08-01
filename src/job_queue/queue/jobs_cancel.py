"""Compatibility re-export: JobsCancelMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.jobs_cancel_one import JobsCancelOneMixin
from job_queue.queue.jobs_cancel_bulk import JobsCancelBulkMixin


class JobsCancelMixin(JobsCancelOneMixin, JobsCancelBulkMixin):
    """Aggregated JobsCancelMixin (split for maintainability)."""

    pass
