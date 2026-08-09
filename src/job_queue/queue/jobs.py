"""Compatibility re-export: JobsMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.jobs_enqueue import JobsEnqueueMixin
from job_queue.queue.jobs_cancel import JobsCancelMixin
from job_queue.queue.jobs_query import JobsQueryMixin
from job_queue.queue.jobs_expire import JobsExpireMixin


class JobsMixin(
    JobsEnqueueMixin, JobsCancelMixin, JobsQueryMixin, JobsExpireMixin
):
    """Aggregated JobsMixin (split for maintainability)."""

    pass
