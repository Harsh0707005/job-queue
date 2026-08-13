"""Compatibility re-export: JobsMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.jobs_enqueue import JobsEnqueueMixin
from job_queue.queue.jobs_cancel import JobsCancelMixin
from job_queue.queue.jobs_query import JobsQueryMixin
from job_queue.queue.jobs_expire import JobsExpireMixin
from job_queue.queue.jobs_freeze import JobsFreezeMixin


class JobsMixin(
    JobsEnqueueMixin,
    JobsCancelMixin,
    JobsQueryMixin,
    JobsExpireMixin,
    JobsFreezeMixin,
):
    """Aggregated JobsMixin (split for maintainability)."""

    pass
