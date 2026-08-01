"""Compatibility re-export: JobsEnqueueMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.jobs_enqueue_core import JobsEnqueueCoreMixin
from job_queue.queue.jobs_enqueue_helpers import JobsEnqueueHelpersMixin


class JobsEnqueueMixin(JobsEnqueueCoreMixin, JobsEnqueueHelpersMixin):
    """Aggregated JobsEnqueueMixin (split for maintainability)."""

    pass
