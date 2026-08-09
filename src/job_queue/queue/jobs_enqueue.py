"""Compatibility re-export: JobsEnqueueMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.jobs_enqueue_core import JobsEnqueueCoreMixin
from job_queue.queue.jobs_enqueue_helpers import JobsEnqueueHelpersMixin
from job_queue.queue.jobs_enqueue_dag import JobsEnqueueDagMixin


class JobsEnqueueMixin(
    JobsEnqueueCoreMixin, JobsEnqueueHelpersMixin, JobsEnqueueDagMixin
):
    """Aggregated JobsEnqueueMixin (split for maintainability)."""

    pass
