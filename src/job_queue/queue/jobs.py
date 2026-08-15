"""Compatibility re-export: JobsMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.jobs_enqueue import JobsEnqueueMixin
from job_queue.queue.jobs_cancel import JobsCancelMixin
from job_queue.queue.jobs_query import JobsQueryMixin
from job_queue.queue.jobs_expire import JobsExpireMixin
from job_queue.queue.jobs_freeze import JobsFreezeMixin
from job_queue.queue.jobs_freeze_bulk import JobsFreezeBulkMixin
from job_queue.queue.jobs_priority import JobsPriorityMixin
from job_queue.queue.jobs_clone import JobsCloneMixin
from job_queue.queue.jobs_queue_move import JobsQueueMoveMixin
from job_queue.queue.jobs_tenant_move import JobsTenantMoveMixin


class JobsMixin(
    JobsEnqueueMixin,
    JobsCancelMixin,
    JobsQueryMixin,
    JobsExpireMixin,
    JobsFreezeMixin,
    JobsFreezeBulkMixin,
    JobsPriorityMixin,
    JobsCloneMixin,
    JobsQueueMoveMixin,
    JobsTenantMoveMixin,
):
    """Aggregated JobsMixin (split for maintainability)."""

    pass
