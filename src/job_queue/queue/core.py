"""JobQueue: domain mixins composed into one class."""

from __future__ import annotations

from job_queue.queue.base import BaseMixin
from job_queue.queue.jobs import JobsMixin
from job_queue.queue.lease import LeaseMixin
from job_queue.queue.tenants import TenantsMixin
from job_queue.queue.webhooks_ops import WebhooksOpsMixin
from job_queue.queue.schedules import SchedulesMixin
from job_queue.queue.audit import AuditMixin
from job_queue.queue.side_effects import SideEffectsMixin
from job_queue.queue.approval_spawn import ApprovalSpawnMixin
from job_queue.queue.dead_letters import DeadLettersMixin
from job_queue.queue.metrics import MetricsMixin
from job_queue.queue.alerts import AlertsMixin
from job_queue.queue.misc import MiscMixin


class JobQueue(
    BaseMixin,
    JobsMixin,
    LeaseMixin,
    TenantsMixin,
    WebhooksOpsMixin,
    SchedulesMixin,
    AuditMixin,
    SideEffectsMixin,
    ApprovalSpawnMixin,
    DeadLettersMixin,
    MetricsMixin,
    AlertsMixin,
    MiscMixin,
):
    """PostgreSQL-backed multi-tenant job queue (split across domain mixins)."""

    pass
