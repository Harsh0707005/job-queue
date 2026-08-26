"""JobQueue: domain mixins composed into one class."""

from __future__ import annotations

from job_queue.queue.base import BaseMixin
from job_queue.queue.payload_crypto import PayloadCryptoMixin
from job_queue.queue.payload_blobs import PayloadBlobsMixin
from job_queue.queue.payload_keys import PayloadKeysMixin
from job_queue.queue.jobs import JobsMixin
from job_queue.queue.lease import LeaseMixin
from job_queue.queue.tenants import TenantsMixin
from job_queue.queue.webhooks_ops import WebhooksOpsMixin
from job_queue.queue.inbound_triggers import InboundTriggersMixin
from job_queue.queue.schedules import SchedulesMixin
from job_queue.queue.audit import AuditMixin
from job_queue.queue.side_effects import SideEffectsMixin
from job_queue.queue.approval_spawn import ApprovalSpawnMixin
from job_queue.queue.dead_letters import DeadLettersMixin
from job_queue.queue.metrics import MetricsMixin
from job_queue.queue.usage import UsageMixin
from job_queue.queue.alerts import AlertsMixin
from job_queue.queue.workflows import WorkflowsMixin
from job_queue.queue.workflows_signals import WorkflowSignalsMixin
from job_queue.queue.partitions import PartitionsMixin
from job_queue.queue.coordinator_leader import CoordinatorLeaderMixin
from job_queue.queue.misc import MiscMixin


class JobQueue(
    BaseMixin,
    PayloadCryptoMixin,
    PayloadBlobsMixin,
    PayloadKeysMixin,
    JobsMixin,
    LeaseMixin,
    TenantsMixin,
    WebhooksOpsMixin,
    InboundTriggersMixin,
    SchedulesMixin,
    AuditMixin,
    SideEffectsMixin,
    ApprovalSpawnMixin,
    DeadLettersMixin,
    MetricsMixin,
    UsageMixin,
    AlertsMixin,
    WorkflowsMixin,
    WorkflowSignalsMixin,
    PartitionsMixin,
    CoordinatorLeaderMixin,
    MiscMixin,
):
    """PostgreSQL-backed multi-tenant job queue (split across domain mixins)."""

    pass
