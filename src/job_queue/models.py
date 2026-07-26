"""Job domain models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID


class JobStatus(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class Job:
    """A unit of work leased by a worker.

    ``lease_token`` is a fencing token minted on each successful ``lease()``.
    Heartbeat / ack / nack must present the same token; after a reclaim the
    previous token is invalid so a zombie worker cannot complete the job.

    ``available_at`` is the earliest time a pending job may be claimed.
    ``dedupe_key`` identifies a logical job for enqueue-time deduplication.
    ``depends_on`` lists parent job ids that must be ``completed`` before this
    job is claimable. If a parent ends in failure, dependents are cancelled.
    """

    id: UUID
    tenant_id: str
    queue_name: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    attempts: int
    max_attempts: int
    visibility_timeout_seconds: int
    max_runtime_seconds: int
    retry_backoff_base_seconds: int
    retry_backoff_max_seconds: int
    available_at: datetime
    dedupe_key: str | None
    schedule_id: UUID | None
    depends_on: tuple[UUID, ...]
    leased_by: str | None
    lease_token: UUID | None
    lease_expires_at: datetime | None
    lease_started_at: datetime | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    result: dict[str, Any] | list[Any] | str | int | float | bool | None
    last_error: str | None

    @property
    def is_leased(self) -> bool:
        return (
            self.status == JobStatus.LEASED
            and self.leased_by is not None
            and self.lease_token is not None
        )


@dataclass(frozen=True, slots=True)
class Schedule:
    """A recurring schedule that enqueues jobs every ``interval_seconds``."""

    id: UUID
    tenant_id: str
    name: str | None
    queue_name: str
    payload: dict[str, Any]
    interval_seconds: int
    priority: int
    max_attempts: int
    visibility_timeout_seconds: int
    enabled: bool
    next_run_at: datetime
    last_enqueued_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ScheduleFire:
    """Result of one schedule tick: the schedule state and the enqueued job."""

    schedule: Schedule
    job: Job


@dataclass(frozen=True, slots=True)
class TenantStats:
    """Per-tenant queue metrics and control flags."""

    tenant_id: str
    paused: bool
    max_concurrent_leases: int | None
    max_pending_jobs: int | None
    pending: int
    leased: int
    completed: int
    failed: int
    dead: int
    cancelled: int
    total: int
    # Ready to lease right now (pending, due, deps met); 0 when paused.
    claimable: int


@dataclass(frozen=True, slots=True)
class TenantOffboardResult:
    """Result of :meth:`~job_queue.queue.JobQueue.offboard_tenant`."""

    tenant_id: str
    pending_cancelled: int
    schedules_deleted: int
    webhooks_deleted: int
    jobs_purged: int
    leased_remaining: int
    # Claimed (status=delivering) webhook rows left for agents to finalize.
    webhooks_delivering_remaining: int
    offboarding: bool
    empty: bool



class WebhookEvent(str, Enum):
    JOB_COMPLETED = "job.completed"
    JOB_FAILED = "job.failed"
    JOB_DEAD = "job.dead"


@dataclass(frozen=True, slots=True)
class WebhookEndpoint:
    """Registered HTTP webhook destination for a tenant."""

    id: UUID
    tenant_id: str
    url: str
    secret: str | None
    events: tuple[str, ...]
    enabled: bool
    max_attempts: int
    backoff_base_seconds: int
    backoff_max_seconds: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class WebhookDelivery:
    """Durable outbox row for one webhook POST attempt lifecycle.

    Status lifecycle: pending → delivering → succeeded | pending (retry) | dead.
    Claim (delivering) is committed *before* HTTP so a crash after a successful
    POST cannot leave the row as pending and cause an automatic resend.
    """

    id: UUID
    endpoint_id: UUID
    tenant_id: str
    job_id: UUID
    event: str
    payload: dict[str, Any]
    status: str
    attempts: int
    max_attempts: int
    backoff_base_seconds: int
    backoff_max_seconds: int
    next_attempt_at: datetime
    delivering_until: datetime | None
    last_error: str | None
    last_status_code: int | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None


@dataclass(frozen=True, slots=True)
class ClaimedWebhook:
    """A delivery claimed for remote HTTP POST by a webhook agent.

    The claim is committed in Postgres (status=delivering) before this is
    returned. The agent POSTs, then calls finalize. Coordinators never POST.
    """

    delivery: WebhookDelivery
    url: str | None
    secret: str | None
    endpoint_enabled: bool
