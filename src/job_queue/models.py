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
    available_at: datetime
    dedupe_key: str | None
    schedule_id: UUID | None
    depends_on: tuple[UUID, ...]
    leased_by: str | None
    lease_token: UUID | None
    lease_expires_at: datetime | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
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
