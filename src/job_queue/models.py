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
    AWAITING_APPROVAL = "awaiting_approval"
    AWAITING_CHILDREN = "awaiting_children"
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

    ``lease_tenant_id`` is the tenant frozen at claim time. Lease ops accept
    either the current ``tenant_id`` or this value so mid-flight
    ``merge_tenant`` does not break workers, while unrelated tenants are
    still rejected even with a valid token.

    ``checkpoint`` is durable progress JSON written by the lease holder.
    It survives reclaim / nack requeue so a new worker can resume. Cleared
    only when the job is cancelled (or explicitly overwritten).

    ``deadline_at`` is an absolute wall-clock deadline. Combined with
    ``estimated_duration_seconds`` (and learned EWMAs) the queue computes
    *slack* and may preempt a less-urgent leased job so this one can start.
    ``preemptible`` controls whether this job may be the victim; checkpoint
    is kept on preemption and the fencing token is cleared so it cannot
    finish twice.

    ``awaiting_approval`` parks a job without a lease (no concurrency slot).
    Workers call ``request_approval`` under a live lease; ``approve`` /
    ``reject`` are tenant-scoped and idempotent. Unresolved requests past
    ``approval_deadline_at`` auto-reject. Checkpoint is kept so approve
    resumes work.

    ``awaiting_children`` parks a parent after ``spawn_children`` while its
    subtasks run. The lease is cleared (no concurrency slot); when every
    child completes the parent becomes ``pending`` again (checkpoint kept).
    If any child fails, is cancelled, or times out, the parent is terminalized
    with an error instead of waiting forever.

    ``available_at`` is the earliest time a pending job may be claimed.
    ``dedupe_key`` identifies a logical job for enqueue-time deduplication.
    ``depends_on`` lists parent job ids that must be ``completed`` before this
    job is claimable. If a parent ends in failure, dependents are cancelled.
    ``parent_job_id`` links a spawned subtask back to its parent job.
    ``waiting_on`` lists child ids a parked parent is joining on.
    ``labels`` is a sorted set of free-form string tags for list / bulk cancel.
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
    parent_job_id: UUID | None
    waiting_on: tuple[UUID, ...]
    labels: tuple[str, ...]
    leased_by: str | None
    lease_token: UUID | None
    lease_tenant_id: str | None
    lease_expires_at: datetime | None
    lease_started_at: datetime | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    result: dict[str, Any] | list[Any] | str | int | float | bool | None
    checkpoint: dict[str, Any] | list[Any] | str | int | float | bool | None
    checkpointed_at: datetime | None
    deadline_at: datetime | None
    estimated_duration_seconds: int | None
    preemptible: bool
    preempt_count: int
    last_preempted_at: datetime | None
    approval_requested_at: datetime | None
    approval_deadline_at: datetime | None
    approval_message: str | None
    approval_outcome: str | None
    approval_resolved_at: datetime | None
    await_children_started_at: datetime | None
    await_children_deadline_at: datetime | None
    await_children_outcome: str | None
    await_children_resolved_at: datetime | None
    last_error: str | None

    @property
    def is_leased(self) -> bool:
        return (
            self.status == JobStatus.LEASED
            and self.leased_by is not None
            and self.lease_token is not None
        )

    @property
    def is_awaiting_approval(self) -> bool:
        return self.status == JobStatus.AWAITING_APPROVAL

    @property
    def is_awaiting_children(self) -> bool:
        return self.status == JobStatus.AWAITING_CHILDREN


@dataclass(frozen=True, slots=True)
class SpawnChildrenResult:
    """Result of :meth:`~job_queue.queue.JobQueue.spawn_children`."""

    parent: Job
    children: tuple[Job, ...]


@dataclass(frozen=True, slots=True)
class BulkCancelResult:
    """Result of :meth:`~job_queue.queue.JobQueue.cancel_matching`.

    ``cancelled`` is the total number cancelled across all batches.
    ``jobs`` is a sample of cancelled rows (capped) so large sweeps stay
    memory-safe; use ``cancelled`` for the full count.
    ``complete`` is True when no matching cancelable jobs remain.
    ``remaining_estimate`` is how many matching cancelable rows were still
    present after a *max_jobs* cap stopped the sweep (0 when complete).
    """

    tenant_id: str
    cancelled: int
    jobs: tuple[Job, ...]
    complete: bool = True
    remaining_estimate: int = 0
    batches: int = 1


@dataclass(frozen=True, slots=True)
class Schedule:
    """A recurring schedule that materializes jobs on a cron or fixed interval.

    Prefer ``cron_expr`` (standard 5-field or 6-field with seconds), e.g.
    ``\"0 9 * * 1-5\"`` (weekdays 09:00) or ``\"0 0 1 * *\"`` (1st of month).
    ``interval_seconds`` remains for simple fixed delays.
    ``labels`` are copied onto every job this schedule enqueues.
    """

    id: UUID
    tenant_id: str
    name: str | None
    queue_name: str
    payload: dict[str, Any]
    interval_seconds: int | None
    cron_expr: str | None
    timezone: str
    priority: int
    max_attempts: int
    visibility_timeout_seconds: int
    enabled: bool
    next_run_at: datetime
    last_enqueued_at: datetime | None
    created_at: datetime
    updated_at: datetime
    labels: tuple[str, ...] = ()


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
    enqueue_rate_per_sec: float | None
    enqueue_burst: int | None
    lease_rate_per_sec: float | None
    lease_burst: int | None
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
class JobRetentionSettings:
    """How long terminal jobs are kept before the cleaner may delete them.

    ``None`` means keep forever (no automatic purge for that status).
    ``0`` means eligible for purge immediately on the next cleaner pass.

    For *tenant* overrides (from :meth:`~job_queue.queue.JobQueue.get_tenant_job_retention`):
    ``completed_inherits`` / ``dead_inherits`` are True when that status uses
    the global default. When inherits is False and seconds is ``None``, the
    tenant explicitly keeps that status forever (does **not** follow global).

    ``metrics_retention_seconds`` controls how long ``queue_metric_buckets`` and
    ``queue_metric_snapshots`` are kept (``None`` = forever; default on new
    installs is 7 days).
    """

    completed_retention_seconds: int | None
    dead_retention_seconds: int | None
    completed_inherits: bool = False
    dead_inherits: bool = False
    metrics_retention_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class RetentionPurgeResult:
    """Result of one :meth:`~job_queue.queue.JobQueue.purge_expired_jobs` pass."""

    completed_deleted: int
    dead_deleted: int
    side_effects_deleted: int
    metric_buckets_deleted: int = 0
    metric_snapshots_deleted: int = 0

    @property
    def total_deleted(self) -> int:
        return self.completed_deleted + self.dead_deleted

    @property
    def metrics_deleted(self) -> int:
        return self.metric_buckets_deleted + self.metric_snapshots_deleted


@dataclass(frozen=True, slots=True)
class DrainResult:
    """Result of a graceful worker drain."""

    drained: bool
    timed_out: bool
    in_flight_at_start: int
    released: int
    wait_seconds: float


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


@dataclass(frozen=True, slots=True)
class TenantMergeResult:
    """Result of :meth:`~job_queue.queue.JobQueue.merge_tenant` (source → target)."""

    source_tenant_id: str
    target_tenant_id: str
    jobs_moved: int
    jobs_leased_moved: int
    schedules_moved: int
    webhook_endpoints_moved: int
    webhook_deliveries_moved: int
    dedupe_keys_cleared: int
    schedule_names_renamed: int
    source_empty: bool


@dataclass(frozen=True, slots=True)
class PreemptResult:
    """One preemption: urgent pending gained a slot by parking a leased job."""

    tenant_id: str
    queue_name: str
    urgent_job_id: UUID
    victim_job_id: UUID
    urgent_slack_seconds: float
    victim_slack_seconds: float


@dataclass(frozen=True, slots=True)
class JobAuditEvent:
    """Immutable job history event (append-only audit stream)."""

    event_id: UUID
    tenant_id: str
    job_id: UUID
    seq: int
    event_type: str
    from_status: str | None
    to_status: str | None
    actor: str | None
    detail: dict[str, Any]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class JobSideEffect:
    """Durable once-only side-effect marker for a job.

    Two-phase: ``started`` (intent + stable idempotency key, before external
    call) then ``completed`` (after success). Reclaim of ``started`` must
    re-invoke the external system with the same ``idempotency_key``.
    """

    job_id: UUID
    effect_key: str
    tenant_id: str
    status: str
    idempotency_key: str
    result: dict[str, Any] | list[Any] | str | int | float | bool | None
    started_by: str | None
    started_at: datetime | None
    completed_by: str | None
    lease_token: UUID | None
    created_at: datetime
    completed_at: datetime | None


@dataclass(frozen=True, slots=True)
class AuditConsistencyReport:
    """Result of replaying audit history against a job's final state."""

    job_id: UUID
    tenant_id: str
    ok: bool
    job_status: str | None
    audit_final_status: str | None
    event_count: int
    gaps: tuple[int, ...]
    error: str | None = None


class WebhookEvent(str, Enum):
    JOB_COMPLETED = "job.completed"
    JOB_FAILED = "job.failed"
    JOB_DEAD = "job.dead"
    ALERT_FIRED = "alert.fired"


@dataclass(frozen=True, slots=True)
class AlertRule:
    """Threshold rule evaluated against live metrics."""

    id: UUID
    name: str
    enabled: bool
    scope: str  # overall | tenant | worker
    tenant_id: str | None
    worker_id: str | None
    metric: str
    comparator: str  # gt | gte | lt | lte
    threshold: float
    window_seconds: int
    cooldown_seconds: int
    last_fired_at: datetime | None
    muted_until: datetime | None
    auto_pause_tenant: bool
    recovery_threshold: float | None
    recovery_evaluations: int
    healthy_streak: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class AlertEvent:
    """One recorded firing of an alert rule."""

    id: UUID
    rule_id: UUID
    rule_name: str
    metric: str
    scope: str
    tenant_id: str | None
    worker_id: str | None
    value: float
    threshold: float
    comparator: str
    window_seconds: int | None
    detail: dict[str, Any]
    fired_at: datetime
    resolved_at: datetime | None = None
    resolved_by: str | None = None
    resolution_note: str | None = None

    @property
    def status(self) -> str:
        return "resolved" if self.resolved_at is not None else "open"


@dataclass(frozen=True, slots=True)
class AlertEvaluationResult:
    """Result of one evaluate_alerts pass (fires + auto-resolves)."""

    fired: list[AlertEvent]
    resolved: list[AlertEvent]

    def __iter__(self):  # type: ignore[no-untyped-def]
        """Iterate fired events for backward-compatible ``for e in result``."""
        return iter(self.fired)

    def __len__(self) -> int:
        return len(self.fired)


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

    ``signing_secret`` is a snapshot of the endpoint secret taken when the
    delivery was enqueued. Agents must HMAC with this value (not the live
    endpoint secret) so in-flight retries still verify after rotation.
    """

    id: UUID
    endpoint_id: UUID
    tenant_id: str
    job_id: UUID | None
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
    signing_secret: str | None = None


@dataclass(frozen=True, slots=True)
class ClaimedWebhook:
    """A delivery claimed for remote HTTP POST by a webhook agent.

    The claim is committed in Postgres (status=delivering) before this is
    returned. The agent POSTs, then calls finalize. Coordinators never POST.

    ``secret`` is the delivery's frozen ``signing_secret`` (fallback: current
    endpoint secret for legacy rows without a snapshot).
    """

    delivery: WebhookDelivery
    url: str | None
    secret: str | None
    endpoint_enabled: bool


@dataclass(frozen=True, slots=True)
class JobProgressEvent:
    """Live job progress event (SSE / LISTEN payload).

    Emitted on status transitions and checkpoints so clients need not poll
    ``GET /v1/jobs/{id}``. Multi-tenant: filter by ``tenant_id`` / ``job_id``.
    """

    event: str
    tenant_id: str
    job_id: UUID
    status: str
    from_status: str | None
    attempts: int
    labels: tuple[str, ...]
    checkpoint: Any
    result: Any
    last_error: str | None
    actor: str | None
    ts: datetime
    snapshot: bool = False  # True for initial snapshot on subscribe
