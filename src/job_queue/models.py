"""Job domain models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from enum import Enum
from typing import Any
from uuid import UUID


class JobStatus(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    AWAITING_APPROVAL = "awaiting_approval"
    AWAITING_CHILDREN = "awaiting_children"
    AWAITING_SKILLS = "awaiting_skills"
    AWAITING_CAPACITY = "awaiting_capacity"
    QUARANTINED = "quarantined"
    FROZEN = "frozen"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"
    CANCELLED = "cancelled"


class WorkflowRunStatus(str, Enum):
    RUNNING = "running"
    COMPENSATING = "compensating"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkflowStepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    COMPENSATED = "compensated"
    AWAITING_SIGNAL = "awaiting_signal"


class WorkflowStepDirection(str, Enum):
    FORWARD = "forward"
    COMPENSATE = "compensate"


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
    ``cloned_from_job_id`` points a re-run at the job it was copied from.
    ``coalesce_key`` groups the same logical work so extras can later be
    folded into one survivor. Unlike ``dedupe_key`` it is not unique.
    ``waiting_on`` lists child ids a parked parent is joining on.
    ``labels`` is a sorted set of free-form string tags for list / bulk cancel.
    ``required_skills`` is the set of worker capabilities this job needs
    (AND). Empty means any worker may claim it. A worker may lease the job
    only when ``required_skills`` is a subset of the worker's advertised
    ``skills``. Unskilled jobs stay pinned to the worker's owned partitions;
    skilled jobs may be claimed by any capable registered worker so they
    cannot stall on a shard whose owner lacks the hardware. Fairness and
    concurrency still apply.

    ``resource_cost`` is how much of each advertised worker resource this
    job occupies while leased (cpu, memory, ...). Empty means free. A
    worker leases the job only when every listed cost fits in remaining
    capacity after live (unexpired) holds. If no *active* worker advertises
    enough of every key, the job is parked as ``awaiting_capacity`` until
    one does (or a deadline / TTL ends it). Lowering worker capacity live
    never drops inflight work.

    ``expires_at`` is an optional hard TTL. If the job is still unfinished
    (pending, leased, or parked) after this instant it is terminated
    according to ``expiry_action``: ``dead`` (dead-letter) or ``cancel``.
    Per-job ``ttl_seconds`` / ``expires_at`` override a tenant default.
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
    partition_id: int = 0
    required_skills: tuple[str, ...] = ()
    resource_cost: tuple[tuple[str, int], ...] = ()
    expires_at: datetime | None = None
    expiry_action: str = "dead"
    failed_workers: tuple[str, ...] = ()
    max_worker_hops: int | None = None
    quarantine_action: str = "dead"
    # Opt this job into last-success-worker preference (queue opt-in also works).
    prefer_last_worker: bool = False
    payload_kind: str = ""
    cloned_from_job_id: UUID | None = None
    coalesce_key: str | None = None
    tick_iteration: int | None = None

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

    @property
    def is_awaiting_skills(self) -> bool:
        return self.status == JobStatus.AWAITING_SKILLS

    @property
    def is_awaiting_capacity(self) -> bool:
        return self.status == JobStatus.AWAITING_CAPACITY

    @property
    def is_quarantined(self) -> bool:
        return self.status == JobStatus.QUARANTINED

    @property
    def is_frozen(self) -> bool:
        return self.status == JobStatus.FROZEN


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
class BulkFreezeResult:
    """Result of :meth:`~job_queue.queue.JobQueue.freeze_matching` / unfreeze.

    ``count`` is the number of rows changed across all batches.
    ``action`` is ``freeze`` or ``unfreeze``. ``jobs`` is a sample (capped).
    ``complete`` is True when no matching target rows remain.
    """

    tenant_id: str
    action: str
    count: int
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
    ``required_skills`` are copied onto every job this schedule enqueues.
    ``misfire_policy`` is what happens after a pause or coordinator gap:
    ``skip`` records missed ticks and jumps to the next future fire;
    ``catch_up_one`` enqueues a single pickup then jumps (default);
    ``catch_up`` drains up to ``catch_up_max`` missed ticks one at a time
    (no overlapping runs) and skips the rest.
    ``missed_ticks`` is a running count of ticks that were not enqueued.
    ``dag_jobs`` is an optional template: when set, each tick enqueues that
    whole DAG atomically instead of a single job.
    ``workflow`` is an optional multi-step saga template. When set, each
    tick starts one workflow run instead of a job or DAG. Mutually
    exclusive with ``dag_jobs``. Step jobs carry ``schedule_id`` and
    ``tick_iteration`` so mutex, waits, and joins treat the whole run as
    one tick.
    ``mutex_group`` names a per-tenant exclusive set: at most one member
    may have a tick or DAG in flight. Missed ticks while blocked follow
    ``misfire_policy`` instead of dumping a backlog.
    ``waits_on`` lists upstream schedule ids. This schedule will not fire
    while any of those has a live tick (any tenant), a cancelled /
    dead-lettered / failed tick with no later completed job, is due but
    has not started that tick, is paused, or has a paused / inflight
    mutex-group peer. Missed ticks while waiting follow ``misfire_policy``.
    ``joins_on`` lists waiter schedule ids. This schedule fires one tick
    per generation after every waiter has completed that same iteration.
    A paused waiter or one stuck behind a mutex peer keeps the join
    blocked. Missed join ticks follow ``misfire_policy``.
    ``tick_iteration`` is the last enqueued generation; ``last_completed_iteration``
    advances only when that generation finishes successfully.
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
    required_skills: tuple[str, ...] = ()
    misfire_policy: str = "catch_up_one"
    catch_up_max: int = 8
    missed_ticks: int = 0
    last_misfire_at: datetime | None = None
    catch_up_remaining: int | None = None
    dag_jobs: tuple[dict[str, Any], ...] = ()
    workflow: dict[str, Any] | None = None
    mutex_group: str | None = None
    waits_on: tuple[UUID, ...] = ()
    joins_on: tuple[UUID, ...] = ()
    tick_iteration: int = 0
    last_completed_iteration: int | None = None


@dataclass(frozen=True, slots=True)
class ScheduleBlackout:
    """A maintenance window that stops schedule fires without pausing the tenant.

    ``schedule_id`` is None for a tenant-wide window. ``kind`` is ``once``
    (absolute ``starts_at``/``ends_at``) or ``daily`` (quiet hours in
    ``timezone``). Overlapping windows are a union. Ticks that land inside
    a window stay overdue so ``misfire_policy`` runs when the window ends.
    """

    id: UUID
    tenant_id: str
    schedule_id: UUID | None
    kind: str
    starts_at: datetime | None
    ends_at: datetime | None
    daily_start: time | None
    daily_end: time | None
    timezone: str
    reason: str | None
    created_at: datetime
    updated_at: datetime
    active: bool = False


@dataclass(frozen=True, slots=True)
class DagEnqueueResult:
    """Result of :meth:`~job_queue.queue.JobQueue.enqueue_dag`.

    *created* is False when *batch_key* matched a previously committed graph
    (idempotent retry after a crash or duplicate client submit).
    *refs* aligns with *jobs* (submission order).
    """

    batch_key: str
    batch_id: UUID
    jobs: tuple[Job, ...]
    refs: tuple[str, ...]
    created: bool

    @property
    def by_ref(self) -> dict[str, Job]:
        return {ref: job for ref, job in zip(self.refs, self.jobs, strict=True)}


@dataclass(frozen=True, slots=True)
class ScheduleFire:
    """Result of one schedule tick: the schedule state and the enqueued job(s).

    ``job`` is the first node (compat). ``jobs`` / ``refs`` are the full
    graph when the schedule has a DAG template; otherwise ``jobs`` is
    ``(job,)``.
    """

    schedule: Schedule
    job: Job
    jobs: tuple[Job, ...] = ()
    refs: tuple[str, ...] = ()
    batch_id: UUID | None = None
    batch_key: str | None = None
    workflow_run_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class ScheduleGenerationOp:
    """Cancel or replay one fan-out generation as a closed set."""

    schedule: Schedule
    iteration: int
    action: str
    members: tuple[UUID, ...]
    cancelled_job_ids: tuple[UUID, ...]
    job: Job | None = None
    jobs: tuple[Job, ...] = ()


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
    # Ready to lease right now (pending, due, deps met); 0 when paused
    # or when the tenant failure breaker is open.
    claimable: int
    awaiting_skills: int = 0
    awaiting_capacity: int = 0
    frozen: int = 0
    breaker_open: bool = False
    # Fair-lease weight (1..100). Missing / unset is 1 (equal share).
    lease_weight: int = 1
    # Queues that reject new intake while existing jobs still finish.
    draining_queues: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TenantBreaker:
    """Per-tenant failure circuit breaker state.

    Operator-set *failure_threshold* / *window_seconds* / *cooldown_seconds*.
    *failures* and *opened_at* are server-written only. NULL threshold means
    the breaker is disabled. *cooldown_seconds* NULL means manual reset only.
    """

    tenant_id: str
    failure_threshold: int | None
    window_seconds: int | None
    cooldown_seconds: int | None
    failures: int
    window_start: datetime | None
    opened_at: datetime | None
    open: bool
    reset_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class TenantSpendCap:
    """Current spend-window quota for one tenant.

    Limits are operator-set. *enqueues* / *runtime_ms* are server-side
    counters for the active window and cannot be written by job payloads.
    """

    tenant_id: str
    window_seconds: int | None
    window_start: datetime | None
    window_end: datetime | None
    max_enqueues: int | None
    enqueues: int
    max_runtime_ms: int | None
    runtime_ms: int
    enqueue_remaining: int | None
    runtime_remaining_ms: int | None
    exhausted: bool
    exhausted_kinds: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TenantUsage:
    """Per-tenant billable usage for a time window (all counts are additive)."""

    tenant_id: str
    period_start: datetime | None
    period_end: datetime | None
    enqueues: int = 0
    leases: int = 0
    acks: int = 0
    nacks: int = 0
    fails: int = 0
    dead_letters: int = 0
    cancels: int = 0
    releases: int = 0
    preemptions: int = 0
    schedule_fires: int = 0
    webhook_successes: int = 0
    webhook_failures: int = 0
    spawn_children: int = 0
    runtime_ms: int = 0
    wait_ms: int = 0

    @property
    def runtime_seconds(self) -> float:
        return self.runtime_ms / 1000.0

    @property
    def webhook_deliveries(self) -> int:
        return self.webhook_successes + self.webhook_failures


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
class TenantSplitBlocker:
    """Why a tenant split cannot move a job without breaking the graph."""

    job_id: UUID
    reason: str
    related_job_id: UUID | None = None
    schedule_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class TenantSplitPlan:
    """Dry-run of :meth:`~job_queue.queue.JobQueue.plan_tenant_split`."""

    source_tenant_id: str
    target_tenant_id: str
    queue_name: str | None
    labels: tuple[str, ...]
    required_skills: tuple[str, ...]
    ok: bool
    move_job_ids: tuple[UUID, ...]
    stay_leased_ids: tuple[UUID, ...]
    blockers: tuple[TenantSplitBlocker, ...]
    move_schedule_ids: tuple[UUID, ...] = ()


@dataclass(frozen=True, slots=True)
class ReplayGraphBlocker:
    """Why a graph-closed dead-letter replay cannot proceed."""

    job_id: UUID
    reason: str
    related_job_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class ReplayGraphPlan:
    """Dry-run of :meth:`~job_queue.queue.JobQueue.plan_replay_dead_letter_graph`."""

    tenant_id: str
    ok: bool
    replay_job_ids: tuple[UUID, ...]
    stay_job_ids: tuple[UUID, ...]
    rejoin_job_ids: tuple[UUID, ...]
    blockers: tuple[ReplayGraphBlocker, ...]


@dataclass(frozen=True, slots=True)
class ReplayGraphResult:
    """Result of :meth:`~job_queue.queue.JobQueue.replay_dead_letter_graph`."""

    tenant_id: str
    jobs_replayed: int
    job_ids: tuple[UUID, ...]
    rejoin_job_ids: tuple[UUID, ...]


@dataclass(frozen=True, slots=True)
class CoalesceResult:
    """Result of :meth:`~job_queue.queue.JobQueue.coalesce_jobs`."""

    tenant_id: str
    queue_name: str
    coalesce_key: str
    survivor_job_id: UUID | None
    jobs_folded: int
    folded_job_ids: tuple[UUID, ...]


@dataclass(frozen=True, slots=True)
class TenantSplitResult:
    """Result of :meth:`~job_queue.queue.JobQueue.split_tenant`."""

    source_tenant_id: str
    target_tenant_id: str
    jobs_moved: int
    job_ids: tuple[UUID, ...]
    schedules_moved: int = 0
    schedule_ids: tuple[UUID, ...] = ()


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
    workflow_runs_moved: int = 0
    generations_resealed: int = 0


@dataclass(frozen=True, slots=True)
class TenantPayloadKey:
    """Per-tenant wrapped DEK used to seal job / schedule payloads."""

    tenant_id: str
    key_id: UUID
    kek_id: str
    status: str
    created_at: datetime
    retired_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class PayloadKeyRotation:
    """Live DEK rotation for one tenant.

    Minting a new active key never stops the fleet: old keys stay
    ``decrypt_only`` so pending / in-flight jobs keep opening. Advance
    re-seals ciphertext onto the active key in crash-safe batches and only
    zeroizes wrapped DEK material after zero remaining references.
    """

    id: UUID
    tenant_id: str
    from_key_id: UUID | None
    to_key_id: UUID
    status: str
    jobs_rewrapped: int
    schedules_rewrapped: int
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class TenantSnapshot:
    """Durable export of one tenant's queue state (ciphertext preserved)."""

    id: UUID
    source_tenant_id: str
    status: str
    counts: dict[str, int]
    created_at: datetime
    completed_at: datetime | None = None
    error: str | None = None
    format_version: int = 1


@dataclass(frozen=True, slots=True)
class TenantRestore:
    """Resumable restore of a snapshot into an empty target tenant."""

    id: UUID
    snapshot_id: UUID
    target_tenant_id: str
    source_tenant_id: str
    status: str
    kind_index: int
    applied: dict[str, int]
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None
    error: str | None = None


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
    ALERT_RESOLVED = "alert.resolved"


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
    healthy_interval_seconds: int
    healthy_streak: int
    last_healthy_at: datetime | None
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
class WorkflowRun:
    """Multi-step saga run with optional reverse compensation."""

    id: UUID
    tenant_id: str
    name: str | None
    status: str
    steps_def: list[dict[str, Any]]
    current_index: int
    fail_step_index: int | None
    fail_reason: str | None
    max_compensate_retries: int
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    schedule_id: UUID | None = None
    tick_iteration: int | None = None


@dataclass(frozen=True, slots=True)
class WorkflowStepRun:
    """One forward or compensate unit of work in a workflow run."""

    id: UUID
    run_id: UUID
    tenant_id: str
    step_index: int
    name: str
    direction: str
    status: str
    job_id: UUID | None
    result: dict[str, Any] | list[Any] | str | int | float | bool | None
    error: str | None
    idempotency_key: str
    retry_count: int
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime
    deadline_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class WorkflowSignal:
    """Named latch on a workflow run. First write wins; tenant-scoped."""

    id: UUID
    run_id: UUID
    tenant_id: str
    name: str
    payload: dict[str, Any]
    created_at: datetime


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


@dataclass(frozen=True, slots=True)
class CoordinatorLeader:
    """The coordinator that currently owns control-plane ticks.

    ``slot`` is the singleton key (``control``). ``epoch`` increments each
    time a different node steals the lease. Tick counters are how soaks
    prove only the leader ran schedules / webhooks / alerts / retention.
    """

    slot: str
    node_id: str
    lease_token: UUID
    epoch: int
    expires_at: datetime
    acquired_at: datetime
    renewed_at: datetime
    schedule_ticks: int = 0
    webhook_ticks: int = 0
    alert_ticks: int = 0
    retention_ticks: int = 0
    last_schedule_at: datetime | None = None
    last_webhook_at: datetime | None = None
    last_alert_at: datetime | None = None
    last_retention_at: datetime | None = None
    handoff_to: str | None = None
    handoff_until: datetime | None = None
