"""SQL schema for the job queue."""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    id                          UUID PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    queue_name                  TEXT NOT NULL,
    payload                     JSONB NOT NULL,
    status                      TEXT NOT NULL,
    priority                    INTEGER NOT NULL DEFAULT 0,
    attempts                    INTEGER NOT NULL DEFAULT 0,
    max_attempts                INTEGER NOT NULL DEFAULT 3,
    visibility_timeout_seconds  INTEGER NOT NULL DEFAULT 30,
    max_runtime_seconds         INTEGER NOT NULL DEFAULT 3600,
    retry_backoff_base_seconds  INTEGER NOT NULL DEFAULT 1,
    retry_backoff_max_seconds   INTEGER NOT NULL DEFAULT 300,
    available_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dedupe_key                  TEXT,
    schedule_id                 UUID,
    depends_on                  UUID[] NOT NULL DEFAULT '{}',
    leased_by                   TEXT,
    lease_token                 UUID,
    -- Tenant id frozen at claim time; used for lease-op tenant checks so a
    -- mid-flight merge_tenant can move tenant_id while workers still send the
    -- pre-merge tenant. Cleared when the lease ends.
    lease_tenant_id             TEXT,
    lease_expires_at            TIMESTAMPTZ,
    lease_started_at            TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at                TIMESTAMPTZ,
    result                      JSONB,
    -- Durable progress for long-running work. Survives lease reclaim so a
    -- new worker can resume instead of restarting. Written only by the
    -- current lease holder via checkpoint().
    checkpoint                  JSONB,
    checkpointed_at             TIMESTAMPTZ,
    -- Absolute deadline; NULL = no deadline. Used with estimated duration for
    -- urgency / preemption (slack = deadline - now - remaining_work).
    deadline_at                 TIMESTAMPTZ,
    estimated_duration_seconds  INTEGER,
    preemptible                 BOOLEAN NOT NULL DEFAULT TRUE,
    preempt_count               INTEGER NOT NULL DEFAULT 0,
    last_preempted_at           TIMESTAMPTZ,
    last_error                  TEXT,

    CONSTRAINT jobs_attempts_check CHECK (attempts >= 0),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts >= 1),
    CONSTRAINT jobs_visibility_check CHECK (visibility_timeout_seconds >= 1),
    CONSTRAINT jobs_max_runtime_check CHECK (max_runtime_seconds >= 1),
    CONSTRAINT jobs_backoff_base_check CHECK (retry_backoff_base_seconds >= 0),
    CONSTRAINT jobs_backoff_max_check CHECK (retry_backoff_max_seconds >= 0),
    CONSTRAINT jobs_estimated_duration_check CHECK (
        estimated_duration_seconds IS NULL OR estimated_duration_seconds >= 1
    ),
    CONSTRAINT jobs_preempt_count_check CHECK (preempt_count >= 0)
);

-- Status check kept replaceable so upgrades can add new statuses.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check CHECK (
    status IN (
        'pending', 'leased', 'awaiting_approval',
        'completed', 'failed', 'dead', 'cancelled'
    )
);

-- Backward-compatible column adds for older installs.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_token UUID;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_tenant_id TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dedupe_key TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS schedule_id UUID;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS depends_on UUID[];
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS result JSONB;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS retry_backoff_base_seconds INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS retry_backoff_max_seconds INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS max_runtime_seconds INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_started_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS checkpoint JSONB;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS checkpointed_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS estimated_duration_seconds INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS preemptible BOOLEAN;
UPDATE jobs SET preemptible = TRUE WHERE preemptible IS NULL;
ALTER TABLE jobs ALTER COLUMN preemptible SET DEFAULT TRUE;
ALTER TABLE jobs ALTER COLUMN preemptible SET NOT NULL;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS preempt_count INTEGER;
UPDATE jobs SET preempt_count = 0 WHERE preempt_count IS NULL;
ALTER TABLE jobs ALTER COLUMN preempt_count SET DEFAULT 0;
ALTER TABLE jobs ALTER COLUMN preempt_count SET NOT NULL;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_preempted_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS approval_requested_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS approval_deadline_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS approval_message TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS approval_outcome TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS approval_resolved_at TIMESTAMPTZ;

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_approval_outcome_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_approval_outcome_check CHECK (
    approval_outcome IS NULL
    OR approval_outcome IN ('approved', 'rejected', 'timeout')
);

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_estimated_duration_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_estimated_duration_check CHECK (
    estimated_duration_seconds IS NULL OR estimated_duration_seconds >= 1
);
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_preempt_count_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_preempt_count_check CHECK (preempt_count >= 0);

-- EWMA of observed runtimes for smarter remaining-time estimates.
CREATE TABLE IF NOT EXISTS duration_estimates (
    tenant_id       TEXT NOT NULL,
    queue_name      TEXT NOT NULL,
    kind            TEXT NOT NULL DEFAULT '',
    ewma_seconds    DOUBLE PRECISION NOT NULL,
    samples         INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, queue_name, kind)
);

UPDATE jobs
SET retry_backoff_base_seconds = 1
WHERE retry_backoff_base_seconds IS NULL;
UPDATE jobs
SET retry_backoff_max_seconds = 300
WHERE retry_backoff_max_seconds IS NULL;
UPDATE jobs
SET max_runtime_seconds = 3600
WHERE max_runtime_seconds IS NULL;
ALTER TABLE jobs ALTER COLUMN retry_backoff_base_seconds SET DEFAULT 1;
ALTER TABLE jobs ALTER COLUMN retry_backoff_max_seconds SET DEFAULT 300;
ALTER TABLE jobs ALTER COLUMN max_runtime_seconds SET DEFAULT 3600;
ALTER TABLE jobs ALTER COLUMN retry_backoff_base_seconds SET NOT NULL;
ALTER TABLE jobs ALTER COLUMN retry_backoff_max_seconds SET NOT NULL;
ALTER TABLE jobs ALTER COLUMN max_runtime_seconds SET NOT NULL;

UPDATE jobs SET available_at = created_at WHERE available_at IS NULL;
ALTER TABLE jobs ALTER COLUMN available_at SET DEFAULT NOW();
ALTER TABLE jobs ALTER COLUMN available_at SET NOT NULL;

UPDATE jobs SET depends_on = '{}' WHERE depends_on IS NULL;
ALTER TABLE jobs ALTER COLUMN depends_on SET DEFAULT '{}';
ALTER TABLE jobs ALTER COLUMN depends_on SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_available
    ON jobs (tenant_id, queue_name, priority DESC, created_at ASC)
    WHERE status IN ('pending', 'leased');

CREATE INDEX IF NOT EXISTS idx_jobs_claimable
    ON jobs (tenant_id, queue_name, available_at, priority DESC, created_at ASC)
    WHERE status = 'pending';

-- Fair multi-tenant scan: priority lanes across tenants for a queue.
CREATE INDEX IF NOT EXISTS idx_jobs_fair_priority
    ON jobs (queue_name, priority DESC, available_at ASC, created_at ASC)
    WHERE status = 'pending';

-- Fast ready-set scan for set-based fair lease (Go lease path / large backlogs).
CREATE INDEX IF NOT EXISTS idx_jobs_fair_ready_v2
    ON jobs (queue_name, priority DESC, created_at ASC)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_jobs_fair_reclaim_v2
    ON jobs (queue_name, priority DESC, created_at ASC)
    WHERE status = 'leased';

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_queue_pending
    ON jobs (tenant_id, queue_name, priority DESC, created_at ASC)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry
    ON jobs (lease_expires_at)
    WHERE status = 'leased';

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_status
    ON jobs (tenant_id, status);

CREATE INDEX IF NOT EXISTS idx_jobs_deadline_pending
    ON jobs (tenant_id, queue_name, deadline_at ASC)
    WHERE status = 'pending' AND deadline_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_leased
    ON jobs (tenant_id)
    WHERE status = 'leased';

-- At most one active (pending/leased/awaiting_approval) job per dedupe key.
DROP INDEX IF EXISTS idx_jobs_dedupe_active;
CREATE UNIQUE INDEX idx_jobs_dedupe_active
    ON jobs (tenant_id, queue_name, dedupe_key)
    WHERE dedupe_key IS NOT NULL
      AND status IN ('pending', 'leased', 'awaiting_approval');

-- At most one in-flight job per recurring schedule (no overlapping runs).
DROP INDEX IF EXISTS idx_jobs_schedule_active;
CREATE UNIQUE INDEX idx_jobs_schedule_active
    ON jobs (schedule_id)
    WHERE schedule_id IS NOT NULL
      AND status IN ('pending', 'leased', 'awaiting_approval');

CREATE INDEX IF NOT EXISTS idx_jobs_approval_deadline
    ON jobs (approval_deadline_at ASC)
    WHERE status = 'awaiting_approval';

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_awaiting_approval
    ON jobs (tenant_id, approval_deadline_at ASC)
    WHERE status = 'awaiting_approval';

CREATE INDEX IF NOT EXISTS idx_jobs_schedule_id
    ON jobs (schedule_id)
    WHERE schedule_id IS NOT NULL;

-- Find children of a parent quickly (contains / overlap queries).
CREATE INDEX IF NOT EXISTS idx_jobs_depends_on
    ON jobs USING GIN (depends_on);

-- Retention cleaner: find old terminal jobs quickly.
CREATE INDEX IF NOT EXISTS idx_jobs_completed_retention
    ON jobs (COALESCE(completed_at, updated_at) ASC)
    WHERE status = 'completed';

CREATE INDEX IF NOT EXISTS idx_jobs_dead_retention
    ON jobs (COALESCE(completed_at, updated_at) ASC)
    WHERE status = 'dead';

-- Global retention defaults (single row). NULL = keep forever.
CREATE TABLE IF NOT EXISTS queue_settings (
    id                              INTEGER PRIMARY KEY DEFAULT 1
                                    CHECK (id = 1),
    completed_retention_seconds     INTEGER,
    dead_retention_seconds          INTEGER,
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT queue_settings_completed_ret_check CHECK (
        completed_retention_seconds IS NULL
        OR completed_retention_seconds >= 0
    ),
    CONSTRAINT queue_settings_dead_ret_check CHECK (
        dead_retention_seconds IS NULL
        OR dead_retention_seconds >= 0
    )
);

INSERT INTO queue_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- Per-tenant concurrency limits + pause flag (NULL row / missing = unlimited, not paused).
-- Optional token-bucket rate limits (NULL rate = unlimited):
--   enqueue_rate_per_sec / lease_rate_per_sec refill tokens over time;
--   burst is the max bucket size. Tokens state is updated under the same
--   per-tenant advisory lock used by enqueue / lease.
CREATE TABLE IF NOT EXISTS tenant_settings (
    tenant_id              TEXT PRIMARY KEY,
    max_concurrent_leases  INTEGER,
    max_pending_jobs       INTEGER,
    paused                 BOOLEAN NOT NULL DEFAULT FALSE,
    offboarding            BOOLEAN NOT NULL DEFAULT FALSE,
    enqueue_rate_per_sec   DOUBLE PRECISION,
    enqueue_burst          INTEGER,
    enqueue_tokens         DOUBLE PRECISION,
    enqueue_tokens_at      TIMESTAMPTZ,
    lease_rate_per_sec     DOUBLE PRECISION,
    lease_burst            INTEGER,
    lease_tokens           DOUBLE PRECISION,
    lease_tokens_at        TIMESTAMPTZ,
    -- Per-tenant retention override (NULL = use global queue_settings).
    completed_retention_seconds INTEGER,
    dead_retention_seconds      INTEGER,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT tenant_settings_max_concurrent_check CHECK (
        max_concurrent_leases IS NULL OR max_concurrent_leases >= 1
    ),
    CONSTRAINT tenant_settings_max_pending_check CHECK (
        max_pending_jobs IS NULL OR max_pending_jobs >= 1
    ),
    CONSTRAINT tenant_settings_enqueue_rate_check CHECK (
        enqueue_rate_per_sec IS NULL OR enqueue_rate_per_sec > 0
    ),
    CONSTRAINT tenant_settings_enqueue_burst_check CHECK (
        enqueue_burst IS NULL OR enqueue_burst >= 1
    ),
    CONSTRAINT tenant_settings_lease_rate_check CHECK (
        lease_rate_per_sec IS NULL OR lease_rate_per_sec > 0
    ),
    CONSTRAINT tenant_settings_lease_burst_check CHECK (
        lease_burst IS NULL OR lease_burst >= 1
    ),
    CONSTRAINT tenant_settings_completed_ret_check CHECK (
        completed_retention_seconds IS NULL
        OR completed_retention_seconds >= 0
    ),
    CONSTRAINT tenant_settings_dead_ret_check CHECK (
        dead_retention_seconds IS NULL
        OR dead_retention_seconds >= 0
    )
);

ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS paused BOOLEAN;
UPDATE tenant_settings SET paused = FALSE WHERE paused IS NULL;
ALTER TABLE tenant_settings ALTER COLUMN paused SET DEFAULT FALSE;
ALTER TABLE tenant_settings ALTER COLUMN paused SET NOT NULL;

ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS offboarding BOOLEAN;
UPDATE tenant_settings SET offboarding = FALSE WHERE offboarding IS NULL;
ALTER TABLE tenant_settings ALTER COLUMN offboarding SET DEFAULT FALSE;
ALTER TABLE tenant_settings ALTER COLUMN offboarding SET NOT NULL;

ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS max_pending_jobs INTEGER;
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_max_pending_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_max_pending_check CHECK (
    max_pending_jobs IS NULL OR max_pending_jobs >= 1
);

ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS completed_retention_seconds INTEGER;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS dead_retention_seconds INTEGER;
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_completed_ret_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_completed_ret_check CHECK (
    completed_retention_seconds IS NULL OR completed_retention_seconds >= 0
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_dead_ret_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_dead_ret_check CHECK (
    dead_retention_seconds IS NULL OR dead_retention_seconds >= 0
);


ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS enqueue_rate_per_sec DOUBLE PRECISION;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS enqueue_burst INTEGER;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS enqueue_tokens DOUBLE PRECISION;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS enqueue_tokens_at TIMESTAMPTZ;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS lease_rate_per_sec DOUBLE PRECISION;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS lease_burst INTEGER;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS lease_tokens DOUBLE PRECISION;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS lease_tokens_at TIMESTAMPTZ;
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_enqueue_rate_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_enqueue_rate_check CHECK (
    enqueue_rate_per_sec IS NULL OR enqueue_rate_per_sec > 0
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_enqueue_burst_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_enqueue_burst_check CHECK (
    enqueue_burst IS NULL OR enqueue_burst >= 1
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_lease_rate_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_lease_rate_check CHECK (
    lease_rate_per_sec IS NULL OR lease_rate_per_sec > 0
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_lease_burst_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_lease_burst_check CHECK (
    lease_burst IS NULL OR lease_burst >= 1
);

-- Fair multi-tenant lease: least-recently-served tenant wins the next claim.
CREATE TABLE IF NOT EXISTS tenant_fairness (
    tenant_id       TEXT PRIMARY KEY,
    last_lease_at   TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tenant_fairness_last_lease
    ON tenant_fairness (last_lease_at ASC);

-- Recurring schedules: cron expression and/or fixed interval_seconds.
-- Prefer cron (5-field min hour dom mon dow, or 6-field with seconds).
-- interval_seconds remains for simple fixed delays / backward compatibility.
CREATE TABLE IF NOT EXISTS schedules (
    id                          UUID PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    name                        TEXT,
    queue_name                  TEXT NOT NULL,
    payload                     JSONB NOT NULL,
    interval_seconds            INTEGER,
    cron_expr                   TEXT,
    timezone                    TEXT NOT NULL DEFAULT 'UTC',
    priority                    INTEGER NOT NULL DEFAULT 0,
    max_attempts                INTEGER NOT NULL DEFAULT 3,
    visibility_timeout_seconds  INTEGER NOT NULL DEFAULT 30,
    enabled                     BOOLEAN NOT NULL DEFAULT TRUE,
    next_run_at                 TIMESTAMPTZ NOT NULL,
    last_enqueued_at            TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT schedules_interval_check CHECK (
        interval_seconds IS NULL OR interval_seconds >= 1
    ),
    CONSTRAINT schedules_spec_check CHECK (
        (cron_expr IS NOT NULL AND btrim(cron_expr) <> '')
        OR (interval_seconds IS NOT NULL AND interval_seconds >= 1)
    ),
    CONSTRAINT schedules_max_attempts_check CHECK (max_attempts >= 1),
    CONSTRAINT schedules_visibility_check CHECK (visibility_timeout_seconds >= 1)
);

CREATE INDEX IF NOT EXISTS idx_schedules_due
    ON schedules (next_run_at ASC)
    WHERE enabled = TRUE;

CREATE INDEX IF NOT EXISTS idx_schedules_tenant
    ON schedules (tenant_id);

-- Optional unique human name per tenant when name is set.
CREATE UNIQUE INDEX IF NOT EXISTS idx_schedules_tenant_name
    ON schedules (tenant_id, name)
    WHERE name IS NOT NULL;

-- Migrate interval-only schedules → allow cron + nullable interval.
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS cron_expr TEXT;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS timezone TEXT;
UPDATE schedules SET timezone = 'UTC' WHERE timezone IS NULL;
ALTER TABLE schedules ALTER COLUMN timezone SET DEFAULT 'UTC';
ALTER TABLE schedules ALTER COLUMN timezone SET NOT NULL;
ALTER TABLE schedules ALTER COLUMN interval_seconds DROP NOT NULL;
ALTER TABLE schedules DROP CONSTRAINT IF EXISTS schedules_interval_check;
ALTER TABLE schedules ADD CONSTRAINT schedules_interval_check CHECK (
    interval_seconds IS NULL OR interval_seconds >= 1
);
ALTER TABLE schedules DROP CONSTRAINT IF EXISTS schedules_spec_check;
ALTER TABLE schedules ADD CONSTRAINT schedules_spec_check CHECK (
    (cron_expr IS NOT NULL AND btrim(cron_expr) <> '')
    OR (interval_seconds IS NOT NULL AND interval_seconds >= 1)
);

-- HTTP webhooks: endpoints + durable delivery outbox.
CREATE TABLE IF NOT EXISTS webhook_endpoints (
    id                          UUID PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    url                         TEXT NOT NULL,
    secret                      TEXT,
    events                      TEXT[] NOT NULL,
    enabled                     BOOLEAN NOT NULL DEFAULT TRUE,
    max_attempts                INTEGER NOT NULL DEFAULT 5,
    backoff_base_seconds        INTEGER NOT NULL DEFAULT 1,
    backoff_max_seconds         INTEGER NOT NULL DEFAULT 300,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT webhook_endpoints_max_attempts_check CHECK (max_attempts >= 1),
    CONSTRAINT webhook_endpoints_backoff_base_check CHECK (backoff_base_seconds >= 0),
    CONSTRAINT webhook_endpoints_backoff_max_check CHECK (backoff_max_seconds >= 0)
);

CREATE INDEX IF NOT EXISTS idx_webhook_endpoints_tenant
    ON webhook_endpoints (tenant_id)
    WHERE enabled = TRUE;

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id                          UUID PRIMARY KEY,
    endpoint_id                 UUID NOT NULL REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
    tenant_id                   TEXT NOT NULL,
    job_id                      UUID NOT NULL,
    event                       TEXT NOT NULL,
    payload                     JSONB NOT NULL,
    status                      TEXT NOT NULL,
    attempts                    INTEGER NOT NULL DEFAULT 0,
    max_attempts                INTEGER NOT NULL,
    backoff_base_seconds        INTEGER NOT NULL DEFAULT 1,
    backoff_max_seconds         INTEGER NOT NULL DEFAULT 300,
    next_attempt_at             TIMESTAMPTZ NOT NULL,
    delivering_until            TIMESTAMPTZ,
    last_error                  TEXT,
    last_status_code            INTEGER,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at                TIMESTAMPTZ,

    CONSTRAINT webhook_deliveries_attempts_check CHECK (attempts >= 0),
    CONSTRAINT webhook_deliveries_max_attempts_check CHECK (max_attempts >= 1)
);

-- Status check replaceable for upgrades (adds 'delivering').
ALTER TABLE webhook_deliveries DROP CONSTRAINT IF EXISTS webhook_deliveries_status_check;
ALTER TABLE webhook_deliveries ADD CONSTRAINT webhook_deliveries_status_check CHECK (
    status IN ('pending', 'delivering', 'succeeded', 'dead')
);

ALTER TABLE webhook_deliveries ADD COLUMN IF NOT EXISTS delivering_until TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_due
    ON webhook_deliveries (next_attempt_at ASC)
    WHERE status = 'pending';

-- Hot claim path: due pending ordered by next_attempt_at under large backlog.
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_claim_pending
    ON webhook_deliveries (next_attempt_at ASC, id ASC)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_stuck
    ON webhook_deliveries (delivering_until ASC)
    WHERE status = 'delivering';

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_tenant_status
    ON webhook_deliveries (tenant_id, status);

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_job
    ON webhook_deliveries (job_id);

CREATE INDEX IF NOT EXISTS idx_webhook_endpoints_id
    ON webhook_endpoints (id);

-- Immutable per-job audit stream (append-only). Written in the same
-- transaction as each job state change so crashes cannot desync history
-- from final job rows. Tenants replay by (tenant_id, job_id, seq).
CREATE TABLE IF NOT EXISTS job_audit_events (
    event_id        UUID PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    job_id          UUID NOT NULL,
    seq             BIGINT NOT NULL,
    event_type      TEXT NOT NULL,
    from_status     TEXT,
    to_status       TEXT,
    actor           TEXT,
    detail          JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT job_audit_events_seq_check CHECK (seq >= 1),
    CONSTRAINT job_audit_events_job_seq_unique UNIQUE (job_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_job_audit_tenant_job_seq
    ON job_audit_events (tenant_id, job_id, seq ASC);

CREATE INDEX IF NOT EXISTS idx_job_audit_tenant_created
    ON job_audit_events (tenant_id, created_at ASC, event_id ASC);

CREATE INDEX IF NOT EXISTS idx_job_audit_job_seq
    ON job_audit_events (job_id, seq ASC);

-- Deny application-level mutation of history (still allow SELECT/INSERT).
-- Superuser maintenance can DROP; normal role should not UPDATE/DELETE.
REVOKE UPDATE, DELETE ON job_audit_events FROM PUBLIC;

-- Durable once-only side effects for a job (survive lease reclaim).
-- Two-phase protocol:
--   1. begin  → status=started with a stable idempotency_key BEFORE the
--               external call (charge / email / …).
--   2. complete → status=completed AFTER success.
-- If a worker dies between the external call and complete, a reclaiming
-- worker sees started and MUST re-invoke the external system with the same
-- idempotency_key (not a fresh key) so the external system dedupes.
CREATE TABLE IF NOT EXISTS job_side_effects (
    job_id              UUID NOT NULL,
    effect_key          TEXT NOT NULL,
    tenant_id           TEXT NOT NULL,
    status              TEXT NOT NULL,
    idempotency_key     TEXT NOT NULL,
    result              JSONB,
    started_by          TEXT,
    started_at          TIMESTAMPTZ,
    completed_by        TEXT,
    lease_token         UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    PRIMARY KEY (job_id, effect_key),
    CONSTRAINT job_side_effects_status_check CHECK (
        status IN ('started', 'completed')
    ),
    CONSTRAINT job_side_effects_key_check CHECK (effect_key <> ''),
    CONSTRAINT job_side_effects_idem_check CHECK (idempotency_key <> '')
);

CREATE INDEX IF NOT EXISTS idx_job_side_effects_tenant_job
    ON job_side_effects (tenant_id, job_id);

CREATE INDEX IF NOT EXISTS idx_job_side_effects_job
    ON job_side_effects (job_id);

-- Migrate older single-phase rows (completed-only, no idempotency_key).
ALTER TABLE job_side_effects ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE job_side_effects ADD COLUMN IF NOT EXISTS started_by TEXT;
ALTER TABLE job_side_effects ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;
UPDATE job_side_effects
SET idempotency_key = job_id::text || ':' || effect_key
WHERE idempotency_key IS NULL OR idempotency_key = '';
ALTER TABLE job_side_effects DROP CONSTRAINT IF EXISTS job_side_effects_status_check;
ALTER TABLE job_side_effects ADD CONSTRAINT job_side_effects_status_check CHECK (
    status IN ('started', 'completed')
);
ALTER TABLE job_side_effects DROP CONSTRAINT IF EXISTS job_side_effects_idem_check;
ALTER TABLE job_side_effects ADD CONSTRAINT job_side_effects_idem_check CHECK (
    idempotency_key IS NOT NULL AND idempotency_key <> ''
);
"""
