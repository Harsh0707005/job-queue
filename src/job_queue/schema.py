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
        'pending', 'leased', 'awaiting_approval', 'awaiting_children',
        'awaiting_skills', 'quarantined', 'frozen',
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

-- Parent/child spawn-join: children point at parent; parked parents list waiting_on.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS parent_job_id UUID;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS waiting_on UUID[];
UPDATE jobs SET waiting_on = '{}' WHERE waiting_on IS NULL;
ALTER TABLE jobs ALTER COLUMN waiting_on SET DEFAULT '{}';
ALTER TABLE jobs ALTER COLUMN waiting_on SET NOT NULL;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS await_children_started_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS await_children_deadline_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS await_children_outcome TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS await_children_resolved_at TIMESTAMPTZ;

-- Free-form string labels (tags) for list / bulk-cancel filters.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS labels TEXT[];
UPDATE jobs SET labels = '{}' WHERE labels IS NULL;
ALTER TABLE jobs ALTER COLUMN labels SET DEFAULT '{}';
ALTER TABLE jobs ALTER COLUMN labels SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_labels_gin
    ON jobs USING GIN (labels);

-- Required worker capabilities (AND). Empty = any worker may claim.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS required_skills TEXT[];
UPDATE jobs SET required_skills = '{}' WHERE required_skills IS NULL;
ALTER TABLE jobs ALTER COLUMN required_skills SET DEFAULT '{}';
ALTER TABLE jobs ALTER COLUMN required_skills SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_required_skills_gin
    ON jobs USING GIN (required_skills);

-- Hard TTL: unfinished jobs past expires_at are cancelled or dead-lettered.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS expiry_action TEXT;
UPDATE jobs SET expiry_action = 'dead' WHERE expiry_action IS NULL;
ALTER TABLE jobs ALTER COLUMN expiry_action SET DEFAULT 'dead';
ALTER TABLE jobs ALTER COLUMN expiry_action SET NOT NULL;
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_expiry_action_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_expiry_action_check CHECK (
    expiry_action IN ('cancel', 'dead')
);

CREATE INDEX IF NOT EXISTS idx_jobs_expires_at
    ON jobs (expires_at ASC)
    WHERE expires_at IS NOT NULL
      AND status IN (
          'pending', 'leased', 'awaiting_approval',
          'awaiting_children', 'awaiting_skills', 'quarantined'
      );

-- Poison-job quarantine: unique workers that killed this job.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS failed_workers TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS max_worker_hops INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS quarantine_action TEXT NOT NULL DEFAULT 'dead';
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_max_worker_hops_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_max_worker_hops_check CHECK (
    max_worker_hops IS NULL OR max_worker_hops >= 1
);
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_quarantine_action_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_quarantine_action_check CHECK (
    quarantine_action IN ('dead', 'release')
);

-- Idempotent atomic DAG enqueue batches (retry after crash returns same jobs).
CREATE TABLE IF NOT EXISTS job_dag_batches (
    id              UUID PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    queue_name      TEXT NOT NULL DEFAULT 'default',
    batch_key       TEXT NOT NULL,
    job_ids         UUID[] NOT NULL,
    refs            TEXT[] NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, queue_name, batch_key),
    CONSTRAINT job_dag_batches_batch_key_check CHECK (batch_key <> ''),
    CONSTRAINT job_dag_batches_len_check CHECK (
        cardinality(job_ids) = cardinality(refs)
        AND cardinality(job_ids) >= 1
    )
);

CREATE INDEX IF NOT EXISTS idx_job_dag_batches_tenant_created
    ON job_dag_batches (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_status_created
    ON jobs (tenant_id, status, created_at DESC);

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_approval_outcome_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_approval_outcome_check CHECK (
    approval_outcome IS NULL
    OR approval_outcome IN ('approved', 'rejected', 'timeout')
);

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_await_children_outcome_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_await_children_outcome_check CHECK (
    await_children_outcome IS NULL
    OR await_children_outcome IN ('joined', 'failed', 'timeout', 'cancelled')
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

-- Last successful worker for similar work (tenant + queue + payload kind).
-- Soft preference only: lease ranks that worker first while they are
-- active and skilled; other workers can still claim so jobs cannot stall.
CREATE TABLE IF NOT EXISTS job_worker_affinity (
    tenant_id         TEXT NOT NULL,
    queue_name        TEXT NOT NULL,
    kind              TEXT NOT NULL DEFAULT '',
    last_worker_id    TEXT NOT NULL,
    last_success_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, queue_name, kind)
);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS prefer_last_worker BOOLEAN;
UPDATE jobs SET prefer_last_worker = FALSE WHERE prefer_last_worker IS NULL;
ALTER TABLE jobs ALTER COLUMN prefer_last_worker SET DEFAULT FALSE;
ALTER TABLE jobs ALTER COLUMN prefer_last_worker SET NOT NULL;

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS payload_kind TEXT;
UPDATE jobs SET payload_kind = '' WHERE payload_kind IS NULL;
ALTER TABLE jobs ALTER COLUMN payload_kind SET DEFAULT '';
ALTER TABLE jobs ALTER COLUMN payload_kind SET NOT NULL;

-- Clone / re-run: new pending job points at the source it was copied from.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cloned_from_job_id UUID;
CREATE INDEX IF NOT EXISTS idx_jobs_cloned_from
    ON jobs (tenant_id, cloned_from_job_id)
    WHERE cloned_from_job_id IS NOT NULL;

-- Live fold key: same logical work may pile up (not unique, unlike dedupe).
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS coalesce_key TEXT;
CREATE INDEX IF NOT EXISTS idx_jobs_coalesce_key
    ON jobs (tenant_id, queue_name, coalesce_key)
    WHERE coalesce_key IS NOT NULL
      AND status IN (
          'pending', 'leased', 'awaiting_skills', 'quarantined', 'frozen'
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

-- At most one active (pending/leased/parked) job per dedupe key.
DROP INDEX IF EXISTS idx_jobs_dedupe_active;
CREATE UNIQUE INDEX idx_jobs_dedupe_active
    ON jobs (tenant_id, queue_name, dedupe_key)
    WHERE dedupe_key IS NOT NULL
      AND status IN (
          'pending', 'leased', 'awaiting_approval', 'awaiting_children',
          'awaiting_skills', 'quarantined', 'frozen'
      );

-- In-flight jobs per schedule (DAG ticks stamp schedule_id on every node,
-- so this is no longer UNIQUE). Overlap is enforced by FOR UPDATE on the
-- schedule row plus "any in-flight job for this schedule".
DROP INDEX IF EXISTS idx_jobs_schedule_active;
CREATE INDEX IF NOT EXISTS idx_jobs_schedule_inflight
    ON jobs (schedule_id)
    WHERE schedule_id IS NOT NULL
      AND status IN (
          'pending', 'leased', 'awaiting_approval', 'awaiting_children',
          'awaiting_skills', 'frozen'
      );

CREATE INDEX IF NOT EXISTS idx_jobs_parent
    ON jobs (tenant_id, parent_job_id)
    WHERE parent_job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_await_children_deadline
    ON jobs (await_children_deadline_at)
    WHERE status = 'awaiting_children' AND await_children_deadline_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_awaiting_children
    ON jobs (tenant_id, created_at ASC)
    WHERE status = 'awaiting_children';

CREATE INDEX IF NOT EXISTS idx_jobs_approval_deadline
    ON jobs (approval_deadline_at ASC)
    WHERE status = 'awaiting_approval';

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_awaiting_approval
    ON jobs (tenant_id, approval_deadline_at ASC)
    WHERE status = 'awaiting_approval';

CREATE INDEX IF NOT EXISTS idx_jobs_awaiting_skills
    ON jobs (tenant_id, created_at ASC)
    WHERE status = 'awaiting_skills';

CREATE INDEX IF NOT EXISTS idx_jobs_pending_skilled
    ON jobs (created_at ASC)
    WHERE status = 'pending' AND required_skills <> '{}';

CREATE INDEX IF NOT EXISTS idx_jobs_awaiting_skills_deadline
    ON jobs (deadline_at ASC)
    WHERE status = 'awaiting_skills' AND deadline_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_schedule_id
    ON jobs (schedule_id)
    WHERE schedule_id IS NOT NULL;

-- Stamp the fan-out generation on every tick job so cancel/replay can
-- close the set without guessing from created_at.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS tick_iteration BIGINT;
CREATE INDEX IF NOT EXISTS idx_jobs_schedule_iteration
    ON jobs (schedule_id, tick_iteration)
    WHERE schedule_id IS NOT NULL AND tick_iteration IS NOT NULL;

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

-- How long metric buckets/snapshots are kept (NULL = keep forever).
ALTER TABLE queue_settings ADD COLUMN IF NOT EXISTS metrics_retention_seconds INTEGER;
ALTER TABLE queue_settings DROP CONSTRAINT IF EXISTS queue_settings_metrics_ret_check;
ALTER TABLE queue_settings ADD CONSTRAINT queue_settings_metrics_ret_check CHECK (
    metrics_retention_seconds IS NULL
    OR metrics_retention_seconds >= 0
);

-- Queue partition count for sharding lease work across workers.
ALTER TABLE queue_settings ADD COLUMN IF NOT EXISTS partition_count INTEGER NOT NULL DEFAULT 32;
ALTER TABLE queue_settings DROP CONSTRAINT IF EXISTS queue_settings_partition_count_check;
ALTER TABLE queue_settings ADD CONSTRAINT queue_settings_partition_count_check CHECK (
    partition_count >= 1 AND partition_count <= 4096
);

-- Shard key on jobs (assigned at enqueue; claim filters by owned partitions).
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS partition_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_partition_id_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_partition_id_check CHECK (partition_id >= 0);

CREATE INDEX IF NOT EXISTS idx_jobs_partition_claim
    ON jobs (queue_name, partition_id, status, priority DESC, created_at ASC);

-- Live worker registry for partition ownership / rebalance.
CREATE TABLE IF NOT EXISTS worker_nodes (
    worker_id           TEXT PRIMARY KEY,
    status              TEXT NOT NULL DEFAULT 'active',
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT worker_nodes_status_check CHECK (status IN ('active', 'dead'))
);

-- Capabilities advertised by the worker. Lease requires
-- jobs.required_skills <@ worker_nodes.skills (empty job skills = any worker).
ALTER TABLE worker_nodes ADD COLUMN IF NOT EXISTS skills TEXT[];
UPDATE worker_nodes SET skills = '{}' WHERE skills IS NULL;
ALTER TABLE worker_nodes ALTER COLUMN skills SET DEFAULT '{}';
ALTER TABLE worker_nodes ALTER COLUMN skills SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_worker_nodes_active_seen
    ON worker_nodes (status, last_seen_at DESC);

-- Partition → worker assignment (generation bumps on rebalance).
CREATE TABLE IF NOT EXISTS partition_assignments (
    partition_id        INTEGER PRIMARY KEY,
    worker_id           TEXT,
    generation          BIGINT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT partition_assignments_id_check CHECK (partition_id >= 0)
);

CREATE INDEX IF NOT EXISTS idx_partition_assignments_worker
    ON partition_assignments (worker_id)
    WHERE worker_id IS NOT NULL;

-- Track one-time schema data migrations. Never re-apply defaults that would
-- overwrite intentional NULL (keep forever) after operators set retention.
CREATE TABLE IF NOT EXISTS schema_migrations (
    name        TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- New installs only: create the singleton settings row with a 7-day metrics
-- retention default. ON CONFLICT DO NOTHING preserves any existing row
-- (including metrics_retention_seconds IS NULL = keep forever).
INSERT INTO queue_settings (id, metrics_retention_seconds)
VALUES (1, 604800) ON CONFLICT (id) DO NOTHING;

-- One-time upgrade path: installs that already had queue_settings before
-- metrics_retention_seconds existed get 7 days once. Subsequent init_schema
-- runs (coordinator restart / reconnect) must NOT rewrite intentional NULL.
WITH ins AS (
    INSERT INTO schema_migrations (name)
    VALUES ('metrics_retention_default_7d_v1')
    ON CONFLICT (name) DO NOTHING
    RETURNING 1
)
UPDATE queue_settings
SET metrics_retention_seconds = 604800,
    updated_at = NOW()
WHERE metrics_retention_seconds IS NULL
  AND EXISTS (SELECT 1 FROM ins);

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
    -- Per-tenant retention override:
    --   NULL = inherit global queue_settings
    --   -1   = keep forever for this tenant (do not use global)
    --   >=0  = seconds to keep before purge
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
        OR completed_retention_seconds >= -1
    ),
    CONSTRAINT tenant_settings_dead_ret_check CHECK (
        dead_retention_seconds IS NULL
        OR dead_retention_seconds >= -1
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
    completed_retention_seconds IS NULL OR completed_retention_seconds >= -1
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_dead_ret_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_dead_ret_check CHECK (
    dead_retention_seconds IS NULL OR dead_retention_seconds >= -1
);

ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS default_job_ttl_seconds INTEGER;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS default_expiry_action TEXT;
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_default_ttl_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_default_ttl_check CHECK (
    default_job_ttl_seconds IS NULL OR default_job_ttl_seconds >= 1
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_default_expiry_action_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_default_expiry_action_check CHECK (
    default_expiry_action IS NULL OR default_expiry_action IN ('cancel', 'dead')
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

-- Spend caps: tumbling window of billed enqueues / runtime. Counters are
-- server-written only (never taken from a job payload).
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS spend_enqueue_limit BIGINT;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS spend_runtime_ms_limit BIGINT;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS spend_window_seconds INTEGER;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS spend_window_start TIMESTAMPTZ;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS spend_enqueues BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS spend_runtime_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_spend_enqueue_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_spend_enqueue_check CHECK (
    spend_enqueue_limit IS NULL OR spend_enqueue_limit >= 1
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_spend_runtime_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_spend_runtime_check CHECK (
    spend_runtime_ms_limit IS NULL OR spend_runtime_ms_limit >= 1
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_spend_window_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_spend_window_check CHECK (
    spend_window_seconds IS NULL OR spend_window_seconds >= 1
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_spend_counters_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_spend_counters_check CHECK (
    spend_enqueues >= 0 AND spend_runtime_ms >= 0
);

ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS default_max_worker_hops INTEGER;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS default_quarantine_action TEXT;
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_default_hops_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_default_hops_check CHECK (
    default_max_worker_hops IS NULL OR default_max_worker_hops >= 1
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_default_quarantine_action_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_default_quarantine_action_check CHECK (
    default_quarantine_action IS NULL
    OR default_quarantine_action IN ('dead', 'release')
);

-- Per-tenant failure circuit breaker. NULL threshold = disabled.
-- Failures are server-counted (worker nack / max-runtime dead). When the
-- count hits threshold inside the window, breaker_opened_at is set and
-- new leases/schedule ticks are skipped until cooldown elapses or reset.
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS breaker_failure_threshold INTEGER;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS breaker_window_seconds INTEGER;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS breaker_cooldown_seconds INTEGER;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS breaker_failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS breaker_window_start TIMESTAMPTZ;
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS breaker_opened_at TIMESTAMPTZ;
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_breaker_threshold_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_breaker_threshold_check CHECK (
    breaker_failure_threshold IS NULL OR breaker_failure_threshold >= 1
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_breaker_window_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_breaker_window_check CHECK (
    breaker_window_seconds IS NULL OR breaker_window_seconds >= 1
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_breaker_cooldown_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_breaker_cooldown_check CHECK (
    breaker_cooldown_seconds IS NULL OR breaker_cooldown_seconds >= 1
);
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_breaker_failures_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_breaker_failures_check CHECK (
    breaker_failures >= 0
);

-- Fair multi-tenant lease: least-recently-served tenant wins the next claim.
-- fair_virtual is a weighted-fair score (increment 1/weight on each serve).
CREATE TABLE IF NOT EXISTS tenant_fairness (
    tenant_id       TEXT PRIMARY KEY,
    last_lease_at   TIMESTAMPTZ NOT NULL
);

ALTER TABLE tenant_fairness ADD COLUMN IF NOT EXISTS fair_virtual DOUBLE PRECISION;
UPDATE tenant_fairness SET fair_virtual = 0 WHERE fair_virtual IS NULL;
ALTER TABLE tenant_fairness ALTER COLUMN fair_virtual SET DEFAULT 0;
ALTER TABLE tenant_fairness ALTER COLUMN fair_virtual SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tenant_fairness_last_lease
    ON tenant_fairness (last_lease_at ASC);

CREATE INDEX IF NOT EXISTS idx_tenant_fairness_virtual
    ON tenant_fairness (fair_virtual ASC);

-- Per-tenant fair-lease weight (NULL / missing = 1). Higher weight gets
-- more turns under load; WFQ virtual time still serves low-weight tenants.
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS lease_weight INTEGER;
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_lease_weight_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_lease_weight_check CHECK (
    lease_weight IS NULL OR (lease_weight >= 1 AND lease_weight <= 100)
);

-- Per-queue drain: block new enqueues + schedule fires for listed queues.
-- Existing pending / leased jobs still finish. Other queues keep running.
-- NULL / empty = nothing draining (not a tenant pause).
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS draining_queues TEXT[];

ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS prefer_last_worker_queues TEXT[];
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS prefer_last_worker_hold_seconds INTEGER;
ALTER TABLE tenant_settings DROP CONSTRAINT IF EXISTS tenant_settings_affinity_hold_check;
ALTER TABLE tenant_settings ADD CONSTRAINT tenant_settings_affinity_hold_check CHECK (
    prefer_last_worker_hold_seconds IS NULL
    OR (prefer_last_worker_hold_seconds >= 0
        AND prefer_last_worker_hold_seconds <= 3600)
);

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

-- Labels stamped onto every job fired by this schedule.
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS labels TEXT[];
UPDATE schedules SET labels = '{}' WHERE labels IS NULL;
ALTER TABLE schedules ALTER COLUMN labels SET DEFAULT '{}';
ALTER TABLE schedules ALTER COLUMN labels SET NOT NULL;

ALTER TABLE schedules ADD COLUMN IF NOT EXISTS required_skills TEXT[];
UPDATE schedules SET required_skills = '{}' WHERE required_skills IS NULL;
ALTER TABLE schedules ALTER COLUMN required_skills SET DEFAULT '{}';
ALTER TABLE schedules ALTER COLUMN required_skills SET NOT NULL;

-- Miss / catch-up policy when ticks were skipped (pause, coordinator down).
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS misfire_policy TEXT;
UPDATE schedules SET misfire_policy = 'catch_up_one' WHERE misfire_policy IS NULL;
ALTER TABLE schedules ALTER COLUMN misfire_policy SET DEFAULT 'catch_up_one';
ALTER TABLE schedules ALTER COLUMN misfire_policy SET NOT NULL;
ALTER TABLE schedules DROP CONSTRAINT IF EXISTS schedules_misfire_policy_check;
ALTER TABLE schedules ADD CONSTRAINT schedules_misfire_policy_check CHECK (
    misfire_policy IN ('skip', 'catch_up', 'catch_up_one')
);
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS catch_up_max INTEGER;
UPDATE schedules SET catch_up_max = 8 WHERE catch_up_max IS NULL;
ALTER TABLE schedules ALTER COLUMN catch_up_max SET DEFAULT 8;
ALTER TABLE schedules ALTER COLUMN catch_up_max SET NOT NULL;
ALTER TABLE schedules DROP CONSTRAINT IF EXISTS schedules_catch_up_max_check;
ALTER TABLE schedules ADD CONSTRAINT schedules_catch_up_max_check CHECK (
    catch_up_max >= 1 AND catch_up_max <= 100
);
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS missed_ticks BIGINT;
UPDATE schedules SET missed_ticks = 0 WHERE missed_ticks IS NULL;
ALTER TABLE schedules ALTER COLUMN missed_ticks SET DEFAULT 0;
ALTER TABLE schedules ALTER COLUMN missed_ticks SET NOT NULL;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS last_misfire_at TIMESTAMPTZ;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS catch_up_remaining INTEGER;

-- Optional DAG template: each tick materializes the whole graph atomically.
-- NULL / empty = single-job schedule (payload becomes the one job).
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS dag_jobs JSONB;
-- Optional multi-step workflow template. Mutually exclusive with dag_jobs.
-- Each tick starts one workflow run; step jobs carry schedule_id +
-- tick_iteration so mutex / waits / joins treat the whole run as one tick.
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS workflow JSONB;

-- Mutually exclusive schedule groups (per tenant). At most one member may
-- have a tick / DAG in flight. Membership is a name; NULL means ungrouped.
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS mutex_group TEXT;
CREATE INDEX IF NOT EXISTS idx_schedules_mutex_group
    ON schedules (tenant_id, mutex_group)
    WHERE mutex_group IS NOT NULL;

-- Downstream schedule waits until each waits_on tick (and any paused /
-- inflight mutex peer of those upstreams) is clear. Missed ticks stay
-- overdue so misfire_policy runs instead of dumping a backlog.
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS waits_on UUID[];
UPDATE schedules SET waits_on = '{}' WHERE waits_on IS NULL;
ALTER TABLE schedules ALTER COLUMN waits_on SET DEFAULT '{}';
ALTER TABLE schedules ALTER COLUMN waits_on SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_schedules_waits_on
    ON schedules USING GIN (waits_on);

-- Fan-out / join: a join schedule lists waiter ids. It fires one tick
-- per generation after every waiter has completed that same iteration.
-- tick_iteration is the last enqueued generation; last_completed_iteration
-- advances only when that generation's jobs all complete successfully.
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS joins_on UUID[];
UPDATE schedules SET joins_on = '{}' WHERE joins_on IS NULL;
ALTER TABLE schedules ALTER COLUMN joins_on SET DEFAULT '{}';
ALTER TABLE schedules ALTER COLUMN joins_on SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_schedules_joins_on
    ON schedules USING GIN (joins_on);
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS tick_iteration BIGINT;
UPDATE schedules SET tick_iteration = 0 WHERE tick_iteration IS NULL;
ALTER TABLE schedules ALTER COLUMN tick_iteration SET DEFAULT 0;
ALTER TABLE schedules ALTER COLUMN tick_iteration SET NOT NULL;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS last_completed_iteration BIGINT;

-- Closed per-generation payload published when an upstream tick succeeds.
-- Waiters and the join for that iteration attach it. Failed / cancelled
-- ticks stay failed with no payload. tenant_id + AAD bind the ciphertext
-- so a mid-generation move cannot leak another tenant's result.
CREATE TABLE IF NOT EXISTS schedule_generations (
    schedule_id     UUID NOT NULL,
    iteration       BIGINT NOT NULL,
    tenant_id       TEXT NOT NULL,
    status          TEXT NOT NULL,
    payload         JSONB,
    payload_key_id  UUID,
    source_job_ids  UUID[] NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (schedule_id, iteration),
    FOREIGN KEY (schedule_id) REFERENCES schedules(id) ON DELETE CASCADE
);
ALTER TABLE schedule_generations DROP CONSTRAINT IF EXISTS schedule_generations_status_check;
ALTER TABLE schedule_generations ADD CONSTRAINT schedule_generations_status_check
    CHECK (status IN ('pending', 'completed', 'failed'));
CREATE INDEX IF NOT EXISTS idx_schedule_generations_tenant
    ON schedule_generations (tenant_id, schedule_id);

ALTER TABLE job_dag_batches ADD COLUMN IF NOT EXISTS schedule_id UUID;

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_dag_batches_schedule_key
    ON job_dag_batches (schedule_id, batch_key)
    WHERE schedule_id IS NOT NULL;

-- Maintenance blackout windows: tenant-wide (schedule_id NULL) or one schedule.
-- During a covering window process_due does not enqueue; next_run_at stays
-- overdue so misfire_policy (skip / catch_up_one / catch_up) runs when the
-- window ends. Overlapping windows are a union. Quiet hours are kind=daily.
CREATE TABLE IF NOT EXISTS schedule_blackouts (
    id              UUID PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    schedule_id     UUID,
    kind            TEXT NOT NULL,
    starts_at       TIMESTAMPTZ,
    ends_at         TIMESTAMPTZ,
    daily_start     TIME,
    daily_end       TIME,
    timezone        TEXT NOT NULL DEFAULT 'UTC',
    reason          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT schedule_blackouts_kind_check CHECK (
        kind IN ('once', 'daily')
    ),
    CONSTRAINT schedule_blackouts_once_check CHECK (
        kind <> 'once' OR (
            starts_at IS NOT NULL
            AND ends_at IS NOT NULL
            AND ends_at > starts_at
        )
    ),
    CONSTRAINT schedule_blackouts_daily_check CHECK (
        kind <> 'daily' OR (
            daily_start IS NOT NULL
            AND daily_end IS NOT NULL
            AND daily_start <> daily_end
        )
    ),
    CONSTRAINT schedule_blackouts_schedule_fk
        FOREIGN KEY (schedule_id) REFERENCES schedules(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_schedule_blackouts_tenant
    ON schedule_blackouts (tenant_id, schedule_id);

CREATE INDEX IF NOT EXISTS idx_schedule_blackouts_once_active
    ON schedule_blackouts (tenant_id, starts_at, ends_at)
    WHERE kind = 'once';

CREATE INDEX IF NOT EXISTS idx_schedule_blackouts_daily
    ON schedule_blackouts (tenant_id)
    WHERE kind = 'daily';

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

-- Snapshot of endpoint.secret at delivery enqueue time. Retries / reclaims
-- must sign with this value so secret rotation mid-flight still verifies.
ALTER TABLE webhook_deliveries ADD COLUMN IF NOT EXISTS signing_secret TEXT;

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

-- Time-bucketed operational counters (durable throughput history).
-- Incremented in the same transaction as lease/ack/nack/webhook/schedule events
-- so high-load fleets cannot lose metric increments when a coordinator crashes.
CREATE TABLE IF NOT EXISTS queue_metric_buckets (
    bucket_start    TIMESTAMPTZ NOT NULL,
    tenant_id       TEXT NOT NULL,
    metric          TEXT NOT NULL,
    count           BIGINT NOT NULL DEFAULT 0,
    sum_ms          DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, tenant_id, metric),
    CONSTRAINT queue_metric_buckets_count_check CHECK (count >= 0),
    CONSTRAINT queue_metric_buckets_sum_ms_check CHECK (sum_ms >= 0)
);

CREATE INDEX IF NOT EXISTS idx_queue_metric_buckets_tenant_metric_time
    ON queue_metric_buckets (tenant_id, metric, bucket_start DESC);

CREATE INDEX IF NOT EXISTS idx_queue_metric_buckets_metric_time
    ON queue_metric_buckets (metric, bucket_start DESC);

-- Point-in-time gauge snapshots (lag, in-flight, backlog) for trend charts.
CREATE TABLE IF NOT EXISTS queue_metric_snapshots (
    sampled_at              TIMESTAMPTZ NOT NULL,
    tenant_id               TEXT NOT NULL,
    pending                 INTEGER NOT NULL DEFAULT 0,
    leased                  INTEGER NOT NULL DEFAULT 0,
    claimable               INTEGER NOT NULL DEFAULT 0,
    failed                  INTEGER NOT NULL DEFAULT 0,
    dead                    INTEGER NOT NULL DEFAULT 0,
    completed               INTEGER NOT NULL DEFAULT 0,
    cancelled               INTEGER NOT NULL DEFAULT 0,
    awaiting_approval       INTEGER NOT NULL DEFAULT 0,
    awaiting_children       INTEGER NOT NULL DEFAULT 0,
    queue_lag_seconds       DOUBLE PRECISION,
    avg_pending_age_seconds DOUBLE PRECISION,
    max_pending_age_seconds DOUBLE PRECISION,
    webhook_pending         INTEGER NOT NULL DEFAULT 0,
    webhook_delivering      INTEGER NOT NULL DEFAULT 0,
    webhook_succeeded       INTEGER NOT NULL DEFAULT 0,
    webhook_dead            INTEGER NOT NULL DEFAULT 0,
    schedules_enabled       INTEGER NOT NULL DEFAULT 0,
    schedules_due           INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (sampled_at, tenant_id)
);

CREATE INDEX IF NOT EXISTS idx_queue_metric_snapshots_tenant_time
    ON queue_metric_snapshots (tenant_id, sampled_at DESC);

-- Allow alert webhook deliveries without a job row (alert.fired / alert.resolved).
ALTER TABLE webhook_deliveries ALTER COLUMN job_id DROP NOT NULL;

-- Metric alert rules (threshold checks evaluated periodically).
CREATE TABLE IF NOT EXISTS alert_rules (
    id                  UUID PRIMARY KEY,
    name                TEXT NOT NULL,
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    scope               TEXT NOT NULL DEFAULT 'overall',
    tenant_id           TEXT,
    worker_id           TEXT,
    metric              TEXT NOT NULL,
    comparator          TEXT NOT NULL DEFAULT 'gt',
    threshold           DOUBLE PRECISION NOT NULL,
    window_seconds      INTEGER NOT NULL DEFAULT 300,
    cooldown_seconds    INTEGER NOT NULL DEFAULT 300,
    last_fired_at       TIMESTAMPTZ,
    muted_until         TIMESTAMPTZ,
    auto_pause_tenant   BOOLEAN NOT NULL DEFAULT FALSE,
    -- Hysteresis for auto-resolve: metric must be healthy vs recovery_threshold
    -- for recovery_evaluations consecutive evaluates before open events close.
    recovery_threshold  DOUBLE PRECISION,
    recovery_evaluations INTEGER NOT NULL DEFAULT 2,
    -- Min seconds between counted healthy ticks (dedupes concurrent evaluates).
    healthy_interval_seconds INTEGER NOT NULL DEFAULT 1,
    healthy_streak      INTEGER NOT NULL DEFAULT 0,
    last_healthy_at     TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT alert_rules_scope_check CHECK (
        scope IN ('overall', 'tenant', 'worker')
    ),
    CONSTRAINT alert_rules_comparator_check CHECK (
        comparator IN ('gt', 'gte', 'lt', 'lte')
    ),
    CONSTRAINT alert_rules_window_check CHECK (window_seconds >= 1),
    CONSTRAINT alert_rules_cooldown_check CHECK (cooldown_seconds >= 0),
    CONSTRAINT alert_rules_tenant_scope_check CHECK (
        (scope = 'tenant' AND tenant_id IS NOT NULL AND btrim(tenant_id) <> '')
        OR (scope <> 'tenant')
    ),
    CONSTRAINT alert_rules_worker_scope_check CHECK (
        (scope = 'worker' AND worker_id IS NOT NULL AND btrim(worker_id) <> '')
        OR (scope <> 'worker')
    ),
    CONSTRAINT alert_rules_auto_pause_scope_check CHECK (
        auto_pause_tenant = FALSE OR scope = 'tenant'
    ),
    CONSTRAINT alert_rules_recovery_evaluations_check CHECK (
        recovery_evaluations >= 0
    ),
    CONSTRAINT alert_rules_healthy_interval_check CHECK (
        healthy_interval_seconds >= 0
    ),
    CONSTRAINT alert_rules_healthy_streak_check CHECK (
        healthy_streak >= 0
    )
);

-- Upgrade path for existing deployments.
ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS muted_until TIMESTAMPTZ;
ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS auto_pause_tenant BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS recovery_threshold DOUBLE PRECISION;
ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS recovery_evaluations INTEGER NOT NULL DEFAULT 2;
ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS healthy_interval_seconds INTEGER NOT NULL DEFAULT 1;
ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS healthy_streak INTEGER NOT NULL DEFAULT 0;
ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS last_healthy_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_alert_rules_enabled
    ON alert_rules (enabled)
    WHERE enabled = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_rules_name
    ON alert_rules (name);

-- Durable history of alert firings (for trends / audit).
CREATE TABLE IF NOT EXISTS alert_events (
    id                  UUID PRIMARY KEY,
    rule_id             UUID NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    rule_name           TEXT NOT NULL,
    metric              TEXT NOT NULL,
    scope               TEXT NOT NULL,
    tenant_id           TEXT,
    worker_id           TEXT,
    value               DOUBLE PRECISION NOT NULL,
    threshold           DOUBLE PRECISION NOT NULL,
    comparator          TEXT NOT NULL,
    window_seconds      INTEGER,
    detail              JSONB NOT NULL DEFAULT '{}'::jsonb,
    fired_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at         TIMESTAMPTZ,
    resolved_by         TEXT,
    resolution_note     TEXT
);

ALTER TABLE alert_events ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;
ALTER TABLE alert_events ADD COLUMN IF NOT EXISTS resolved_by TEXT;
ALTER TABLE alert_events ADD COLUMN IF NOT EXISTS resolution_note TEXT;

CREATE INDEX IF NOT EXISTS idx_alert_events_fired
    ON alert_events (fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_events_rule_fired
    ON alert_events (rule_id, fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_events_tenant_fired
    ON alert_events (tenant_id, fired_at DESC)
    WHERE tenant_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_alert_events_open
    ON alert_events (tenant_id, fired_at DESC)
    WHERE resolved_at IS NULL;

-- Multi-step saga workflows: run + per-step jobs with reverse compensation.
CREATE TABLE IF NOT EXISTS workflow_runs (
    id                  UUID PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    name                TEXT,
    status              TEXT NOT NULL DEFAULT 'running',
    steps_def           JSONB NOT NULL,
    current_index       INTEGER NOT NULL DEFAULT 0,
    fail_step_index     INTEGER,
    fail_reason         TEXT,
    max_compensate_retries INTEGER NOT NULL DEFAULT 5,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    CONSTRAINT workflow_runs_status_check CHECK (
        status IN ('running', 'compensating', 'completed', 'failed', 'cancelled')
    ),
    CONSTRAINT workflow_runs_current_index_check CHECK (current_index >= 0),
    CONSTRAINT workflow_runs_max_comp_retries_check CHECK (
        max_compensate_retries >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_tenant_created
    ON workflow_runs (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_tenant_status
    ON workflow_runs (tenant_id, status);

ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS schedule_id UUID;
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS tick_iteration BIGINT;
CREATE INDEX IF NOT EXISTS idx_workflow_runs_schedule
    ON workflow_runs (schedule_id)
    WHERE schedule_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_runs_schedule_inflight
    ON workflow_runs (schedule_id)
    WHERE schedule_id IS NOT NULL
      AND status IN ('running', 'compensating');

CREATE TABLE IF NOT EXISTS workflow_step_runs (
    id                  UUID PRIMARY KEY,
    run_id              UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    tenant_id           TEXT NOT NULL,
    step_index          INTEGER NOT NULL,
    name                TEXT NOT NULL,
    direction           TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    job_id              UUID UNIQUE,
    result              JSONB,
    error               TEXT,
    idempotency_key     TEXT NOT NULL,
    retry_count         INTEGER NOT NULL DEFAULT 0,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT workflow_step_runs_direction_check CHECK (
        direction IN ('forward', 'compensate')
    ),
    CONSTRAINT workflow_step_runs_status_check CHECK (
        status IN (
            'pending', 'running', 'completed', 'failed',
            'skipped', 'compensated', 'awaiting_signal'
        )
    ),
    CONSTRAINT workflow_step_runs_index_check CHECK (step_index >= 0),
    CONSTRAINT workflow_step_runs_retry_check CHECK (retry_count >= 0),
    CONSTRAINT workflow_step_runs_unique_dir UNIQUE (run_id, step_index, direction)
);

ALTER TABLE workflow_step_runs ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflow_step_runs ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ;

ALTER TABLE workflow_step_runs DROP CONSTRAINT IF EXISTS workflow_step_runs_status_check;
ALTER TABLE workflow_step_runs ADD CONSTRAINT workflow_step_runs_status_check CHECK (
    status IN (
        'pending', 'running', 'completed', 'failed',
        'skipped', 'compensated', 'awaiting_signal'
    )
);

-- Per-run cap for redriving failed/killed compensate jobs (not job max_attempts).
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS max_compensate_retries INTEGER NOT NULL DEFAULT 5;

CREATE INDEX IF NOT EXISTS idx_workflow_step_runs_run
    ON workflow_step_runs (run_id, step_index, direction);

CREATE INDEX IF NOT EXISTS idx_workflow_step_runs_tenant_run
    ON workflow_step_runs (tenant_id, run_id);

CREATE INDEX IF NOT EXISTS idx_workflow_step_runs_job
    ON workflow_step_runs (job_id)
    WHERE job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_workflow_runs_compensating
    ON workflow_runs (status, updated_at)
    WHERE status = 'compensating';

CREATE INDEX IF NOT EXISTS idx_workflow_step_runs_awaiting_signal
    ON workflow_step_runs (deadline_at)
    WHERE status = 'awaiting_signal' AND deadline_at IS NOT NULL;

-- Named signals latch per (run, name). Early send still counts; tenant_id
-- is required on every read/write so one tenant cannot consume another.
CREATE TABLE IF NOT EXISTS workflow_signals (
    id              UUID PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    tenant_id       TEXT NOT NULL,
    name            TEXT NOT NULL,
    payload         JSONB,
    payload_key_id  UUID,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT workflow_signals_name_check CHECK (
        char_length(name) BETWEEN 1 AND 128
    ),
    CONSTRAINT workflow_signals_unique UNIQUE (run_id, name)
);

CREATE INDEX IF NOT EXISTS idx_workflow_signals_tenant_run
    ON workflow_signals (tenant_id, run_id);

CREATE INDEX IF NOT EXISTS idx_workflow_signals_tenant_name
    ON workflow_signals (tenant_id, name);

-- Per-tenant envelope encryption keys (DEK wrapped by external KEK).
CREATE TABLE IF NOT EXISTS tenant_payload_keys (
    tenant_id       TEXT NOT NULL,
    key_id          UUID NOT NULL,
    wrapped_dek     BYTEA NOT NULL,
    wrap_nonce      BYTEA NOT NULL,
    kek_id          TEXT NOT NULL,
    status          TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retired_at      TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, key_id),
    CONSTRAINT tenant_payload_keys_status_check CHECK (
        status IN ('active', 'decrypt_only', 'retired')
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tenant_payload_keys_one_active
    ON tenant_payload_keys (tenant_id)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_tenant_payload_keys_status
    ON tenant_payload_keys (tenant_id, status);

CREATE TABLE IF NOT EXISTS payload_key_rotations (
    id                      UUID PRIMARY KEY,
    tenant_id               TEXT NOT NULL,
    from_key_id             UUID,
    to_key_id               UUID NOT NULL,
    status                  TEXT NOT NULL,
    jobs_rewrapped          INTEGER NOT NULL DEFAULT 0,
    schedules_rewrapped     INTEGER NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at            TIMESTAMPTZ,
    CONSTRAINT payload_key_rotations_status_check CHECK (
        status IN ('reencrypting', 'complete', 'aborted')
    ),
    CONSTRAINT payload_key_rotations_jobs_check CHECK (jobs_rewrapped >= 0),
    CONSTRAINT payload_key_rotations_schedules_check CHECK (
        schedules_rewrapped >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_payload_key_rotations_tenant_status
    ON payload_key_rotations (tenant_id, status, created_at);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS payload_key_id UUID;
CREATE INDEX IF NOT EXISTS idx_jobs_payload_key
    ON jobs (tenant_id, payload_key_id)
    WHERE payload_key_id IS NOT NULL;

ALTER TABLE schedules ADD COLUMN IF NOT EXISTS payload_key_id UUID;
CREATE INDEX IF NOT EXISTS idx_schedules_payload_key
    ON schedules (tenant_id, payload_key_id)
    WHERE payload_key_id IS NOT NULL;

ALTER TABLE webhook_deliveries ADD COLUMN IF NOT EXISTS payload_key_id UUID;
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_payload_key
    ON webhook_deliveries (tenant_id, payload_key_id)
    WHERE payload_key_id IS NOT NULL;

ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS payload_key_id UUID;
CREATE INDEX IF NOT EXISTS idx_workflow_runs_payload_key
    ON workflow_runs (tenant_id, payload_key_id)
    WHERE payload_key_id IS NOT NULL;

-- Live tenant export / restore (crash-safe, source keeps running).
ALTER TABLE tenant_settings ADD COLUMN IF NOT EXISTS restoring BOOLEAN;
UPDATE tenant_settings SET restoring = FALSE WHERE restoring IS NULL;
ALTER TABLE tenant_settings ALTER COLUMN restoring SET DEFAULT FALSE;
ALTER TABLE tenant_settings ALTER COLUMN restoring SET NOT NULL;

CREATE TABLE IF NOT EXISTS tenant_snapshots (
    id                  UUID PRIMARY KEY,
    source_tenant_id    TEXT NOT NULL,
    format_version      INTEGER NOT NULL DEFAULT 1,
    status              TEXT NOT NULL,
    counts              JSONB NOT NULL DEFAULT '{}'::jsonb,
    error               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    CONSTRAINT tenant_snapshots_status_check CHECK (
        status IN ('capturing', 'ready', 'failed')
    )
);

CREATE INDEX IF NOT EXISTS idx_tenant_snapshots_source
    ON tenant_snapshots (source_tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS tenant_snapshot_blobs (
    snapshot_id     UUID NOT NULL REFERENCES tenant_snapshots(id) ON DELETE CASCADE,
    kind            TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    row_count       INTEGER NOT NULL DEFAULT 0,
    rows            JSONB NOT NULL,
    PRIMARY KEY (snapshot_id, kind, seq),
    CONSTRAINT tenant_snapshot_blobs_seq_check CHECK (seq >= 0),
    CONSTRAINT tenant_snapshot_blobs_row_count_check CHECK (row_count >= 0)
);

CREATE TABLE IF NOT EXISTS tenant_restores (
    id                  UUID PRIMARY KEY,
    snapshot_id         UUID NOT NULL REFERENCES tenant_snapshots(id),
    target_tenant_id    TEXT NOT NULL,
    source_tenant_id    TEXT NOT NULL,
    status              TEXT NOT NULL,
    kind_index          INTEGER NOT NULL DEFAULT 0,
    blob_seq            INTEGER NOT NULL DEFAULT 0,
    blob_offset         INTEGER NOT NULL DEFAULT 0,
    applied             JSONB NOT NULL DEFAULT '{}'::jsonb,
    id_map              JSONB NOT NULL DEFAULT '{}'::jsonb,
    exported_settings   JSONB,
    error               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    CONSTRAINT tenant_restores_status_check CHECK (
        status IN ('restoring', 'complete', 'failed', 'aborted')
    ),
    CONSTRAINT tenant_restores_kind_index_check CHECK (kind_index >= 0),
    CONSTRAINT tenant_restores_blob_seq_check CHECK (blob_seq >= 0),
    CONSTRAINT tenant_restores_blob_offset_check CHECK (blob_offset >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tenant_restores_one_active_target
    ON tenant_restores (target_tenant_id)
    WHERE status = 'restoring';

CREATE INDEX IF NOT EXISTS idx_tenant_restores_target
    ON tenant_restores (target_tenant_id, created_at DESC);

-- Per-tenant billing meters. Hourly buckets; incremented in the same
-- transaction as enqueue/lease/ack/nack/… so a crash cannot leave a job
-- mutation without its usage increment (or vice versa). Independent of
-- queue_metric_buckets retention so billing history is not purged.
CREATE TABLE IF NOT EXISTS tenant_usage_buckets (
    bucket_start            TIMESTAMPTZ NOT NULL,
    tenant_id               TEXT NOT NULL,
    enqueues                BIGINT NOT NULL DEFAULT 0,
    leases                  BIGINT NOT NULL DEFAULT 0,
    acks                    BIGINT NOT NULL DEFAULT 0,
    nacks                   BIGINT NOT NULL DEFAULT 0,
    fails                   BIGINT NOT NULL DEFAULT 0,
    dead_letters            BIGINT NOT NULL DEFAULT 0,
    cancels                 BIGINT NOT NULL DEFAULT 0,
    releases                BIGINT NOT NULL DEFAULT 0,
    preemptions             BIGINT NOT NULL DEFAULT 0,
    schedule_fires          BIGINT NOT NULL DEFAULT 0,
    webhook_successes       BIGINT NOT NULL DEFAULT 0,
    webhook_failures        BIGINT NOT NULL DEFAULT 0,
    spawn_children          BIGINT NOT NULL DEFAULT 0,
    runtime_ms              BIGINT NOT NULL DEFAULT 0,
    wait_ms                 BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, tenant_id),
    CONSTRAINT tenant_usage_buckets_enqueues_check CHECK (enqueues >= 0),
    CONSTRAINT tenant_usage_buckets_leases_check CHECK (leases >= 0),
    CONSTRAINT tenant_usage_buckets_acks_check CHECK (acks >= 0),
    CONSTRAINT tenant_usage_buckets_nacks_check CHECK (nacks >= 0),
    CONSTRAINT tenant_usage_buckets_fails_check CHECK (fails >= 0),
    CONSTRAINT tenant_usage_buckets_dead_letters_check CHECK (dead_letters >= 0),
    CONSTRAINT tenant_usage_buckets_cancels_check CHECK (cancels >= 0),
    CONSTRAINT tenant_usage_buckets_releases_check CHECK (releases >= 0),
    CONSTRAINT tenant_usage_buckets_preemptions_check CHECK (preemptions >= 0),
    CONSTRAINT tenant_usage_buckets_schedule_fires_check CHECK (schedule_fires >= 0),
    CONSTRAINT tenant_usage_buckets_webhook_successes_check CHECK (webhook_successes >= 0),
    CONSTRAINT tenant_usage_buckets_webhook_failures_check CHECK (webhook_failures >= 0),
    CONSTRAINT tenant_usage_buckets_spawn_children_check CHECK (spawn_children >= 0),
    CONSTRAINT tenant_usage_buckets_runtime_ms_check CHECK (runtime_ms >= 0),
    CONSTRAINT tenant_usage_buckets_wait_ms_check CHECK (wait_ms >= 0)
);

CREATE INDEX IF NOT EXISTS idx_tenant_usage_buckets_tenant_time
    ON tenant_usage_buckets (tenant_id, bucket_start DESC);

-- Single-row control-plane leader lease. Coordinators renew this; the
-- holder runs schedules / webhooks / alerts / retention. Followers serve
-- worker HTTP only. An expired lease is stolen on the next acquire.
CREATE TABLE IF NOT EXISTS coordinator_leader (
    slot                TEXT PRIMARY KEY,
    node_id             TEXT NOT NULL,
    lease_token         UUID NOT NULL,
    epoch               BIGINT NOT NULL,
    expires_at          TIMESTAMPTZ NOT NULL,
    acquired_at         TIMESTAMPTZ NOT NULL,
    renewed_at          TIMESTAMPTZ NOT NULL,
    schedule_ticks      BIGINT NOT NULL DEFAULT 0,
    webhook_ticks       BIGINT NOT NULL DEFAULT 0,
    alert_ticks         BIGINT NOT NULL DEFAULT 0,
    retention_ticks     BIGINT NOT NULL DEFAULT 0,
    last_schedule_at    TIMESTAMPTZ,
    last_webhook_at     TIMESTAMPTZ,
    last_alert_at       TIMESTAMPTZ,
    last_retention_at   TIMESTAMPTZ,
    CONSTRAINT coordinator_leader_epoch_check CHECK (epoch >= 1),
    CONSTRAINT coordinator_leader_schedule_ticks_check CHECK (schedule_ticks >= 0),
    CONSTRAINT coordinator_leader_webhook_ticks_check CHECK (webhook_ticks >= 0),
    CONSTRAINT coordinator_leader_alert_ticks_check CHECK (alert_ticks >= 0),
    CONSTRAINT coordinator_leader_retention_ticks_check CHECK (retention_ticks >= 0)
);

"""
