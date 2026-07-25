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
    lease_expires_at            TIMESTAMPTZ,
    lease_started_at            TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at                TIMESTAMPTZ,
    result                      JSONB,
    last_error                  TEXT,

    CONSTRAINT jobs_attempts_check CHECK (attempts >= 0),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts >= 1),
    CONSTRAINT jobs_visibility_check CHECK (visibility_timeout_seconds >= 1),
    CONSTRAINT jobs_max_runtime_check CHECK (max_runtime_seconds >= 1),
    CONSTRAINT jobs_backoff_base_check CHECK (retry_backoff_base_seconds >= 0),
    CONSTRAINT jobs_backoff_max_check CHECK (retry_backoff_max_seconds >= 0)
);

-- Status check kept replaceable so upgrades can add new statuses.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check CHECK (
    status IN ('pending', 'leased', 'completed', 'failed', 'dead', 'cancelled')
);

-- Backward-compatible column adds for older installs.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_token UUID;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dedupe_key TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS schedule_id UUID;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS depends_on UUID[];
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS result JSONB;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS retry_backoff_base_seconds INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS retry_backoff_max_seconds INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS max_runtime_seconds INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_started_at TIMESTAMPTZ;

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

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_leased
    ON jobs (tenant_id)
    WHERE status = 'leased';

-- At most one active (pending/leased) job per dedupe key within a tenant queue.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_dedupe_active
    ON jobs (tenant_id, queue_name, dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'leased');

-- At most one in-flight job per recurring schedule (no overlapping runs).
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_schedule_active
    ON jobs (schedule_id)
    WHERE schedule_id IS NOT NULL AND status IN ('pending', 'leased');

CREATE INDEX IF NOT EXISTS idx_jobs_schedule_id
    ON jobs (schedule_id)
    WHERE schedule_id IS NOT NULL;

-- Find children of a parent quickly (contains / overlap queries).
CREATE INDEX IF NOT EXISTS idx_jobs_depends_on
    ON jobs USING GIN (depends_on);

-- Per-tenant concurrency limits + pause flag (NULL row / missing = unlimited, not paused).
CREATE TABLE IF NOT EXISTS tenant_settings (
    tenant_id              TEXT PRIMARY KEY,
    max_concurrent_leases  INTEGER,
    paused                 BOOLEAN NOT NULL DEFAULT FALSE,
    offboarding            BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT tenant_settings_max_concurrent_check CHECK (
        max_concurrent_leases IS NULL OR max_concurrent_leases >= 1
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

-- Fair multi-tenant lease: least-recently-served tenant wins the next claim.
CREATE TABLE IF NOT EXISTS tenant_fairness (
    tenant_id       TEXT PRIMARY KEY,
    last_lease_at   TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tenant_fairness_last_lease
    ON tenant_fairness (last_lease_at ASC);

-- Recurring schedules: tick to enqueue jobs every interval_seconds.
CREATE TABLE IF NOT EXISTS schedules (
    id                          UUID PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    name                        TEXT,
    queue_name                  TEXT NOT NULL,
    payload                     JSONB NOT NULL,
    interval_seconds            INTEGER NOT NULL,
    priority                    INTEGER NOT NULL DEFAULT 0,
    max_attempts                INTEGER NOT NULL DEFAULT 3,
    visibility_timeout_seconds  INTEGER NOT NULL DEFAULT 30,
    enabled                     BOOLEAN NOT NULL DEFAULT TRUE,
    next_run_at                 TIMESTAMPTZ NOT NULL,
    last_enqueued_at            TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT schedules_interval_check CHECK (interval_seconds >= 1),
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

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_stuck
    ON webhook_deliveries (delivering_until ASC)
    WHERE status = 'delivering';

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_tenant_status
    ON webhook_deliveries (tenant_id, status);

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_job
    ON webhook_deliveries (job_id);
"""
