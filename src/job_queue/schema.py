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
    retry_backoff_base_seconds  INTEGER NOT NULL DEFAULT 1,
    retry_backoff_max_seconds   INTEGER NOT NULL DEFAULT 300,
    available_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dedupe_key                  TEXT,
    schedule_id                 UUID,
    depends_on                  UUID[] NOT NULL DEFAULT '{}',
    leased_by                   TEXT,
    lease_token                 UUID,
    lease_expires_at            TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at                TIMESTAMPTZ,
    result                      JSONB,
    last_error                  TEXT,

    CONSTRAINT jobs_attempts_check CHECK (attempts >= 0),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts >= 1),
    CONSTRAINT jobs_visibility_check CHECK (visibility_timeout_seconds >= 1),
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

UPDATE jobs
SET retry_backoff_base_seconds = 1
WHERE retry_backoff_base_seconds IS NULL;
UPDATE jobs
SET retry_backoff_max_seconds = 300
WHERE retry_backoff_max_seconds IS NULL;
ALTER TABLE jobs ALTER COLUMN retry_backoff_base_seconds SET DEFAULT 1;
ALTER TABLE jobs ALTER COLUMN retry_backoff_max_seconds SET DEFAULT 300;
ALTER TABLE jobs ALTER COLUMN retry_backoff_base_seconds SET NOT NULL;
ALTER TABLE jobs ALTER COLUMN retry_backoff_max_seconds SET NOT NULL;

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

-- Per-tenant concurrency limits (NULL row / missing = unlimited).
CREATE TABLE IF NOT EXISTS tenant_settings (
    tenant_id              TEXT PRIMARY KEY,
    max_concurrent_leases  INTEGER,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT tenant_settings_max_concurrent_check CHECK (
        max_concurrent_leases IS NULL OR max_concurrent_leases >= 1
    )
);

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
"""
