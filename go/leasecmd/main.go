// Lease coordinator: optimized fair multi-tenant lease path for the job queue.
//
// Implements the hot worker protocol against the same Postgres schema as the
// Python JobQueue, using set-based UPDATE ... FROM (SELECT ... FOR UPDATE
// SKIP LOCKED LIMIT n) so batch fair pulls do not N-round-trip per job.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := env("JOB_QUEUE_DSN", "postgresql://queue:queue@localhost:5432/queue")
	addr := env("LISTEN_ADDR", ":8080")
	maxConns := int32(envInt("PG_POOL_MAX", 64))
	minConns := int32(envInt("PG_POOL_MIN", 8))

	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("parse dsn: %v", err)
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = minConns
	cfg.MaxConnLifetime = 30 * time.Second
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping: %v", err)
	}
	if err := ensureLeaseIndexes(ctx, pool); err != nil {
		log.Printf("warn: ensure indexes: %v", err)
	}
	log.Printf("go-lease-coordinator listening on %s (pool max=%d)", addr, maxConns)

	s := &Server{pool: pool, node: env("COORDINATOR_NODE", "go-lease")}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/v1/lease", s.handleLease)
	mux.HandleFunc("/v1/jobs/", s.handleJobPath)
	mux.HandleFunc("/v1/jobs", s.handleEnqueue)
	mux.HandleFunc("/v1/admin/backlog", s.handleBacklog)
	mux.HandleFunc("/v1/tenants/", s.handleTenantPath)

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           withLogging(mux),
		ReadHeaderTimeout: 10 * time.Second,
		// Lease under load can hold a transaction briefly; keep generous.
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

type Server struct {
	pool *pgxpool.Pool
	node string
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "role": "coordinator", "node": s.node, "impl": "go-lease"})
}

func (s *Server) handleLease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		WorkerID                  string  `json:"worker_id"`
		TenantID                  *string `json:"tenant_id"`
		QueueName                 string  `json:"queue_name"`
		VisibilityTimeoutSeconds  *int    `json:"visibility_timeout_seconds"`
		Limit                     int     `json:"limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": "invalid json"})
		return
	}
	if body.WorkerID == "" {
		writeJSON(w, 400, map[string]any{"error": "worker_id is required"})
		return
	}
	if body.QueueName == "" {
		body.QueueName = "default"
	}
	if body.Limit < 1 {
		body.Limit = 1
	}
	if body.Limit > 500 {
		writeJSON(w, 400, map[string]any{"error": "limit must be <= 500"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var (
		jobs []map[string]any
		err  error
	)
	if body.TenantID != nil && *body.TenantID != "" {
		jobs, err = s.leaseForTenant(ctx, *body.TenantID, body.WorkerID, body.QueueName, body.VisibilityTimeoutSeconds, body.Limit)
	} else {
		jobs, err = s.leaseFair(ctx, body.WorkerID, body.QueueName, body.VisibilityTimeoutSeconds, body.Limit)
	}
	if err != nil {
		log.Printf("lease error: %v", err)
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	if len(jobs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	payload := map[string]any{"jobs": jobs, "count": len(jobs)}
	if len(jobs) == 1 {
		payload["job"] = jobs[0]
	}
	writeJSON(w, 200, payload)
}

// leaseFair claims up to limit jobs in ONE set-based statement ordered by
// priority lanes then least-recently-served tenant (approx fairness for the batch).
func (s *Server) leaseFair(
	ctx context.Context,
	workerID, queueName string,
	vtOverride *int,
	limit int,
) ([]map[string]any, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Lightweight bookkeeping; does not scan the whole ready set.
	if err := deadLetterExhausted(ctx, tx, nil); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	// Default VT from job row when override is nil — use coalesce in SQL.
	vtArg := interface{}(nil)
	if vtOverride != nil {
		vtArg = *vtOverride
	}

	// Set-based claim: lock candidates with SKIP LOCKED, update all, return rows.
	// Fairness: order by priority DESC, last_lease_at ASC (never-served first),
	// then created_at. Within a batch a hot tenant may still take multiple slots
	// if it dominates a priority lane — acceptable trade for throughput; LRS is
	// updated for every distinct tenant in the batch.
	const q = `
WITH candidates AS (
  SELECT j.id
  FROM jobs j
  WHERE j.queue_name = $1
    AND NOT EXISTS (
      SELECT 1 FROM tenant_settings ts
      WHERE ts.tenant_id = j.tenant_id AND ts.paused = TRUE
    )
    AND (
      (
        j.status = 'pending'
        AND j.available_at <= $2
        AND (
          COALESCE(j.depends_on, '{}'::uuid[]) = '{}'::uuid[]
          OR NOT EXISTS (
            SELECT 1
            FROM unnest(COALESCE(j.depends_on, '{}'::uuid[])) AS parent_id
            WHERE NOT EXISTS (
              SELECT 1 FROM jobs parent
              WHERE parent.id = parent_id
                AND parent.tenant_id = j.tenant_id
                AND parent.status = 'completed'
            )
          )
        )
        AND (
          NOT EXISTS (
            SELECT 1 FROM tenant_settings ts
            WHERE ts.tenant_id = j.tenant_id AND ts.max_concurrent_leases IS NOT NULL
          )
          OR (
            SELECT COUNT(*)::int FROM jobs j2
            WHERE j2.tenant_id = j.tenant_id AND j2.status = 'leased'
          ) < (
            SELECT ts.max_concurrent_leases FROM tenant_settings ts
            WHERE ts.tenant_id = j.tenant_id
          )
        )
      )
      OR (
        j.status = 'leased'
        AND j.lease_expires_at < $2
        AND j.attempts < j.max_attempts
      )
    )
  ORDER BY
    j.priority DESC,
    COALESCE(
      (SELECT tf.last_lease_at FROM tenant_fairness tf WHERE tf.tenant_id = j.tenant_id),
      TIMESTAMPTZ '-infinity'
    ) ASC,
    j.created_at ASC
  FOR UPDATE OF j SKIP LOCKED
  LIMIT $3
),
claimed AS (
  UPDATE jobs j
  SET status = 'leased',
      leased_by = $4,
      lease_token = gen_random_uuid(),
      lease_expires_at = $2 + make_interval(secs => LEAST(
        COALESCE($5::int, j.visibility_timeout_seconds),
        COALESCE(j.max_runtime_seconds, 3600)
      )),
      lease_started_at = $2,
      attempts = j.attempts + 1,
      visibility_timeout_seconds = COALESCE($5::int, j.visibility_timeout_seconds),
      updated_at = $2,
      last_error = NULL
  FROM candidates c
  WHERE j.id = c.id
  RETURNING j.*
),
fair AS (
  INSERT INTO tenant_fairness (tenant_id, last_lease_at)
  SELECT tenant_id, clock_timestamp()
  FROM (SELECT DISTINCT tenant_id FROM claimed) d
  ON CONFLICT (tenant_id) DO UPDATE
  SET last_lease_at = clock_timestamp()
  RETURNING tenant_id
)
SELECT
  id, tenant_id, queue_name, payload, status, priority, attempts, max_attempts,
  visibility_timeout_seconds, max_runtime_seconds,
  retry_backoff_base_seconds, retry_backoff_max_seconds,
  available_at, dedupe_key, schedule_id, depends_on,
  leased_by, lease_token, lease_expires_at, lease_started_at,
  created_at, updated_at, completed_at, result, last_error
FROM claimed
ORDER BY priority DESC, created_at ASC
`
	rows, err := tx.Query(ctx, q, queueName, now, limit, workerID, vtArg)
	if err != nil {
		return nil, fmt.Errorf("fair claim: %w", err)
	}
	jobs, err := collectJobs(rows)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *Server) leaseForTenant(
	ctx context.Context,
	tenantID, workerID, queueName string,
	vtOverride *int,
	limit int,
) ([]map[string]any, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Serialize concurrency decisions for this tenant.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, tenantID); err != nil {
		return nil, err
	}
	if err := deadLetterExhausted(ctx, tx, &tenantID); err != nil {
		return nil, err
	}

	var paused bool
	err = tx.QueryRow(ctx, `SELECT paused FROM tenant_settings WHERE tenant_id=$1`, tenantID).Scan(&paused)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if paused {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}

	var maxConc *int
	_ = tx.QueryRow(ctx, `SELECT max_concurrent_leases FROM tenant_settings WHERE tenant_id=$1`, tenantID).Scan(&maxConc)
	var leasedCount int
	if err := tx.QueryRow(ctx,
		`SELECT COUNT(*)::int FROM jobs WHERE tenant_id=$1 AND status='leased'`, tenantID,
	).Scan(&leasedCount); err != nil {
		return nil, err
	}
	take := limit
	if maxConc != nil {
		room := *maxConc - leasedCount
		if room < 1 {
			// Only reclaim expired under hard cap.
			take = limit
		} else if room < take {
			take = room
		}
	}

	now := time.Now().UTC()
	vtArg := interface{}(nil)
	if vtOverride != nil {
		vtArg = *vtOverride
	}

	const q = `
WITH candidates AS (
  SELECT j.id
  FROM jobs j
  WHERE j.tenant_id = $1
    AND j.queue_name = $2
    AND (
      (
        j.status = 'pending'
        AND j.available_at <= $3
        AND (
          COALESCE(j.depends_on, '{}'::uuid[]) = '{}'::uuid[]
          OR NOT EXISTS (
            SELECT 1 FROM unnest(COALESCE(j.depends_on, '{}'::uuid[])) AS parent_id
            WHERE NOT EXISTS (
              SELECT 1 FROM jobs parent
              WHERE parent.id = parent_id
                AND parent.tenant_id = j.tenant_id
                AND parent.status = 'completed'
            )
          )
        )
      )
      OR (
        j.status = 'leased'
        AND j.lease_expires_at < $3
        AND j.attempts < j.max_attempts
      )
    )
  ORDER BY j.priority DESC, j.created_at ASC
  FOR UPDATE OF j SKIP LOCKED
  LIMIT $4
)
UPDATE jobs j
SET status = 'leased',
    leased_by = $5,
    lease_token = gen_random_uuid(),
    lease_expires_at = $3 + make_interval(secs => LEAST(
      COALESCE($6::int, j.visibility_timeout_seconds),
      COALESCE(j.max_runtime_seconds, 3600)
    )),
    lease_started_at = $3,
    attempts = j.attempts + 1,
    visibility_timeout_seconds = COALESCE($6::int, j.visibility_timeout_seconds),
    updated_at = $3,
    last_error = NULL
FROM candidates c
WHERE j.id = c.id
RETURNING
  j.id, j.tenant_id, j.queue_name, j.payload, j.status, j.priority, j.attempts, j.max_attempts,
  j.visibility_timeout_seconds, j.max_runtime_seconds,
  j.retry_backoff_base_seconds, j.retry_backoff_max_seconds,
  j.available_at, j.dedupe_key, j.schedule_id, j.depends_on,
  j.leased_by, j.lease_token, j.lease_expires_at, j.lease_started_at,
  j.created_at, j.updated_at, j.completed_at, j.result, j.last_error
`
	// When at cap, only reclaim path: filter pending out by setting take on expired only.
	// Simplified: if at cap, use reclaim-only query.
	var rows pgx.Rows
	if maxConc != nil && leasedCount >= *maxConc {
		const reclaim = `
WITH candidates AS (
  SELECT j.id FROM jobs j
  WHERE j.tenant_id = $1 AND j.queue_name = $2
    AND j.status = 'leased' AND j.lease_expires_at < $3 AND j.attempts < j.max_attempts
  ORDER BY j.priority DESC, j.created_at ASC
  FOR UPDATE OF j SKIP LOCKED
  LIMIT $4
)
UPDATE jobs j
SET status = 'leased',
    leased_by = $5,
    lease_token = gen_random_uuid(),
    lease_expires_at = $3 + make_interval(secs => LEAST(
      COALESCE($6::int, j.visibility_timeout_seconds),
      COALESCE(j.max_runtime_seconds, 3600)
    )),
    lease_started_at = $3,
    attempts = j.attempts + 1,
    visibility_timeout_seconds = COALESCE($6::int, j.visibility_timeout_seconds),
    updated_at = $3,
    last_error = NULL
FROM candidates c
WHERE j.id = c.id
RETURNING
  j.id, j.tenant_id, j.queue_name, j.payload, j.status, j.priority, j.attempts, j.max_attempts,
  j.visibility_timeout_seconds, j.max_runtime_seconds,
  j.retry_backoff_base_seconds, j.retry_backoff_max_seconds,
  j.available_at, j.dedupe_key, j.schedule_id, j.depends_on,
  j.leased_by, j.lease_token, j.lease_expires_at, j.lease_started_at,
  j.created_at, j.updated_at, j.completed_at, j.result, j.last_error
`
		rows, err = tx.Query(ctx, reclaim, tenantID, queueName, now, take, workerID, vtArg)
	} else {
		rows, err = tx.Query(ctx, q, tenantID, queueName, now, take, workerID, vtArg)
	}
	if err != nil {
		return nil, err
	}
	jobs, err := collectJobs(rows)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return jobs, nil
}

func deadLetterExhausted(ctx context.Context, tx pgx.Tx, tenantID *string) error {
	now := time.Now().UTC()
	if tenantID != nil {
		_, err := tx.Exec(ctx, `
UPDATE jobs
SET status = 'dead',
    leased_by = NULL,
    lease_token = NULL,
    lease_expires_at = NULL,
    lease_started_at = NULL,
    last_error = COALESCE(last_error, 'exceeded max_attempts after lease expiry'),
    updated_at = $1
WHERE tenant_id = $2
  AND status = 'leased'
  AND lease_expires_at < $1
  AND attempts >= max_attempts
`, now, *tenantID)
		return err
	}
	_, err := tx.Exec(ctx, `
UPDATE jobs
SET status = 'dead',
    leased_by = NULL,
    lease_token = NULL,
    lease_expires_at = NULL,
    lease_started_at = NULL,
    last_error = COALESCE(last_error, 'exceeded max_attempts after lease expiry'),
    updated_at = $1
WHERE status = 'leased'
  AND lease_expires_at < $1
  AND attempts >= max_attempts
`, now)
	return err
}

func collectJobs(rows pgx.Rows) ([]map[string]any, error) {
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		m, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func scanJob(row pgx.Row) (map[string]any, error) {
	var (
		id, leaseToken, scheduleID                                          *uuid.UUID
		idVal                                                               uuid.UUID
		tenantID, queueName, status, leasedBy, lastError, dedupeKey         *string
		tenant, queue, st                                                   string
		payload                                                             []byte
		priority, attempts, maxAttempts, vt, maxRT, bb, bm                  int
		availableAt, createdAt, updatedAt                                   time.Time
		leaseExpires, leaseStarted, completedAt                             *time.Time
		dependsOn                                                           []uuid.UUID
		result                                                              []byte
	)
	err := row.Scan(
		&idVal, &tenant, &queue, &payload, &st, &priority, &attempts, &maxAttempts,
		&vt, &maxRT, &bb, &bm,
		&availableAt, &dedupeKey, &scheduleID, &dependsOn,
		&leasedBy, &leaseToken, &leaseExpires, &leaseStarted,
		&createdAt, &updatedAt, &completedAt, &result, &lastError,
	)
	if err != nil {
		return nil, err
	}
	id = &idVal
	tenantID = &tenant
	queueName = &queue
	status = &st
	_ = id
	var payloadObj any
	if len(payload) > 0 {
		_ = json.Unmarshal(payload, &payloadObj)
	}
	if payloadObj == nil {
		payloadObj = map[string]any{}
	}
	var resultObj any
	if len(result) > 0 {
		_ = json.Unmarshal(result, &resultObj)
	}
	deps := make([]string, 0, len(dependsOn))
	for _, d := range dependsOn {
		deps = append(deps, d.String())
	}
	m := map[string]any{
		"id":                           idVal.String(),
		"tenant_id":                    *tenantID,
		"queue_name":                   *queueName,
		"payload":                      payloadObj,
		"status":                       *status,
		"priority":                     priority,
		"attempts":                     attempts,
		"max_attempts":                 maxAttempts,
		"visibility_timeout_seconds":   vt,
		"max_runtime_seconds":          maxRT,
		"retry_backoff_base_seconds":   bb,
		"retry_backoff_max_seconds":    bm,
		"available_at":                 availableAt.UTC().Format(time.RFC3339Nano),
		"dedupe_key":                   dedupeKey,
		"schedule_id":                  uuidPtr(scheduleID),
		"depends_on":                   deps,
		"leased_by":                    derefStr(leasedBy),
		"lease_token":                  uuidPtr(leaseToken),
		"lease_expires_at":             timePtr(leaseExpires),
		"lease_started_at":             timePtr(leaseStarted),
		"created_at":                   createdAt.UTC().Format(time.RFC3339Nano),
		"updated_at":                   updatedAt.UTC().Format(time.RFC3339Nano),
		"completed_at":                 timePtr(completedAt),
		"result":                       resultObj,
		"last_error":                   derefStr(lastError),
	}
	return m, nil
}

func (s *Server) handleJobPath(w http.ResponseWriter, r *http.Request) {
	// /v1/jobs/{id}/{action}
	path := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 {
		writeJSON(w, 400, map[string]any{"error": "invalid job path"})
		return
	}
	jobID, err := uuid.Parse(parts[0])
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": "invalid job id"})
		return
	}
	action := parts[1]
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		TenantID   string          `json:"tenant_id"`
		WorkerID   string          `json:"worker_id"`
		LeaseToken string          `json:"lease_token"`
		Result     json.RawMessage `json:"result"`
		Error      *string         `json:"error"`
		Requeue    *bool           `json:"requeue"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": "invalid json"})
		return
	}
	if body.TenantID == "" || body.WorkerID == "" || body.LeaseToken == "" {
		writeJSON(w, 400, map[string]any{"error": "tenant_id, worker_id, lease_token required"})
		return
	}
	token, err := uuid.Parse(body.LeaseToken)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": "invalid lease_token"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	var job map[string]any
	switch action {
	case "heartbeat":
		job, err = s.heartbeat(ctx, jobID, body.TenantID, body.WorkerID, token, nil)
	case "ack":
		job, err = s.ack(ctx, jobID, body.TenantID, body.WorkerID, token, body.Result)
	case "nack":
		requeue := true
		if body.Requeue != nil {
			requeue = *body.Requeue
		}
		job, err = s.nack(ctx, jobID, body.TenantID, body.WorkerID, token, body.Error, requeue)
	default:
		writeJSON(w, 404, map[string]any{"error": "unknown action"})
		return
	}
	if err != nil {
		writeJSON(w, 409, map[string]any{"error": err.Error(), "code": "lease_error"})
		return
	}
	writeJSON(w, 200, map[string]any{"job": job})
}

func (s *Server) heartbeat(ctx context.Context, jobID uuid.UUID, tenant, worker string, token uuid.UUID, extend *int) (map[string]any, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	now := time.Now().UTC()
	row := tx.QueryRow(ctx, `
SELECT id, tenant_id, queue_name, payload, status, priority, attempts, max_attempts,
  visibility_timeout_seconds, max_runtime_seconds,
  retry_backoff_base_seconds, retry_backoff_max_seconds,
  available_at, dedupe_key, schedule_id, depends_on,
  leased_by, lease_token, lease_expires_at, lease_started_at,
  created_at, updated_at, completed_at, result, last_error
FROM jobs WHERE id=$1 AND tenant_id=$2 FOR UPDATE
`, jobID, tenant)
	job, err := scanJob(row)
	if err != nil {
		return nil, fmt.Errorf("job not found")
	}
	if err := assertHolder(job, worker, token); err != nil {
		return nil, err
	}
	// Max runtime hard stop.
	if ls, ok := parseTime(job["lease_started_at"]); ok {
		maxRT := intVal(job["max_runtime_seconds"], 3600)
		if now.Sub(ls) >= time.Duration(maxRT)*time.Second {
			// Force release to pending/dead simplified as pending requeue.
			_, _ = tx.Exec(ctx, `
UPDATE jobs SET status='pending', leased_by=NULL, lease_token=NULL,
  lease_expires_at=NULL, lease_started_at=NULL, last_error='max runtime exceeded',
  available_at=$1, updated_at=$1
WHERE id=$2 AND status='leased'`, now, jobID)
			_ = tx.Commit(ctx)
			return nil, fmt.Errorf("job %s max runtime exceeded", jobID)
		}
	}
	vt := intVal(job["visibility_timeout_seconds"], 30)
	if extend != nil {
		vt = *extend
	}
	newExp := now.Add(time.Duration(vt) * time.Second)
	if ls, ok := parseTime(job["lease_started_at"]); ok {
		maxRT := intVal(job["max_runtime_seconds"], 3600)
		hard := ls.Add(time.Duration(maxRT) * time.Second)
		if newExp.After(hard) {
			newExp = hard
		}
	}
	row = tx.QueryRow(ctx, `
UPDATE jobs SET lease_expires_at=$1, updated_at=$2
WHERE id=$3 AND tenant_id=$4 AND lease_token=$5 AND status='leased'
RETURNING id, tenant_id, queue_name, payload, status, priority, attempts, max_attempts,
  visibility_timeout_seconds, max_runtime_seconds,
  retry_backoff_base_seconds, retry_backoff_max_seconds,
  available_at, dedupe_key, schedule_id, depends_on,
  leased_by, lease_token, lease_expires_at, lease_started_at,
  created_at, updated_at, completed_at, result, last_error
`, newExp, now, jobID, tenant, token)
	out, err := scanJob(row)
	if err != nil {
		return nil, fmt.Errorf("lease lost during heartbeat")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) ack(ctx context.Context, jobID uuid.UUID, tenant, worker string, token uuid.UUID, result json.RawMessage) (map[string]any, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	now := time.Now().UTC()
	row := tx.QueryRow(ctx, `
SELECT id, tenant_id, queue_name, payload, status, priority, attempts, max_attempts,
  visibility_timeout_seconds, max_runtime_seconds,
  retry_backoff_base_seconds, retry_backoff_max_seconds,
  available_at, dedupe_key, schedule_id, depends_on,
  leased_by, lease_token, lease_expires_at, lease_started_at,
  created_at, updated_at, completed_at, result, last_error
FROM jobs WHERE id=$1 AND tenant_id=$2 FOR UPDATE
`, jobID, tenant)
	job, err := scanJob(row)
	if err != nil {
		return nil, fmt.Errorf("job not found")
	}
	if err := assertHolder(job, worker, token); err != nil {
		return nil, err
	}
	if exp, ok := parseTime(job["lease_expires_at"]); ok && now.After(exp) {
		return nil, fmt.Errorf("lease expired")
	}
	var resultArg interface{}
	if len(result) > 0 && string(result) != "null" {
		resultArg = []byte(result)
	}
	row = tx.QueryRow(ctx, `
UPDATE jobs SET status='completed', leased_by=NULL, lease_token=NULL,
  lease_expires_at=NULL, lease_started_at=NULL, completed_at=$1, result=$2, updated_at=$1
WHERE id=$3 AND tenant_id=$4 AND lease_token=$5 AND status='leased'
RETURNING id, tenant_id, queue_name, payload, status, priority, attempts, max_attempts,
  visibility_timeout_seconds, max_runtime_seconds,
  retry_backoff_base_seconds, retry_backoff_max_seconds,
  available_at, dedupe_key, schedule_id, depends_on,
  leased_by, lease_token, lease_expires_at, lease_started_at,
  created_at, updated_at, completed_at, result, last_error
`, now, resultArg, jobID, tenant, token)
	out, err := scanJob(row)
	if err != nil {
		return nil, fmt.Errorf("lease lost during ack")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) nack(ctx context.Context, jobID uuid.UUID, tenant, worker string, token uuid.UUID, errMsg *string, requeue bool) (map[string]any, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	now := time.Now().UTC()
	row := tx.QueryRow(ctx, `
SELECT id, tenant_id, queue_name, payload, status, priority, attempts, max_attempts,
  visibility_timeout_seconds, max_runtime_seconds,
  retry_backoff_base_seconds, retry_backoff_max_seconds,
  available_at, dedupe_key, schedule_id, depends_on,
  leased_by, lease_token, lease_expires_at, lease_started_at,
  created_at, updated_at, completed_at, result, last_error
FROM jobs WHERE id=$1 AND tenant_id=$2 FOR UPDATE
`, jobID, tenant)
	job, err := scanJob(row)
	if err != nil {
		return nil, fmt.Errorf("job not found")
	}
	if err := assertHolder(job, worker, token); err != nil {
		return nil, err
	}
	attempts := intVal(job["attempts"], 0)
	maxAttempts := intVal(job["max_attempts"], 3)
	status := "failed"
	nextAvail := now
	if requeue && attempts < maxAttempts {
		status = "pending"
		base := intVal(job["retry_backoff_base_seconds"], 1)
		capS := intVal(job["retry_backoff_max_seconds"], 300)
		// base * 2^(attempts-1)
		delay := base
		for i := 1; i < attempts; i++ {
			delay *= 2
			if delay > capS {
				delay = capS
				break
			}
		}
		if delay > capS {
			delay = capS
		}
		nextAvail = now.Add(time.Duration(delay) * time.Second)
	} else if attempts >= maxAttempts {
		status = "dead"
	}
	row = tx.QueryRow(ctx, `
UPDATE jobs SET status=$1, leased_by=NULL, lease_token=NULL,
  lease_expires_at=NULL, lease_started_at=NULL, available_at=$2, last_error=$3, updated_at=$4
WHERE id=$5 AND tenant_id=$6 AND lease_token=$7 AND status='leased'
RETURNING id, tenant_id, queue_name, payload, status, priority, attempts, max_attempts,
  visibility_timeout_seconds, max_runtime_seconds,
  retry_backoff_base_seconds, retry_backoff_max_seconds,
  available_at, dedupe_key, schedule_id, depends_on,
  leased_by, lease_token, lease_expires_at, lease_started_at,
  created_at, updated_at, completed_at, result, last_error
`, status, nextAvail, errMsg, now, jobID, tenant, token)
	out, err := scanJob(row)
	if err != nil {
		return nil, fmt.Errorf("lease lost during nack")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		TenantID                 string         `json:"tenant_id"`
		Payload                  map[string]any `json:"payload"`
		QueueName                string         `json:"queue_name"`
		Priority                 int            `json:"priority"`
		MaxAttempts              int            `json:"max_attempts"`
		VisibilityTimeoutSeconds int            `json:"visibility_timeout_seconds"`
		MaxRuntimeSeconds        int            `json:"max_runtime_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": "invalid json"})
		return
	}
	if body.TenantID == "" {
		writeJSON(w, 400, map[string]any{"error": "tenant_id required"})
		return
	}
	if body.QueueName == "" {
		body.QueueName = "default"
	}
	if body.MaxAttempts < 1 {
		body.MaxAttempts = 3
	}
	if body.VisibilityTimeoutSeconds < 1 {
		body.VisibilityTimeoutSeconds = 30
	}
	if body.MaxRuntimeSeconds < 1 {
		body.MaxRuntimeSeconds = 3600
	}
	if body.Payload == nil {
		body.Payload = map[string]any{}
	}
	// Reject enqueue while offboarding.
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	var offboarding bool
	err := s.pool.QueryRow(ctx, `SELECT offboarding FROM tenant_settings WHERE tenant_id=$1`, body.TenantID).Scan(&offboarding)
	if err == nil && offboarding {
		writeJSON(w, 400, map[string]any{"error": fmt.Sprintf("tenant %q is offboarding; enqueue is rejected", body.TenantID)})
		return
	}
	payload, _ := json.Marshal(body.Payload)
	now := time.Now().UTC()
	id := uuid.New()
	row := s.pool.QueryRow(ctx, `
INSERT INTO jobs (
  id, tenant_id, queue_name, payload, status, priority, attempts, max_attempts,
  visibility_timeout_seconds, max_runtime_seconds,
  retry_backoff_base_seconds, retry_backoff_max_seconds,
  available_at, depends_on, created_at, updated_at
) VALUES (
  $1,$2,$3,$4,'pending',$5,0,$6,
  $7,$8,
  1,300,
  $9,'{}',$9,$9
)
RETURNING id, tenant_id, queue_name, payload, status, priority, attempts, max_attempts,
  visibility_timeout_seconds, max_runtime_seconds,
  retry_backoff_base_seconds, retry_backoff_max_seconds,
  available_at, dedupe_key, schedule_id, depends_on,
  leased_by, lease_token, lease_expires_at, lease_started_at,
  created_at, updated_at, completed_at, result, last_error
`, id, body.TenantID, body.QueueName, payload, body.Priority, body.MaxAttempts,
		body.VisibilityTimeoutSeconds, body.MaxRuntimeSeconds, now)
	job, err := scanJob(row)
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 201, map[string]any{"job": job})
}

func (s *Server) handleBacklog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	var pending, leased, failed, dead, completed, cancelled int
	err := s.pool.QueryRow(ctx, `
SELECT
  COUNT(*) FILTER (WHERE status='pending')::int,
  COUNT(*) FILTER (WHERE status='leased')::int,
  COUNT(*) FILTER (WHERE status='failed')::int,
  COUNT(*) FILTER (WHERE status='dead')::int,
  COUNT(*) FILTER (WHERE status='completed')::int,
  COUNT(*) FILTER (WHERE status='cancelled')::int
FROM jobs
`).Scan(&pending, &leased, &failed, &dead, &completed, &cancelled)
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{
		"jobs": map[string]int{
			"pending": pending, "leased": leased, "failed": failed,
			"dead": dead, "completed": completed, "cancelled": cancelled,
		},
		"webhooks": map[string]int{"pending": 0, "delivering": 0, "dead": 0, "succeeded": 0},
	})
}

func (s *Server) handleTenantPath(w http.ResponseWriter, r *http.Request) {
	// /v1/tenants/{id}/pause|resume
	path := strings.TrimPrefix(r.URL.Path, "/v1/tenants/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || r.Method != http.MethodPost {
		writeJSON(w, 404, map[string]any{"error": "unknown tenant route"})
		return
	}
	tenantID, action := parts[0], parts[1]
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	now := time.Now().UTC()
	switch action {
	case "pause":
		_, err := s.pool.Exec(ctx, `
INSERT INTO tenant_settings (tenant_id, max_concurrent_leases, paused, offboarding, updated_at)
VALUES ($1, NULL, TRUE, FALSE, $2)
ON CONFLICT (tenant_id) DO UPDATE SET paused=TRUE, updated_at=EXCLUDED.updated_at
`, tenantID, now)
		if err != nil {
			writeJSON(w, 500, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"tenant_id": tenantID, "paused": true})
	case "resume":
		var offboarding bool
		err := s.pool.QueryRow(ctx, `SELECT offboarding FROM tenant_settings WHERE tenant_id=$1`, tenantID).Scan(&offboarding)
		if err == nil && offboarding {
			writeJSON(w, 400, map[string]any{"error": fmt.Sprintf("tenant %q is offboarding and cannot be resumed", tenantID)})
			return
		}
		_, err = s.pool.Exec(ctx, `
INSERT INTO tenant_settings (tenant_id, max_concurrent_leases, paused, offboarding, updated_at)
VALUES ($1, NULL, FALSE, FALSE, $2)
ON CONFLICT (tenant_id) DO UPDATE SET paused=FALSE, updated_at=EXCLUDED.updated_at
`, tenantID, now)
		if err != nil {
			writeJSON(w, 500, map[string]any{"error": err.Error()})
			return
		}
		// Drop empty settings rows.
		_, _ = s.pool.Exec(ctx, `
DELETE FROM tenant_settings
WHERE tenant_id=$1 AND max_concurrent_leases IS NULL AND paused=FALSE AND offboarding=FALSE
`, tenantID)
		writeJSON(w, 200, map[string]any{"tenant_id": tenantID, "paused": false})
	default:
		writeJSON(w, 404, map[string]any{"error": "unknown tenant action"})
	}
}

func ensureLeaseIndexes(ctx context.Context, pool *pgxpool.Pool) error {
	// Hot-path indexes for fair multi-tenant claim under large ready sets.
	stmts := []string{
		`CREATE INDEX IF NOT EXISTS idx_jobs_fair_ready_v2
           ON jobs (queue_name, priority DESC, created_at ASC)
           WHERE status = 'pending'`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_fair_reclaim_v2
           ON jobs (queue_name, priority DESC, created_at ASC)
           WHERE status = 'leased'`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_tenant_queue_pending
           ON jobs (tenant_id, queue_name, priority DESC, created_at ASC)
           WHERE status = 'pending'`,
		`CREATE INDEX IF NOT EXISTS idx_tenant_settings_paused
           ON tenant_settings (tenant_id) WHERE paused = TRUE`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func assertHolder(job map[string]any, worker string, token uuid.UUID) error {
	if asString(job["status"]) != "leased" {
		return fmt.Errorf("job not leased")
	}
	if asString(job["leased_by"]) != worker {
		return fmt.Errorf("wrong worker")
	}
	lt := asString(job["lease_token"])
	if lt != token.String() {
		return fmt.Errorf("lease token mismatch")
	}
	return nil
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case *string:
		if t == nil {
			return ""
		}
		return *t
	case fmt.Stringer:
		return t.String()
	default:
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if r.URL.Path == "/v1/lease" || strings.HasSuffix(r.URL.Path, "/ack") {
			// Keep noise low under load; debug only for slow calls.
			if d := time.Since(start); d > 200*time.Millisecond {
				log.Printf("%s %s %s", r.Method, r.URL.Path, d)
			}
		}
	})
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func derefStr(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

func uuidPtr(u *uuid.UUID) any {
	if u == nil {
		return nil
	}
	return u.String()
}

func timePtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok || s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return time.Time{}, false
		}
	}
	return t, true
}

func intVal(v any, def int) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return def
	}
}
