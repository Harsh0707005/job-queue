"""Live tenant export: consistent snapshot while the source keeps running."""

from __future__ import annotations

import base64
from datetime import datetime, time
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import TenantSnapshot
from job_queue.queue._common import (
    RestoreError,
    _JOB_COLUMNS,
    _SCHEDULE_COLUMNS,
    _as_utc,
    _utcnow,
)

SNAPSHOT_FORMAT_VERSION = 1

CHUNK = 200

# Restore order: keys before ciphertext, parents before children, endpoints
# before deliveries, runs before steps.
SNAPSHOT_KINDS: tuple[str, ...] = (
    "payload_keys",
    "payload_key_rotations",
    "tenant_settings",
    "tenant_fairness",
    "duration_estimates",
    "job_worker_affinity",
    "schedules",
    "schedule_blackouts",
    "workflow_runs",
    "jobs",
    "schedule_generations",
    "job_dag_batches",
    "workflow_step_runs",
    "job_side_effects",
    "job_audit_events",
    "webhook_endpoints",
    "webhook_deliveries",
    "alert_rules",
    "alert_events",
    "queue_metric_buckets",
    "queue_metric_snapshots",
    "tenant_usage_buckets",
)

_KIND_SQL: dict[str, tuple[str, str]] = {
    "payload_keys": (
        "tenant_id, key_id, wrapped_dek, wrap_nonce, kek_id, status, "
        "created_at, retired_at",
        "tenant_payload_keys",
    ),
    "payload_key_rotations": (
        "id, tenant_id, from_key_id, to_key_id, status, jobs_rewrapped, "
        "schedules_rewrapped, created_at, updated_at, completed_at",
        "payload_key_rotations",
    ),
    "tenant_settings": (
        "tenant_id, max_concurrent_leases, max_pending_jobs, paused, "
        "offboarding, enqueue_rate_per_sec, enqueue_burst, enqueue_tokens, "
        "enqueue_tokens_at, lease_rate_per_sec, lease_burst, lease_tokens, "
        "lease_tokens_at, completed_retention_seconds, dead_retention_seconds, "
        "default_job_ttl_seconds, default_expiry_action, "
        "default_max_worker_hops, default_quarantine_action, "
        "spend_enqueue_limit, spend_runtime_ms_limit, spend_window_seconds, "
        "breaker_failure_threshold, breaker_window_seconds, "
        "breaker_cooldown_seconds, "
        "lease_weight, draining_queues, "
        "prefer_last_worker_queues, prefer_last_worker_hold_seconds, "
        "updated_at, restoring",
        "tenant_settings",
    ),
    "tenant_fairness": ("tenant_id, last_lease_at, fair_virtual", "tenant_fairness"),
    "duration_estimates": (
        "tenant_id, queue_name, kind, ewma_seconds, samples, updated_at",
        "duration_estimates",
    ),
    "job_worker_affinity": (
        "tenant_id, queue_name, kind, last_worker_id, last_success_at",
        "job_worker_affinity",
    ),
    "schedules": (_SCHEDULE_COLUMNS.strip().replace("\n", " "), "schedules"),
    "schedule_generations": (
        "schedule_id, iteration, tenant_id, status, payload, payload_key_id, "
        "source_job_ids, created_at, updated_at",
        "schedule_generations",
    ),
    "schedule_blackouts": (
        "id, tenant_id, schedule_id, kind, starts_at, ends_at, "
        "daily_start, daily_end, timezone, reason, created_at, updated_at",
        "schedule_blackouts",
    ),
    "workflow_runs": (
        "id, tenant_id, name, status, steps_def, current_index, "
        "fail_step_index, fail_reason, max_compensate_retries, created_at, "
        "updated_at, completed_at, payload_key_id",
        "workflow_runs",
    ),
    "jobs": (_JOB_COLUMNS.strip().replace("\n", " "), "jobs"),
    "job_dag_batches": (
        "id, tenant_id, queue_name, batch_key, job_ids, refs, created_at, "
        "schedule_id",
        "job_dag_batches",
    ),
    "workflow_step_runs": (
        "id, run_id, tenant_id, step_index, name, direction, status, job_id, "
        "result, error, idempotency_key, retry_count, started_at, completed_at, "
        "created_at, updated_at",
        "workflow_step_runs",
    ),
    "job_side_effects": (
        "job_id, effect_key, tenant_id, status, idempotency_key, result, "
        "started_by, started_at, completed_by, lease_token, created_at, "
        "completed_at",
        "job_side_effects",
    ),
    "job_audit_events": (
        "event_id, tenant_id, job_id, seq, event_type, from_status, to_status, "
        "actor, detail, created_at",
        "job_audit_events",
    ),
    "webhook_endpoints": (
        "id, tenant_id, url, secret, events, enabled, max_attempts, "
        "backoff_base_seconds, backoff_max_seconds, created_at, updated_at",
        "webhook_endpoints",
    ),
    "webhook_deliveries": (
        "id, endpoint_id, tenant_id, job_id, event, payload, status, attempts, "
        "max_attempts, backoff_base_seconds, backoff_max_seconds, "
        "next_attempt_at, delivering_until, last_error, last_status_code, "
        "created_at, updated_at, completed_at, signing_secret, payload_key_id",
        "webhook_deliveries",
    ),
    "alert_rules": (
        "id, name, enabled, scope, tenant_id, worker_id, metric, comparator, "
        "threshold, window_seconds, cooldown_seconds, last_fired_at, "
        "muted_until, auto_pause_tenant, recovery_threshold, "
        "recovery_evaluations, healthy_interval_seconds, healthy_streak, "
        "last_healthy_at, created_at, updated_at",
        "alert_rules",
    ),
    "alert_events": (
        "id, rule_id, rule_name, metric, scope, tenant_id, worker_id, value, "
        "threshold, comparator, window_seconds, detail, fired_at, resolved_at, "
        "resolved_by, resolution_note",
        "alert_events",
    ),
    "queue_metric_buckets": (
        "bucket_start, tenant_id, metric, count, sum_ms",
        "queue_metric_buckets",
    ),
    "queue_metric_snapshots": (
        "sampled_at, tenant_id, pending, leased, claimable, failed, dead, "
        "completed, cancelled, awaiting_approval, awaiting_children, "
        "queue_lag_seconds, avg_pending_age_seconds, max_pending_age_seconds, "
        "webhook_pending, webhook_delivering, webhook_succeeded, webhook_dead, "
        "schedules_enabled, schedules_due",
        "queue_metric_snapshots",
    ),
    "tenant_usage_buckets": (
        "bucket_start, tenant_id, enqueues, leases, acks, nacks, fails, "
        "dead_letters, cancels, releases, preemptions, schedule_fires, "
        "webhook_successes, webhook_failures, spawn_children, runtime_ms, "
        "wait_ms",
        "tenant_usage_buckets",
    ),
}

# Restore must find these empty on the dest. tenant_settings is exempt:
# empty dests commonly already have limits/pause rows, and restore itself
# writes paused+restoring before applying snapshot blobs.
RESTORE_RESIDUE_TABLES: tuple[str, ...] = tuple(
    dict.fromkeys(
        table
        for kind, (_cols, table) in _KIND_SQL.items()
        if kind != "tenant_settings"
    )
)


def tenant_residue_tables(conn: Any, tenant_id: str) -> list[str]:
    """Return snapshot tables that still have rows for *tenant_id*."""
    leftover: list[str] = []
    for table in RESTORE_RESIDUE_TABLES:
        row = conn.execute(
            f"SELECT 1 FROM {table} WHERE tenant_id = %s LIMIT 1",
            (tenant_id,),
        ).fetchone()
        if row is not None:
            leftover.append(table)
    return leftover


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    if isinstance(value, time):
        return value.strftime("%H:%M:%S")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, memoryview):
        value = bytes(value)
    if isinstance(value, (bytes, bytearray)):
        return {"$b64": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, float) and (value != value):  # NaN
        return None
    return value


def snapshot_from_row(row: dict[str, Any]) -> TenantSnapshot:
    counts = row.get("counts") or {}
    if isinstance(counts, str):
        import json

        counts = json.loads(counts)
    return TenantSnapshot(
        id=row["id"],
        source_tenant_id=str(row["source_tenant_id"]),
        status=str(row["status"]),
        counts={str(k): int(v) for k, v in dict(counts).items()},
        created_at=row["created_at"],
        completed_at=row.get("completed_at"),
        error=row.get("error"),
        format_version=int(row.get("format_version") or SNAPSHOT_FORMAT_VERSION),
    )


class TenantsExportMixin:
    def export_tenant(self, tenant_id: str) -> TenantSnapshot:
        """Capture a consistent snapshot of *tenant_id* without pausing it.

        Uses ``REPEATABLE READ`` so in-flight workers keep running. Ciphertext
        is copied as-is (no decrypt). The snapshot lives in Postgres so a
        later restore can resume after a coordinator crash.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        sid = uuid4()
        with transaction(self._pool) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            conn.execute(
                """
                INSERT INTO tenant_snapshots (
                    id, source_tenant_id, format_version, status, counts,
                    created_at
                ) VALUES (
                    %s, %s, %s, 'capturing', '{}'::jsonb, %s
                )
                """,
                (sid, tenant_id, SNAPSHOT_FORMAT_VERSION, now),
            )
            counts: dict[str, int] = {}
            try:
                for kind in SNAPSHOT_KINDS:
                    cols, table = _KIND_SQL[kind]
                    n = self._capture_kind(
                        conn,
                        snapshot_id=sid,
                        kind=kind,
                        table=table,
                        cols=cols,
                        tenant_id=tenant_id,
                    )
                    counts[kind] = n
            except Exception as exc:  # noqa: BLE001
                conn.execute(
                    """
                    UPDATE tenant_snapshots
                    SET status = 'failed', error = %s, completed_at = %s
                    WHERE id = %s
                    """,
                    (str(exc)[:2000], _utcnow(), sid),
                )
                raise RestoreError(f"export failed for tenant {tenant_id!r}: {exc}") from exc
            conn.execute(
                """
                UPDATE tenant_snapshots
                SET status = 'ready',
                    counts = %s,
                    completed_at = %s
                WHERE id = %s
                """,
                (Jsonb(counts), _utcnow(), sid),
            )
            row = conn.execute(
                """
                SELECT id, source_tenant_id, format_version, status, counts,
                       error, created_at, completed_at
                FROM tenant_snapshots WHERE id = %s
                """,
                (sid,),
            ).fetchone()
            assert row is not None
            return snapshot_from_row(row)

    def get_tenant_snapshot(self, snapshot_id: UUID) -> TenantSnapshot | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT id, source_tenant_id, format_version, status, counts,
                       error, created_at, completed_at
                FROM tenant_snapshots WHERE id = %s
                """,
                (snapshot_id,),
            ).fetchone()
            return snapshot_from_row(row) if row else None

    def _capture_kind(
        self,
        conn: Any,
        *,
        snapshot_id: UUID,
        kind: str,
        table: str,
        cols: str,
        tenant_id: str,
    ) -> int:
        if kind == "alert_rules":
            where = "tenant_id = %s OR (scope = 'tenant' AND tenant_id = %s)"
            params: tuple[Any, ...] = (tenant_id, tenant_id)
        else:
            where = "tenant_id = %s"
            params = (tenant_id,)
        order = {
            "payload_keys": "key_id ASC",
            "duration_estimates": "queue_name ASC, kind ASC",
            "job_worker_affinity": "queue_name ASC, kind ASC",
            "tenant_settings": "tenant_id ASC",
            "tenant_fairness": "tenant_id ASC",
            "job_side_effects": "job_id ASC, effect_key ASC",
            "queue_metric_buckets": "bucket_start ASC, metric ASC",
            "queue_metric_snapshots": "sampled_at ASC",
            "tenant_usage_buckets": "bucket_start ASC",
            "job_dag_batches": "created_at ASC, id ASC",
            "job_audit_events": "event_id ASC",
        }.get(kind, "id ASC")
        total = 0
        seq = 0
        offset = 0
        while True:
            rows = conn.execute(
                f"""
                SELECT {cols}
                FROM {table}
                WHERE {where}
                ORDER BY {order}
                LIMIT %s OFFSET %s
                """,
                params + (CHUNK, offset),
            ).fetchall()
            if not rows:
                break
            blob = [_jsonable(dict(r)) for r in rows]
            conn.execute(
                """
                INSERT INTO tenant_snapshot_blobs (
                    snapshot_id, kind, seq, row_count, rows
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (snapshot_id, kind, seq, len(blob), Jsonb(blob)),
            )
            total += len(rows)
            seq += 1
            offset += len(rows)
            if len(rows) < CHUNK:
                break
        return total
