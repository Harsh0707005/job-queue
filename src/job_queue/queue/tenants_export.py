"""Live tenant export: consistent snapshot while the source keeps running."""

from __future__ import annotations

import base64
import hashlib
import json
from datetime import datetime, time
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.crypto import is_blob_pointer
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
    "payload_blobs",
    "payload_key_rotations",
    "tenant_settings",
    "tenant_fairness",
    "duration_estimates",
    "job_worker_affinity",
    "schedules",
    "schedule_blackouts",
    "workflow_runs",
    "workflow_signals",
    "jobs",
    "schedule_generations",
    "job_dag_batches",
    "workflow_step_runs",
    "workflow_activities",
    "job_side_effects",
    "job_audit_events",
    "webhook_endpoints",
    "webhook_deliveries",
    "inbound_triggers",
    "inbound_trigger_nonces",
    "inbound_job_callbacks",
    "alert_rules",
    "alert_events",
    "queue_metric_buckets",
    "queue_metric_snapshots",
    "tenant_usage_buckets",
)

# Relocate also carries prior export/restore catalog so dest keeps backup
# history. These are not part of a normal export_tenant snapshot (that would
# nest the snapshot inside itself).
RELOCATE_EXTRA_KINDS: tuple[str, ...] = (
    "tenant_snapshots",
    "tenant_snapshot_blobs",
    "tenant_restores",
)
RELOCATE_SNAPSHOT_KINDS: tuple[str, ...] = SNAPSHOT_KINDS + RELOCATE_EXTRA_KINDS

_KIND_SQL: dict[str, tuple[str, str]] = {
    "payload_blobs": (
        "id, tenant_id, job_id, kind, sha256, nbytes, key_id, status, "
        "created_at, committed_at",
        "job_payload_blobs",
    ),
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
        "archive_after_seconds, archive_max_live_bytes, "
        "default_job_ttl_seconds, default_expiry_action, "
        "default_max_worker_hops, default_quarantine_action, "
        "spend_enqueue_limit, spend_runtime_ms_limit, spend_window_seconds, "
        "spend_window_start, spend_enqueues, spend_runtime_ms, "
        "breaker_failure_threshold, breaker_window_seconds, "
        "breaker_cooldown_seconds, breaker_failures, breaker_window_start, "
        "breaker_opened_at, "
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
        "updated_at, completed_at, payload_key_id, schedule_id, tick_iteration, "
        "deadline_at",
        "workflow_runs",
    ),
    "workflow_signals": (
        "id, run_id, tenant_id, name, payload, payload_key_id, created_at",
        "workflow_signals",
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
        "created_at, updated_at, deadline_at",
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
        "actor, detail, created_at, tenant_seq, payload_key_id",
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
        "created_at, updated_at, completed_at, signing_secret, payload_key_id, "
        "callback_url",
        "webhook_deliveries",
    ),
    "inbound_triggers": (
        "id, tenant_id, name, secret, kinds, enabled, created_at, updated_at",
        "inbound_triggers",
    ),
    "inbound_trigger_nonces": (
        "trigger_id, nonce, seen_at",
        "inbound_trigger_nonces",
    ),
    "inbound_job_callbacks": (
        "job_id, trigger_id, tenant_id, callback_url, signing_secret, created_at",
        "inbound_job_callbacks",
    ),
    "workflow_activities": (
        "id, step_run_id, job_id, run_id, tenant_id, name, url, status, "
        "timeout_seconds, heartbeat_timeout_seconds, started_at, "
        "last_heartbeat_at, heartbeat_due_at, timeout_at, completed_at, "
        "result, error, created_at, updated_at",
        "workflow_activities",
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
    "tenant_snapshots": (
        "id, source_tenant_id, format_version, status, counts, error, "
        "created_at, completed_at",
        "tenant_snapshots",
    ),
    "tenant_snapshot_blobs": (
        "snapshot_id, kind, seq, row_count, rows",
        "tenant_snapshot_blobs",
    ),
    "tenant_restores": (
        "id, snapshot_id, target_tenant_id, source_tenant_id, status, "
        "kind_index, blob_seq, blob_offset, applied, id_map, "
        "exported_settings, error, created_at, updated_at, completed_at",
        "tenant_restores",
    ),
}

# Keyset columns (unique per tenant) so pages do not OFFSET-scan prior rows.
_KIND_KEYS: dict[str, tuple[str, ...]] = {
    "payload_keys": ("key_id",),
    "payload_blobs": ("id",),
    "payload_key_rotations": ("id",),
    "tenant_settings": ("tenant_id",),
    "tenant_fairness": ("tenant_id",),
    "duration_estimates": ("queue_name", "kind"),
    "job_worker_affinity": ("queue_name", "kind"),
    "schedules": ("id",),
    "schedule_generations": ("schedule_id", "iteration"),
    "schedule_blackouts": ("id",),
    "workflow_runs": ("id",),
    "workflow_signals": ("id",),
    "jobs": ("id",),
    "job_dag_batches": ("id",),
    "workflow_step_runs": ("id",),
    "workflow_activities": ("id",),
    "job_side_effects": ("job_id", "effect_key"),
    "job_audit_events": ("event_id",),
    "webhook_endpoints": ("id",),
    "webhook_deliveries": ("id",),
    "inbound_triggers": ("id",),
    "inbound_trigger_nonces": ("trigger_id", "nonce"),
    "inbound_job_callbacks": ("job_id",),
    "alert_rules": ("id",),
    "alert_events": ("id",),
    "queue_metric_buckets": ("bucket_start", "metric"),
    "queue_metric_snapshots": ("sampled_at",),
    "tenant_usage_buckets": ("bucket_start",),
    "tenant_snapshots": ("id",),
    "tenant_snapshot_blobs": ("snapshot_id", "kind", "seq"),
    "tenant_restores": ("id",),
}

# Primary key (or unique identity) used for replica upsert / delete reconcile.
KIND_PK: dict[str, tuple[str, ...]] = {
    "payload_blobs": ("id",),
    "payload_keys": ("tenant_id", "key_id"),
    "payload_key_rotations": ("id",),
    "tenant_settings": ("tenant_id",),
    "tenant_fairness": ("tenant_id",),
    "duration_estimates": ("tenant_id", "queue_name", "kind"),
    "job_worker_affinity": ("tenant_id", "queue_name", "kind"),
    "schedules": ("id",),
    "schedule_generations": ("schedule_id", "iteration"),
    "schedule_blackouts": ("id",),
    "workflow_runs": ("id",),
    "workflow_signals": ("id",),
    "jobs": ("id",),
    "job_dag_batches": ("id",),
    "workflow_step_runs": ("id",),
    "workflow_activities": ("id",),
    "job_side_effects": ("job_id", "effect_key"),
    "job_audit_events": ("event_id",),
    "webhook_endpoints": ("id",),
    "webhook_deliveries": ("id",),
    "inbound_triggers": ("id",),
    "inbound_trigger_nonces": ("trigger_id", "nonce"),
    "inbound_job_callbacks": ("job_id",),
    "alert_rules": ("id",),
    "alert_events": ("id",),
    "queue_metric_buckets": ("bucket_start", "tenant_id", "metric"),
    "queue_metric_snapshots": ("sampled_at", "tenant_id"),
    "tenant_usage_buckets": ("bucket_start", "tenant_id"),
    "tenant_snapshots": ("id",),
    "tenant_snapshot_blobs": ("snapshot_id", "kind", "seq"),
    "tenant_restores": ("id",),
}


def _kind_key_cols(kind: str) -> tuple[str, ...]:
    return _KIND_KEYS.get(kind, ("id",))


def _keyset_from_row(kind: str, row: dict[str, Any] | None) -> tuple[Any, ...] | None:
    if not row:
        return None
    cols = _kind_key_cols(kind)
    return tuple(row.get(c) for c in cols)


def _keyset_sql(kind: str, after: Any) -> tuple[str, tuple[Any, ...]]:
    if after is None:
        return "", ()
    cols = _kind_key_cols(kind)
    values = tuple(after) if isinstance(after, (tuple, list)) else (after,)
    if len(values) != len(cols):
        return "", ()
    if len(cols) == 1:
        return f"AND {cols[0]} > %s", values
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    return f"AND ({col_list}) > ({placeholders})", values

# Restore must find these empty on the dest. tenant_settings is exempt:
# empty dests commonly already have limits/pause rows, and restore itself
# writes paused+restoring before applying snapshot blobs.
RESTORE_RESIDUE_TABLES: tuple[str, ...] = tuple(
    dict.fromkeys(
        table
        for kind, (_cols, table) in _KIND_SQL.items()
        if kind not in (
            "tenant_settings",
            "inbound_trigger_nonces",
            "tenant_snapshots",
            "tenant_snapshot_blobs",
            "tenant_restores",
        )
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
    nonce = conn.execute(
        """
        SELECT 1
        FROM inbound_trigger_nonces n
        JOIN inbound_triggers t ON t.id = n.trigger_id
        WHERE t.tenant_id = %s
        LIMIT 1
        """,
        (tenant_id,),
    ).fetchone()
    if nonce is not None:
        leftover.append("inbound_trigger_nonces")
    return leftover


def relocate_residue_tables(conn: Any, tenant_id: str) -> list[str]:
    """Residue that blocks a new relocate onto *tenant_id*.

    Unlike restore, an existing ``tenant_settings`` row is occupancy:
    dest already has configured limits/pause/spend/breaker. A dest that
    is mid-restore (``restoring``) is the in-progress copy and is not
    treated as a foreign settings row.
    """
    leftover = tenant_residue_tables(conn, tenant_id)
    snap = conn.execute(
        """
        SELECT 1 FROM tenant_snapshots
        WHERE source_tenant_id = %s
        LIMIT 1
        """,
        (tenant_id,),
    ).fetchone()
    if snap is not None:
        leftover.append("tenant_snapshots")
    rst = conn.execute(
        """
        SELECT 1 FROM tenant_restores
        WHERE source_tenant_id = %s OR target_tenant_id = %s
        LIMIT 1
        """,
        (tenant_id, tenant_id),
    ).fetchone()
    if rst is not None:
        leftover.append("tenant_restores")
    row = conn.execute(
        """
        SELECT restoring FROM tenant_settings
        WHERE tenant_id = %s
        """,
        (tenant_id,),
    ).fetchone()
    if row is None:
        return leftover
    if row.get("restoring"):
        return leftover
    leftover.append("tenant_settings")
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
    def export_tenant(
        self,
        tenant_id: str,
        *,
        include_blob_bytes: bool = True,
    ) -> TenantSnapshot:
        """Capture a snapshot of *tenant_id* without pausing it.

        Pages with keyset reads in short transactions so a huge tenant cannot
        hold a long ``REPEATABLE READ`` or OFFSET-scan prior rows. Ciphertext
        is copied as-is (no decrypt). *include_blob_bytes* embeds spilled
        files as ``data_b64`` so restore can rebuild a fresh fleet; relocate
        leaves files on the shared/dest store instead.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        sid = uuid4()
        with transaction(self._pool) as conn:
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
        try:
            for kind in SNAPSHOT_KINDS:
                after: Any = None
                seq = 0
                while True:
                    with transaction(self._pool) as conn:
                        n, after = self._export_kind_page(
                            conn,
                            snapshot_id=sid,
                            kind=kind,
                            tenant_id=tenant_id,
                            after=after,
                            limit=CHUNK,
                            seq=seq,
                            include_blob_bytes=include_blob_bytes,
                            hydrate_sealed=include_blob_bytes,
                        )
                    if n == 0:
                        break
                    seq += 1
            with transaction(self._pool) as conn:
                counts = self._snapshot_kind_counts(conn, sid)
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
        except Exception as exc:  # noqa: BLE001
            with transaction(self._pool) as conn:
                conn.execute(
                    """
                    UPDATE tenant_snapshots
                    SET status = 'failed', error = %s, completed_at = %s
                    WHERE id = %s
                    """,
                    (str(exc)[:2000], _utcnow(), sid),
                )
            raise RestoreError(
                f"export failed for tenant {tenant_id!r}: {exc}"
            ) from exc
        snap = self.get_tenant_snapshot(sid)
        assert snap is not None
        return snap

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

    def _snapshot_kind_counts(self, conn: Any, snapshot_id: UUID) -> dict[str, int]:
        rows = conn.execute(
            """
            SELECT kind, COALESCE(SUM(row_count), 0) AS n
            FROM tenant_snapshot_blobs
            WHERE snapshot_id = %s
            GROUP BY kind
            """,
            (snapshot_id,),
        ).fetchall()
        return {str(r["kind"]): int(r["n"] or 0) for r in rows}

    def _export_kind_resume(
        self, conn: Any, *, snapshot_id: UUID, kind: str
    ) -> tuple[int, Any]:
        """Return ``(next_seq, after_key)`` from already-written pages."""
        row = conn.execute(
            """
            SELECT seq, rows
            FROM tenant_snapshot_blobs
            WHERE snapshot_id = %s AND kind = %s
            ORDER BY seq DESC
            LIMIT 1
            """,
            (snapshot_id, kind),
        ).fetchone()
        if row is None:
            return 0, None
        raw = row["rows"] or []
        if isinstance(raw, str):
            raw = json.loads(raw)
        last = raw[-1] if raw else None
        return int(row["seq"]) + 1, _keyset_from_row(kind, last)

    def _export_kind_page(
        self,
        conn: Any,
        *,
        snapshot_id: UUID,
        kind: str,
        tenant_id: str,
        after: Any,
        limit: int,
        seq: int,
        include_blob_bytes: bool = True,
        hydrate_sealed: bool = True,
    ) -> tuple[int, Any]:
        """Capture one keyset page. Returns ``(row_count, next_after)``."""
        if limit < 1:
            return 0, after
        if kind == "payload_blobs":
            return self._export_payload_blobs_page(
                conn,
                snapshot_id=snapshot_id,
                tenant_id=tenant_id,
                after=after,
                limit=limit,
                seq=seq,
                include_blob_bytes=include_blob_bytes,
            )
        if kind == "inbound_trigger_nonces":
            return self._export_nonces_page(
                conn,
                snapshot_id=snapshot_id,
                tenant_id=tenant_id,
                after=after,
                limit=limit,
                seq=seq,
            )
        if kind in ("tenant_snapshots", "tenant_snapshot_blobs", "tenant_restores"):
            return self._export_backup_history_page(
                conn,
                snapshot_id=snapshot_id,
                kind=kind,
                tenant_id=tenant_id,
                after=after,
                limit=limit,
                seq=seq,
            )
        cols, table = _KIND_SQL[kind]
        key_cols = _kind_key_cols(kind)
        keyset_sql, keyset_params = _keyset_sql(kind, after)
        if kind == "alert_rules":
            where = "(tenant_id = %s OR (scope = 'tenant' AND tenant_id = %s))"
            params: tuple[Any, ...] = (tenant_id, tenant_id)
        else:
            where = "tenant_id = %s"
            params = (tenant_id,)
        order = ", ".join(f"{c} ASC" for c in key_cols)
        rows = conn.execute(
            f"""
            SELECT {cols}
            FROM {table}
            WHERE {where} {keyset_sql}
            ORDER BY {order}
            LIMIT %s
            """,
            params + keyset_params + (int(limit),),
        ).fetchall()
        if not rows:
            return 0, after
        blob: list[dict[str, Any]] = []
        for r in rows:
            item = _jsonable(dict(r))
            if hydrate_sealed and kind in ("jobs", "webhook_deliveries"):
                if "payload" in item:
                    item["payload"] = self._snapshot_sealed_value(item.get("payload"))
                if item.get("result") is not None:
                    item["result"] = self._snapshot_sealed_value(item.get("result"))
            blob.append(item)
        conn.execute(
            """
            INSERT INTO tenant_snapshot_blobs (
                snapshot_id, kind, seq, row_count, rows
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_id, kind, seq) DO NOTHING
            """,
            (snapshot_id, kind, seq, len(blob), Jsonb(blob)),
        )
        return len(rows), _keyset_from_row(kind, dict(rows[-1]))

    def _export_nonces_page(
        self,
        conn: Any,
        *,
        snapshot_id: UUID,
        tenant_id: str,
        after: Any,
        limit: int,
        seq: int,
    ) -> tuple[int, Any]:
        keyset_sql, keyset_params = _keyset_sql("inbound_trigger_nonces", after)
        rows = conn.execute(
            f"""
            SELECT n.trigger_id, n.nonce, n.seen_at
            FROM inbound_trigger_nonces n
            JOIN inbound_triggers t ON t.id = n.trigger_id
            WHERE t.tenant_id = %s {keyset_sql}
            ORDER BY n.trigger_id ASC, n.nonce ASC
            LIMIT %s
            """,
            (tenant_id,) + keyset_params + (int(limit),),
        ).fetchall()
        if not rows:
            return 0, after
        blob = [_jsonable(dict(r)) for r in rows]
        conn.execute(
            """
            INSERT INTO tenant_snapshot_blobs (
                snapshot_id, kind, seq, row_count, rows
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_id, kind, seq) DO NOTHING
            """,
            (snapshot_id, "inbound_trigger_nonces", seq, len(blob), Jsonb(blob)),
        )
        return len(rows), _keyset_from_row("inbound_trigger_nonces", dict(rows[-1]))

    def _export_backup_history_page(
        self,
        conn: Any,
        *,
        snapshot_id: UUID,
        kind: str,
        tenant_id: str,
        after: Any,
        limit: int,
        seq: int,
    ) -> tuple[int, Any]:
        """Copy prior tenant snapshots / restore rows, excluding this snapshot."""
        cols, table = _KIND_SQL[kind]
        key_cols = _kind_key_cols(kind)
        keyset_sql, keyset_params = _keyset_sql(kind, after)
        order = ", ".join(f"{c} ASC" for c in key_cols)
        if kind == "tenant_snapshots":
            where = "source_tenant_id = %s AND id <> %s"
            params: tuple[Any, ...] = (tenant_id, snapshot_id)
        elif kind == "tenant_snapshot_blobs":
            where = (
                "snapshot_id IN ("
                "SELECT id FROM tenant_snapshots "
                "WHERE source_tenant_id = %s AND id <> %s)"
            )
            params = (tenant_id, snapshot_id)
        else:
            where = (
                "(source_tenant_id = %s OR target_tenant_id = %s) "
                "AND snapshot_id <> %s"
            )
            params = (tenant_id, tenant_id, snapshot_id)
        rows = conn.execute(
            f"""
            SELECT {cols}
            FROM {table}
            WHERE {where} {keyset_sql}
            ORDER BY {order}
            LIMIT %s
            """,
            params + keyset_params + (int(limit),),
        ).fetchall()
        if not rows:
            return 0, after
        blob = [_jsonable(dict(r)) for r in rows]
        conn.execute(
            """
            INSERT INTO tenant_snapshot_blobs (
                snapshot_id, kind, seq, row_count, rows
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_id, kind, seq) DO NOTHING
            """,
            (snapshot_id, kind, seq, len(blob), Jsonb(blob)),
        )
        return len(rows), _keyset_from_row(kind, dict(rows[-1]))

    def _snapshot_sealed_value(self, value: Any) -> Any:
        """Copy ciphertext into the snapshot so restore does not need files.

        Blob pointers become a full envelope (``c`` present). Missing files
        stay as pointers; restore then fail-closes that row instead of
        poisoning dest lease.
        """
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return value
        if not isinstance(value, dict):
            return value
        if is_blob_pointer(value) and hasattr(self, "_hydrate_envelope"):
            try:
                return self._hydrate_envelope(value)
            except Exception:
                return value
        return value

    def _export_payload_blobs_page(
        self,
        conn: Any,
        *,
        snapshot_id: UUID,
        tenant_id: str,
        after: Any,
        limit: int,
        seq: int,
        include_blob_bytes: bool,
    ) -> tuple[int, Any]:
        keyset_sql, keyset_params = _keyset_sql("payload_blobs", after)
        rows = conn.execute(
            f"""
            SELECT id, tenant_id, job_id, kind, sha256, nbytes, key_id,
                   status, created_at, committed_at
            FROM job_payload_blobs
            WHERE tenant_id = %s {keyset_sql}
            ORDER BY id ASC
            LIMIT %s
            """,
            (tenant_id,) + keyset_params + (int(limit),),
        ).fetchall()
        if not rows:
            return 0, after
        store = getattr(self, "_blob_store", None)
        out: list[dict[str, Any]] = []
        for r in rows:
            item = _jsonable(dict(r))
            if include_blob_bytes and store is not None:
                try:
                    raw = store.read(r["id"])
                    item["data_b64"] = base64.b64encode(raw).decode("ascii")
                    if not item.get("nbytes"):
                        item["nbytes"] = len(raw)
                    if not item.get("sha256"):
                        item["sha256"] = hashlib.sha256(raw).hexdigest()
                except Exception:
                    item["data_b64"] = ""
            out.append(item)
        conn.execute(
            """
            INSERT INTO tenant_snapshot_blobs (
                snapshot_id, kind, seq, row_count, rows
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_id, kind, seq) DO NOTHING
            """,
            (snapshot_id, "payload_blobs", seq, len(out), Jsonb(out)),
        )
        return len(rows), _keyset_from_row("payload_blobs", dict(rows[-1]))
