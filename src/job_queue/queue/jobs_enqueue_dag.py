"""Atomic DAG enqueue: all-or-nothing graph with idempotent batch retry."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import DagEnqueueResult, Job
from job_queue.queue._common import (
    _DAG_BATCH_LOCK_CLASS,
    _JOB_COLUMNS,
    DependencyError,
    _as_utc,
    _normalize_labels,
    _normalize_skills,
    _utcnow,
)


_MAX_DAG_JOBS = 200
_MAX_BATCH_KEY_LEN = 256


class JobsEnqueueDagMixin:
    def enqueue_dag(
        self,
        *,
        tenant_id: str,
        jobs: Sequence[dict[str, Any]],
        batch_key: str,
        queue_name: str = "default",
    ) -> DagEnqueueResult:
        """Enqueue a job DAG in one transaction (all-or-nothing).

        Each entry in *jobs* is a dict with:

        * ``ref`` (required): unique name in this batch
        * ``payload`` (required): dict
        * ``depends_on``: list of other ``ref`` values and/or existing job UUIDs
        * optional: ``job_id``, ``priority``, ``max_attempts``,
          ``visibility_timeout_seconds``, ``max_runtime_seconds``,
          ``dedupe_key``, ``labels``, ``required_skills``, ``ttl_seconds``,
          ``expires_at``, ``expiry_action``, ``deadline_at``,
          ``estimated_duration_seconds``, ``preemptible``, ``delay_seconds``,
          ``available_at``

        *batch_key* identifies the graph for idempotent retry. Concurrent
        callers and crash-retries with the same tenant/queue/key see the
        same committed jobs; a failure before commit leaves nothing.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if not batch_key or not str(batch_key).strip():
            raise ValueError("batch_key is required")
        batch_key = str(batch_key).strip()
        if len(batch_key) > _MAX_BATCH_KEY_LEN:
            raise ValueError(f"batch_key must be <= {_MAX_BATCH_KEY_LEN} chars")
        if not jobs:
            raise ValueError("jobs must be a non-empty list")
        if len(jobs) > _MAX_DAG_JOBS:
            raise ValueError(f"jobs must have at most {_MAX_DAG_JOBS} nodes")

        specs = self._normalize_dag_specs(jobs)
        now = _utcnow()

        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            self._lock_dag_batch_key(
                conn,
                tenant_id=tenant_id,
                queue_name=queue_name,
                batch_key=batch_key,
            )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding; enqueue is rejected"
                )
            if self._is_tenant_restoring(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is restoring; enqueue is rejected"
                )

            existing = conn.execute(
                """
                SELECT id, job_ids, refs
                FROM job_dag_batches
                WHERE tenant_id = %s AND queue_name = %s AND batch_key = %s
                """,
                (tenant_id, queue_name, batch_key),
            ).fetchone()
            if existing is not None:
                return self._dag_result_from_existing(
                    conn,
                    existing,
                    batch_key=batch_key,
                    specs=specs,
                )

            ref_to_id = {spec["ref"]: spec["job_id"] for spec in specs}
            in_batch_ids = {spec["job_id"] for spec in specs}
            for spec in specs:
                dep_ids: list[UUID] = []
                for raw in spec["depends_on_raw"]:
                    if raw in ref_to_id:
                        dep_ids.append(ref_to_id[raw])
                    else:
                        try:
                            dep_ids.append(UUID(str(raw)))
                        except (ValueError, TypeError, AttributeError) as exc:
                            raise DependencyError(
                                f"job {spec['ref']!r} depends_on unknown "
                                f"ref or UUID {raw!r}"
                            ) from exc
                if spec["job_id"] in dep_ids:
                    raise DependencyError(
                        f"job {spec['ref']!r} cannot depend on itself"
                    )
                spec["dep_ids"] = self._normalize_depends_on(dep_ids)

            # Cycle check uses resolved UUIDs so A↔B via explicit job_ids
            # (not just in-batch refs) cannot deadlock after commit.
            self._assert_dag_acyclic(specs)
            external = []
            for spec in specs:
                for pid in spec["dep_ids"]:
                    if pid not in in_batch_ids:
                        external.append(pid)
            if external:
                self._validate_dependencies(
                    conn, tenant_id=tenant_id, depends_on=list(dict.fromkeys(external))
                )

            dedupe_keys = sorted(
                {
                    spec["dedupe_key"]
                    for spec in specs
                    if spec["dedupe_key"] is not None
                }
            )
            for dk in dedupe_keys:
                self._lock_dedupe_key(
                    conn,
                    tenant_id=tenant_id,
                    queue_name=queue_name,
                    dedupe_key=dk,
                )
                hit = self._find_active_dedupe(
                    conn,
                    tenant_id=tenant_id,
                    queue_name=queue_name,
                    dedupe_key=dk,
                )
                if hit is not None:
                    raise ValueError(
                        f"dedupe_key {dk!r} already has an active job; "
                        "refusing to enqueue a partial DAG"
                    )

            self._assert_pending_capacity_for(
                conn, tenant_id, extra=len(specs)
            )
            for _ in specs:
                self._consume_enqueue_rate(conn, tenant_id, now=now)
            if hasattr(self, "_consume_spend_enqueues"):
                self._consume_spend_enqueues(
                    conn, tenant_id, extra=len(specs), now=now
                )

            inserted: list[Job] = []
            refs: list[str] = []
            for spec in specs:
                expires, exp_action = self._resolve_job_expiry(
                    conn,
                    tenant_id=tenant_id,
                    now=now,
                    ttl_seconds=spec["ttl_seconds"],
                    expires_at=spec["expires_at"],
                    expiry_action=spec["expiry_action"],
                )
                sealed, payload_key_id = self._seal_job_payload(
                    conn,
                    tenant_id=tenant_id,
                    job_id=spec["job_id"],
                    payload=spec["payload"],
                )
                status = self._enqueue_status_for_skills(conn, spec["skills"])
                part_id = (
                    self._compute_partition_id(
                        conn, tenant_id=tenant_id, job_id=spec["job_id"]
                    )
                    if hasattr(self, "_compute_partition_id")
                    else 0
                )
                row = conn.execute(
                    f"""
                    INSERT INTO jobs (
                        id, tenant_id, queue_name, payload, status, priority,
                        attempts, max_attempts, visibility_timeout_seconds,
                        max_runtime_seconds,
                        retry_backoff_base_seconds, retry_backoff_max_seconds,
                        available_at, dedupe_key, schedule_id, depends_on,
                        labels, required_skills,
                        deadline_at, estimated_duration_seconds, preemptible,
                        created_at, updated_at, partition_id, payload_key_id,
                        expires_at, expiry_action
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        0, %s, %s,
                        %s,
                        %s, %s,
                        %s, %s, NULL, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s
                    )
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        spec["job_id"],
                        tenant_id,
                        queue_name,
                        Jsonb(sealed),
                        status,
                        spec["priority"],
                        spec["max_attempts"],
                        spec["visibility_timeout_seconds"],
                        spec["max_runtime_seconds"],
                        spec["retry_backoff_base_seconds"],
                        spec["retry_backoff_max_seconds"],
                        spec["run_at"],
                        spec["dedupe_key"],
                        spec["dep_ids"],
                        spec["labels"],
                        spec["skills"],
                        spec["deadline"],
                        spec["estimated_duration_seconds"],
                        spec["preemptible"],
                        now,
                        now,
                        part_id,
                        payload_key_id,
                        expires,
                        exp_action,
                    ),
                ).fetchone()
                assert row is not None
                if hasattr(self, "_recheck_awaiting_skills_row_locked"):
                    row = self._recheck_awaiting_skills_row_locked(
                        conn, row, now=now
                    )
                job = self._job(row, conn=conn)
                if hasattr(self, "_append_job_audit"):
                    self._append_job_audit(
                        conn,
                        job_row=row,
                        event_type="enqueued",
                        from_status=None,
                        actor=None,
                        detail={
                            "queue_name": queue_name,
                            "dag_batch_key": batch_key,
                            "dag_ref": spec["ref"],
                            "required_skills": spec["skills"],
                            "status": job.status.value,
                        },
                        now=now,
                    )
                if hasattr(self, "_stamp_job_quarantine"):
                    self._stamp_job_quarantine(
                        conn,
                        job_id=job.id,
                        tenant_id=tenant_id,
                        max_worker_hops=spec.get("max_worker_hops"),
                        quarantine_action=spec.get("quarantine_action"),
                    )
                    stamped = conn.execute(
                        f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                        (job.id,),
                    ).fetchone()
                    if stamped is not None:
                        job = self._job(stamped, conn=conn)
                inserted.append(job)
                refs.append(spec["ref"])

            if hasattr(self, "_record_usage"):
                self._record_usage(
                    conn, tenant_id=tenant_id, now=now, enqueues=len(inserted)
                )

            batch_id = uuid4()
            conn.execute(
                """
                INSERT INTO job_dag_batches (
                    id, tenant_id, queue_name, batch_key, job_ids, refs,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    batch_id,
                    tenant_id,
                    queue_name,
                    batch_key,
                    [j.id for j in inserted],
                    refs,
                    now,
                ),
            )
            return DagEnqueueResult(
                batch_key=batch_key,
                batch_id=batch_id,
                jobs=tuple(inserted),
                refs=tuple(refs),
                created=True,
            )

    def _dag_result_from_existing(
        self,
        conn: Any,
        existing: dict[str, Any],
        *,
        batch_key: str,
        specs: list[dict[str, Any]],
    ) -> DagEnqueueResult:
        stored_refs = [str(r) for r in (existing.get("refs") or [])]
        want_refs = [s["ref"] for s in specs]
        if stored_refs != want_refs:
            raise ValueError(
                f"batch_key {batch_key!r} already committed with different "
                f"refs {stored_refs} (got {want_refs})"
            )
        want_ids = [s["job_id"] for s in specs]
        stored_ids = [UUID(str(x)) for x in (existing.get("job_ids") or [])]
        # Allow retry without repeating client job_ids; if they were sent,
        # they must match the committed graph.
        for spec, sid in zip(specs, stored_ids, strict=True):
            if spec.get("job_id_explicit") and spec["job_id"] != sid:
                raise ValueError(
                    f"batch_key {batch_key!r} job {spec['ref']!r} id mismatch"
                )
        rows = []
        for jid in stored_ids:
            row = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (jid,),
            ).fetchone()
            if row is None:
                raise RuntimeError(
                    f"dag batch {batch_key!r} missing job {jid}; "
                    "graph is inconsistent"
                )
            rows.append(self._job(row, conn=conn))
        return DagEnqueueResult(
            batch_key=batch_key,
            batch_id=existing["id"],
            jobs=tuple(rows),
            refs=tuple(stored_refs),
            created=False,
        )

    @staticmethod
    def _lock_dag_batch_key(
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        batch_key: str,
    ) -> None:
        lock_key = f"{tenant_id}\x1f{queue_name}\x1f{batch_key}"
        conn.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
            (_DAG_BATCH_LOCK_CLASS, lock_key),
        )

    def _assert_pending_capacity_for(
        self, conn: Any, tenant_id: str, *, extra: int
    ) -> None:
        if extra < 1:
            return
        limit = self._read_pending_limit(conn, tenant_id)
        if limit is None:
            return
        n = self._count_pending(conn, tenant_id)
        if n + extra > limit:
            raise ValueError(
                f"tenant {tenant_id!r} pending job cap reached "
                f"({n}+{extra}/{limit}); DAG enqueue is rejected"
            )

    @staticmethod
    def _assert_dag_acyclic(specs: list[dict[str, Any]]) -> None:
        ids = {s["job_id"] for s in specs}
        incoming = {s["job_id"]: 0 for s in specs}
        adj: dict[UUID, list[UUID]] = {s["job_id"]: [] for s in specs}
        for spec in specs:
            for pid in spec.get("dep_ids") or ():
                if pid in ids:
                    adj[pid].append(spec["job_id"])
                    incoming[spec["job_id"]] += 1
        ready = [jid for jid, c in incoming.items() if c == 0]
        seen = 0
        while ready:
            node = ready.pop()
            seen += 1
            for child in adj[node]:
                incoming[child] -= 1
                if incoming[child] == 0:
                    ready.append(child)
        if seen != len(specs):
            raise DependencyError("job DAG contains a cycle")

    def _normalize_dag_specs(
        self, jobs: Sequence[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        specs: list[dict[str, Any]] = []
        seen_refs: set[str] = set()
        seen_ids: set[UUID] = set()
        seen_dedupe: set[str] = set()
        now = _utcnow()
        for i, raw in enumerate(jobs):
            if not isinstance(raw, dict):
                raise ValueError(f"jobs[{i}] must be a dict")
            ref = str(raw.get("ref") or "").strip()
            if not ref:
                raise ValueError(f"jobs[{i}].ref is required")
            if ref in seen_refs:
                raise ValueError(f"duplicate dag ref {ref!r}")
            seen_refs.add(ref)
            payload = raw.get("payload")
            if not isinstance(payload, dict):
                raise ValueError(f"jobs[{i}].payload must be a dict")
            deps_raw = raw.get("depends_on") or []
            if isinstance(deps_raw, (str, UUID)):
                deps_raw = [deps_raw]
            if not isinstance(deps_raw, (list, tuple)):
                raise ValueError(f"jobs[{i}].depends_on must be a list")
            dep_names: list[str] = []
            for d in deps_raw:
                if isinstance(d, UUID):
                    dep_names.append(str(d))
                else:
                    s = str(d).strip()
                    if not s:
                        raise ValueError(f"jobs[{i}].depends_on has an empty entry")
                    dep_names.append(s)
            jid_raw = raw.get("job_id")
            if jid_raw is None:
                jid = uuid4()
                explicit = False
            else:
                jid = jid_raw if isinstance(jid_raw, UUID) else UUID(str(jid_raw))
                explicit = True
            if jid in seen_ids:
                raise ValueError(f"duplicate job_id {jid} in DAG")
            seen_ids.add(jid)
            dedupe_key = raw.get("dedupe_key")
            if dedupe_key is not None:
                dedupe_key = str(dedupe_key)
                if dedupe_key == "":
                    raise ValueError(f"jobs[{i}].dedupe_key must be non-empty")
                if dedupe_key in seen_dedupe:
                    raise ValueError(
                        f"duplicate dedupe_key {dedupe_key!r} in DAG"
                    )
                seen_dedupe.add(dedupe_key)
            available_at = raw.get("available_at")
            delay_seconds = raw.get("delay_seconds")
            if available_at is not None and delay_seconds is not None:
                raise ValueError(
                    f"jobs[{i}]: pass only one of available_at or delay_seconds"
                )
            if available_at is not None:
                run_at = _as_utc(available_at)
            elif delay_seconds is not None:
                if float(delay_seconds) < 0:
                    raise ValueError(f"jobs[{i}].delay_seconds must be >= 0")
                run_at = now + timedelta(seconds=float(delay_seconds))
            else:
                run_at = now
            deadline_raw = raw.get("deadline_at")
            deadline = _as_utc(deadline_raw) if deadline_raw is not None else None
            expires_raw = raw.get("expires_at")
            expires_at = _as_utc(expires_raw) if expires_raw is not None else None
            est = raw.get("estimated_duration_seconds")
            if est is not None:
                est = int(est)
                if est < 1:
                    raise ValueError(
                        f"jobs[{i}].estimated_duration_seconds must be >= 1"
                    )
            vt = int(raw.get("visibility_timeout_seconds") or 30)
            max_rt = int(raw.get("max_runtime_seconds") or 3600)
            max_attempts = int(raw.get("max_attempts") or 3)
            if vt < 1 or max_rt < 1 or max_attempts < 1:
                raise ValueError(f"jobs[{i}] timeouts/attempts must be >= 1")
            ttl = raw.get("ttl_seconds")
            if ttl is not None and float(ttl) < 1:
                raise ValueError(f"jobs[{i}].ttl_seconds must be >= 1")
            specs.append(
                {
                    "ref": ref,
                    "payload": dict(payload),
                    "depends_on_raw": dep_names,
                    "job_id": jid,
                    "job_id_explicit": explicit,
                    "priority": int(raw.get("priority") or 0),
                    "max_attempts": max_attempts,
                    "visibility_timeout_seconds": vt,
                    "max_runtime_seconds": max_rt,
                    "retry_backoff_base_seconds": (
                        1
                        if raw.get("retry_backoff_base_seconds") is None
                        else int(raw["retry_backoff_base_seconds"])
                    ),
                    "retry_backoff_max_seconds": (
                        300
                        if raw.get("retry_backoff_max_seconds") is None
                        else int(raw["retry_backoff_max_seconds"])
                    ),
                    "dedupe_key": dedupe_key,
                    "labels": _normalize_labels(raw.get("labels")),
                    "skills": _normalize_skills(raw.get("required_skills")),
                    "ttl_seconds": ttl,
                    "expires_at": expires_at,
                    "expiry_action": raw.get("expiry_action"),
                    "max_worker_hops": raw.get("max_worker_hops"),
                    "quarantine_action": raw.get("quarantine_action"),
                    "deadline": deadline,
                    "estimated_duration_seconds": est,
                    "preemptible": bool(raw.get("preemptible", True)),
                    "run_at": run_at,
                }
            )
        return specs
