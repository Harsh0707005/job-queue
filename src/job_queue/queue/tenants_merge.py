"""JobQueue tenant merge."""

from __future__ import annotations

from job_queue.db import transaction
from job_queue.models import (
    JobStatus,
    TenantMergeResult,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    MergeError,
    _utcnow,
)


class TenantsMergeMixin:
    def merge_tenant(
        self,
        source_tenant_id: str,
        target_tenant_id: str,
    ) -> TenantMergeResult:
        """Transfer ownership of *source* into *target* without stopping workers.

        Moves **all** of the source tenant's rows to the target in one
        transaction:

        * jobs (pending, leased/running, and terminal history)
        * schedules
        * webhook endpoints and deliveries (including ``delivering``)

        Lease fencing tokens, ``leased_by``, and ``lease_tenant_id`` are
        preserved so in-flight workers keep heartbeating / acking with the
        pre-merge tenant (lease ops accept current owner **or** the frozen
        claim tenant). Unrelated tenants remain rejected.

        Conflict handling (so unique indexes cannot fail the transfer):

        * Active ``dedupe_key`` collisions with the target → clear the
          *source* job's dedupe key (job itself still moves).
        * Schedule ``name`` collisions → rename source schedule to
          ``{name}__from_{source_tenant_id}``.

        Source is blocked for new work (paused + offboarding) for the
        duration of the transfer, then its settings / fairness rows are
        deleted. Target settings (pause, caps, offboarding) are unchanged.
        Refuses to merge into an offboarding target, or if the target's
        pending-job cap would be exceeded by the combined pending set.

        After a successful call, ``tenant_is_empty(source)`` is True.
        """
        if not source_tenant_id or not target_tenant_id:
            raise ValueError("source_tenant_id and target_tenant_id are required")
        if source_tenant_id == target_tenant_id:
            raise ValueError("source_tenant_id and target_tenant_id must differ")

        now = _utcnow()
        source = source_tenant_id
        target = target_tenant_id

        with transaction(self._pool) as conn:
            # Serialize with enqueue / pending-cap / other merges for both
            # tenants (ordered lock acquisition avoids deadlocks).
            for tid in sorted((source, target)):
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (tid,),
                )

            if self._is_tenant_offboarding(conn, target):
                raise MergeError(
                    f"cannot merge into tenant {target!r}: target is offboarding"
                )

            # Block concurrent source enqueue / schedule / webhook registration.
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at
                )
                VALUES (%s, NULL, NULL, TRUE, TRUE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET paused = TRUE,
                    offboarding = TRUE,
                    updated_at = EXCLUDED.updated_at
                """,
                (source, now),
            )

            # Pending-cap: refuse if target would exceed after absorbing source.
            target_limit = self._read_pending_limit(conn, target)
            if target_limit is not None:
                source_pending = self._count_pending(conn, source)
                target_pending = self._count_pending(conn, target)
                if source_pending + target_pending > target_limit:
                    raise MergeError(
                        f"cannot merge tenant {source!r} into {target!r}: "
                        f"pending job cap would be exceeded "
                        f"({source_pending}+{target_pending} > {target_limit}); "
                        f"raise or clear the target pending limit first"
                    )

            # Resolve active dedupe collisions (prefer target's active key).
            dedupe_cleared = conn.execute(
                """
                UPDATE jobs j
                SET dedupe_key = NULL,
                    updated_at = %s
                WHERE j.tenant_id = %s
                  AND j.dedupe_key IS NOT NULL
                  AND j.status IN (
                      'pending', 'leased', 'awaiting_approval',
                      'awaiting_children', 'awaiting_skills'
                  )
                  AND EXISTS (
                        SELECT 1
                        FROM jobs t
                        WHERE t.tenant_id = %s
                          AND t.queue_name = j.queue_name
                          AND t.dedupe_key = j.dedupe_key
                          AND t.status IN (
                              'pending', 'leased',
                              'awaiting_approval', 'awaiting_children',
                              'awaiting_skills'
                          )
                  )
                RETURNING j.id
                """,
                (now, source, target),
            ).fetchall()

            # Payload keys: keep old DEKs decrypt_only under the target so
            # in-flight ciphertext stays open. Demote source active first so
            # the one-active-key unique index still holds after the move.
            conn.execute(
                """
                UPDATE tenant_payload_keys
                SET status = 'decrypt_only'
                WHERE tenant_id = %s AND status = 'active'
                """,
                (source,),
            )
            conn.execute(
                """
                UPDATE tenant_payload_keys
                SET tenant_id = %s
                WHERE tenant_id = %s
                """,
                (target, source),
            )
            conn.execute(
                """
                UPDATE payload_key_rotations
                SET tenant_id = %s, updated_at = %s
                WHERE tenant_id = %s
                """,
                (target, now, source),
            )

            # Resolve schedule name collisions (unique per tenant when set).
            names_renamed = conn.execute(
                """
                UPDATE schedules s
                SET name = s.name || %s,
                    updated_at = %s
                WHERE s.tenant_id = %s
                  AND s.name IS NOT NULL
                  AND EXISTS (
                        SELECT 1
                        FROM schedules t
                        WHERE t.tenant_id = %s
                          AND t.name = s.name
                  )
                RETURNING s.id
                """,
                (f"__from_{source}", now, source, target),
            ).fetchall()

            jobs_moved = conn.execute(
                """
                UPDATE jobs
                SET tenant_id = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                RETURNING id, status
                """,
                (target, now, source),
            ).fetchall()
            for jm in jobs_moved:
                moved_row = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (jm["id"],),
                ).fetchone()
                if moved_row is not None:
                    self._append_job_audit(
                        conn,
                        job_row=moved_row,
                        event_type="tenant_merged",
                        from_status=jm["status"],
                        actor="system",
                        detail={
                            "from_tenant": source,
                            "to_tenant": target,
                        },
                        now=now,
                    )
            leased_moved = sum(
                1 for r in jobs_moved if r["status"] == JobStatus.LEASED.value
            )

            sched_moved = conn.execute(
                """
                UPDATE schedules
                SET tenant_id = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                RETURNING id
                """,
                (target, now, source),
            ).fetchall()

            # Endpoints before deliveries is fine (no tenant FK); both move.
            ep_moved = conn.execute(
                """
                UPDATE webhook_endpoints
                SET tenant_id = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                RETURNING id
                """,
                (target, now, source),
            ).fetchall()
            del_moved = conn.execute(
                """
                UPDATE webhook_deliveries
                SET tenant_id = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                RETURNING id
                """,
                (target, now, source),
            ).fetchall()

            # Side-effect markers follow job ownership (job_id unchanged).
            conn.execute(
                """
                UPDATE job_side_effects
                SET tenant_id = %s
                WHERE tenant_id = %s
                """,
                (target, source),
            )

            conn.execute(
                """
                UPDATE job_dag_batches s
                SET batch_key = s.batch_key || %s
                WHERE s.tenant_id = %s
                  AND EXISTS (
                        SELECT 1 FROM job_dag_batches t
                        WHERE t.tenant_id = %s
                          AND t.queue_name = s.queue_name
                          AND t.batch_key = s.batch_key
                  )
                """,
                (f"__from_{source}", source, target),
            )
            conn.execute(
                """
                UPDATE job_dag_batches
                SET tenant_id = %s
                WHERE tenant_id = %s
                """,
                (target, source),
            )

            # Billing meters: add source hours into target (no double row).
            conn.execute(
                """
                INSERT INTO tenant_usage_buckets (
                    bucket_start, tenant_id,
                    enqueues, leases, acks, nacks, fails, dead_letters,
                    cancels, releases, preemptions, schedule_fires,
                    webhook_successes, webhook_failures, spawn_children,
                    runtime_ms, wait_ms
                )
                SELECT
                    bucket_start, %s,
                    enqueues, leases, acks, nacks, fails, dead_letters,
                    cancels, releases, preemptions, schedule_fires,
                    webhook_successes, webhook_failures, spawn_children,
                    runtime_ms, wait_ms
                FROM tenant_usage_buckets
                WHERE tenant_id = %s
                ON CONFLICT (bucket_start, tenant_id) DO UPDATE SET
                    enqueues = tenant_usage_buckets.enqueues + EXCLUDED.enqueues,
                    leases = tenant_usage_buckets.leases + EXCLUDED.leases,
                    acks = tenant_usage_buckets.acks + EXCLUDED.acks,
                    nacks = tenant_usage_buckets.nacks + EXCLUDED.nacks,
                    fails = tenant_usage_buckets.fails + EXCLUDED.fails,
                    dead_letters = tenant_usage_buckets.dead_letters
                        + EXCLUDED.dead_letters,
                    cancels = tenant_usage_buckets.cancels + EXCLUDED.cancels,
                    releases = tenant_usage_buckets.releases + EXCLUDED.releases,
                    preemptions = tenant_usage_buckets.preemptions
                        + EXCLUDED.preemptions,
                    schedule_fires = tenant_usage_buckets.schedule_fires
                        + EXCLUDED.schedule_fires,
                    webhook_successes = tenant_usage_buckets.webhook_successes
                        + EXCLUDED.webhook_successes,
                    webhook_failures = tenant_usage_buckets.webhook_failures
                        + EXCLUDED.webhook_failures,
                    spawn_children = tenant_usage_buckets.spawn_children
                        + EXCLUDED.spawn_children,
                    runtime_ms = tenant_usage_buckets.runtime_ms
                        + EXCLUDED.runtime_ms,
                    wait_ms = tenant_usage_buckets.wait_ms + EXCLUDED.wait_ms
                """,
                (target, source),
            )
            conn.execute(
                "DELETE FROM tenant_usage_buckets WHERE tenant_id = %s",
                (source,),
            )

            # Drop source control-plane state (settings + fairness).
            conn.execute(
                "DELETE FROM tenant_settings WHERE tenant_id = %s",
                (source,),
            )
            conn.execute(
                "DELETE FROM tenant_fairness WHERE tenant_id = %s",
                (source,),
            )

            if not self._tenant_is_empty(conn, source):
                # Should be unreachable; surface loudly if a new table is added.
                raise MergeError(
                    f"merge left residual state on source tenant {source!r}"
                )

            return TenantMergeResult(
                source_tenant_id=source,
                target_tenant_id=target,
                jobs_moved=len(jobs_moved),
                jobs_leased_moved=leased_moved,
                schedules_moved=len(sched_moved),
                webhook_endpoints_moved=len(ep_moved),
                webhook_deliveries_moved=len(del_moved),
                dedupe_keys_cleared=len(dedupe_cleared),
                schedule_names_renamed=len(names_renamed),
                source_empty=True,
            )
