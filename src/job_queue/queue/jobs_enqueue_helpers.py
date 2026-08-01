"""JobQueue enqueue helpers: dedupe and deps."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from job_queue.models import (
    JobStatus,
)
from job_queue.queue._common import (
    _DEDUPE_LOCK_CLASS,
    _JOB_COLUMNS,
    DependencyError,
)


class JobsEnqueueHelpersMixin:
    @staticmethod
    def _lock_dedupe_key(
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        dedupe_key: str,
    ) -> None:
        """Transaction-scoped lock shared by enqueue and replay for one key."""
        # Two-arg form avoids colliding with tenant concurrency locks.
        # Use unit separators (not NUL — psycopg/Postgres reject 0x00 in text).
        lock_key = f"{tenant_id}\x1f{queue_name}\x1f{dedupe_key}"
        conn.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
            (_DEDUPE_LOCK_CLASS, lock_key),
        )

    @staticmethod
    def _normalize_depends_on(
        depends_on: UUID | Sequence[UUID] | None,
    ) -> list[UUID]:
        if depends_on is None:
            return []
        if isinstance(depends_on, UUID):
            return [depends_on]
        ids = list(depends_on)
        for item in ids:
            if not isinstance(item, UUID):
                raise TypeError("depends_on entries must be UUID instances")
        # Preserve order, drop duplicates.
        seen: set[UUID] = set()
        unique: list[UUID] = []
        for item in ids:
            if item not in seen:
                seen.add(item)
                unique.append(item)
        return unique

    def _validate_dependencies(
        self,
        conn: Any,
        *,
        tenant_id: str,
        depends_on: list[UUID],
    ) -> None:
        """Ensure parents exist for this tenant and are not already terminal-failed."""
        terminal_fail = {
            JobStatus.FAILED.value,
            JobStatus.DEAD.value,
            JobStatus.CANCELLED.value,
        }
        for parent_id in depends_on:
            row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                """,
                (parent_id, tenant_id),
            ).fetchone()
            if row is None:
                raise DependencyError(
                    f"dependency {parent_id} not found for tenant {tenant_id!r}"
                )
            if row["status"] in terminal_fail:
                raise DependencyError(
                    f"dependency {parent_id} is already {row['status']}; "
                    "refusing to enqueue a child that can never run"
                )

    @staticmethod
    def _find_active_dedupe(
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        dedupe_key: str,
    ) -> dict[str, Any] | None:
        return conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND queue_name = %s
              AND dedupe_key = %s
              AND status IN (%s, %s, %s, %s)
            LIMIT 1
            """,
            (
                tenant_id,
                queue_name,
                dedupe_key,
                JobStatus.PENDING.value,
                JobStatus.LEASED.value,
                JobStatus.AWAITING_APPROVAL.value,
                JobStatus.AWAITING_CHILDREN.value,
            ),
        ).fetchone()
