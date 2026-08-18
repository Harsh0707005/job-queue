"""Coordinator API handlers."""

from __future__ import annotations

from job_queue.coordinator.auth import (
    _UUID_RE,
)

from typing import Any
from uuid import UUID

from job_queue.codec import (
    job_to_dict,
    schedule_to_dict,
)
from job_queue.models import JobStatus
from job_queue.queue import (
    ScheduleError,
    SpendCapError,
)


class SchedulesApiMixin:
    def _create_schedule(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            cron = body.get("cron") or body.get("cron_expr")
            interval_raw = body.get("interval_seconds")
            interval = None if interval_raw is None else int(interval_raw)
            sched = self.queue.create_schedule(
                tenant_id=str(body["tenant_id"]),
                payload=dict(body.get("payload") or {}),
                cron=str(cron) if cron is not None else None,
                interval_seconds=interval,
                timezone=str(body.get("timezone") or "UTC"),
                queue_name=str(body.get("queue_name") or "default"),
                name=body.get("name"),
                priority=int(body.get("priority") or 0),
                max_attempts=int(body.get("max_attempts") or 3),
                visibility_timeout_seconds=int(
                    body.get("visibility_timeout_seconds") or 30
                ),
                enabled=bool(body.get("enabled", True)),
                labels=body.get("labels"),
                required_skills=body.get("required_skills"),
                misfire_policy=body.get("misfire_policy"),
                catch_up_max=(
                    None
                    if body.get("catch_up_max") is None
                    else int(body["catch_up_max"])
                ),
                dag_jobs=body.get("dag_jobs"),
                mutex_group=body.get("mutex_group"),
            )
        except (KeyError, ValueError, ScheduleError) as exc:
            return 400, {"error": str(exc)}
        return 201, {"schedule": schedule_to_dict(sched, include_payload=True)}

    def _pause_schedule(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / schedules / {id} / pause
        if len(parts) != 4:
            return 400, {"error": "invalid schedule path"}
        sid = parts[2]
        if not _UUID_RE.match(sid):
            return 400, {"error": "invalid schedule id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        try:
            sched = self.queue.pause_schedule(UUID(sid), tenant_id=str(tenant_id))
        except ScheduleError as exc:
            return 404, {"error": str(exc)}
        return 200, {"schedule": schedule_to_dict(sched)}

    def _resume_schedule(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid schedule path"}
        sid = parts[2]
        if not _UUID_RE.match(sid):
            return 400, {"error": "invalid schedule id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        next_raw = body.get("next_run_at")
        next_run = None
        if next_raw is not None and next_raw != "":
            from datetime import datetime as _dt

            next_run = _dt.fromisoformat(str(next_raw).replace("Z", "+00:00"))
        try:
            sched = self.queue.resume_schedule(
                UUID(sid), tenant_id=str(tenant_id), next_run_at=next_run
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        except ScheduleError as exc:
            return 404, {"error": str(exc)}
        return 200, {"schedule": schedule_to_dict(sched)}

    def _fire_schedule(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid schedule path"}
        sid = parts[2]
        if not _UUID_RE.match(sid):
            return 400, {"error": "invalid schedule id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        try:
            fire = self.queue.fire_schedule(UUID(sid), tenant_id=str(tenant_id))
        except ValueError as exc:
            return 400, {"error": str(exc)}
        except SpendCapError as exc:
            return 429, {
                "error": str(exc),
                "code": "spend_capped",
                "tenant_id": exc.tenant_id,
            }
        except ScheduleError as exc:
            msg = str(exc)
            if "not found" in msg:
                return 404, {"error": msg, "code": "schedule_error"}
            return 409, {"error": msg, "code": "schedule_error"}
        return 201, {
            "schedule": schedule_to_dict(fire.schedule),
            "job": job_to_dict(fire.job, include_payload=True),
            "jobs": [
                job_to_dict(j, include_payload=True)
                for j in (fire.jobs or (fire.job,))
            ],
            "refs": list(fire.refs or ()),
            "batch_id": str(fire.batch_id) if fire.batch_id else None,
            "batch_key": fire.batch_key,
        }

    def _update_schedule(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid schedule path"}
        sid = parts[2]
        if not _UUID_RE.match(sid):
            return 400, {"error": "invalid schedule id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        kwargs: dict[str, Any] = {}
        if "cron" in body or "cron_expr" in body:
            kwargs["cron"] = body.get("cron", body.get("cron_expr"))
        if "interval_seconds" in body:
            raw_iv = body.get("interval_seconds")
            kwargs["interval_seconds"] = None if raw_iv is None else int(raw_iv)
        if "timezone" in body:
            kwargs["timezone"] = body.get("timezone")
        if "payload" in body:
            kwargs["payload"] = dict(body.get("payload") or {})
        if "dag_jobs" in body:
            kwargs["dag_jobs"] = body.get("dag_jobs")
        if "queue_name" in body:
            kwargs["queue_name"] = body.get("queue_name")
        if "name" in body:
            kwargs["name"] = body.get("name")
        if "priority" in body:
            kwargs["priority"] = int(body["priority"])
        if "max_attempts" in body:
            kwargs["max_attempts"] = int(body["max_attempts"])
        if "visibility_timeout_seconds" in body:
            kwargs["visibility_timeout_seconds"] = int(
                body["visibility_timeout_seconds"]
            )
        if "labels" in body:
            kwargs["labels"] = body.get("labels")
        if "required_skills" in body:
            kwargs["required_skills"] = body.get("required_skills")
        if "misfire_policy" in body:
            kwargs["misfire_policy"] = body.get("misfire_policy")
        if "catch_up_max" in body:
            kwargs["catch_up_max"] = (
                None
                if body.get("catch_up_max") is None
                else int(body["catch_up_max"])
            )
        if "mutex_group" in body:
            kwargs["mutex_group"] = body.get("mutex_group")
        try:
            sched = self.queue.update_schedule(
                UUID(sid), tenant_id=str(tenant_id), **kwargs
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        except ScheduleError as exc:
            msg = str(exc)
            if "not found" in msg:
                return 404, {"error": msg, "code": "schedule_error"}
            return 409, {"error": msg, "code": "schedule_error"}
        return 200, {"schedule": schedule_to_dict(sched, include_payload=True)}

    def _set_schedule_misfire(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid schedule path"}
        sid = parts[2]
        if not _UUID_RE.match(sid):
            return 400, {"error": "invalid schedule id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        try:
            sched = self.queue.set_schedule_misfire(
                UUID(sid),
                tenant_id=str(tenant_id),
                misfire_policy=body.get("misfire_policy"),
                catch_up_max=(
                    None
                    if body.get("catch_up_max") is None
                    else int(body["catch_up_max"])
                ),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        except ScheduleError as exc:
            return 404, {"error": str(exc)}
        return 200, {"schedule": schedule_to_dict(sched)}

    def _delete_schedule(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid schedule path"}
        sid = parts[2]
        if not _UUID_RE.match(sid):
            return 400, {"error": "invalid schedule id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        wait = bool(body.get("wait") or False)
        timeout_raw = body.get("wait_timeout_seconds")
        timeout: float | None
        if timeout_raw is None or timeout_raw == "":
            timeout = None
        else:
            try:
                timeout = float(timeout_raw)
            except (TypeError, ValueError):
                return 400, {"error": "wait_timeout_seconds must be a number"}
        try:
            deleted = self.queue.delete_schedule(
                UUID(sid),
                tenant_id=str(tenant_id),
                wait=wait,
                wait_timeout_seconds=timeout,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        except ScheduleError as exc:
            return 409, {"error": str(exc), "code": "schedule_error"}
        if not deleted:
            return 404, {"error": f"schedule {sid} not found for tenant"}
        return 200, {"deleted": True, "schedule_id": sid}

    def _list_schedules(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        tenant_id = (query.get("tenant_id") or [None])[0]
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        enabled_only = (query.get("enabled_only") or ["0"])[0] in (
            "1",
            "true",
            "yes",
        )
        try:
            limit = int((query.get("limit") or ["100"])[0])
            offset = int((query.get("offset") or ["0"])[0])
        except ValueError:
            return 400, {"error": "limit/offset must be integers"}
        try:
            rows = self.queue.list_schedules(
                tenant_id=str(tenant_id),
                enabled_only=enabled_only,
                limit=limit,
                offset=offset,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"schedules": [schedule_to_dict(s) for s in rows]}

    def _get_schedule(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 3:
            return 400, {"error": "invalid schedule path"}
        sid = parts[2]
        if not _UUID_RE.match(sid):
            return 400, {"error": "invalid schedule id"}
        tenant_id = (query.get("tenant_id") or [None])[0]
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        sched = self.queue.get_schedule(UUID(sid), tenant_id=str(tenant_id))
        if sched is None:
            return 404, {"error": "schedule not found"}
        return 200, {"schedule": schedule_to_dict(sched)}

    def _completed_by_schedule(self) -> tuple[int, dict[str, Any]]:
        with self.queue._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT schedule_id::text AS schedule_id, COUNT(*)::int AS completed
                FROM jobs
                WHERE schedule_id IS NOT NULL
                  AND status = %s
                GROUP BY schedule_id
                ORDER BY schedule_id
                """,
                (JobStatus.COMPLETED.value,),
            ).fetchall()
            scheds = conn.execute(
                """
                SELECT id::text AS id, tenant_id, name, enabled,
                       interval_seconds, cron_expr, timezone
                FROM schedules
                ORDER BY tenant_id
                """
            ).fetchall()
        by = {r["schedule_id"]: int(r["completed"]) for r in rows}
        return 200, {
            "completed_by_schedule": by,
            "schedules": [dict(s) for s in scheds],
        }
