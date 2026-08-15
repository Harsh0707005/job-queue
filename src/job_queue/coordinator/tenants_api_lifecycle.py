"""Coordinator tenant pause/offboard/merge/residue API handlers."""

from __future__ import annotations

from typing import Any

from uuid import UUID

from job_queue.codec import (
    audit_consistency_report_to_dict,
    job_audit_event_to_dict,
    payload_key_rotation_to_dict,
    tenant_merge_result_to_dict,
    tenant_split_plan_to_dict,
    tenant_split_result_to_dict,
    tenant_offboard_result_to_dict,
    tenant_payload_key_to_dict,
    tenant_restore_to_dict,
    tenant_snapshot_to_dict,
)
from job_queue.queue import RestoreError


class TenantsApiLifecycleMixin:
    def _tenant_audit(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant audit path"}
        tenant_id = parts[2]
        limit = int((query.get("limit") or ["500"])[0])
        try:
            events = self.queue.list_tenant_audit(tenant_id, limit=limit)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "events": [job_audit_event_to_dict(e) for e in events],
            "count": len(events),
        }

    def _tenant_audit_verify(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 5:
            return 400, {"error": "invalid tenant audit verify path"}
        tenant_id = parts[2]
        limit = int((query.get("limit") or ["10000"])[0])
        reports = self.queue.verify_tenant_audit(tenant_id, limit=limit)
        bad = [r for r in reports if not r.ok]
        return 200, {
            "ok": len(bad) == 0,
            "checked": len(reports),
            "mismatches": len(bad),
            "reports": [
                audit_consistency_report_to_dict(r) for r in (bad or reports[:20])
            ],
        }

    def _pause_tenant(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / pause
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        self.queue.pause_tenant(tenant_id)
        return 200, {"tenant_id": tenant_id, "paused": True}

    def _resume_tenant(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        try:
            self.queue.resume_tenant(tenant_id)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"tenant_id": tenant_id, "paused": False}

    def _drain_tenant_queue(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        raw = body.get("queue_name", body.get("queue"))
        if raw is None or str(raw).strip() == "":
            return 400, {"error": "queue_name is required"}
        try:
            queues = self.queue.drain_tenant_queue(tenant_id, str(raw))
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "tenant_id": tenant_id,
            "queue_name": str(raw).strip(),
            "draining": True,
            "draining_queues": queues,
        }

    def _undrain_tenant_queue(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        raw = body.get("queue_name", body.get("queue"))
        if raw is None or str(raw).strip() == "":
            return 400, {"error": "queue_name is required"}
        try:
            queues = self.queue.undrain_tenant_queue(tenant_id, str(raw))
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "tenant_id": tenant_id,
            "queue_name": str(raw).strip(),
            "draining": False,
            "draining_queues": queues,
        }

    def _list_draining_queues(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        try:
            queues = self.queue.list_draining_queues(tenant_id)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"tenant_id": tenant_id, "draining_queues": queues}

    def _offboard_tenant(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / offboard
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        try:
            result = self.queue.offboard_tenant(tenant_id)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "result": tenant_offboard_result_to_dict(result),
            "tenant_id": tenant_id,
            "empty": result.empty,
            "offboarding": result.offboarding,
            "leased_remaining": result.leased_remaining,
            "webhooks_delivering_remaining": result.webhooks_delivering_remaining,
        }

    def _merge_tenant(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {source_id} / merge
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        source_tenant_id = parts[2]
        target_tenant_id = body.get("target_tenant_id") or body.get("into")
        if not target_tenant_id:
            return 400, {"error": "target_tenant_id required"}
        try:
            result = self.queue.merge_tenant(source_tenant_id, str(target_tenant_id))
        except ValueError as exc:
            return 400, {"error": str(exc)}
        except Exception as exc:
            # MergeError (and unexpected) surface as 409 conflict / 400.
            from job_queue.queue import MergeError

            if isinstance(exc, MergeError):
                return 409, {"error": str(exc), "code": "merge_error"}
            raise
        return 200, {
            "result": tenant_merge_result_to_dict(result),
            "source_tenant_id": result.source_tenant_id,
            "target_tenant_id": result.target_tenant_id,
            "source_empty": result.source_empty,
            "jobs_moved": result.jobs_moved,
            "jobs_leased_moved": result.jobs_leased_moved,
        }

    def _plan_tenant_split(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {source} / split / plan
        if len(parts) != 5:
            return 400, {"error": "invalid tenant split path"}
        source = parts[2]
        dest = body.get("target_tenant_id") or body.get("to_tenant_id")
        if not dest:
            return 400, {"error": "target_tenant_id is required"}
        try:
            plan = self.queue.plan_tenant_split(
                source,
                str(dest),
                queue_name=(
                    str(body["queue_name"])
                    if body.get("queue_name") is not None
                    else None
                ),
                labels=body.get("labels"),
                required_skills=body.get("required_skills"),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, tenant_split_plan_to_dict(plan)

    def _split_tenant(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {source} / split
        if len(parts) != 4:
            return 400, {"error": "invalid tenant split path"}
        source = parts[2]
        dest = body.get("target_tenant_id") or body.get("to_tenant_id")
        if not dest:
            return 400, {"error": "target_tenant_id is required"}
        try:
            result = self.queue.split_tenant(
                source,
                str(dest),
                queue_name=(
                    str(body["queue_name"])
                    if body.get("queue_name") is not None
                    else None
                ),
                labels=body.get("labels"),
                required_skills=body.get("required_skills"),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        except Exception as exc:
            from job_queue.queue import QueueDrainError, TenantSplitError

            if isinstance(exc, QueueDrainError):
                return 409, {
                    "error": str(exc),
                    "code": "queue_draining",
                    "tenant_id": exc.tenant_id,
                    "queue_name": exc.queue_name,
                }
            if isinstance(exc, TenantSplitError):
                payload: dict[str, Any] = {
                    "error": str(exc),
                    "code": "tenant_split_error",
                }
                if getattr(exc, "plan", None) is not None:
                    payload["plan"] = tenant_split_plan_to_dict(exc.plan)
                return 409, payload
            raise
        return 200, tenant_split_result_to_dict(result)

    def _export_tenant(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / export
        if len(parts) != 4:
            return 400, {"error": "invalid export path"}
        tenant_id = parts[2]
        try:
            snap = self.queue.export_tenant(tenant_id)
        except RestoreError as exc:
            return 409, {"error": str(exc), "code": "export_error"}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"snapshot": tenant_snapshot_to_dict(snap)}

    def _get_snapshot(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / snapshots / {id}
        if len(parts) != 3:
            return 400, {"error": "invalid snapshot path"}
        try:
            sid = UUID(parts[2])
        except ValueError:
            return 400, {"error": "invalid snapshot id"}
        snap = self.queue.get_tenant_snapshot(sid)
        if snap is None:
            return 404, {"error": "snapshot not found"}
        return 200, {"snapshot": tenant_snapshot_to_dict(snap)}

    def _restore_tenant(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / restore
        if len(parts) != 4:
            return 400, {"error": "invalid restore path"}
        target = parts[2]
        raw = body.get("snapshot_id")
        if not raw:
            return 400, {"error": "snapshot_id is required"}
        try:
            sid = UUID(str(raw))
        except ValueError:
            return 400, {"error": "invalid snapshot_id"}
        try:
            rst = self.queue.restore_tenant(
                target,
                snapshot_id=sid,
                batch_size=int(body.get("batch_size") or 100),
                run_until_complete=bool(body.get("run_until_complete") or False),
            )
        except RestoreError as exc:
            return 409, {"error": str(exc), "code": "restore_error"}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"restore": tenant_restore_to_dict(rst)}

    def _advance_tenant_restore(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / restore / advance
        if len(parts) != 5:
            return 400, {"error": "invalid restore advance path"}
        target = parts[2]
        try:
            rst = self.queue.advance_tenant_restore(
                target, batch_size=int(body.get("batch_size") or 100)
            )
        except RestoreError as exc:
            return 409, {"error": str(exc), "code": "restore_error"}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"restore": tenant_restore_to_dict(rst)}

    def _get_tenant_restore(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / restore
        if len(parts) != 4:
            return 400, {"error": "invalid restore path"}
        rst = self.queue.get_tenant_restore(parts[2])
        if rst is None:
            return 404, {"error": "restore not found"}
        return 200, {"restore": tenant_restore_to_dict(rst)}

    def _advance_all_restores(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        try:
            rots = self.queue.process_tenant_restores(
                limit=int(body.get("limit") or 20),
                batch_size=int(body.get("batch_size") or 100),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "count": len(rots),
            "restores": [tenant_restore_to_dict(r) for r in rots],
        }

    def _list_tenant_payload_keys(
        self, path: str
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / payload-keys
        if len(parts) != 4:
            return 400, {"error": "invalid payload-keys path"}
        tenant_id = parts[2]
        try:
            keys = self.queue.list_tenant_payload_keys(tenant_id)
            rot = self.queue.get_payload_key_rotation(tenant_id)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "tenant_id": tenant_id,
            "keys": [tenant_payload_key_to_dict(k) for k in keys],
            "rotation": (
                None if rot is None else payload_key_rotation_to_dict(rot)
            ),
        }

    def _rotate_tenant_payload_key(
        self, path: str
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / payload-keys / rotate
        if len(parts) != 5:
            return 400, {"error": "invalid payload-keys rotate path"}
        tenant_id = parts[2]
        try:
            rot = self.queue.rotate_tenant_payload_key(tenant_id)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"rotation": payload_key_rotation_to_dict(rot)}

    def _advance_tenant_payload_key(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / payload-keys / advance
        if len(parts) != 5:
            return 400, {"error": "invalid payload-keys advance path"}
        tenant_id = parts[2]
        try:
            batch = int(body.get("batch_size") or 200)
            rot = self.queue.advance_payload_key_rotation(
                tenant_id, batch_size=batch
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"rotation": payload_key_rotation_to_dict(rot)}

    def _advance_all_payload_keys(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        try:
            limit = int(body.get("limit") or 20)
            batch = int(body.get("batch_size") or 200)
            tenant_id = body.get("tenant_id")
            rots = self.queue.process_payload_key_rotations(
                limit=limit,
                batch_size=batch,
                tenant_id=str(tenant_id) if tenant_id else None,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "count": len(rots),
            "rotations": [payload_key_rotation_to_dict(r) for r in rots],
        }

    def _tenant_residue(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        """Per-tenant residual state for offboard/empty soak verification.

        Pass ``?tenant=id`` for one tenant, or omit for every tenant that still
        has any jobs / schedules / webhooks / settings / fairness row.
        """
        single = (query.get("tenant") or [None])[0]

        def residue_for(conn: Any, tenant_id: str) -> dict[str, Any]:
            jobs = conn.execute(
                """
                SELECT
                  COUNT(*)::int AS total,
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'leased')::int AS leased,
                  COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
                  COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled,
                  COUNT(*) FILTER (WHERE status = 'dead')::int AS dead,
                  COUNT(*) FILTER (WHERE status = 'failed')::int AS failed
                FROM jobs
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            schedules = conn.execute(
                "SELECT COUNT(*)::int AS n FROM schedules WHERE tenant_id = %s",
                (tenant_id,),
            ).fetchone()
            endpoints = conn.execute(
                """
                SELECT COUNT(*)::int AS n
                FROM webhook_endpoints
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            deliveries = conn.execute(
                """
                SELECT
                  COUNT(*)::int AS total,
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'delivering')::int AS delivering,
                  COUNT(*) FILTER (WHERE status = 'succeeded')::int AS succeeded,
                  COUNT(*) FILTER (WHERE status = 'dead')::int AS dead
                FROM webhook_deliveries
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            settings = conn.execute(
                """
                SELECT paused, offboarding, breaker_opened_at
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            fairness = conn.execute(
                """
                SELECT 1 FROM tenant_fairness WHERE tenant_id = %s LIMIT 1
                """,
                (tenant_id,),
            ).fetchone()
            j_total = int(jobs["total"]) if jobs else 0
            s_n = int(schedules["n"]) if schedules else 0
            e_n = int(endpoints["n"]) if endpoints else 0
            d_total = int(deliveries["total"]) if deliveries else 0
            has_settings = settings is not None
            has_fairness = fairness is not None
            empty = (
                j_total == 0
                and s_n == 0
                and e_n == 0
                and d_total == 0
                and not has_settings
                and not has_fairness
            )
            return {
                "tenant_id": tenant_id,
                "empty": empty,
                "offboarding": bool(settings["offboarding"]) if settings else False,
                "paused": bool(settings["paused"]) if settings else False,
                "breaker_open": bool(settings["breaker_opened_at"])
                if settings
                else False,
                "jobs": dict(jobs) if jobs else {},
                "schedules": s_n,
                "webhook_endpoints": e_n,
                "webhook_deliveries": dict(deliveries) if deliveries else {},
                "has_settings": has_settings,
                "has_fairness": has_fairness,
            }

        with self.queue._pool.connection() as conn:
            if single:
                return 200, {"tenants": [residue_for(conn, str(single))]}
            rows = conn.execute(
                """
                SELECT tenant_id FROM jobs
                UNION
                SELECT tenant_id FROM schedules
                UNION
                SELECT tenant_id FROM webhook_endpoints
                UNION
                SELECT tenant_id FROM webhook_deliveries
                UNION
                SELECT tenant_id FROM tenant_settings
                UNION
                SELECT tenant_id FROM tenant_fairness
                ORDER BY tenant_id
                """
            ).fetchall()
            tenants = [residue_for(conn, r["tenant_id"]) for r in rows]
            return 200, {"tenants": tenants}
