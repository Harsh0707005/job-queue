"""Coordinator worker partition / rebalance API."""

from __future__ import annotations

from typing import Any

from job_queue.queue import WorkerError


class PartitionsApiMixin:
    def _register_worker(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        wid = body.get("worker_id")
        if not wid:
            return 400, {"error": "worker_id is required"}
        try:
            max_leases = None
            if body.get("max_leases") is not None:
                try:
                    max_leases = int(body["max_leases"])
                except (TypeError, ValueError) as exc:
                    return 400, {"error": f"max_leases must be an integer: {exc}"}
            out = self.queue.register_worker(
                str(wid),
                skills=body.get("skills"),
                heartbeat_ttl_seconds=(
                    int(body["heartbeat_ttl_seconds"])
                    if body.get("heartbeat_ttl_seconds") is not None
                    else None
                ),
                max_leases=max_leases,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, out

    def _worker_heartbeat(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        wid = body.get("worker_id")
        if not wid:
            return 400, {"error": "worker_id is required"}
        try:
            out = self.queue.worker_heartbeat(
                str(wid),
                skills=body.get("skills"),
                heartbeat_ttl_seconds=(
                    int(body["heartbeat_ttl_seconds"])
                    if body.get("heartbeat_ttl_seconds") is not None
                    else None
                ),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, out

    def _deregister_worker(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        wid = body.get("worker_id")
        if not wid:
            return 400, {"error": "worker_id is required"}
        try:
            out = self.queue.deregister_worker(str(wid))
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, out

    def _list_worker_partitions(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        wid = (query.get("worker_id") or [None])[0]
        if not wid:
            return 400, {"error": "worker_id is required"}
        try:
            parts = self.queue.list_worker_partitions(str(wid))
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"worker_id": str(wid), "partitions": parts, "count": len(parts)}

    def _list_partition_assignments(self) -> tuple[int, dict[str, Any]]:
        rows = self.queue.list_partition_assignments()
        return 200, {
            "assignments": [
                {
                    "partition_id": r["partition_id"],
                    "worker_id": r["worker_id"],
                    "generation": r["generation"],
                    "updated_at": (
                        r["updated_at"].isoformat() if r.get("updated_at") else None
                    ),
                }
                for r in rows
            ],
            "count": len(rows),
            "partition_count": self.queue.get_partition_count(),
        }

    def _process_worker_timeouts(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        try:
            n = self.queue.process_worker_timeouts(
                heartbeat_ttl_seconds=(
                    int(body["heartbeat_ttl_seconds"])
                    if body.get("heartbeat_ttl_seconds") is not None
                    else None
                )
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"expired": n}

    def _set_partition_count(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        if body.get("partition_count") is None:
            return 400, {"error": "partition_count is required"}
        try:
            n = self.queue.set_partition_count(int(body["partition_count"]))
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 200, {"partition_count": n}

    def _serialize_worker(self, row: dict[str, Any]) -> dict[str, Any]:
        last_seen = row.get("last_seen_at")
        return {
            "worker_id": row["worker_id"],
            "status": row["status"],
            "draining": bool(row.get("draining")),
            "maintenance": bool(row.get("maintenance")),
            "skills": list(row.get("skills") or []),
            "partitions": list(row.get("partitions") or []),
            "leased": int(row.get("leased") or 0),
            "last_seen_at": (
                last_seen.isoformat() if hasattr(last_seen, "isoformat") else last_seen
            ),
            "partition_count": row.get("partition_count"),
            "generation": row.get("generation"),
            "released": row.get("released"),
            "released_job_ids": list(row.get("released_job_ids") or []),
            "release_held": bool(row.get("release_held"))
            if row.get("release_held") is not None
            else None,
            "max_leases": row.get("max_leases"),
        }

    def _list_workers(self) -> tuple[int, dict[str, Any]]:
        rows = self.queue.list_workers()
        workers = [self._serialize_worker(r) for r in rows]
        draining = [w["worker_id"] for w in workers if w["draining"]]
        return 200, {
            "workers": workers,
            "count": len(workers),
            "draining_workers": draining,
        }

    def _get_worker(self, worker_id: str) -> tuple[int, dict[str, Any]]:
        try:
            row = self.queue.get_worker(worker_id)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        if row is None:
            return 404, {
                "error": f"worker {worker_id!r} is not registered",
                "code": "worker_not_found",
            }
        return 200, self._serialize_worker(row)

    @staticmethod
    def _parse_optional_bool(raw: Any) -> bool | None:
        if raw is None or raw == "":
            return None
        if isinstance(raw, bool):
            return raw
        s = str(raw).strip().lower()
        if s in {"1", "true", "yes", "on"}:
            return True
        if s in {"0", "false", "no", "off"}:
            return False
        raise ValueError("boolean value required")

    def _set_worker_maintenance(
        self,
        worker_id: str,
        *,
        draining: bool,
        body: dict[str, Any] | None = None,
        query: dict[str, list[str]] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        body = body or {}
        query = query or {}
        try:
            release_held = None
            if draining:
                raw = body.get("release_held")
                if raw is None:
                    raw = (query.get("release_held") or [None])[0]
                release_held = self._parse_optional_bool(raw)
            row = self.queue.set_worker_maintenance(
                worker_id, draining=draining, release_held=release_held
            )
        except WorkerError as exc:
            status = 404 if exc.code == "worker_not_found" else 409
            return status, {"error": str(exc), "code": exc.code}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, self._serialize_worker(row)

    def _release_worker_held(
        self, worker_id: str
    ) -> tuple[int, dict[str, Any]]:
        try:
            row = self.queue.release_worker_held_leases(worker_id)
        except WorkerError as exc:
            status = 404 if exc.code == "worker_not_found" else 409
            return status, {"error": str(exc), "code": exc.code}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, self._serialize_worker(row)

    def _get_worker_drain_policy(self) -> tuple[int, dict[str, Any]]:
        return 200, {
            "release_held": self.queue.get_worker_drain_release_held()
        }

    def _set_worker_drain_policy(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        if "release_held" not in body:
            return 400, {"error": "release_held is required"}
        try:
            parsed = self._parse_optional_bool(body.get("release_held"))
        except ValueError as exc:
            return 400, {"error": str(exc)}
        if parsed is None:
            return 400, {"error": "release_held is required"}
        enabled = self.queue.set_worker_drain_release_held(parsed)
        return 200, {"release_held": enabled}

    def _set_worker_max_leases(
        self, worker_id: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        if "max_leases" not in body:
            return 400, {"error": "max_leases is required"}
        raw = body.get("max_leases")
        cap: int | None
        if raw is None or raw == "":
            cap = None
        else:
            try:
                cap = int(raw)
            except (TypeError, ValueError):
                return 400, {"error": "max_leases must be an integer or null"}
        try:
            row = self.queue.set_worker_max_leases(worker_id, cap)
        except WorkerError as exc:
            status = 404 if exc.code == "worker_not_found" else 409
            return status, {"error": str(exc), "code": exc.code}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, self._serialize_worker(row)
