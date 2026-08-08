"""Coordinator worker partition / rebalance API."""

from __future__ import annotations

from typing import Any


class PartitionsApiMixin:
    def _register_worker(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        wid = body.get("worker_id")
        if not wid:
            return 400, {"error": "worker_id is required"}
        try:
            out = self.queue.register_worker(
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
