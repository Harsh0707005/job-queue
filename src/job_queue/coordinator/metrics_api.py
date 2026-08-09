"""Coordinator monitoring / metrics API handlers."""

from __future__ import annotations

from typing import Any

from job_queue.codec import tenant_usage_to_dict


class MetricsApiMixin:
    def _metrics_snapshot(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        try:
            tenant = (query.get("tenant_id") or query.get("tenant") or [None])[0]
            window = int((query.get("window_seconds") or ["300"])[0])
            fmt = (query.get("format") or ["json"])[0]
            export = self.queue.export_metrics(
                kind="snapshot",
                format=fmt,
                tenant_id=tenant or None,
                window_seconds=window,
            )
        except (ValueError, RuntimeError) as exc:
            return 400, {"error": str(exc)}
        return 200, export

    def _metrics_history(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        try:
            tenant = (query.get("tenant_id") or query.get("tenant") or [None])[0]
            start = (query.get("start") or [None])[0]
            end = (query.get("end") or [None])[0]
            fmt = (query.get("format") or ["json"])[0]
            export = self.queue.export_metrics(
                kind="history",
                format=fmt,
                tenant_id=tenant or None,
                start=start,
                end=end,
            )
        except (ValueError, RuntimeError) as exc:
            return 400, {"error": str(exc)}
        return 200, export

    def _metrics_noisy(self, query: dict[str, list[str]]) -> tuple[int, dict[str, Any]]:
        try:
            window = int((query.get("window_seconds") or ["300"])[0])
            min_rate = float((query.get("min_rate") or ["0.5"])[0])
            limit = int((query.get("limit") or ["50"])[0])
            fmt = (query.get("format") or ["json"])[0]
            export = self.queue.export_metrics(
                kind="noisy",
                format=fmt,
                window_seconds=window,
                min_rate=min_rate,
                limit=limit,
            )
        except (ValueError, RuntimeError) as exc:
            return 400, {"error": str(exc)}
        return 200, export

    def _metrics_sample(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            tenant = body.get("tenant_id")
            snaps = self.queue.sample_metrics(tenant_id=str(tenant) if tenant else None)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        # Serialize datetimes
        out = []
        for s in snaps:
            row = dict(s)
            if hasattr(row.get("sampled_at"), "isoformat"):
                row["sampled_at"] = row["sampled_at"].isoformat()
            out.append(row)
        return 200, {"snapshots": out, "count": len(out)}

    def _metrics_purge(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            limit = int(body.get("limit") or 5000)
            result = self.queue.purge_expired_metrics(limit=limit)
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 200, result

    def _get_metrics_retention(self) -> tuple[int, dict[str, Any]]:
        return 200, {
            "metrics_retention_seconds": self.queue.get_metrics_retention()
        }

    def _set_metrics_retention(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        if (
            "metrics_retention_seconds" not in body
            and "seconds" not in body
            and "metrics_seconds" not in body
        ):
            return 400, {
                "error": "metrics_retention_seconds required (null = keep forever)"
            }
        try:
            raw = body.get(
                "metrics_retention_seconds",
                body.get("metrics_seconds", body.get("seconds")),
            )
            seconds = None if raw is None else int(raw)
            applied = self.queue.set_metrics_retention(seconds)
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 200, {"metrics_retention_seconds": applied}

    def _usage_get(self, query: dict[str, list[str]]) -> tuple[int, dict[str, Any]]:
        tenant = (query.get("tenant_id") or query.get("tenant") or [None])[0]
        start = (query.get("start") or [None])[0]
        end = (query.get("end") or [None])[0]
        try:
            if tenant:
                usage = self.queue.get_tenant_usage(
                    str(tenant), start=start, end=end
                )
                return 200, {"usage": tenant_usage_to_dict(usage)}
            rows = self.queue.list_tenant_usage(start=start, end=end)
            return 200, {
                "usage": [tenant_usage_to_dict(u) for u in rows],
                "count": len(rows),
            }
        except ValueError as exc:
            return 400, {"error": str(exc)}

    def _usage_buckets(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        tenant = (query.get("tenant_id") or query.get("tenant") or [None])[0]
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        start = (query.get("start") or [None])[0]
        end = (query.get("end") or [None])[0]
        try:
            limit = int((query.get("limit") or ["500"])[0])
            buckets = self.queue.list_tenant_usage_buckets(
                str(tenant), start=start, end=end, limit=limit
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"tenant_id": str(tenant), "buckets": buckets, "count": len(buckets)}
