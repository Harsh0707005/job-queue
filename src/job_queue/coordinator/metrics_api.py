"""Coordinator monitoring / metrics API handlers."""

from __future__ import annotations

from typing import Any


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
