"""Coordinator tenant limit/rate/retention API handlers."""

from __future__ import annotations

from typing import Any

from job_queue.codec import (
    job_retention_to_dict,
    tenant_breaker_to_dict,
    tenant_spend_cap_to_dict,
)


class TenantsApiLimitsMixin:
    def _set_concurrency(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / concurrency
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        if "max_concurrent_leases" not in body and "limit" not in body:
            return 400, {"error": "max_concurrent_leases required (or null to clear)"}
        raw = body.get("max_concurrent_leases", body.get("limit"))
        try:
            limit = None if raw is None else int(raw)
            self.queue.set_concurrency_limit(tenant_id, limit)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "tenant_id": tenant_id,
            "max_concurrent_leases": limit,
        }

    def _set_pending_limit(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / pending-limit
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        if "max_pending_jobs" not in body and "limit" not in body:
            return 400, {"error": "max_pending_jobs required (or null to clear)"}
        raw = body.get("max_pending_jobs", body.get("limit"))
        try:
            limit = None if raw is None else int(raw)
            self.queue.set_pending_limit(tenant_id, limit)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "tenant_id": tenant_id,
            "max_pending_jobs": limit,
        }

    def _set_tenant_job_ttl(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        if "ttl_seconds" not in body and "seconds" not in body:
            return 400, {"error": "ttl_seconds required (null clears the default)"}
        raw = body.get("ttl_seconds", body.get("seconds"))
        try:
            ttl = None if raw is None else int(raw)
            out = self.queue.set_tenant_job_ttl(
                tenant_id,
                ttl,
                expiry_action=body.get("expiry_action"),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, out

    def _get_tenant_job_ttl(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        try:
            return 200, self.queue.get_tenant_job_ttl(parts[2])
        except ValueError as exc:
            return 400, {"error": str(exc)}

    def _set_quarantine(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        if "max_worker_hops" not in body and "hops" not in body:
            return 400, {"error": "max_worker_hops required (null clears)"}
        raw = body.get("max_worker_hops", body.get("hops"))
        try:
            hops = None if raw is None else int(raw)
            out = self.queue.set_tenant_quarantine(
                tenant_id,
                hops,
                action=body.get("action") or body.get("quarantine_action"),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, out

    def _get_quarantine(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        try:
            return 200, self.queue.get_tenant_quarantine(parts[2])
        except ValueError as exc:
            return 400, {"error": str(exc)}

    def _set_spend_cap(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        try:
            if body.get("clear"):
                cap = self.queue.set_tenant_spend_cap(tenant_id, clear=True)
            else:
                max_e = body.get("max_enqueues", body.get("enqueue_limit"))
                max_r = body.get(
                    "max_runtime_ms", body.get("runtime_ms_limit")
                )
                window = body.get("window_seconds", body.get("window"))
                cap = self.queue.set_tenant_spend_cap(
                    tenant_id,
                    window_seconds=None if window is None else int(window),
                    max_enqueues=None if max_e is None else int(max_e),
                    max_runtime_ms=None if max_r is None else int(max_r),
                )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, tenant_spend_cap_to_dict(cap)

    def _set_breaker(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        try:
            if body.get("clear"):
                br = self.queue.set_tenant_breaker(tenant_id, clear=True)
            else:
                raw_t = body.get("failure_threshold", body.get("threshold"))
                raw_w = body.get("window_seconds", body.get("window"))
                raw_c = body.get("cooldown_seconds", body.get("cooldown"))
                if raw_t is None and "failure_threshold" not in body and "threshold" not in body:
                    return 400, {
                        "error": "failure_threshold required (null / clear=true "
                        "disables the breaker)"
                    }
                br = self.queue.set_tenant_breaker(
                    tenant_id,
                    failure_threshold=None if raw_t is None else int(raw_t),
                    window_seconds=None if raw_w is None else int(raw_w),
                    cooldown_seconds=None if raw_c is None else int(raw_c),
                )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, tenant_breaker_to_dict(br)

    def _get_breaker(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        try:
            return 200, tenant_breaker_to_dict(
                self.queue.get_tenant_breaker(parts[2])
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}

    def _reset_breaker(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / breaker / reset
        if len(parts) != 5:
            return 400, {"error": "invalid tenant path"}
        try:
            return 200, tenant_breaker_to_dict(
                self.queue.reset_tenant_breaker(parts[2])
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}

    def _get_spend_cap(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        try:
            return 200, tenant_spend_cap_to_dict(
                self.queue.get_tenant_spend_cap(parts[2])
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}

    def _set_lease_weight(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        if body.get("clear"):
            raw = None
        elif "lease_weight" not in body and "weight" not in body:
            return 400, {
                "error": "lease_weight required (null / clear=true resets to 1)"
            }
        else:
            raw = body.get("lease_weight", body.get("weight"))
        try:
            weight = None if raw is None else int(raw)
            applied = self.queue.set_tenant_lease_weight(tenant_id, weight)
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 200, {"tenant_id": tenant_id, "lease_weight": applied}

    def _get_lease_weight(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        try:
            tenant_id = parts[2]
            return 200, {
                "tenant_id": tenant_id,
                "lease_weight": self.queue.get_tenant_lease_weight(tenant_id),
            }
        except ValueError as exc:
            return 400, {"error": str(exc)}

    def _set_rate_limits(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / rate-limits
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        if (
            "enqueue_per_second" not in body
            and "lease_per_second" not in body
            and "enqueue_rate_per_sec" not in body
            and "lease_rate_per_sec" not in body
        ):
            return 400, {
                "error": "enqueue_per_second and/or lease_per_second required "
                "(null clears that limit)"
            }
        try:
            if "enqueue_per_second" in body or "enqueue_rate_per_sec" in body:
                raw = body.get("enqueue_per_second", body.get("enqueue_rate_per_sec"))
                burst = body.get("enqueue_burst")
                rate = None if raw is None else float(raw)
                self.queue.set_enqueue_rate_limit(
                    tenant_id,
                    rate,
                    burst=None if burst is None else int(burst),
                )
            if "lease_per_second" in body or "lease_rate_per_sec" in body:
                raw = body.get("lease_per_second", body.get("lease_rate_per_sec"))
                burst = body.get("lease_burst")
                rate = None if raw is None else float(raw)
                self.queue.set_lease_rate_limit(
                    tenant_id,
                    rate,
                    burst=None if burst is None else int(burst),
                )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        enq = self.queue.get_enqueue_rate_limit(tenant_id)
        lease = self.queue.get_lease_rate_limit(tenant_id)
        return 200, {
            "tenant_id": tenant_id,
            "enqueue_rate_per_sec": enq[0],
            "enqueue_burst": enq[1],
            "lease_rate_per_sec": lease[0],
            "lease_burst": lease[1],
        }

    def _get_rate_limits(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        enq = self.queue.get_enqueue_rate_limit(tenant_id)
        lease = self.queue.get_lease_rate_limit(tenant_id)
        return 200, {
            "tenant_id": tenant_id,
            "enqueue_rate_per_sec": enq[0],
            "enqueue_burst": enq[1],
            "lease_rate_per_sec": lease[0],
            "lease_burst": lease[1],
        }

    def _set_tenant_retention(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / retention
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        if body.get("clear"):
            try:
                settings = self.queue.set_tenant_job_retention(tenant_id, clear=True)
            except ValueError as exc:
                return 400, {"error": str(exc)}
            out = job_retention_to_dict(settings)
            out["tenant_id"] = tenant_id
            return 200, out
        kwargs: dict[str, Any] = {}
        if "completed_retention_seconds" in body or "completed_seconds" in body:
            c_raw = body.get(
                "completed_retention_seconds", body.get("completed_seconds")
            )
            kwargs["completed_seconds"] = None if c_raw is None else int(c_raw)
        if "dead_retention_seconds" in body or "dead_seconds" in body:
            d_raw = body.get("dead_retention_seconds", body.get("dead_seconds"))
            kwargs["dead_seconds"] = None if d_raw is None else int(d_raw)
        if not kwargs:
            return 400, {
                "error": "completed_retention_seconds and/or "
                "dead_retention_seconds required (null = keep forever "
                "for this tenant); or clear=true to inherit global"
            }
        try:
            settings = self.queue.set_tenant_job_retention(tenant_id, **kwargs)
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        out = job_retention_to_dict(settings)
        out["tenant_id"] = tenant_id
        return 200, out

    def _get_tenant_retention(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        settings = self.queue.get_tenant_job_retention(tenant_id)
        out = job_retention_to_dict(settings)
        out["tenant_id"] = tenant_id
        return 200, out

    def _set_retention(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        has_job = (
            "completed_retention_seconds" in body
            or "completed_seconds" in body
            or "dead_retention_seconds" in body
            or "dead_seconds" in body
        )
        has_metrics = (
            "metrics_retention_seconds" in body or "metrics_seconds" in body
        )
        if not has_job and not has_metrics:
            return 400, {
                "error": "completed_retention_seconds, dead_retention_seconds, "
                "and/or metrics_retention_seconds required (null = keep forever)"
            }
        try:
            c_raw = body.get(
                "completed_retention_seconds", body.get("completed_seconds")
            )
            d_raw = body.get("dead_retention_seconds", body.get("dead_seconds"))
            m_raw = body.get("metrics_retention_seconds", body.get("metrics_seconds"))
            # Allow partial update: only provided keys change.
            current = self.queue.get_job_retention()
            completed = (
                current.completed_retention_seconds
                if "completed_retention_seconds" not in body
                and "completed_seconds" not in body
                else (None if c_raw is None else int(c_raw))
            )
            dead = (
                current.dead_retention_seconds
                if "dead_retention_seconds" not in body and "dead_seconds" not in body
                else (None if d_raw is None else int(d_raw))
            )
            kwargs: dict[str, Any] = {
                "completed_seconds": completed,
                "dead_seconds": dead,
            }
            if has_metrics:
                kwargs["metrics_seconds"] = None if m_raw is None else int(m_raw)
            settings = self.queue.set_job_retention(**kwargs)
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 200, job_retention_to_dict(settings)

    def _get_retention(self) -> tuple[int, dict[str, Any]]:
        return 200, job_retention_to_dict(self.queue.get_job_retention())
