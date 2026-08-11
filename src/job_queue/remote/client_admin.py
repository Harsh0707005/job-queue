"""CoordinatorClient admin: tenants, schedules, webhooks, dead letters, retention, stats."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from job_queue.codec import job_from_dict
from job_queue.models import Job


class ClientAdminMixin:
    def process_expired_jobs(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/jobs/process-expired", body)
        assert payload is not None
        return payload

    def set_tenant_job_ttl(
        self,
        tenant_id: str,
        ttl_seconds: int | None,
        *,
        expiry_action: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"ttl_seconds": ttl_seconds}
        if expiry_action is not None:
            body["expiry_action"] = expiry_action
        payload = self._request("POST", f"/v1/tenants/{tenant_id}/job-ttl", body)
        assert payload is not None
        return payload

    def get_tenant_job_ttl(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("GET", f"/v1/tenants/{tenant_id}/job-ttl")
        assert payload is not None
        return payload

    def set_tenant_quarantine(
        self,
        tenant_id: str,
        max_worker_hops: int | None,
        *,
        action: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"max_worker_hops": max_worker_hops}
        if action is not None:
            body["action"] = action
        payload = self._request(
            "POST", f"/v1/tenants/{tenant_id}/quarantine", body
        )
        assert payload is not None
        return payload

    def get_tenant_quarantine(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("GET", f"/v1/tenants/{tenant_id}/quarantine")
        assert payload is not None
        return payload

    def set_tenant_spend_cap(
        self,
        tenant_id: str,
        *,
        window_seconds: int | None = None,
        max_enqueues: int | None = None,
        max_runtime_ms: int | None = None,
        clear: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"clear": clear}
        if window_seconds is not None:
            body["window_seconds"] = window_seconds
        if max_enqueues is not None:
            body["max_enqueues"] = max_enqueues
        if max_runtime_ms is not None:
            body["max_runtime_ms"] = max_runtime_ms
        payload = self._request(
            "POST", f"/v1/tenants/{tenant_id}/spend-cap", body
        )
        assert payload is not None
        return payload

    def get_tenant_spend_cap(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("GET", f"/v1/tenants/{tenant_id}/spend-cap")
        assert payload is not None
        return payload

    def get_tenant_usage(
        self,
        tenant_id: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> dict[str, Any]:
        q = [f"tenant_id={tenant_id}"]
        if start:
            q.append(f"start={start}")
        if end:
            q.append(f"end={end}")
        payload = self._request("GET", "/v1/usage?" + "&".join(q))
        assert payload is not None
        return payload.get("usage") or payload

    def list_tenant_usage(
        self,
        *,
        start: str | None = None,
        end: str | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        parts: list[str] = []
        if tenant_id:
            parts.append(f"tenant_id={tenant_id}")
        if start:
            parts.append(f"start={start}")
        if end:
            parts.append(f"end={end}")
        path = "/v1/usage" + (("?" + "&".join(parts)) if parts else "")
        payload = self._request("GET", path)
        assert payload is not None
        return payload

    def list_tenant_usage_buckets(
        self,
        tenant_id: str,
        *,
        start: str | None = None,
        end: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        q = [f"tenant_id={tenant_id}", f"limit={int(limit)}"]
        if start:
            q.append(f"start={start}")
        if end:
            q.append(f"end={end}")
        payload = self._request("GET", "/v1/usage/buckets?" + "&".join(q))
        assert payload is not None
        return payload

    def process_due_schedules(
        self, *, tenant_id: str | None = None, limit: int = 100
    ) -> int:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/schedules/process-due", body)
        assert payload is not None
        return int(payload.get("count") or 0)

    def claim_webhooks(
        self,
        *,
        limit: int = 50,
        http_timeout_seconds: float = 10.0,
        claim_seconds: float | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "limit": limit,
            "http_timeout_seconds": http_timeout_seconds,
        }
        if claim_seconds is not None:
            body["claim_seconds"] = claim_seconds
        payload = self._request("POST", "/v1/webhooks/claim", body)
        assert payload is not None
        return payload

    def finalize_webhook(
        self,
        delivery_id: str | UUID,
        *,
        status_code: int | None = None,
        error: str | None = None,
        endpoint_missing: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"endpoint_missing": endpoint_missing}
        if status_code is not None:
            body["status_code"] = status_code
        if error is not None:
            body["error"] = error
        payload = self._request("POST", f"/v1/webhooks/{delivery_id}/finalize", body)
        assert payload is not None
        return payload

    def pause_tenant(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("POST", f"/v1/tenants/{tenant_id}/pause", {})
        assert payload is not None
        return payload

    def resume_tenant(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("POST", f"/v1/tenants/{tenant_id}/resume", {})
        assert payload is not None
        return payload

    def offboard_tenant(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("POST", f"/v1/tenants/{tenant_id}/offboard", {})
        assert payload is not None
        return payload

    def merge_tenant(
        self, source_tenant_id: str, target_tenant_id: str
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/tenants/{source_tenant_id}/merge",
            {"target_tenant_id": target_tenant_id},
        )
        assert payload is not None
        return payload

    def list_tenant_payload_keys(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("GET", f"/v1/tenants/{tenant_id}/payload-keys")
        assert payload is not None
        return payload

    def rotate_tenant_payload_key(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request(
            "POST", f"/v1/tenants/{tenant_id}/payload-keys/rotate", {}
        )
        assert payload is not None
        return payload

    def advance_tenant_payload_key(
        self, tenant_id: str, *, batch_size: int = 200
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/tenants/{tenant_id}/payload-keys/advance",
            {"batch_size": batch_size},
        )
        assert payload is not None
        return payload

    def export_tenant(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("POST", f"/v1/tenants/{tenant_id}/export", {})
        assert payload is not None
        return payload

    def restore_tenant(
        self,
        target_tenant_id: str,
        *,
        snapshot_id: str | UUID,
        batch_size: int = 100,
        run_until_complete: bool = False,
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/tenants/{target_tenant_id}/restore",
            {
                "snapshot_id": str(snapshot_id),
                "batch_size": batch_size,
                "run_until_complete": run_until_complete,
            },
        )
        assert payload is not None
        return payload

    def advance_tenant_restore(
        self, target_tenant_id: str, *, batch_size: int = 100
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/tenants/{target_tenant_id}/restore/advance",
            {"batch_size": batch_size},
        )
        assert payload is not None
        return payload

    def get_tenant_restore(self, target_tenant_id: str) -> dict[str, Any]:
        payload = self._request("GET", f"/v1/tenants/{target_tenant_id}/restore")
        assert payload is not None
        return payload

    def process_tenant_restores(
        self, *, limit: int = 20, batch_size: int = 100
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            "/v1/admin/restores/advance",
            {"limit": limit, "batch_size": batch_size},
        )
        assert payload is not None
        return payload

    def process_payload_key_rotations(
        self,
        *,
        limit: int = 20,
        batch_size: int = 200,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"limit": limit, "batch_size": batch_size}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/admin/payload-keys/advance", body)
        assert payload is not None
        return payload

    def create_schedule(self, **kwargs: Any) -> dict[str, Any]:
        payload = self._request("POST", "/v1/schedules", kwargs)
        assert payload is not None
        return payload["schedule"]

    def rotate_webhook_secret(
        self,
        endpoint_id: str | UUID,
        *,
        tenant_id: str,
        secret: str | None,
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/webhooks/{endpoint_id}/rotate-secret",
            {"tenant_id": tenant_id, "secret": secret},
        )
        assert payload is not None
        return payload

    def register_webhook(self, **kwargs: Any) -> dict[str, Any]:
        payload = self._request("POST", "/v1/webhooks", kwargs)
        assert payload is not None
        return payload["endpoint"]

    def pause_schedule(self, schedule_id: str, *, tenant_id: str) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/schedules/{schedule_id}/pause",
            {"tenant_id": tenant_id},
        )
        assert payload is not None
        return payload["schedule"]

    def resume_schedule(self, schedule_id: str, *, tenant_id: str) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/schedules/{schedule_id}/resume",
            {"tenant_id": tenant_id},
        )
        assert payload is not None
        return payload["schedule"]

    def delete_schedule(self, schedule_id: str, *, tenant_id: str) -> bool:
        payload = self._request(
            "POST",
            f"/v1/schedules/{schedule_id}/delete",
            {"tenant_id": tenant_id},
        )
        if payload is None:
            return False
        if payload.get("_http") == 404:
            return False
        return bool(payload.get("deleted"))

    def list_schedules(
        self,
        *,
        tenant_id: str,
        enabled_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        from urllib.parse import urlencode

        q = urlencode(
            {
                "tenant_id": tenant_id,
                "enabled_only": "1" if enabled_only else "0",
                "limit": str(limit),
                "offset": str(offset),
            }
        )
        payload = self._request("GET", f"/v1/schedules?{q}")
        assert payload is not None
        return list(payload.get("schedules") or [])

    def get_schedule(
        self, schedule_id: str | UUID, *, tenant_id: str
    ) -> dict[str, Any] | None:
        from urllib.parse import urlencode

        q = urlencode({"tenant_id": tenant_id})
        payload = self._request("GET", f"/v1/schedules/{schedule_id}?{q}")
        if payload is None or payload.get("_http") == 404:
            return None
        return payload.get("schedule")

    def list_webhooks(self, *, tenant_id: str) -> list[dict[str, Any]]:
        from urllib.parse import urlencode

        q = urlencode({"tenant_id": tenant_id})
        payload = self._request("GET", f"/v1/webhooks?{q}")
        assert payload is not None
        return list(payload.get("endpoints") or [])

    def set_webhook_enabled(
        self,
        endpoint_id: str | UUID,
        *,
        tenant_id: str,
        enabled: bool,
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/webhooks/{endpoint_id}/set-enabled",
            {"tenant_id": tenant_id, "enabled": enabled},
        )
        assert payload is not None
        return payload["endpoint"]

    def delete_webhook(self, endpoint_id: str | UUID, *, tenant_id: str) -> bool:
        payload = self._request(
            "POST",
            f"/v1/webhooks/{endpoint_id}/delete",
            {"tenant_id": tenant_id},
        )
        if payload is None or payload.get("_http") == 404:
            return False
        return bool(payload.get("deleted"))

    def list_dead_letters(
        self,
        *,
        tenant_id: str,
        queue_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        from urllib.parse import urlencode

        q: list[tuple[str, str]] = [
            ("tenant_id", tenant_id),
            ("limit", str(limit)),
            ("offset", str(offset)),
        ]
        if queue_name is not None:
            q.append(("queue_name", queue_name))
        payload = self._request("GET", f"/v1/dead-letters?{urlencode(q)}")
        assert payload is not None
        return [job_from_dict(j) for j in (payload.get("jobs") or [])]

    def replay_dead_letter(
        self,
        job_id: str | UUID,
        *,
        tenant_id: str,
        reset_attempts: bool = True,
        max_attempts: int | None = None,
        delay_seconds: float | int | None = None,
        clear_error: bool = False,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "reset_attempts": reset_attempts,
            "clear_error": clear_error,
        }
        if max_attempts is not None:
            body["max_attempts"] = max_attempts
        if delay_seconds is not None:
            body["delay_seconds"] = delay_seconds
        payload = self._request("POST", f"/v1/dead-letters/{job_id}/replay", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def replay_dead_letters(
        self,
        *,
        tenant_id: str,
        job_ids: list[str | UUID] | None = None,
        queue_name: str | None = None,
        limit: int | None = None,
        reset_attempts: bool = True,
        max_attempts: int | None = None,
        delay_seconds: float | int | None = None,
        clear_error: bool = False,
    ) -> list[Job]:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "reset_attempts": reset_attempts,
            "clear_error": clear_error,
        }
        if job_ids is not None:
            body["job_ids"] = [str(j) for j in job_ids]
        if queue_name is not None:
            body["queue_name"] = queue_name
        if limit is not None:
            body["limit"] = limit
        if max_attempts is not None:
            body["max_attempts"] = max_attempts
        if delay_seconds is not None:
            body["delay_seconds"] = delay_seconds
        payload = self._request("POST", "/v1/dead-letters/replay", body)
        assert payload is not None
        return [job_from_dict(j) for j in (payload.get("jobs") or [])]

    def set_retention(
        self,
        *,
        completed_seconds: int | None = None,
        dead_seconds: int | None = None,
        set_completed: bool = True,
        set_dead: bool = True,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if set_completed:
            body["completed_retention_seconds"] = completed_seconds
        if set_dead:
            body["dead_retention_seconds"] = dead_seconds
        payload = self._request("POST", "/v1/admin/retention", body)
        assert payload is not None
        return payload

    def get_retention(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/admin/retention")
        assert payload is not None
        return payload

    def purge_expired_jobs(
        self,
        *,
        limit: int = 500,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/admin/purge-expired-jobs", body)
        assert payload is not None
        return payload

    def backlog(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/admin/backlog")
        assert payload is not None
        return payload

    def completed_by_schedule(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/admin/completed-by-schedule")
        assert payload is not None
        return payload

    def webhook_stats(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/admin/webhook-stats")
        assert payload is not None
        return payload

    def metrics_snapshot(
        self,
        *,
        tenant_id: str | None = None,
        window_seconds: int = 300,
        format: str = "json",
    ) -> dict[str, Any] | tuple[str, bytes]:
        """Live gauges + throughput. JSON dict, or ``(content_type, body)`` for csv/parquet."""
        from urllib.parse import urlencode

        q: dict[str, Any] = {
            "window_seconds": window_seconds,
            "format": format,
        }
        if tenant_id is not None:
            q["tenant_id"] = tenant_id
        path = f"/v1/metrics/snapshot?{urlencode(q)}"
        if format == "json":
            payload = self._request("GET", path)
            assert payload is not None
            return payload
        return self._request_bytes("GET", path)

    def metrics_history(
        self,
        *,
        tenant_id: str | None = None,
        start: str | None = None,
        end: str | None = None,
        format: str = "json",
    ) -> dict[str, Any] | tuple[str, bytes]:
        from urllib.parse import urlencode

        q: dict[str, Any] = {"format": format}
        if tenant_id is not None:
            q["tenant_id"] = tenant_id
        if start is not None:
            q["start"] = start
        if end is not None:
            q["end"] = end
        path = f"/v1/metrics/history?{urlencode(q)}"
        if format == "json":
            payload = self._request("GET", path)
            assert payload is not None
            return payload
        return self._request_bytes("GET", path)

    def metrics_noisy(
        self,
        *,
        window_seconds: int = 300,
        min_rate: float = 0.5,
        limit: int = 50,
        format: str = "json",
    ) -> dict[str, Any] | tuple[str, bytes]:
        from urllib.parse import urlencode

        q: dict[str, Any] = {
            "window_seconds": window_seconds,
            "min_rate": min_rate,
            "limit": limit,
            "format": format,
        }
        path = f"/v1/metrics/noisy?{urlencode(q)}"
        if format == "json":
            payload = self._request("GET", path)
            assert payload is not None
            return payload
        return self._request_bytes("GET", path)

    def metrics_sample(self, *, tenant_id: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/metrics/sample", body)
        assert payload is not None
        return payload

    def metrics_purge(self, *, limit: int = 5000) -> dict[str, Any]:
        payload = self._request("POST", "/v1/metrics/purge", {"limit": limit})
        assert payload is not None
        return payload

    def get_metrics_retention(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/metrics/retention")
        assert payload is not None
        return payload

    def set_metrics_retention(self, seconds: int | None) -> dict[str, Any]:
        payload = self._request(
            "POST",
            "/v1/metrics/retention",
            {"metrics_retention_seconds": seconds},
        )
        assert payload is not None
        return payload

    def register_worker(
        self,
        worker_id: str,
        *,
        skills: list[str] | None = None,
        heartbeat_ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"worker_id": worker_id}
        if skills is not None:
            body["skills"] = skills
        if heartbeat_ttl_seconds is not None:
            body["heartbeat_ttl_seconds"] = heartbeat_ttl_seconds
        payload = self._request("POST", "/v1/workers/register", body)
        assert payload is not None
        return payload

    def worker_heartbeat(
        self,
        worker_id: str,
        *,
        skills: list[str] | None = None,
        heartbeat_ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"worker_id": worker_id}
        if skills is not None:
            body["skills"] = skills
        if heartbeat_ttl_seconds is not None:
            body["heartbeat_ttl_seconds"] = heartbeat_ttl_seconds
        payload = self._request("POST", "/v1/workers/heartbeat", body)
        assert payload is not None
        return payload

    def deregister_worker(self, worker_id: str) -> dict[str, Any]:
        payload = self._request(
            "POST", "/v1/workers/deregister", {"worker_id": worker_id}
        )
        assert payload is not None
        return payload

    def list_worker_partitions(self, worker_id: str) -> dict[str, Any]:
        from urllib.parse import urlencode

        q = urlencode({"worker_id": worker_id})
        payload = self._request("GET", f"/v1/workers/partitions?{q}")
        assert payload is not None
        return payload

    def list_partition_assignments(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/partitions")
        assert payload is not None
        return payload

    def process_worker_timeouts(
        self, *, heartbeat_ttl_seconds: int | None = None
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if heartbeat_ttl_seconds is not None:
            body["heartbeat_ttl_seconds"] = heartbeat_ttl_seconds
        payload = self._request("POST", "/v1/workers/process-timeouts", body)
        assert payload is not None
        return payload

    def set_partition_count(self, partition_count: int) -> dict[str, Any]:
        payload = self._request(
            "POST", "/v1/partitions/count", {"partition_count": partition_count}
        )
        assert payload is not None
        return payload

    def create_alert_rule(self, **kwargs: Any) -> dict[str, Any]:
        payload = self._request("POST", "/v1/alerts/rules", dict(kwargs))
        assert payload is not None
        return payload

    def list_alert_rules(self, *, enabled_only: bool = False) -> dict[str, Any]:
        from urllib.parse import urlencode

        q = urlencode({"enabled_only": "1" if enabled_only else "0"})
        payload = self._request("GET", f"/v1/alerts/rules?{q}")
        assert payload is not None
        return payload

    def delete_alert_rule(self, rule_id: str | UUID) -> dict[str, Any]:
        payload = self._request("DELETE", f"/v1/alerts/rules/{rule_id}")
        assert payload is not None
        return payload

    def mute_alert_rule(
        self,
        rule_id: str | UUID,
        *,
        duration_seconds: int | None = None,
        muted_until: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if duration_seconds is not None:
            body["duration_seconds"] = duration_seconds
        if muted_until is not None:
            body["muted_until"] = muted_until
        payload = self._request("POST", f"/v1/alerts/rules/{rule_id}/mute", body)
        assert payload is not None
        return payload

    def unmute_alert_rule(self, rule_id: str | UUID) -> dict[str, Any]:
        payload = self._request("POST", f"/v1/alerts/rules/{rule_id}/unmute", {})
        assert payload is not None
        return payload

    def resolve_alert_event(
        self,
        event_id: str | UUID,
        *,
        tenant_id: str,
        resolved_by: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"tenant_id": tenant_id}
        if resolved_by is not None:
            body["resolved_by"] = resolved_by
        if note is not None:
            body["note"] = note
        payload = self._request(
            "POST", f"/v1/alerts/events/{event_id}/resolve", body
        )
        assert payload is not None
        return payload

    def evaluate_alerts(self) -> dict[str, Any]:
        payload = self._request("POST", "/v1/alerts/evaluate", {})
        assert payload is not None
        return payload

    def list_alert_events(
        self,
        *,
        tenant_id: str,
        rule_id: str | UUID | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        """List alert history for one tenant (required). Use ``__overall__`` for fleet-wide."""
        from urllib.parse import urlencode

        if not tenant_id:
            raise ValueError("tenant_id is required")
        q: dict[str, Any] = {
            "tenant_id": tenant_id,
            "limit": limit,
            "offset": offset,
        }
        if rule_id is not None:
            q["rule_id"] = str(rule_id)
        if status is not None:
            q["status"] = status
        payload = self._request("GET", f"/v1/alerts/events?{urlencode(q)}")
        assert payload is not None
        return payload
