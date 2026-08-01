"""CoordinatorClient core: construction, failover request, health."""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from typing import Any

from job_queue.models import Job
from job_queue.queue import LeaseError


class _ActiveRemoteJob:
    __slots__ = ("job", "lease_lost", "stop_hb")

    def __init__(
        self,
        job: Job,
        lease_lost: threading.Event,
        stop_hb: threading.Event,
    ) -> None:
        self.job = job
        self.lease_lost = lease_lost
        self.stop_hb = stop_hb


class ClientCoreMixin:
    def __init__(
        self,
        base_url: str | list[str],
        *,
        timeout: float = 30.0,
        api_token: str | None = None,
    ) -> None:
        if isinstance(base_url, str):
            urls = [u.strip() for u in base_url.split(",") if u.strip()]
        else:
            urls = list(base_url)
        if not urls:
            raise ValueError("at least one coordinator URL is required")
        self.base_urls = [u.rstrip("/") for u in urls]
        self.timeout = timeout
        # Prefer explicit token; else env (same keys as the coordinator).
        if api_token is not None:
            self.api_token = api_token or None
        else:
            import os

            self.api_token = (
                os.environ.get("JOB_QUEUE_API_TOKEN")
                or os.environ.get("COORDINATOR_TOKEN")
                or None
            )
        self._rr = 0
        self._lock = threading.Lock()

    @property
    def base_url(self) -> str:
        return self.base_urls[0]

    def _next_order(self) -> list[str]:
        with self._lock:
            n = len(self.base_urls)
            start = self._rr % n
            self._rr += 1
        return self.base_urls[start:] + self.base_urls[:start]

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        allow_204: bool = False,
    ) -> dict[str, Any] | None:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, default=str).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"

        last_err: Exception | None = None
        for base in self._next_order():
            url = f"{base}{path}"
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    code = int(resp.getcode())
                    raw = resp.read()
                    if code == 204 or (allow_204 and not raw):
                        return None
                    if not raw:
                        return {}
                    return json.loads(raw.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                raw = exc.read() if exc.fp else b""
                try:
                    payload = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError:
                    payload = {
                        "error": raw.decode("utf-8", errors="replace") or str(exc)
                    }
                if exc.code == 401:
                    raise RuntimeError(
                        f"coordinator unauthorized for {method} {path}: {payload}"
                    ) from exc
                if exc.code == 409 and payload.get("code") == "lease_error":
                    raise LeaseError(payload.get("error") or "lease error") from exc
                if exc.code == 409 and payload.get("code") == "approval_error":
                    from job_queue.queue import ApprovalError

                    raise ApprovalError(
                        payload.get("error") or "approval error"
                    ) from exc
                if exc.code == 409 and payload.get("code") == "cancel_error":
                    from job_queue.queue import CancelError

                    raise CancelError(payload.get("error") or "cancel error") from exc
                if exc.code == 409 and payload.get("code") == "replay_error":
                    from job_queue.queue import ReplayError

                    raise ReplayError(payload.get("error") or "replay error") from exc
                if int(exc.code) == 404:
                    return {"error": payload.get("error") or "not found", "_http": 404}
                # 5xx → try next coordinator; 4xx is definitive.
                if 500 <= int(exc.code) < 600:
                    last_err = RuntimeError(
                        f"coordinator {method} {path} -> {exc.code}: {payload}"
                    )
                    continue
                raise RuntimeError(
                    f"coordinator {method} {path} -> {exc.code}: {payload}"
                ) from exc
            except urllib.error.URLError as exc:
                last_err = RuntimeError(f"coordinator {base} unreachable: {exc}")
                continue
        raise RuntimeError(f"all coordinators failed for {method} {path}: {last_err}")

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health") or {}

    def wait_healthy(self, *, timeout: float = 120.0, interval: float = 1.0) -> None:
        deadline = time.time() + timeout
        last: Exception | None = None
        while time.time() < deadline:
            try:
                h = self.health()
                if h.get("ok"):
                    return
            except Exception as exc:  # noqa: BLE001
                last = exc
            time.sleep(interval)
        raise RuntimeError(f"coordinator not healthy within {timeout}s: {last}")
