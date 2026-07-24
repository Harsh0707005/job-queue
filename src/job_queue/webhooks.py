"""HTTP webhook delivery with durable Postgres outbox semantics."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from job_queue.models import WebhookDelivery
from job_queue.queue import JobQueue, compute_retry_backoff_seconds

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WebhookDeliveryResult:
    delivery: WebhookDelivery
    success: bool
    status_code: int | None
    error: str | None


def sign_payload(secret: str, body: bytes) -> str:
    """HMAC-SHA256 hex digest for webhook body."""
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def post_webhook(
    url: str,
    payload: dict[str, Any],
    *,
    secret: str | None = None,
    timeout_seconds: float = 10.0,
    idempotency_key: str | None = None,
) -> tuple[int, str]:
    """POST JSON payload. Returns (status_code, response_snippet_or_error).

    *idempotency_key* is sent as ``Idempotency-Key`` and
    ``X-Job-Queue-Delivery-Id`` so receivers can treat retries as one event.
    """
    body = json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "job-queue-webhook/0.1",
    }
    if secret:
        headers["X-Job-Queue-Signature"] = sign_payload(secret, body)
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
        headers["X-Job-Queue-Delivery-Id"] = idempotency_key

    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            code = int(resp.getcode())
            raw = resp.read(512)
            text = raw.decode("utf-8", errors="replace")
            return code, text
    except urllib.error.HTTPError as exc:
        raw = exc.read(512) if exc.fp else b""
        text = raw.decode("utf-8", errors="replace")
        return int(exc.code), text or str(exc)
    except Exception as exc:  # noqa: BLE001 — network errors become retryable failures
        return 0, str(exc)


class WebhookDispatcher:
    """Poll due webhook deliveries and POST them with backoff / dead-letter."""

    def __init__(
        self,
        queue: JobQueue,
        *,
        poll_interval_seconds: float = 1.0,
        batch_limit: int = 50,
        http_timeout_seconds: float = 10.0,
        on_result: Callable[[WebhookDeliveryResult], None] | None = None,
    ) -> None:
        self.queue = queue
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_limit = batch_limit
        self.http_timeout_seconds = http_timeout_seconds
        self.on_result = on_result
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run_once(self) -> list[WebhookDeliveryResult]:
        return self.queue.process_webhook_deliveries(
            limit=self.batch_limit,
            http_timeout_seconds=self.http_timeout_seconds,
            on_result=self.on_result,
        )

    def run(self) -> int:
        total = 0
        while not self._stop.is_set():
            results = self.run_once()
            total += len(results)
            self._stop.wait(self.poll_interval_seconds)
        return total
