"""HTTP webhook delivery with durable Postgres outbox semantics.

Signing
-------
When an endpoint has a ``secret``, each delivery is HMAC-SHA256 signed over
the exact JSON body bytes. Header::

    X-Job-Queue-Signature: v1=<hex>

Receivers should call :func:`verify_webhook_signature` with the secret they
were given. The secret used for a delivery is **snapshotted** onto the
outbox row at enqueue time (``signing_secret``), so if the endpoint secret
is rotated mid-flight, retries of in-progress deliveries still verify under
the original secret.
"""

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

SIGNATURE_HEADER = "X-Job-Queue-Signature"
SIGNATURE_VERSION_PREFIX = "v1="


@dataclass(frozen=True, slots=True)
class WebhookDeliveryResult:
    delivery: WebhookDelivery
    success: bool
    status_code: int | None
    error: str | None


def sign_payload(secret: str, body: bytes) -> str:
    """Return ``v1=<hmac-sha256-hex>`` over *body* with *secret*."""
    digest = hmac.new(
        secret.encode("utf-8"), body, hashlib.sha256
    ).hexdigest()
    return f"{SIGNATURE_VERSION_PREFIX}{digest}"


def verify_webhook_signature(
    secret: str,
    body: bytes,
    signature_header: str | None,
) -> bool:
    """Constant-time check that *signature_header* matches *body* under *secret*.

    Accepts ``v1=<hex>`` (preferred) and bare hex (legacy deliveries).
    """
    if not secret or not signature_header:
        return False
    header = signature_header.strip()
    if not header:
        return False
    expected_digest = hmac.new(
        secret.encode("utf-8"), body, hashlib.sha256
    ).hexdigest()
    if header.startswith(SIGNATURE_VERSION_PREFIX):
        provided = header[len(SIGNATURE_VERSION_PREFIX) :].strip()
    elif header.startswith("sha256="):
        provided = header[len("sha256=") :].strip()
    else:
        provided = header
    if len(provided) != len(expected_digest):
        return False
    try:
        return hmac.compare_digest(provided.lower(), expected_digest.lower())
    except (TypeError, ValueError):
        return False


def post_webhook(
    url: str,
    payload: dict[str, Any],
    *,
    secret: str | None = None,
    timeout_seconds: float = 10.0,
    idempotency_key: str | None = None,
) -> tuple[int, str]:
    """POST JSON payload. Returns (status_code, response_snippet_or_error).

    When *secret* is set, signs the body with :func:`sign_payload` and sets
    ``X-Job-Queue-Signature``. *idempotency_key* is sent as ``Idempotency-Key``
    and ``X-Job-Queue-Delivery-Id`` so receivers can treat retries as one event.
    """
    body = json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "job-queue-webhook/0.1",
    }
    if secret:
        headers[SIGNATURE_HEADER] = sign_payload(secret, body)
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
