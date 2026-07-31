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
import ipaddress
import json
import logging
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

from job_queue.models import WebhookDelivery
from job_queue.queue import JobQueue, compute_retry_backoff_seconds

logger = logging.getLogger(__name__)

SIGNATURE_HEADER = "X-Job-Queue-Signature"
SIGNATURE_VERSION_PREFIX = "v1="

# Env escape hatch for local soak tests that POST to 127.0.0.1 sinks.
_ALLOW_PRIVATE_WEBHOOKS_ENV = "JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS"


def _private_webhooks_allowed() -> bool:
    return os.environ.get(_ALLOW_PRIVATE_WEBHOOKS_ENV, "").lower() in (
        "1",
        "true",
        "yes",
    )


def validate_webhook_url(url: str, *, resolve_dns: bool = True) -> str:
    """Validate *url* for outbound webhook delivery (SSRF hardening).

    Requires ``http``/``https``, rejects credentials in the URL, and blocks
    loopback / link-local / private / reserved destinations unless
    ``JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS=1``.

    When *resolve_dns* is True (default), also resolves the hostname and
    rejects private A/AAAA records (DNS rebinding to internal IPs).

    Returns the stripped URL on success; raises ``ValueError`` otherwise.
    """
    if not url or not isinstance(url, str):
        raise ValueError("url must be a non-empty string")
    cleaned = url.strip()
    parsed = urlparse(cleaned)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("url must be an http(s) URL")
    if not parsed.hostname:
        raise ValueError("url must include a hostname")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("url must not contain credentials")
    if _private_webhooks_allowed():
        return cleaned

    host = parsed.hostname.lower().rstrip(".")
    blocked_hosts = {
        "localhost",
        "localhost.localdomain",
        "metadata.google.internal",
        "metadata",
        "kubernetes.default",
        "kubernetes.default.svc",
    }
    if host in blocked_hosts:
        raise ValueError(f"webhook URL host {host!r} is not allowed")

    # Literal IP in the URL.
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None:
        if not ip.is_global:
            raise ValueError(
                f"webhook URL host resolves to non-public address {ip}"
            )
        return cleaned

    # Hostname path: optional DNS check against private A/AAAA records.
    if resolve_dns:
        try:
            infos = socket.getaddrinfo(
                host,
                parsed.port or (443 if parsed.scheme == "https" else 80),
                type=socket.SOCK_STREAM,
            )
        except socket.gaierror as dns_exc:
            raise ValueError(
                f"webhook URL host could not be resolved: {host}"
            ) from dns_exc
        for info in infos:
            addr = info[4][0]
            try:
                resolved = ipaddress.ip_address(addr)
            except ValueError:
                continue
            if not resolved.is_global:
                raise ValueError(
                    f"webhook URL host resolves to non-public address {resolved}"
                )
    return cleaned


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

    # Defense in depth: refuse private destinations at POST time too
    # (registration should already have checked; DNS may have changed).
    try:
        validate_webhook_url(url, resolve_dns=True)
    except ValueError as exc:
        return 0, f"ssrf_blocked: {exc}"

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
