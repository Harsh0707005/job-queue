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
import os
import socket
import threading
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from job_queue.models import WebhookDelivery
from job_queue.queue import JobQueue

SIGNATURE_HEADER = "X-Job-Queue-Signature"
SIGNATURE_VERSION_PREFIX = "v1="
TIMESTAMP_HEADER = "X-Job-Queue-Timestamp"
NONCE_HEADER = "X-Job-Queue-Nonce"
INBOUND_MAX_SKEW_SECONDS = 300

# Env escape hatch for local soak tests that POST to 127.0.0.1 sinks.
_ALLOW_PRIVATE_WEBHOOKS_ENV = "JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS"


def _private_webhooks_allowed() -> bool:
    return os.environ.get(_ALLOW_PRIVATE_WEBHOOKS_ENV, "").lower() in (
        "1",
        "true",
        "yes",
    )


def _ip_forbidden_for_webhook(
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
) -> str | None:
    """Return a rejection reason if *ip* must never be a webhook target.

    ``JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS=1`` permits loopback and RFC1918-style
    private addresses (docker / local sinks) but **never** link-local or
    cloud-metadata ranges (e.g. ``169.254.169.254``).
    """
    # Always blocked — even when private webhooks are allowed.
    if ip.is_unspecified:
        return f"unspecified address {ip}"
    if ip.is_multicast:
        return f"multicast address {ip}"
    if ip.is_link_local:
        return f"link-local address {ip}"
    # IPv4 link-local / APIPA is is_link_local; also catch documented metadata.
    if isinstance(ip, ipaddress.IPv4Address) and ip in ipaddress.ip_network(
        "169.254.0.0/16"
    ):
        return f"link-local address {ip}"

    if _private_webhooks_allowed():
        # Loopback + private OK for local sinks / docker networks.
        return None

    if not ip.is_global:
        return f"non-public address {ip}"
    return None


def validate_webhook_url(url: str, *, resolve_dns: bool = True) -> str:
    """Validate *url* for outbound webhook delivery (SSRF hardening).

    Requires ``http``/``https``, rejects credentials in the URL, and blocks
    non-public destinations. With ``JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS=1``,
    loopback and private (RFC1918 / ULA) hosts are allowed for local sinks,
    but **link-local / metadata** ranges stay blocked.

    When *resolve_dns* is True (default), also resolves the hostname and
    rejects forbidden A/AAAA records (DNS rebinding). Callers that follow
    HTTP redirects must re-invoke this for every hop.

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

    host = parsed.hostname.lower().rstrip(".")
    blocked_hosts = {
        "localhost",
        "localhost.localdomain",
        "metadata.google.internal",
        "metadata",
        "kubernetes.default",
        "kubernetes.default.svc",
    }
    # localhost hostnames: only when private webhooks are not allowed.
    if host in blocked_hosts:
        if host.startswith("metadata") or host.startswith("kubernetes"):
            raise ValueError(f"webhook URL host {host!r} is not allowed")
        if not _private_webhooks_allowed():
            raise ValueError(f"webhook URL host {host!r} is not allowed")

    # Literal IP in the URL.
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None:
        reason = _ip_forbidden_for_webhook(ip)
        if reason is not None:
            raise ValueError(f"webhook URL host resolves to {reason}")
        return cleaned

    # Hostname path: optional DNS check against forbidden A/AAAA records.
    if resolve_dns:
        try:
            infos = socket.getaddrinfo(
                host,
                parsed.port or (443 if parsed.scheme == "https" else 80),
                type=socket.SOCK_STREAM,
            )
        except socket.gaierror as dns_exc:
            # Unresolvable hosts cannot be proven public. In strict mode reject;
            # with private webhooks allowed (tests / docker) permit registration
            # of placeholder sinks like ``http://example.test/hook``.
            if _private_webhooks_allowed():
                return cleaned
            raise ValueError(
                f"webhook URL host could not be resolved: {host}"
            ) from dns_exc
        for info in infos:
            addr = info[4][0]
            try:
                resolved = ipaddress.ip_address(addr)
            except ValueError:
                continue
            reason = _ip_forbidden_for_webhook(resolved)
            if reason is not None:
                raise ValueError(f"webhook URL host resolves to {reason}")
    return cleaned


@dataclass(frozen=True, slots=True)
class WebhookDeliveryResult:
    delivery: WebhookDelivery
    success: bool
    status_code: int | None
    error: str | None


def sign_payload(secret: str, body: bytes) -> str:
    """Return ``v1=<hmac-sha256-hex>`` over *body* with *secret*."""
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
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
    expected_digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
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


def inbound_signing_payload(timestamp: str, nonce: str, body: bytes) -> bytes:
    """Canonical bytes signed for inbound trigger POSTs."""
    ts = str(timestamp or "").strip()
    n = str(nonce or "").strip()
    return f"{ts}.{n}.".encode("ascii") + bytes(body or b"")


def sign_inbound(
    secret: str,
    body: bytes,
    *,
    timestamp: str | int,
    nonce: str,
) -> str:
    """HMAC-SHA256 over ``timestamp.nonce.body`` for inbound triggers."""
    return sign_payload(secret, inbound_signing_payload(str(timestamp), nonce, body))


def verify_inbound_signature(
    secret: str,
    body: bytes,
    signature_header: str | None,
    *,
    timestamp: str | None,
    nonce: str | None,
    now: float | None = None,
    max_skew_seconds: int = INBOUND_MAX_SKEW_SECONDS,
) -> str | None:
    """Return None if the inbound request is authentic, else an error code."""
    import time as _time

    if not secret:
        return "missing_secret"
    if not timestamp or not str(timestamp).strip():
        return "missing_timestamp"
    if not nonce or not str(nonce).strip():
        return "missing_nonce"
    try:
        ts = int(str(timestamp).strip())
    except (TypeError, ValueError):
        return "bad_timestamp"
    now_ts = float(now if now is not None else _time.time())
    if abs(now_ts - ts) > int(max_skew_seconds):
        return "stale_timestamp"
    if not verify_webhook_signature(
        secret,
        inbound_signing_payload(str(ts), str(nonce).strip(), body),
        signature_header,
    ):
        return "bad_signature"
    return None


class _SsrfSafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Follow redirects only when each hop passes :func:`validate_webhook_url`.

    Without this, a public host can 302 to ``http://169.254.169.254/`` (or any
    private IP) and urllib's default redirect following would complete the
    SSRF. Validation runs on every ``Location`` before the hop is taken.
    """

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        try:
            validate_webhook_url(newurl, resolve_dns=True)
        except ValueError as exc:
            # Raise URLError so post_webhook surfaces a clear ssrf_blocked
            # error instead of silently following into the private network.
            raise urllib.error.URLError(f"ssrf_blocked_redirect: {exc}") from exc
        return super().redirect_request(req, fp, code, msg, headers, newurl)


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

    SSRF: the initial URL and **every redirect hop** are checked with
    :func:`validate_webhook_url` (private / link-local / metadata blocked
    unless ``JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS=1``).
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
    # Custom opener: default urlopen follows redirects without re-validating.
    opener = urllib.request.build_opener(_SsrfSafeRedirectHandler)
    try:
        with opener.open(req, timeout=timeout_seconds) as resp:
            code = int(resp.getcode())
            raw = resp.read(512)
            text = raw.decode("utf-8", errors="replace")
            return code, text
    except urllib.error.HTTPError as exc:
        raw = exc.read(512) if exc.fp else b""
        text = raw.decode("utf-8", errors="replace")
        return int(exc.code), text or str(exc)
    except urllib.error.URLError as exc:
        reason = str(getattr(exc, "reason", exc) or exc)
        if "ssrf_blocked" in reason:
            return 0, reason
        return 0, reason
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
