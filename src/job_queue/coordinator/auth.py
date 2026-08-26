"""Coordinator auth, limits, and path helpers."""

from __future__ import annotations

import os
import re
from typing import Any

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# Cap request bodies so a forged Content-Length cannot pin a worker thread
# reading multi-GB payloads. Enqueue payloads are JSON; 2 MiB is generous.
_MAX_REQUEST_BODY_BYTES = 2 * 1024 * 1024

# Optional shared secret. When set (env ``JOB_QUEUE_API_TOKEN`` or
# ``COORDINATOR_TOKEN``), all routes except ``GET /health`` require
# ``Authorization: Bearer <token>`` or ``X-API-Key: <token>``.
_API_TOKEN_ENV_KEYS = ("JOB_QUEUE_API_TOKEN", "COORDINATOR_TOKEN")


class _BodyTooLarge(ValueError):
    """Request body exceeds :data:`_MAX_REQUEST_BODY_BYTES`."""


def _configured_api_token() -> str | None:
    for key in _API_TOKEN_ENV_KEYS:
        val = os.environ.get(key)
        if val:
            return val
    return None


def _is_public_inbound_path(path: str) -> bool:
    """POST /v1/inbound/{uuid} is signed per-trigger; no API token."""
    parts = (path or "").strip("/").split("/")
    return (
        len(parts) == 3
        and parts[0] == "v1"
        and parts[1] == "inbound"
        and bool(_UUID_RE.match(parts[2]))
    )


def _extract_request_token(headers: Any) -> str | None:
    """Pull bearer / API key from request headers (case-insensitive mapping)."""
    if headers is None:
        return None
    # BaseHTTPRequestHandler headers are case-insensitive.
    auth = headers.get("Authorization") or headers.get("authorization") or ""
    if isinstance(auth, str) and auth.lower().startswith("bearer "):
        return auth[7:].strip() or None
    key = headers.get("X-API-Key") or headers.get("x-api-key")
    if key:
        return str(key).strip() or None
    return None
