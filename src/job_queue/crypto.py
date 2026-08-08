"""Envelope encryption for per-tenant job payloads.

DEKs live wrapped (AES-256-GCM) in Postgres. The wrapping KEK is supplied
via environment and never persisted next to ciphertext. Crash-safe rotation
keeps old wrapped DEKs until every remaining ciphertext has been re-sealed;
key material is zeroized only after that point.
"""

from __future__ import annotations

import base64
import json
import os
import threading
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

ENVELOPE_VERSION = 1
ENVELOPE_MARKER = "_jq_enc"
REDACTED_PAYLOAD: dict[str, Any] = {"_encrypted": True}

_NONCE_LEN = 12
_DEK_LEN = 32
_KEK_LEN = 32

_ENV_KEK = "JOB_QUEUE_PAYLOAD_KEK"
_ENV_KEK_ID = "JOB_QUEUE_PAYLOAD_KEK_ID"
_ENV_KEKS = "JOB_QUEUE_PAYLOAD_KEKS"


class PayloadCryptoError(Exception):
    """Raised when a payload cannot be sealed or opened."""


def decode_key_bytes(raw: str, *, label: str = "key") -> bytes:
    """Decode a 32-byte key from hex or (urlsafe) base64."""
    text = (raw or "").strip()
    if not text:
        raise ValueError(f"{label} is empty")
    key: bytes | None = None
    if len(text) == 64 and all(c in "0123456789abcdefABCDEF" for c in text):
        key = bytes.fromhex(text)
    else:
        pad = "=" * (-len(text) % 4)
        for decoder in (base64.urlsafe_b64decode, base64.b64decode):
            try:
                key = decoder(text + pad)
                break
            except Exception:  # noqa: BLE001
                continue
    if key is None or len(key) != _KEK_LEN:
        got = 0 if key is None else len(key)
        raise ValueError(f"{label} must decode to {_KEK_LEN} bytes, got {got}")
    return key


def load_keks_from_env(
    env: dict[str, str] | None = None,
) -> tuple[dict[str, bytes], str]:
    """Return ``({kek_id: bytes}, active_kek_id)`` from process env."""
    src = env if env is not None else os.environ
    keks: dict[str, bytes] = {}
    extra = src.get(_ENV_KEKS)
    if extra:
        parsed = json.loads(extra)
        if not isinstance(parsed, dict) or not parsed:
            raise ValueError(f"{_ENV_KEKS} must be a JSON object of id → key")
        for kid, raw in parsed.items():
            keks[str(kid)] = decode_key_bytes(str(raw), label=f"KEK {kid}")
    active_id = (src.get(_ENV_KEK_ID) or "").strip() or "default"
    primary = src.get(_ENV_KEK)
    if primary:
        keks[active_id] = decode_key_bytes(primary, label=_ENV_KEK)
    elif active_id not in keks and keks:
        # Extra map only: first key is active unless JOB_QUEUE_PAYLOAD_KEK_ID set.
        if src.get(_ENV_KEK_ID):
            if active_id not in keks:
                raise ValueError(
                    f"{_ENV_KEK_ID}={active_id!r} is not present in {_ENV_KEKS}"
                )
        else:
            active_id = next(iter(keks))
    if not keks:
        raise PayloadCryptoError(
            f"{_ENV_KEK} (32-byte hex or base64) is required for payload encryption"
        )
    if active_id not in keks:
        raise ValueError(f"active KEK id {active_id!r} is not loaded")
    return keks, active_id


def is_envelope(payload: Any) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get(ENVELOPE_MARKER) == ENVELOPE_VERSION
        and isinstance(payload.get("kid"), str)
        and isinstance(payload.get("n"), str)
        and isinstance(payload.get("c"), str)
    )


def job_aad(job_id: UUID | str) -> bytes:
    return f"job-queue:v1:job:{job_id}".encode()


def schedule_aad(schedule_id: UUID | str) -> bytes:
    return f"job-queue:v1:schedule:{schedule_id}".encode()


def delivery_aad(delivery_id: UUID | str) -> bytes:
    return f"job-queue:v1:webhook-delivery:{delivery_id}".encode()


def workflow_steps_aad(run_id: UUID | str) -> bytes:
    return f"job-queue:v1:workflow-steps:{run_id}".encode()


def result_aad(job_id: UUID | str) -> bytes:
    return f"job-queue:v1:job-result:{job_id}".encode()


def dek_aad(*, tenant_id: str, key_id: UUID | str) -> bytes:
    # Bind wrap to key_id only. tenant_id is intentionally omitted so
    # merge_tenant can move key rows without re-wrapping (live, no stop).
    # Tenant isolation is the table PK + who may load the row.
    _ = tenant_id
    return f"job-queue:v1:dek:{key_id}".encode()


def redact_payload(_payload: Any) -> dict[str, Any]:
    return dict(REDACTED_PAYLOAD)


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64d(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


def encrypt_json(
    dek: bytes,
    *,
    key_id: UUID | str,
    plaintext: dict[str, Any],
    aad: bytes,
) -> dict[str, Any]:
    if not isinstance(plaintext, dict):
        raise ValueError("payload must be a dict")
    if is_envelope(plaintext):
        raise PayloadCryptoError("refusing to double-encrypt an envelope")
    raw = json.dumps(plaintext, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    nonce = os.urandom(_NONCE_LEN)
    ct = AESGCM(dek).encrypt(nonce, raw, aad)
    return {
        ENVELOPE_MARKER: ENVELOPE_VERSION,
        "kid": str(key_id),
        "n": _b64e(nonce),
        "c": _b64e(ct),
    }


def decrypt_json(
    dek: bytes,
    envelope: dict[str, Any],
    *,
    aad: bytes,
) -> dict[str, Any]:
    if not is_envelope(envelope):
        raise PayloadCryptoError("not a payload envelope")
    try:
        nonce = _b64d(str(envelope["n"]))
        ct = _b64d(str(envelope["c"]))
        raw = AESGCM(dek).decrypt(nonce, ct, aad)
        out = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise PayloadCryptoError("payload decrypt failed") from exc
    if not isinstance(out, dict):
        raise PayloadCryptoError("decrypted payload is not an object")
    return out


def wrap_dek(
    kek: bytes,
    dek: bytes,
    *,
    tenant_id: str,
    key_id: UUID | str,
) -> tuple[bytes, bytes]:
    nonce = os.urandom(_NONCE_LEN)
    wrapped = AESGCM(kek).encrypt(nonce, dek, dek_aad(tenant_id=tenant_id, key_id=key_id))
    return wrapped, nonce


def unwrap_dek(
    kek: bytes,
    wrapped: bytes,
    nonce: bytes,
    *,
    tenant_id: str,
    key_id: UUID | str,
) -> bytes:
    try:
        dek = AESGCM(kek).decrypt(
            nonce, wrapped, dek_aad(tenant_id=tenant_id, key_id=key_id)
        )
    except Exception as exc:  # noqa: BLE001
        raise PayloadCryptoError("DEK unwrap failed") from exc
    if len(dek) != _DEK_LEN:
        raise PayloadCryptoError("unwrapped DEK has unexpected length")
    return dek


def new_dek() -> bytes:
    return os.urandom(_DEK_LEN)


@dataclass(frozen=True, slots=True)
class ActiveDek:
    tenant_id: str
    key_id: UUID
    dek: bytes
    kek_id: str
    status: str


class PayloadCrypto:
    """In-process KEK set + DEK unwrap cache."""

    def __init__(self, keks: dict[str, bytes], active_kek_id: str) -> None:
        if not keks:
            raise PayloadCryptoError("at least one KEK is required")
        if active_kek_id not in keks:
            raise PayloadCryptoError(f"unknown active KEK id {active_kek_id!r}")
        self._keks = dict(keks)
        self.active_kek_id = active_kek_id
        self._dek_cache: dict[tuple[str, UUID], bytes] = {}
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> PayloadCrypto:
        keks, active = load_keks_from_env(env)
        return cls(keks, active)

    @property
    def active_kek(self) -> bytes:
        return self._keks[self.active_kek_id]

    def kek(self, kek_id: str) -> bytes:
        try:
            return self._keks[kek_id]
        except KeyError as exc:
            raise PayloadCryptoError(f"unknown KEK id {kek_id!r}") from exc

    def cache_dek(self, tenant_id: str, key_id: UUID, dek: bytes) -> None:
        with self._lock:
            self._dek_cache[(tenant_id, key_id)] = dek

    def cached_dek(self, tenant_id: str, key_id: UUID) -> bytes | None:
        with self._lock:
            return self._dek_cache.get((tenant_id, key_id))

    def drop_dek(self, tenant_id: str, key_id: UUID) -> None:
        with self._lock:
            self._dek_cache.pop((tenant_id, key_id), None)

    def drop_tenant(self, tenant_id: str) -> None:
        with self._lock:
            for key in [k for k in self._dek_cache if k[0] == tenant_id]:
                self._dek_cache.pop(key, None)

    def wrap_new_dek(self, *, tenant_id: str, key_id: UUID) -> tuple[bytes, bytes, bytes]:
        dek = new_dek()
        wrapped, nonce = wrap_dek(
            self.active_kek, dek, tenant_id=tenant_id, key_id=key_id
        )
        self.cache_dek(tenant_id, key_id, dek)
        return dek, wrapped, nonce

    def unwrap_stored_dek(
        self,
        *,
        tenant_id: str,
        key_id: UUID,
        wrapped: bytes,
        nonce: bytes,
        kek_id: str,
    ) -> bytes:
        cached = self.cached_dek(tenant_id, key_id)
        if cached is not None:
            return cached
        dek = unwrap_dek(
            self.kek(kek_id),
            wrapped,
            nonce,
            tenant_id=tenant_id,
            key_id=key_id,
        )
        self.cache_dek(tenant_id, key_id, dek)
        return dek
