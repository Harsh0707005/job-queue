"""Seal / open job and schedule payloads with per-tenant DEKs."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

from job_queue.crypto import (
    ActiveDek,
    PayloadCrypto,
    PayloadCryptoError,
    decrypt_json,
    encrypt_json,
    is_envelope,
    job_aad,
    result_aad,
    schedule_aad,
    workflow_steps_aad,
)
from job_queue.models import Job, Schedule
from job_queue.queue._common import _row_to_job, _row_to_schedule


class PayloadCryptoMixin:
    """Helpers mixed into JobQueue for envelope encrypt/decrypt."""

    _crypto: PayloadCrypto

    def _require_crypto(self) -> PayloadCrypto:
        crypto = getattr(self, "_crypto", None)
        if crypto is None:
            raise PayloadCryptoError("payload crypto is not configured")
        return crypto

    def _ensure_active_key(self, conn: Any, tenant_id: str) -> ActiveDek:
        crypto = self._require_crypto()
        row = conn.execute(
            """
            SELECT key_id, wrapped_dek, wrap_nonce, kek_id, status
            FROM tenant_payload_keys
            WHERE tenant_id = %s AND status = 'active'
            FOR UPDATE
            """,
            (tenant_id,),
        ).fetchone()
        if row is not None:
            dek = crypto.unwrap_stored_dek(
                tenant_id=tenant_id,
                key_id=row["key_id"],
                wrapped=bytes(row["wrapped_dek"]),
                nonce=bytes(row["wrap_nonce"]),
                kek_id=str(row["kek_id"]),
            )
            return ActiveDek(
                tenant_id=tenant_id,
                key_id=row["key_id"],
                dek=dek,
                kek_id=str(row["kek_id"]),
                status=str(row["status"]),
            )

        key_id = uuid4()
        dek, wrapped, nonce = crypto.wrap_new_dek(tenant_id=tenant_id, key_id=key_id)
        conn.execute(
            """
            INSERT INTO tenant_payload_keys (
                tenant_id, key_id, wrapped_dek, wrap_nonce, kek_id,
                status, created_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                'active', NOW()
            )
            """,
            (tenant_id, key_id, wrapped, nonce, crypto.active_kek_id),
        )
        return ActiveDek(
            tenant_id=tenant_id,
            key_id=key_id,
            dek=dek,
            kek_id=crypto.active_kek_id,
            status="active",
        )

    def _load_dek(self, conn: Any | None, tenant_id: str, key_id: UUID) -> bytes:
        crypto = self._require_crypto()
        cached = crypto.cached_dek(tenant_id, key_id)
        if cached is not None:
            return cached

        def _fetch(c: Any) -> bytes:
            row = c.execute(
                """
                SELECT wrapped_dek, wrap_nonce, kek_id, status
                FROM tenant_payload_keys
                WHERE tenant_id = %s AND key_id = %s
                """,
                (tenant_id, key_id),
            ).fetchone()
            if row is None:
                raise PayloadCryptoError(
                    f"unknown payload key {key_id} for tenant {tenant_id!r}"
                )
            if str(row["status"]) == "retired":
                raise PayloadCryptoError(
                    f"payload key {key_id} for tenant {tenant_id!r} is retired"
                )
            wrapped = bytes(row["wrapped_dek"] or b"")
            nonce = bytes(row["wrap_nonce"] or b"")
            if not wrapped or not nonce:
                raise PayloadCryptoError(
                    f"payload key {key_id} for tenant {tenant_id!r} has no material"
                )
            return crypto.unwrap_stored_dek(
                tenant_id=tenant_id,
                key_id=key_id,
                wrapped=wrapped,
                nonce=nonce,
                kek_id=str(row["kek_id"]),
            )

        if conn is not None:
            return _fetch(conn)
        with self._pool.connection() as c:  # type: ignore[attr-defined]
            return _fetch(c)

    def _seal_dict(
        self,
        conn: Any,
        *,
        tenant_id: str,
        plaintext: dict[str, Any],
        aad: bytes,
        job_id: UUID | None = None,
        blob_kind: str | None = None,
        force_new: bool = False,
    ) -> tuple[dict[str, Any], UUID]:
        if not isinstance(plaintext, dict):
            raise ValueError("plaintext must be a dict")
        if is_envelope(plaintext) and not force_new:
            return plaintext, UUID(str(plaintext["kid"]))
        rec = self._ensure_active_key(conn, tenant_id)
        env = encrypt_json(
            rec.dek,
            key_id=rec.key_id,
            plaintext=plaintext,
            aad=aad,
        )
        if hasattr(self, "_should_spill_envelope") and self._should_spill_envelope(
            env
        ):
            kind = blob_kind or "other"
            env = self._spill_envelope(
                conn,
                env,
                tenant_id=tenant_id,
                job_id=job_id,
                kind=kind,
            )
        return env, rec.key_id

    def _open_dict(
        self,
        payload: Any,
        *,
        tenant_id: str,
        aad: bytes,
        conn: Any | None = None,
    ) -> dict[str, Any]:
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return {}
        if not isinstance(payload, dict):
            return {}
        if not is_envelope(payload):
            return payload
        env = payload
        if hasattr(self, "_hydrate_envelope"):
            env = self._hydrate_envelope(env, conn=conn)
        dek = self._load_dek(conn, tenant_id, UUID(str(env["kid"])))
        return decrypt_json(dek, env, aad=aad)

    def _seal_job_payload(
        self,
        conn: Any,
        *,
        tenant_id: str,
        job_id: UUID,
        payload: dict[str, Any],
        force_new: bool = False,
    ) -> tuple[dict[str, Any], UUID]:
        return self._seal_dict(
            conn,
            tenant_id=tenant_id,
            plaintext=payload,
            aad=job_aad(job_id),
            job_id=job_id,
            blob_kind="payload",
            force_new=force_new,
        )

    def _seal_schedule_payload(
        self,
        conn: Any,
        *,
        tenant_id: str,
        schedule_id: UUID,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any], UUID]:
        return self._seal_dict(
            conn,
            tenant_id=tenant_id,
            plaintext=payload,
            aad=schedule_aad(schedule_id),
        )

    def _open_job_payload(
        self,
        payload: Any,
        *,
        tenant_id: str,
        job_id: UUID,
        conn: Any | None = None,
    ) -> dict[str, Any]:
        return self._open_dict(
            payload, tenant_id=tenant_id, aad=job_aad(job_id), conn=conn
        )

    def _open_schedule_payload(
        self,
        payload: Any,
        *,
        tenant_id: str,
        schedule_id: UUID,
        conn: Any | None = None,
    ) -> dict[str, Any]:
        return self._open_dict(
            payload,
            tenant_id=tenant_id,
            aad=schedule_aad(schedule_id),
            conn=conn,
        )

    def _seal_json_value(
        self,
        conn: Any,
        *,
        tenant_id: str,
        value: Any,
        aad: bytes,
        job_id: UUID | None = None,
        blob_kind: str = "result",
        force_new: bool = False,
    ) -> tuple[dict[str, Any], UUID]:
        return self._seal_dict(
            conn,
            tenant_id=tenant_id,
            plaintext={"_jq_val": value},
            aad=aad,
            job_id=job_id,
            blob_kind=blob_kind,
            force_new=force_new,
        )

    def _open_json_value(
        self,
        payload: Any,
        *,
        tenant_id: str,
        aad: bytes,
        conn: Any | None = None,
    ) -> Any:
        if payload is None:
            return None
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return payload
        if not is_envelope(payload):
            return payload
        opened = self._open_dict(
            payload, tenant_id=tenant_id, aad=aad, conn=conn
        )
        if isinstance(opened, dict) and "_jq_val" in opened and set(opened) == {"_jq_val"}:
            return opened["_jq_val"]
        return opened

    def _parse_steps_def(
        self, run_row: dict[str, Any], conn: Any | None = None
    ) -> list[dict[str, Any]]:
        raw = run_row.get("steps_def")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return []
        if isinstance(raw, list):
            return [dict(x) for x in raw if isinstance(x, dict)]
        if not isinstance(raw, dict):
            return []
        opened = self._open_dict(
            raw,
            tenant_id=str(run_row["tenant_id"]),
            aad=workflow_steps_aad(run_row["id"]),
            conn=conn,
        )
        steps = opened.get("steps") if isinstance(opened, dict) else None
        if isinstance(steps, list):
            return [dict(x) for x in steps if isinstance(x, dict)]
        return []

    def _job(
        self,
        row: dict[str, Any],
        *,
        decrypt: bool = True,
        conn: Any | None = None,
    ) -> Job:
        data = dict(row)
        payload = data.get("payload")
        tenant_id = str(data["tenant_id"])
        job_id = data["id"]
        if decrypt and hasattr(self, "_overlay_archive_row"):
            data = self._overlay_archive_row(data, conn=conn)
            payload = data.get("payload")
        if decrypt:
            data["payload"] = self._open_job_payload(
                payload,
                tenant_id=tenant_id,
                job_id=job_id,
                conn=conn,
            )
            if data.get("result") is not None:
                data["result"] = self._open_json_value(
                    data.get("result"),
                    tenant_id=tenant_id,
                    aad=result_aad(job_id),
                    conn=conn,
                )
        else:
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = {}
            data["payload"] = payload if isinstance(payload, dict) else {}
        return _row_to_job(data)

    def _job_readable(
        self,
        row: dict[str, Any],
        *,
        conn: Any | None = None,
    ) -> Job:
        """Like ``_job`` but a missing/corrupt blob does not raise."""
        from job_queue.blob_store import BlobStoreError

        try:
            return self._job(row, conn=conn)
        except BlobStoreError:
            return self._job(row, decrypt=False, conn=conn)

    def _schedule(
        self,
        row: dict[str, Any],
        *,
        decrypt: bool = True,
        conn: Any | None = None,
    ) -> Schedule:
        data = dict(row)
        payload = data.get("payload")
        if decrypt:
            data["payload"] = self._open_schedule_payload(
                payload,
                tenant_id=str(data["tenant_id"]),
                schedule_id=data["id"],
                conn=conn,
            )
        else:
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = {}
            data["payload"] = payload if isinstance(payload, dict) else {}
        return _row_to_schedule(data)

    def _row_plaintext_payload(
        self,
        row: dict[str, Any],
        *,
        conn: Any | None = None,
    ) -> dict[str, Any]:
        return self._open_job_payload(
            row.get("payload"),
            tenant_id=str(row["tenant_id"]),
            job_id=row["id"],
            conn=conn,
        )
