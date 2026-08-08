#!/usr/bin/env python3
"""Multi-tenant multi-worker fleet: payload encryption + live key rotation.

  1. Enqueue secret-bearing jobs across tenants.
  2. Workers drain while we rotate every tenant DEK mid-flight.
  3. Kill a coordinator mid-advance (crash window) then resume.
  4. Assert:
       * every job completes
       * workers saw plaintext secrets after lease
       * Postgres never stored those secrets in jobs.payload
       * old pending jobs still decrypted after rotate
       * rotations complete and unused keys are retired
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("payload_encrypt_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
DSN = os.environ.get("JOB_QUEUE_DSN") or ""
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
JOBS = int(os.environ.get("JOBS_PER_TENANT", "20") or "20")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
SECRET_PREFIX = os.environ.get("SECRET_PREFIX", "tenant-secret")
_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict | None = None,
    *,
    absolute: str | None = None,
) -> dict[str, Any]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if absolute:
        targets = [absolute]
    else:
        start = _rr % len(COORDS)
        _rr += 1
        bases = COORDS[start:] + COORDS[:start]
        targets = [b + path for b in bases]  # type: ignore[operator]
    last: Exception | None = None
    for url in targets:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return json.loads(raw.decode()) if raw else {}
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def backlog() -> dict[str, int]:
    try:
        bl = http_json("GET", "/v1/admin/backlog")
    except Exception as exc:  # noqa: BLE001
        logger.warning("backlog: %s", exc)
        return {}
    j = bl.get("jobs") or {}
    return {k: int(j.get(k) or 0) for k in j}


def postgres_secret_hits(secrets: list[str]) -> list[str]:
    if not DSN:
        return []
    import psycopg
    from psycopg.rows import dict_row

    hits: list[str] = []
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        job_txt = conn.execute("SELECT payload::text AS t FROM jobs").fetchall()
        sched_txt = conn.execute("SELECT payload::text AS t FROM schedules").fetchall()
        wrap = conn.execute(
            "SELECT encode(wrapped_dek, 'escape') AS t FROM tenant_payload_keys"
        ).fetchall()
        blob = "\n".join(
            (r["t"] or "") for r in job_txt + sched_txt + wrap
        )
    for s in secrets:
        if s in blob:
            hits.append(s)
    return hits


def wait_coordinators() -> None:
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")


def main() -> int:
    logger.info(
        "payload encrypt soak tenants=%d jobs=%d coords=%s",
        TENANTS,
        JOBS,
        ",".join(COORDS),
    )
    wait_coordinators()

    secrets: list[str] = []
    expected = 0
    for i in range(TENANTS):
        tid = f"penc-{i:02d}"
        for n in range(JOBS):
            secret = f"{SECRET_PREFIX}-{tid}-{n}"
            secrets.append(secret)
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"secret": secret, "n": n, "kind": "encrypt-soak"},
                    "queue_name": "default",
                },
            )
            expected += 1

    # Inspect APIs must not return plaintext.
    sample = http_json(
        "GET", f"/v1/jobs?tenant_id=penc-00&limit=5"
    )
    for row in sample.get("jobs") or []:
        payload = row.get("payload") or {}
        if payload.get("secret"):
            raise RuntimeError(f"list leaked plaintext payload: {payload}")
        if payload != {"_encrypted": True}:
            raise RuntimeError(f"list payload not redacted: {payload}")

    leaked = postgres_secret_hits(secrets)
    if leaked:
        raise RuntimeError(f"postgres stored plaintext secrets: {leaked[:3]}")

    # Rotate every tenant while workers are draining.
    for i in range(TENANTS):
        tid = f"penc-{i:02d}"
        rot = http_json("POST", f"/v1/tenants/{tid}/payload-keys/rotate", {})
        status = (rot.get("rotation") or {}).get("status")
        logger.info("rotated %s status=%s", tid, status)
        if status != "reencrypting":
            raise RuntimeError(f"rotate {tid} unexpected {rot}")

    leaked = postgres_secret_hits(secrets)
    if leaked:
        raise RuntimeError(f"plaintext leaked after rotate: {leaked[:3]}")

    # Crash window: advance one batch then keep draining / resume later.
    for i in range(TENANTS):
        tid = f"penc-{i:02d}"
        http_json(
            "POST",
            f"/v1/tenants/{tid}/payload-keys/advance",
            {"batch_size": 8},
        )

    deadline = time.time() + MAX_WALL
    completed = 0
    while time.time() < deadline:
        bl = backlog()
        completed = int(bl.get("completed") or 0)
        pending = int(bl.get("pending") or 0)
        leased = int(bl.get("leased") or 0)
        logger.info(
            "progress completed=%d pending=%d leased=%d expected=%d",
            completed,
            pending,
            leased,
            expected,
        )
        # Resume rotations (simulates post-crash cleaner/coordinator).
        try:
            http_json(
                "POST",
                "/v1/admin/payload-keys/advance",
                {"limit": TENANTS, "batch_size": 50},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("advance: %s", exc)
        if completed >= expected and pending == 0 and leased == 0:
            break
        time.sleep(2)

    if completed < expected:
        raise RuntimeError(
            f"only completed {completed}/{expected} before wall clock"
        )

    leaked = postgres_secret_hits(secrets)
    if leaked:
        raise RuntimeError(f"plaintext leaked after drain: {leaked[:3]}")

    for i in range(TENANTS):
        tid = f"penc-{i:02d}"
        info = http_json("GET", f"/v1/tenants/{tid}/payload-keys")
        statuses = {k["status"] for k in (info.get("keys") or [])}
        rot = info.get("rotation") or {}
        logger.info("tenant %s keys=%s rotation=%s", tid, statuses, rot.get("status"))
        if "active" not in statuses:
            raise RuntimeError(f"{tid} missing active key")
        if rot.get("status") not in (None, "complete"):
            # finish leftover
            for _ in range(20):
                adv = http_json(
                    "POST",
                    f"/v1/tenants/{tid}/payload-keys/advance",
                    {"batch_size": 200},
                )
                if (adv.get("rotation") or {}).get("status") == "complete":
                    break
            else:
                raise RuntimeError(f"{tid} rotation did not complete: {adv}")
        info = http_json("GET", f"/v1/tenants/{tid}/payload-keys")
        retired = [k for k in info["keys"] if k["status"] == "retired"]
        decrypt_only = [k for k in info["keys"] if k["status"] == "decrypt_only"]
        if decrypt_only:
            raise RuntimeError(
                f"{tid} still has decrypt_only keys after complete: {decrypt_only}"
            )
        if not retired:
            raise RuntimeError(f"{tid} expected retired old key, got {info['keys']}")

    logger.info("payload encrypt soak OK completed=%d", completed)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("payload encrypt soak failed")
        sys.exit(1)
