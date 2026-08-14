#!/usr/bin/env python3
"""Fleet soak: drain one queue on a tenant; other queues keep running.

  * Dual coordinators, four workers, two tenants, two queues each.
  * Existing email + sms work is already pending when email is drained
    on tenant A only.
  * New A/email enqueues and A/email schedule fires are blocked.
  * A/default and all of tenant B keep taking new work and finishing.
  * A's leftover email jobs still complete. Undrain re-opens intake.
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
logger = logging.getLogger("queue_drain_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
EACH = int(os.environ.get("JOBS_EACH", "20") or "20")
EXTRA = int(os.environ.get("EXTRA_EACH", "8") or "8")
JOB_MS = int(os.environ.get("JOB_MS", "40") or "40")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
TENANTS = ("qd-a", "qd-b")
# Fleet workers lease "default". Soak leases "email" itself to prove
# a drained queue still finishes already-queued work.
QUEUES = ("default", "email")
_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict | None = None,
    *,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    start = _rr % len(COORDS)
    _rr += 1
    last: Exception | None = None
    for base in COORDS[start:] + COORDS[:start]:
        req = urllib.request.Request(
            base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                payload = json.loads(raw.decode()) if raw else {}
                return int(resp.status), payload
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode(errors="replace")}
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(f"{base}{path} -> {exc.code}: {payload}")
                continue
            return int(exc.code), payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def wait_coords() -> None:
    for _ in range(90):
        try:
            code, body = http_json("GET", "/health")
            if code == 200 and body.get("ok"):
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError("coordinators not ready")


def list_jobs(tid: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def enqueue(tid: str, queue: str, n: int, *, prefix: str) -> list[str]:
    ids: list[str] = []
    for i in range(n):
        code, resp = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "queue_name": queue,
                "payload": {
                    "kind": "work",
                    "ms": JOB_MS,
                    "i": i,
                    "p": prefix,
                    "q": queue,
                },
            },
        )
        if code != 201:
            raise RuntimeError(f"enqueue {tid}/{queue} {code} {resp}")
        ids.append(str(resp["job"]["id"]))
    return ids


def bucket(jobs: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    out = {
        q: {"pending": 0, "leased": 0, "completed": 0, "other": 0} for q in QUEUES
    }
    for j in jobs:
        q = str(j.get("queue_name") or "default")
        if q not in out:
            continue
        st = j.get("status")
        if st in out[q]:
            out[q][st] += 1
        else:
            out[q]["other"] += 1
    return out


def main() -> int:
    logger.info(
        "queue-drain soak tenants=%s queues=%s each=%d extra=%d ms=%d",
        TENANTS,
        QUEUES,
        EACH,
        EXTRA,
        JOB_MS,
    )
    wait_coords()

    for tid in TENANTS:
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")
        for q in QUEUES:
            enqueue(tid, q, EACH, prefix="seed")

    code, body = http_json(
        "POST",
        "/v1/tenants/qd-a/drain-queue",
        {"queue_name": "email"},
    )
    if code != 200 or body.get("draining_queues") != ["email"]:
        raise RuntimeError(f"drain {code} {body}")

    for tid in TENANTS:
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    # New intake: A/email blocked; A/default and B/* accepted.
    code, blocked = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": "qd-a",
            "queue_name": "email",
            "payload": {"kind": "work", "ms": JOB_MS, "p": "blocked"},
        },
    )
    if code != 409 or blocked.get("code") != "queue_draining":
        raise RuntimeError(f"expected A/email 409, got {code} {blocked}")
    enqueue("qd-a", "default", EXTRA, prefix="extra-a-default")
    enqueue("qd-b", "default", EXTRA, prefix="extra-b-default")
    enqueue("qd-b", "email", EXTRA, prefix="extra-b-email")

    # A/email schedule must not add work while draining.
    code, sched = http_json(
        "POST",
        "/v1/schedules",
        {
            "tenant_id": "qd-a",
            "queue_name": "email",
            "interval_seconds": 60,
            "payload": {"kind": "work", "ms": JOB_MS, "from": "sched"},
            "name": "qd-a-email",
        },
    )
    if code != 201:
        raise RuntimeError(f"schedule {code} {sched}")
    http_json("POST", "/v1/schedules/process-due", {"tenant_id": "qd-a", "limit": 20})

    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        snaps = {tid: bucket(list_jobs(tid)) for tid in TENANTS}
        now = time.time()
        if now - last >= 2:
            logger.info("drain %s", snaps)
            last = now
        a = snaps["qd-a"]
        b = snaps["qd-b"]
        # Finish leftover A/email (and B/email extras) via explicit leases so
        # the fleet's default-queue workers are not the only drain path.
        for tid, q in (("qd-a", "email"), ("qd-b", "email")):
            code, leased = http_json(
                "POST",
                "/v1/lease",
                {
                    "worker_id": f"soak-{tid}-{q}",
                    "tenant_id": tid,
                    "queue_name": q,
                    "limit": 4,
                },
            )
            if code == 200:
                for job in list(leased.get("jobs") or []):
                    token = job.get("lease_token")
                    if not token:
                        continue
                    http_json(
                        "POST",
                        f"/v1/jobs/{job['id']}/ack",
                        {
                            "tenant_id": tid,
                            "worker_id": f"soak-{tid}-{q}",
                            "lease_token": token,
                            "result": {"ok": True},
                        },
                    )
        if a["email"]["completed"] != EACH:
            time.sleep(0.2)
            continue
        if a["default"]["completed"] != EACH + EXTRA:
            time.sleep(0.2)
            continue
        if b["email"]["completed"] != EACH + EXTRA:
            time.sleep(0.2)
            continue
        if b["default"]["completed"] != EACH + EXTRA:
            time.sleep(0.2)
            continue
        # A/email must not have grown past the seed (no schedule / extra).
        a_email_total = sum(a["email"].values())
        if a_email_total != EACH:
            raise RuntimeError(f"A/email grew while draining: {a}")
        break
    else:
        raise RuntimeError(f"timed out waiting for isolation drain {snaps}")

    # Undrain A/email and prove intake works again.
    code, body = http_json(
        "POST",
        "/v1/tenants/qd-a/undrain-queue",
        {"queue_name": "email"},
    )
    if code != 200 or body.get("draining"):
        raise RuntimeError(f"undrain {code} {body}")
    enqueue("qd-a", "email", EXTRA, prefix="after")
    while time.time() < deadline:
        code, leased = http_json(
            "POST",
            "/v1/lease",
            {
                "worker_id": "soak-after",
                "tenant_id": "qd-a",
                "queue_name": "email",
                "limit": 4,
            },
        )
        if code == 200:
            for job in list(leased.get("jobs") or []):
                token = job.get("lease_token")
                if not token:
                    continue
                http_json(
                    "POST",
                    f"/v1/jobs/{job['id']}/ack",
                    {
                        "tenant_id": "qd-a",
                        "worker_id": "soak-after",
                        "lease_token": token,
                        "result": {"ok": True},
                    },
                )
        a = bucket(list_jobs("qd-a"))
        if a["email"]["completed"] == EACH + EXTRA:
            logger.info("queue-drain soak OK final A=%s", a)
            return 0
        time.sleep(0.2)
    raise RuntimeError("timed out after undrain")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("queue-drain soak failed")
        sys.exit(1)
