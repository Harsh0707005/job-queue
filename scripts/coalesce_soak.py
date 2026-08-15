#!/usr/bin/env python3
"""Fleet soak: live coalesce/fold under multi-tenant multi-worker load.

  * Dual coordinators, four workers, several tenants.
  * Pile the same coalesce_key as many pending jobs, fold them, and
    assert extras never run.
  * Dependents of a folded extra wait on the survivor and complete once.
  * A control tenant keeps running through the fold.
  * A live race accepts 200 (one survivor) or 409 (two already leased).
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
logger = logging.getLogger("coalesce_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
FOLD_EACH = int(os.environ.get("FOLD_EACH", "16") or "16")
KEEP_EACH = int(os.environ.get("KEEP_EACH", "8") or "8")
JOB_MS = int(os.environ.get("JOB_MS", "25") or "25")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
PAIRS = (("coal-src-a", "coal-ctl-a"), ("coal-src-b", "coal-ctl-b"))
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


def list_jobs(tid: str, **extra: str) -> list[dict[str, Any]]:
    q = f"/v1/jobs?tenant_id={tid}&limit=500"
    for key, val in extra.items():
        q += f"&{key}={val}"
    code, body = http_json("GET", q)
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def enqueue(
    tid: str,
    n: int,
    *,
    prefix: str,
    coalesce_key: str | None = None,
    depends_on: list[str] | None = None,
) -> list[str]:
    ids: list[str] = []
    for i in range(n):
        body: dict[str, Any] = {
            "tenant_id": tid,
            "queue_name": "default",
            "payload": {"kind": "render", "ms": JOB_MS, "i": i, "p": prefix},
            "labels": [prefix],
        }
        if coalesce_key is not None:
            body["coalesce_key"] = coalesce_key
        if depends_on is not None:
            body["depends_on"] = depends_on
        code, resp = http_json("POST", "/v1/jobs", body)
        if code != 201:
            raise RuntimeError(f"enqueue {tid} {code} {resp}")
        ids.append(str(resp["job"]["id"]))
    return ids


def completed(jobs: list[dict[str, Any]]) -> int:
    return sum(1 for j in jobs if j.get("status") == "completed")


def wait_completed(tids: list[str], expect: dict[str, int]) -> None:
    deadline = time.time() + MAX_WALL
    last = 0.0
    counts = {tid: 0 for tid in tids}
    while time.time() < deadline:
        now = time.time()
        counts = {tid: completed(list_jobs(tid)) for tid in tids}
        if now - last >= 2:
            logger.info("drain %s", counts)
            last = now
        if all(counts[tid] >= expect[tid] for tid in tids):
            return
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for drain: {counts}")


def main() -> int:
    logger.info(
        "coalesce soak fold=%d keep=%d ms=%d", FOLD_EACH, KEEP_EACH, JOB_MS
    )
    wait_coords()
    srcs = [s for s, _ in PAIRS]
    ctls = [c for _, c in PAIRS]
    for tid in srcs:
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")

    piled: dict[str, list[str]] = {}
    kids: dict[str, list[str]] = {}
    keep: dict[str, list[str]] = {}
    for src, ctl in PAIRS:
        piled[src] = enqueue(
            src, FOLD_EACH, prefix="inv", coalesce_key="invoice"
        )
        kids[src] = [
            enqueue(src, 1, prefix="ch", depends_on=[piled[src][0]])[0],
            enqueue(src, 1, prefix="ch", depends_on=[piled[src][1]])[0],
        ]
        keep[src] = enqueue(src, KEEP_EACH, prefix="keep")
        enqueue(ctl, KEEP_EACH, prefix="ctl")

    wait_completed(ctls, {c: KEEP_EACH for c in ctls})

    for src, _ in PAIRS:
        code, body = http_json(
            "POST",
            f"/v1/tenants/{src}/coalesce",
            {"coalesce_key": "invoice", "queue_name": "default"},
        )
        if code != 200:
            raise RuntimeError(f"coalesce {src} {code} {body}")
        if int(body.get("jobs_folded") or 0) != FOLD_EACH - 1:
            raise RuntimeError(f"fold count {src}: {body}")
        survivor = str(body.get("survivor_job_id") or "")
        if survivor != piled[src][0]:
            raise RuntimeError(f"{src} expected oldest survivor {body}")
        folded = set(body.get("folded_job_ids") or [])
        if folded != set(piled[src][1:]):
            raise RuntimeError(f"{src} unexpected folded set {body}")
        for jid in piled[src][1:]:
            gc, got = http_json("GET", f"/v1/jobs/{jid}?tenant_id={src}")
            if gc != 200 or (got.get("job") or {}).get("status") != "cancelled":
                raise RuntimeError(f"extra not folded {jid}: {gc} {got}")
        sc, sgot = http_json("GET", f"/v1/jobs/{survivor}?tenant_id={src}")
        if sc != 200 or (sgot.get("job") or {}).get("status") != "pending":
            raise RuntimeError(f"survivor {survivor}: {sc} {sgot}")
        for kid in kids[src]:
            kc, kgot = http_json("GET", f"/v1/jobs/{kid}?tenant_id={src}")
            job = (kgot.get("job") or {}) if kc == 200 else {}
            if job.get("status") != "pending":
                raise RuntimeError(f"child cancelled {kid}: {kc} {kgot}")
            if job.get("depends_on") != [survivor]:
                raise RuntimeError(f"child deps {kid}: {job.get('depends_on')}")
        listed = list_jobs(src)
        if any(
            j.get("status") == "pending"
            and j.get("id") in folded
            for j in listed
        ):
            raise RuntimeError(f"{src} folded job still pending")

    for src in srcs:
        code, body = http_json("POST", f"/v1/tenants/{src}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {src} {code} {body}")

    expect = {s: 1 + 2 + KEEP_EACH for s in srcs}
    expect.update({c: KEEP_EACH for c in ctls})
    wait_completed(srcs + ctls, expect)
    for src, _ in PAIRS:
        done = [j for j in list_jobs(src) if j.get("status") == "completed"]
        inv = [j for j in done if j.get("id") in set(piled[src])]
        if len(inv) != 1:
            raise RuntimeError(f"{src} invoice completed {len(inv)} times")
        if inv[0]["id"] != piled[src][0]:
            raise RuntimeError(f"{src} wrong survivor completed {inv}")

    # Live race: resume first, then fold. 200 or 409; never dual-complete
    # extras that were still pending.
    race_pairs = (("race-a", "race-ctl-a"), ("race-b", "race-ctl-b"))
    rsrcs = [s for s, _ in race_pairs]
    rctls = [c for _, c in race_pairs]
    for tid in rsrcs:
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")
    race_ids: dict[str, list[str]] = {}
    for src, ctl in race_pairs:
        race_ids[src] = enqueue(
            src, FOLD_EACH, prefix="race", coalesce_key="race-k"
        )
        enqueue(src, KEEP_EACH, prefix="rkeep")
        enqueue(ctl, KEEP_EACH, prefix="rctl")
    for tid in rsrcs + rctls:
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")
    time.sleep(0.2)
    for src, _ in race_pairs:
        code, body = http_json(
            "POST",
            f"/v1/tenants/{src}/coalesce",
            {"coalesce_key": "race-k", "queue_name": "default"},
        )
        if code not in (200, 409):
            raise RuntimeError(f"race coalesce {src} {code} {body}")
        logger.info("race coalesce %s code=%s folded=%s", src, code, body.get("jobs_folded"))
        if code == 200:
            for jid in body.get("folded_job_ids") or []:
                gc, got = http_json("GET", f"/v1/jobs/{jid}?tenant_id={src}")
                st = (got.get("job") or {}).get("status")
                if gc != 200 or st == "pending":
                    raise RuntimeError(f"race extra still pending {jid} {st}")
                if st == "completed":
                    raise RuntimeError(f"race extra completed after fold {jid}")

    need = {s: 1 + KEEP_EACH for s in rsrcs}
    # If fold refused, more than one race job may complete; still drain keep.
    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        counts = {tid: completed(list_jobs(tid)) for tid in rsrcs + rctls}
        now = time.time()
        if now - last >= 2:
            logger.info("race drain %s", counts)
            last = now
        if all(counts[s] >= need[s] for s in rsrcs) and all(
            counts[c] >= KEEP_EACH for c in rctls
        ):
            break
        time.sleep(0.2)
    else:
        raise RuntimeError(f"race drain timed out {counts}")
    logger.info("coalesce soak OK")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("coalesce soak failed")
        sys.exit(1)
