#!/usr/bin/env python3
"""Fleet soak: tenant split stages (plan, then execute).

SPLIT_STAGE=1  dry-run only: plan while other tenants keep completing.
SPLIT_STAGE=2  execute a graph-safe queue split; dest finishes moved jobs.
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
logger = logging.getLogger("tenant_split_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
STAGE = int(os.environ.get("SPLIT_STAGE", "1") or "1")
EMAIL_EACH = int(os.environ.get("EMAIL_EACH", "16") or "16")
KEEP_EACH = int(os.environ.get("KEEP_EACH", "8") or "8")
JOB_MS = int(os.environ.get("JOB_MS", "25") or "25")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
PAIRS = (("spl-src-a", "spl-dst-a"), ("spl-src-b", "spl-dst-b"))
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


def enqueue(tid: str, n: int, *, queue_name: str, prefix: str) -> list[str]:
    ids: list[str] = []
    for i in range(n):
        code, resp = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "queue_name": queue_name,
                "payload": {
                    "kind": "render",
                    "ms": JOB_MS,
                    "i": i,
                    "p": prefix,
                },
                "labels": [prefix, queue_name],
            },
        )
        if code != 201:
            raise RuntimeError(f"enqueue {tid}/{queue_name} {code} {resp}")
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


def plan_split(src: str, dest: str, queue_name: str) -> dict[str, Any]:
    code, body = http_json(
        "POST",
        f"/v1/tenants/{src}/split/plan",
        {"target_tenant_id": dest, "queue_name": queue_name},
    )
    if code != 200:
        raise RuntimeError(f"plan {src}->{dest} {code} {body}")
    return body


def main() -> int:
    logger.info(
        "tenant-split soak stage=%d email=%d keep=%d ms=%d",
        STAGE,
        EMAIL_EACH,
        KEEP_EACH,
        JOB_MS,
    )
    wait_coords()
    srcs = [s for s, _ in PAIRS]
    dests = [d for _, d in PAIRS]
    for tid in srcs + dests:
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")

    email: dict[str, list[str]] = {}
    keep: dict[str, list[str]] = {}
    dest_keep: dict[str, list[str]] = {}
    broken: dict[str, tuple[str, str]] = {}
    for src, dest in PAIRS:
        email[src] = enqueue(src, EMAIL_EACH, queue_name="email", prefix="mail")
        keep[src] = enqueue(src, KEEP_EACH, queue_name="default", prefix="keep")
        dest_keep[dest] = enqueue(
            dest, KEEP_EACH, queue_name="default", prefix="dst"
        )
        code, resp = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": src,
                "queue_name": "default",
                "payload": {"kind": "render", "ms": JOB_MS, "p": "par"},
            },
        )
        if code != 201:
            raise RuntimeError(f"broken parent {src} {code} {resp}")
        parent_id = str(resp["job"]["id"])
        code, resp = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": src,
                "queue_name": "email",
                "payload": {"kind": "render", "ms": JOB_MS, "p": "ch"},
                "depends_on": [parent_id],
            },
        )
        if code != 201:
            raise RuntimeError(f"broken child {src} {code} {resp}")
        broken[src] = (parent_id, str(resp["job"]["id"]))

    # Dest default work runs while we plan (and later split) the source.
    for dest in dests:
        code, body = http_json("POST", f"/v1/tenants/{dest}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume dest {dest} {code} {body}")

    for src, dest in PAIRS:
        good = plan_split(src, dest, "email")
        # Isolated email jobs should be listed; the child that depends on
        # a default-queue parent must fail the whole email plan closed.
        if good.get("ok") is not False:
            raise RuntimeError(f"{src} expected fail-closed plan: {good}")
        reasons = {b.get("reason") for b in (good.get("blockers") or [])}
        if "graph_outside_filter" not in reasons:
            raise RuntimeError(f"{src} missing graph blocker: {good}")
        parent_id, child_id = broken[src]
        if child_id in (good.get("move_job_ids") or []):
            raise RuntimeError(f"{src} would half-move child {child_id}")
        # Isolated email seeds should still be in the move list (or not, if
        # ok is false we still report them). Confirm they are listed.
        moved = set(good.get("move_job_ids") or [])
        if not set(email[src]).issubset(moved):
            raise RuntimeError(f"{src} plan missed isolated email jobs {moved}")
        # Plan must not mutate ownership.
        for sid in email[src] + keep[src] + [parent_id, child_id]:
            code, body = http_json("GET", f"/v1/jobs/{sid}?tenant_id={src}")
            if code != 200:
                raise RuntimeError(f"plan mutated {src}/{sid}: {code} {body}")

    wait_completed(dests, {d: KEEP_EACH for d in dests})
    if STAGE < 2:
        logger.info("tenant-split stage 1 OK (plan only)")
        return 0

    # Fail-closed execute while the broken DAG is still present.
    for src, dest in PAIRS:
        code, body = http_json(
            "POST",
            f"/v1/tenants/{src}/split",
            {"target_tenant_id": dest, "queue_name": "email"},
        )
        if code != 409 or body.get("code") != "tenant_split_error":
            raise RuntimeError(f"{src} expected split refuse: {code} {body}")
        for sid in email[src]:
            code, got = http_json("GET", f"/v1/jobs/{sid}?tenant_id={src}")
            if code != 200:
                raise RuntimeError(f"refused split mutated {sid}: {code} {got}")

    # Remove the cross-queue DAG, then split isolated email work.
    for src, dest in PAIRS:
        parent_id, child_id = broken[src]
        for jid in (child_id, parent_id):
            code, body = http_json(
                "POST", f"/v1/jobs/{jid}/cancel", {"tenant_id": src}
            )
            if code != 200:
                raise RuntimeError(f"cancel dag {src}/{jid} {code} {body}")
        plan = plan_split(src, dest, "email")
        if plan.get("ok") is not True:
            raise RuntimeError(f"{src} plan after cancel not ok: {plan}")
        if set(plan.get("move_job_ids") or []) != set(email[src]):
            raise RuntimeError(f"{src} unexpected move set {plan}")
        code, body = http_json(
            "POST",
            f"/v1/tenants/{src}/split",
            {"target_tenant_id": dest, "queue_name": "email"},
        )
        if code != 200:
            raise RuntimeError(f"split {src}->{dest} {code} {body}")
        if int(body.get("jobs_moved") or 0) != EMAIL_EACH:
            raise RuntimeError(f"split count {body}")
        for sid in email[src]:
            code, _ = http_json("GET", f"/v1/jobs/{sid}?tenant_id={src}")
            if code != 404:
                raise RuntimeError(f"source still owns {sid}")
            code, got = http_json("GET", f"/v1/jobs/{sid}?tenant_id={dest}")
            if code != 200 or got.get("job", {}).get("tenant_id") != dest:
                raise RuntimeError(f"dest missing {sid}: {code} {got}")
        for sid in keep[src]:
            code, _ = http_json("GET", f"/v1/jobs/{sid}?tenant_id={src}")
            if code != 200:
                raise RuntimeError(f"keep job left {src}: {sid}")

    for src in srcs:
        code, body = http_json("POST", f"/v1/tenants/{src}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume src {src} {code} {body}")

    expect = {d: KEEP_EACH + EMAIL_EACH for d in dests}
    expect.update({s: KEEP_EACH for s in srcs})
    wait_completed(srcs + dests, expect)
    logger.info("tenant-split stage 2 OK")
    if STAGE < 3:
        return 0

    # Stage 3: split while workers may already be leasing the email queue.
    race_pairs = (("race-src-a", "race-dst-a"), ("race-src-b", "race-dst-b"))
    rsrcs = [s for s, _ in race_pairs]
    rdests = [d for _, d in race_pairs]
    for tid in rsrcs + rdests:
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")
    remail: dict[str, list[str]] = {}
    rkeep: dict[str, list[str]] = {}
    for src, dest in race_pairs:
        remail[src] = enqueue(src, EMAIL_EACH, queue_name="email", prefix="race")
        rkeep[src] = enqueue(src, KEEP_EACH, queue_name="default", prefix="rkeep")
        enqueue(dest, KEEP_EACH, queue_name="default", prefix="rdst")
    for tid in rsrcs + rdests:
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")
    time.sleep(0.25)
    for src, dest in race_pairs:
        code, body = http_json(
            "POST",
            f"/v1/tenants/{src}/split",
            {"target_tenant_id": dest, "queue_name": "email"},
        )
        if code not in (200, 409):
            raise RuntimeError(f"race split {src} {code} {body}")
        for sid in remail[src]:
            sc, _ = http_json("GET", f"/v1/jobs/{sid}?tenant_id={src}")
            dc, _ = http_json("GET", f"/v1/jobs/{sid}?tenant_id={dest}")
            if sc == 200 and dc == 200:
                raise RuntimeError(f"{sid} owned by both tenants")
            if sc != 200 and dc != 200:
                raise RuntimeError(f"{sid} owned by neither tenant")
        logger.info("race split %s -> %s code=%s", src, dest, code)
    need = EMAIL_EACH + KEEP_EACH + KEEP_EACH
    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        totals = {
            src: completed(list_jobs(src)) + completed(list_jobs(dest))
            for src, dest in race_pairs
        }
        now = time.time()
        if now - last >= 2:
            logger.info("race drain %s need=%d", totals, need)
            last = now
        if all(n >= need for n in totals.values()):
            break
        time.sleep(0.2)
    else:
        raise RuntimeError(f"race drain timed out {totals}")
    # Every email/keep job finished on exactly one tenant.
    for src, dest in race_pairs:
        seen: set[str] = set()
        for tid in (src, dest):
            for j in list_jobs(tid):
                jid = str(j["id"])
                if jid in remail[src] or jid in rkeep[src]:
                    if j.get("status") != "completed":
                        raise RuntimeError(f"{tid} {jid} unfinished {j}")
                    if jid in seen:
                        raise RuntimeError(f"{jid} duplicated")
                    seen.add(jid)
        want = set(remail[src]) | set(rkeep[src])
        if seen != want:
            raise RuntimeError(f"{src} missing {want - seen}")
    logger.info("tenant-split stage 3 OK")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("tenant-split soak failed")
        sys.exit(1)
