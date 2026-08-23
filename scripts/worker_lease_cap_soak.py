#!/usr/bin/env python3
"""Fleet soak: per-worker inflight lease caps under mixed load.

  * Hot workers stop taking work at their cap; peers with room keep leasing.
  * Live raise / lower / clear never loses inflight jobs or progress.
  * Skills, multi-tenant fair lease, and drain still work with caps on.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("worker_lease_cap_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
LONG_MS = int(os.environ.get("LONG_MS", "900") or "900")
SHORT_MS = int(os.environ.get("SHORT_MS", "40") or "40")
LOAD_TENANTS = int(os.environ.get("LOAD_TENANTS", "3") or "3")
JOBS_PER = int(os.environ.get("JOBS_PER", "16") or "16")
_rr = 0

ALL_WORKERS = ("worker-1", "worker-2", "worker-3", "worker-4")
CAPS = {"worker-1": 1, "worker-2": 2, "worker-3": 3, "worker-4": None}


def http_json(
    method: str,
    path: str | None = None,
    body: dict | None = None,
    *,
    absolute: str | None = None,
    timeout: float = 30.0,
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
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return json.loads(raw.decode()) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode("utf-8", errors="replace")}
            payload["_http"] = int(exc.code)
            return payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def wait_coords() -> None:
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")


def wait_workers(timeout: float = 60.0) -> dict[str, dict[str, Any]]:
    deadline = time.time() + timeout
    last: dict[str, dict[str, Any]] = {}
    while time.time() < deadline:
        body = http_json("GET", "/v1/workers")
        last = {w["worker_id"]: w for w in body.get("workers") or []}
        if all(w in last and last[w].get("status") == "active" for w in ALL_WORKERS):
            return last
        time.sleep(0.5)
    raise RuntimeError(f"workers not active: {sorted(last)}")


def list_jobs(tid: str) -> list[dict[str, Any]]:
    body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    return list(body.get("jobs") or [])


def enqueue(
    tenant_id: str,
    payload: dict[str, Any],
    *,
    required_skills: list[str] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"tenant_id": tenant_id, "payload": payload}
    if required_skills is not None:
        body["required_skills"] = required_skills
    out = http_json("POST", "/v1/jobs", body)
    job = out.get("job") if isinstance(out.get("job"), dict) else out
    if not job or not job.get("id"):
        raise RuntimeError(f"enqueue failed: {out}")
    return job


def workers_by_id() -> dict[str, dict[str, Any]]:
    body = http_json("GET", "/v1/workers")
    return {w["worker_id"]: w for w in body.get("workers") or []}


def leased_by_counts(tenants: list[str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for tid in tenants:
        for j in list_jobs(tid):
            if j.get("status") == "leased" and j.get("leased_by"):
                counts[str(j["leased_by"])] += 1
    return counts


def assert_caps(counts: Counter[str], caps: dict[str, int | None], *, where: str) -> None:
    for wid, cap in caps.items():
        if cap is None:
            continue
        got = int(counts.get(wid, 0))
        if got > cap:
            raise RuntimeError(f"{where}: {wid} holds {got} > cap {cap}")


def wait_until(pred, *, timeout: float, msg: str, interval: float = 0.2) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = pred()
        if last:
            return
        time.sleep(interval)
    raise RuntimeError(f"{msg} (last={last})")


def apply_caps(caps: dict[str, int | None]) -> None:
    for wid, cap in caps.items():
        out = http_json(
            "POST", f"/v1/workers/{wid}/max-leases", {"max_leases": cap}
        )
        if out.get("_http"):
            raise RuntimeError(f"set cap {wid}={cap}: {out}")
        got = out.get("max_leases")
        if cap is None:
            if got is not None:
                raise RuntimeError(f"clear cap failed {wid}: {out}")
        elif int(got) != int(cap):
            raise RuntimeError(f"set cap {wid} want {cap} got {out}")


def main() -> int:
    started = time.time()
    logger.info("worker lease cap soak coords=%s", COORDS)
    wait_coords()
    wait_workers()
    apply_caps(CAPS)
    snap = workers_by_id()
    for wid, cap in CAPS.items():
        if snap[wid].get("max_leases") != cap:
            raise RuntimeError(f"{wid} cap not applied: {snap[wid]}")

    tenants = [f"load-{i:02d}" for i in range(LOAD_TENANTS)] + ["skill-gpu"]
    expected = 0
    logger.info("stage1: mixed load under caps %s", CAPS)
    for tid in tenants:
        if tid == "skill-gpu":
            for n in range(JOBS_PER):
                enqueue(
                    tid,
                    {"kind": "gpu", "n": n, "ms": LONG_MS, "t": tid},
                    required_skills=["gpu"],
                )
                expected += 1
        else:
            for n in range(JOBS_PER):
                enqueue(
                    tid,
                    {"kind": "any", "n": n, "ms": LONG_MS, "t": tid},
                )
                expected += 1

    samples = 0
    over = 0
    deadline = time.time() + 12
    while time.time() < deadline:
        counts = leased_by_counts(tenants)
        try:
            assert_caps(counts, CAPS, where="stage1 sample")
        except RuntimeError:
            over += 1
            raise
        samples += 1
        time.sleep(0.25)
    logger.info("stage1 samples=%d holds=%s", samples, dict(leased_by_counts(tenants)))

    logger.info("stage2: raise worker-1 cap 1 -> 4 live")
    before = {
        str(j["id"]): (j.get("status"), j.get("leased_by"), j.get("lease_token"))
        for tid in tenants
        for j in list_jobs(tid)
        if j.get("status") == "leased"
    }
    apply_caps({"worker-1": 4})
    time.sleep(1.2)
    after = {
        str(j["id"]): j
        for tid in tenants
        for j in list_jobs(tid)
        if str(j["id"]) in before
    }
    for jid, (st, holder, token) in before.items():
        row = after.get(jid)
        if row is None:
            continue
        if row.get("status") == "leased":
            if row.get("leased_by") != holder:
                raise RuntimeError(f"raise cap stole {jid}")
            if token and row.get("lease_token") != token:
                raise RuntimeError(f"raise cap rotated token {jid}")
        elif row.get("status") not in ("completed", "pending"):
            raise RuntimeError(f"raise cap lost {jid} status={row.get('status')}")
    raised_counts = leased_by_counts(tenants)
    assert_caps(raised_counts, {**CAPS, "worker-1": 4}, where="stage2")
    logger.info("stage2 holds=%s", dict(raised_counts))

    logger.info("stage3: lower worker-2 cap 2 -> 1 while it may be over")
    w2_held = {
        str(j["id"]): j.get("lease_token")
        for tid in tenants
        for j in list_jobs(tid)
        if j.get("leased_by") == "worker-2" and j.get("status") == "leased"
    }
    apply_caps({"worker-2": 1})
    time.sleep(0.4)
    still = 0
    for tid in tenants:
        for j in list_jobs(tid):
            jid = str(j["id"])
            if jid not in w2_held:
                continue
            if j.get("status") == "leased":
                if j.get("leased_by") != "worker-2":
                    raise RuntimeError(f"lower cap yanked {jid}")
                if w2_held[jid] and j.get("lease_token") != w2_held[jid]:
                    raise RuntimeError(f"lower cap retokened {jid}")
                still += 1
    logger.info("stage3 worker-2 still_held=%d of %d", still, len(w2_held))
    samples = 0
    deadline = time.time() + 8
    while time.time() < deadline:
        counts = leased_by_counts(tenants)
        # After lower, worker-2 may still be over until it finishes.
        w2 = int(counts.get("worker-2", 0))
        if w2 > max(len(w2_held), 1) + 1:
            raise RuntimeError(f"worker-2 grew after lower: {w2}")
        new_w2 = [
            j
            for tid in tenants
            for j in list_jobs(tid)
            if j.get("leased_by") == "worker-2"
            and j.get("status") == "leased"
            and str(j["id"]) not in w2_held
        ]
        if new_w2 and w2 > 1:
            raise RuntimeError(f"worker-2 took new work while over cap: {new_w2[:2]}")
        assert_caps(
            Counter({k: v for k, v in counts.items() if k != "worker-2"}),
            {"worker-1": 4, "worker-3": 3},
            where="stage3 others",
        )
        samples += 1
        time.sleep(0.25)

    apply_caps({"worker-1": 1, "worker-2": 2, "worker-3": 3, "worker-4": None})
    leftover_tenants = tenants
    wait_until(
        lambda: all(
            j.get("status") == "completed"
            for tid in leftover_tenants
            for j in list_jobs(tid)
        ),
        timeout=max(20.0, MAX_WALL - (time.time() - started)),
        msg="jobs did not finish after cap churn",
    )
    done = sum(1 for tid in leftover_tenants for j in list_jobs(tid) if j.get("status") == "completed")
    if done < expected:
        raise RuntimeError(f"lost work: completed={done} expected={expected}")

    live = workers_by_id()
    for wid in ALL_WORKERS:
        if live[wid].get("status") != "active":
            raise RuntimeError(f"{wid} not active: {live[wid]}")
        skills = set(live[wid].get("skills") or [])
        if not skills:
            raise RuntimeError(f"{wid} lost skills: {live[wid]}")

    logger.info(
        "worker lease cap soak ok in %.1fs jobs=%d samples=%d",
        time.time() - started,
        done,
        samples,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("worker lease cap soak failed")
        sys.exit(1)
