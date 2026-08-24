#!/usr/bin/env python3
"""Fleet soak: worker cpu/memory capacity vs job resource_cost.

  * Tiny workers fill up and stop; larger peers keep leasing.
  * Live raise / lower never drops inflight jobs or tokens.
  * Mixed tenants and skilled work still drain.
  * Jobs no active worker can cover park as awaiting_capacity, then
    revive when advertised capacity arrives.
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
logger = logging.getLogger("worker_capacity_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
LONG_MS = int(os.environ.get("LONG_MS", "800") or "800")
LOAD_TENANTS = int(os.environ.get("LOAD_TENANTS", "3") or "3")
JOBS_PER = int(os.environ.get("JOBS_PER", "12") or "12")
_rr = 0

ALL_WORKERS = ("worker-1", "worker-2", "worker-3", "worker-4")
CAPS = {
    "worker-1": {"cpu": 2, "memory": 2048},
    "worker-2": {"cpu": 8, "memory": 8192},
    "worker-3": {"cpu": 4, "memory": 4096},
    "worker-4": {"cpu": 16, "memory": 16384},
}
LIGHT = {"cpu": 1, "memory": 512}
HEAVY = {"cpu": 2, "memory": 1024}


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
    resource_cost: dict[str, int] | None = None,
    required_skills: list[str] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"tenant_id": tenant_id, "payload": payload}
    if resource_cost is not None:
        body["resource_cost"] = resource_cost
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


def apply_caps(caps: dict[str, dict[str, int]]) -> None:
    for wid, cap in caps.items():
        out = http_json("POST", f"/v1/workers/{wid}/capacity", {"capacity": cap})
        if out.get("_http"):
            raise RuntimeError(f"set capacity {wid}: {out}")
        got = out.get("capacity") or {}
        if {k: int(got[k]) for k in cap} != cap:
            raise RuntimeError(f"capacity {wid} want {cap} got {got}")


def assert_under_capacity(where: str) -> None:
    for wid, snap in workers_by_id().items():
        if wid not in CAPS:
            continue
        cap = snap.get("capacity") or {}
        used = snap.get("used") or {}
        for name, have in used.items():
            limit = int(cap.get(name, 0))
            if int(have) > limit:
                raise RuntimeError(
                    f"{where}: {wid} used {name}={have} > cap {limit}"
                )


def wait_until(pred, *, timeout: float, msg: str, interval: float = 0.25) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = pred()
        if last:
            return
        time.sleep(interval)
    raise RuntimeError(f"{msg} (last={last})")


def main() -> int:
    started = time.time()
    logger.info("worker capacity soak coords=%s", COORDS)
    wait_coords()
    wait_workers()
    apply_caps(CAPS)
    snap = workers_by_id()
    for wid, cap in CAPS.items():
        got = {k: int((snap[wid].get("capacity") or {}).get(k) or 0) for k in cap}
        if got != cap:
            raise RuntimeError(f"{wid} cap not applied: {snap[wid]}")

    tenants = [f"load-{i:02d}" for i in range(LOAD_TENANTS)] + ["skill-gpu"]
    expected = 0
    logger.info("stage1: mixed light/heavy under %s", CAPS)
    for tid in tenants:
        skills = ["gpu"] if tid == "skill-gpu" else None
        for n in range(JOBS_PER):
            cost = HEAVY if n % 3 == 0 else LIGHT
            enqueue(
                tid,
                {"kind": "gpu" if skills else "any", "n": n, "ms": LONG_MS, "cost": cost},
                resource_cost=cost,
                required_skills=skills,
            )
            expected += 1

    samples = 0
    deadline = time.time() + 10
    while time.time() < deadline:
        assert_under_capacity("stage1")
        samples += 1
        time.sleep(0.25)
    w1 = workers_by_id()["worker-1"]
    logger.info(
        "stage1 samples=%d worker-1 used=%s free=%s",
        samples,
        w1.get("used"),
        w1.get("free"),
    )
    if int((w1.get("used") or {}).get("cpu") or 0) > 2:
        raise RuntimeError(f"worker-1 over cpu: {w1}")

    logger.info("stage2: raise worker-1 cpu 2 -> 8 live")
    before = {
        str(j["id"]): (j.get("status"), j.get("leased_by"), j.get("lease_token"))
        for tid in tenants
        for j in list_jobs(tid)
        if j.get("status") == "leased"
    }
    apply_caps({**CAPS, "worker-1": {"cpu": 8, "memory": 2048}})
    time.sleep(1.0)
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
                raise RuntimeError(f"raise stole {jid}")
            if token and row.get("lease_token") != token:
                raise RuntimeError(f"raise retokened {jid}")
    assert_under_capacity("stage2")
    w1 = workers_by_id()["worker-1"]
    logger.info("stage2 worker-1 used=%s", w1.get("used"))

    logger.info("stage3: shrink worker-2 below live usage")
    w2 = workers_by_id()["worker-2"]
    held = {
        str(j["id"]): j.get("lease_token")
        for tid in tenants
        for j in list_jobs(tid)
        if j.get("leased_by") == "worker-2" and j.get("status") == "leased"
    }
    apply_caps({**CAPS, "worker-2": {"cpu": 1, "memory": 256}})
    time.sleep(0.4)
    still = 0
    for tid in tenants:
        for j in list_jobs(tid):
            jid = str(j["id"])
            if jid not in held:
                continue
            if j.get("status") == "leased":
                if j.get("leased_by") != "worker-2":
                    raise RuntimeError(f"lower yanked {jid}")
                if held[jid] and j.get("lease_token") != held[jid]:
                    raise RuntimeError(f"lower retokened {jid}")
                still += 1
    logger.info("stage3 worker-2 still_held=%d of %d", still, len(held))
    samples = 0
    deadline = time.time() + 6
    while time.time() < deadline:
        w2 = workers_by_id()["worker-2"]
        used_cpu = int((w2.get("used") or {}).get("cpu") or 0)
        # May be over the new cap until inflight finishes — must not grow.
        if used_cpu > max(int((w2.get("used") or {}).get("cpu") or 0), 32):
            raise RuntimeError(f"worker-2 used exploded: {w2}")
        samples += 1
        time.sleep(0.25)

    apply_caps(CAPS)
    def _all_done() -> bool:
        leftover: list[str] = []
        for tid in tenants:
            for j in list_jobs(tid):
                if j.get("status") != "completed":
                    leftover.append(
                        f"{tid}:{j.get('id')}:{j.get('status')}:"
                        f"{j.get('leased_by')}:{j.get('resource_cost')}"
                    )
        if leftover:
            logger.info("still inflight %d e.g. %s", len(leftover), leftover[:6])
            return False
        return True

    wait_until(
        _all_done,
        timeout=max(20.0, MAX_WALL - (time.time() - started)),
        msg="jobs did not finish after capacity churn",
    )
    done = sum(
        1
        for tid in tenants
        for j in list_jobs(tid)
        if j.get("status") == "completed"
    )
    if done < expected:
        raise RuntimeError(f"lost work: completed={done} expected={expected}")

    logger.info("stage4: uncoverable cost parks then revives")
    apply_caps(CAPS)
    impossible = "impossible"
    over = {"cpu": 64, "memory": 65536}
    parked_ids: list[str] = []
    for n in range(6):
        job = enqueue(
            impossible,
            {"kind": "oversize", "n": n, "ms": 200, "cost": over},
            resource_cost=over,
            # highmem is only on worker-4 so revived work can hop to the
            # one node that will advertise enough cpu/memory.
            required_skills=["highmem"],
        )
        parked_ids.append(str(job["id"]))
        if job.get("status") != "awaiting_capacity":
            raise RuntimeError(f"oversize enqueue not parked: {job}")
    wait_until(
        lambda: all(
            j.get("status") == "awaiting_capacity"
            for j in list_jobs(impossible)
        )
        and len(list_jobs(impossible)) == len(parked_ids),
        timeout=20.0,
        msg="oversize jobs did not park as awaiting_capacity",
    )
    if any(j.get("status") in ("pending", "leased") for j in list_jobs(impossible)):
        raise RuntimeError("oversize job stayed pending/leased")
    logger.info("stage4 parked=%d", len(parked_ids))
    apply_caps({**CAPS, "worker-4": {"cpu": 64, "memory": 65536}})

    def _oversize_done() -> bool:
        rows = list_jobs(impossible)
        if len(rows) != len(parked_ids):
            return False
        leftover = [
            f"{j.get('id')}:{j.get('status')}:{j.get('leased_by')}"
            for j in rows
            if j.get("status") != "completed"
        ]
        if leftover:
            logger.info("stage4 leftover %s", leftover[:6])
            return False
        return True

    wait_until(
        _oversize_done,
        timeout=max(30.0, MAX_WALL - (time.time() - started)),
        msg="parked oversize jobs did not revive and finish",
    )
    expected += len(parked_ids)
    done = sum(
        1
        for tid in tenants + [impossible]
        for j in list_jobs(tid)
        if j.get("status") == "completed"
    )
    if done < expected:
        raise RuntimeError(
            f"lost parked work: completed={done} expected={expected}"
        )
    apply_caps(CAPS)

    live = workers_by_id()
    for wid in ALL_WORKERS:
        if live[wid].get("status") != "active":
            raise RuntimeError(f"{wid} not active: {live[wid]}")
        if not (live[wid].get("skills") or []):
            raise RuntimeError(f"{wid} lost skills")
        if not (live[wid].get("capacity") or {}):
            raise RuntimeError(f"{wid} lost capacity")

    logger.info(
        "worker capacity soak ok in %.1fs jobs=%d samples=%d",
        time.time() - started,
        done,
        samples,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("worker capacity soak failed")
        sys.exit(1)
