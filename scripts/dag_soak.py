#!/usr/bin/env python3
"""Fleet soak: atomic DAG enqueue under multi-tenant multi-worker load.

Production-like mix:

  * Dual coordinators, four workers, six tenants.
  * Diamond/join DAGs submitted with the same batch_key from many threads
    (double-click / crash-retry). Exactly one graph per key may land.
  * Mid-drain retries must reuse the committed graph (no duplicates).
  * Invalid graphs (cycle, pending-cap, dead parent) must leave nothing.
  * Same batch_key on two tenants is independent.
  * Join nodes must not become leased/completed while a parent is unfinished.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dag_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "6") or "6")
DAGS_PER_TENANT = int(os.environ.get("DAGS_PER_TENANT", "4") or "4")
DUP_SUBMITS = int(os.environ.get("DUP_SUBMITS", "4") or "4")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
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


def dag_jobs(i: int) -> list[dict[str, Any]]:
    return [
        {
            "ref": "extract",
            "payload": {"kind": "work", "ms": 15, "step": "extract", "i": i},
        },
        {
            "ref": "t1",
            "payload": {"kind": "work", "ms": 15, "step": "t1", "i": i},
            "depends_on": ["extract"],
        },
        {
            "ref": "t2",
            "payload": {"kind": "work", "ms": 15, "step": "t2", "i": i},
            "depends_on": ["extract"],
        },
        {
            "ref": "join",
            "payload": {"kind": "work", "ms": 15, "step": "join", "i": i},
            "depends_on": ["t1", "t2"],
        },
        {
            "ref": "notify",
            "payload": {"kind": "work", "ms": 10, "step": "notify", "i": i},
            "depends_on": ["join"],
        },
    ]


def cycle_jobs() -> list[dict[str, Any]]:
    return [
        {"ref": "a", "payload": {"kind": "work", "ms": 10}, "depends_on": ["b"]},
        {"ref": "b", "payload": {"kind": "work", "ms": 10}, "depends_on": ["a"]},
    ]


def list_jobs(tid: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def usage_enqueues(tid: str) -> int:
    code, body = http_json("GET", f"/v1/usage?tenant_id={tid}")
    if code != 200:
        raise RuntimeError(f"usage {tid} {code}: {body}")
    usage = body.get("usage") or {}
    return int(usage.get("enqueues") or 0)


def submit_dag(tid: str, key: str, jobs: list[dict[str, Any]]) -> tuple[int, dict]:
    return http_json(
        "POST",
        "/v1/jobs/dag",
        {"tenant_id": tid, "batch_key": key, "jobs": jobs},
    )


def tenant_id(t: int) -> str:
    return f"dag-{t:02d}"


def check_parent_before_child(jobs: list[dict[str, Any]]) -> None:
    by_id = {j["id"]: j for j in jobs}
    for job in jobs:
        deps = job.get("depends_on") or []
        if not deps:
            continue
        st = job.get("status")
        if st not in ("leased", "completed"):
            continue
        for pid in deps:
            parent = by_id.get(pid)
            if parent is None:
                raise RuntimeError(
                    f"child {job['id']} depends on missing parent {pid}"
                )
            pst = parent.get("status")
            if pst != "completed":
                raise RuntimeError(
                    f"child {job['id']} status={st} while parent {pid} "
                    f"is {pst}"
                )


def assert_completed_order(jobs: list[dict[str, Any]]) -> None:
    by_id = {j["id"]: j for j in jobs}
    for job in jobs:
        child_done = job.get("completed_at")
        if not child_done:
            continue
        for pid in job.get("depends_on") or []:
            parent = by_id.get(pid) or {}
            parent_done = parent.get("completed_at")
            if not parent_done:
                raise RuntimeError(
                    f"child {job['id']} completed without parent {pid}"
                )
            if parent_done > child_done:
                raise RuntimeError(
                    f"parent {pid} completed_at {parent_done} after child "
                    f"{job['id']} {child_done}"
                )


def main() -> int:
    expect_dags = TENANTS * DAGS_PER_TENANT + 2  # + shared-key on two tenants
    expect_jobs = expect_dags * 5
    logger.info(
        "dag soak tenants=%d dags_each=%d dups=%d expect_jobs=%d",
        TENANTS,
        DAGS_PER_TENANT,
        DUP_SUBMITS,
        expect_jobs,
    )
    wait_coords()

    batches = [
        (tenant_id(t), f"batch-{t:02d}-{i}", dag_jobs(i))
        for t in range(TENANTS)
        for i in range(DAGS_PER_TENANT)
    ]
    # Same batch_key, two tenants: must not collide or share job ids.
    batches.append((tenant_id(0), "shared-key", dag_jobs(90)))
    batches.append((tenant_id(1), "shared-key", dag_jobs(91)))

    created = 0
    reused = 0
    errors: list[str] = []

    def submit(item: tuple[str, str, list[dict[str, Any]]]) -> tuple[bool, str]:
        tid, key, jobs = item
        code, body = submit_dag(tid, key, jobs)
        if code not in (200, 201):
            return False, f"{tid}/{key} -> {code} {body}"
        return bool(body.get("created")), ""

    work = batches * DUP_SUBMITS
    with ThreadPoolExecutor(max_workers=16) as pool:
        futs = [pool.submit(submit, w) for w in work]
        for f in as_completed(futs):
            ok, err = f.result()
            if err:
                errors.append(err)
                continue
            if ok:
                created += 1
            else:
                reused += 1
    if errors:
        raise RuntimeError("enqueue errors: " + "; ".join(errors[:6]))
    if created != expect_dags:
        raise RuntimeError(f"created {created} unique DAGs want {expect_dags}")
    if reused != expect_dags * (DUP_SUBMITS - 1):
        raise RuntimeError(
            f"idempotent reuse {reused} want {expect_dags * (DUP_SUBMITS - 1)}"
        )
    logger.info("enqueued unique=%d reused=%d", created, reused)

    # Atomic rejects: cycle, pending cap, dead parent. Concurrent hits.
    code, _ = http_json(
        "POST",
        "/v1/tenants/dag-cap/pending-limit",
        {"max_pending_jobs": 1},
    )
    if code not in (200, 201):
        raise RuntimeError(f"set pending-limit failed {code}")
    code, body = http_json(
        "POST", "/v1/jobs", {"tenant_id": "dag-cap", "payload": {"keep": True}}
    )
    if code != 201:
        raise RuntimeError(f"cap filler enqueue {code} {body}")

    # Unmatched skill so fleet workers cannot complete it before we cancel.
    code, dead_body = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": "dag-dead",
            "payload": {"x": 1},
            "required_skills": ["soak-dead-parent"],
        },
    )
    if code != 201:
        raise RuntimeError(f"dead parent enqueue {code} {dead_body}")
    dead_id = (dead_body.get("job") or {}).get("id")
    ccode, cbody = http_json(
        "POST",
        f"/v1/jobs/{dead_id}/cancel",
        {"tenant_id": "dag-dead"},
    )
    if ccode != 200:
        raise RuntimeError(f"cancel dead parent {ccode} {cbody}")
    nack_ok = True

    reject_work: list[tuple[str, str, list[dict[str, Any]], int]] = []
    for i in range(DUP_SUBMITS):
        reject_work.append(("dag-cyc", f"cyc-{i % 2}", cycle_jobs(), 400))
        reject_work.append(("dag-cap", "too-big", dag_jobs(0), 400))
        if nack_ok and dead_id:
            reject_work.append(
                (
                    "dag-dead",
                    f"child-{i}",
                    [
                        {
                            "ref": "c",
                            "payload": {"kind": "work", "ms": 10},
                            "depends_on": [str(dead_id)],
                        },
                        {
                            "ref": "d",
                            "payload": {"kind": "work", "ms": 10},
                            "depends_on": ["c"],
                        },
                    ],
                    400,
                )
            )

    def reject(item: tuple[str, str, list[dict[str, Any]], int]) -> str | None:
        tid, key, jobs, want = item
        code, body = submit_dag(tid, key, jobs)
        if code != want:
            return f"{tid}/{key} -> {code} want {want} {body}"
        return None

    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = [pool.submit(reject, w) for w in reject_work]
        rejects = [f.result() for f in as_completed(futs)]
    bad = [e for e in rejects if e]
    if bad:
        raise RuntimeError("reject-path errors: " + "; ".join(bad[:6]))

    if len(list_jobs("dag-cyc")) != 0:
        raise RuntimeError("cycle DAG left leftover jobs")
    cap_jobs = list_jobs("dag-cap")
    if len(cap_jobs) != 1:
        raise RuntimeError(f"pending-cap left {len(cap_jobs)} jobs, want 1")
    dead_jobs = list_jobs("dag-dead")
    extra_dead = [j for j in dead_jobs if j.get("id") != dead_id]
    if extra_dead:
        raise RuntimeError(f"dead-parent DAG left {len(extra_dead)} jobs")
    if not any(j.get("status") == "cancelled" for j in dead_jobs):
        raise RuntimeError("expected cancelled dead parent")

    # Exact job count right after enqueue (no half-graphs / no dups).
    happy = [tenant_id(t) for t in range(TENANTS)]
    total = sum(len(list_jobs(t)) for t in happy)
    if total != expect_jobs:
        raise RuntimeError(f"post-enqueue jobs={total} want {expect_jobs}")
    for t in happy:
        billed = usage_enqueues(t)
        got = len(list_jobs(t))
        if billed != got:
            raise RuntimeError(f"{t} usage enqueues={billed} jobs={got}")

    # Cross-tenant isolation: job id sets must be disjoint.
    ids_0 = {j["id"] for j in list_jobs(tenant_id(0))}
    ids_1 = {j["id"] for j in list_jobs(tenant_id(1))}
    if ids_0 & ids_1:
        raise RuntimeError("cross-tenant job id collision")
    logger.info("post-enqueue jobs=%d rejects ok", total)

    deadline = time.time() + MAX_WALL
    last = 0.0
    retried = False
    while time.time() < deadline:
        done = 0
        pending = 0
        snapshot: list[dict[str, Any]] = []
        for t in happy:
            snapshot.extend(list_jobs(t))
        check_parent_before_child(snapshot)
        for j in snapshot:
            st = j.get("status")
            if st == "completed":
                done += 1
            elif st in ("pending", "leased"):
                pending += 1
            else:
                raise RuntimeError(f"unexpected status {st} on {j.get('id')}")
        now = time.time()
        if now - last >= 2:
            logger.info("completed=%d/%d pending=%d", done, expect_jobs, pending)
            last = now
        # Simulate client crash-retry while workers are draining.
        if not retried and done >= max(10, expect_jobs // 5):
            retried = True
            mid_created = 0
            with ThreadPoolExecutor(max_workers=12) as pool:
                futs = [pool.submit(submit, b) for b in batches]
                for f in as_completed(futs):
                    ok, err = f.result()
                    if err:
                        raise RuntimeError(f"mid-drain retry failed: {err}")
                    if ok:
                        mid_created += 1
            if mid_created != 0:
                raise RuntimeError(
                    f"mid-drain retry created {mid_created} new DAGs"
                )
            after = sum(len(list_jobs(t)) for t in happy)
            if after != expect_jobs:
                raise RuntimeError(
                    f"mid-drain retry duplicated graphs: {after} != {expect_jobs}"
                )
            logger.info("mid-drain retry reused all %d batches", len(batches))
        if done == expect_jobs and pending == 0:
            assert_completed_order(snapshot)
            # Cap/cycle tenants must still be untouched.
            if len(list_jobs("dag-cyc")) != 0:
                raise RuntimeError("cycle tenant grew during drain")
            if len(list_jobs("dag-cap")) != 1:
                raise RuntimeError("cap tenant grew during drain")
            logger.info("dag soak OK jobs=%d", done)
            return 0
        time.sleep(0.4)
    raise RuntimeError("dag soak timed out waiting for drain")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("dag soak failed")
        sys.exit(1)
