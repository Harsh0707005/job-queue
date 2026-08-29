#!/usr/bin/env python3
"""Fleet soak: per-tenant archival of finished jobs.

Stages
------
1. Multi-tenant enqueue. Age and size tenants archive; a keep tenant
   does not; an offboard tenant is wiped after archive.
2. Workers complete jobs. Leader ticks archive finished rows to the
   shared blob store. list/get still rehydrate payload + result.
3. Kill the coordinator leader mid-archive. A standby takes over;
   leftover fat rows archive; already-stubbed rows stay readable.
4. Live export/restore of an archived tenant. Offboard wipes archive
   files. The keep tenant keeps working.

  COMPOSE_PROJECT_NAME=jq-arch docker compose -p jq-arch \\
    -f docker-compose.yml -f docker-compose.coordinator-leader.yml \\
    -f docker-compose.archive.yml up --build -d
  docker logs -f jq-arch-archive-soak-1
"""

from __future__ import annotations

import http.client
import json
import logging
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote, urlencode

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("archive_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080,http://coordinator-3:8080"
    ).split(",")
    if u.strip()
]
DSN = os.environ.get("JOB_QUEUE_DSN") or ""
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
JOBS = int(os.environ.get("JOBS_PER_TENANT", "16") or "16")
PAYLOAD_BYTES = int(os.environ.get("PAYLOAD_BYTES", "4096") or "4096")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-arch")
_rr = 0


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str) -> None:
        super().__init__("localhost")
        self.socket_path = socket_path

    def connect(self) -> None:  # noqa: D102
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.socket_path)
        self.sock = sock


def http_one(
    base: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        base + path, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return int(resp.status), json.loads(raw.decode()) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read() if exc.fp else b""
        try:
            payload = json.loads(raw.decode() or "{}")
        except json.JSONDecodeError:
            payload = {"error": raw.decode(errors="replace")}
        return int(exc.code), payload


def http_json(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    global _rr
    start = _rr % len(COORDS)
    _rr += 1
    last: Exception | None = None
    for base in COORDS[start:] + COORDS[:start]:
        try:
            code, payload = http_one(base, method, path, body, timeout=timeout)
            if 500 <= code < 600:
                last = RuntimeError(f"{base}{path} -> {code}: {payload}")
                continue
            return code, payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"all coordinators failed: {last}")


def wait_healthy() -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        ready = 0
        for base in COORDS:
            try:
                code, h = http_one(base, "GET", "/health", timeout=3)
                if code == 200 and h.get("ok"):
                    ready += 1
            except Exception:  # noqa: BLE001
                pass
        if ready >= 2:
            return
        time.sleep(1)
    raise RuntimeError("coordinators not ready")


def poll_leaders() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for base in COORDS:
        try:
            code, h = http_one(base, "GET", "/health", timeout=3)
            if code == 200:
                out.append({"base": base, **h})
        except Exception:  # noqa: BLE001
            continue
    return out


def unique_leader() -> dict[str, Any] | None:
    rows = poll_leaders()
    leads = [r for r in rows if r.get("leader")]
    if len(leads) != 1:
        return None
    names = {r.get("leader_node") for r in rows if r.get("leader_node")}
    if names != {leads[0].get("node")}:
        return None
    return leads[0]


def wait_unique_leader(*, timeout: float = 25.0) -> dict[str, Any]:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = unique_leader()
        if last is not None:
            return last
        time.sleep(0.2)
    raise RuntimeError(f"no unique leader: {poll_leaders()}")


def _docker_sock(method: str, path: str) -> tuple[int, bytes]:
    conn = _UnixHTTPConnection("/var/run/docker.sock")
    try:
        conn.request(method, path, headers={"Host": "localhost"})
        resp = conn.getresponse()
        return int(resp.status), resp.read()
    finally:
        conn.close()


def docker_kill(node: str) -> None:
    name = f"{COMPOSE_PROJECT}-{node}-1"
    try:
        r = subprocess.run(
            ["docker", "kill", name],
            capture_output=True,
            text=True,
            check=False,
        )
        if r.returncode == 0:
            return
        cli_err = r.stderr
    except FileNotFoundError:
        cli_err = "docker cli not installed"
    code, body = _docker_sock("POST", f"/v1.41/containers/{name}/kill")
    if code in (200, 204):
        return
    filt = quote(json.dumps({"name": [node]}))
    code, raw = _docker_sock("GET", f"/v1.41/containers/json?filters={filt}")
    if code != 200:
        raise RuntimeError(f"kill {node} failed: {cli_err} sock={code} {body!r}")
    rows = json.loads(raw.decode() or "[]")
    if not rows:
        raise RuntimeError(f"kill {node} failed: no container ({cli_err})")
    cid = rows[0]["Id"]
    code, body = _docker_sock("POST", f"/v1.41/containers/{cid}/kill")
    if code not in (200, 204):
        raise RuntimeError(f"kill {cid} -> {code} {body!r}")


def connect_queue():
    from job_queue import JobQueue

    if not DSN:
        raise RuntimeError("JOB_QUEUE_DSN is required")
    return JobQueue.connect(DSN, min_size=1, max_size=4)


def tenant_ids() -> dict[str, str]:
    # age / size / keep / offboard (plus extras if TENANTS > 4)
    names = ["age", "size", "keep", "gone"]
    while len(names) < TENANTS:
        names.append(f"extra-{len(names):02d}")
    return {name: f"arch-{name}" for name in names[:TENANTS]}


def enqueue_job(*, tenant_id: str, secret: str, pad: str, n: int) -> None:
    code, out = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": tenant_id,
            "payload": {
                "kind": "archive",
                "secret": secret,
                "n": n,
                "blob": pad,
                "sleep_ms": 80,
                "ack_result": {"echo": secret, "n": n, "blob": pad},
            },
            "max_attempts": 4,
            "visibility_timeout_seconds": 45,
        },
    )
    if code not in (200, 201):
        raise RuntimeError(f"enqueue {tenant_id} -> {code}: {out}")


def wait_completed(q, tenants: list[str], want: int, *, deadline: float) -> None:
    while time.time() < deadline:
        try:
            q.process_job_archives(limit=200)
        except Exception as exc:  # noqa: BLE001
            logger.warning("archive tick: %s", exc)
        done = 0
        pending = 0
        with q._pool.connection() as conn:
            for tid in tenants:
                row = conn.execute(
                    """
                    SELECT
                      COUNT(*) FILTER (WHERE status = 'completed')::int AS done,
                      COUNT(*) FILTER (
                          WHERE status IN ('pending', 'leased')
                      )::int AS live
                    FROM jobs WHERE tenant_id = %s
                    """,
                    (tid,),
                ).fetchone()
                done += int(row["done"] or 0)
                pending += int(row["live"] or 0)
        logger.info("completed=%s live=%s want=%s", done, pending, want)
        if done >= want and pending == 0:
            return
        time.sleep(1.0)
    raise RuntimeError(f"jobs did not finish (want {want})")


def archive_counts(q, tenant_id: str) -> tuple[int, int]:
    with q._pool.connection() as conn:
        row = conn.execute(
            """
            SELECT
              COUNT(*) FILTER (WHERE archived_at IS NOT NULL)::int AS archived,
              COUNT(*) FILTER (
                  WHERE status IN ('completed', 'failed', 'dead', 'cancelled')
                    AND archived_at IS NULL
              )::int AS live_finished
            FROM jobs WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
    return int(row["archived"] or 0), int(row["live_finished"] or 0)


def assert_rehydrate(q, tenant_id: str, secrets: dict[int, str]) -> None:
    jobs = q.list_jobs(tenant_id=tenant_id, limit=500)
    if len(jobs) != len(secrets):
        raise RuntimeError(
            f"{tenant_id} list {len(jobs)} != expected {len(secrets)}"
        )
    for job in jobs:
        n = int(job.payload.get("n"))
        secret = secrets[n]
        if job.payload.get("secret") != secret:
            raise RuntimeError(f"{tenant_id} {job.id} payload mismatch")
        if not isinstance(job.result, dict) or job.result.get("echo") != secret:
            raise RuntimeError(f"{tenant_id} {job.id} result mismatch: {job.result}")
        got = q.get(job.id, tenant_id=tenant_id)
        if got is None or got.result != job.result:
            raise RuntimeError(f"{tenant_id} get {job.id} drifted")


def main() -> int:
    wait_healthy()
    lead0 = wait_unique_leader()
    logger.info("leader=%s epoch=%s", lead0.get("node"), lead0.get("leader_epoch"))
    q = connect_queue()
    ids = tenant_ids()
    age_tid = ids["age"]
    size_tid = ids["size"]
    keep_tid = ids["keep"]
    gone_tid = ids.get("gone") or ids["age"]
    pad = "A" * PAYLOAD_BYTES
    secrets: dict[str, dict[int, str]] = {tid: {} for tid in ids.values()}

    code, body = http_json(
        "POST",
        f"/v1/tenants/{age_tid}/archive",
        {"archive_after_seconds": 0},
    )
    if code != 200:
        raise RuntimeError(f"set age archive -> {code}: {body}")
    code, body = http_json(
        "POST",
        f"/v1/tenants/{size_tid}/archive",
        {"archive_max_live_bytes": 2048},
    )
    if code != 200:
        raise RuntimeError(f"set size archive -> {code}: {body}")
    if gone_tid != age_tid:
        code, body = http_json(
            "POST",
            f"/v1/tenants/{gone_tid}/archive",
            {"archive_after_seconds": 0},
        )
        if code != 200:
            raise RuntimeError(f"set gone archive -> {code}: {body}")

    for tid in ids.values():
        for n in range(JOBS):
            secret = f"{tid}-{n}"
            secrets[tid][n] = secret
            enqueue_job(tenant_id=tid, secret=secret, pad=pad, n=n)

    want = JOBS * len(ids)
    deadline = time.time() + MAX_WALL
    wait_completed(q, list(ids.values()), want, deadline=deadline)

    # Kick a few archive passes so the kill hits mid-batch if possible.
    for _ in range(3):
        try:
            q.process_job_archives(limit=8)
        except Exception as exc:  # noqa: BLE001
            logger.warning("pre-kill archive: %s", exc)

    age_n, age_live = archive_counts(q, age_tid)
    logger.info("pre-kill age archived=%s live_finished=%s", age_n, age_live)

    before = wait_unique_leader()
    dead = str(before.get("node") or "")
    epoch0 = int(before.get("leader_epoch") or 0)
    logger.info("killing leader %s mid-archive", dead)
    docker_kill(dead)

    t1 = time.time()
    after = None
    while time.time() - t1 < 25:
        after = unique_leader()
        if after is not None and after.get("node") != dead:
            break
        after = None
        time.sleep(0.25)
    if after is None:
        raise RuntimeError(f"no failover leader after killing {dead}")
    logger.info(
        "failover leader=%s epoch=%s (was %s/%s)",
        after.get("node"),
        after.get("leader_epoch"),
        dead,
        epoch0,
    )

    # Keep tenant still accepts work after the kill.
    extra_secret = f"{keep_tid}-post"
    enqueue_job(tenant_id=keep_tid, secret=extra_secret, pad=pad, n=JOBS)
    secrets[keep_tid][JOBS] = extra_secret

    drain_deadline = time.time() + min(90, MAX_WALL)
    while time.time() < drain_deadline:
        try:
            q.process_job_archives(limit=200)
        except Exception as exc:  # noqa: BLE001
            logger.warning("post-kill archive: %s", exc)
        age_n, age_live = archive_counts(q, age_tid)
        size_n, size_live = archive_counts(q, size_tid)
        keep_n, keep_live = archive_counts(q, keep_tid)
        extra_live = 0
        with q._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)::int AS n FROM jobs
                WHERE tenant_id = %s
                  AND status IN ('pending', 'leased')
                """,
                (keep_tid,),
            ).fetchone()
            extra_live = int(row["n"] or 0)
        logger.info(
            "post-kill age=%s/%s size=%s/%s keep=%s/%s extra_live=%s",
            age_n,
            age_live,
            size_n,
            size_live,
            keep_n,
            keep_live,
            extra_live,
        )
        if age_live == 0 and age_n >= JOBS and extra_live == 0:
            break
        time.sleep(1.0)
    else:
        raise RuntimeError("archive did not finish after failover")

    if age_n < JOBS:
        raise RuntimeError(f"{age_tid} archived {age_n} < {JOBS}")
    if keep_n != 0:
        raise RuntimeError(f"{keep_tid} unexpectedly archived {keep_n}")
    if size_n < 1:
        raise RuntimeError(f"{size_tid} size policy archived nothing")

    assert_rehydrate(q, age_tid, secrets[age_tid])
    assert_rehydrate(q, size_tid, secrets[size_tid])
    assert_rehydrate(q, keep_tid, secrets[keep_tid])

    # Stubs really left Postgres.
    with q._pool.connection() as conn:
        fat = conn.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM jobs
            WHERE tenant_id = %s
              AND archived_at IS NOT NULL
              AND payload ? '_jq_archived'
            """,
            (age_tid,),
        ).fetchone()
    if int(fat["n"] or 0) < JOBS:
        raise RuntimeError(f"{age_tid} missing archive stubs")

    snap = q.export_tenant(age_tid)
    dest = f"{age_tid}-dst"
    rst = q.restore_tenant(
        dest, snapshot_id=snap.id, batch_size=40, run_until_complete=True
    )
    if rst.status != "complete":
        raise RuntimeError(f"restore did not complete: {rst}")
    dest_jobs = q.list_jobs(tenant_id=dest, limit=500)
    if len(dest_jobs) != JOBS:
        raise RuntimeError(f"restore dest listed {len(dest_jobs)}")
    for job in dest_jobs:
        n = int(job.payload.get("n"))
        if job.payload.get("secret") != secrets[age_tid][n]:
            raise RuntimeError(f"restored payload mismatch {job.id}")
        if not job.is_archived:
            raise RuntimeError(f"restored job {job.id} is not archived")

    if gone_tid in ids.values() and gone_tid != keep_tid:
        code, off = http_json("POST", f"/v1/tenants/{gone_tid}/offboard", {})
        if code != 200:
            raise RuntimeError(f"offboard {gone_tid} -> {code}: {off}")
        t_off = time.time() + 60
        while time.time() < t_off:
            try:
                q.process_offboard_finalizes(limit=20)
                q.process_payload_blobs(limit=200)
            except Exception as exc:  # noqa: BLE001
                logger.warning("offboard finalize: %s", exc)
            if q.tenant_is_empty(gone_tid):
                break
            time.sleep(0.8)
        else:
            raise RuntimeError(f"{gone_tid} not empty after offboard")
        with q._pool.connection() as conn:
            leftover = conn.execute(
                "SELECT COUNT(*)::int AS n FROM job_payload_blobs WHERE tenant_id = %s",
                (gone_tid,),
            ).fetchone()
        if int(leftover["n"] or 0) != 0:
            raise RuntimeError(f"{gone_tid} blob catalog leftovers")

    # Survivors still list/get after offboard.
    assert_rehydrate(q, age_tid, secrets[age_tid])
    assert_rehydrate(q, keep_tid, secrets[keep_tid])
    logger.info("archive soak ok age=%s size=%s keep_live", age_n, size_n)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("archive soak failed")
        sys.exit(1)
