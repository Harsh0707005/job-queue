#!/usr/bin/env python3
"""Fleet soak: shadow-replicate one tenant, then fenced cutover.

  COMPOSE_PROJECT_NAME=jq-repl docker compose -p jq-repl \\
    -f docker-compose.yml -f docker-compose.coordinator-leader.yml \\
    -f docker-compose.tenant-replica.yml up --build -d
  docker logs -f jq-repl-tenant-replica-soak-1

Source keeps serving while dest catches up. Other tenants on both fleets
keep completing. Mid-replica leader kill must resume. Cutover leaves dest
live and source tombstoned — never both serving.
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
from urllib.parse import quote

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("tenant_replica_soak")

SRC_COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080,http://coordinator-3:8080"
    ).split(",")
    if u.strip()
]
DST_COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("DEST_COORDINATOR_URLS")
        or "http://coordinator-dest-1:8080,http://coordinator-dest-2:8080"
    ).split(",")
    if u.strip()
]
DEST_DSN = os.environ.get(
    "JOB_QUEUE_REPLICA_DEST_DSN",
    os.environ.get(
        "JOB_QUEUE_RELOCATE_DEST_DSN",
        "postgresql://queue:queue@postgres-dest:5432/queue",
    ),
)
DEST_BLOB_DIR = os.environ.get(
    "JOB_QUEUE_REPLICA_DEST_BLOB_DIR",
    os.environ.get("JOB_QUEUE_RELOCATE_DEST_BLOB_DIR", "/var/lib/job-queue/dest-blobs"),
)
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "360") or "360")
MOVE_JOBS = int(os.environ.get("MOVE_JOBS", "16") or "16")
KEEP_JOBS = int(os.environ.get("KEEP_JOBS", "10") or "10")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-repl")
MOVE = os.environ.get("MOVE_TENANT", "shadow")
KEEP_SRC = os.environ.get("KEEP_SRC_TENANT", "keep-src")
KEEP_DST = os.environ.get("KEEP_DST_TENANT", "keep-dst")
_rr = 0


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str) -> None:
        super().__init__("localhost")
        self.socket_path = socket_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.socket_path)
        self.sock = sock


def http_one(base, method, path, body=None, *, timeout=30.0):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
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


def http_json(method, path, body=None, *, bases=None, timeout=30.0):
    global _rr
    coords = list(bases or SRC_COORDS)
    start = _rr % len(coords)
    _rr += 1
    last = None
    for base in coords[start:] + coords[:start]:
        try:
            code, payload = http_one(base, method, path, body, timeout=timeout)
            if 500 <= code < 600:
                last = RuntimeError(f"{base}{path} -> {code}")
                continue
            return code, payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"all coordinators failed: {last}")


def wait_healthy(coords, *, need=2, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        ready = 0
        for base in coords:
            try:
                code, h = http_one(base, "GET", "/health", timeout=3)
                if code == 200 and h.get("ok"):
                    ready += 1
            except Exception:
                pass
        if ready >= need:
            return
        time.sleep(1)
    raise RuntimeError("coordinators not ready")


def unique_leader(coords=None):
    rows = []
    for base in coords or SRC_COORDS:
        try:
            code, h = http_one(base, "GET", "/health", timeout=3)
            if code == 200:
                rows.append(h)
        except Exception:
            continue
    leads = [r for r in rows if r.get("leader")]
    if len(leads) != 1:
        return None
    names = {r.get("leader_node") for r in rows if r.get("leader_node")}
    if names != {leads[0].get("node")}:
        return None
    return leads[0]


def docker_kill(node: str) -> None:
    name = f"{COMPOSE_PROJECT}-{node}-1"
    try:
        r = subprocess.run(
            ["docker", "kill", name], capture_output=True, text=True, check=False
        )
        if r.returncode == 0:
            return
    except FileNotFoundError:
        pass
    conn = _UnixHTTPConnection("/var/run/docker.sock")
    try:
        conn.request(
            "POST", f"/v1.41/containers/{name}/kill", headers={"Host": "localhost"}
        )
        resp = conn.getresponse()
        if int(resp.status) in (200, 204):
            return
        resp.read()
    finally:
        conn.close()
    filt = quote(json.dumps({"name": [node]}))
    conn = _UnixHTTPConnection("/var/run/docker.sock")
    try:
        conn.request(
            "GET",
            f"/v1.41/containers/json?filters={filt}",
            headers={"Host": "localhost"},
        )
        resp = conn.getresponse()
        rows = json.loads(resp.read().decode() or "[]")
    finally:
        conn.close()
    if not rows:
        raise RuntimeError(f"kill {node} failed")
    cid = rows[0]["Id"]
    conn = _UnixHTTPConnection("/var/run/docker.sock")
    try:
        conn.request(
            "POST", f"/v1.41/containers/{cid}/kill", headers={"Host": "localhost"}
        )
        resp = conn.getresponse()
        if int(resp.status) not in (200, 204):
            raise RuntimeError(f"kill {cid} -> {resp.status}")
    finally:
        conn.close()


def enqueue(tid: str, n: int, *, bases=None, prefix="j") -> list[str]:
    ids = []
    for i in range(n):
        code, body = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "queue_name": "default",
                "payload": {"k": f"{prefix}-{i}", "n": i},
            },
            bases=bases,
        )
        if code not in (200, 201):
            raise RuntimeError(f"enqueue {tid} -> {code}: {body}")
        job = body.get("job") or body
        ids.append(str(job["id"]))
    return ids


def list_jobs(tid: str, *, bases=None) -> list[dict[str, Any]]:
    code, body = http_json(
        "GET", f"/v1/jobs?tenant_id={quote(tid)}&limit=400", bases=bases
    )
    if code != 200:
        raise RuntimeError(f"list {tid} -> {code}: {body}")
    return list(body.get("jobs") or body.get("items") or [])


def count_status(jobs: list[dict[str, Any]], status: str) -> int:
    return sum(1 for j in jobs if j.get("status") == status)


def wait_completed(tid: str, need: int, *, bases=None, timeout=90.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = list_jobs(tid, bases=bases)
        n = count_status(jobs, "completed")
        if n >= need:
            return
        time.sleep(0.4)
    jobs = list_jobs(tid, bases=bases)
    raise RuntimeError(
        f"{tid} completed {count_status(jobs, 'completed')}/{need} of {len(jobs)}"
    )


def get_replica() -> dict[str, Any] | None:
    code, body = http_json("GET", f"/v1/tenants/{MOVE}/replica")
    if code == 404:
        return None
    if code != 200:
        raise RuntimeError(f"get replica -> {code}: {body}")
    return body.get("replica") or body


def advance_replica() -> dict[str, Any]:
    code, body = http_json(
        "POST",
        f"/v1/tenants/{MOVE}/replica/advance",
        {"batch_size": 80},
    )
    if code == 503:
        http_json("POST", "/v1/admin/replicas/advance", {"batch_size": 80})
        rel = get_replica()
        return rel or {}
    if code not in (200, 409):
        raise RuntimeError(f"advance replica -> {code}: {body}")
    return body.get("replica") or body


def main() -> int:
    t0 = time.time()
    wait_healthy(SRC_COORDS, need=2)
    wait_healthy(DST_COORDS, need=1)
    logger.info("fleets up src=%s dest=%s", SRC_COORDS, DST_COORDS)

    keep_src_ids = enqueue(KEEP_SRC, KEEP_JOBS, prefix="ks")
    keep_dst_ids = enqueue(KEEP_DST, KEEP_JOBS, bases=DST_COORDS, prefix="kd")
    move_ids = enqueue(MOVE, MOVE_JOBS, prefix="mv")
    logger.info(
        "seeded keep-src=%d keep-dst=%d shadow=%d",
        len(keep_src_ids),
        len(keep_dst_ids),
        len(move_ids),
    )
    wait_completed(MOVE, max(1, MOVE_JOBS // 2), timeout=80)

    code, body = http_json(
        "POST",
        f"/v1/tenants/{MOVE}/replica",
        {
            "dest_dsn": DEST_DSN,
            "dest_blob_dir": DEST_BLOB_DIR,
            "batch_size": 40,
            "min_catchup_rounds": 1,
            "run_until_complete": False,
        },
    )
    if code not in (200, 409):
        raise RuntimeError(f"start replica -> {code}: {body}")
    logger.info("replica started %s", body.get("replica") or body)

    live_after = enqueue(MOVE, 6, prefix="live")
    move_ids.extend(live_after)
    logger.info("source still serving; enqueued %d more", len(live_after))

    dest_lease_code, dest_lease = http_json(
        "POST",
        "/v1/lease",
        {"tenant_id": MOVE, "worker_id": "too-soon", "queue_name": "default"},
        bases=DST_COORDS,
    )
    if dest_lease_code == 200 and (dest_lease.get("job") or dest_lease.get("jobs")):
        raise RuntimeError(f"dest leased before cutover: {dest_lease}")

    killed = False
    cutover_sent = False
    last_stage = None
    while time.time() - t0 < MAX_WALL:
        rel = get_replica() or {}
        stage = rel.get("stage")
        status = rel.get("status")
        if stage != last_stage:
            logger.info(
                "replica status=%s stage=%s rounds=%s",
                status,
                stage,
                rel.get("catchup_rounds"),
            )
            last_stage = stage
        if status == "complete":
            break
        if (
            not cutover_sent
            and stage == "catching_up"
            and int(rel.get("catchup_rounds") or 0) >= 1
        ):
            logger.info("requesting cutover after catch-up")
            code, body = http_json(
                "POST",
                f"/v1/tenants/{MOVE}/replica/cutover",
                {"batch_size": 80},
                timeout=60,
            )
            if code in (200, 409):
                cutover_sent = True
            else:
                logger.info("cutover start -> %s %s", code, body)
        if not killed and stage in ("catching_up", "freezing", "verifying", "cutover"):
            lead = unique_leader()
            node = (lead or {}).get("node") or "coordinator-3"
            logger.info("killing leader %s mid-replica", node)
            docker_kill(node)
            killed = True
            time.sleep(2)
        try:
            advance_replica()
        except Exception as exc:  # noqa: BLE001
            logger.info("advance retry: %s", exc)
        extra = enqueue(KEEP_SRC, 1, prefix=f"ks{int(time.time())}")
        keep_src_ids.extend(extra)
        time.sleep(0.3)
    else:
        raise RuntimeError(f"replica not complete: {get_replica()}")

    dest_jobs = list_jobs(MOVE, bases=DST_COORDS)
    dest_ids = {str(j["id"]) for j in dest_jobs}
    missing = [jid for jid in move_ids if jid not in dest_ids]
    if missing:
        raise RuntimeError(f"dest missing {len(missing)} jobs e.g. {missing[:3]}")
    src_jobs = list_jobs(MOVE)
    if src_jobs:
        raise RuntimeError(f"source still has {len(src_jobs)} shadow jobs")

    wait_completed(MOVE, len(move_ids), bases=DST_COORDS, timeout=90)
    wait_completed(KEEP_SRC, len(keep_src_ids), timeout=90)
    wait_completed(KEEP_DST, len(keep_dst_ids), bases=DST_COORDS, timeout=90)

    code, body = http_json(
        "POST",
        "/v1/jobs",
        {"tenant_id": MOVE, "payload": {"late": True}, "queue_name": "default"},
    )
    if code not in (400, 409):
        raise RuntimeError(f"source enqueue after cutover should fail, got {code} {body}")
    extra_dst = enqueue(KEEP_DST, 2, bases=DST_COORDS, prefix="postd")
    extra_src = enqueue(KEEP_SRC, 2, prefix="post")
    wait_completed(KEEP_SRC, len(keep_src_ids) + 2, timeout=60)
    wait_completed(KEEP_DST, len(keep_dst_ids) + 2, bases=DST_COORDS, timeout=60)

    logger.info(
        "ok replica dest_jobs=%d keep_src=%d keep_dst=%d killed=%s wall=%.1fs",
        len(dest_jobs),
        len(keep_src_ids) + len(extra_src),
        len(keep_dst_ids) + len(extra_dst),
        killed,
        time.time() - t0,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("replica soak failed")
        sys.exit(1)
