#!/usr/bin/env python3
"""Fleet soak: live-relocate one tenant to a second Postgres.

  COMPOSE_PROJECT_NAME=jq-reloc docker compose -p jq-reloc \\
    -f docker-compose.yml -f docker-compose.coordinator-leader.yml \\
    -f docker-compose.tenant-relocate.yml up --build -d
  docker logs -f jq-reloc-tenant-relocate-soak-1

Other tenants on source and dest keep completing work. The moving tenant is
paused, copied, cut over, and source-purged. Mid-relocate leader kill must
resume without a half-moved tenant.
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
logger = logging.getLogger("tenant_relocate_soak")

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
    "JOB_QUEUE_RELOCATE_DEST_DSN",
    "postgresql://queue:queue@postgres-dest:5432/queue",
)
DEST_BLOB_DIR = os.environ.get(
    "JOB_QUEUE_RELOCATE_DEST_BLOB_DIR", "/var/lib/job-queue/dest-blobs"
)
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "300") or "300")
MOVE_JOBS = int(os.environ.get("MOVE_JOBS", "16") or "16")
KEEP_JOBS = int(os.environ.get("KEEP_JOBS", "12") or "12")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-reloc")
MOVE = os.environ.get("MOVE_TENANT", "move")
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
        "GET", f"/v1/jobs?tenant_id={quote(tid)}&limit=200", bases=bases
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
        f"{tid} completed {count_status(jobs, 'completed')}/{need} "
        f"of {len(jobs)}"
    )


def get_relocate() -> dict[str, Any] | None:
    code, body = http_json("GET", f"/v1/tenants/{MOVE}/relocate")
    if code == 404:
        return None
    if code != 200:
        raise RuntimeError(f"get relocate -> {code}: {body}")
    return body.get("relocate") or body


def advance_relocate() -> dict[str, Any]:
    code, body = http_json(
        "POST",
        f"/v1/tenants/{MOVE}/relocate/advance",
        {"batch_size": 80},
    )
    if code == 503:
        http_json("POST", "/v1/admin/relocations/advance", {"batch_size": 80})
        rel = get_relocate()
        return rel or {}
    if code not in (200, 409):
        raise RuntimeError(f"advance relocate -> {code}: {body}")
    return body.get("relocate") or body


def main() -> int:
    t0 = time.time()
    wait_healthy(SRC_COORDS, need=2)
    wait_healthy(DST_COORDS, need=1)
    logger.info("fleets up src=%s dest=%s", SRC_COORDS, DST_COORDS)

    keep_src_ids = enqueue(KEEP_SRC, KEEP_JOBS, prefix="ks")
    keep_dst_ids = enqueue(KEEP_DST, KEEP_JOBS, bases=DST_COORDS, prefix="kd")
    move_ids = enqueue(MOVE, MOVE_JOBS, prefix="mv")
    logger.info(
        "seeded keep-src=%d keep-dst=%d move=%d",
        len(keep_src_ids),
        len(keep_dst_ids),
        len(move_ids),
    )

    wait_completed(MOVE, max(1, MOVE_JOBS // 2), timeout=80)
    leftover = enqueue(MOVE, 6, prefix="pend")
    move_ids.extend(leftover)
    http_json(
        "POST",
        "/v1/schedules",
        {
            "tenant_id": MOVE,
            "name": "reloc-tick",
            "queue_name": "default",
            "interval_seconds": 30,
            "payload": {"from": "sched"},
        },
    )
    logger.info("move has leftover pending=%d", len(leftover))

    code, body = http_json(
        "POST",
        f"/v1/tenants/{MOVE}/relocate",
        {
            "dest_dsn": DEST_DSN,
            "dest_blob_dir": DEST_BLOB_DIR,
            "batch_size": 40,
            "run_until_complete": False,
        },
    )
    if code not in (200, 409):
        raise RuntimeError(f"start relocate -> {code}: {body}")
    logger.info("relocate started %s", body.get("relocate") or body)

    killed = False
    last_stage = None
    while time.time() - t0 < MAX_WALL:
        rel = get_relocate() or {}
        stage = rel.get("stage")
        status = rel.get("status")
        if stage != last_stage:
            logger.info("relocate status=%s stage=%s", status, stage)
            last_stage = stage
        if status == "complete":
            break
        if not killed and stage in ("applying", "verifying", "cutover", "purging"):
            lead = unique_leader()
            node = (lead or {}).get("node") or "coordinator-3"
            logger.info("killing leader %s mid-relocate", node)
            docker_kill(node)
            killed = True
            time.sleep(2)
        try:
            advance_relocate()
        except Exception as exc:  # noqa: BLE001
            logger.info("advance retry: %s", exc)
        extra = enqueue(KEEP_SRC, 1, prefix=f"live{int(time.time())}")
        keep_src_ids.extend(extra)
        time.sleep(0.3)
    else:
        raise RuntimeError(f"relocate not complete: {get_relocate()}")

    dest_jobs = list_jobs(MOVE, bases=DST_COORDS)
    dest_ids = {str(j["id"]) for j in dest_jobs}
    missing = [jid for jid in move_ids if jid not in dest_ids]
    if missing:
        raise RuntimeError(f"dest missing {len(missing)} moved jobs e.g. {missing[:3]}")
    src_jobs = list_jobs(MOVE)
    if src_jobs:
        raise RuntimeError(f"source still has {len(src_jobs)} move jobs")

    wait_completed(MOVE, len(move_ids), bases=DST_COORDS, timeout=90)
    wait_completed(KEEP_SRC, len(keep_src_ids), timeout=90)
    wait_completed(KEEP_DST, len(keep_dst_ids), bases=DST_COORDS, timeout=90)

    code, body = http_json("GET", f"/v1/tenants/{MOVE}/audit?after_seq=0&limit=200")
    if code != 200:
        raise RuntimeError(f"source audit after relocate -> {code}")
    src_events = body.get("events") or []
    if src_events:
        raise RuntimeError(f"source still has {len(src_events)} move events")
    code, body = http_json(
        "GET",
        f"/v1/tenants/{MOVE}/audit?after_seq=0&limit=200",
        bases=DST_COORDS,
    )
    if code != 200:
        raise RuntimeError(f"dest audit -> {code}: {body}")
    dest_events = body.get("events") or []
    types = {e.get("event_type") or e.get("type") for e in dest_events}
    if "enqueued" not in types:
        raise RuntimeError(f"dest history missing enqueue: {types}")

    code, body = http_json(
        "POST",
        "/v1/jobs",
        {"tenant_id": MOVE, "payload": {"late": True}, "queue_name": "default"},
    )
    if code not in (400, 409):
        raise RuntimeError(f"source enqueue after relocate should fail, got {code} {body}")

    extra_src = enqueue(KEEP_SRC, 2, prefix="post")
    extra_dst = enqueue(KEEP_DST, 2, bases=DST_COORDS, prefix="postd")
    wait_completed(KEEP_SRC, len(keep_src_ids) + 2, timeout=60)
    wait_completed(KEEP_DST, len(keep_dst_ids) + 2, bases=DST_COORDS, timeout=60)

    logger.info(
        "ok relocate dest_jobs=%d dest_events=%d keep_src=%d keep_dst=%d killed=%s wall=%.1fs",
        len(dest_jobs),
        len(dest_events),
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
        logger.exception("relocate soak failed")
        sys.exit(1)
