#!/usr/bin/env python3
"""Fleet soak: offboard spilled payloads on a shared 3-coordinator store.

Stages
------
1. Unique leader. Enqueue large jobs + webhooks on several tenants.
2. Offboard two tenants that still have leased work / delivering hooks.
3. Kill the leader and a worker mid-offboard.
4. Offboarded tenants drain empty: no jobs, schedules, webhooks, catalog
   rows, or files on the shared volume.
5. Survivor tenants keep completing; their blobs stay on the volume.

  COMPOSE_PROJECT_NAME=jq-oblob-ha docker compose -p jq-oblob-ha \\
    -f docker-compose.yml -f docker-compose.coordinator-leader.yml \\
    -f docker-compose.offboard-blob-ha.yml up --build -d
  docker logs -f jq-oblob-ha-offboard-blob-ha-soak-1
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
from uuid import UUID

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("offboard_blob_ha_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080,http://coordinator-3:8080"
    ).split(",")
    if u.strip()
]
DSN = os.environ.get("JOB_QUEUE_DSN") or ""
BLOB_DIR = os.environ.get("JOB_QUEUE_BLOB_DIR") or ""
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
JOBS = int(os.environ.get("JOBS_PER_TENANT", "10") or "10")
PAYLOAD_BYTES = int(os.environ.get("PAYLOAD_BYTES", "49152") or "49152")
SLEEP_MS = int(os.environ.get("JOB_SLEEP_MS", "2500") or "2500")
OFFBOARD_COUNT = int(os.environ.get("OFFBOARD_COUNT", "2") or "2")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL") or ""
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-oblob-ha")
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
        if ready == len(COORDS):
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


def tenant_ids() -> list[str]:
    return [f"obh-{i:02d}" for i in range(TENANTS)]


def residue(tid: str) -> dict[str, Any]:
    code, body = http_json(
        "GET", "/v1/admin/tenant-residue?" + urlencode({"tenant": tid})
    )
    if code != 200:
        raise RuntimeError(f"residue {tid} -> {code}: {body}")
    rows = body.get("tenants") or []
    return rows[0] if rows else {"tenant_id": tid, "empty": True}


def catalog_ids(q, tenants: list[str]) -> dict[str, list[UUID]]:
    out: dict[str, list[UUID]] = {t: [] for t in tenants}
    with q._pool.connection() as conn:
        rows = conn.execute(
            """
            SELECT tenant_id, id
            FROM job_payload_blobs
            WHERE tenant_id = ANY(%s)
            """,
            (tenants,),
        ).fetchall()
    for row in rows:
        out.setdefault(row["tenant_id"], []).append(row["id"])
    return out


def files_exist(q, blob_ids: list[UUID]) -> list[UUID]:
    store = q._blob_store
    return [bid for bid in blob_ids if store.exists(bid)]


def main() -> int:
    logger.info(
        "offboard blob ha soak coords=%d tenants=%d jobs=%d bytes=%d",
        len(COORDS),
        TENANTS,
        JOBS,
        PAYLOAD_BYTES,
    )
    wait_healthy()
    lead = wait_unique_leader()
    logger.info("stage1 leader=%s epoch=%s", lead.get("node"), lead.get("leader_epoch"))

    q = connect_queue()
    pad = "O" * PAYLOAD_BYTES
    tenants = tenant_ids()
    gone = tenants[:OFFBOARD_COUNT]
    keep = tenants[OFFBOARD_COUNT:]
    if not gone or not keep:
        raise RuntimeError("need both offboard and survivor tenants")
    try:
        if WEBHOOK_URL:
            from job_queue import WebhookEvent

            for tid in tenants:
                q.register_webhook(
                    tenant_id=tid,
                    url=WEBHOOK_URL,
                    events=[WebhookEvent.JOB_COMPLETED.value],
                    secret=f"hook-{tid}",
                )
            for tid in tenants:
                http_json(
                    "POST",
                    "/v1/schedules",
                    {
                        "tenant_id": tid,
                        "name": f"obh-{tid}",
                        "interval_seconds": 8,
                        "payload": {
                            "kind": "offboard-blob",
                            "secret": f"sched-{tid}",
                            "blob": pad,
                            "sleep_ms": SLEEP_MS,
                        },
                    },
                )

        for tid in tenants:
            for n in range(JOBS):
                code, out = http_json(
                    "POST",
                    "/v1/jobs",
                    {
                        "tenant_id": tid,
                        "payload": {
                            "kind": "offboard-blob",
                            "secret": f"{tid}-{n}",
                            "blob": pad,
                            "sleep_ms": SLEEP_MS,
                            "ack_result": {"echo": f"{tid}-{n}", "blob": pad},
                        },
                        "max_attempts": 2,
                        "visibility_timeout_seconds": 45,
                    },
                )
                if code not in (200, 201):
                    raise RuntimeError(f"enqueue {tid} -> {code}: {out}")

        # Let workers take leases and some webhooks start POSTing.
        time.sleep(1.6)
        before = catalog_ids(q, tenants)
        for tid in tenants:
            if not before.get(tid):
                raise RuntimeError(f"{tid} has no spilled blobs before offboard")
        logger.info(
            "stage1 blobs=%s",
            {t: len(before[t]) for t in tenants},
        )

        offboarded: dict[str, dict[str, Any]] = {}
        for tid in gone:
            code, body = http_json("POST", f"/v1/tenants/{tid}/offboard", {})
            if code != 200:
                raise RuntimeError(f"offboard {tid} -> {code}: {body}")
            offboarded[tid] = body
            logger.info(
                "offboard %s empty=%s leased=%s delivering=%s blobs_del=%s remain=%s",
                tid,
                body.get("empty"),
                body.get("leased_remaining"),
                body.get("webhooks_delivering_remaining"),
                (body.get("result") or {}).get("blobs_deleted"),
                (body.get("result") or {}).get("blobs_remaining"),
            )
            code, again = http_json(
                "POST",
                "/v1/jobs",
                {"tenant_id": tid, "payload": {"n": 1}},
            )
            if code in (200, 201):
                raise RuntimeError(f"enqueue after offboard succeeded for {tid}")

        # Stage 3: kill leader + a worker while drain is in flight.
        before_lead = wait_unique_leader()
        dead = str(before_lead.get("node") or "")
        epoch0 = int(before_lead.get("leader_epoch") or 0)
        logger.info("killing leader %s and worker-2 mid-offboard", dead)
        docker_kill(dead)
        docker_kill("worker-2")

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
            "stage3 leader=%s epoch=%s (was %s/%s)",
            after.get("node"),
            after.get("leader_epoch"),
            dead,
            epoch0,
        )

        # Survivors keep taking new work after failover.
        for i, tid in enumerate(keep):
            code, out = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {
                        "kind": "survivor",
                        "secret": f"{tid}-post-{i}",
                        "blob": pad,
                        "sleep_ms": 200,
                    },
                },
            )
            if code not in (200, 201):
                raise RuntimeError(f"survivor enqueue {tid} -> {code}: {out}")

        deadline = time.time() + MAX_WALL
        while time.time() < deadline:
            try:
                q.process_offboard_finalizes(limit=20)
                q.process_payload_blobs(limit=200)
            except Exception as exc:  # noqa: BLE001
                logger.warning("finalize/gc: %s", exc)
            leftover = []
            for tid in gone:
                row = residue(tid)
                if not row.get("empty"):
                    leftover.append(row)
            if not leftover:
                break
            logger.info("offboard drain: %s", leftover)
            time.sleep(1.2)
        else:
            raise RuntimeError(f"offboarded tenants not empty: {leftover}")

        for tid in gone:
            if not q.tenant_is_empty(tid):
                raise RuntimeError(f"{tid} JobQueue.tenant_is_empty is false")
            if _blob_ids := catalog_ids(q, [tid])[tid]:
                raise RuntimeError(f"{tid} catalog leftovers: {_blob_ids}")
            lingering = files_exist(q, before[tid])
            if lingering:
                raise RuntimeError(f"{tid} files remain on volume: {lingering[:5]}")

        keep_files = 0
        for tid in keep:
            row = residue(tid)
            if row.get("offboarding"):
                raise RuntimeError(f"survivor {tid} marked offboarding")
            ids = catalog_ids(q, [tid])[tid]
            keep_files += len(files_exist(q, ids or before.get(tid) or []))
            code, jobs = http_json(
                "GET",
                "/v1/jobs?" + urlencode({"tenant_id": tid, "limit": "50"}),
            )
            if code != 200:
                raise RuntimeError(f"list {tid} -> {code}")
            for j in jobs.get("jobs") or []:
                if j.get("tenant_id") != tid:
                    raise RuntimeError(f"tenant leak {tid}: {j}")
        if keep_files < 1:
            raise RuntimeError("survivor tenants lost every spilled blob file")

        logger.info(
            "offboard blob ha soak OK leader=%s emptied=%s survivors=%s files=%d",
            after.get("node"),
            gone,
            keep,
            keep_files,
        )
    finally:
        q.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("offboard blob ha soak failed")
        sys.exit(1)
