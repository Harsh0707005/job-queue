#!/usr/bin/env python3
"""Fleet soak: split/merge then offboard source must keep dest event history.

  COMPOSE_PROJECT_NAME=jq-evrehome docker compose -p jq-evrehome \\
    -f docker-compose.yml -f docker-compose.coordinator-leader.yml \\
    -f docker-compose.tenant-events-rehome.yml up --build -d
  docker logs -f jq-evrehome-tenant-events-rehome-soak-1
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
logger = logging.getLogger("events_rehome_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080,http://coordinator-3:8080"
    ).split(",")
    if u.strip()
]
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
JOBS = int(os.environ.get("JOBS_PER_QUEUE", "8") or "8")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-evrehome")
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


def http_json(method, path, body=None, *, timeout=30.0):
    global _rr
    start = _rr % len(COORDS)
    _rr += 1
    last = None
    for base in COORDS[start:] + COORDS[:start]:
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


def wait_healthy():
    deadline = time.time() + 180
    while time.time() < deadline:
        ready = 0
        for base in COORDS:
            try:
                code, h = http_one(base, "GET", "/health", timeout=3)
                if code == 200 and h.get("ok"):
                    ready += 1
            except Exception:
                pass
        if ready >= 2:
            return
        time.sleep(1)
    raise RuntimeError("coordinators not ready")


def unique_leader():
    rows = []
    for base in COORDS:
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


def wait_unique_leader(*, timeout=25.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        last = unique_leader()
        if last is not None:
            return last
        time.sleep(0.2)
    raise RuntimeError("no unique leader")


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
        conn.request("POST", f"/v1.41/containers/{name}/kill", headers={"Host": "localhost"})
        resp = conn.getresponse()
        if int(resp.status) in (200, 204):
            return
        resp.read()
    finally:
        conn.close()
    filt = quote(json.dumps({"name": [node]}))
    conn = _UnixHTTPConnection("/var/run/docker.sock")
    try:
        conn.request("GET", f"/v1.41/containers/json?filters={filt}", headers={"Host": "localhost"})
        resp = conn.getresponse()
        rows = json.loads(resp.read().decode() or "[]")
    finally:
        conn.close()
    if not rows:
        raise RuntimeError(f"kill {node} failed")
    cid = rows[0]["Id"]
    conn = _UnixHTTPConnection("/var/run/docker.sock")
    try:
        conn.request("POST", f"/v1.41/containers/{cid}/kill", headers={"Host": "localhost"})
        resp = conn.getresponse()
        if int(resp.status) not in (200, 204):
            raise RuntimeError(f"kill {cid} -> {resp.status}")
    finally:
        conn.close()


def follow(tid: str) -> list[dict[str, Any]]:
    out = []
    after = 0
    for _ in range(80):
        code, body = http_json("GET", f"/v1/tenants/{tid}/audit?after_seq={after}&limit=80")
        if code != 200:
            raise RuntimeError(f"events {tid} -> {code}: {body}")
        batch = body.get("events") or []
        if not batch:
            break
        out.extend(batch)
        nxt = body.get("next_seq")
        if nxt is None:
            break
        after = int(nxt)
        if len(batch) < 80:
            break
    return out


def job_audit(job_id: str, tenant_id: str) -> list[dict[str, Any]]:
    code, body = http_json(
        "GET", f"/v1/jobs/{job_id}/audit?tenant_id={tenant_id}&limit=200"
    )
    if code != 200:
        raise RuntimeError(f"job audit {job_id} -> {code}: {body}")
    return body.get("events") or []


def enqueue(tid: str, n: int, *, labels=None, depends=None) -> str:
    body = {
        "tenant_id": tid,
        "queue_name": "default",
        "payload": {
            "kind": "events",
            "secret": f"{tid}-{n}",
            "n": n,
            "sleep_ms": 30,
            "ack_result": {"echo": f"{tid}-{n}"},
        },
        "max_attempts": 4,
    }
    if labels:
        body["labels"] = list(labels)
    if depends:
        deps = list(depends) if isinstance(depends, (list, tuple)) else [depends]
        body["depends_on"] = deps
    code, out = http_json("POST", "/v1/jobs", body)
    if code not in (200, 201):
        raise RuntimeError(f"enqueue -> {code}: {out}")
    return (out.get("job") or out).get("id")


def wait_completed(tenant: str, want: int, *, deadline: float) -> None:
    while time.time() < deadline:
        code, body = http_json("GET", f"/v1/admin/stats?tenant={tenant}")
        stats = (body.get("stats") or [{}])[0] if code == 200 else {}
        done = int(stats.get("completed") or 0)
        logger.info("%s completed=%s want=%s", tenant, done, want)
        if done >= want:
            return
        time.sleep(1.0)
    raise RuntimeError(f"{tenant} did not finish {want} jobs")


def assert_dest_history(job_id: str, dst: str, *, src: str, need_completed: bool) -> None:
    ev = job_audit(job_id, dst)
    if not ev or ev[0].get("event_type") != "enqueued":
        raise RuntimeError(f"dest missing history for {job_id}: {ev[:4]}")
    if any(e.get("tenant_id") == src for e in ev):
        raise RuntimeError(f"{job_id} still stamped with source {src}")
    types = {e.get("event_type") for e in ev}
    if need_completed and "completed" not in types:
        raise RuntimeError(f"{job_id} dest history missing completed: {types}")


def main() -> int:
    wait_healthy()
    lead = wait_unique_leader()
    logger.info("leader=%s", lead.get("node"))
    src, dst, keep, mrg = "rehome-src", "rehome-dst", "rehome-keep", "rehome-mrg"
    xfer = [enqueue(src, n, labels=["xfer"]) for n in range(JOBS)]
    enqueue(src, 99, labels=["stay"])
    enqueue(keep, 1, labels=["xfer"])
    merged = [enqueue(mrg, n) for n in range(JOBS)]
    wait_completed(src, JOBS + 1, deadline=time.time() + MAX_WALL)
    wait_completed(mrg, JOBS, deadline=time.time() + MAX_WALL)

    # Pending child depending on every completed xfer job so the closed
    # split set includes the full labeled history, not just one parent.
    enqueue(src, 100, labels=["xfer"], depends=xfer)

    code, split = http_json(
        "POST",
        f"/v1/tenants/{src}/split",
        {"target_tenant_id": dst, "labels": ["xfer"]},
    )
    if code != 200:
        raise RuntimeError(f"split -> {code}: {split}")
    moved = (split.get("result") or split).get("job_ids") or split.get("job_ids") or []
    if len(moved) < JOBS + 1:
        raise RuntimeError(f"split moved {len(moved)}, want >= {JOBS + 1}: {split}")
    logger.info("split moved %s", len(moved))
    for jid in moved:
        assert_dest_history(str(jid), dst, src=src, need_completed=str(jid) in set(xfer))

    code, merged_out = http_json(
        "POST",
        f"/v1/tenants/{mrg}/merge",
        {"target_tenant_id": dst},
    )
    if code != 200:
        raise RuntimeError(f"merge -> {code}: {merged_out}")
    logger.info("merged %s jobs into dest", JOBS)
    for jid in merged:
        assert_dest_history(str(jid), dst, src=mrg, need_completed=True)

    lead = wait_unique_leader()
    dead = str(lead.get("node") or "")
    logger.info("killing leader %s mid-offboard", dead)
    code, off = http_json("POST", f"/v1/tenants/{src}/offboard", {})
    if code != 200:
        raise RuntimeError(f"offboard -> {code}: {off}")
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
        raise RuntimeError("no failover")
    logger.info("failover %s", after.get("node"))

    deadline = time.time() + 60
    while time.time() < deadline:
        code, body = http_json("GET", f"/v1/tenants/{src}/audit?after_seq=0&limit=5")
        src_left = (body.get("events") or []) if code == 200 else ["?"]
        if code == 200 and not src_left:
            break
        time.sleep(0.8)

    for jid in moved:
        assert_dest_history(str(jid), dst, src=src, need_completed=str(jid) in set(xfer))
    for jid in merged:
        assert_dest_history(str(jid), dst, src=mrg, need_completed=True)
    dest_follow = follow(dst)
    seqs = [e.get("tenant_seq") for e in dest_follow]
    if seqs != list(range(1, len(seqs) + 1)):
        raise RuntimeError(f"dest cursor gaps: {seqs}")
    dest_jobs = {e.get("job_id") for e in dest_follow}
    missing = [jid for jid in list(moved) + list(merged) if jid not in dest_jobs]
    if missing:
        raise RuntimeError(f"dest cursor missing moved jobs: {missing[:4]}")
    keep_ev = follow(keep)
    if not keep_ev:
        raise RuntimeError("unrelated tenant lost events")
    logger.info(
        "rehome soak ok dest_events=%s moved=%s merged=%s keep=%s",
        len(dest_follow),
        len(moved),
        len(merged),
        len(keep_ev),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("rehome soak failed")
        sys.exit(1)
