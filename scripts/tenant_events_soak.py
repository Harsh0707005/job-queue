#!/usr/bin/env python3
"""Fleet soak: sealed per-tenant event log + cursor under coordinator kill.

  COMPOSE_PROJECT_NAME=jq-events docker compose -p jq-events \\
    -f docker-compose.yml -f docker-compose.coordinator-leader.yml \\
    -f docker-compose.tenant-events.yml up --build -d
  docker logs -f jq-events-tenant-events-soak-1
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
logger = logging.getLogger("tenant_events_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080,http://coordinator-3:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
JOBS = int(os.environ.get("JOBS_PER_TENANT", "12") or "12")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-events")
_rr = 0


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str) -> None:
        super().__init__("localhost")
        self.socket_path = socket_path

    def connect(self) -> None:
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


def unique_leader() -> dict[str, Any] | None:
    rows = []
    for base in COORDS:
        try:
            code, h = http_one(base, "GET", "/health", timeout=3)
            if code == 200:
                rows.append({"base": base, **h})
        except Exception:  # noqa: BLE001
            continue
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
    raise RuntimeError(f"no unique leader: {last}")


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
    conn = _UnixHTTPConnection("/var/run/docker.sock")
    try:
        conn.request("POST", f"/v1.41/containers/{name}/kill", headers={"Host": "localhost"})
        resp = conn.getresponse()
        if int(resp.status) in (200, 204):
            return
        body = resp.read()
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
        raise RuntimeError(f"kill {node} failed ({cli_err})")
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


def tenants() -> list[str]:
    return [f"ev-{i:02d}" for i in range(TENANTS)]


def follow_all(tid: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    after = 0
    for _ in range(200):
        code, body = http_json(
            "GET", f"/v1/tenants/{tid}/audit?after_seq={after}&limit=50"
        )
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
        if len(batch) < 50:
            break
    return out


def main() -> int:
    wait_healthy()
    lead0 = wait_unique_leader()
    logger.info("leader=%s epoch=%s", lead0.get("node"), lead0.get("leader_epoch"))
    ids = tenants()
    for tid in ids:
        for n in range(JOBS):
            code, out = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {
                        "kind": "events",
                        "secret": f"{tid}-{n}",
                        "n": n,
                        "sleep_ms": 40,
                        "ack_result": {"echo": f"{tid}-{n}"},
                    },
                    "max_attempts": 4,
                },
            )
            if code not in (200, 201):
                raise RuntimeError(f"enqueue {tid} -> {code}: {out}")

    deadline = time.time() + MAX_WALL
    while time.time() < deadline:
        done = 0
        for tid in ids:
            code, body = http_json(
                "GET", f"/v1/admin/stats?tenant={tid}", timeout=10
            )
            if code != 200:
                continue
            stats = (body.get("stats") or [{}])[0]
            done += int(stats.get("completed") or 0)
        logger.info("completed=%s want=%s", done, JOBS * len(ids))
        if done >= JOBS * len(ids):
            break
        time.sleep(1.2)
    else:
        raise RuntimeError("jobs did not complete")

    before = {tid: follow_all(tid) for tid in ids}
    for tid, evs in before.items():
        seqs = [e.get("tenant_seq") for e in evs]
        if seqs != list(range(1, len(seqs) + 1)):
            raise RuntimeError(f"{tid} tenant_seq gaps: {seqs[:20]}")
        jobs = {e.get("job_id") for e in evs}
        if not jobs:
            raise RuntimeError(f"{tid} has no events")
        others = set()
        for other, oev in before.items():
            if other == tid:
                continue
            others |= {e.get("job_id") for e in oev}
        if jobs & others:
            raise RuntimeError(f"{tid} event job ids leaked")
        types = {e.get("event_type") for e in evs}
        if "enqueued" not in types or "completed" not in types:
            raise RuntimeError(f"{tid} missing transitions: {types}")

    lead = wait_unique_leader()
    dead = str(lead.get("node") or "")
    logger.info("killing leader %s mid-follow", dead)
    docker_kill(dead)
    t1 = time.time()
    after_lead = None
    while time.time() - t1 < 25:
        after_lead = unique_leader()
        if after_lead is not None and after_lead.get("node") != dead:
            break
        after_lead = None
        time.sleep(0.25)
    if after_lead is None:
        raise RuntimeError(f"no failover after killing {dead}")
    logger.info("failover leader=%s", after_lead.get("node"))

    extra_tid = ids[0]
    code, out = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": extra_tid,
            "payload": {
                "kind": "events",
                "secret": f"{extra_tid}-post",
                "n": JOBS,
                "sleep_ms": 40,
                "ack_result": {"echo": f"{extra_tid}-post"},
            },
        },
    )
    if code not in (200, 201):
        raise RuntimeError(f"post-kill enqueue -> {code}: {out}")

    drain_deadline = time.time() + 60
    while time.time() < drain_deadline:
        evs = follow_all(extra_tid)
        if any(
            e.get("event_type") == "completed"
            and (e.get("detail") or {}).get("job", {}).get("payload", {}).get("n")
            == JOBS
            for e in evs
        ) or any(e.get("event_type") == "completed" for e in evs[-5:]):
            if len(evs) > len(before[extra_tid]):
                break
        time.sleep(1.0)
    else:
        logger.warning("post-kill extra job may still be draining; checking cursor")

    after = follow_all(extra_tid)
    seqs = [e.get("tenant_seq") for e in after]
    if seqs != list(range(1, len(seqs) + 1)):
        raise RuntimeError(f"post-kill {extra_tid} gaps: {seqs}")
    if len(after) < len(before[extra_tid]):
        raise RuntimeError("event history shrank after coordinator kill")
    logger.info(
        "tenant events soak ok tenants=%s events=%s",
        len(ids),
        {t: len(follow_all(t)) for t in ids},
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("tenant events soak failed")
        sys.exit(1)
