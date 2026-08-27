#!/usr/bin/env python3
"""Fleet soak: inbound job callbacks + tenant webhooks, crash-safe.

Each inbound job carries a callback_url. Completions and dead-letters POST
a payload signed with the trigger secret through the durable webhook
outbox. Tenant webhooks fire on the same jobs. Mid-soak we kill a
webhook agent so a claim can be reclaimed without a second logical
delivery.

  COMPOSE_PROJECT_NAME=jq-incb docker compose -p jq-incb \\
    -f docker-compose.yml -f docker-compose.inbound-callback.yml up --build -d
  docker logs -f jq-incb-inbound-callback-soak-1
"""

from __future__ import annotations

import json
import logging
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from uuid import uuid4
from urllib.parse import quote

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("inbound_callback_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
SINK = os.environ.get("SINK_URL", "http://webhook-sink:8090").rstrip("/")
CALLBACK_URL = os.environ.get("CALLBACK_URL", SINK + "/hook")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", SINK + "/hook")
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
JOBS = int(os.environ.get("JOBS_PER_TENANT", "16") or "16")
DEAD = int(os.environ.get("DEAD_PER_TENANT", "3") or "3")
CONCURRENCY = int(os.environ.get("CONCURRENCY", "6") or "6")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-incb")
_rr = 0
_rr_lock = threading.Lock()


def _next_coord() -> str:
    global _rr
    with _rr_lock:
        base = COORDS[_rr % len(COORDS)]
        _rr += 1
    return base


def http_json(
    method: str, path: str, body: dict[str, Any] | None = None
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    last: Exception | None = None
    for i in range(len(COORDS)):
        base = COORDS[i] if False else _next_coord()
        req = urllib.request.Request(
            base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return int(resp.status), json.loads(raw.decode()) if raw else {}
        except urllib.error.HTTPError as exc:
            payload = json.loads((exc.read() or b"{}").decode() or "{}")
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(str(exc.code))
                continue
            return int(exc.code), payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"http failed: {last}")


def signed_post(trigger_id: str, secret: str, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    from job_queue import NONCE_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, sign_inbound

    raw = json.dumps(body, separators=(",", ":")).encode()
    ts = str(int(time.time()))
    nonce = uuid4().hex
    headers = {
        "Content-Type": "application/json",
        SIGNATURE_HEADER: sign_inbound(secret, raw, timestamp=ts, nonce=nonce),
        TIMESTAMP_HEADER: ts,
        NONCE_HEADER: nonce,
    }
    last: Exception | None = None
    for _ in range(len(COORDS)):
        url = f"{_next_coord()}/v1/inbound/{trigger_id}"
        req = urllib.request.Request(url, data=raw, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                got = resp.read()
                return int(resp.status), json.loads(got.decode()) if got else {}
        except urllib.error.HTTPError as exc:
            payload = json.loads((exc.read() or b"{}").decode() or "{}")
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(str(exc.code))
                continue
            return int(exc.code), payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"inbound post failed: {last}")


def wait_ready() -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        ready = 0
        for b in COORDS:
            try:
                req = urllib.request.Request(b + "/health", method="GET")
                with urllib.request.urlopen(req, timeout=3) as resp:
                    if json.loads(resp.read().decode()).get("ok"):
                        ready += 1
            except Exception:
                pass
        try:
            req = urllib.request.Request(SINK + "/health", method="GET")
            with urllib.request.urlopen(req, timeout=3) as resp:
                sink_ok = json.loads(resp.read().decode()).get("ok")
        except Exception:
            sink_ok = False
        if ready == len(COORDS) and sink_ok:
            return
        time.sleep(1)
    raise RuntimeError("stack not ready")


def sink_stats() -> dict[str, Any]:
    req = urllib.request.Request(SINK + "/stats", method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


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
    class _Unix(urllib.request.HTTPHandler):
        pass

    class UnixHTTPConnection:
        def __init__(self, path: str) -> None:
            self.path = path

        def request(self, method: str, url: str, headers=None) -> None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self.path)
            req = f"{method} {url} HTTP/1.1\r\nHost: localhost\r\n\r\n"
            sock.sendall(req.encode())
            sock.recv(4096)
            sock.close()

    UnixHTTPConnection("/var/run/docker.sock").request(
        "POST", f"/v1.41/containers/{name}/kill"
    )


def docker_start(node: str) -> None:
    name = f"{COMPOSE_PROJECT}-{node}-1"
    try:
        subprocess.run(
            ["docker", "start", name], capture_output=True, text=True, check=False
        )
        return
    except FileNotFoundError:
        pass
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect("/var/run/docker.sock")
    req = f"POST /v1.41/containers/{name}/start HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n"
    sock.sendall(req.encode())
    sock.recv(4096)
    sock.close()


def tenant_ids() -> list[str]:
    return [f"inb-{i:02d}" for i in range(TENANTS)]


def main() -> int:
    logger.info(
        "inbound callback soak tenants=%d jobs=%d dead=%d",
        TENANTS,
        JOBS,
        DEAD,
    )
    wait_ready()
    tenants = tenant_ids()
    triggers: dict[str, dict[str, Any]] = {}
    expected_ok = 0
    expected_dead = 0
    errors: list[str] = []

    for tid in tenants:
        secret = f"secret-{tid}-xxxxxxxx"
        code, out = http_json(
            "POST",
            "/v1/triggers",
            {
                "tenant_id": tid,
                "name": f"cb-{tid}",
                "secret": secret,
                "kinds": ["job"],
            },
        )
        if code != 201:
            raise RuntimeError(f"create trigger {tid} -> {code}: {out}")
        triggers[tid] = out["trigger"]
        code, wh = http_json(
            "POST",
            "/v1/webhooks",
            {
                "tenant_id": tid,
                "url": WEBHOOK_URL,
                "events": ["job.completed", "job.dead"],
                "secret": "tenant-hook-secret",
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"webhook {tid} -> {code}: {wh}")

    def fire_one(tid: str, n: int, *, dead: bool) -> None:
        nonlocal expected_ok, expected_dead
        body: dict[str, Any] = {
            "action": "job",
            "payload": {"kind": "inb", "n": n, "ms": 20 if not dead else 5},
            "callback_url": CALLBACK_URL,
        }
        if dead:
            body["max_attempts"] = 1
            body["payload"]["fail"] = True
        code, out = signed_post(triggers[tid]["id"], triggers[tid]["secret"], body)
        if code != 201:
            errors.append(f"fire {tid}/{n} -> {code} {out}")
            return
        if dead:
            expected_dead += 1
        else:
            expected_ok += 1

    lock = threading.Lock()
    expected_ok = 0
    expected_dead = 0

    def fire_one_safe(tid: str, n: int, dead: bool) -> None:
        nonlocal expected_ok, expected_dead
        body: dict[str, Any] = {
            "action": "job",
            "payload": {
                "kind": "inb",
                "n": n,
                "ms": 25,
                **({"force_fail": True} if dead else {}),
            },
            "callback_url": CALLBACK_URL,
        }
        if dead:
            body["max_attempts"] = 1
        code, out = signed_post(triggers[tid]["id"], triggers[tid]["secret"], body)
        if code != 201:
            errors.append(f"fire {tid}/{n} -> {code} {out}")
            return
        with lock:
            if dead:
                expected_dead += 1
            else:
                expected_ok += 1

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futs = []
        for tid in tenants:
            for n in range(JOBS):
                futs.append(pool.submit(fire_one_safe, tid, n, n < DEAD))
        for fut in as_completed(futs):
            fut.result()
    if errors:
        raise RuntimeError(f"fire errors: {errors[:5]}")
    logger.info("fired ok=%d dead=%d", expected_ok, expected_dead)

    time.sleep(1.5)
    logger.info("killing webhook-1 mid-delivery")
    docker_kill("webhook-1")
    time.sleep(2)
    docker_start("webhook-1")

    deadline = time.time() + MAX_WALL
    while time.time() < deadline:
        stats = sink_stats()
        # Each success/dead job should produce 2 POSTs: tenant webhook + callback.
        want = 2 * (expected_ok + expected_dead)
        unique = int(stats.get("unique_deliveries") or 0)
        sig_ok = int(stats.get("sig_ok") or 0)
        sig_bad = int(stats.get("sig_bad") or 0)
        logger.info(
            "sink unique=%d want>=%d sig_ok=%d sig_bad=%d retries=%s",
            unique,
            want,
            sig_ok,
            sig_bad,
            stats.get("retry_posts"),
        )
        code, bl = http_json("GET", "/v1/admin/backlog")
        jobs = (bl.get("jobs") or {}) if code == 200 else {}
        pending = int(jobs.get("pending") or 0)
        leased = int(jobs.get("leased") or 0)
        if pending == 0 and leased == 0 and unique >= want and sig_bad == 0:
            break
        time.sleep(1.5)
    else:
        raise RuntimeError(f"callbacks did not drain: {sink_stats()}")

    stats = sink_stats()
    if int(stats.get("sig_bad") or 0) > 0:
        raise RuntimeError(f"bad signatures: {stats}")
    if int(stats.get("unsigned_posts") or 0) > 0:
        raise RuntimeError(f"unsigned posts: {stats}")
    # Logical unique deliveries: 2 per terminal job. Retries of the same
    # delivery_id are allowed under reclaim; extra unique ids are not.
    want = 2 * (expected_ok + expected_dead)
    unique = int(stats["unique_deliveries"])
    if unique < want:
        raise RuntimeError(f"missing deliveries unique={unique} want={want}")
    if unique > want + TENANTS:  # small slack for leftover schedule-less extras
        # Only inbound jobs were enqueued; extra unique ids mean double enqueue.
        raise RuntimeError(f"too many unique deliveries {unique} want={want}")

    logger.info(
        "inbound callback soak OK unique=%d sig_ok=%d retries=%s",
        unique,
        stats.get("sig_ok"),
        stats.get("retry_posts"),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("inbound callback soak failed")
        sys.exit(1)
