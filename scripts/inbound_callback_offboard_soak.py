#!/usr/bin/env python3
"""Fleet soak: inbound callbacks must drain while the tenant is offboarding.

Offboard pauses the tenant and waits for pending inbound callbacks. Those
rows must still be claimable (tenant webhooks stay blocked). Mid-soak we
kill a webhook agent so a claimed callback is reclaimed, not dead-lettered.

  COMPOSE_PROJECT_NAME=jq-cbob docker compose -p jq-cbob \\
    -f docker-compose.yml -f docker-compose.inbound-callback-offboard.yml \\
    up --build -d
  docker logs -f jq-cbob-inbound-callback-offboard-soak-1
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
from urllib.parse import urlencode
from uuid import uuid4

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("inbound_callback_offboard_soak")

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
JOBS = int(os.environ.get("JOBS_PER_TENANT", "12") or "12")
DEAD = int(os.environ.get("DEAD_PER_TENANT", "2") or "2")
OFFBOARD_COUNT = int(os.environ.get("OFFBOARD_COUNT", "2") or "2")
SLEEP_MS = int(os.environ.get("JOB_SLEEP_MS", "1800") or "1800")
CONCURRENCY = int(os.environ.get("CONCURRENCY", "6") or "6")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-cbob")
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
    for _ in range(len(COORDS)):
        req = urllib.request.Request(
            _next_coord() + path, data=data, headers=headers, method=method
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


def residue(tid: str) -> dict[str, Any]:
    code, body = http_json(
        "GET", "/v1/admin/tenant-residue?" + urlencode({"tenant": tid})
    )
    if code != 200:
        raise RuntimeError(f"residue {tid} -> {code}: {body}")
    rows = body.get("tenants") or []
    if not rows:
        return {"tenant_id": tid, "empty": True}
    return rows[0]


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
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect("/var/run/docker.sock")
    req = f"POST /v1.41/containers/{name}/kill HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n"
    sock.sendall(req.encode())
    sock.recv(4096)
    sock.close()


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
    return [f"cob-{i:02d}" for i in range(TENANTS)]


def main() -> int:
    logger.info(
        "callback offboard soak tenants=%d jobs=%d dead=%d offboard=%d",
        TENANTS,
        JOBS,
        DEAD,
        OFFBOARD_COUNT,
    )
    wait_ready()
    tenants = tenant_ids()
    gone = tenants[:OFFBOARD_COUNT]
    keep = tenants[OFFBOARD_COUNT:]
    triggers: dict[str, dict[str, Any]] = {}
    fired: dict[str, int] = {tid: 0 for tid in tenants}
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

    lock = threading.Lock()

    def fire_one(tid: str, n: int, dead: bool) -> None:
        body: dict[str, Any] = {
            "action": "job",
            "payload": {
                "kind": "cob",
                "n": n,
                "sleep_ms": 40 if dead else SLEEP_MS,
                **({"force_fail": True} if dead else {}),
            },
            "callback_url": CALLBACK_URL,
            "visibility_timeout_seconds": 45,
        }
        if dead:
            body["max_attempts"] = 1
        code, out = signed_post(triggers[tid]["id"], triggers[tid]["secret"], body)
        if code != 201:
            errors.append(f"fire {tid}/{n} -> {code} {out}")
            return
        with lock:
            fired[tid] += 1

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futs = []
        for tid in tenants:
            for n in range(JOBS):
                futs.append(pool.submit(fire_one, tid, n, n < DEAD))
        for fut in as_completed(futs):
            fut.result()
    if errors:
        raise RuntimeError(f"fire errors: {errors[:5]}")
    logger.info("fired %s", fired)

    # Wait until every tenant has no pending jobs so offboard cancels nothing.
    deadline = time.time() + 90
    while time.time() < deadline:
        leftover = 0
        for tid in tenants:
            row = residue(tid)
            leftover += int((row.get("jobs") or {}).get("pending") or 0)
        if leftover == 0:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("jobs never left pending")

    pre = {tid: residue(tid) for tid in gone}
    logger.info("pre-offboard residue: %s", json.dumps(pre, default=str))
    for tid in gone:
        code, out = http_json("POST", f"/v1/tenants/{tid}/offboard", {})
        if code != 200:
            raise RuntimeError(f"offboard {tid} -> {code}: {out}")
        result = out.get("result") or out
        logger.info(
            "offboarded %s empty=%s remaining_wh=%s leased=%s",
            tid,
            result.get("empty"),
            result.get("webhooks_delivering_remaining"),
            result.get("leased_remaining"),
        )
        if result.get("empty"):
            continue
        if not result.get("offboarding"):
            raise RuntimeError(f"{tid} neither empty nor offboarding: {out}")

    logger.info("killing webhook-1 mid-callback")
    docker_kill("webhook-1")
    time.sleep(2)
    docker_start("webhook-1")

    deadline = time.time() + MAX_WALL
    while time.time() < deadline:
        gone_rows = {tid: residue(tid) for tid in gone}
        keep_rows = {tid: residue(tid) for tid in keep}
        stats = sink_stats()
        by_tenant = stats.get("by_tenant") or {}
        gone_empty = all(r.get("empty") for r in gone_rows.values())
        keep_pending = sum(
            int((r.get("jobs") or {}).get("pending") or 0) for r in keep_rows.values()
        )
        keep_leased = sum(
            int((r.get("jobs") or {}).get("leased") or 0) for r in keep_rows.values()
        )
        keep_wh = sum(
            int((r.get("webhook_deliveries") or {}).get("pending") or 0)
            + int((r.get("webhook_deliveries") or {}).get("delivering") or 0)
            for r in keep_rows.values()
        )
        logger.info(
            "gone_empty=%s keep_pending=%d keep_leased=%d keep_wh=%d "
            "sink_unique=%s sig_ok=%s by_tenant=%s",
            gone_empty,
            keep_pending,
            keep_leased,
            keep_wh,
            stats.get("unique_deliveries"),
            stats.get("sig_ok"),
            by_tenant,
        )
        if (
            gone_empty
            and keep_pending == 0
            and keep_leased == 0
            and keep_wh == 0
            and int(stats.get("sig_bad") or 0) == 0
        ):
            break
        time.sleep(1.5)
    else:
        raise RuntimeError(
            f"did not drain: gone={[residue(t) for t in gone]} "
            f"keep={[residue(t) for t in keep]} sink={sink_stats()}"
        )

    stats = sink_stats()
    if int(stats.get("sig_bad") or 0) > 0:
        raise RuntimeError(f"bad signatures: {stats}")
    if int(stats.get("unsigned_posts") or 0) > 0:
        raise RuntimeError(f"unsigned posts: {stats}")

    by_tenant = stats.get("by_tenant") or {}
    for tid in gone:
        got = int(by_tenant.get(tid) or 0)
        want = fired[tid]
        # Offboard purges unclaimed tenant webhooks; callbacks must still land.
        if got < want:
            raise RuntimeError(f"{tid} callbacks missing got={got} want>={want}")
    for tid in keep:
        got = int(by_tenant.get(tid) or 0)
        want = 2 * fired[tid]
        if got < want:
            raise RuntimeError(f"{tid} missing webhook+callback got={got} want>={want}")

    # Survivors still accept inbound fires after the others are gone.
    survivor = keep[0]
    code, extra = signed_post(
        triggers[survivor]["id"],
        triggers[survivor]["secret"],
        {
            "action": "job",
            "payload": {"kind": "cob", "n": 99, "sleep_ms": 20},
            "callback_url": CALLBACK_URL,
        },
    )
    if code != 201:
        raise RuntimeError(f"survivor fire after offboard -> {code}: {extra}")
    extra_deadline = time.time() + 60
    while time.time() < extra_deadline:
        row = residue(survivor)
        jobs = row.get("jobs") or {}
        deliveries = row.get("webhook_deliveries") or {}
        if (
            int(jobs.get("pending") or 0) == 0
            and int(jobs.get("leased") or 0) == 0
            and int(deliveries.get("pending") or 0) == 0
            and int(deliveries.get("delivering") or 0) == 0
        ):
            break
        time.sleep(1)
    else:
        raise RuntimeError(f"survivor extra job stuck: {residue(survivor)}")

    for tid in gone:
        if not residue(tid).get("empty"):
            raise RuntimeError(f"{tid} reappeared after extra fire")

    logger.info(
        "callback offboard soak OK unique=%s sig_ok=%s retries=%s by_tenant=%s",
        stats.get("unique_deliveries"),
        stats.get("sig_ok"),
        stats.get("retry_posts"),
        by_tenant,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("callback offboard soak failed")
        sys.exit(1)
