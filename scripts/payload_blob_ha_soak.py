#!/usr/bin/env python3
"""Fleet soak: shared blob store across a multi-coordinator HA fleet.

Stages
------
1. Unique leader + standbys. Enqueue a large spill on coordinator A and
   lease plaintext from coordinator B (workers never see this queue).
2. Multi-tenant load via round-robin enqueue. List stays redacted; PG
   never holds secrets.
3. Live DEK rotate + export while workers drain default-queue jobs.
4. Kill the leader mid-flight. A standby takes over; workers keep
   completing; leftover pointers stay openable.
5. Restore the mid-flight snapshot onto a dest tenant and lease it from
   a surviving coordinator.

  COMPOSE_PROJECT_NAME=jq-pblob-ha docker compose -p jq-pblob-ha \\
    -f docker-compose.yml -f docker-compose.coordinator-leader.yml \\
    -f docker-compose.payload-blob-ha.yml up --build -d
  docker logs -f jq-pblob-ha-payload-blob-ha-soak-1
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
logger = logging.getLogger("payload_blob_ha_soak")

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
JOBS = int(os.environ.get("JOBS_PER_TENANT", "12") or "12")
PAYLOAD_BYTES = int(os.environ.get("PAYLOAD_BYTES", "49152") or "49152")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "300") or "300")
SECRET_PREFIX = os.environ.get("SECRET_PREFIX", "pblob-ha")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL") or ""
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-pblob-ha")
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
    # Project image has no docker CLI; talk to the engine over the socket.
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


def postgres_secret_hits(secrets: list[str]) -> list[str]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT payload::text AS t FROM jobs
            UNION ALL
            SELECT COALESCE(result::text, '') FROM jobs
            UNION ALL
            SELECT payload::text FROM webhook_deliveries
            """
        ).fetchall()
        blob = "\n".join(r["t"] or "" for r in rows)
    return [s for s in secrets if s in blob]


def assert_pointers_openable(q, tenant_id: str) -> None:
    from job_queue.crypto import is_blob_pointer

    with q._pool.connection() as conn:
        rows = conn.execute(
            """
            SELECT id, payload, result, status, last_error
            FROM jobs WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchall()
        dels = conn.execute(
            """
            SELECT id, payload, status FROM webhook_deliveries
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchall()
    for row in rows:
        for col in ("payload", "result"):
            env = row.get(col)
            if not is_blob_pointer(env):
                continue
            q._assert_envelope_readable(env)
            got = q.get(row["id"], tenant_id=tenant_id)
            if got is None:
                raise RuntimeError(f"{tenant_id} job {row['id']} vanished")
        if row.get("status") == "dead" and row.get("last_error"):
            err = str(row["last_error"]).lower()
            if "missing" in err or "blob" in err:
                raise RuntimeError(
                    f"{tenant_id} job {row['id']} dead from blob: {row['last_error']}"
                )
    for row in dels:
        if is_blob_pointer(row.get("payload")):
            q._assert_envelope_readable(row["payload"])


def tenant_ids() -> list[str]:
    return [f"pha-{i:02d}" for i in range(TENANTS)]


def enqueue_large(
    *,
    tenant_id: str,
    secret: str,
    pad: str,
    n: int,
    base: str | None = None,
    queue_name: str = "default",
) -> dict[str, Any]:
    body = {
        "tenant_id": tenant_id,
        "queue_name": queue_name,
        "payload": {
            "kind": "blob-ha",
            "secret": secret,
            "blob": pad,
            "n": n,
            "ack_result": {"echo": secret, "blob": pad},
        },
        "max_attempts": 2,
    }
    if base:
        code, out = http_one(base, "POST", "/v1/jobs", body)
    else:
        code, out = http_json("POST", "/v1/jobs", body)
    if code not in (200, 201):
        raise RuntimeError(f"enqueue {tenant_id} -> {code}: {out}")
    return out.get("job") or out


def main() -> int:
    logger.info(
        "blob ha soak coords=%d tenants=%d jobs=%d bytes=%d",
        len(COORDS),
        TENANTS,
        JOBS,
        PAYLOAD_BYTES,
    )
    wait_healthy()
    lead = wait_unique_leader()
    logger.info("stage1 leader=%s epoch=%s", lead.get("node"), lead.get("leader_epoch"))
    standbys = [r for r in poll_leaders() if r.get("ok") and not r.get("leader")]
    if len(standbys) < 2:
        raise RuntimeError(f"expected >=2 standbys, got {standbys}")

    q = connect_queue()
    pad = "H" * PAYLOAD_BYTES
    secrets: list[str] = []
    expected = 0
    try:
        # Stage 1: spill on one coordinator, hydrate on another.
        writer = standbys[0]["base"]
        reader = standbys[1]["base"] if len(standbys) > 1 else COORDS[0]
        if reader == writer:
            reader = next(b for b in COORDS if b != writer)
        cross_secret = f"{SECRET_PREFIX}-cross"
        secrets.append(cross_secret)
        job = enqueue_large(
            tenant_id="pha-cross",
            secret=cross_secret,
            pad=pad,
            n=0,
            base=writer,
            queue_name="cross-hydrate",
        )
        job_id = str((job.get("id") if isinstance(job, dict) else "") or "")
        listed = http_one(
            reader,
            "GET",
            "/v1/jobs?" + urlencode({"tenant_id": "pha-cross", "limit": "8"}),
        )
        if listed[0] != 200:
            raise RuntimeError(f"cross list -> {listed}")
        for row in listed[1].get("jobs") or []:
            payload = row.get("payload") or {}
            if payload.get("secret"):
                raise RuntimeError(f"list leaked plaintext: {payload}")
            if payload != {"_encrypted": True}:
                raise RuntimeError(f"list not redacted: {payload}")
        # Coordinators require a registered worker. Reuse a fleet worker
        # (they only auto-lease the default queue) so we do not rebalance.
        got = None
        lease_wid = None
        last_lease: tuple[int, dict[str, Any]] | None = None
        for wid in ("worker-1", "worker-2", "worker-3", "worker-4"):
            last_lease = http_one(
                reader,
                "POST",
                "/v1/lease",
                {
                    "worker_id": wid,
                    "tenant_id": "pha-cross",
                    "queue_name": "cross-hydrate",
                },
            )
            if last_lease[0] != 200:
                continue
            cand = last_lease[1].get("job") or (
                last_lease[1].get("jobs") or [None]
            )[0]
            if cand:
                got = cand
                lease_wid = wid
                break
        if not got or not lease_wid:
            raise RuntimeError(f"cross lease on {reader} -> {last_lease}")
        if job_id and str(got.get("id")) != job_id:
            raise RuntimeError(f"cross lease id mismatch {got.get('id')} != {job_id}")
        if (got.get("payload") or {}).get("secret") != cross_secret:
            raise RuntimeError(f"cross lease did not hydrate: {got.get('payload')}")
        token = got.get("lease_token")
        if not token:
            raise RuntimeError(f"cross lease missing token: {got}")
        code, acked = http_one(
            reader,
            "POST",
            f"/v1/jobs/{got['id']}/ack",
            {
                "tenant_id": "pha-cross",
                "worker_id": lease_wid,
                "lease_token": token,
                "result": {"echo": cross_secret},
            },
        )
        if code != 200:
            raise RuntimeError(f"cross ack -> {code}: {acked}")
        logger.info(
            "stage1 ok: spill on %s hydrated on %s as %s",
            writer,
            reader,
            lease_wid,
        )

        if WEBHOOK_URL:
            from job_queue import WebhookEvent

            for tid in tenant_ids():
                q.register_webhook(
                    tenant_id=tid,
                    url=WEBHOOK_URL,
                    events=[WebhookEvent.JOB_COMPLETED.value],
                    secret="pha-hook",
                )

        # Stage 2: multi-tenant RR load on the default queue.
        for i, tid in enumerate(tenant_ids()):
            for n in range(JOBS):
                secret = f"{SECRET_PREFIX}-{tid}-{n}"
                secrets.append(secret)
                enqueue_large(tenant_id=tid, secret=secret, pad=pad, n=n)
                expected += 1

        leaked = postgres_secret_hits(secrets)
        if leaked:
            raise RuntimeError(f"plaintext before rotate: {leaked[:3]}")
        logger.info("stage2 enqueued %d large jobs", expected)

        # Let workers take some leases so rotate overlaps inflight work.
        time.sleep(1.5)

        for tid in tenant_ids():
            code, rot = http_json(
                "POST", f"/v1/tenants/{tid}/payload-keys/rotate", {}
            )
            status = (rot.get("rotation") or {}).get("status")
            logger.info("rotate %s -> %s %s", tid, code, status)
            if status != "reencrypting":
                raise RuntimeError(f"rotate {tid} unexpected {rot}")

        snap = q.export_tenant(tenant_ids()[0])
        if snap.status != "ready":
            raise RuntimeError(f"mid-rotate export failed: {snap}")
        logger.info(
            "exported %s mid-rotate blobs=%s",
            tenant_ids()[0],
            snap.counts.get("payload_blobs"),
        )

        leaked = postgres_secret_hits(secrets)
        if leaked:
            raise RuntimeError(f"plaintext after rotate start: {leaked[:3]}")

        # Stage 3: kill the leader while rotate + workers are in flight.
        before = wait_unique_leader()
        dead = str(before.get("node") or "")
        epoch0 = int(before.get("leader_epoch") or 0)
        logger.info("killing leader %s epoch=%s", dead, epoch0)
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
            raise RuntimeError(
                f"no failover leader after killing {dead}: {poll_leaders()}"
            )
        logger.info(
            "stage3 leader=%s epoch=%s (was %s/%s)",
            after.get("node"),
            after.get("leader_epoch"),
            dead,
            epoch0,
        )

        deadline = time.time() + MAX_WALL
        while time.time() < deadline:
            try:
                http_json(
                    "POST",
                    "/v1/admin/payload-keys/advance",
                    {"limit": TENANTS, "batch_size": 40},
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("advance: %s", exc)
            bl = http_json("GET", "/v1/admin/backlog")[1].get("jobs") or {}
            completed = int(bl.get("completed") or 0)
            pending = int(bl.get("pending") or 0)
            leased_n = int(bl.get("leased") or 0)
            done_rots = 0
            for tid in tenant_ids():
                try:
                    info = http_json("GET", f"/v1/tenants/{tid}/payload-keys")[1]
                except Exception:  # noqa: BLE001
                    continue
                if (info.get("rotation") or {}).get("status") in (None, "complete"):
                    done_rots += 1
            logger.info(
                "progress completed=%d pending=%d leased=%d rots=%d/%d expected=%d",
                completed,
                pending,
                leased_n,
                done_rots,
                TENANTS,
                expected,
            )
            if (
                completed >= expected
                and pending == 0
                and leased_n == 0
                and done_rots == TENANTS
            ):
                break
            time.sleep(1.5)
        else:
            raise RuntimeError("drain/rotate did not finish after failover")

        for tid in tenant_ids():
            info = http_json("GET", f"/v1/tenants/{tid}/payload-keys")[1]
            rot = info.get("rotation") or {}
            if rot.get("status") not in (None, "complete"):
                raise RuntimeError(f"{tid} rotation stuck: {rot}")
            decrypt_only = [
                k for k in (info.get("keys") or []) if k.get("status") == "decrypt_only"
            ]
            if decrypt_only:
                raise RuntimeError(f"{tid} leftover decrypt_only: {decrypt_only}")
            assert_pointers_openable(q, tid)
            q.process_payload_blobs(limit=200)
            assert_pointers_openable(q, tid)

        leaked = postgres_secret_hits(secrets)
        if leaked:
            raise RuntimeError(f"plaintext after complete: {leaked[:3]}")

        rst = q.restore_tenant(
            "pha-restore",
            snapshot_id=snap.id,
            batch_size=40,
            run_until_complete=True,
        )
        if rst.status != "complete":
            raise RuntimeError(f"restore after failover failed: {rst.status}")
        assert_pointers_openable(q, "pha-restore")

        survivor = after.get("base") or next(
            (b for b in COORDS if dead not in b), COORDS[0]
        )
        code, dest_lease = http_one(
            survivor,
            "POST",
            "/v1/lease",
            {"worker_id": "restore-peer", "tenant_id": "pha-restore"},
        )
        dest_job = None
        if code == 200:
            dest_job = dest_lease.get("job") or (dest_lease.get("jobs") or [None])[0]
        if dest_job and str(
            (dest_job.get("payload") or {}).get("secret") or ""
        ).startswith(SECRET_PREFIX):
            logger.info("restore leased plaintext from %s", survivor)
        else:
            # Snapshot may have only completed rows; decrypt locally from dest.
            sample = q.list_jobs(tenant_id="pha-restore", limit=1)
            if not sample or not str(
                sample[0].payload.get("secret") or ""
            ).startswith(SECRET_PREFIX):
                raise RuntimeError(
                    f"restored job did not decrypt on {survivor}: {code} {dest_lease}"
                )

        if WEBHOOK_URL:
            q.process_webhook_deliveries(limit=80)

        logger.info(
            "payload blob ha soak OK leader=%s epoch=%s completed=%d",
            after.get("node"),
            after.get("leader_epoch"),
            expected,
        )
    finally:
        q.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("payload blob ha soak failed")
        sys.exit(1)
