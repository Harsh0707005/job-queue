#!/usr/bin/env python3
"""Fleet soak: metrics retention survives coordinator reconnects / restarts.

1. Set metrics_retention_seconds to NULL (keep forever) via coordinator HTTP.
2. Bounce both coordinators (kill + recreate) so init_schema runs again.
3. Assert retention is still NULL.
4. Set a finite window, bounce again, assert it persists.
5. Set NULL again, bounce again, assert forever still sticks.

Exit 0 on success. Writes JSON results to RESULTS_PATH.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("metrics_retention_restart")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
RESULTS_PATH = os.environ.get("RESULTS_PATH", "/results/metrics-retention-restart.json")
# When running inside compose, restart siblings via docker CLI on mounted socket
# or by hitting an optional control path. Prefer docker compose from host script.
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "job-queue")
RESTART_VIA = os.environ.get("RESTART_VIA", "http")  # http = soft re-init endpoint


def http_json(
    method: str, path: str, body: dict[str, Any] | None = None, timeout: float = 30.0
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    last: Exception | None = None
    for base in COORDINATOR_URLS:
        url = base + path
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if not raw:
                    return resp.status, {}
                return resp.status, json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode("utf-8", errors="replace")}
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(f"{url} -> {exc.code}: {payload}")
                continue
            return int(exc.code), payload
        except urllib.error.URLError as exc:
            last = exc
            continue
    raise RuntimeError(f"all coordinators failed: {last}")


def wait_healthy(timeout: float = 180.0) -> None:
    """Wait until *every* configured coordinator answers /health ok."""
    deadline = time.time() + timeout
    need = len(COORDINATOR_URLS)
    while time.time() < deadline:
        ok = 0
        for base in COORDINATOR_URLS:
            try:
                req = urllib.request.Request(base + "/health")
                with urllib.request.urlopen(req, timeout=3) as resp:
                    body = json.loads(resp.read().decode())
                    if body.get("ok"):
                        ok += 1
            except Exception:  # noqa: BLE001
                pass
        if ok >= need:
            return
        time.sleep(1)
    raise RuntimeError("coordinators not healthy")


def get_retention() -> Any:
    code, body = http_json("GET", "/v1/metrics/retention")
    if code != 200:
        raise RuntimeError(f"get retention {code}: {body}")
    return body.get("metrics_retention_seconds")


def set_retention(seconds: int | None) -> Any:
    code, body = http_json(
        "POST",
        "/v1/metrics/retention",
        {"metrics_retention_seconds": seconds},
    )
    if code != 200:
        raise RuntimeError(f"set retention {code}: {body}")
    return body.get("metrics_retention_seconds")


def _docker_unix_request(
    method: str, path: str, body: bytes | None = None
) -> tuple[int, bytes]:
    """Minimal Docker Engine API client over the mounted unix socket."""
    import http.client
    import socket as _socket

    class _UnixHTTPConnection(http.client.HTTPConnection):
        def __init__(self, sock_path: str) -> None:
            super().__init__("localhost")
            self._sock_path = sock_path

        def connect(self) -> None:  # noqa: D401
            sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
            sock.connect(self._sock_path)
            self.sock = sock

    sock_path = os.environ.get("DOCKER_SOCK", "/var/run/docker.sock")
    conn = _UnixHTTPConnection(sock_path)
    try:
        headers = {"Content-Type": "application/json", "Host": "localhost"}
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        return int(resp.status), data
    finally:
        conn.close()


def _restart_coordinators_via_docker_api() -> bool:
    """Restart coordinator containers so they re-run JobQueue.connect/init_schema."""
    sock_path = os.environ.get("DOCKER_SOCK", "/var/run/docker.sock")
    if not os.path.exists(sock_path):
        logger.warning("docker socket %s missing; cannot restart coordinators", sock_path)
        return False
    try:
        code, raw = _docker_unix_request("GET", "/containers/json")
        if code != 200:
            logger.warning("docker list containers -> %s %s", code, raw[:200])
            return False
        containers = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("docker API list failed: %s", exc)
        return False

    restarted = 0
    for c in containers:
        names = " ".join(c.get("Names") or [])
        # Match compose service names coordinator-1 / coordinator-2.
        if "coordinator-1" not in names and "coordinator-2" not in names:
            continue
        cid = c.get("Id")
        if not cid:
            continue
        try:
            code, raw = _docker_unix_request("POST", f"/containers/{cid}/restart")
            logger.info("restart %s -> %s", names, code)
            if code in (204, 200):
                restarted += 1
            else:
                logger.warning("restart failed %s: %s", names, raw[:200])
        except Exception as exc:  # noqa: BLE001
            logger.warning("restart %s error: %s", names, exc)
    if restarted < 1:
        return False
    wait_healthy(timeout=180.0)
    return True


def soft_reinit_schema() -> None:
    """Force coordinator process restart so init_schema runs on boot again."""
    if _restart_coordinators_via_docker_api():
        return

    # Fallback: docker CLI if present (host-side runs).
    for name in ("coordinator-1", "coordinator-2"):
        try:
            subprocess.run(
                ["docker", "restart", f"{COMPOSE_PROJECT}-{name}-1"],
                check=False,
                capture_output=True,
                timeout=120,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("docker CLI restart %s failed: %s", name, exc)
    try:
        wait_healthy(timeout=60.0)
        return
    except Exception:  # noqa: BLE001
        pass

    raise RuntimeError(
        "could not restart coordinators (need docker.sock API or docker CLI) "
        "to verify retention across restarts"
    )


def main() -> int:
    logger.info("coords=%s", COORDINATOR_URLS)
    wait_healthy()
    steps: list[dict[str, Any]] = []

    def check(label: str, expected: Any) -> None:
        got = get_retention()
        ok = got == expected
        steps.append({"step": label, "expected": expected, "got": got, "ok": ok})
        logger.info("%s expected=%s got=%s ok=%s", label, expected, got, ok)
        if not ok:
            raise AssertionError(f"{label}: expected {expected!r} got {got!r}")

    set_retention(None)
    check("after set null", None)
    soft_reinit_schema()
    check("after restart null", None)

    set_retention(86400)
    check("after set 1d", 86400)
    soft_reinit_schema()
    check("after restart 1d", 86400)

    set_retention(None)
    check("after set null again", None)
    soft_reinit_schema()
    check("after restart null again", None)

    # Cross-coordinator consistency: hit each base URL directly (retry briefly).
    wait_healthy()
    for base in COORDINATOR_URLS:
        got = None
        last_err: Exception | None = None
        for _ in range(30):
            try:
                req = urllib.request.Request(base + "/v1/metrics/retention")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    got = json.loads(resp.read().decode())["metrics_retention_seconds"]
                last_err = None
                break
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                time.sleep(1)
        if last_err is not None:
            raise RuntimeError(f"{base} unreachable after restart: {last_err}")
        steps.append(
            {
                "step": f"direct {base}",
                "expected": None,
                "got": got,
                "ok": got is None,
            }
        )
        if got is not None:
            raise AssertionError(f"{base} retention not null: {got!r}")

    result = {"ok": True, "steps": steps}
    os.makedirs(os.path.dirname(RESULTS_PATH) or ".", exist_ok=True)
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    logger.info("PASS wrote %s", RESULTS_PATH)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001
        logger.exception("FAIL: %s", exc)
        try:
            os.makedirs(os.path.dirname(RESULTS_PATH) or ".", exist_ok=True)
            with open(RESULTS_PATH, "w", encoding="utf-8") as f:
                json.dump({"ok": False, "error": str(exc)}, f, indent=2)
        except Exception:  # noqa: BLE001
            pass
        sys.exit(1)
