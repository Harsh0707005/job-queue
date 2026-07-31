"""nodes."""
from __future__ import annotations

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any
from uuid import UUID

from job_queue.codec import job_from_dict
from job_queue.models import BulkCancelResult, DrainResult, Job, SpawnChildrenResult
from job_queue.queue import ApprovalRequested, ChildrenAwaited, LeaseError


from job_queue.remote.client import CoordinatorClient
from job_queue.remote.worker_agent import RemoteWorkerAgent

logger = logging.getLogger(__name__)


class CleanerNode:
    """Retention agent: call coordinator purge-expired-jobs on an interval."""

    def __init__(
        self,
        client: CoordinatorClient,
        *,
        poll_interval_seconds: float = 15.0,
        batch_limit: int = 500,
    ) -> None:
        self.client = client
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_limit = batch_limit
        self._stop = threading.Event()
        self.deleted = 0

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> int:
        logger.info("cleaner node starting")
        while not self._stop.is_set():
            try:
                # Drain while work remains so large terminal backlogs catch up.
                while not self._stop.is_set():
                    result = self.client.purge_expired_jobs(
                        limit=self.batch_limit
                    )
                    n = int(result.get("total_deleted") or 0)
                    if n:
                        self.deleted += n
                        logger.info(
                            "purged completed=%s dead=%s total=%d",
                            result.get("completed_deleted"),
                            result.get("dead_deleted"),
                            self.deleted,
                        )
                    if n == 0:
                        break
            except Exception:
                logger.exception("purge-expired-jobs failed")
            self._stop.wait(self.poll_interval_seconds)
        return self.deleted


class ScheduleNode:
    """Polls coordinator process-due endpoint."""

    def __init__(
        self,
        client: CoordinatorClient,
        *,
        poll_interval_seconds: float = 0.5,
        batch_limit: int = 100,
    ) -> None:
        self.client = client
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_limit = batch_limit
        self._stop = threading.Event()
        self.fires = 0

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> int:
        logger.info("schedule node starting")
        while not self._stop.is_set():
            try:
                n = self.client.process_due_schedules(limit=self.batch_limit)
                if n:
                    self.fires += n
                    logger.info("fired %d schedule(s) (total=%d)", n, self.fires)
            except Exception:
                logger.exception("schedule process-due failed")
            self._stop.wait(self.poll_interval_seconds)
        return self.fires


class WebhookNode:
    """Webhook agent: claim via coordinator, POST itself, then finalize.

    Coordinators never perform outbound HTTP — only durable claim/finalize.
    """

    def __init__(
        self,
        client: CoordinatorClient,
        *,
        poll_interval_seconds: float = 0.5,
        batch_limit: int = 50,
        http_timeout_seconds: float = 10.0,
    ) -> None:
        self.client = client
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_limit = batch_limit
        self.http_timeout_seconds = http_timeout_seconds
        self._stop = threading.Event()
        self.delivered = 0

    def stop(self) -> None:
        self._stop.set()

    def run_once(self) -> int:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from job_queue.webhooks import post_webhook

        payload = self.client.claim_webhooks(
            limit=self.batch_limit,
            http_timeout_seconds=self.http_timeout_seconds,
        )
        claimed = list(payload.get("claimed") or [])
        n = 0

        def _one(item: dict[str, Any]) -> tuple[bool, bool]:
            """Return (handled, success)."""
            delivery = item["delivery"]
            did = delivery["id"]
            if not item.get("endpoint_enabled") or not item.get("url"):
                self.client.finalize_webhook(did, endpoint_missing=True)
                return True, False
            code, detail = post_webhook(
                item["url"],
                delivery.get("payload") or {},
                secret=item.get("secret"),
                timeout_seconds=self.http_timeout_seconds,
                idempotency_key=str(did),
            )
            self.client.finalize_webhook(
                did,
                status_code=code,
                error=None if 200 <= code < 300 else detail,
            )
            return True, 200 <= code < 300

        # Parallel POST+finalize within the claimed batch (I/O bound).
        # Cap concurrency: too many simultaneous finalize calls thrash the
        # coordinator DB pool and leave stuck delivering rows until reclaim.
        # Default sequential: high parallelism often thrash finalize + reclaim.
        max_par = int(os.environ.get("WEBHOOK_PARALLEL_POST", "1") or "1")
        if max_par < 1:
            max_par = 1
        workers = min(max_par, max(1, len(claimed)))
        if workers == 1 or len(claimed) <= 1:
            for item in claimed:
                handled, ok = _one(item)
                if handled:
                    n += 1
                if ok:
                    self.delivered += 1
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futs = [pool.submit(_one, item) for item in claimed]
                for fut in as_completed(futs):
                    handled, ok = fut.result()
                    if handled:
                        n += 1
                    if ok:
                        self.delivered += 1

        auto = payload.get("auto_finalized") or []
        n += len(auto)
        for r in auto:
            if r.get("success"):
                self.delivered += 1
        return n

    def run(self) -> int:
        logger.info(
            "webhook agent starting (claim+POST+finalize; coordinator does not POST)"
        )
        while not self._stop.is_set():
            try:
                n = self.run_once()
                if n:
                    logger.info(
                        "handled %d webhook claim(s) (delivered_ok=%d)",
                        n,
                        self.delivered,
                    )
            except Exception:
                logger.exception("webhook agent cycle failed")
            self._stop.wait(self.poll_interval_seconds)
        return self.delivered


def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def _coord_url() -> str:
    # COORDINATOR_URLS takes precedence (comma-separated for multi-coordinator).
    multi = _env("COORDINATOR_URLS")
    if multi:
        return multi
    return (
        _env("COORDINATOR_URL")
        or _env("JOB_QUEUE_COORDINATOR_URL")
        or "http://127.0.0.1:8080"
    )


def worker_main(argv: list[str] | None = None) -> int:
    import argparse
    import signal

    logging.basicConfig(
        level=_env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    p = argparse.ArgumentParser(prog="job-queue-worker-agent")
    p.add_argument("--coordinator", default=_coord_url())
    p.add_argument("--worker-id", default=_env("WORKER_ID"))
    p.add_argument("--tenant-id", default=_env("TENANT_ID"))  # empty => fair
    p.add_argument("--poll", type=float, default=float(_env("POLL_INTERVAL", "0.5") or "0.5"))
    p.add_argument(
        "--visibility-timeout",
        type=int,
        default=int(_env("VISIBILITY_TIMEOUT", "30") or "30"),
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=int(_env("BATCH_SIZE", "1") or "1"),
        help="Jobs to lease per pull (default 1; use >1 for batch pulls)",
    )
    p.add_argument(
        "--drain-timeout",
        type=float,
        default=float(_env("DRAIN_TIMEOUT", "60") or "60"),
        help="Seconds to finish in-flight work on SIGTERM/SIGINT before "
        "releasing remaining leases (default 60)",
    )
    args = p.parse_args(argv)
    client = CoordinatorClient(args.coordinator)
    client.wait_healthy(timeout=float(_env("READY_TIMEOUT", "120") or "120"))
    tenant = args.tenant_id or None
    if tenant == "":
        tenant = None
    agent = RemoteWorkerAgent(
        client,
        worker_id=args.worker_id,
        batch_size=args.batch_size,
        tenant_id=tenant,
        poll_interval_seconds=args.poll,
        visibility_timeout_seconds=args.visibility_timeout,
        drain_timeout_seconds=args.drain_timeout,
    )
    draining = {"done": False}

    def _on_signal(signum: int, _frame: Any) -> None:
        if draining["done"]:
            logger.warning("second signal %s: hard stop", signum)
            agent.stop()
            return
        draining["done"] = True
        logger.info(
            "signal %s: graceful drain (timeout=%.1fs)",
            signum,
            args.drain_timeout,
        )
        # Drain from a thread so we don't re-enter the signal handler stack
        # while the main loop is mid-job.
        threading.Thread(
            target=agent.drain,
            kwargs={"timeout_seconds": args.drain_timeout},
            name="drain",
            daemon=True,
        ).start()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    try:
        agent.run()
    except KeyboardInterrupt:
        if not draining["done"]:
            agent.drain(timeout_seconds=args.drain_timeout)
    return 0


def scheduler_main(argv: list[str] | None = None) -> int:
    import argparse

    logging.basicConfig(
        level=_env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    p = argparse.ArgumentParser(prog="job-queue-scheduler-node")
    p.add_argument("--coordinator", default=_coord_url())
    p.add_argument("--poll", type=float, default=float(_env("POLL_INTERVAL", "0.5") or "0.5"))
    args = p.parse_args(argv)
    client = CoordinatorClient(args.coordinator)
    client.wait_healthy(timeout=float(_env("READY_TIMEOUT", "120") or "120"))
    node = ScheduleNode(client, poll_interval_seconds=args.poll)
    try:
        node.run()
    except KeyboardInterrupt:
        node.stop()
    return 0


def cleaner_main(argv: list[str] | None = None) -> int:
    import argparse

    logging.basicConfig(
        level=_env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    p = argparse.ArgumentParser(prog="job-queue-cleaner-node")
    p.add_argument("--coordinator", default=_coord_url())
    p.add_argument(
        "--poll",
        type=float,
        default=float(_env("POLL_INTERVAL", "15") or "15"),
    )
    p.add_argument(
        "--batch-limit",
        type=int,
        default=int(_env("BATCH_LIMIT", "500") or "500"),
    )
    args = p.parse_args(argv)
    client = CoordinatorClient(args.coordinator)
    client.wait_healthy(timeout=float(_env("READY_TIMEOUT", "120") or "120"))
    node = CleanerNode(
        client,
        poll_interval_seconds=args.poll,
        batch_limit=args.batch_limit,
    )
    try:
        node.run()
    except KeyboardInterrupt:
        node.stop()
    return 0


def webhook_main(argv: list[str] | None = None) -> int:
    import argparse

    logging.basicConfig(
        level=_env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    p = argparse.ArgumentParser(prog="job-queue-webhook-node")
    p.add_argument("--coordinator", default=_coord_url())
    p.add_argument("--poll", type=float, default=float(_env("POLL_INTERVAL", "0.5") or "0.5"))
    p.add_argument(
        "--batch-limit",
        type=int,
        default=int(_env("BATCH_LIMIT", "50") or "50"),
        help="Deliveries to claim per pull",
    )
    p.add_argument(
        "--http-timeout",
        type=float,
        default=float(_env("HTTP_TIMEOUT", "10") or "10"),
    )
    args = p.parse_args(argv)
    client = CoordinatorClient(args.coordinator)
    client.wait_healthy(timeout=float(_env("READY_TIMEOUT", "120") or "120"))
    node = WebhookNode(
        client,
        poll_interval_seconds=args.poll,
        batch_limit=args.batch_limit,
        http_timeout_seconds=args.http_timeout,
    )
    try:
        node.run()
    except KeyboardInterrupt:
        node.stop()
    return 0


