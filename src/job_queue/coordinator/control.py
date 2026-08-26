"""Background loop: hold the control-plane leader lease and run ticks."""

from __future__ import annotations

import logging
import os
import threading
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from job_queue.coordinator.app import Coordinator

logger = logging.getLogger(__name__)


class CoordinatorControlLoop:
    """Renew the leader lease; only the holder runs control ticks.

    Followers keep serving worker HTTP (lease/ack/enqueue). When the
    holder dies the lease expires and another coordinator steals it.
    """

    def __init__(
        self,
        coordinator: Coordinator,
        *,
        ttl_seconds: float = 3.0,
        interval_seconds: float = 0.5,
        schedule_limit: int = 100,
        webhook_limit: int = 25,
        retention_limit: int = 200,
        webhook_timeout_seconds: float = 5.0,
        before_tick=None,
    ) -> None:
        self.coordinator = coordinator
        self.queue = coordinator.queue
        self.node_id = coordinator.node_id
        self.lease_token = uuid4()
        self.ttl_seconds = float(ttl_seconds)
        self.interval_seconds = float(interval_seconds)
        self.schedule_limit = int(schedule_limit)
        self.webhook_limit = int(webhook_limit)
        self.retention_limit = int(retention_limit)
        self.webhook_timeout_seconds = float(webhook_timeout_seconds)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._holding = False
        self._epoch: int | None = None
        # Test hook: called after acquire, before the fenced tick.
        self.before_tick = before_tick

    def is_leader(self) -> bool:
        with self._lock:
            if not self._holding:
                return False
        return bool(
            self.queue.holds_control_leader(
                node_id=self.node_id,
                lease_token=self.lease_token,
            )
        )

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name=f"coord-control-{self.node_id}",
            daemon=True,
        )
        if hasattr(self.queue, "touch_control_node"):
            try:
                self.queue.touch_control_node(self.node_id)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "control node heartbeat failed node=%s", self.node_id
                )
        self._thread.start()
        logger.info(
            "control loop starting node=%s ttl=%.2fs interval=%.2fs",
            self.node_id,
            self.ttl_seconds,
            self.interval_seconds,
        )

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5)
        try:
            self.queue.release_control_leader(
                node_id=self.node_id,
                lease_token=self.lease_token,
            )
        except Exception:  # noqa: BLE001
            logger.exception("release control leader failed node=%s", self.node_id)
        with self._lock:
            self._holding = False

    def _run(self) -> None:
        # Let sibling coordinators finish schema bootstrap before we take
        # row locks on jobs / coordinator_leader.
        self._stop.wait(min(0.4, self.interval_seconds))
        while not self._stop.is_set():
            try:
                if hasattr(self.queue, "touch_control_node"):
                    self.queue.touch_control_node(self.node_id)
            except Exception:  # noqa: BLE001
                logger.exception("control node heartbeat failed node=%s", self.node_id)
            try:
                lease = self.queue.acquire_or_renew_control_leader(
                    node_id=self.node_id,
                    lease_token=self.lease_token,
                    ttl_seconds=self.ttl_seconds,
                )
            except Exception:  # noqa: BLE001
                logger.exception("control lease acquire failed node=%s", self.node_id)
                lease = None
            with self._lock:
                was = self._holding
                self._holding = lease is not None
                self._epoch = None if lease is None else int(lease.epoch)
            if lease is not None and not was:
                logger.info(
                    "became control leader node=%s epoch=%s",
                    self.node_id,
                    lease.epoch,
                )
            elif lease is None and was:
                logger.info("lost control leader node=%s", self.node_id)
            if lease is not None and not self._stop.is_set():
                hook = self.before_tick
                if hook is not None:
                    try:
                        hook(self, lease)
                    except Exception:  # noqa: BLE001
                        logger.exception("before_tick hook failed node=%s", self.node_id)
                if not self._stop.is_set():
                    self._tick(epoch=int(lease.epoch))
            self._stop.wait(self.interval_seconds)

    def _tick(self, *, epoch: int) -> None:
        token = self.lease_token
        fence = {"leader_token": token, "leader_epoch": epoch}
        try:
            fires = self.queue.process_due_schedules(
                limit=self.schedule_limit, **fence
            )
            self.queue.bump_control_tick(
                lease_token=token, kind="schedules", count=1
            )
            if fires:
                logger.info("control fired %d schedule(s)", len(fires))
        except Exception:  # noqa: BLE001
            logger.exception("control schedule tick failed node=%s", self.node_id)
        if self._stop.is_set() or not self.is_leader():
            return
        try:
            self.queue.process_webhook_deliveries(
                limit=self.webhook_limit,
                http_timeout_seconds=self.webhook_timeout_seconds,
                **fence,
            )
            self.queue.bump_control_tick(
                lease_token=token, kind="webhooks", count=1
            )
        except Exception:  # noqa: BLE001
            logger.exception("control webhook tick failed node=%s", self.node_id)
        if self._stop.is_set() or not self.is_leader():
            return
        try:
            self.queue.evaluate_alerts(**fence)
            self.queue.bump_control_tick(
                lease_token=token, kind="alerts", count=1
            )
        except Exception:  # noqa: BLE001
            logger.exception("control alert tick failed node=%s", self.node_id)
        if self._stop.is_set() or not self.is_leader():
            return
        try:
            self.queue.purge_expired_jobs(limit=self.retention_limit, **fence)
            if hasattr(self.queue, "process_offboard_finalizes"):
                self.queue.process_offboard_finalizes(limit=50)
            if hasattr(self.queue, "process_inbound_nonces"):
                self.queue.process_inbound_nonces(limit=500)
            if hasattr(self.queue, "process_payload_blobs"):
                self.queue.process_payload_blobs(limit=self.retention_limit)
            self.queue.bump_control_tick(
                lease_token=token,
                kind="retention",
                count=1,
            )
        except Exception:  # noqa: BLE001
            logger.exception("control retention tick failed node=%s", self.node_id)
        if self._stop.is_set() or not self.is_leader():
            return
        try:
            if hasattr(self.queue, "process_payload_key_rotations"):
                self.queue.process_payload_key_rotations(
                    limit=20, batch_size=min(self.retention_limit, 200)
                )
        except Exception:  # noqa: BLE001
            logger.exception("control key-rotation tick failed node=%s", self.node_id)
        if self._stop.is_set() or not self.is_leader():
            return
        try:
            if hasattr(self.queue, "process_tenant_restores"):
                self.queue.process_tenant_restores(
                    limit=20, batch_size=min(self.retention_limit, 200)
                )
        except Exception:  # noqa: BLE001
            logger.exception("control restore tick failed node=%s", self.node_id)


def _env_flag(name: str, default: str = "1") -> bool:
    raw = os.environ.get(name, default)
    return str(raw or default).strip().lower() not in ("0", "false", "no", "off")
