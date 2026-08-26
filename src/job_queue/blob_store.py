"""Shared filesystem blob store for spilled job ciphertext."""

from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path
from uuid import UUID, uuid4


class BlobStoreError(Exception):
    """Raised when a payload blob cannot be read or written."""


class FilesystemBlobStore:
    """Crash-safe shared blobs: unique tmp, fsync, atomic replace.

    All coordinators in a fleet must point ``JOB_QUEUE_BLOB_DIR`` at the
    same durable directory (a shared volume). Reads retry briefly so a
    lease on another node can see a spill that just committed.
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        read_retries: int = 8,
        read_retry_seconds: float = 0.025,
    ) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.read_retries = max(1, int(read_retries))
        self.read_retry_seconds = max(0.0, float(read_retry_seconds))

    def path_for(self, blob_id: UUID | str) -> Path:
        hid = str(blob_id).replace("-", "")
        return self.root / hid[:2] / hid[2:4] / f"{blob_id}.bin"

    def write(self, blob_id: UUID | str, data: bytes) -> str:
        if not isinstance(data, (bytes, bytearray)):
            raise BlobStoreError("blob data must be bytes")
        dest = self.path_for(blob_id)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(f".{dest.stem}.{os.getpid()}.{uuid4().hex[:8]}.tmp")
        fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            view = memoryview(data)
            offset = 0
            while offset < len(view):
                offset += os.write(fd, view[offset:])
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(str(tmp), str(dest))
        dir_fd = os.open(str(dest.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        return hashlib.sha256(data).hexdigest()

    def read(self, blob_id: UUID | str) -> bytes:
        dest = self.path_for(blob_id)
        last: Exception | None = None
        for attempt in range(self.read_retries):
            try:
                return dest.read_bytes()
            except FileNotFoundError as exc:
                last = exc
                if attempt + 1 >= self.read_retries:
                    break
                if self.read_retry_seconds:
                    time.sleep(self.read_retry_seconds)
        raise BlobStoreError(f"payload blob {blob_id} is missing") from last

    def delete(self, blob_id: UUID | str) -> None:
        dest = self.path_for(blob_id)
        try:
            dest.unlink()
        except FileNotFoundError:
            return
        parent = dest.parent
        if parent.is_dir():
            for leftover in parent.glob(f".{dest.stem}.*.tmp"):
                try:
                    leftover.unlink()
                except FileNotFoundError:
                    continue

    def exists(self, blob_id: UUID | str) -> bool:
        return self.path_for(blob_id).is_file()

    def iter_entries(self) -> list[tuple[UUID, float]]:
        """Return ``(blob_id, mtime)`` for every durable ``.bin`` under root."""
        out: list[tuple[UUID, float]] = []
        if not self.root.is_dir():
            return out
        for path in self.root.rglob("*.bin"):
            try:
                bid = UUID(path.stem)
            except ValueError:
                continue
            try:
                mtime = path.stat().st_mtime
            except FileNotFoundError:
                continue
            out.append((bid, mtime))
        return out
