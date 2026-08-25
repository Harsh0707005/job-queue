"""Durable filesystem blob store for spilled job ciphertext."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from uuid import UUID


class BlobStoreError(Exception):
    """Raised when a payload blob cannot be read or written."""


class FilesystemBlobStore:
    """Crash-safe local blobs: write tmp, fsync, atomic replace."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, blob_id: UUID | str) -> Path:
        hid = str(blob_id).replace("-", "")
        return self.root / hid[:2] / hid[2:4] / f"{blob_id}.bin"

    def write(self, blob_id: UUID | str, data: bytes) -> str:
        if not isinstance(data, (bytes, bytearray)):
            raise BlobStoreError("blob data must be bytes")
        dest = self.path_for(blob_id)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".tmp")
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
        try:
            return dest.read_bytes()
        except FileNotFoundError as exc:
            raise BlobStoreError(f"payload blob {blob_id} is missing") from exc

    def delete(self, blob_id: UUID | str) -> None:
        dest = self.path_for(blob_id)
        try:
            dest.unlink()
        except FileNotFoundError:
            return
        tmp = dest.with_suffix(".tmp")
        try:
            tmp.unlink()
        except FileNotFoundError:
            return

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
