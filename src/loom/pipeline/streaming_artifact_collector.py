"""Bounded, no-follow collection of Pipeline output files."""

from __future__ import annotations

import asyncio
import hashlib
import os
import stat
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from loom.pipeline.artifact_commit import MAX_APPLICATION_BUFFER_BYTES, confined_relative_path


class ArtifactCollectionError(RuntimeError):
    pass


@dataclass(frozen=True)
class CollectedFile:
    relative_path: str
    size_bytes: int
    sha256: str


class StreamingArtifactCollector:
    """Enumerate and stream regular output files without following links."""

    def __init__(self, root: Path, *, chunk_size: int = MAX_APPLICATION_BUFFER_BYTES) -> None:
        if not 1 <= chunk_size <= MAX_APPLICATION_BUFFER_BYTES:
            raise ValueError("collector chunk size must be within the 64 MiB bound")
        self._root = root
        self._chunk_size = chunk_size

    def _open(self, relative_path: str) -> int:
        relative_path = confined_relative_path(relative_path)
        root_fd = os.open(self._root, os.O_RDONLY | os.O_DIRECTORY)
        current_fd = root_fd
        try:
            parts = PurePosixPath(relative_path).parts
            for part in parts[:-1]:
                next_fd = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=current_fd,
                )
                if current_fd != root_fd:
                    os.close(current_fd)
                current_fd = next_fd
            result = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW, dir_fd=current_fd)
            info = os.fstat(result)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                os.close(result)
                raise ArtifactCollectionError("output must be a singly-linked regular file")
            return result
        except OSError as exc:
            raise ArtifactCollectionError("output path is not safely openable") from exc
        finally:
            if current_fd != root_fd:
                os.close(current_fd)
            os.close(root_fd)

    async def stream(self, relative_path: str) -> AsyncIterator[bytes]:
        fd = self._open(relative_path)
        try:
            while True:
                chunk = await asyncio.to_thread(os.read, fd, self._chunk_size)
                if not chunk:
                    break
                yield chunk
        finally:
            os.close(fd)

    async def inspect(self, relative_path: str, *, max_bytes: int) -> CollectedFile:
        digest = hashlib.sha256()
        size = 0
        async for chunk in self.stream(relative_path):
            size += len(chunk)
            if size > max_bytes:
                raise ArtifactCollectionError("output exceeds declared maximum")
            digest.update(chunk)
        return CollectedFile(
            relative_path=relative_path,
            size_bytes=size,
            sha256=f"sha256:{digest.hexdigest()}",
        )

    async def inspect_all(
        self,
        declared: dict[str, int],
    ) -> list[CollectedFile]:
        names = sorted(declared, key=lambda item: item.encode())
        if len(names) != len(set(names)):
            raise ArtifactCollectionError("output paths must be unique")
        return [await self.inspect(name, max_bytes=declared[name]) for name in names]


__all__ = ["ArtifactCollectionError", "CollectedFile", "StreamingArtifactCollector"]
