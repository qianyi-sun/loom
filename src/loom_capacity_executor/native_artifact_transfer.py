"""Bounded credential-free artifact pipe; transport facts are not publication.

The mapped caller must finish runtime cleanup and verify the artifact before
export. The outer receiver owns its socket and gets only a scoped transport spool.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import socket
import stat
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from loom_capacity_agent.build_admission import BuildArtifactV1
from loom_capacity_executor.native_authority_bridge import _readable
from loom_capacity_executor.native_build_source import _settled_io, _write_all
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes

_MAGIC = b"LOOMNAT1"
_MAX_HEADER = 4096
_CHUNK = 1024 * 1024


class NativeArtifactTransferV1(StrictV1Model):
    claim_digest: Digest
    source_binding_sha256: Digest
    artifact: BuildArtifactV1


@dataclass(frozen=True, slots=True)
class NativeReceivedArtifact:
    """Exact transferred bytes; management OCI verification is still required."""

    archive: Path
    artifact: BuildArtifactV1


def _configure(channel: socket.socket, claim_digest: str, source_binding_sha256: str,
    max_artifact_bytes: int, timeout_seconds: int,
) -> None:
    if (type(max_artifact_bytes) is not int or max_artifact_bytes <= 0
        or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 1800):
        raise ValueError("native artifact transfer limits are invalid")
    NativeArtifactTransferV1(claim_digest=claim_digest, source_binding_sha256=source_binding_sha256,
        artifact=BuildArtifactV1(archive_sha256="0" * 64, archive_size_bytes=1))
    if channel.family != socket.AF_UNIX or channel.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_STREAM:
        raise ValueError("native artifact transfer requires a local byte stream")
    channel.setblocking(False)
    channel.set_inheritable(False)


async def _recv(channel: socket.socket, count: int) -> bytes:
    while True:
        try:
            chunk, ancillary, flags, _address = channel.recvmsg(count, 0, socket.MSG_CMSG_CLOEXEC)
        except BlockingIOError:
            await _readable(channel)
            continue
        if ancillary or flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
            raise ValueError("native artifact stream carried unexpected descriptors")
        return chunk


async def _exact(channel: socket.socket, count: int) -> bytes:
    value = bytearray()
    while len(value) < count:
        chunk = await _recv(channel, count - len(value))
        if not chunk:
            raise ValueError("native artifact stream header is truncated")
        value.extend(chunk)
    return bytes(value)


async def send_native_artifact(channel: socket.socket, *, archive: Path,
    claim_digest: str, source_binding_sha256: str, max_artifact_bytes: int,
    timeout_seconds: int = 1800,
) -> BuildArtifactV1:
    """Send one already-verified, stopped-runtime artifact; never a path RPC."""
    _configure(channel, claim_digest, source_binding_sha256, max_artifact_bytes, timeout_seconds)
    descriptor = os.open(archive, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or not 1 <= metadata.st_size <= max_artifact_bytes:
            raise ValueError("native artifact export is not a bounded regular archive")
        async with asyncio.timeout(timeout_seconds):
            digest = hashlib.sha256()
            size = 0
            while chunk := await _settled_io(os.read, descriptor, _CHUNK):
                size += len(chunk)
                if size > metadata.st_size:
                    raise ValueError("native artifact changed while preparing export")
                digest.update(chunk)
            if size != metadata.st_size:
                raise ValueError("native artifact changed while preparing export")
            artifact = BuildArtifactV1(archive_sha256=digest.hexdigest(), archive_size_bytes=size)
            header = canonical_bytes(NativeArtifactTransferV1(claim_digest=claim_digest,
                source_binding_sha256=source_binding_sha256, artifact=artifact))
            if len(header) > _MAX_HEADER:
                raise ValueError("native artifact transfer header exceeds bound")
            os.lseek(descriptor, 0, os.SEEK_SET)
            loop = asyncio.get_running_loop()
            await loop.sock_sendall(channel, _MAGIC + len(header).to_bytes(4, "big") + header)
            sent = 0
            observed = hashlib.sha256()
            while chunk := await _settled_io(os.read, descriptor, _CHUNK):
                sent += len(chunk)
                if sent > size:
                    raise ValueError("native artifact changed during export")
                observed.update(chunk)
                await loop.sock_sendall(channel, chunk)
            if sent != size or observed.hexdigest() != artifact.archive_sha256:
                raise ValueError("native artifact changed during export")
            channel.shutdown(socket.SHUT_WR)
            return artifact
    finally:
        os.close(descriptor)


@asynccontextmanager
async def receive_native_artifact(channel: socket.socket, *, workspace: Path,
    claim_digest: str, source_binding_sha256: str, max_artifact_bytes: int,
    timeout_seconds: int = 1800,
) -> AsyncIterator[NativeReceivedArtifact]:
    """Validate the entire byte stream before yielding a private scoped spool.

    The total timeout covers reception and settled writes, not caller upload.
    Cancellation settles any in-flight write before removing its workspace.
    Socket closure and exact child settlement remain the outer caller's duties.
    """
    _configure(channel, claim_digest, source_binding_sha256, max_artifact_bytes, timeout_seconds)
    if not workspace.is_absolute() or workspace == Path("/") or ".." in workspace.parts:
        raise ValueError("native artifact workspace must be absolute and private")
    directory = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(directory)
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
            raise ValueError("native artifact workspace must be private and owner-controlled")
        with tempfile.TemporaryDirectory(prefix="native-artifact-", dir=f"/proc/self/fd/{directory}") as temporary:
            path = Path(temporary) / "artifact.tar"
            async with asyncio.timeout(timeout_seconds):
                prefix = await _exact(channel, 12)
                count = int.from_bytes(prefix[8:], "big")
                if prefix[:8] != _MAGIC or not 1 <= count <= _MAX_HEADER:
                    raise ValueError("native artifact stream header is invalid")
                wire = await _exact(channel, count)
                envelope = NativeArtifactTransferV1.model_validate_json(wire)
                if (canonical_bytes(envelope) != wire or envelope.claim_digest != claim_digest
                    or envelope.source_binding_sha256 != source_binding_sha256
                    or envelope.artifact.archive_size_bytes > max_artifact_bytes):
                    raise ValueError("native artifact stream identity or size changed")
                descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
                try:
                    size = 0
                    digest = hashlib.sha256()
                    while chunk := await _recv(channel, min(_CHUNK, envelope.artifact.archive_size_bytes - size + 1)):
                        size += len(chunk)
                        if size > envelope.artifact.archive_size_bytes:
                            raise ValueError("native artifact stream exceeds declared size")
                        digest.update(chunk)
                        await _settled_io(_write_all, descriptor, chunk)
                    if size != envelope.artifact.archive_size_bytes or digest.hexdigest() != envelope.artifact.archive_sha256:
                        raise ValueError("native artifact stream content changed")
                    await _settled_io(os.fsync, descriptor)
                    os.fchmod(descriptor, 0o400)
                finally:
                    os.close(descriptor)
            yield NativeReceivedArtifact(path, envelope.artifact)
    finally:
        os.close(directory)
