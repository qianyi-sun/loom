"""Verified credential snapshots for an immutable management admission route."""

from __future__ import annotations

import hmac
import os
import ssl
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path

from pydantic import Field, field_validator

from loom_capacity_agent.client import canonical_manager_origin, read_owner_only_bytes
from loom_capacity_manager.auth import MAX_BEARER_TOKEN_BYTES
from loom_capacity_manager.contracts import Digest, StrictV1Model


class PinnedAdmissionFileV1(StrictV1Model):
    path: str = Field(min_length=1,max_length=4096)
    sha256: Digest

    @field_validator("path")
    @classmethod
    def _canonical_path(cls,value: str) -> str:
        path = Path(value)
        if (not path.is_absolute() or str(path) != value or value == "/"
            or ".." in path.parts or "\0" in value or path.is_symlink()):
            raise ValueError("pinned admission path must be canonical and absolute")
        return value


class PinnedBuildAdmissionConnectionV1(StrictV1Model):
    origin: str
    bearer_token: PinnedAdmissionFileV1
    ca: PinnedAdmissionFileV1
    certificate: PinnedAdmissionFileV1
    private_key: PinnedAdmissionFileV1
    timeout_seconds: float = Field(default=30.0,ge=0.05,le=60)

    @field_validator("origin")
    @classmethod
    def _origin(cls,value: str) -> str:
        return canonical_manager_origin(value)


def _read_pinned(value: PinnedAdmissionFileV1, *, maximum: int) -> bytes:
    wire = read_owner_only_bytes(Path(value.path),max_bytes=maximum)
    if not hmac.compare_digest(sha256(wire).hexdigest(),value.sha256):
        raise ValueError("pinned admission credential digest changed")
    return wire


@contextmanager
def _sealed_pem(payload: bytes) -> Iterator[str]:
    """OpenSSL consumes the already-hashed snapshot, not a mutable source path."""
    # Reuse the executor's verified Linux sealing primitives, including the
    # libc fallback for Python distributions which omit os.memfd_create.
    # Resolve lazily to keep runtime assembly imports acyclic.
    from loom_capacity_executor.trusted_launcher import (
        _create_candidate_snapshot_descriptor,
        _seal_candidate_snapshot,
        _write_all,
    )

    descriptor = _create_candidate_snapshot_descriptor()
    try:
        os.fchmod(descriptor,0o600)
        _write_all(descriptor,payload)
        _seal_candidate_snapshot(descriptor)
        os.lseek(descriptor,0,os.SEEK_SET)
        yield f"/proc/self/fd/{descriptor}"
    finally:
        os.close(descriptor)


def load_pinned_admission_credentials(config: PinnedBuildAdmissionConnectionV1) -> tuple[ssl.SSLContext,str]:
    config = PinnedBuildAdmissionConnectionV1.model_validate_json(config.model_dump_json())
    token_wire = _read_pinned(config.bearer_token,maximum=MAX_BEARER_TOKEN_BYTES)
    ca_wire = _read_pinned(config.ca,maximum=1024*1024)
    certificate = _read_pinned(config.certificate,maximum=1024*1024)
    key = _read_pinned(config.private_key,maximum=1024*1024)
    try:
        token = token_wire.decode("ascii").strip()
        ca = ca_wire.decode("ascii")
    except UnicodeDecodeError:
        raise ValueError("pinned admission credentials are not ASCII") from None
    if not token or any(not 0x21 <= ord(character) <= 0x7e for character in token):
        raise ValueError("pinned admission bearer token is invalid")
    # Only the explicitly pinned CA bundle is trusted by this route.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cadata=ca)
    with _sealed_pem(certificate) as certificate_path, _sealed_pem(key) as key_path:
        context.load_cert_chain(certfile=certificate_path,keyfile=key_path)
    return context,token
