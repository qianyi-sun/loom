"""Strict node-bearer authentication for the task-image authority service."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from loom_task_image_authority.contracts import (
    GuardScope,
    TaskImageGuardPrincipalV1,
)

_MAX_REGISTRY_BYTES = 1024 * 1024
MAX_BEARER_TOKEN_BYTES = 4096
_BEARER = re.compile(rf"Bearer ([^\s]{{1,{MAX_BEARER_TOKEN_BYTES}}})", re.ASCII)
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class TaskImagePrincipalRegistryError(ValueError):
    """Raised when the on-disk node-principal registry is unsafe or invalid."""


class TaskImageAuthorityAuthorizationError(ValueError):
    """One intentionally indistinguishable node credential failure."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PrincipalDocument(_StrictModel):
    principal_id: Annotated[
        str,
        Field(min_length=1, max_length=128, pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$"),
    ]
    token_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    slurm_cluster_id: Literal["oldlab", "gb10"]
    node_name: Annotated[
        str,
        Field(min_length=1, max_length=253, pattern=r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$"),
    ]
    scopes: Annotated[tuple[GuardScope, ...], Field(min_length=1, max_length=2)]

    @field_validator("token_sha256")
    @classmethod
    def _nonzero_token_digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None or value == "0" * 64:
            raise ValueError("invalid token digest")
        return value

    @field_validator("scopes")
    @classmethod
    def _canonical_scopes(cls, value: tuple[GuardScope, ...]) -> tuple[GuardScope, ...]:
        if len(value) != len(set(value)) or value != tuple(sorted(value)):
            raise ValueError("invalid principal scope")
        return value

    @model_validator(mode="after")
    def _cluster_node_binding(self) -> _PrincipalDocument:
        expected_prefix = (
            "trt-eai-oldlab-" if self.slurm_cluster_id == "oldlab" else "trt-gb10-"
        )
        if not self.node_name.startswith(expected_prefix):
            raise ValueError("cluster and node disagree")
        return self

    def principal(self) -> TaskImageGuardPrincipalV1:
        return TaskImageGuardPrincipalV1(
            principal_id=self.principal_id,
            slurm_cluster_id=self.slurm_cluster_id,
            node_name=self.node_name,
            scopes=self.scopes,
        )


class _RegistryDocument(_StrictModel):
    schema_version: Literal[1]
    principals: Annotated[tuple[_PrincipalDocument, ...], Field(min_length=1, max_length=4096)]

    @model_validator(mode="before")
    @classmethod
    def _restore_json_tuples(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        principals = normalized.get("principals")
        if isinstance(principals, list):
            normalized["principals"] = tuple(
                {
                    **item,
                    "scopes": tuple(item["scopes"]),
                }
                if isinstance(item, dict) and isinstance(item.get("scopes"), list)
                else item
                for item in principals
            )
        return normalized

    @model_validator(mode="after")
    def _unique_bindings(self) -> _RegistryDocument:
        principal_ids = [item.principal_id for item in self.principals]
        if len(principal_ids) != len(set(principal_ids)):
            raise ValueError("duplicate principal_id")
        token_hashes = [item.token_sha256 for item in self.principals]
        if len(token_hashes) != len(set(token_hashes)):
            raise ValueError("duplicate token_sha256")
        nodes = [(item.slurm_cluster_id, item.node_name) for item in self.principals]
        if len(nodes) != len(set(nodes)):
            raise ValueError("duplicate node")
        return self


def _read_owner_only_file(path: Path) -> bytes:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise TaskImagePrincipalRegistryError("cannot read principal registry") from exc
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
        raise TaskImagePrincipalRegistryError(
            "principal registry must be a regular nonsymlink file"
        )
    if metadata.st_uid != os.getuid():
        raise TaskImagePrincipalRegistryError(
            "principal registry must be owned by the current uid"
        )
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise TaskImagePrincipalRegistryError(
            "principal registry mode must be exactly 0600"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
                raise TaskImagePrincipalRegistryError(
                    "principal registry changed while opening"
                )
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.getuid()
                or stat.S_IMODE(opened.st_mode) != 0o600
            ):
                raise TaskImagePrincipalRegistryError(
                    "principal registry metadata changed while opening"
                )
            chunks: list[bytes] = []
            total = 0
            while total <= _MAX_REGISTRY_BYTES:
                chunk = os.read(
                    descriptor,
                    min(64 * 1024, _MAX_REGISTRY_BYTES + 1 - total),
                )
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
            finished = os.fstat(descriptor)
            if (
                finished.st_dev,
                finished.st_ino,
                finished.st_mode,
                finished.st_uid,
                finished.st_size,
                finished.st_mtime_ns,
                finished.st_ctime_ns,
            ) != (
                opened.st_dev,
                opened.st_ino,
                opened.st_mode,
                opened.st_uid,
                opened.st_size,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
            ):
                raise TaskImagePrincipalRegistryError(
                    "principal registry changed while reading"
                )
            payload = b"".join(chunks)
        finally:
            os.close(descriptor)
    except TaskImagePrincipalRegistryError:
        raise
    except OSError as exc:
        raise TaskImagePrincipalRegistryError("cannot read principal registry") from exc
    if len(payload) > _MAX_REGISTRY_BYTES:
        raise TaskImagePrincipalRegistryError(
            "principal registry exceeds maximum byte size"
        )
    return payload


class TaskImagePrincipalVerifier:
    """Immutable registry verifier that retains token digests, never bearers."""

    def __init__(
        self,
        principals: tuple[tuple[bytes, TaskImageGuardPrincipalV1], ...],
    ) -> None:
        self._principals = principals

    @classmethod
    def from_file(cls, path: Path) -> TaskImagePrincipalVerifier:
        raw = _read_owner_only_file(path)
        try:
            document = _RegistryDocument.model_validate_json(raw)
        except (ValidationError, ValueError, json.JSONDecodeError) as exc:
            message = str(exc).lower()
            invalid_fields = {
                str(location)
                for error in exc.errors()
                for location in error.get("loc", ())
            } if isinstance(exc, ValidationError) else set()
            if "duplicate principal_id" in message:
                label = "duplicate principal_id"
            elif "duplicate token_sha256" in message:
                label = "duplicate token_sha256"
            elif "duplicate node" in message:
                label = "duplicate node"
            elif "token_sha256" in invalid_fields or "token digest" in message:
                label = "invalid token digest"
            elif "scopes" in invalid_fields or "scope" in message:
                label = "invalid principal scope"
            elif "cluster and node" in message:
                label = "cluster and node disagree"
            else:
                label = "unknown or invalid principal registry field"
            raise TaskImagePrincipalRegistryError(label) from None
        return cls(
            tuple(
                (bytes.fromhex(item.token_sha256), item.principal())
                for item in document.principals
            )
        )

    def verify_bearer(self, header: str | None) -> TaskImageGuardPrincipalV1:
        matched = _BEARER.fullmatch(header or "")
        if matched is None:
            raise TaskImageAuthorityAuthorizationError(
                "invalid task-image authority credentials"
            )
        presented = hashlib.sha256(matched.group(1).encode("utf-8")).digest()
        principal: TaskImageGuardPrincipalV1 | None = None
        for candidate, value in self._principals:
            if hmac.compare_digest(presented, candidate):
                principal = value
        if principal is None:
            raise TaskImageAuthorityAuthorizationError(
                "invalid task-image authority credentials"
            )
        return principal


__all__ = [
    "MAX_BEARER_TOKEN_BYTES",
    "TaskImageAuthorityAuthorizationError",
    "TaskImagePrincipalRegistryError",
    "TaskImagePrincipalVerifier",
]
