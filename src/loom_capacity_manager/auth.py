"""Strict bearer identity verification for the capacity manager."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

_MAX_REGISTRY_BYTES = 1024 * 1024
MAX_BEARER_TOKEN_BYTES = 4096
_BEARER = re.compile(rf"Bearer ([^\s]{{1,{MAX_BEARER_TOKEN_BYTES}}})", re.ASCII)
CapacityScope = Literal[
    "capacity:configure:fleet",
    "capacity:configure:subject",
    "capacity:configure:activate",
    "capacity:project:development",
    "capacity:reconcile",
    "capacity:read",
    "capacity:report:demand",
    "capacity:report:pool",
    "capacity:grant:manage",
    "capacity:execute:pool",
]


def bearer_token_sha256(header: str | None) -> str:
    """Validate one bearer header and return only its lowercase digest."""

    matched = _BEARER.fullmatch(header or "")
    if matched is None:
        raise AuthorizationError("invalid capacity credentials")
    return hashlib.sha256(matched.group(1).encode("utf-8")).hexdigest()


class PrincipalRegistryError(ValueError):
    """Raised when an on-disk principal registry is unsafe or invalid."""


class AuthorizationError(ValueError):
    """One intentionally indistinguishable credential failure."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PrincipalDocument(_StrictModel):
    principal_id: Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[a-z0-9-]+$")]
    token_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    scopes: Annotated[tuple[CapacityScope, ...], Field(min_length=1)]
    subject_id: UUID | None = None
    subject_incarnation: UUID | None = None
    demand_reporter_incarnation: UUID | None = None
    pool_id: Annotated[
        str | None,
        Field(min_length=1, max_length=128, pattern=r"^[a-z0-9-]+$"),
    ]
    pool_reporter_incarnation: UUID | None = None
    executor_id: Annotated[
        str | None,
        Field(min_length=1, max_length=128, pattern=r"^[a-z0-9-]+$"),
    ] = None
    executor_incarnation: UUID | None = None

    @field_validator("scopes")
    @classmethod
    def _canonical_scopes(cls, value: tuple[CapacityScope, ...]) -> tuple[CapacityScope, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate principal scope")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def _complete_reporter_binding(self) -> _PrincipalDocument:
        subject_values = (
            self.subject_id,
            self.subject_incarnation,
            self.demand_reporter_incarnation,
        )
        has_subject = any(value is not None for value in subject_values)
        has_pool_reporter = self.pool_reporter_incarnation is not None
        has_executor = any(
            value is not None for value in (self.executor_id, self.executor_incarnation)
        )
        if has_subject and not all(value is not None for value in subject_values):
            raise ValueError("incomplete subject binding")
        if has_pool_reporter and self.pool_id is None:
            raise ValueError("incomplete pool binding")
        if has_executor and not all(
            value is not None
            for value in (self.pool_id, self.executor_id, self.executor_incarnation)
        ):
            raise ValueError("incomplete executor binding")
        if has_subject and self.pool_id is not None:
            raise ValueError("principal cannot combine subject and pool bindings")
        if has_pool_reporter and has_executor:
            raise ValueError("principal cannot combine reporter and executor bindings")
        if self.pool_id is not None and not (has_pool_reporter or has_executor):
            raise ValueError("incomplete pool binding")
        if "capacity:report:demand" in self.scopes and not has_subject:
            raise ValueError("demand reporter requires complete subject binding")
        if "capacity:report:pool" in self.scopes and not has_pool_reporter:
            raise ValueError("pool reporter requires complete pool binding")
        if "capacity:execute:pool" in self.scopes and not has_executor:
            raise ValueError("pool executor requires complete executor binding")
        if has_subject and "capacity:report:demand" not in self.scopes:
            raise ValueError("subject binding requires demand reporter scope")
        if has_pool_reporter and "capacity:report:pool" not in self.scopes:
            raise ValueError("pool binding requires pool reporter scope")
        if has_executor and "capacity:execute:pool" not in self.scopes:
            raise ValueError("executor binding requires pool executor scope")
        if has_subject and set(self.scopes) != {"capacity:report:demand"}:
            raise ValueError("subject reporter principal must be single-purpose")
        if has_pool_reporter and set(self.scopes) != {"capacity:report:pool"}:
            raise ValueError("pool reporter principal must be single-purpose")
        if has_executor and set(self.scopes) != {"capacity:execute:pool"}:
            raise ValueError("pool executor principal must be single-purpose")
        return self


class _RegistryDocument(_StrictModel):
    schema_version: Literal[1]
    principals: Annotated[tuple[_PrincipalDocument, ...], Field(min_length=1, max_length=4096)]

    @model_validator(mode="after")
    def _unique_authority(self) -> _RegistryDocument:
        principal_ids = [principal.principal_id for principal in self.principals]
        if len(principal_ids) != len(set(principal_ids)):
            raise ValueError("duplicate principal id")
        token_hashes = [principal.token_sha256 for principal in self.principals]
        if len(token_hashes) != len(set(token_hashes)):
            raise ValueError("duplicate token hash")
        if not any(
            "capacity:reconcile" in principal.scopes
            and principal.subject_id is None
            and principal.pool_id is None
            for principal in self.principals
        ):
            raise ValueError("principal registry requires an unbound operator")
        return self


@dataclass(frozen=True, slots=True)
class CapacityPrincipal:
    principal_id: str
    scopes: frozenset[CapacityScope]
    subject_id: UUID | None
    subject_incarnation: UUID | None
    demand_reporter_incarnation: UUID | None
    pool_id: str | None
    pool_reporter_incarnation: UUID | None
    executor_id: str | None
    executor_incarnation: UUID | None

    def has_scope(self, scope: CapacityScope) -> bool:
        return scope in self.scopes


def _read_owner_only_file(path: Path) -> bytes:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise PrincipalRegistryError("cannot read principal registry") from exc
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
        raise PrincipalRegistryError("principal registry must be a regular nonsymlink file")
    if metadata.st_uid != os.getuid():
        raise PrincipalRegistryError("principal registry must be owned by the current uid")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise PrincipalRegistryError("principal registry mode must be exactly 0600")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
                raise PrincipalRegistryError("principal registry changed while opening")
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.getuid()
                or stat.S_IMODE(opened.st_mode) != 0o600
            ):
                raise PrincipalRegistryError("principal registry metadata changed while opening")
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
            payload = b"".join(chunks)
        finally:
            os.close(descriptor)
    except PrincipalRegistryError:
        raise
    except OSError as exc:
        raise PrincipalRegistryError("cannot read principal registry") from exc
    if len(payload) > _MAX_REGISTRY_BYTES:
        raise PrincipalRegistryError("principal registry exceeds maximum byte size")
    return payload


class CapacityPrincipalVerifier:
    """Immutable registry-backed verifier that never stores plaintext tokens."""

    def __init__(self, principals: tuple[tuple[bytes, CapacityPrincipal], ...]) -> None:
        self._principals = principals

    @classmethod
    def from_file(cls, path: Path) -> CapacityPrincipalVerifier:
        raw = _read_owner_only_file(path)
        try:
            document = _RegistryDocument.model_validate_json(raw)
        except (ValidationError, ValueError, json.JSONDecodeError) as exc:
            message = str(exc).lower()
            if "duplicate principal" in message:
                label = "duplicate principal"
            elif "duplicate token" in message:
                label = "duplicate token"
            elif "subject binding" in message:
                label = "invalid subject binding"
            elif "executor binding" in message:
                label = "invalid executor binding"
            elif "pool binding" in message:
                label = "invalid pool binding"
            elif "single-purpose" in message:
                label = "bound principal must be single-purpose"
            elif "operator" in message:
                label = "principal registry requires an operator"
            elif "scope" in message or "literal_error" in message:
                label = "invalid principal scope"
            else:
                label = "unknown or invalid principal registry field"
            raise PrincipalRegistryError(label) from exc
        principals = tuple(
            (
                bytes.fromhex(item.token_sha256),
                CapacityPrincipal(
                    principal_id=item.principal_id,
                    scopes=frozenset(item.scopes),
                    subject_id=item.subject_id,
                    subject_incarnation=item.subject_incarnation,
                    demand_reporter_incarnation=item.demand_reporter_incarnation,
                    pool_id=item.pool_id,
                    pool_reporter_incarnation=item.pool_reporter_incarnation,
                    executor_id=item.executor_id,
                    executor_incarnation=item.executor_incarnation,
                ),
            )
            for item in document.principals
        )
        return cls(principals)

    def verify_bearer(self, header: str | None) -> CapacityPrincipal:
        presented = bytes.fromhex(bearer_token_sha256(header))
        principal: CapacityPrincipal | None = None
        for candidate, value in self._principals:
            if hmac.compare_digest(presented, candidate):
                principal = value
        if principal is None:
            raise AuthorizationError("invalid capacity credentials")
        return principal


__all__ = [
    "MAX_BEARER_TOKEN_BYTES",
    "AuthorizationError",
    "CapacityPrincipal",
    "CapacityPrincipalVerifier",
    "CapacityScope",
    "PrincipalRegistryError",
    "bearer_token_sha256",
]
