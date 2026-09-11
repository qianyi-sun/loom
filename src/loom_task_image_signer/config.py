"""Closed protected startup configuration with explicit public-root and TLS pins."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom_task_image_authority.contracts import Identifier
from loom_task_image_authority.publication_contracts import (
    PublicationTimestamp,
    _reject_constant,
    _unique_object,
)
from loom_task_image_authority.publication_keyset import ExecutionGrantTrustRoot
from loom_task_image_signer.policy import PublicationSelection
from loom_task_image_signer.server import SignerServerLimits

PublicKey = Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{43}$")]


def _public(value: str) -> bytes:
    raw = base64.urlsafe_b64decode(value + "=")
    if len(raw) != 32 or base64.urlsafe_b64encode(raw).rstrip(b"=").decode() != value:
        raise ValueError("invalid signer public pin")
    return raw


class _Closed(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class KeySettings(_Closed):
    key_id: Identifier
    public_key: PublicKey
    seed_file: Path

    def public_bytes(self) -> bytes:
        return _public(self.public_key)

    @model_validator(mode="after")
    def _valid(self) -> KeySettings:
        self.public_bytes()
        if not self.seed_file.is_absolute() or ".." in self.seed_file.parts:
            raise ValueError("signer key path must be absolute")
        return self


class ExecutionSettings(KeySettings):
    environment: Identifier
    activated_at: PublicationTimestamp
    expires_at: PublicationTimestamp

    def trust_root(self) -> ExecutionGrantTrustRoot:
        return ExecutionGrantTrustRoot(
            key_id=self.key_id, environment=self.environment, public_key=self.public_bytes(),
            activated_at=datetime.strptime(self.activated_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC),
            expires_at=datetime.strptime(self.expires_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC),
        )


class SignerSettings(_Closed):
    schema_name: Literal["loom.task-image-signer/v1"] = Field(alias="schema")
    host: Literal["127.0.0.1", "0.0.0.0"]
    port: Annotated[int, Field(ge=1, le=65535)]
    database_url_file: Path
    ca_file: Path
    certificate_file: Path
    private_key_file: Path
    execution: ExecutionSettings
    publication: KeySettings
    selections: Annotated[tuple[PublicationSelection, ...], Field(min_length=1, max_length=128)]
    peer_operations: Annotated[dict[Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")], tuple[Literal["keyset", "publication"], ...]], Field(min_length=1, max_length=128)]
    limits: SignerServerLimits = SignerServerLimits()
    policy_timeout_seconds: Annotated[float, Field(gt=0, le=10)] = 5.0
    keyset_lifetime_seconds: Annotated[int, Field(ge=1, le=900)] = 300

    def trust_root(self) -> ExecutionGrantTrustRoot:
        return self.execution.trust_root()

    @model_validator(mode="after")
    def _valid(self) -> SignerSettings:
        root = self.trust_root()
        self.limits.__post_init__()
        if (
            root.key_id == self.publication.key_id or root.public_key == self.publication.public_bytes()
            or self.execution.seed_file == self.publication.seed_file
            or any(selection.environment != root.environment for selection in self.selections)
            or len(set(self.selections)) != len(self.selections)
            or any(not operations or len(set(operations)) != len(operations) for operations in self.peer_operations.values())
        ):
            raise ValueError("invalid separate signer authorities")
        for path in (self.database_url_file, self.ca_file, self.certificate_file, self.private_key_file):
            if not path.is_absolute() or ".." in path.parts:
                raise ValueError("signer configuration paths must be absolute")
        return self


def decode_signer_settings(wire: bytes) -> SignerSettings:
    if type(wire) is not bytes or not 0 < len(wire) <= 64 * 1024:
        raise ValueError("signer configuration exceeds byte ceiling")
    try:
        # Check duplicates before pydantic's JSON decoder; JSON mode deliberately
        # admits Path/date/tuple encodings while preserving strict scalar types.
        json.loads(wire, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
        return SignerSettings.model_validate_json(wire)
    except (ValueError, TypeError, RecursionError):
        raise ValueError("invalid dedicated signer configuration") from None
