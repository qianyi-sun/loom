"""Public execution trust configuration; no signing seeds or discovered roots."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom_task_image_authority.config import _validate_https_origin, read_owner_only_bytes
from loom_task_image_authority.contracts import BuildPurpose, Identifier
from loom_task_image_authority.publication_contracts import (
    CanonicalUUID,
    PublicationTimestamp,
    _reject_constant,
    _unique_object,
)
from loom_task_image_authority.publication_keyset import (
    ExecutionGrantTrustRoot,
    _base64url,
    _instant,
)


class _Closed(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ExecutionRootSettings(_Closed):
    key_id: Identifier
    environment: Identifier
    public_key: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{43}$")]
    activated_at: PublicationTimestamp
    expires_at: PublicationTimestamp

    def trust_root(self) -> ExecutionGrantTrustRoot:
        return ExecutionGrantTrustRoot(self.key_id, self.environment, _base64url(self.public_key, 32),
                                       _instant(self.activated_at), _instant(self.expires_at))


class _TrustSettings(_Closed):
    root: ExecutionRootSettings
    purpose: BuildPurpose
    shadow_campaign_id: CanonicalUUID | None = None

    @model_validator(mode="after")
    def _valid(self) -> _TrustSettings:
        root = self.root.trust_root()
        if (self.purpose == "production") != (self.shadow_campaign_id is None):
            raise ValueError("execution purpose and campaign disagree")
        if not root.activated_at <= datetime.now(UTC) < root.expires_at:
            raise ValueError("execution release root is not currently valid")
        return self


class ExecutionReaderSettings(_TrustSettings):
    schema_name: Literal["loom.task-image-execution-reader/v1"] = Field(alias="schema")


class ExecutionSignerSettings(_Closed):
    origin: str
    ca_file: Path
    client_cert_file: Path
    client_key_file: Path

    @model_validator(mode="after")
    def _valid(self) -> ExecutionSignerSettings:
        _validate_https_origin(self.origin, label="execution signer")
        for path in (self.ca_file, self.client_cert_file, self.client_key_file):
            if not path.is_absolute() or ".." in path.parts:
                raise ValueError("signer TLS paths must be absolute")
        return self


class ExecutionAdmissionSettings(_TrustSettings):
    schema_name: Literal["loom.task-image-execution-admission/v1"] = Field(alias="schema")
    signer: ExecutionSignerSettings


_Settings = TypeVar("_Settings", ExecutionReaderSettings, ExecutionAdmissionSettings)


def _load(path: Path, model: type[_Settings]) -> _Settings:
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("execution configuration path must be absolute")
    wire = read_owner_only_bytes(path, max_bytes=64 * 1024)
    try:
        json.loads(wire, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
        return model.model_validate_json(wire)
    except (ValueError, TypeError, RecursionError):
        raise ValueError("invalid release execution configuration") from None


def load_execution_reader_settings(path: Path) -> ExecutionReaderSettings:
    return _load(path, ExecutionReaderSettings)


def load_execution_admission_settings(path: Path) -> ExecutionAdmissionSettings:
    return _load(path, ExecutionAdmissionSettings)
