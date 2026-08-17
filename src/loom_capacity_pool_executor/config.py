"""Stable controller-local configuration for read-only Slurm inventory."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    MAX_NODES_PER_DOMAIN,
    Digest,
    Identifier,
    NodeEnvelopeV1,
    PositiveQuantity,
    ResourceVectorV1,
)
from loom_capacity_pool_executor.slurm_inventory import SlurmInventoryPolicy

MAX_SLURM_INVENTORY_POLICY_BYTES = MAX_CONTRACT_BYTES
_DIGEST = re.compile(r"[0-9a-f]{64}")
_GENERIC_ERROR = "Slurm inventory policy is invalid"


class SlurmInventoryPolicyError(ValueError):
    """The controller-local policy file cannot be trusted or parsed exactly."""


def _invalid() -> SlurmInventoryPolicyError:
    return SlurmInventoryPolicyError(_GENERIC_ERROR)


class _StrictPolicyDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SlurmInventoryNodeDocument(_StrictPolicyDocument):
    """One explicit pool-bound node in the complete controller inventory."""

    pool_id: Literal["gb10", "oldlab"]
    node_id: Identifier
    allocatable: ResourceVectorV1
    features: tuple[Identifier, ...] = ()

    @field_validator("features", mode="before")
    @classmethod
    def _feature_tuple(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("features")
    @classmethod
    def _canonical_features(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("Slurm inventory node features must be unique")
        return tuple(sorted(value))

    def node_envelope(self) -> NodeEnvelopeV1:
        return NodeEnvelopeV1(
            node_id=self.node_id,
            allocatable=self.allocatable,
            features=self.features,
        )


class SlurmInventoryPolicyDocument(_StrictPolicyDocument):
    """Canonical non-secret JSON document consumed on exactly one controller."""

    schema_version: Literal[1]
    pool_id: Literal["gb10", "oldlab"]
    pool_generation: PositiveQuantity
    reporter_incarnation: Annotated[str, Field(min_length=36, max_length=36)]
    nodes: Annotated[
        tuple[SlurmInventoryNodeDocument, ...],
        Field(min_length=1, max_length=MAX_NODES_PER_DOMAIN),
    ]
    relevant_partitions: Annotated[tuple[Identifier, ...], Field(min_length=1, max_length=128)]
    slot_resources: ResourceVectorV1
    controller_cluster: Identifier
    slurm_version: tuple[int, int, int]
    data_parser: Annotated[
        str,
        Field(min_length=1, max_length=128, pattern=r"^data_parser/v[0-9]+\.[0-9]+\.[0-9]+$"),
    ]
    query_principal: Identifier
    query_uid: Annotated[int, Field(gt=0, lt=2**32 - 1)]
    job_visibility_evidence_sha256: Digest
    scontrol_sha256: Digest
    squeue_sha256: Digest
    slurm_conf_sha256: Digest

    @field_validator("nodes", "relevant_partitions", "slurm_version", mode="before")
    @classmethod
    def _tuple_fields(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("nodes")
    @classmethod
    def _canonical_nodes(
        cls,
        value: tuple[SlurmInventoryNodeDocument, ...],
    ) -> tuple[SlurmInventoryNodeDocument, ...]:
        identities = [node.node_id.casefold() for node in value]
        if len(identities) != len(set(identities)):
            raise ValueError("Slurm inventory nodes must be unique")
        return tuple(sorted(value, key=lambda node: node.node_id))

    @field_validator("relevant_partitions")
    @classmethod
    def _canonical_partitions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("Slurm inventory partitions must be unique")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def _exact_controller_boundary(self) -> SlurmInventoryPolicyDocument:
        try:
            reporter = UUID(self.reporter_incarnation)
        except ValueError as exc:
            raise ValueError("Slurm inventory reporter incarnation is invalid") from exc
        if reporter.int == 0 or str(reporter) != self.reporter_incarnation:
            raise ValueError("Slurm inventory reporter incarnation is invalid")
        if any(node.pool_id != self.pool_id for node in self.nodes):
            raise ValueError("Slurm inventory node belongs to another pool")
        if self.job_visibility_evidence_sha256 == "0" * 64:
            raise ValueError("Slurm inventory visibility evidence is invalid")
        self.to_policy()
        return self

    def to_policy(self) -> SlurmInventoryPolicy:
        return SlurmInventoryPolicy(
            pool_id=self.pool_id,
            pool_generation=self.pool_generation,
            reporter_incarnation=UUID(self.reporter_incarnation),
            nodes=tuple(node.node_envelope() for node in self.nodes),
            relevant_partitions=self.relevant_partitions,
            slot_resources=self.slot_resources,
            controller_cluster=self.controller_cluster,
            slurm_version=self.slurm_version,
            data_parser=self.data_parser,
            query_principal=self.query_principal,
            query_uid=self.query_uid,
            job_visibility_evidence_sha256=self.job_visibility_evidence_sha256,
            scontrol_sha256=self.scontrol_sha256,
            squeue_sha256=self.squeue_sha256,
            slurm_conf_sha256=self.slurm_conf_sha256,
        )


def canonical_slurm_inventory_policy_bytes(
    document: SlurmInventoryPolicyDocument,
) -> bytes:
    """Encode one renderer/loader-shared document without platform drift."""

    if not isinstance(document, SlurmInventoryPolicyDocument):
        raise TypeError("Slurm inventory policy document is invalid")
    encoded = json.dumps(
        document.model_dump(mode="json", exclude_none=False),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    if len(encoded) > MAX_SLURM_INVENTORY_POLICY_BYTES:
        raise ValueError("Slurm inventory policy document exceeds its byte bound")
    return encoded


def load_slurm_inventory_policy(
    path: Path,
    *,
    expected_sha256: str,
) -> SlurmInventoryPolicy:
    """Load a bounded stable regular file and verify its exact raw digest."""

    if (
        not isinstance(expected_sha256, str)
        or _DIGEST.fullmatch(expected_sha256) is None
        or expected_sha256 == "0" * 64
    ):
        raise _invalid()
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.geteuid()
                or stat.S_IMODE(before.st_mode) & 0o022
                or before.st_nlink != 1
                or not 0 < before.st_size <= MAX_SLURM_INVENTORY_POLICY_BYTES
            ):
                raise _invalid()
            chunks: list[bytes] = []
            remaining = MAX_SLURM_INVENTORY_POLICY_BYTES + 1
            while remaining > 0:
                chunk = os.read(descriptor, min(64 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
    except SlurmInventoryPolicyError:
        raise
    except OSError:
        raise _invalid() from None
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_nlink",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if (
        len(payload) != before.st_size
        or len(payload) > MAX_SLURM_INVENTORY_POLICY_BYTES
        or any(getattr(before, field) != getattr(after, field) for field in stable_fields)
        or hashlib.sha256(payload).hexdigest() != expected_sha256
    ):
        raise _invalid()
    try:
        document = SlurmInventoryPolicyDocument.model_validate_json(payload)
        return document.to_policy()
    except (ValidationError, ValueError, TypeError):
        raise _invalid() from None


__all__ = [
    "MAX_SLURM_INVENTORY_POLICY_BYTES",
    "SlurmInventoryNodeDocument",
    "SlurmInventoryPolicyDocument",
    "SlurmInventoryPolicyError",
    "canonical_slurm_inventory_policy_bytes",
    "load_slurm_inventory_policy",
]
