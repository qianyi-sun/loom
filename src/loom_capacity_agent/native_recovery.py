"""Versioned recovery facts, not an attestation, execution or deletion permit.

The expected digest and preparation must come from authenticated protected
readback. A digest beside a worker-owned locator supplies no such provenance.
Publication, installed-profile admission and terminal recovery are separate.
"""

from __future__ import annotations

import hashlib
import re
from itertools import pairwise
from pathlib import PurePosixPath
from typing import Annotated, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from loom_capacity_agent.admission import PhysicalJobBindingV2
from loom_capacity_manager.contracts import Digest, Identifier, StrictV1Model, canonical_bytes
from loom_capacity_manager.executable_contracts import StrictV2Model, canonical_executable_bytes

_MAX_WIRE = 128 * 1024
_ID_LIMIT = 2**32 - 1


class NativeInstalledAttemptV1(StrictV1Model):
    """Compatibility locator; insufficient for automated destructive recovery."""

    physical: PhysicalJobBindingV2
    worker_id: UUID
    worker_incarnation: UUID
    config_sha256: Digest
    release_manifest_sha256: Digest
    directory: str
    device: int = Field(ge=0)
    inode: int = Field(gt=0)


def _path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (not path.is_absolute() or str(path) != value or value.startswith("//") or path == PurePosixPath("/")
        or ".." in path.parts or len(value) > 4096 or len(path.parts) > 64
        or any(character in value for character in ("\0", "\n", "\r"))):
        raise ValueError("native recovery path must be bounded, canonical and absolute")
    return path


class NativeRecoveryPreparationV1(StrictV1Model):
    """Original-host observations to retain before the mapped launcher starts."""

    locator: NativeInstalledAttemptV1
    launch_profile_sha256: Digest
    node_configuration_sha256: Digest
    node_id: Identifier
    boot_id: UUID
    original_uid: int = Field(gt=0, lt=_ID_LIMIT)
    original_gid: int = Field(gt=0, lt=_ID_LIMIT)
    cgroup_path: str
    cgroup_device: int = Field(ge=0)
    cgroup_inode: int = Field(gt=0)
    cgroup_mount_id: int = Field(gt=0)

    @model_validator(mode="after")
    def _bound_scope(self) -> Self:
        _path(self.locator.directory)
        scope = _path(self.cgroup_path)
        physical = self.locator.physical
        job = physical.slurm_job_id
        names = {f"job_{job}", f"job_{job.split('_', 1)[0]}"}
        ancestors = scope.parts[1:-1]
        slurm = any(part in {"slurm", "slurmstepd.scope"} or part.endswith("_slurmstepd.scope") for part in ancestors)
        if (self.node_id not in physical.binding.node_ids or scope.name not in names
            or not slurm or any(part.startswith("job_") for part in ancestors)):
            raise ValueError("native recovery node or job scope differs from allocation")
        # Shape/binding validation is not a kernel observation or Slurm proof.
        # The fixed original-UID observer must authenticate the actual job scope.
        return self


class NativeRecoveryMappingRange(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    inside: int = Field(ge=0, lt=_ID_LIMIT)
    outside: int = Field(gt=0, lt=_ID_LIMIT)
    count: int = Field(gt=0, lt=_ID_LIMIT)


class NativeInstalledAttemptV2(StrictV2Model):
    """Final mapped observations; no terminal, quiescent or executable boolean."""

    preparation: NativeRecoveryPreparationV1
    runtime_spec_sha256: Digest
    uid_map: tuple[NativeRecoveryMappingRange, ...] = Field(min_length=1, max_length=340)
    gid_map: tuple[NativeRecoveryMappingRange, ...] = Field(min_length=1, max_length=340)

    @model_validator(mode="after")
    def _actual_maps(self) -> Self:
        for ranges, original in ((self.uid_map, self.preparation.original_uid),
            (self.gid_map, self.preparation.original_gid)):
            root = ranges[0]
            if (root.inside, root.outside, root.count) != (0, original, 1):
                raise ValueError("native recovery root map differs from original identity")
            if any(item.inside + item.count > _ID_LIMIT or item.outside + item.count > _ID_LIMIT for item in ranges):
                raise ValueError("native recovery mapping exceeds identity range")
            for coordinate in ("inside", "outside"):
                ordered = sorted((getattr(item, coordinate), item.count) for item in ranges)
                if any(start + count > following for (start, count), (following, _) in pairwise(ordered)):
                    raise ValueError("native recovery mapping overlaps")
        return self


NativeRecoveryLocator = NativeInstalledAttemptV1 | NativeInstalledAttemptV2
_LOCATOR: TypeAdapter[NativeRecoveryLocator] = TypeAdapter(
    Annotated[NativeRecoveryLocator, Field(discriminator="schema_version")])


def parse_native_recovery_locator(wire: bytes) -> NativeRecoveryLocator:
    """Strict compatibility reader for inspection, never inferred authority."""
    if not isinstance(wire, bytes) or not 1 <= len(wire) <= _MAX_WIRE:
        raise ValueError("native recovery record exceeds byte bound")
    record = _LOCATOR.validate_json(wire)
    canonical = (canonical_executable_bytes(record) if isinstance(record, NativeInstalledAttemptV2)
        else canonical_bytes(record))
    if canonical != wire:
        raise ValueError("native recovery record must be canonical")
    return record


def read_final_native_recovery(wire: bytes, *, expected_sha256: str,
    expected_preparation: NativeRecoveryPreparationV1,
) -> NativeInstalledAttemptV2:
    """Check finalized bytes against externally retained preparation and digest.

    This proves only exact historical correspondence. The caller still needs
    protected installed-profile provenance, terminal fencing, local whole-subtree
    quiescence and retained mapping policy before considering any cleanup.
    """
    if (not isinstance(expected_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None
        or not isinstance(wire, bytes) or len(wire) > _MAX_WIRE
        or hashlib.sha256(wire).hexdigest() != expected_sha256):
        raise ValueError("native recovery expected digest changed")
    record = parse_native_recovery_locator(wire)
    if not isinstance(record, NativeInstalledAttemptV2):
        raise ValueError("native recovery requires finalized V2 evidence")
    if not isinstance(expected_preparation, NativeRecoveryPreparationV1):
        raise ValueError("native recovery expected preparation is invalid")
    expected = NativeRecoveryPreparationV1.model_validate_json(expected_preparation.model_dump_json())
    if record.preparation != expected:
        raise ValueError("native recovery preparation differs from retained evidence")
    return record
