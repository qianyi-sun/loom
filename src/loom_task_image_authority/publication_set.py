"""Verify a complete expected image set, never issue execution/start authority.

The task snapshot, expected publications/pins and keyset counters/digest must be
supplied by an independently authenticated execution grant. They are not inferred
from the untrusted envelopes. The caller still verifies immutable task-source
bytes, claim identity/capability and consumes fresh serialized one-use start
authority before creating any runtime. This module has no production composition.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime

from pydantic import TypeAdapter

from loom.models.task import TaskConfig
from loom.task_image_materialization import (
    MAX_TASK_IMAGE_COMPONENTS,
    required_task_image_architectures,
    required_task_image_components,
)
from loom_task_image_authority.contracts import Digest
from loom_task_image_authority.publication_contracts import (
    MAX_SIGNER_REPLY_BYTES,
    PublicationUnsignedInput,
    canonical_publication_bytes,
    decode_unsigned_input,
)
from loom_task_image_authority.publication_keyset import (
    MAX_KEYSET_ENVELOPE_BYTES,
    ExecutionGrantTrustRoot,
    verify_keyset_publication,
)
from loom_task_image_authority.publication_signing import PublicationState, VerifiedPublication

MAX_PUBLICATION_SET_BYTES = MAX_TASK_IMAGE_COMPONENTS * MAX_SIGNER_REPLY_BYTES + MAX_KEYSET_ENVELOPE_BYTES
_COMPONENT_FIELDS = {"component", "repository", "root", "manifest", "config", "layers", "observed_base_digests"}


@dataclass(frozen=True)
class ExpectedPublication:
    """Exact grant-bound unsigned identity and complete original envelope pin."""

    unsigned: PublicationUnsignedInput
    envelope_sha256: str

    def __post_init__(self) -> None:
        if type(self.unsigned) is not PublicationUnsignedInput:
            raise ValueError("expected publication requires the closed unsigned contract")
        decode_unsigned_input(canonical_publication_bytes(self.unsigned))
        TypeAdapter(Digest).validate_python(self.envelope_sha256, strict=True)


@dataclass(frozen=True)
class VerifiedPublicationSet:
    publications: tuple[VerifiedPublication, ...]
    registry_images: tuple[tuple[str, str], ...]


def verify_publication_set(
    *, publication_wires: tuple[bytes, ...], expected: tuple[ExpectedPublication, ...],
    task: TaskConfig, keyset_wire: bytes, trust_root: ExecutionGrantTrustRoot,
    expected_state: PublicationState, expected_snapshot_sha256: str, now: datetime,
) -> VerifiedPublicationSet:
    """Return all-or-nothing verification; no key fetching, readiness or start."""
    if (
        type(publication_wires) is not tuple or type(expected) is not tuple
        or not 1 <= len(expected) <= MAX_TASK_IMAGE_COMPONENTS
        or len(publication_wires) != len(expected)
        or type(keyset_wire) is not bytes or not 0 < len(keyset_wire) <= MAX_KEYSET_ENVELOPE_BYTES
        or any(type(wire) is not bytes or not 0 < len(wire) <= MAX_SIGNER_REPLY_BYTES for wire in publication_wires)
        or type(task) is not TaskConfig
    ):
        raise ValueError("invalid bounded publication set")
    if sum(map(len, publication_wires)) + len(keyset_wire) > MAX_PUBLICATION_SET_BYTES:
        raise ValueError("publication set exceeds aggregate byte ceiling")
    # Revalidate frozen models: dataclass/model construction and model_copy can
    # otherwise bypass validators. Use the copied validated snapshot throughout.
    task = TaskConfig.model_validate(task.model_dump(mode="python", warnings=False))
    sidecar_names = tuple(sidecar.name for sidecar in task.environment.sidecars)
    if task.environment.os != "linux" or len(set(sidecar_names)) != len(sidecar_names):
        raise ValueError("publication set requires Linux and unambiguous sidecar names")
    unsigneds: list[PublicationUnsignedInput] = []
    for entry in expected:
        if type(entry) is not ExpectedPublication:
            raise ValueError("invalid expected publication binding")
        entry.__post_init__()
        unsigneds.append(decode_unsigned_input(canonical_publication_bytes(entry.unsigned)))
    names = tuple(item.component for item in unsigneds)
    if names != tuple(sorted(required_task_image_components(task))):
        raise ValueError("publication set differs from frozen task components")
    first = unsigneds[0]
    arch = "arm64" if first.platform == "linux/arm64" else "x86_64"
    if first.task_id != task.task.id or arch not in required_task_image_architectures(task):
        raise ValueError("publication set differs from frozen task identity or architecture")
    common = first.model_dump(mode="json", by_alias=True, exclude_none=True, exclude=_COMPONENT_FIELDS)
    if any(item.model_dump(mode="json", by_alias=True, exclude_none=True, exclude=_COMPONENT_FIELDS) != common for item in unsigneds):
        raise ValueError("publication set mixes build authority")
    verified = []
    for wire, entry, unsigned in zip(publication_wires, expected, unsigneds, strict=True):
        if hashlib.sha256(wire).hexdigest() != entry.envelope_sha256:
            raise ValueError("publication set original envelope pin differs")
        verified.append(verify_keyset_publication(
            wire, keyset_wire=keyset_wire, trust_root=trust_root, expected_state=expected_state,
            expected_snapshot_sha256=expected_snapshot_sha256, expected_unsigned=unsigned, now=now,
        ))
    images = tuple(
        (item.statement.component, f"{item.statement.registry_origin.removeprefix('https://')}/{item.statement.repository}@{item.statement.manifest.digest}")
        for item in verified
    )
    return VerifiedPublicationSet(tuple(verified), images)
