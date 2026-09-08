"""Versioned executable allocation evidence for delegated personal membership."""

from __future__ import annotations

import json
from typing import Literal

from loom_capacity_manager.allocator import ExecutableEpochV2
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.membership import resolved_subject_references
from loom_capacity_manager.membership_contracts import (
    DelegatedAllocationInputV2,
    PersonalMembershipSnapshotV1,
)


class ExecutableEpochV3(ExecutableEpochV2):
    """Immutable base placement plus the exact membership used to authorize it."""

    # Deliberately distinct wire version; legacy V2 validation remains unchanged.
    schema_version: Literal[3] = 3  # type: ignore[assignment]
    membership: PersonalMembershipSnapshotV1


def bind_executable_membership(
    epoch: ExecutableEpochV2, value: DelegatedAllocationInputV2
) -> ExecutableEpochV3:
    """Seal the already CAS-verified input without changing placement or authority."""

    if (
        epoch.input_digest != canonical_digest(value)
        or epoch.configuration != value.configuration
        or epoch.execution.execution_manifest_sha256
        != canonical_executable_digest(value.preparation)
    ):
        raise ValueError("executable membership input binding changed")
    resolved_subject_references(value)
    payload = epoch.model_dump(mode="python") | {
        "schema_version": 3,
        "membership": value.membership.model_dump(mode="python"),
    }
    return ExecutableEpochV3.model_validate(payload)


def parse_executable_epoch(payload: str | bytes) -> ExecutableEpochV2:
    """Keep the exact V2 parser while requiring an integer V3 wire discriminator."""

    document = json.loads(payload)
    if not isinstance(document, dict):
        raise ValueError("executable allocation must be an object")
    version = document.get("schema_version")
    if version == 3 and type(version) is int:
        return ExecutableEpochV3.model_validate_json(payload)
    if version == 2 and type(version) is int:
        return ExecutableEpochV2.model_validate_json(payload)
    raise ValueError("unsupported executable allocation schema")


__all__ = ["ExecutableEpochV3", "bind_executable_membership", "parse_executable_epoch"]
