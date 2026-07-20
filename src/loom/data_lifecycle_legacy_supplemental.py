"""Strict legacy ownership for physical objects absent from artifact descriptors.

Legacy staging wrote several exact object layouts before every object had a
registry row.  This module recognizes only layouts whose owner can be proven
from the same repeatable-read database snapshot.  It never grants prefix
deletion authority: every accepted object is inspected, hashed, and registered
as one exact bucket/key/version identity.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from loom.data_lifecycle import DataClass, OwnerKind
from loom.data_lifecycle_gc import ObservedObject
from loom.data_lifecycle_legacy import (
    LegacyAuthoritySeed,
    LegacyClassificationError,
    LegacySupplementalObject,
)


class LegacyObjectInspector(Protocol):
    def inspect(
        self,
        *,
        bucket: str,
        object_key: str,
        version_id: str | None,
    ) -> tuple[str | None, str, int] | None: ...


_TASKSET_SLUG_RE = re.compile(r"^[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?$")
_FAMILY_SNAPSHOT_RE = re.compile(r"^state-[0-9]{8}t[0-9]{12}z[.]tar[.]gz$")


@dataclass(frozen=True, slots=True)
class LegacyArtifactSource:
    authority: LegacyAuthoritySeed
    batch_id: str | None


@dataclass(frozen=True, slots=True)
class LegacySupplementalSources:
    trials: Mapping[str, LegacyAuthoritySeed]
    batches: Mapping[str, LegacyAuthoritySeed]
    artifacts: Mapping[str, LegacyArtifactSource]
    task_sets: Mapping[tuple[str, str], LegacyAuthoritySeed]
    family_states: frozenset[tuple[str, str]]


def _safe_parts(object_key: str) -> tuple[str, ...] | None:
    if not object_key or object_key.startswith("/"):
        return None
    parts = tuple(object_key.split("/"))
    if any(
        not part
        or part in {".", ".."}
        or "\\" in part
        or any(ord(character) < 32 or ord(character) == 127 for character in part)
        for part in parts
    ):
        return None
    return parts


def _object_evidence_id(item: ObservedObject) -> str:
    payload = "\0".join((*item.identity, str(item.size_bytes))).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def _fixed_authority(item: ObservedObject, *, catalog: bool) -> LegacyAuthoritySeed:
    if item.last_modified is None:
        raise LegacyClassificationError(
            f"legacy supplemental object {_object_evidence_id(item)} lacks creation authority"
        )
    return LegacyAuthoritySeed(
        team_id=None,
        data_class=DataClass.CATALOG if catalog else DataClass.SYSTEM,
        owner_kind=OwnerKind.SYSTEM,
        owner_id=(
            "legacy-catalog:skillflow-iter-e2e"
            if catalog
            else "legacy-system:sample-task-results-download"
        ),
        created_at=item.last_modified,
        pinned=True,
    )


def _classify_authority(
    item: ObservedObject,
    *,
    artifacts_bucket: str,
    trajectories_bucket: str,
    sources: LegacySupplementalSources,
) -> LegacyAuthoritySeed:
    parts = _safe_parts(item.object_key)
    if parts is None or item.delete_marker:
        raise LegacyClassificationError(
            f"legacy supplemental object {_object_evidence_id(item)} has unsafe identity"
        )

    if item.bucket == trajectories_bucket:
        if len(parts) == 3 and parts[2] in {"atif.json", "events.jsonl"}:
            authority = sources.trials.get(parts[1])
            if authority is not None and str(authority.team_id) == parts[0]:
                return authority

    elif item.bucket == artifacts_bucket:
        if len(parts) >= 4 and parts[2] == "main":
            authority = sources.trials.get(parts[1])
            if authority is not None and str(authority.team_id) == parts[0]:
                return authority

        if len(parts) >= 5 and parts[0] == "delivery-exports":
            artifact = sources.artifacts.get(parts[3])
            batch = sources.batches.get(parts[2])
            if (
                artifact is not None
                and batch is not None
                and artifact.batch_id == parts[2]
                and str(artifact.authority.team_id) == parts[1]
                and str(batch.team_id) == parts[1]
            ):
                return artifact.authority

        if (
            len(parts) >= 4
            and parts[0] == "family-state"
            and _FAMILY_SNAPSHOT_RE.fullmatch(parts[-1]) is not None
        ):
            authority = sources.batches.get(parts[1])
            family_key = "/".join(parts[2:-1])
            if authority is not None and (parts[1], family_key) in sources.family_states:
                return authority

        if (
            len(parts) >= 5
            and parts[:2] == ("tasksets", "user")
            and _TASKSET_SLUG_RE.fullmatch(parts[3]) is not None
        ):
            authority = sources.task_sets.get((parts[2], parts[3]))
            if authority is not None and str(authority.team_id) == parts[2]:
                return authority

        if len(parts) >= 2 and parts[0] == "skillflow-iter-e2e":
            return _fixed_authority(item, catalog=True)

        if parts == ("downloads", "sample-tasks-results.zip"):
            return _fixed_authority(item, catalog=False)

    raise LegacyClassificationError(
        f"legacy supplemental object {_object_evidence_id(item)} is unclassified"
    )


def inspect_supplemental_object(
    item: ObservedObject,
    *,
    artifacts_bucket: str,
    trajectories_bucket: str,
    sources: LegacySupplementalSources,
    inspector: LegacyObjectInspector,
) -> tuple[LegacyAuthoritySeed, LegacySupplementalObject]:
    """Classify and hash one exact observed identity without prefix authority."""
    authority = _classify_authority(
        item,
        artifacts_bucket=artifacts_bucket,
        trajectories_bucket=trajectories_bucket,
        sources=sources,
    )
    if item.last_modified is None:
        raise LegacyClassificationError(
            f"legacy supplemental object {_object_evidence_id(item)} lacks creation authority"
        )
    observation = inspector.inspect(
        bucket=item.bucket,
        object_key=item.object_key,
        version_id=item.version_id,
    )
    if observation is None:
        raise LegacyClassificationError(
            f"legacy supplemental object {_object_evidence_id(item)} disappeared"
        )
    version_id, content_sha256, size_bytes = observation
    if version_id != item.version_id or size_bytes != item.size_bytes:
        raise LegacyClassificationError(
            f"legacy supplemental object {_object_evidence_id(item)} drifted"
        )
    return authority, LegacySupplementalObject(
        authority_data_class=authority.data_class,
        authority_owner_kind=authority.owner_kind,
        authority_owner_id=authority.owner_id,
        bucket=item.bucket,
        object_key=item.object_key,
        version_id=version_id,
        content_sha256=content_sha256,
        size_bytes=size_bytes,
        created_at=item.last_modified,
    )


__all__ = [
    "LegacyArtifactSource",
    "LegacySupplementalSources",
    "inspect_supplemental_object",
]
