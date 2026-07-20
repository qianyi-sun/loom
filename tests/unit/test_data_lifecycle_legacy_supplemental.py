from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from uuid import UUID

import pytest

from loom.data_lifecycle import DataClass, OwnerKind
from loom.data_lifecycle_gc import ObservedObject
from loom.data_lifecycle_legacy import LegacyAuthoritySeed, LegacyClassificationError
from loom.data_lifecycle_legacy_supplemental import (
    LegacyArtifactSource,
    LegacySupplementalSources,
    inspect_supplemental_object,
)

NOW = datetime(2026, 7, 19, 12, tzinfo=UTC)
TEAM = UUID("11111111-1111-1111-1111-111111111111")
TRIAL = UUID("22222222-2222-2222-2222-222222222222")
BATCH = UUID("33333333-3333-3333-3333-333333333333")
ARTIFACT = UUID("44444444-4444-4444-4444-444444444444")
ARTIFACTS = "loom-staging-artifacts"
TRAJECTORIES = "loom-staging-trajectories"


def _seed(
    data_class: DataClass,
    owner_kind: OwnerKind,
    owner_id: str,
    *,
    pinned: bool = False,
) -> LegacyAuthoritySeed:
    return LegacyAuthoritySeed(
        team_id=TEAM,
        data_class=data_class,
        owner_kind=owner_kind,
        owner_id=owner_id,
        created_at=NOW,
        pinned=pinned,
    )


TRIAL_SEED = _seed(DataClass.TRIAL, OwnerKind.TRIAL, str(TRIAL))
BATCH_SEED = _seed(DataClass.RUN, OwnerKind.BATCH, str(BATCH))
ARTIFACT_SEED = _seed(DataClass.ARTIFACT, OwnerKind.ARTIFACT, str(ARTIFACT))
TASKSET_SEED = _seed(
    DataClass.CATALOG,
    OwnerKind.SYSTEM,
    f"taskset:ts/{TEAM}/alpha",
    pinned=True,
)
SOURCES = LegacySupplementalSources(
    trials={str(TRIAL): TRIAL_SEED},
    batches={str(BATCH): BATCH_SEED},
    artifacts={
        str(ARTIFACT): LegacyArtifactSource(
            authority=ARTIFACT_SEED,
            batch_id=str(BATCH),
        )
    },
    task_sets={(str(TEAM), "alpha"): TASKSET_SEED},
    family_states=frozenset({(str(BATCH), "benchmark/family")}),
)


class _Inspector:
    def inspect(self, **kwargs: object) -> tuple[None, str, int]:
        assert kwargs["version_id"] is None
        return None, hashlib.sha256(b"body").hexdigest(), 4


def _observed(bucket: str, key: str, **kwargs: object) -> ObservedObject:
    return ObservedObject(
        bucket=bucket,
        object_key=key,
        version_id=None,
        size_bytes=4,
        last_modified=NOW,
        **kwargs,  # type: ignore[arg-type]
    )


@pytest.mark.parametrize(
    ("item", "authority"),
    (
        (
            _observed(TRAJECTORIES, f"{TEAM}/{TRIAL}/events.jsonl"),
            TRIAL_SEED,
        ),
        (
            _observed(TRAJECTORIES, f"{TEAM}/{TRIAL}/atif.json"),
            TRIAL_SEED,
        ),
        (
            _observed(ARTIFACTS, f"{TEAM}/{TRIAL}/main/report/output.json"),
            TRIAL_SEED,
        ),
        (
            _observed(
                ARTIFACTS,
                f"delivery-exports/{TEAM}/{BATCH}/{ARTIFACT}/archive.tar.gz.sha256",
            ),
            ARTIFACT_SEED,
        ),
        (
            _observed(
                ARTIFACTS,
                f"family-state/{BATCH}/benchmark/family/state-20260719t120000000000z.tar.gz",
            ),
            BATCH_SEED,
        ),
        (
            _observed(ARTIFACTS, f"tasksets/user/{TEAM}/alpha/tasks/one/task.toml"),
            TASKSET_SEED,
        ),
    ),
)
def test_exact_db_owned_layouts_bind_one_object(
    item: ObservedObject,
    authority: LegacyAuthoritySeed,
) -> None:
    actual_authority, object_item = inspect_supplemental_object(
        item,
        artifacts_bucket=ARTIFACTS,
        trajectories_bucket=TRAJECTORIES,
        sources=SOURCES,
        inspector=_Inspector(),
    )

    assert actual_authority == authority
    assert object_item.authority_key == (
        authority.data_class,
        authority.owner_kind,
        authority.owner_id,
    )
    assert object_item.identity == item.identity
    assert object_item.content_sha256 == hashlib.sha256(b"body").hexdigest()


@pytest.mark.parametrize(
    ("key", "data_class", "owner_id"),
    (
        ("skillflow-iter-e2e/task/instruction.md", DataClass.CATALOG, "legacy-catalog"),
        ("downloads/sample-tasks-results.zip", DataClass.SYSTEM, "legacy-system"),
    ),
)
def test_fixed_legacy_roots_are_pinned_not_gc_authority(
    key: str,
    data_class: DataClass,
    owner_id: str,
) -> None:
    authority, _object = inspect_supplemental_object(
        _observed(ARTIFACTS, key),
        artifacts_bucket=ARTIFACTS,
        trajectories_bucket=TRAJECTORIES,
        sources=SOURCES,
        inspector=_Inspector(),
    )

    assert authority.data_class is data_class
    assert authority.owner_kind is OwnerKind.SYSTEM
    assert authority.owner_id.startswith(owner_id)
    assert authority.team_id is None
    assert authority.pinned is True


@pytest.mark.parametrize(
    "item",
    (
        _observed(TRAJECTORIES, f"{UUID(int=9)}/{TRIAL}/events.jsonl"),
        _observed(TRAJECTORIES, f"{TEAM}/{TRIAL}/unknown.json"),
        _observed(ARTIFACTS, f"{TEAM}/{TRIAL}/main/../escape"),
        _observed(ARTIFACTS, f"family-state/{BATCH}/other/state-20260719t120000000000z.tar.gz"),
        _observed(ARTIFACTS, f"tasksets/user/{TEAM}/unknown/manifest.yaml"),
        _observed(ARTIFACTS, "unknown/prefix"),
        _observed(ARTIFACTS, "skillflow-iter-e2e/file", delete_marker=True),
    ),
)
def test_unknown_cross_team_or_unsafe_layouts_fail_closed(item: ObservedObject) -> None:
    with pytest.raises(LegacyClassificationError):
        inspect_supplemental_object(
            item,
            artifacts_bucket=ARTIFACTS,
            trajectories_bucket=TRAJECTORIES,
            sources=SOURCES,
            inspector=_Inspector(),
        )


def test_observed_identity_must_remain_stable_during_hash() -> None:
    class Drifted:
        def inspect(self, **_kwargs: object) -> tuple[None, str, int]:
            return None, "f" * 64, 5

    with pytest.raises(LegacyClassificationError, match="drifted"):
        inspect_supplemental_object(
            _observed(TRAJECTORIES, f"{TEAM}/{TRIAL}/atif.json"),
            artifacts_bucket=ARTIFACTS,
            trajectories_bucket=TRAJECTORIES,
            sources=SOURCES,
            inspector=Drifted(),
        )
