"""Canonical bundle readers retain attempt identity and reject unsafe inventories."""

from __future__ import annotations

from uuid import uuid4

import pytest

from loom.db.schema import Artifact, Trial
from loom_service.delivery_export_errors import InvalidDeliveryBatchFamilyError
from loom_service.trial_bundles import (
    canonical_bundle_from_artifact,
    canonical_trial_bundle_manifest,
)


@pytest.fixture
def bundle_records() -> tuple[Artifact, Trial]:
    trial = Trial(id=uuid4(), task_id="task-one", attempt_count=2)
    artifact = Artifact(
        id=uuid4(),
        trial_id=trial.id,
        manifest_sha256="sha256:" + "a" * 64,
        content_hash="sha256:" + "b" * 64,
        storage={
            "schema_version": "loom.canonical-trial-bundle-storage.v1",
            "attempt": 2,
            "files": [
                {
                    "relative_path": "result.json",
                    "bucket": "artifacts",
                    "key": "trial/result",
                    "size_bytes": 2,
                    "sha256": "sha256:" + "c" * 64,
                    "media_type": "application/json",
                }
            ],
            "source_evidence": [
                {
                    "relative_path": "source/_manifest.json",
                    "bucket": "artifacts",
                    "key": "trial/source",
                    "size_bytes": 3,
                    "sha256": "sha256:" + "a" * 64,
                    "media_type": "application/json",
                }
            ],
        },
    )
    return artifact, trial


def test_bundle_preserves_attempt_and_separates_outputs_from_source(
    bundle_records: tuple[Artifact, Trial],
) -> None:
    artifact, trial = bundle_records
    bundle = canonical_bundle_from_artifact(artifact, trial=trial)
    assert bundle is not None
    assert bundle.attempt == 2
    assert bundle.trial_id == trial.id
    assert bundle.artifact_id == artifact.id
    assert [file.relative_path for file in bundle.files] == [
        "files/result.json",
        "source/_manifest.json",
    ]
    manifest = canonical_trial_bundle_manifest(bundle)
    assert manifest["manifest_sha256"] == "sha256:" + "a" * 64
    assert manifest["content_sha256"] == "sha256:" + "b" * 64
    assert manifest["files"][0] == {
        "relative_path": "files/result.json",
        "size_bytes": 2,
        "sha256": "sha256:" + "c" * 64,
        "media_type": "application/json",
    }


@pytest.mark.parametrize("mismatch", ["trial", "attempt", "schema"])
def test_unrelated_bundle_is_not_selected(
    bundle_records: tuple[Artifact, Trial], mismatch: str
) -> None:
    artifact, trial = bundle_records
    if mismatch == "trial":
        artifact.trial_id = uuid4()
    elif mismatch == "attempt":
        artifact.storage["attempt"] = 1
    else:
        artifact.storage["schema_version"] = "other"
    assert canonical_bundle_from_artifact(artifact, trial=trial) is None


def test_historical_manifest_digest_uses_retained_source_evidence(
    bundle_records: tuple[Artifact, Trial],
) -> None:
    artifact, trial = bundle_records
    artifact.manifest_sha256 = None
    bundle = canonical_bundle_from_artifact(artifact, trial=trial)
    assert bundle is not None
    assert bundle.manifest_sha256 == "sha256:" + "a" * 64


@pytest.mark.parametrize("invalid_path", ["../secret", "..\\secret", "/absolute"])
def test_bundle_rejects_unsafe_source_paths(
    bundle_records: tuple[Artifact, Trial], invalid_path: str
) -> None:
    artifact, trial = bundle_records
    artifact.storage["source_evidence"][0]["relative_path"] = invalid_path
    with pytest.raises(InvalidDeliveryBatchFamilyError) as error:
        canonical_bundle_from_artifact(artifact, trial=trial)
    assert error.value.status_code == 400
    assert error.value.detail["code"] == "delivery_export_invalid_batch_family"


@pytest.mark.parametrize(
    "corruption", ["digest", "missing_inventory", "duplicate_path", "boolean_size"]
)
def test_bundle_rejects_corrupt_file_identity(
    bundle_records: tuple[Artifact, Trial], corruption: str
) -> None:
    artifact, trial = bundle_records
    if corruption == "digest":
        artifact.storage["files"][0]["sha256"] = "invalid"
    elif corruption == "missing_inventory":
        del artifact.storage["source_evidence"]
    elif corruption == "duplicate_path":
        artifact.storage["files"].append(dict(artifact.storage["files"][0]))
    else:
        artifact.storage["files"][0]["size_bytes"] = True
    with pytest.raises(InvalidDeliveryBatchFamilyError):
        canonical_bundle_from_artifact(artifact, trial=trial)
