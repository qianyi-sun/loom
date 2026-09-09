from __future__ import annotations

import json
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_task_image_authority.contracts import (
    TaskImageBaseResolutionEvidenceV1,
    TaskImagePublicationCandidateRequestV1,
    TaskImagePublicationCandidateRequestV2,
)
from loom_task_image_authority.http_contracts import (
    TaskImagePublicationCandidateResponseV1,
    TaskImagePublicationCandidateResponseV2,
)

ROOT = "sha256:" + "a" * 64
BASE = "sha256:" + "b" * 64


def _evidence(**changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "schema": "loom.task-image-base-resolution/v1",
        "solve_ref": "solve_1-abc",
        "platform": "linux/arm64",
        "output_digest": ROOT,
        "observed_base_digests": [],
    }
    values.update(changes)
    return values


def _candidate(**changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "schema_version": 2,
        "operation_id": UUID("11111111-1111-4111-8111-111111111111"),
        "grant_id": UUID("22222222-2222-4222-8222-222222222222"),
        "session_id": UUID("33333333-3333-4333-8333-333333333333"),
        "session_generation": 1,
        "session_token": "loom_tibs_" + "x" * 64,
        "materialization_id": UUID("44444444-4444-4444-8444-444444444444"),
        "attempt_id": UUID("55555555-5555-4555-8555-555555555555"),
        "lease_epoch": 1,
        "credential_id": UUID("66666666-6666-4666-8666-666666666666"),
        "credential_generation": 1,
        "component": "task",
        "manifest_digest": ROOT,
        "manifest_size": 512,
        "oci_file_sha256": "c" * 64,
        "oci_file_size": 4096,
        "platform": "linux/arm64",
        "base_resolution": _evidence(),
    }
    values.update(changes)
    return values


@pytest.mark.parametrize("bases", [[], [ROOT, BASE]])
def test_base_record_roundtrips_exact_wire_and_owns_immutable_observations(
    bases: list[str],
) -> None:
    payload = _evidence(observed_base_digests=bases)
    evidence = TaskImageBaseResolutionEvidenceV1.model_validate(payload)
    expected = dict(payload, observed_base_digests=list(bases))
    bases.append("sha256:" + "d" * 64)
    assert json.loads(evidence.model_dump_json()) == expected
    assert isinstance(evidence.observed_base_digests, tuple)
    with pytest.raises(ValidationError):
        evidence.solve_ref = "replaced"


@pytest.mark.parametrize(
    "changes",
    [
        {"schema": "loom.task-image-base-resolution/v2"},
        {"schema_version": 1},
        {"schema_name": "loom.task-image-base-resolution/v1"},
        {"solve_ref": ""},
        {"solve_ref": "a" * 129},
        {"solve_ref": "solve\n"},
        {"solve_ref": "../solve"},
        {"solve_ref": "sölve"},
        {"platform": "linux/amd64,linux/arm64"},
        {"output_digest": "sha256:" + "0" * 64},
        {"observed_base_digests": None},
        {"observed_base_digests": {}},
        {"observed_base_digests": [None]},
        {"observed_base_digests": ["sha256:" + "0" * 64]},
        {"observed_base_digests": ["sha256:" + "A" * 64]},
        {"observed_base_digests": [ROOT, ROOT]},
        {"observed_base_digests": [BASE, ROOT]},
        {"observed_base_digests": [f"sha256:{i:064x}" for i in range(1, 130)]},
    ],
)
def test_base_record_rejects_malformed_or_ambiguous_evidence(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        TaskImageBaseResolutionEvidenceV1.model_validate(_evidence(**changes))


@pytest.mark.parametrize("field", list(_evidence()))
def test_base_record_never_defaults_missing_fields(field: str) -> None:
    payload = _evidence()
    del payload[field]
    with pytest.raises(ValidationError):
        TaskImageBaseResolutionEvidenceV1.model_validate(payload)


def test_base_record_accepts_exact_bounds_and_both_native_platforms() -> None:
    bases = [f"sha256:{i:064x}" for i in range(1, 129)]
    for platform in ("linux/amd64", "linux/arm64"):
        evidence = TaskImageBaseResolutionEvidenceV1.model_validate(
            _evidence(solve_ref="a" * 128, platform=platform, observed_base_digests=bases)
        )
        assert evidence.observed_base_digests == tuple(bases)


def test_candidate_v2_keeps_metadata_and_rejects_v1_downgrade_or_upgrade() -> None:
    request = TaskImagePublicationCandidateRequestV2.model_validate(_candidate())
    assert request.public_binding()["base_resolution"] == _evidence()
    with pytest.raises(ValidationError):
        TaskImagePublicationCandidateRequestV1.model_validate(request.model_dump())
    legacy = _candidate(schema_version=1)
    del legacy["base_resolution"]
    TaskImagePublicationCandidateRequestV1.model_validate(legacy)
    with pytest.raises(ValidationError):
        TaskImagePublicationCandidateRequestV2.model_validate(legacy)


@pytest.mark.parametrize(
    "evidence",
    [None, _evidence(platform="linux/amd64"), _evidence(output_digest=BASE)],
)
def test_candidate_v2_rejects_missing_or_substituted_metadata(evidence: object) -> None:
    with pytest.raises(ValidationError):
        TaskImagePublicationCandidateRequestV2.model_validate(_candidate(base_resolution=evidence))


def test_candidate_v2_revalidates_constructed_nested_evidence() -> None:
    record = TaskImageBaseResolutionEvidenceV1.model_validate(_evidence())
    forged = record.model_copy(update={"observed_base_digests": ("invalid",)})
    with pytest.raises(ValidationError):
        TaskImagePublicationCandidateRequestV2.model_validate(_candidate(base_resolution=forged))
    missing = _candidate()
    del missing["base_resolution"]
    with pytest.raises(ValidationError):
        TaskImagePublicationCandidateRequestV2.model_validate(missing)


def test_candidate_v2_accepts_revalidated_evidence_instances() -> None:
    record = TaskImageBaseResolutionEvidenceV1.model_validate(_evidence())
    assert TaskImageBaseResolutionEvidenceV1.model_validate(record) == record
    request = TaskImagePublicationCandidateRequestV2.model_validate(
        _candidate(base_resolution=record)
    )
    assert request.public_binding()["base_resolution"] == _evidence()


@pytest.mark.parametrize("version", [None, 1, 2.0, "2", True])
def test_candidate_v2_requires_explicit_integer_version(version: object) -> None:
    payload = _candidate(schema_version=version)
    if version is None:
        del payload["schema_version"]
    with pytest.raises(ValidationError):
        TaskImagePublicationCandidateRequestV2.model_validate(payload)


def test_v2_acknowledgement_preserves_evidence_and_rejects_substitution() -> None:
    request = TaskImagePublicationCandidateRequestV2.model_validate(_candidate())
    payload = request.model_dump(mode="json", exclude={"session_token"})
    payload.update(
        schema_version="loom.task-image-publication-candidate.v2",
        candidate_id="77777777-7777-4777-8777-777777777777",
        attempt_number=1,
        builder_id="rootless:" + "e" * 32,
        repository="loom-task-image-attempts/arm64/55555555-5555-4555-8555-555555555555/task",
        recorded_at="2026-09-05T12:00:00Z",
    )
    response = TaskImagePublicationCandidateResponseV2.model_validate_json(json.dumps(payload))
    assert json.loads(response.model_dump_json())["base_resolution"] == _evidence()
    with pytest.raises(ValidationError):
        TaskImagePublicationCandidateResponseV1.model_validate_json(response.model_dump_json())
    missing_version = dict(payload)
    del missing_version["schema_version"]
    with pytest.raises(ValidationError):
        TaskImagePublicationCandidateResponseV2.model_validate_json(json.dumps(missing_version))
    payload["base_resolution"] = _evidence(output_digest=BASE)
    with pytest.raises(ValidationError):
        TaskImagePublicationCandidateResponseV2.model_validate_json(json.dumps(payload))
