from __future__ import annotations

import pytest

from loom.pipeline.artifact_validators import (
    PipelineCoreArtifactV1,
    validate_official_artifact_document,
)
from loom.pipeline.keys import canonical_digest
from loom_worker.pipeline_attempt_workspace import parse_attempt_complete

DIGEST = "sha256:" + "a" * 64


def _core_artifact(schema_version: str = "loom.pipeline-core-seed.v1") -> dict[str, object]:
    value = {"items": ["one", "two"]}
    return {
        "schema_version": schema_version,
        "payload": {"value": value, "value_sha256": canonical_digest(value)},
        "files": [],
    }


def test_official_registry_validates_exact_type_and_payload_digest() -> None:
    result = validate_official_artifact_document("loom.pipeline-core-seed.v1", _core_artifact())
    assert isinstance(result, PipelineCoreArtifactV1)

    drift = _core_artifact()
    payload = drift["payload"]
    assert isinstance(payload, dict)
    payload["value_sha256"] = DIGEST
    with pytest.raises(ValueError, match="payload digest drift"):
        validate_official_artifact_document("loom.pipeline-core-seed.v1", drift)


def test_official_registry_rejects_unknown_type_schema_drift_and_secret_literals() -> None:
    with pytest.raises(ValueError, match="no installed official validator"):
        validate_official_artifact_document("example.unknown.v1", _core_artifact())

    with pytest.raises(ValueError, match="does not match"):
        validate_official_artifact_document("loom.pipeline-core-item.v1", _core_artifact())

    secret = _core_artifact()
    payload = secret["payload"]
    assert isinstance(payload, dict)
    payload["value"] = {"text": "Bearer abcdefghijklmnopqrstuvwxyz"}
    payload["value_sha256"] = canonical_digest(payload["value"])
    with pytest.raises(ValueError, match="secret-looking literal"):
        validate_official_artifact_document("loom.pipeline-core-seed.v1", secret)


def test_legacy_behavior_attempt_marker_is_read_while_new_markers_use_loom_schema() -> None:
    marker = parse_attempt_complete(
        {
            "schema_version": "behavior.attempt-complete.v1",
            "idempotency_key": "attempt-key",
            "stage_result_sha256": DIGEST,
            "outputs": [],
        }
    )
    assert marker.schema_version == "behavior.attempt-complete.v1"
    assert marker.idempotency_key == "attempt-key"

    current = parse_attempt_complete(
        marker.model_dump(mode="json") | {"schema_version": "loom.attempt-complete.v1"}
    )
    assert current.schema_version == "loom.attempt-complete.v1"
