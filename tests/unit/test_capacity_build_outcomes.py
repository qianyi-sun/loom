"""Outcome evidence is bounded, retry-specific, and not verified success."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom_capacity_agent.build_admission import (
    BuildArtifactV1,
    BuildClaimRequestV1,
    BuildOutcomeReceiptV1,
    BuildOutcomeRequestV1,
    native_build_artifact_key,
)
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_capacity_build_admission_client import native_registration


def claim_request():
    worker = native_registration()
    return BuildClaimRequestV1(binding=worker.binding, operation_id=uuid4(), request_id=uuid4(),
        worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation)


@pytest.mark.parametrize("boundary", ["success", "missing-artifact", "failure-artifact", "zero-size", "bool-size", "overflow-size", "digest", "extra"])
def test_outcome_rejects_invalid_or_ambiguous_archive_evidence(boundary):
    request = BuildOutcomeRequestV1(claim=claim_request(), operation_id=uuid4(), result="artifact-ready",
        artifact=BuildArtifactV1(archive_sha256="a" * 64, archive_size_bytes=1024))
    payload = request.model_dump(mode="json")
    if boundary == "success":
        payload["result"] = "success"
    elif boundary == "missing-artifact":
        payload["artifact"] = None
    elif boundary == "failure-artifact":
        payload["result"] = "failed"
    elif boundary == "extra":
        payload["artifact"]["url"] = "https://foreign.invalid/artifact.tar"
    else:
        key, value = ("archive_sha256", "z" * 64) if boundary == "digest" else ("archive_size_bytes", {
            "zero-size": 0, "bool-size": True, "overflow-size": 2**63}[boundary])
        payload["artifact"][key] = value
    import json

    with pytest.raises(ValidationError):
        BuildOutcomeRequestV1.model_validate_json(json.dumps(payload))


def test_artifact_identity_is_claim_specific_and_receipt_is_not_executable():
    claim = claim_request()
    key = native_build_artifact_key(claim)
    assert str(claim.request_id) in key and str(claim.binding.intent_id) in key and str(claim.operation_id) in key
    assert key != native_build_artifact_key(claim.model_copy(update={"operation_id": uuid4()}))
    assert key != native_build_artifact_key(claim.model_copy(update={"binding": claim.binding.model_copy(update={"intent_id": uuid4()})}))
    request = BuildOutcomeRequestV1(claim=claim, operation_id=uuid4(), result="failed")
    receipt = BuildOutcomeReceiptV1(request=request, request_digest=canonical_digest(request))
    assert not receipt.executable and receipt.live_claim_count == 0 and receipt.claim_high_water == 1
    assert BuildOutcomeReceiptV1.model_validate_json(canonical_bytes(receipt)) == receipt
