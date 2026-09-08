"""Only an exact authenticated release can be persisted for membership teardown."""

import json
from importlib import import_module

import pytest

from loom.personal_dev_membership_checkpoint import PersonalDevMembershipEnvelopeV1
from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_manager.membership_outcomes import parse_membership_operation_outcome
from tests.unit.test_personal_dev_membership_client import _outcome_payload
from tests.unit.test_personal_dev_membership_subject_client import _accepted, _response


@pytest.mark.parametrize("historical", (False, True))
async def test_exact_release_roundtrips_with_original_current_or_historical_destroy(historical):
    envelope = _accepted()
    _, payload = _response(envelope, "verified")
    if historical:
        _, outcome = _outcome_payload(envelope, "committed")
        envelope = envelope.model_copy(
            update={
                "result": None,
                "historical_outcome": parse_membership_operation_outcome(json.dumps(outcome)),
            }
        )
    accepted = PersonalDevMembershipEnvelopeV1.model_validate_json(
        json.dumps(envelope.model_dump(mode="json") | {"release": payload})
    )
    assert accepted.release.model_dump(mode="json") == payload
    assert PersonalDevMembershipEnvelopeV1.model_validate_json(canonical_bytes(accepted)) == accepted
    assert accepted.result == envelope.result
    assert accepted.historical_outcome == envelope.historical_outcome


@pytest.mark.parametrize("tamper", ("query", "receipt", "pending", "missing_commit", "charged"))
def test_release_rejects_wrong_binding_or_unreleased_work(tamper):
    module = import_module("loom.personal_dev_membership_checkpoint")
    envelope = _accepted()
    _, payload = _response(envelope, "pending" if tamper == "pending" else "verified")
    if tamper == "query":
        payload["query_sha256"] = "f" * 64
    elif tamper == "receipt":
        payload["membership_receipt"]["result"]["head_sha256"] = "f" * 64
    elif tamper == "missing_commit":
        envelope = envelope.model_copy(update={"result": None})
    elif tamper == "charged":
        payload["incarnation_work"]["observed_commitments"] = 1
    assert hasattr(module, "validate_membership_release")
    with pytest.raises(ValueError):
        PersonalDevMembershipEnvelopeV1.model_validate_json(
            json.dumps(envelope.model_dump(mode="json") | {"release": payload})
        )
