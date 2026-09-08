"""Durable personal membership retries retain the original trusted observation."""

import json
from datetime import UTC, datetime
from importlib import import_module
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMembershipMutationV1,
    PersonalApplicationMembershipResponseV1,
    PersonalApplicationMembershipResultV1,
    PersonalApplicationMemberV1,
    PersonalMembershipCheckpointV1,
)
from tests.capacity_fixtures import development_projection
from tests.unit.test_capacity_manager_executable_allocator import execution_authority_fixture
from tests.unit.test_personal_dev_capacity import _projection_response

_DEFAULT_MEMBERSHIP_KEY = UUID(int=202)


def membership_envelope_values():
    projection = development_projection()
    execution = execution_authority_fixture()
    checkpoint = PersonalMembershipCheckpointV1(
        execution=execution,
        namespace_id=UUID(int=201),
        revision=0,
        head_sha256="0" * 64,
    )
    acknowledgement = SubjectExecutionAcknowledgementV2(
        subject_id=projection.subject_id,
        subject_incarnation=projection.subject_incarnation,
        configuration_generation=projection.configuration_generation,
        deployment_generation=projection.deployment_generation,
        candidate=CandidateBindingV2(
            algorithm="source-sha256",
            identity=projection.candidate_sha256,
            publication_sha256=projection.candidate_publication_sha256,
        ),
        reporter_incarnation=projection.demand_reporter_incarnation,
        protected_admission_sha256=projection.protected_admission_sha256,
        legacy_writer_high_water=0,
        acknowledgement_sha256="1" * 64,
    )
    request = PersonalApplicationMembershipMutationV1(
        execution=execution,
        namespace_id=checkpoint.namespace_id,
        expected_revision=0,
        projection=projection,
        acknowledgement=acknowledgement,
    )
    return {
        "mode": "membership-v1",
        "management_principal_id": "personal-membership-manager",
        "idempotency_key": UUID(int=202),
        "expected_checkpoint": checkpoint,
        "request": request,
        "request_sha256": canonical_digest(request),
        "observation": {
            "operation_id": projection.operation_id,
            "operation_epoch": projection.operation_epoch,
            "attempt_id": UUID(int=203),
            "observation_lease_epoch": 4,
            "observed_at": datetime(2026, 9, 8, tzinfo=UTC),
            "execution": execution,
            "local_activation_sha256": projection.local_activation_sha256,
            "capacity_agent_installation_sha256": projection.capacity_agent_installation_sha256,
            "acknowledgement": acknowledgement,
        },
    }


def membership_response(
    request,
    *,
    actor="personal-membership-manager",
    key=_DEFAULT_MEMBERSHIP_KEY,
    previous_head="0" * 64,
):
    subject = _projection_response(request.projection)["subject"]
    member = PersonalApplicationMemberV1.model_validate_json(
        json.dumps(
            {
                "revision": request.expected_revision + 1,
                "owner_id": str(request.projection.owner_id),
                "configuration": subject,
                "acknowledgement": request.acknowledgement.model_dump(mode="json"),
            }
        )
    )
    from loom_capacity_manager.membership_store import _head_digest

    head = _head_digest(
        actor=actor,
        execution_epoch=request.execution.execution_epoch,
        idempotency_key=key,
        operation_id=request.projection.operation_id,
        previous_sha256=previous_head,
        request_digest=canonical_digest(request),
        request_payload=request.model_dump(mode="json", exclude_none=False),
        member=member,
        revision=member.revision,
    )
    return PersonalApplicationMembershipResponseV1(
        checkpoint=PersonalMembershipCheckpointV1(
            execution=request.execution,
            namespace_id=request.namespace_id,
            revision=member.revision,
            head_sha256=head,
        ),
        result=PersonalApplicationMembershipResultV1(
            revision=member.revision,
            head_sha256=head,
            member=member,
            replayed=False,
        ),
    )


def test_membership_envelope_preserves_full_canonical_request_and_observation() -> None:
    module = import_module("loom.personal_dev_membership_checkpoint")
    value = module.PersonalDevMembershipEnvelopeV1.model_validate(membership_envelope_values())
    restored = module.PersonalDevMembershipEnvelopeV1.model_validate_json(canonical_bytes(value))
    assert canonical_bytes(restored.request) == canonical_bytes(value.request)
    assert restored.observation.observation_lease_epoch == 4
    assert restored.observation == value.observation
    assert restored.request_sha256 == canonical_digest(restored.request)
    assert b'reporter_token"' not in canonical_bytes(restored)
    assert restored.result is None


@pytest.mark.parametrize(
    "tamper",
    (
        "digest",
        "namespace",
        "revision",
        "authority",
        "base",
        "operation",
        "lease",
        "acknowledgement",
        "installation",
        "activation",
    ),
)
def test_membership_envelope_rejects_mixed_or_forged_bindings(tamper: str) -> None:
    module = import_module("loom.personal_dev_membership_checkpoint")
    values = membership_envelope_values()
    if tamper == "digest":
        values["request_sha256"] = "f" * 64
    elif tamper in {"namespace", "revision", "authority", "base"}:
        request = values["request"]
        if tamper == "namespace":
            request = request.model_copy(update={"namespace_id": UUID(int=999)})
        elif tamper == "revision":
            request = request.model_copy(update={"expected_revision": 1})
        elif tamper == "authority":
            request = request.model_copy(
                update={
                    "execution": request.execution.model_copy(
                        update={"execution_manifest_sha256": "f" * 64},
                    )
                }
            )
        else:
            request = request.model_copy(
                update={
                    "projection": request.projection.model_copy(
                        update={"expected_configuration_epoch": 99},
                    )
                }
            )
        values["request"] = request
        values["request_sha256"] = canonical_digest(request)
    else:
        observation = dict(values["observation"])
        field, changed = {
            "operation": ("operation_id", UUID(int=999)),
            "lease": ("observation_lease_epoch", 0),
            "acknowledgement": (
                "acknowledgement",
                values["request"].acknowledgement.model_copy(
                    update={"acknowledgement_sha256": "f" * 64},
                ),
            ),
            "installation": ("capacity_agent_installation_sha256", "a" * 64),
            "activation": ("local_activation_sha256", "a" * 64),
        }[tamper]
        observation[field] = changed
        values["observation"] = observation
    with pytest.raises(ValidationError):
        module.PersonalDevMembershipEnvelopeV1.model_validate(values)


def test_revision_refresh_changes_only_checkpoint_and_request_revision() -> None:
    module = import_module("loom.personal_dev_membership_checkpoint")
    original = module.PersonalDevMembershipEnvelopeV1.model_validate(membership_envelope_values())
    checkpoint = original.expected_checkpoint.model_copy(
        update={"revision": 7, "head_sha256": "7" * 64}
    )
    refreshed = module.refresh_membership_checkpoint(original, checkpoint)
    assert refreshed.observation == original.observation
    assert refreshed.idempotency_key == original.idempotency_key
    assert refreshed.request.projection == original.request.projection
    assert refreshed.request.acknowledgement == original.request.acknowledgement
    assert refreshed.request.expected_revision == 7
    assert refreshed.request_sha256 == canonical_digest(refreshed.request)
    with pytest.raises(ValueError):
        module.refresh_membership_checkpoint(refreshed, original.expected_checkpoint)
    with pytest.raises(ValueError):
        module.refresh_membership_checkpoint(
            original, checkpoint.model_copy(update={"namespace_id": UUID(int=999)})
        )


def test_exact_result_and_replay_bind_to_original_request() -> None:
    module = import_module("loom.personal_dev_membership_checkpoint")
    values = membership_envelope_values()
    response = membership_response(values["request"])
    accepted = module.PersonalDevMembershipEnvelopeV1.model_validate(values | {"result": response})
    assert accepted.result == response
    with pytest.raises(ValueError):
        module.refresh_membership_checkpoint(accepted, accepted.expected_checkpoint)
    wrong = response.model_copy(
        update={
            "checkpoint": response.checkpoint.model_copy(
                update={"namespace_id": UUID(int=999)},
            )
        }
    )
    with pytest.raises(ValidationError):
        module.PersonalDevMembershipEnvelopeV1.model_validate(values | {"result": wrong})


def test_existing_zero_idempotency_key_receipt_remains_valid() -> None:
    module = import_module("loom.personal_dev_membership_checkpoint")
    values = membership_envelope_values() | {"idempotency_key": UUID(int=0)}
    response = membership_response(values["request"], key=UUID(int=0))
    accepted = module.PersonalDevMembershipEnvelopeV1.model_validate(values | {"result": response})
    assert accepted.result == response


@pytest.mark.parametrize("outcome", ("committed", "terminal-not-committed"))
def test_historical_outcome_preserves_request_without_current_acceptance(outcome):
    from loom_capacity_manager.membership_outcomes import parse_membership_operation_outcome
    from tests.unit.test_personal_dev_membership_client import _outcome_payload

    module = import_module("loom.personal_dev_membership_checkpoint")
    envelope = module.PersonalDevMembershipEnvelopeV1.model_validate(membership_envelope_values())
    _, payload = _outcome_payload(envelope, outcome)
    historical = parse_membership_operation_outcome(json.dumps(payload))
    saved = module.PersonalDevMembershipEnvelopeV1.model_validate(
        envelope.model_dump(mode="python") | {"historical_outcome": historical}
    )
    assert saved.result is None
    assert canonical_bytes(saved.request) == canonical_bytes(envelope.request)
    assert saved.historical_outcome == historical
    with pytest.raises(ValueError):
        module.refresh_membership_checkpoint(
            saved,
            envelope.expected_checkpoint.model_copy(
                update={"revision": 1, "head_sha256": "a" * 64}
            ),
        )


@pytest.mark.parametrize("tamper", ("unresolved", "actor", "receipt", "current-result"))
def test_saved_historical_outcome_rejects_ambiguity_substitution_and_current_acceptance(tamper):
    from loom_capacity_manager.membership_outcomes import parse_membership_operation_outcome
    from tests.unit.test_personal_dev_membership_client import _outcome_payload

    module = import_module("loom.personal_dev_membership_checkpoint")
    envelope = module.PersonalDevMembershipEnvelopeV1.model_validate(membership_envelope_values())
    _, payload = _outcome_payload(envelope, "unresolved" if tamper == "unresolved" else "committed")
    if tamper == "actor":
        payload["original_actor"] = "different-delegate"
    if tamper == "receipt":
        payload["receipt"] = membership_response(
            envelope.request, actor="different-delegate"
        ).model_dump(mode="json")
    values = envelope.model_dump(mode="python") | {
        "historical_outcome": parse_membership_operation_outcome(json.dumps(payload))
    }
    if tamper == "current-result":
        values["result"] = membership_response(envelope.request)
    with pytest.raises(ValueError):
        module.PersonalDevMembershipEnvelopeV1.model_validate(values)


@pytest.mark.parametrize("tamper", ("operation", "token", "actor", "key", "previous-head"))
def test_receipt_event_hash_binds_full_request_and_original_checkpoint(tamper: str) -> None:
    module = import_module("loom.personal_dev_membership_checkpoint")
    values = membership_envelope_values()
    request = values["request"]
    kwargs = {}
    if tamper in {"operation", "token"}:
        field, changed = (
            ("operation_id", UUID(int=999))
            if tamper == "operation"
            else ("demand_reporter_token_sha256", "9" * 64)
        )
        request = request.model_copy(
            update={"projection": request.projection.model_copy(update={field: changed})}
        )
    else:
        kwargs = {
            "actor": {"actor": "other-principal"},
            "key": {"key": UUID(int=999)},
            "previous-head": {"previous_head": "9" * 64},
        }[tamper]
    receipt = membership_response(request, **kwargs)
    # Receipt is internally valid and its visible subject/acknowledgement match;
    # only authentication against the full saved event preimage detects it.
    assert receipt.result.member.acknowledgement == values["request"].acknowledgement
    with pytest.raises(ValidationError):
        module.PersonalDevMembershipEnvelopeV1.model_validate(values | {"result": receipt})
