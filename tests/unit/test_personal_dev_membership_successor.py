"""A historical outcome may continue only through exact reviewed current adoption."""

import json
from dataclasses import replace
from datetime import timedelta
from importlib import import_module
from uuid import uuid4

import pytest

from loom.personal_dev_environment import (
    PersonalDevEnvironmentApplyRequest,
    PersonalDevEnvironmentDestroyRequest,
)
from loom.personal_dev_membership_checkpoint import PersonalDevMembershipEnvelopeV1
from loom_capacity_manager.contracts import (
    ConfigurationGenerationRefV1,
    ConfigurationSnapshotV1,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.membership_contracts import ExecutionPreparationV3
from tests.unit.test_personal_dev_membership_admission import admission_values
from tests.unit.test_personal_dev_membership_checkpoint import membership_response
from tests.unit.test_personal_dev_membership_reconciler import _NOW, _pending_claim
from tests.unit.test_personal_dev_membership_recovery import _Observer


def _intent(operation):
    owner = dict(
        name=operation.environment_name,
        owner_user_id=operation.owner_user_id,
        owner_team_id=operation.owner_team_id,
        expected_operation_epoch=operation.expected_operation_epoch,
        idempotency_key=operation.idempotency_key,
    )
    if operation.kind == "destroy":
        return PersonalDevEnvironmentDestroyRequest(**owner, keep_data=operation.keep_data)
    return PersonalDevEnvironmentApplyRequest(
        **owner, candidate_id=operation.candidate_id, candidate_sha=operation.candidate_sha,
        min_slots=operation.min_slots, max_slots=operation.max_slots,
    )


def _persisted_evidence(operation):
    projection = operation.capacity_membership_envelope.request.projection
    return replace(
        operation,
        request_sha256=_intent(operation).request_sha256,
        capacity_reporter_incarnation=projection.demand_reporter_incarnation,
        capacity_reporter_token_sha256=projection.demand_reporter_token_sha256,
        local_activation_sha256=projection.local_activation_sha256,
        protected_admission_sha256=projection.protected_admission_sha256,
        capacity_agent_installation_sha256=projection.capacity_agent_installation_sha256,
        capacity_supported_pool_ids=projection.supported_pool_ids,
        capacity_supported_architectures=projection.supported_architectures,
    )


def successor_case(kind="create", outcome="committed"):
    original = _pending_claim()
    first_envelope = original.operation.capacity_membership_envelope
    receipt = membership_response(first_envelope.request, key=first_envelope.idempotency_key)
    accepted_envelope = PersonalDevMembershipEnvelopeV1.model_validate(
        first_envelope.model_dump(mode="python") | {"result": receipt}
    )
    accepted = replace(
        original.operation,
        state="succeeded",
        checkpoint="complete",
        capacity_membership_envelope=accepted_envelope,
    )
    if kind != "create":
        operation_id, attempt_id, key = uuid4(), uuid4(), uuid4()
        generation = 2 if kind == "update" else 1
        projection = first_envelope.request.projection.model_copy(
            update={
                "operation_id": operation_id,
                "operation_epoch": 2,
                "configuration_generation": 2,
                "operation_kind": kind,
                "min_slots": 0,
                "max_slots": 0 if kind == "destroy" else original.operation.max_slots,
                "candidate_generation": generation,
                "deployment_generation": generation,
                "demand_reporter_incarnation": uuid4()
                if kind == "update"
                else first_envelope.request.projection.demand_reporter_incarnation,
            }
        )
        ack = first_envelope.request.acknowledgement.model_copy(
            update={
                "configuration_generation": 2,
                "deployment_generation": generation,
                "reporter_incarnation": projection.demand_reporter_incarnation,
            }
        )
        request = first_envelope.request.model_copy(
            update={"projection": projection, "acknowledgement": ack}
        )
        envelope = PersonalDevMembershipEnvelopeV1.model_validate(
            first_envelope.model_dump(mode="python")
            | {
                "idempotency_key": key,
                "request": request,
                "request_sha256": canonical_digest(request),
                "observation": first_envelope.observation.model_copy(
                    update={
                        "operation_id": operation_id,
                        "operation_epoch": 2,
                        "attempt_id": attempt_id,
                        "acknowledgement": ack,
                    }
                ),
            }
        )
        original = replace(
            original,
            operation=replace(
                original.operation,
                id=operation_id,
                idempotency_key=key,
                operation_epoch=2,
                expected_operation_epoch=1,
                kind=kind,
                deployment_generation=generation,
                attempt_id=attempt_id,
                capacity_membership_envelope=envelope,
            ),
            attempt=replace(
                original.attempt, id=attempt_id, operation_id=operation_id, operation_epoch=2
            ),
            environment=replace(
                original.environment,
                operation_id=operation_id,
                operation_epoch=2,
                accepted_capacity_mode="membership-v1",
                accepted_capacity_membership_checkpoint=receipt.checkpoint,
            ),
        )
    else:
        envelope = first_envelope
        original = replace(
            original,
            environment=replace(
                original.environment,
                ready_at=None,
                accepted_capacity_mode="shadow-v1",
                accepted_capacity_membership_checkpoint=None,
                capacity_configuration_epoch=None,
                capacity_configuration_sha256=None,
            ),
        )
    historical = _Observer(envelope, outcome).outcome
    envelope = PersonalDevMembershipEnvelopeV1.model_validate(
        envelope.model_dump(mode="python") | {"historical_outcome": historical}
    )
    original = replace(
        original,
        operation=replace(
            original.operation,
            checkpoint="membership_outcome_resolved",
            capacity_membership_envelope=envelope,
        ),
        attempt=replace(original.attempt, checkpoint="membership_outcome_resolved"),
    )
    member = (
        historical.receipt.result.member
        if outcome == "committed"
        else receipt.result.member
        if kind != "create"
        else None
    )
    authority = admission_values()
    prep = authority["preparation"]
    prep["subject_acknowledgements"] = (
        [] if member is None else [member.acknowledgement.model_dump(mode="json")]
    )
    prep["personal_membership"]["managed_base_subject_ids"] = (
        [] if member is None else [str(member.configuration.subject_id)]
    )
    preparation = ExecutionPreparationV3.model_validate_json(json.dumps(prep))
    authority["preparation"] = preparation.model_dump(mode="json")
    authority["execution"]["execution_manifest_sha256"] = canonical_executable_digest(preparation)
    authority["execution"]["execution_epoch"] = 99
    configuration = ConfigurationSnapshotV1(
        configuration_epoch=preparation.configuration_epoch,
        fleet=ConfigurationGenerationRefV1(
            scope="fleet", generation=preparation.fleet_generation, digest=preparation.fleet_digest
        ),
        subjects=()
        if member is None
        else (
            ConfigurationGenerationRefV1(
                scope="subject",
                generation=member.configuration.configuration_generation,
                digest=canonical_digest(member.configuration),
                subject_id=member.configuration.subject_id,
                subject_incarnation=member.configuration.subject_incarnation,
            ),
        ),
    )
    op = original.operation
    owner = dict(
        name=op.environment_name,
        owner_user_id=op.owner_user_id,
        owner_team_id=op.owner_team_id,
        expected_operation_epoch=op.expected_operation_epoch,
        idempotency_key=op.idempotency_key,
    )
    request = (
        PersonalDevEnvironmentDestroyRequest(**owner, keep_data=op.keep_data)
        if kind == "destroy"
        else PersonalDevEnvironmentApplyRequest(
            **owner,
            candidate_id=op.candidate_id,
            candidate_sha=op.candidate_sha,
            min_slots=op.min_slots,
            max_slots=op.max_slots,
        )
    )
    original = replace(original, operation=replace(op, request_sha256=request.request_sha256))
    original = replace(original, operation=_persisted_evidence(original.operation))
    accepted = _persisted_evidence(accepted)
    return (
        original,
        accepted if kind != "create" else None,
        {
            "reviewed_at": (_NOW - timedelta(minutes=1)).isoformat(),
            "expires_at": (_NOW + timedelta(hours=1)).isoformat(),
            "authority": authority,
            "current_configuration": configuration.model_dump(mode="json"),
            "predecessor_operation_id": str(original.operation.id),
            "predecessor_envelope_sha256": canonical_digest(envelope),
            "owner_team_id": str(original.operation.owner_team_id),
            "request_sha256": original.operation.request_sha256,
            "adopted_member": None if member is None else member.model_dump(mode="json"),
        },
    )


@pytest.mark.parametrize(
    "kind,outcome,effective",
    (
        ("create", "committed", "update"),
        ("update", "committed", "update"),
        ("capacity", "committed", "update"),
        ("create", "terminal-not-committed", "create"),
        ("update", "terminal-not-committed", "update"),
        ("capacity", "terminal-not-committed", "update"),
        ("destroy", "terminal-not-committed", "destroy"),
    ),
)
def test_reviewed_successor_preserves_history_and_selects_ordinary_mutation(
    kind, outcome, effective
):
    module = import_module("loom.personal_dev_membership_successor")
    claim, accepted, values = successor_case(kind, outcome)
    binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    before = canonical_bytes(claim.operation.capacity_membership_envelope)
    decision = module.validate_membership_successor(
        binding, claim=claim, accepted_operation=accepted, now=_NOW
    )
    assert decision.kind == effective
    assert decision.operation_epoch == claim.operation.operation_epoch + 1
    assert decision.deployment_generation == claim.operation.deployment_generation + (
        0 if effective == "destroy" else 1
    )
    assert canonical_bytes(claim.operation.capacity_membership_envelope) == before


@pytest.mark.parametrize(
    "change",
    (
        "missing-adoption",
        "wrong-team",
        "wrong-request",
        "wrong-envelope",
        "wrong-operation",
        "expired",
        "unresolved",
        "missing-accepted",
    ),
)
def test_successor_rejects_drift_before_changing_any_lifecycle_identity(change):
    module = import_module("loom.personal_dev_membership_successor")
    claim, accepted, values = successor_case("update", "terminal-not-committed")
    now = _NOW
    if change == "missing-adoption":
        values["adopted_member"] = None
    elif change == "wrong-team":
        values["owner_team_id"] = str(uuid4())
    elif change == "wrong-request":
        values["request_sha256"] = "f" * 64
    elif change == "wrong-envelope":
        values["predecessor_envelope_sha256"] = "f" * 64
    elif change == "wrong-operation":
        values["predecessor_operation_id"] = str(uuid4())
    elif change == "expired":
        now += timedelta(days=2)
    elif change == "unresolved":
        claim = replace(
            claim, operation=replace(claim.operation, checkpoint="capacity_projection_pending")
        )
    else:
        accepted = None
    with pytest.raises(ValueError):
        binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(
            json.dumps(values)
        )
        module.validate_membership_successor(
            binding, claim=claim, accepted_operation=accepted, now=now
        )


def test_committed_destroy_uses_existing_release_recovery_not_successor():
    module = import_module("loom.personal_dev_membership_successor")
    claim, accepted, values = successor_case("destroy", "committed")
    with pytest.raises(ValueError):
        binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(
            json.dumps(values)
        )
        module.validate_membership_successor(
            binding, claim=claim, accepted_operation=accepted, now=_NOW
        )


@pytest.mark.parametrize(
    "field,value",
    (
        ("candidate_id", uuid4()),
        ("min_slots", 1),
        ("max_slots", 3),
        ("expected_operation_epoch", 10),
    ),
)
def test_successor_rejects_local_target_drift_even_with_original_saved_digest(field, value):
    module = import_module("loom.personal_dev_membership_successor")
    claim, accepted, values = successor_case()
    binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    claim = replace(claim, operation=replace(claim.operation, **{field: value}))
    with pytest.raises(ValueError, match="intent"):
        module.validate_membership_successor(
            binding, claim=claim, accepted_operation=accepted, now=_NOW
        )


@pytest.mark.parametrize("change", ("none", "digest", "version", "duplicate", "oversize"))
def test_successor_parser_requires_exact_canonical_reviewed_binding(change):
    module = import_module("loom.personal_dev_membership_successor")
    _, _, values = successor_case()
    binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    wire = canonical_bytes(binding)
    digest = canonical_digest(binding)
    if change == "digest":
        digest = "f" * 64
    elif change == "version":
        wire = wire.replace(b'"schema_version":1', b'"schema_version":1.0')
    elif change == "duplicate":
        wire = b'{"schema_version":1,' + wire[1:]
    elif change == "oversize":
        wire = b" " * (module.MAX_CONTRACT_BYTES + 1)
    if change == "none":
        assert (
            module.parse_membership_successor_binding(wire, expected_binding_sha256=digest)
            == binding
        )
    else:
        with pytest.raises(ValueError):
            module.parse_membership_successor_binding(wire, expected_binding_sha256=digest)


@pytest.mark.parametrize("kind", ("update", "capacity", "destroy"))
@pytest.mark.parametrize("tamper", (None, "snapshot", "projection", "accepted-mode"))
def test_shadow_adoption_requires_full_exact_retained_projection_and_snapshot(kind, tamper):
    module = import_module("loom.personal_dev_membership_successor")
    claim, accepted, values = successor_case(kind, "terminal-not-committed")
    projection = accepted.capacity_membership_envelope.request.projection
    snapshot = ConfigurationSnapshotV1.model_validate_json(
        json.dumps(values["current_configuration"])
    )
    snapshot = snapshot.model_copy(
        update={"configuration_epoch": projection.expected_configuration_epoch + 1}
    )
    accepted = replace(
        accepted,
        capacity_mode="shadow-v1",
        capacity_membership_envelope=None,
        capacity_projection_request_sha256=canonical_digest(projection),
        capacity_configuration_epoch=snapshot.configuration_epoch,
        capacity_configuration_sha256=canonical_digest(snapshot),
    )
    claim = replace(
        claim,
        environment=replace(
            claim.environment,
            accepted_capacity_mode="shadow-v1",
            accepted_capacity_membership_checkpoint=None,
            capacity_configuration_epoch=snapshot.configuration_epoch,
            capacity_configuration_sha256=canonical_digest(snapshot),
        ),
    )
    values["accepted_shadow_configuration"] = snapshot.model_dump(mode="json")
    values["accepted_shadow_projection"] = projection.model_dump(mode="json")
    if tamper == "snapshot":
        values["accepted_shadow_configuration"]["configuration_epoch"] += 1
    elif tamper == "projection":
        values["accepted_shadow_projection"]["min_slots"] = 1
    elif tamper == "accepted-mode":
        claim = replace(
            claim, environment=replace(claim.environment, accepted_capacity_mode="membership-v1")
        )
    if tamper is not None:
        with pytest.raises(ValueError):
            binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(
                json.dumps(values)
            )
            module.validate_membership_successor(
                binding, claim=claim, accepted_operation=accepted, now=_NOW
            )
    else:
        binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(
            json.dumps(values)
        )
        decision = module.validate_membership_successor(
            binding, claim=claim, accepted_operation=accepted, now=_NOW
        )
        assert decision.kind == ("destroy" if kind == "destroy" else "update")


@pytest.mark.parametrize("history", (False, True))
@pytest.mark.parametrize(
    "field,value",
    (
        ("idempotency_key", uuid4()),
        ("attempt_id", uuid4()),
        ("local_activation_sha256", "e" * 64),
        ("capacity_reporter_incarnation", uuid4()),
        ("capacity_reporter_token_sha256", "e" * 64),
        ("protected_admission_sha256", "e" * 64),
        ("capacity_agent_installation_sha256", "e" * 64),
        ("capacity_supported_pool_ids", ("oldlab",)),
        ("capacity_supported_architectures", ("x86_64",)),
        ("min_slots", 1),
        ("max_slots", 4),
    ),
)
def test_successor_rejects_record_envelope_drift_even_with_rehashed_intent(history, field, value):
    module = import_module("loom.personal_dev_membership_successor")
    claim, accepted, values = successor_case("update", "terminal-not-committed")
    record = replace(accepted if history else claim.operation, **{field: value})
    record = replace(record, request_sha256=_intent(record).request_sha256)
    if history:
        accepted = record
    else:
        claim = replace(claim, operation=record)
        values["request_sha256"] = record.request_sha256
        if field == "attempt_id":
            claim = replace(claim, attempt=replace(claim.attempt, id=value))
    binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    with pytest.raises(ValueError):
        module.validate_membership_successor(
            binding, claim=claim, accepted_operation=accepted, now=_NOW
        )


@pytest.mark.parametrize("change", ("operation-id", "checkpoint", "request-digest"))
def test_successor_requires_exact_completed_accepted_operation(change):
    module = import_module("loom.personal_dev_membership_successor")
    claim, accepted, values = successor_case("capacity", "terminal-not-committed")
    if change == "operation-id":
        accepted = replace(accepted, id=uuid4())
    elif change == "checkpoint":
        accepted = replace(accepted, checkpoint="capacity_projection_pending")
    else:
        accepted = replace(accepted, request_sha256="f" * 64)
    binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    with pytest.raises(ValueError):
        module.validate_membership_successor(
            binding, claim=claim, accepted_operation=accepted, now=_NOW
        )


@pytest.mark.parametrize("field,value", (("publication_sha256", "e" * 64), ("id", uuid4())))
def test_successor_requires_independent_candidate_publication(field, value):
    module = import_module("loom.personal_dev_membership_successor")
    claim, accepted, values = successor_case()
    claim = replace(claim, candidate=replace(claim.candidate, **{field: value}))
    binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    with pytest.raises(ValueError):
        module.validate_membership_successor(
            binding, claim=claim, accepted_operation=accepted, now=_NOW
        )


@pytest.mark.parametrize("kind", ("update", "destroy"))
@pytest.mark.parametrize("review_expired", (False, True))
def test_recovery_window_is_independent_of_positive_admission(kind, review_expired):
    module = import_module("loom.personal_dev_membership_successor")
    claim, accepted, values = successor_case(kind, "terminal-not-committed")
    values["authority"]["started_at"] = (_NOW - timedelta(hours=2)).isoformat()
    values["authority"]["expires_at"] = (_NOW - timedelta(hours=1)).isoformat()
    if review_expired:
        values["reviewed_at"] = (_NOW - timedelta(hours=2)).isoformat()
        values["expires_at"] = _NOW.isoformat()
    binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    if kind == "destroy" and not review_expired:
        decision = module.validate_membership_successor(
            binding, claim=claim, accepted_operation=accepted, now=_NOW
        )
        assert decision.kind == "destroy"
    else:
        with pytest.raises(ValueError):
            module.validate_membership_successor(
                binding, claim=claim, accepted_operation=accepted, now=_NOW
            )


@pytest.mark.parametrize("change", ("naive", "reversed", "unbounded", "not-started"))
def test_successor_requires_bounded_current_operator_review(change):
    module = import_module("loom.personal_dev_membership_successor")
    claim, accepted, values = successor_case("destroy", "terminal-not-committed")
    if change == "naive":
        values["reviewed_at"] = _NOW.replace(tzinfo=None).isoformat()
    elif change == "reversed":
        values["expires_at"] = (_NOW - timedelta(days=1)).isoformat()
    elif change == "unbounded":
        values["expires_at"] = (_NOW + timedelta(days=2)).isoformat()
    else:
        values["reviewed_at"] = (_NOW + timedelta(minutes=1)).isoformat()
    with pytest.raises(ValueError):
        binding = module.PersonalDevMembershipSuccessorBindingV1.model_validate_json(
            json.dumps(values)
        )
        module.validate_membership_successor(
            binding, claim=claim, accepted_operation=accepted, now=_NOW
        )
