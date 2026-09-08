"""Historical status and release wire contracts reject contradictory evidence."""

import json

import pytest

from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES, canonical_bytes, canonical_digest
from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMembershipResponseV1,
    PersonalApplicationMembershipResultV1,
    PersonalMembershipCheckpointV1,
)
from loom_capacity_manager.membership_subject_status import (
    PersonalMembershipSubjectQueryV1,
    parse_membership_release_observation,
    parse_membership_subject_query,
    parse_membership_subject_status,
)
from tests.unit.test_capacity_manager_executable_allocator import execution_authority_fixture
from tests.unit.test_capacity_membership import delegated_input_with_new_owner


def _query() -> PersonalMembershipSubjectQueryV1:
    value = delegated_input_with_new_owner()
    member = value.membership.members[-1]
    member = member.model_copy(
        update={
            "configuration": member.configuration.model_copy(
                update={"lifecycle_state": "disabled", "min_slots": 0, "max_slots": 0},
            )
        }
    )
    result = PersonalApplicationMembershipResultV1(
        revision=member.revision,
        head_sha256="a" * 64,
        member=member,
        replayed=False,
    )
    return PersonalMembershipSubjectQueryV1(
        membership_receipt=PersonalApplicationMembershipResponseV1(
            checkpoint=PersonalMembershipCheckpointV1(
                execution=execution_authority_fixture(),
                namespace_id=value.membership.namespace_id,
                revision=result.revision,
                head_sha256=result.head_sha256,
            ),
            result=result,
        )
    )


def _observation():  # type: ignore[no-untyped-def]
    query = _query()
    checkpoint = query.membership_receipt.checkpoint
    execution = checkpoint.execution
    return {
        "schema_version": 1,
        "query_sha256": canonical_digest(query),
        "membership_receipt": query.membership_receipt.model_dump(mode="json"),
        "historical": True,
        "worker_available": False,
        "current": {
            "schema_version": 1,
            "authority_incarnation": str(execution.authority_incarnation),
            "writer_epoch": execution.writer_epoch,
            "execution_epoch": execution.execution_epoch,
            "execution_manifest_sha256": execution.execution_manifest_sha256,
            "execution_state": "active",
            "configuration_epoch": execution.configuration_epoch,
            "configuration_sha256": "b" * 64,
            "membership_execution_epoch": execution.execution_epoch,
            "membership_revision": checkpoint.revision,
            "membership_head_sha256": checkpoint.head_sha256,
            "subject": query.membership_receipt.result.member.configuration.model_dump(mode="json"),
        },
        "incarnation_work": {
            "schema_version": 1,
            "executable_intents": 0,
            "unreleased_executable_intents": 0,
            "legacy_reservations": 0,
            "unreleased_legacy_reservations": 0,
            "observed_commitments": 0,
        },
    }


def test_subject_query_and_observations_roundtrip() -> None:
    query = _query()
    assert parse_membership_subject_query(canonical_bytes(query)) == query
    observation = _observation()
    status = observation | {"deployment_work": observation["incarnation_work"]}
    assert parse_membership_subject_status(json.dumps(status)).model_dump(mode="json") == status
    verified = observation | {"outcome": "verified", "release_set_sha256": "c" * 64}
    assert (
        parse_membership_release_observation(json.dumps(verified)).model_dump(mode="json")
        == verified
    )
    observation["incarnation_work"]["observed_commitments"] = 1
    pending = observation | {"outcome": "pending", "blockers": ["observed-commitments"]}
    assert (
        parse_membership_release_observation(json.dumps(pending)).model_dump(mode="json") == pending
    )


@pytest.mark.parametrize(
    "tamper",
    (
        "active-receipt",
        "enabled-current",
        "charged",
        "wrong-subject",
        "counts",
        "head",
        "shadow",
        "regressed-generation",
    ),
)
def test_verified_release_rejects_contradictory_evidence(tamper: str) -> None:
    value = _observation() | {"outcome": "verified", "release_set_sha256": "c" * 64}
    if tamper == "active-receipt":
        value["membership_receipt"]["result"]["member"]["configuration"]["lifecycle_state"] = (
            "active"
        )
    elif tamper == "enabled-current":
        value["current"]["subject"]["lifecycle_state"] = "active"
    elif tamper == "charged":
        value["incarnation_work"]["observed_commitments"] = 1
    elif tamper == "wrong-subject":
        value["current"]["subject"]["subject_id"] = "00000000-0000-0000-0000-000000000999"
    elif tamper == "counts":
        value["incarnation_work"]["unreleased_executable_intents"] = 1
    elif tamper == "head":
        value["current"]["membership_revision"] = 0
    elif tamper == "shadow":
        value["current"]["execution_state"] = "shadow"
    else:
        member = value["membership_receipt"]["result"]["member"]
        member["configuration"]["configuration_generation"] += 1
        member["acknowledgement"]["configuration_generation"] += 1
    with pytest.raises(ValueError):
        parse_membership_release_observation(json.dumps(value))


@pytest.mark.parametrize(
    "blockers", ([], ["executable-intents"], ["observed-commitments", "observed-commitments"])
)
def test_pending_release_requires_exact_nonduplicated_blockers(blockers: list[str]) -> None:
    value = _observation() | {"outcome": "pending", "blockers": blockers}
    value["incarnation_work"]["observed_commitments"] = 1
    with pytest.raises(ValueError):
        parse_membership_release_observation(json.dumps(value))


@pytest.mark.parametrize("tag", (True, 1.0, "1"))
def test_queries_reject_noninteger_nested_versions(tag: object) -> None:
    value = _query().model_dump(mode="json")
    value["membership_receipt"]["result"]["schema_version"] = tag
    with pytest.raises(ValueError):
        parse_membership_subject_query(json.dumps(value))


def test_queries_reject_duplicate_keys_and_oversized_input() -> None:
    payload = canonical_bytes(_query())
    for invalid in (
        payload.replace(b'"replayed":', b'"replayed":false,"replayed":'),
        b" " * (MAX_CONTRACT_BYTES + 1) + payload,
    ):
        with pytest.raises(ValueError):
            parse_membership_subject_query(invalid)
