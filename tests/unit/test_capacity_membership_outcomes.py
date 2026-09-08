"""Outcome JSON is bounded, duplicate-rejecting and strictly discriminated."""

import json
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES, canonical_bytes, canonical_digest
from loom_capacity_manager.membership_outcomes import (
    PersonalMembershipOperationOutcomeQueryV1,
    PersonalMembershipOperationUnresolvedV1,
    parse_membership_operation_outcome,
    parse_membership_operation_outcome_query,
)
from tests.integration.test_capacity_membership import DELEGATE, _request
from tests.unit.test_capacity_manager_executable_contracts import _authority


def _query() -> PersonalMembershipOperationOutcomeQueryV1:
    return PersonalMembershipOperationOutcomeQueryV1(
        original_actor=DELEGATE, idempotency_key=UUID(int=26010), request=_request(_authority())
    )


def test_outcome_query_exact_canonical_roundtrip() -> None:
    query = _query()
    assert parse_membership_operation_outcome_query(canonical_bytes(query)) == query
    outcome = PersonalMembershipOperationUnresolvedV1(
        query_sha256=canonical_digest(query),
        request_sha256=canonical_digest(query.request),
        original_actor=query.original_actor,
        idempotency_key=query.idempotency_key,
        operation_id=query.request.projection.operation_id,
        execution_epoch=query.request.execution.execution_epoch,
        execution_manifest_sha256=query.request.execution.execution_manifest_sha256,
        namespace_id=query.request.namespace_id,
        epoch_state="active",
    )
    assert parse_membership_operation_outcome(canonical_bytes(outcome)) == outcome
    for changed in (
        {"outcome": "terminal-not-committed"},
        {"outcome": "committed"},
        {"epoch_state": "retired"},
        {"receipt": {}},
        {"schema_version": True},
    ):
        with pytest.raises(ValueError):
            parse_membership_operation_outcome(
                json.dumps(outcome.model_dump(mode="json") | changed)
            )


def test_outcome_query_preserves_uuid_keys_accepted_by_existing_mutation_api() -> None:
    query = _query().model_copy(update={"idempotency_key": UUID(int=0)})
    assert parse_membership_operation_outcome_query(canonical_bytes(query)) == query


@pytest.mark.parametrize("location", ("root", "request", "execution", "projection"))
@pytest.mark.parametrize("tag", (True, 1.0, "1"))
def test_query_rejects_noninteger_nested_wire_tags(location: str, tag: object) -> None:
    payload = _query().model_dump(mode="json")
    target = payload if location == "root" else payload["request"]
    if location in {"execution", "projection"}:
        target = target[location]
    target["schema_version"] = (
        2.0 if location == "execution" and tag == 1.0 and type(tag) is float else tag
    )
    with pytest.raises(ValueError):
        parse_membership_operation_outcome_query(json.dumps(payload))


def test_query_rejects_duplicate_nested_keys_and_oversize_json() -> None:
    payload = canonical_bytes(_query())
    for altered in (
        payload.replace(b'"original_actor":', b'"original_actor":"forged","original_actor":'),
        payload.replace(b'"expected_revision":', b'"expected_revision":99,"expected_revision":'),
        b" " * (MAX_CONTRACT_BYTES + 1) + payload,
    ):
        with pytest.raises(ValueError):
            parse_membership_operation_outcome_query(altered)
