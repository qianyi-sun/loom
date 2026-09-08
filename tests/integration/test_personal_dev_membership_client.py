"""Real manager HTTP/store compatibility for concurrent lifecycle checkpoints."""

from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

import pytest

from loom.personal_dev_membership_checkpoint import (
    PersonalDevMembershipEnvelopeV1,
    refresh_membership_checkpoint,
)
from loom.personal_dev_membership_client import (
    CapacityManagerPersonalDevMembershipClient,
    PersonalDevMembershipRevisionConflictError,
)
from loom_capacity_manager.auth import CapacityPrincipalVerifier
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.membership_contracts import PersonalMembershipCheckpointV1
from loom_capacity_manager.membership_outcomes import PersonalMembershipOperationCommittedV1
from loom_capacity_manager.membership_store import CapacityMembershipStore
from tests.integration.test_capacity_membership import DELEGATE, _active_v3, _projection, _request
from tests.integration.test_capacity_membership_agent_auth import _http_client
from tests.integration.test_capacity_membership_api import (
    MEMBERSHIP_TOKEN,
    membership_api,  # noqa: F401
)
from tests.integration.test_capacity_membership_outcomes import READ_TOKEN, _principal, _retire


def _envelope(request, checkpoint, *, identity: int):
    projection = request.projection
    return PersonalDevMembershipEnvelopeV1.model_validate(
        {
            "management_principal_id": DELEGATE,
            "idempotency_key": UUID(int=identity),
            "expected_checkpoint": checkpoint,
            "request": request,
            "request_sha256": canonical_digest(request),
            "observation": {
                "operation_id": projection.operation_id,
                "operation_epoch": projection.operation_epoch,
                "attempt_id": UUID(int=identity + 1),
                "observation_lease_epoch": 1,
                "observed_at": datetime.now(UTC),
                "execution": request.execution,
                "local_activation_sha256": projection.local_activation_sha256,
                "capacity_agent_installation_sha256": projection.capacity_agent_installation_sha256,
                "acknowledgement": request.acknowledgement,
            },
        }
    )


async def test_two_owner_revision_conflict_then_original_commit_replay(membership_api):  # noqa: F811
    http, execution, _, _ = membership_api
    client = CapacityManagerPersonalDevMembershipClient(
        manager_origin="https://capacity.test",
        bearer_token=MEMBERSHIP_TOKEN,
        http_client=http,
    )
    original_checkpoint = await client.membership_checkpoint()
    first = _envelope(_request(execution), original_checkpoint, identity=25100)
    second_projection = _projection(
        operation_id=UUID(int=25102),
        subject_id=UUID(int=25103),
        subject_incarnation=UUID(int=25104),
        owner_id=UUID(int=25105),
        environment_name="carol",
        reporter_incarnation=UUID(int=25106),
    ).model_copy(update={"demand_reporter_token_sha256": "9" * 64})
    second = _envelope(_request(execution, second_projection), original_checkpoint, identity=25107)
    original_bytes = canonical_bytes(first)
    first_response = await client.mutate_membership(first)
    assert first_response.result.revision == 1
    with pytest.raises(PersonalDevMembershipRevisionConflictError):
        await client.mutate_membership(second)
    refreshed = refresh_membership_checkpoint(second, await client.membership_checkpoint())
    assert refreshed.observation == second.observation
    second_response = await client.mutate_membership(refreshed)
    assert second_response.result.revision == 2
    assert (await client.membership_checkpoint()).revision == 2
    replay = await client.mutate_membership(first)
    assert replay.result.replayed
    assert replay.checkpoint == first_response.checkpoint
    assert replay.result.member == first_response.result.member
    assert canonical_bytes(first) == original_bytes


async def test_client_recovers_original_receipt_after_retirement_with_current_observer(
    capacity_session,
):
    fixture, active = await _active_v3(capacity_session)
    request = _request(active)
    envelope = _envelope(
        request,
        PersonalMembershipCheckpointV1(
            execution=active,
            namespace_id=request.namespace_id,
            revision=0,
            head_sha256="0" * 64,
        ),
        identity=25120,
    )
    committed = await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        request,
        actor=DELEGATE,
        idempotency_key=envelope.idempotency_key,
    )
    await _retire(capacity_session, fixture, active)
    # The old mutation credential is absent; recovery has only current read authority.
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (http, _):
        client = CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.test",
            bearer_token=READ_TOKEN,
            http_client=http,
        )
        outcome = await client.membership_operation_outcome(envelope)
        assert isinstance(outcome, PersonalMembershipOperationCommittedV1)
        assert outcome.receipt.result == committed
        assert outcome.receipt.checkpoint.revision == 1
        assert envelope.result is None


async def test_client_keeps_absence_unresolved_until_authenticated_retirement(capacity_session):
    fixture, active = await _active_v3(capacity_session)
    request = _request(active)
    envelope = _envelope(
        request,
        PersonalMembershipCheckpointV1(
            execution=active,
            namespace_id=request.namespace_id,
            revision=0,
            head_sha256="0" * 64,
        ),
        identity=25130,
    )
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (http, _):
        client = CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.test",
            bearer_token=READ_TOKEN,
            http_client=http,
        )
        assert (await client.membership_operation_outcome(envelope)).outcome == "unresolved"
        await _retire(capacity_session, fixture, active)
        outcome = await client.membership_operation_outcome(envelope)
        assert outcome.outcome == "terminal-not-committed"
        assert envelope.result is None


async def test_client_reads_exact_historical_subject_and_authenticated_release(capacity_session):
    from tests.integration.test_capacity_membership_subject_status import _empty_disabled

    fixture, active, original, created, disabled = await _empty_disabled(capacity_session)
    projection = original.projection.model_copy(
        update={
            "operation_kind": "destroy",
            "operation_epoch": 2,
            "configuration_generation": 2,
            "operation_id": UUID(int=27002),
        }
    )
    pending = _envelope(
        _request(active, projection, expected_revision=1), created.checkpoint, identity=27003
    )
    saved = PersonalDevMembershipEnvelopeV1.model_validate(
        pending.model_dump(mode="python") | {"result": disabled}
    )
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (http, _):
        client = CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.test",
            bearer_token=READ_TOKEN,
            http_client=http,
        )
        status = await client.membership_subject_status(saved)
        assert status.membership_receipt == disabled
        assert status.historical and not status.worker_available
        release = await client.membership_subject_release(saved)
        assert release.membership_receipt == disabled
        assert release.outcome == "verified"
        assert not release.worker_available
