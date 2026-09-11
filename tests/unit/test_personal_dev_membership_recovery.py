"""Historical recovery never re-attests pending work or grants current readiness."""

import json
from importlib import import_module

import pytest

from loom.personal_dev_membership_client import (
    PersonalDevMembershipError,
    PersonalDevMembershipRevisionConflictError,
)
from loom_capacity_manager.membership_outcomes import parse_membership_operation_outcome
from loom_capacity_manager.membership_subject_status import PersonalMembershipSubjectStatusV1
from tests.unit.test_personal_dev_membership_checkpoint import membership_response
from tests.unit.test_personal_dev_membership_client import _outcome_payload
from tests.unit.test_personal_dev_membership_reconciler import (
    _NOW,
    _Admission,
    _Authority,
    _Client,
    _Installer,
    _pending_claim,
    _run,
)
from tests.unit.test_personal_dev_membership_subject_client import _response


class _RecoveryAuthority(_Authority):
    async def record_capacity_membership_outcome(self, **kwargs):
        self.calls.append(("historical", kwargs))


class _Observer:
    def __init__(self, envelope, kind, *, transitioned=True):
        _, payload = _outcome_payload(envelope, kind)
        if kind == "committed":
            payload["receipt"] = membership_response(
                envelope.request, key=envelope.idempotency_key
            ).model_dump(mode="json")
        self.outcome = parse_membership_operation_outcome(json.dumps(payload))
        self.requests = []
        self.status_requests = []
        self.transitioned = transitioned

    async def membership_operation_outcome(self, envelope):
        self.requests.append(envelope)
        return self.outcome

    async def membership_subject_status(self, envelope):
        self.status_requests.append(envelope)
        accepted = envelope.model_copy(update={"historical_outcome": None, "result": self.outcome.receipt})
        _, payload = _response(accepted, "status")
        if self.transitioned:
            payload["current"].update(execution_epoch=99, membership_execution_epoch=99)
        return PersonalMembershipSubjectStatusV1.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize("kind", ("committed", "terminal-not-committed"))
async def test_failed_replay_resolves_with_current_observer_under_current_lease(kind):
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = _pending_claim()
    saved = claim.operation.capacity_membership_envelope
    authority, installer = _RecoveryAuthority(), _Installer()
    client = _Client(saved, error=PersonalDevMembershipError("old delegate revoked"))
    observer = _Observer(saved, kind)
    driver = module.PersonalDevMembershipReconciler(
        admission=_Admission(),
        authority=authority, client=client, installer=installer, observer=observer
    )
    await driver.reconcile(claim, lease={"lease_epoch": 99}, now=lambda: _NOW, run=_run)
    assert client.requests == [saved]
    assert observer.requests == [saved]
    assert not client.reads and not installer.verifications
    assert authority.calls == [
        ("historical", {"lease_epoch": 99, "now": _NOW, "outcome": observer.outcome})
    ]
    assert saved.result is None and saved.historical_outcome is None


async def test_unresolved_outcome_preserves_failed_pending_request_without_store_transition():
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = _pending_claim()
    saved = claim.operation.capacity_membership_envelope
    authority, installer = _RecoveryAuthority(), _Installer()
    failure = PersonalDevMembershipError("unconfirmed")
    client, observer = _Client(saved, error=failure), _Observer(saved, "unresolved")
    driver = module.PersonalDevMembershipReconciler(
        admission=_Admission(),
        authority=authority, client=client, installer=installer, observer=observer
    )
    with pytest.raises(PersonalDevMembershipError) as error:
        await driver.reconcile(claim, lease={"lease_epoch": 99}, now=lambda: _NOW, run=_run)
    assert error.value is failure
    assert not authority.calls and not installer.verifications
    assert observer.requests == client.requests == [saved]


async def test_changed_authority_during_revision_refresh_recovers_without_replacing_request():
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = _pending_claim()
    saved = claim.operation.capacity_membership_envelope
    checkpoint = saved.expected_checkpoint.model_copy(
        update={"execution": saved.request.execution.model_copy(update={"execution_epoch": 99})}
    )
    authority, installer = _RecoveryAuthority(), _Installer()
    client = _Client(saved, error=PersonalDevMembershipRevisionConflictError(), checkpoint=checkpoint)
    observer = _Observer(saved, "terminal-not-committed")
    driver = module.PersonalDevMembershipReconciler(
        admission=_Admission(),
        authority=authority, client=client, installer=installer, observer=observer
    )
    await driver.reconcile(claim, lease={"lease_epoch": 99}, now=lambda: _NOW, run=_run)
    assert client.requests == observer.requests == [saved]
    assert [name for name, _ in authority.calls] == ["historical"]


async def test_recovery_revalidates_full_outcome_even_with_alternative_client():
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = _pending_claim()
    saved = claim.operation.capacity_membership_envelope
    authority, installer = _RecoveryAuthority(), _Installer()
    observer = _Observer(saved, "committed")
    observer.outcome = observer.outcome.model_copy(update={"query_sha256": "f" * 64})
    driver = module.PersonalDevMembershipReconciler(
        admission=_Admission(),
        authority=authority,
        client=_Client(saved, error=PersonalDevMembershipError()),
        installer=installer,
        observer=observer,
    )
    with pytest.raises(ValueError, match="different request"):
        await driver.reconcile(claim, lease={"lease_epoch": 99}, now=lambda: _NOW, run=_run)
    assert not authority.calls and not installer.verifications


async def test_retired_delegate_can_recover_when_checkpoint_read_is_also_rejected():
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = _pending_claim()
    saved = claim.operation.capacity_membership_envelope

    class Client(_Client):
        async def membership_checkpoint(self):
            raise PersonalDevMembershipError("delegate retired during refresh")

    authority = _RecoveryAuthority()
    observer = _Observer(saved, "committed")
    driver = module.PersonalDevMembershipReconciler(
        admission=_Admission(),
        authority=authority,
        client=Client(saved, error=PersonalDevMembershipRevisionConflictError()),
        installer=_Installer(), observer=observer,
    )
    await driver.reconcile(claim, lease={"lease_epoch": 99}, now=lambda: _NOW, run=_run)
    assert observer.requests == [saved]
    assert [name for name, _ in authority.calls] == ["historical"]


async def test_lost_response_in_same_active_authority_keeps_exact_pending_replay():
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = _pending_claim()
    saved = claim.operation.capacity_membership_envelope
    authority, installer = _RecoveryAuthority(), _Installer()
    failure = PersonalDevMembershipError("response lost")
    client = _Client(saved, error=failure)
    observer = _Observer(saved, "committed", transitioned=False)
    driver = module.PersonalDevMembershipReconciler(
        admission=_Admission(),
        authority=authority, client=client, installer=installer, observer=observer,
    )
    with pytest.raises(PersonalDevMembershipError) as error:
        await driver.reconcile(claim, lease={"lease_epoch": 99}, now=lambda: _NOW, run=_run)
    assert error.value is failure
    assert not authority.calls and not installer.verifications
    assert len(observer.status_requests) == 1
    client.error = None
    await driver.reconcile(claim, lease={"lease_epoch": 100}, now=lambda: _NOW, run=_run)
    assert client.requests == [saved, saved]
    assert [name for name, _ in authority.calls] == ["record"]
    assert authority.calls[0][1]["lease_epoch"] == 100
