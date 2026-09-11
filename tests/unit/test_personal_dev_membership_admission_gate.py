"""Admission authorization must not suppress independent historical recovery."""

from dataclasses import replace
from datetime import timedelta

import pytest

from loom.personal_dev_membership_admission import PersonalDevMembershipAdmissionError
from loom.personal_dev_membership_reconciler import PersonalDevMembershipReconciler
from loom.personal_dev_reconciler import PersonalDevEnvironmentReconciler
from loom_capacity_manager.contracts import canonical_digest
from tests.unit.test_personal_dev_membership_reconciler import (
    _NOW,
    _Client,
    _Installer,
    _pending_claim,
    _run,
)
from tests.unit.test_personal_dev_membership_recovery import _Observer, _RecoveryAuthority
from tests.unit.test_personal_dev_reconciler import _Authority, _Executor, _Projector


@pytest.mark.parametrize("initial", (False, True))
async def test_missing_admission_authority_rejects_before_installation_or_mutation(initial):
    claim = _pending_claim()
    saved = claim.operation.capacity_membership_envelope
    if initial:
        claim = replace(claim, operation=replace(
            claim.operation, checkpoint="activation_acknowledged", capacity_membership_envelope=None,
        ))
    authority, installer, client = _RecoveryAuthority(), _Installer(), _Client(saved)
    driver = PersonalDevMembershipReconciler(
        authority=authority, client=client, installer=installer,
        management_principal_id=saved.management_principal_id,
    )
    with pytest.raises(PersonalDevMembershipAdmissionError, match="configured"):
        await driver.reconcile(claim, lease={}, now=lambda: _NOW, run=_run)
    assert not client.requests and not client.reads and not authority.calls


@pytest.mark.parametrize("outcome", ("committed", "terminal-not-committed", "unresolved"))
async def test_expired_admission_can_resolve_history_but_cannot_send_pending_increase(outcome):
    class Expired:
        async def assert_admission_ready(self, *, now):
            raise PersonalDevMembershipAdmissionError("window expired")

    claim = _pending_claim()
    saved = claim.operation.capacity_membership_envelope
    authority, installer, client = _RecoveryAuthority(), _Installer(), _Client(saved)
    observer = _Observer(saved, outcome)
    driver = PersonalDevMembershipReconciler(
        authority=authority, client=client, installer=installer,
        observer=observer, admission=Expired(),
    )
    if outcome == "unresolved":
        with pytest.raises(PersonalDevMembershipAdmissionError, match="window"):
            await driver.reconcile(claim, lease={"lease_epoch": 99}, now=lambda: _NOW, run=_run)
        assert not authority.calls
    else:
        await driver.reconcile(claim, lease={"lease_epoch": 99}, now=lambda: _NOW, run=_run)
        assert [name for name, _ in authority.calls] == ["historical"]
    assert not client.requests and not client.reads and not installer.verifications
    assert observer.requests == [saved]


async def test_initial_admission_is_checked_after_the_checkpoint_read_before_installation():
    claim = _pending_claim()
    saved = claim.operation.capacity_membership_envelope
    claim = replace(claim, operation=replace(
        claim.operation, checkpoint="activation_acknowledged", capacity_membership_envelope=None,
    ))
    events = []

    class Client(_Client):
        async def membership_checkpoint(self):
            events.append("checkpoint")
            return saved.expected_checkpoint

    class Admission:
        async def assert_admission_ready(self, *, now):
            events.append("admission")
            raise PersonalDevMembershipAdmissionError("expired during checkpoint read")

    driver = PersonalDevMembershipReconciler(
        authority=_RecoveryAuthority(), client=Client(saved), installer=_Installer(),
        admission=Admission(), management_principal_id=saved.management_principal_id,
    )
    with pytest.raises(PersonalDevMembershipAdmissionError):
        await driver.reconcile(claim, lease={}, now=lambda: _NOW, run=_run)
    assert events == ["checkpoint", "admission"]


@pytest.mark.parametrize("kind", ("create", "capacity"))
@pytest.mark.parametrize("slow_step", ("mutation", "publication"))
async def test_expiry_during_pending_io_cannot_record_current_readiness(kind, slow_step):
    claim = _pending_claim()
    saved = claim.operation.capacity_membership_envelope
    projection = saved.request.projection.model_copy(update={"operation_kind": kind})
    request = saved.request.model_copy(update={"projection": projection})
    saved = type(saved).model_validate(saved.model_dump(mode="python") | {
        "request": request, "request_sha256": canonical_digest(request),
    })
    claim = replace(claim, operation=replace(
        claim.operation, kind=kind, capacity_membership_envelope=saved,
    ))
    current = _NOW
    expires = _NOW + timedelta(seconds=1)
    checks = []

    class Admission:
        async def assert_admission_ready(self, *, now):
            checks.append(now)
            if now >= expires:
                raise PersonalDevMembershipAdmissionError("window expired")

    class Client(_Client):
        async def mutate_membership(self, envelope):
            nonlocal current
            response = await super().mutate_membership(envelope)
            if slow_step == "mutation":
                current = expires
            return response

    class Installer(_Installer):
        async def verify_membership_publishing(self, claim, envelope):
            nonlocal current
            await super().verify_membership_publishing(claim, envelope)
            if slow_step == "publication":
                current = expires

    authority, client = _RecoveryAuthority(), Client(saved)
    driver = PersonalDevMembershipReconciler(
        authority=authority, client=client, installer=Installer(), admission=Admission(),
    )
    with pytest.raises(PersonalDevMembershipAdmissionError, match="expired"):
        await driver.reconcile(claim, lease={}, now=lambda: current, run=_run)
    assert checks == [_NOW, expires]
    assert client.requests == [saved]
    assert not authority.calls
    assert claim.operation.capacity_membership_envelope == saved
    assert saved.result is None and saved.historical_outcome is None


@pytest.mark.parametrize("missing", (False, True))
async def test_candidate_preparation_requires_membership_admission_before_owner_access(missing):
    claim = _pending_claim()
    claim = replace(claim, operation=replace(
        claim.operation, state="running", checkpoint="candidate_build", capacity_membership_envelope=None,
    ))
    authority, executor = _Authority(claim), _Executor()

    class Expired:
        async def assert_admission_ready(self, *, now):
            raise PersonalDevMembershipAdmissionError("expired")

    async def access_loader(_claim):
        pytest.fail("expired membership cannot read or copy owner access")

    driver = PersonalDevMembershipReconciler(
        authority=_RecoveryAuthority(), client=None, installer=_Installer(), admission=Expired(),
    )
    reconciler = PersonalDevEnvironmentReconciler(
        authority=authority, executor=executor, capacity_installer=_Installer(),
        capacity_projector=_Projector(), access_loader=access_loader,
        reconciler_id="reconciler-a", lease_seconds=60,
        membership_reconciler=None if missing else driver,
    )
    with pytest.raises(RuntimeError, match=r"membership|expired"):
        await reconciler.reconcile_once(now=_NOW)
    assert executor.prepared == executor.bootstrapped == 0
    assert not authority.failed and not authority.begun


@pytest.mark.parametrize("slow_step", ("access", "prepare", "bootstrap"))
async def test_preparation_expiry_preserves_retry_without_bootstrap_or_activation(slow_step):
    claim = _pending_claim()
    claim = replace(claim, operation=replace(
        claim.operation, state="running", checkpoint="candidate_build", capacity_membership_envelope=None,
    ))
    authority = _Authority(claim)
    expired = False
    events = []

    class Admission:
        async def assert_admission_ready(self, *, now):
            if expired:
                raise PersonalDevMembershipAdmissionError("expired")

    class Executor(_Executor):
        async def prepare(self, claim, *, access):
            nonlocal expired
            events.append("prepare")
            result = await super().prepare(claim, access=access)
            expired = slow_step == "prepare"
            return result

        async def bootstrap_access(self, claim, *, access):
            nonlocal expired
            events.append("bootstrap")
            expired = slow_step == "bootstrap"

    async def access_loader(_claim):
        nonlocal expired
        events.append("access")
        expired = slow_step == "access"
        return "owner-access-before"

    executor = Executor()
    driver = PersonalDevMembershipReconciler(
        authority=_RecoveryAuthority(), client=None, installer=_Installer(), admission=Admission(),
    )
    reconciler = PersonalDevEnvironmentReconciler(
        authority=authority, executor=executor, capacity_installer=_Installer(),
        capacity_projector=_Projector(), access_loader=access_loader,
        reconciler_id="reconciler-a", lease_seconds=60, membership_reconciler=driver,
    )
    with pytest.raises(PersonalDevMembershipAdmissionError, match="expired"):
        await reconciler.reconcile_once(now=_NOW)
    assert not authority.failed and not authority.begun
    assert events == {
        "access": ["access"],
        "prepare": ["access", "prepare"],
        "bootstrap": ["access", "prepare", "access", "bootstrap"],
    }[slow_step]
