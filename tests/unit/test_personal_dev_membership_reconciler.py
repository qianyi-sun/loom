"""Membership retries cannot reinstall or silently cross execution authority."""

from dataclasses import replace
from importlib import import_module

import pytest

from loom.personal_dev_capacity import personal_dev_capacity_projection
from loom.personal_dev_membership_checkpoint import PersonalDevMembershipEnvelopeV1
from loom.personal_dev_membership_client import (
    PersonalDevMembershipError,
    PersonalDevMembershipRevisionConflictError,
)
from loom.personal_dev_reconciler import PersonalDevEnvironmentReconciler
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.executable_contracts import CandidateBindingV2
from tests.unit.test_personal_dev_membership_checkpoint import (
    membership_envelope_values,
    membership_response,
)
from tests.unit.test_personal_dev_reconciler import (
    _NOW,
    _claim,
    _Executor,
    _installation,
    _Projector,
)
from tests.unit.test_personal_dev_reconciler import (
    _Authority as _LegacyAuthority,
)
from tests.unit.test_personal_dev_reconciler import (
    _Installer as _LegacyInstaller,
)


def _pending_claim():
    claim = _claim(state="activating", checkpoint="capacity_projection_pending")
    values = membership_envelope_values()
    projection = personal_dev_capacity_projection(
        claim,
        _installation(),
        expected_configuration_epoch=values["request"].execution.configuration_epoch,
    )
    acknowledgement = values["request"].acknowledgement.model_copy(
        update={
            "subject_id": projection.subject_id,
            "subject_incarnation": projection.subject_incarnation,
            "configuration_generation": projection.configuration_generation,
            "deployment_generation": projection.deployment_generation,
            "reporter_incarnation": projection.demand_reporter_incarnation,
            "protected_admission_sha256": projection.protected_admission_sha256,
            "candidate": CandidateBindingV2(
                algorithm="source-sha256",
                identity=projection.candidate_sha256,
                publication_sha256=projection.candidate_publication_sha256,
            ),
        }
    )
    request = values["request"].model_copy(
        update={"projection": projection, "acknowledgement": acknowledgement}
    )
    envelope = PersonalDevMembershipEnvelopeV1.model_validate(
        values
        | {
            "idempotency_key": claim.operation.idempotency_key,
            "request": request,
            "request_sha256": canonical_digest(request),
            "observation": values["observation"]
            | {
                "operation_id": claim.operation.id,
                "operation_epoch": claim.operation.operation_epoch,
                "attempt_id": claim.attempt.id,
                "acknowledgement": acknowledgement,
                "local_activation_sha256": projection.local_activation_sha256,
                "capacity_agent_installation_sha256": projection.capacity_agent_installation_sha256,
            },
        }
    )
    return replace(
        claim,
        operation=replace(
            claim.operation, capacity_mode="membership-v1", capacity_membership_envelope=envelope
        ),
    )


class _Authority:
    def __init__(self):
        self.calls = []

    async def record_capacity_membership(self, **kwargs):
        self.calls.append(("record", kwargs))

    async def refresh_capacity_membership(self, **kwargs):
        self.calls.append(("refresh", kwargs))

    async def prepare_capacity_membership(self, **kwargs):
        self.calls.append(("prepare", kwargs))

    async def complete_activation(self, **kwargs):
        self.calls.append(("complete", kwargs))


class _Client:
    def __init__(self, envelope, error=None, checkpoint=None):
        self.envelope = envelope
        self.error = error
        self.checkpoint = checkpoint or envelope.expected_checkpoint.model_copy(
            update={"revision": 1, "head_sha256": "a" * 64}
        )
        self.requests = []
        self.reads = 0

    async def mutate_membership(self, envelope):
        self.requests.append(envelope)
        if self.error:
            raise self.error
        return membership_response(envelope.request, key=envelope.idempotency_key)

    async def membership_checkpoint(self):
        self.reads += 1
        return self.checkpoint


class _Installer:
    def __init__(self):
        self.verifications = []

    async def verify_membership_publishing(self, claim, envelope):
        self.verifications.append(envelope)

    async def converge(self, claim):
        raise AssertionError("pending membership must not reinstall")


class _Admission:
    async def assert_admission_ready(self, *, now):
        pass


async def _run(awaitable):
    return await awaitable


async def test_membership_operation_never_falls_back_to_shadow_reconciliation():
    claim = _pending_claim()
    authority, installer, projector = _LegacyAuthority(claim), _LegacyInstaller(), _Projector()
    reconciler = PersonalDevEnvironmentReconciler(
        authority=authority,
        executor=_Executor(),
        capacity_installer=installer,
        capacity_projector=projector,
        access_loader=None,
        reconciler_id="reconciler-a",
        lease_seconds=60,
    )
    with pytest.raises(RuntimeError, match="membership"):
        await reconciler.reconcile_once(now=_NOW)
    assert installer.calls == 0 and not projector.requests


async def test_membership_pending_dispatch_uses_current_lease_and_original_envelope():
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = _pending_claim()
    authority, installer, projector = _LegacyAuthority(claim), _LegacyInstaller(), _Projector()
    member_authority, member_installer = _Authority(), _Installer()
    driver = module.PersonalDevMembershipReconciler(
        authority=member_authority,
        admission=_Admission(),
        client=_Client(claim.operation.capacity_membership_envelope),
        installer=member_installer,
    )
    reconciler = PersonalDevEnvironmentReconciler(
        authority=authority,
        executor=_Executor(),
        capacity_installer=installer,
        capacity_projector=projector,
        access_loader=None,
        reconciler_id="reconciler-a",
        lease_seconds=60,
        membership_reconciler=driver,
    )
    assert await reconciler.reconcile_once(now=_NOW)
    assert installer.calls == 0 and not projector.requests
    assert member_authority.calls[0][1]["lease_epoch"] == claim.attempt.lease_epoch


async def test_first_membership_observation_is_persisted_before_any_mutation():
    module = import_module("loom.personal_dev_membership_reconciler")
    pending = _pending_claim()
    original = pending.operation.capacity_membership_envelope
    claim = replace(
        pending,
        operation=replace(
            pending.operation,
            checkpoint="activation_acknowledged",
            capacity_membership_envelope=None,
        ),
    )
    authority = _Authority()
    client = _Client(original, checkpoint=original.expected_checkpoint)
    observations = []

    class Installer(_Installer):
        def validate_membership_context(self, claim, checkpoint, *, observed_at):
            assert checkpoint == original.expected_checkpoint

        async def converge(self, claim):
            return _installation()

        async def observe_membership(self, claim, installation, checkpoint, *, observed_at):
            observations.append((claim, checkpoint))
            return original.observation.model_copy(
                update={
                    "observation_lease_epoch": claim.attempt.lease_epoch,
                    "observed_at": observed_at,
                }
            )

    driver = module.PersonalDevMembershipReconciler(
        authority=authority,
        client=client,
        installer=Installer(),
        admission=_Admission(),
        management_principal_id=original.management_principal_id,
    )
    await driver.reconcile(
        claim, lease={"lease_epoch": claim.attempt.lease_epoch}, now=lambda: _NOW, run=_run
    )
    assert len(observations) == 1
    assert not client.requests
    assert authority.calls[0][0] == "prepare"
    saved = authority.calls[0][1]["envelope"]
    assert saved.request == original.request
    assert saved.observation.observation_lease_epoch == claim.attempt.lease_epoch
    assert saved.result is None


async def test_wrong_execution_is_rejected_before_initial_installation():
    module = import_module("loom.personal_dev_membership_reconciler")
    pending = _pending_claim()
    original = pending.operation.capacity_membership_envelope
    claim = replace(
        pending,
        operation=replace(
            pending.operation,
            checkpoint="activation_acknowledged",
            capacity_membership_envelope=None,
        ),
    )
    authority = _Authority()
    effects = []

    class Installer(_Installer):
        def validate_membership_context(self, claim, checkpoint, *, observed_at):
            raise ValueError("wrong execution")

        async def converge(self, claim):
            effects.append("install")
            return _installation()

        async def observe_membership(self, *args, **kwargs):
            raise ValueError("wrong execution")

    driver = module.PersonalDevMembershipReconciler(
        authority=authority,
        client=_Client(original),
        installer=Installer(),
        admission=_Admission(),
        management_principal_id=original.management_principal_id,
    )
    with pytest.raises(ValueError, match="wrong execution"):
        await driver.reconcile(claim, lease={}, now=lambda: _NOW, run=_run)
    assert not effects and not authority.calls


async def test_unfinished_membership_teardown_cannot_be_treated_as_shadow_release():
    module = import_module("loom.personal_dev_membership_reconciler")
    pending = _pending_claim()
    claim = replace(
        pending, operation=replace(pending.operation, kind="destroy", checkpoint="cleanup_pending")
    )
    authority = _Authority()
    driver = module.PersonalDevMembershipReconciler(
        authority=authority,
        client=_Client(pending.operation.capacity_membership_envelope),
        installer=_Installer(),
    )
    with pytest.raises(ValueError, match="membership cleanup"):
        await driver.reconcile(claim, lease={}, now=lambda: _NOW, run=_run)
    assert not authority.calls


async def test_pending_membership_reuses_saved_request_across_lease_takeover():
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = _pending_claim()
    envelope = claim.operation.capacity_membership_envelope
    authority, installer = _Authority(), _Installer()
    client = _Client(envelope)
    driver = module.PersonalDevMembershipReconciler(
        authority=authority, client=client, installer=installer, admission=_Admission()
    )
    lease = {"lease_epoch": 99, "attempt_id": claim.attempt.id}
    await driver.reconcile_pending(claim, lease=lease, now=lambda: _NOW, run=_run)
    assert client.requests == [envelope]
    assert client.requests[0].observation.observation_lease_epoch == 4
    assert client.reads == 0
    assert installer.verifications == [envelope]
    assert authority.calls[0][0] == "record"
    assert authority.calls[0][1]["lease_epoch"] == 99
    assert authority.calls[0][1]["response"] == membership_response(
        envelope.request, key=envelope.idempotency_key
    )


async def test_wrong_saved_operation_is_rejected_before_network_io():
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = _pending_claim()
    envelope = claim.operation.capacity_membership_envelope
    claim = replace(claim, operation=replace(claim.operation, operation_epoch=99))
    authority, installer = _Authority(), _Installer()
    client = _Client(envelope)
    driver = module.PersonalDevMembershipReconciler(
        authority=authority, client=client, installer=installer, admission=_Admission()
    )
    with pytest.raises(ValueError):
        await driver.reconcile_pending(claim, lease={}, now=lambda: _NOW, run=_run)
    assert not client.requests and not authority.calls


@pytest.mark.parametrize(
    "error", (PersonalDevMembershipError("lost response"), RuntimeError("transport unavailable"))
)
async def test_ambiguous_membership_failure_never_refreshes_or_records(error):
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = _pending_claim()
    authority, installer = _Authority(), _Installer()
    client = _Client(claim.operation.capacity_membership_envelope, error=error)
    driver = module.PersonalDevMembershipReconciler(
        authority=authority, client=client, installer=installer, admission=_Admission()
    )
    with pytest.raises(type(error)):
        await driver.reconcile_pending(claim, lease={}, now=lambda: _NOW, run=_run)
    assert not authority.calls and not installer.verifications
    assert client.reads == 0


@pytest.mark.parametrize("changed_authority", (False, True))
async def test_only_typed_same_authority_conflict_persists_refresh(changed_authority):
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = _pending_claim()
    envelope = claim.operation.capacity_membership_envelope
    authority, installer = _Authority(), _Installer()
    checkpoint = envelope.expected_checkpoint.model_copy(
        update={"revision": 1, "head_sha256": "a" * 64}
    )
    if changed_authority:
        checkpoint = checkpoint.model_copy(
            update={
                "execution": checkpoint.execution.model_copy(
                    update={"execution_manifest_sha256": "b" * 64}
                )
            }
        )
    client = _Client(
        envelope,
        error=PersonalDevMembershipRevisionConflictError("conflict"),
        checkpoint=checkpoint,
    )
    driver = module.PersonalDevMembershipReconciler(
        authority=authority, client=client, installer=installer, admission=_Admission()
    )
    if changed_authority:
        with pytest.raises(ValueError):
            await driver.reconcile_pending(claim, lease={}, now=lambda: _NOW, run=_run)
        assert not authority.calls
    else:
        await driver.reconcile_pending(claim, lease={}, now=lambda: _NOW, run=_run)
        assert authority.calls == [("refresh", {"now": _NOW, "checkpoint": checkpoint})]
    assert len(client.requests) == 1
    assert not installer.verifications
