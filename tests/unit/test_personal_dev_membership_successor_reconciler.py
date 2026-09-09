"""Only explicit reviewed current adoption may continue a historical outcome."""

import asyncio
import json
from datetime import timedelta

import pytest

from loom.personal_dev_membership_admission import PersonalDevMembershipAdmissionError
from loom.personal_dev_membership_reconciler import PersonalDevMembershipReconciler
from loom.personal_dev_membership_successor import PersonalDevMembershipSuccessorBindingV1
from loom_capacity_manager.contracts import canonical_digest
from tests.unit.test_personal_dev_membership_reconciler import _NOW, _Client, _Installer, _run
from tests.unit.test_personal_dev_membership_successor import successor_case


@pytest.mark.parametrize("kind,outcome", (
    ("create", "committed"), ("update", "committed"), ("capacity", "committed"),
    ("create", "terminal-not-committed"), ("update", "terminal-not-committed"),
    ("capacity", "terminal-not-committed"), ("destroy", "terminal-not-committed"),
))
async def test_reviewed_historical_outcome_creates_one_fresh_successor_without_reinstall(kind, outcome):
    claim, _, values = successor_case(kind, outcome)
    binding = PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    calls = []

    class Authority:
        async def create_membership_successor(self, **kwargs):
            calls.append(kwargs)

    class Admission:
        async def assert_admission_ready(self, *, now):
            assert now == _NOW
            calls.append("admission")

    current = claim.operation.capacity_membership_envelope.expected_checkpoint.model_copy(update={
        "execution": binding.authority.execution, "namespace_id": binding.authority.namespace_id,
    })
    client = _Client(claim.operation.capacity_membership_envelope, checkpoint=current)
    reconciler = PersonalDevMembershipReconciler(
        authority=Authority(), client=client, installer=_Installer(), admission=Admission(),
        management_principal_id=binding.authority.management_principal_id,
        successor_bindings={claim.operation.id: binding},
    )
    lease = dict(operation_id=claim.operation.id, operation_epoch=claim.operation.operation_epoch,
                 attempt_id=claim.attempt.id, reconciler_id=claim.attempt.claimed_by, lease_epoch=claim.attempt.lease_epoch)
    await reconciler.reconcile(claim, lease=lease, now=lambda: _NOW, run=_run)
    assert calls[:-1] == ([] if kind == "destroy" else ["admission"])
    assert calls[-1] == lease | dict(binding=binding, expected_binding_sha256=canonical_digest(binding),
                                    current_checkpoint=current, now=_NOW)
    assert client.reads == 1 and client.requests == []


@pytest.mark.parametrize("guard", ("missing", "delegate", "authority", "expired", "expired_during_io", "admission"))
async def test_successor_guard_rejects_before_durable_creation(guard):
    claim, _, values = successor_case()
    binding = PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    clock = [_NOW]

    class Authority:
        async def create_membership_successor(self, **kwargs):
            pytest.fail("invalid successor reached durable mutation")

    class Admission:
        async def assert_admission_ready(self, *, now):
            if guard == "admission":
                raise PersonalDevMembershipAdmissionError("expired positive admission")

    class Client:
        async def membership_checkpoint(self):
            if guard == "expired_during_io":
                clock[0] = binding.expires_at
            checkpoint = claim.operation.capacity_membership_envelope.expected_checkpoint
            return checkpoint if guard == "authority" else checkpoint.model_copy(update={
                "execution": binding.authority.execution, "namespace_id": binding.authority.namespace_id,
            })

    if guard == "expired":
        clock[0] = binding.expires_at + timedelta(seconds=1)
    reconciler = PersonalDevMembershipReconciler(
        authority=Authority(), client=Client(), installer=_Installer(), admission=Admission(),
        management_principal_id="wrong-delegate" if guard == "delegate" else binding.authority.management_principal_id,
        successor_bindings={} if guard == "missing" else {claim.operation.id: binding},
    )
    with pytest.raises((ValueError, PersonalDevMembershipAdmissionError)):
        await reconciler.reconcile(claim, lease={}, now=lambda: clock[0], run=_run)


async def test_service_loop_delivers_reviewed_successor_map_to_current_session_authority(monkeypatch):
    from loom_service import personal_dev_lifecycle as lifecycle
    from tests.unit.test_personal_dev_reconciler import _Executor, _Projector

    claim, _, values = successor_case()
    binding = PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    current = claim.operation.capacity_membership_envelope.expected_checkpoint.model_copy(update={
        "execution": binding.authority.execution, "namespace_id": binding.authority.namespace_id,
    })
    calls = []

    class Clock:
        @staticmethod
        def now(_tz):
            return _NOW

    class Authority:
        reads = 0

        async def claim_next_reconciliation(self, **kwargs):
            self.reads += 1
            if self.reads > 1:
                raise asyncio.CancelledError
            return claim

        async def create_membership_successor(self, **kwargs):
            calls.append(kwargs)

    class Admission:
        async def assert_admission_ready(self, **kwargs):
            pass

    authority = Authority()
    monkeypatch.setattr(lifecycle, "datetime", Clock)
    monkeypatch.setattr(lifecycle, "SessionPersonalDevReconciliationAuthority", lambda *a, **kw: authority)
    installer = _Installer()
    runtime = lifecycle.PersonalDevMembershipRuntime(
        installer=installer, client=_Client(claim.operation.capacity_membership_envelope, checkpoint=current),
        observer=None, admission=Admission(), management_principal_id=binding.authority.management_principal_id,
        successor_bindings={claim.operation.id: binding},
    )
    with pytest.raises(asyncio.CancelledError):
        await lifecycle.personal_dev_reconcile_run_loop(
            session_factory=None, executor=_Executor(), capacity_installer=installer,
            capacity_projector=_Projector(), limits=None, reconciler_id=claim.attempt.claimed_by,
            lease_seconds=60, poll_interval_seconds=0.001, membership=runtime,
        )
    assert len(calls) == 1
    assert calls[0]["binding"] == binding
    assert calls[0]["operation_id"] == claim.operation.id
    assert calls[0]["attempt_id"] == claim.attempt.id
