"""The service loop dispatches persisted membership, including recovery-only work."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from loom.personal_dev_membership_admission import PersonalDevMembershipAdmissionError
from loom_service import personal_dev_lifecycle as lifecycle
from tests.unit.test_personal_dev_membership_reconciler import (
    _Admission,
    _Client,
    _Installer,
    _pending_claim,
)
from tests.unit.test_personal_dev_membership_recovery import _Observer, _RecoveryAuthority
from tests.unit.test_personal_dev_reconciler import _Executor, _Projector


@pytest.mark.parametrize("expired", (False, True))
async def test_service_loop_uses_current_session_authority_and_separate_observer(monkeypatch, expired):
    claim = _pending_claim()
    claim = replace(claim, attempt=replace(
        claim.attempt, lease_expires_at=datetime.now(UTC) + timedelta(minutes=2),
    ))
    saved = claim.operation.capacity_membership_envelope

    class Authority(_RecoveryAuthority):
        reads = 0

        async def claim_next_reconciliation(self, **kwargs):
            self.reads += 1
            if self.reads > 1:
                raise asyncio.CancelledError
            return claim

    class Expired:
        async def assert_admission_ready(self, *, now):
            raise PersonalDevMembershipAdmissionError("expired")

    authority, installer, client = Authority(), _Installer(), _Client(saved)
    observer, projector = _Observer(saved, "committed"), _Projector()
    monkeypatch.setattr(lifecycle, "SessionPersonalDevReconciliationAuthority", lambda *a, **k: authority)
    runtime = lifecycle.PersonalDevMembershipRuntime(
        client=client, observer=observer, installer=installer,
        admission=Expired() if expired else _Admission(),
        management_principal_id=saved.management_principal_id,
    )
    with pytest.raises(asyncio.CancelledError):
        await lifecycle.personal_dev_reconcile_run_loop(
            session_factory=None, executor=_Executor(), capacity_installer=installer,
            capacity_projector=projector, limits=None, reconciler_id="reconciler-a",
            lease_seconds=60, poll_interval_seconds=0.001, membership=runtime,
        )
    assert not projector.requests
    assert [kind for kind, _ in authority.calls] == ["historical" if expired else "record"]
    assert authority.calls[0][1]["lease_epoch"] == claim.attempt.lease_epoch
    assert authority.calls[0][1]["operation_id"] == claim.operation.id
    assert client.requests == ([] if expired else [saved])
    assert observer.requests == ([saved] if expired else [])
