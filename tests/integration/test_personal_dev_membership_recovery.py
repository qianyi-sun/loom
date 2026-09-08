"""Reconciler response-loss recovery through real authenticated manager routes."""

from dataclasses import replace
from hashlib import sha256

import pytest

from loom.personal_dev_membership_client import (
    CapacityManagerPersonalDevMembershipClient,
    PersonalDevMembershipError,
)
from loom.personal_dev_membership_reconciler import PersonalDevMembershipReconciler
from loom_capacity_manager.auth import CapacityPrincipalVerifier
from tests.integration.test_capacity_membership import DELEGATE, _active_v3, _request
from tests.integration.test_capacity_membership_agent_auth import _http_client
from tests.integration.test_capacity_membership_api import MEMBERSHIP_TOKEN
from tests.integration.test_capacity_membership_outcomes import READ_TOKEN, _principal, _retire
from tests.integration.test_personal_dev_membership_client import _envelope
from tests.unit.test_personal_dev_membership_reconciler import (
    _Admission,
    _Installer,
    _pending_claim,
    _run,
)
from tests.unit.test_personal_dev_membership_recovery import _RecoveryAuthority


@pytest.mark.parametrize("retired", (False, True))
async def test_real_commit_response_loss_preserves_replay_until_authority_transition(
    capacity_session, retired
):
    fixture, active = await _active_v3(capacity_session)
    verifier = CapacityPrincipalVerifier((
        (sha256(READ_TOKEN.encode()).digest(), _principal()),
        (sha256(MEMBERSHIP_TOKEN.encode()).digest(), replace(
            _principal(), principal_id=DELEGATE, scopes=frozenset({"capacity:membership:manage"}),
        )),
    ))
    async with _http_client(capacity_session, fixture, active, verifier) as (http, _):
        manager = CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.test", bearer_token=MEMBERSHIP_TOKEN, http_client=http,
        )
        observer = CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.test", bearer_token=READ_TOKEN, http_client=http,
        )
        saved = _envelope(_request(active), await manager.membership_checkpoint(), identity=29810)
        projection = saved.request.projection
        base = _pending_claim()
        claim = replace(
            base,
            operation=replace(
                base.operation,
                id=projection.operation_id,
                idempotency_key=saved.idempotency_key,
                attempt_id=saved.observation.attempt_id,
                operation_epoch=projection.operation_epoch,
                kind=projection.operation_kind,
                subject_id=projection.subject_id,
                subject_incarnation=projection.subject_incarnation,
                owner_user_id=projection.owner_id,
                environment_name=projection.environment_name,
                deployment_generation=projection.deployment_generation,
                candidate_sha=projection.candidate_sha256,
                local_activation_sha256=projection.local_activation_sha256,
                min_slots=projection.min_slots, max_slots=projection.max_slots,
                capacity_membership_envelope=saved,
            ),
            candidate=replace(base.candidate, publication_sha256=projection.candidate_publication_sha256),
        )
        sends = []

        class LostResponse:
            async def mutate_membership(self, envelope):
                sends.append(envelope)
                receipt = await manager.mutate_membership(envelope)
                if len(sends) == 1:
                    if retired:
                        await _retire(capacity_session, fixture, active)
                    raise PersonalDevMembershipError("simulated lost response after real commit")
                return receipt

            async def membership_checkpoint(self):
                return await manager.membership_checkpoint()

        authority, installer = _RecoveryAuthority(), _Installer()
        driver = PersonalDevMembershipReconciler(
            admission=_Admission(),
            authority=authority, client=LostResponse(), installer=installer, observer=observer,
        )
        if retired:
            await driver.reconcile(
                claim, lease={"lease_epoch": 90}, now=lambda: saved.observation.observed_at, run=_run,
            )
            assert [name for name, _ in authority.calls] == ["historical"]
            assert authority.calls[0][1]["outcome"].receipt.result.revision == 1
            assert not installer.verifications and len(sends) == 1
        else:
            with pytest.raises(PersonalDevMembershipError, match="lost response"):
                await driver.reconcile(
                    claim, lease={"lease_epoch": 90}, now=lambda: saved.observation.observed_at, run=_run,
                )
            assert not authority.calls and not installer.verifications
            await driver.reconcile(
                claim, lease={"lease_epoch": 91}, now=lambda: saved.observation.observed_at, run=_run,
            )
            assert [name for name, _ in authority.calls] == ["record"]
            assert authority.calls[0][1]["response"].result.replayed
            assert sends == [saved, saved]
        assert saved.result is None and saved.historical_outcome is None
