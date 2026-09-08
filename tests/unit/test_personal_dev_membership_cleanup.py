"""Release gating applies before every destructive runtime entrypoint."""

import json
from dataclasses import replace
from importlib import import_module

import pytest

from loom.personal_dev_capacity_runtime import KubectlPersonalDevCapacityInstaller
from loom.personal_dev_membership_checkpoint import PersonalDevMembershipEnvelopeV1
from loom.personal_dev_runtime import PersonalDevPreparationRuntime
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.membership_subject_status import parse_membership_release_observation
from tests.unit.test_personal_dev_membership_checkpoint import membership_response
from tests.unit.test_personal_dev_membership_reconciler import _NOW, _Client, _pending_claim, _run
from tests.unit.test_personal_dev_membership_recovery import _RecoveryAuthority
from tests.unit.test_personal_dev_membership_subject_client import _response


def retiring_claim(checkpoint="cleanup_pending", *, released=False, keep_data=False):
    claim = _pending_claim()
    old = claim.operation.capacity_membership_envelope
    request = old.request.model_copy(
        update={
            "projection": old.request.projection.model_copy(
                update={"operation_kind": "destroy", "min_slots": 0, "max_slots": 0}
            )
        }
    )
    envelope = PersonalDevMembershipEnvelopeV1.model_validate(
        old.model_dump(mode="python")
        | {
            "request": request,
            "request_sha256": canonical_digest(request),
            "result": membership_response(request, key=old.idempotency_key),
        }
    )
    if released:
        _, release = _response(envelope, "verified")
        envelope = PersonalDevMembershipEnvelopeV1.model_validate_json(
            json.dumps(envelope.model_dump(mode="json") | {"release": release})
        )
    return replace(
        claim,
        operation=replace(
            claim.operation, kind="destroy", state="running", checkpoint=checkpoint,
            capacity_membership_envelope=envelope, keep_data=keep_data,
        ),
        environment=replace(
            claim.environment, status="deleting", operation_step=checkpoint, keep_data=keep_data
        ),
        attempt=replace(claim.attempt, state="running", checkpoint=checkpoint),
    )


class _Effects:
    def __init__(self):
        self.calls = []

    async def seal(self, *args):
        self.calls.append("seal")

    async def destroy(self, *args):
        self.calls.append("destroy")

    async def remove_buckets(self, *args):
        self.calls.append("buckets")

    async def delete(self, *args):
        self.calls.append("delete")


def cleanup_runtime():
    effects = _Effects()
    installer = object.__new__(KubectlPersonalDevCapacityInstaller)
    installer._database = effects
    runtime = PersonalDevPreparationRuntime(
        config=None, sql=effects, buckets=effects, vault=effects,
        object_store_tenant=effects, cluster=effects, access=effects,
    )
    return effects, installer, runtime


_ACTIONS = (
    ("seal", "release_verified"),
    ("destroy", "namespace_deleted"),
    ("delete_namespace", "local_authority_sealed"),
    ("delete_buckets", "database_deleted"),
    ("delete_tenant", "buckets_deleted"),
    ("delete_credentials", "tenant_deleted"),
)


@pytest.mark.parametrize("action,checkpoint", _ACTIONS)
async def test_runtime_requires_persisted_release_before_any_cleanup(action, checkpoint):
    effects, installer, runtime = cleanup_runtime()
    target = installer if action in {"seal", "destroy"} else runtime
    with pytest.raises(ValueError, match="release"):
        await getattr(target, action)(retiring_claim(checkpoint))
    assert not effects.calls


@pytest.mark.parametrize("action,checkpoint", _ACTIONS)
async def test_runtime_accepts_exact_released_destroy_checkpoint(action, checkpoint):
    effects, installer, runtime = cleanup_runtime()
    target = installer if action in {"seal", "destroy"} else runtime
    await getattr(target, action)(retiring_claim(checkpoint, released=True))
    assert len(effects.calls) == 1


@pytest.mark.parametrize("tamper", ("operation", "owner", "attempt", "incarnation", "checkpoint"))
async def test_runtime_rejects_release_attached_to_different_lifecycle_claim(tamper):
    effects, installer, _ = cleanup_runtime()
    claim = retiring_claim("release_verified", released=True)
    if tamper == "operation":
        claim = replace(claim, operation=replace(claim.operation, operation_epoch=999))
    elif tamper == "owner":
        claim = replace(claim, operation=replace(claim.operation, owner_user_id=claim.operation.id))
    elif tamper == "attempt":
        claim = replace(claim, attempt=replace(claim.attempt, operation_id=claim.attempt.id))
    elif tamper == "incarnation":
        claim = replace(claim, environment=replace(claim.environment, subject_incarnation=claim.attempt.id))
    else:
        claim = replace(claim, operation=replace(claim.operation, checkpoint="cleanup_pending"))
    with pytest.raises(ValueError):
        await installer.seal(claim)
    assert not effects.calls


class _CleanupAuthority(_RecoveryAuthority):
    async def record_capacity_membership_release(self, **kwargs):
        self.calls.append(("release", kwargs))

    async def advance_destroy_checkpoint(self, **kwargs):
        self.calls.append(("advance", kwargs))


@pytest.mark.parametrize("kind", ("pending", "verified"))
async def test_destroy_queries_release_without_sealing_or_deleting(kind):
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = retiring_claim()
    saved = claim.operation.capacity_membership_envelope
    _, payload = _response(saved, kind)
    release = parse_membership_release_observation(json.dumps(payload))
    observed = []

    class Observer:
        async def membership_subject_release(self, envelope):
            observed.append(envelope)
            return release

    authority = _CleanupAuthority()
    effects, installer, runtime = cleanup_runtime()
    client = _Client(saved)
    driver = module.PersonalDevMembershipReconciler(
        authority=authority, client=client, installer=installer,
        observer=Observer(), cleanup_executor=runtime,
    )
    await driver.reconcile(claim, lease={"lease_epoch": 99}, now=lambda: _NOW, run=_run)
    assert observed == [saved] and not effects.calls and not client.requests
    if kind == "pending":
        assert not authority.calls
    else:
        assert authority.calls == [("release", {"lease_epoch": 99, "now": _NOW, "release": release})]


@pytest.mark.parametrize("checkpoint,next_checkpoint,keep_data", (
    ("release_verified", "local_authority_sealed", False),
    ("local_authority_sealed", "namespace_deleted", False),
    ("namespace_deleted", "database_deleted", False),
    ("database_deleted", "buckets_deleted", False),
    ("buckets_deleted", "tenant_deleted", False),
    ("tenant_deleted", "complete", False),
    ("namespace_deleted", "tenant_deleted", True),
))
async def test_destroy_advances_one_released_checkpoint(checkpoint, next_checkpoint, keep_data):
    module = import_module("loom.personal_dev_membership_reconciler")
    claim = retiring_claim(checkpoint, released=True, keep_data=keep_data)
    authority = _CleanupAuthority()
    effects, installer, runtime = cleanup_runtime()
    driver = module.PersonalDevMembershipReconciler(
        authority=authority, client=_Client(claim.operation.capacity_membership_envelope),
        installer=installer, cleanup_executor=runtime,
    )
    await driver.reconcile(claim, lease={"lease_epoch": 99}, now=lambda: _NOW, run=_run)
    assert len(effects.calls) == 1
    assert authority.calls == [("advance", {
        "lease_epoch": 99, "now": _NOW,
        "expected_checkpoint": checkpoint, "checkpoint": next_checkpoint,
    })]
