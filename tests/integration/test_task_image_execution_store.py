"""Retained strong source/publication -> signed grant -> durable online consume.

Signing uses disposable keys; build/registry observations are fixture inputs,
not a native-kernel or live-fleet acceptance result.
"""

import asyncio
import json
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import delete, select, text, update
from sqlalchemy.exc import DBAPIError

from loom.db.schema import (
    TaskImageExecutionGrant,
    TaskImageExecutionStart,
    TaskImageMaterialization,
    TaskImagePublicationKey,
    TaskImagePublicationState,
    Trial,
    TrialTaskImageMaterialization,
    Worker,
)
from loom.models.task import TaskConfig
from loom.pipeline.keys import canonical_digest
from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_image_materialization import ensure_task_image_materializations
from loom_task_image_authority import execution_store as store
from loom_task_image_authority.execution_start import ExecutionStartRequest
from loom_task_image_authority.publication_keyset import ExecutionGrantTrustRoot
from loom_task_image_authority.publication_keyset_store import finalize_keyset, prepare_keyset
from loom_worker.task_image_execution import WorkerTaskImageExecution
from tests.integration import test_task_image_publication_completion as completion
from tests.integration.test_task_bundle_source_admission import _task
from tests.integration.test_task_bundle_source_journal import _publish, _receipts, _upload
from tests.integration.test_task_image_execution_claim_authority import seed
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import NOW
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.integration.test_trial_legacy_claim_identity import _TOKEN_HASH
from tests.unit.test_task_bundle_registration import _bundle
from tests.unit.test_task_image_execution_grant import sign_grant
from tests.unit.test_task_image_publication_keyset import _sign, _time


async def ready(factory, issuer, tmp_path, monkeypatch):
    directory = _bundle(tmp_path)
    toml = directory / "task.toml"
    toml.write_text(toml.read_text().replace("[environment]", '[environment]\ncpu_arch="x86_64"\nsidecars=[]'))
    spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(directory, task_id="benchmark/" + uuid4().hex),
        bucket="task-sources",
    )
    ticket = await _upload(factory, spec)
    await _receipts(factory, ticket)
    await _publish(factory, ticket)

    async def queued(session):
        return (await ensure_task_image_materializations(session, task_row=_task(spec)))[0]

    monkeypatch.setattr(completion, "_queued_materialization", queued)
    async with factory() as session:
        values = await completion._signed_job(session, issuer)
        await completion._complete(session, values)
        await session.commit()
    now = NOW + timedelta(seconds=15)
    private = Ed25519PrivateKey.generate()
    root = ExecutionGrantTrustRoot(
        "execution-1", values[0].snapshot.environment, private.public_key().public_bytes_raw(),
        NOW - timedelta(days=1), NOW + timedelta(days=1),
    )
    async with factory.begin() as session:
        prepared = await prepare_keyset(session, trust_root=root)
    keyset = _sign(dict(
        schema="loom.task-image-publication-keyset/v1", environment=root.environment,
        keyset_version=prepared.proposed_state.keyset_version,
        revocation_epoch=prepared.proposed_state.revocation_epoch,
        issued_at=_time(now), expires_at=_time(now + timedelta(minutes=5)),
        keys=[key.model_dump(mode="json", exclude_none=True) for key in prepared.keys],
    ), private)
    async with factory.begin() as session:
        await finalize_keyset(session, preparation=prepared, wire=keyset, trust_root=root, clock=lambda: now)
    claim = await seed(factory)
    # This fixture tests authority over an already claimed row. Native-ready
    # selection in claim_work remains separate pending server composition.
    async with factory.begin() as session:
        await session.merge(_task(spec))
        await session.flush()
        trial = await session.get(Trial, UUID(claim.trial_id))
        trial.task_id = spec.catalog_task_id
        trial.requires_caps = dict(trial.requires_caps, cpu_arch="x86_64")
        image = (await session.scalars(select(TaskImageMaterialization))).one()
        session.add(TrialTaskImageMaterialization(trial_id=UUID(claim.trial_id), materialization_id=image.id))
        worker = await session.get(Worker, UUID(claim.worker_id))
        worker.capabilities = [dict(cap, cpu_arch="x86_64") for cap in worker.capabilities]
        snapshot = dict(worker.capability_snapshot_json, cpu_arch="x86_64")
        worker.capability_snapshot_json = snapshot
        worker.capability_snapshot_digest = canonical_digest(snapshot)
    return claim, root, private, now, directory


async def test_current_signed_grant_consumes_once_after_real_source_verification(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch,
):
    assert hasattr(store, "prepare_execution_grant"), "signed grant issuance missing"
    factory = registry_authority_session
    claim, root, private, now, directory = await ready(factory, registry_issuer, tmp_path, monkeypatch)
    common = dict(
        claim=claim, worker_token_hash=_TOKEN_HASH, trust_root=root,
        purpose="production", shadow_campaign_id=None, clock=lambda: now,
    )
    async with factory.begin() as session:
        grant = await store.prepare_execution_grant(session, **common)
    wire = sign_grant(grant.model_dump(mode="json", by_alias=True, exclude_none=True), private)
    async with factory.begin() as session:
        delivery = await store.finalize_execution_grant(session, wire=wire, **common)
    requests = []

    async def consume(request):
        requests.append(request)
        async with factory.begin() as session:
            return await store.consume_execution_start(session, request=request, **common)

    consumer = WorkerTaskImageExecution(
        wire=delivery.grant_envelope.encode(), plan_wire=delivery.frozen_plan.encode(),
        publication_wires=tuple(item.encode() for item in delivery.publications),
        keyset_wire=delivery.keyset.encode(), trust_root=root, expected_claim=claim,
        expected_purpose="production", expected_shadow_campaign_id=None, task_dir=directory,
        task_config=TaskConfig.model_validate(json.loads(grant.canonical_task_config)),
        task_checksum=grant.task_checksum, cpu_arch="x86_64", task_image=grant.components[0].image,
        consume=consume, clock=lambda: now,
    )
    assert await consumer.authorize() is True
    # Fresh transaction and process-independent request: not the client's latch.
    with pytest.raises(ValueError, match="consumed"):
        async with factory.begin() as session:
            await store.consume_execution_start(session, request=requests[0], **common)
    async with factory.begin() as session:
        started = (await session.scalars(select(TaskImageExecutionStart))).one()
        assert started.claim_id == UUID(claim.claim_id)
        assert started.request_sha256 == requests[0].digest
        original = (await session.scalars(select(TaskImageExecutionGrant))).one()
        for mutation in (
            update(TaskImageExecutionGrant).where(TaskImageExecutionGrant.grant_id == original.grant_id).values(revision=2),
            update(TaskImageExecutionGrant).where(TaskImageExecutionGrant.grant_id == original.grant_id).values(canonical_envelope=None, envelope_sha256=None),
            update(TaskImageExecutionStart).where(TaskImageExecutionStart.claim_id == started.claim_id).values(request_sha256="1" * 64),
            delete(TaskImageExecutionStart).where(TaskImageExecutionStart.claim_id == started.claim_id),
        ):
            with pytest.raises(DBAPIError, match="immutable"):
                async with session.begin_nested():
                    await session.execute(mutation.execution_options(synchronize_session=False))
        assert original.canonical_envelope == wire


def start_request(grant, wire):
    import hashlib
    return ExecutionStartRequest.model_validate(dict(
        schema="loom.task-image-execution-start-request/v1", grant_id=grant.grant_id,
        revision=grant.revision, envelope_sha256=hashlib.sha256(wire).hexdigest(),
        claim=grant.claim, keyset_sha256=grant.keyset_sha256,
        keyset_version=grant.keyset_version, revocation_epoch=grant.revocation_epoch,
    ))


@pytest.mark.parametrize("change", ["pending", "key_revoked", "grant_revoked", "cancelled", "epoch", "request", "expired", "materialization_retried"])
async def test_start_rechecks_live_authority_after_signature(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch, change,
):
    factory = registry_authority_session
    claim, root, private, now, _ = await ready(factory, registry_issuer, tmp_path, monkeypatch)
    common = dict(claim=claim, worker_token_hash=_TOKEN_HASH, trust_root=root,
                  purpose="production", shadow_campaign_id=None, clock=lambda: now)
    async with factory.begin() as session:
        grant = await store.prepare_execution_grant(session, **common)
    wire = sign_grant(grant.model_dump(mode="json", by_alias=True, exclude_none=True), private)
    if change != "pending":
        async with factory.begin() as session:
            await store.finalize_execution_grant(session, wire=wire, **common)
    request = start_request(grant, wire)
    async with factory.begin() as session:
        if change == "key_revoked":
            key = (await session.scalars(select(TaskImagePublicationKey))).one()
            key.status, key.revoked_at = "revoked", now
        elif change == "grant_revoked":
            row = (await session.scalars(select(TaskImageExecutionGrant))).one()
            row.revoked_at = now
        elif change == "cancelled":
            row = await session.get(Trial, UUID(claim.trial_id))
            row.state = "cancelled"
        elif change == "epoch":
            worker = await session.get(Worker, UUID(claim.worker_id))
            worker.lease_epoch += 1
        elif change == "request":
            request = request.model_copy(update={"keyset_sha256": "1" * 64})
        elif change == "expired":
            now += timedelta(seconds=121)
        elif change == "materialization_retried":
            from loom_control_plane.task_image_materializations import (
                retry_task_image_materialization,
            )

            await retry_task_image_materialization(session, materialization_id=UUID(grant.materialization_id))
    with pytest.raises(ValueError):
        async with factory.begin() as session:
            await store.consume_execution_start(session, request=request, **common)
    async with factory.begin() as session:
        assert not (await session.scalars(select(TaskImageExecutionStart))).all()


async def test_refresh_fences_old_grant_and_concurrent_consumers(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch,
):
    factory = registry_authority_session
    claim, root, private, now, _ = await ready(factory, registry_issuer, tmp_path, monkeypatch)
    common = dict(claim=claim, worker_token_hash=_TOKEN_HASH, trust_root=root,
                  purpose="production", shadow_campaign_id=None, clock=lambda: now)

    async def issue():
        async with factory.begin() as session:
            grant = await store.prepare_execution_grant(session, **common)
        wire = sign_grant(grant.model_dump(mode="json", by_alias=True, exclude_none=True), private)
        async with factory.begin() as session:
            await store.finalize_execution_grant(session, wire=wire, **common)
        return grant, start_request(grant, wire)

    first, old = await issue()
    assert (await issue())[0] == first  # Lost issuance acknowledgement is replayable before start.
    now += timedelta(seconds=121)
    second, current = await issue()
    assert second.grant_id == first.grant_id and second.revision == first.revision + 1
    with pytest.raises(ValueError):
        async with factory.begin() as session:
            await store.consume_execution_start(session, request=old, **common)

    async def consume():
        async with factory.begin() as session:
            return await store.consume_execution_start(session, request=current, **common)

    results = await asyncio.gather(consume(), consume(), return_exceptions=True)
    assert sum(isinstance(result, ValueError) for result in results) == 1
    assert sum(not isinstance(result, BaseException) for result in results) == 1
    with pytest.raises(ValueError, match="consumed"):
        await issue()
    async with factory.begin() as session:
        assert len((await session.scalars(select(TaskImageExecutionStart))).all()) == 1
        assert len((await session.scalars(select(TaskImageExecutionGrant))).all()) == 2


async def test_revocation_commit_wins_over_a_start_waiting_on_publication_state(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch,
):
    factory = registry_authority_session
    claim, root, private, now, _ = await ready(factory, registry_issuer, tmp_path, monkeypatch)
    common = dict(claim=claim, worker_token_hash=_TOKEN_HASH, trust_root=root,
                  purpose="production", shadow_campaign_id=None, clock=lambda: now)
    async with factory.begin() as session:
        grant = await store.prepare_execution_grant(session, **common)
    wire = sign_grant(grant.model_dump(mode="json", by_alias=True, exclude_none=True), private)
    async with factory.begin() as session:
        await store.finalize_execution_grant(session, wire=wire, **common)
    pid = asyncio.get_running_loop().create_future()

    async def consume():
        async with factory.begin() as session:
            pid.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
            return await store.consume_execution_start(session, request=start_request(grant, wire), **common)

    blocked = None
    try:
        async with factory.begin() as revoker:
            await revoker.scalar(select(TaskImagePublicationState).with_for_update())
            blocked = asyncio.create_task(consume())
            async with asyncio.timeout(3):
                backend = await pid
                while not await revoker.scalar(text("SELECT cardinality(pg_blocking_pids(:pid)) > 0"), {"pid": backend}):
                    await asyncio.sleep(0.01)
            key = (await revoker.scalars(select(TaskImagePublicationKey))).one()
            key.status, key.revoked_at = "revoked", now
        with pytest.raises(ValueError):
            await asyncio.wait_for(blocked, 3)
    finally:
        if blocked is not None:
            blocked.cancel()
            await asyncio.gather(blocked, return_exceptions=True)
    async with factory.begin() as session:
        assert not (await session.scalars(select(TaskImageExecutionStart))).all()
