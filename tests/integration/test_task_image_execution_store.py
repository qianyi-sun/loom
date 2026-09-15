"""Retained strong source/publication -> signed grant -> durable online consume.

Signing uses disposable keys; build/registry observations are fixture inputs,
not a native-kernel or live-fleet acceptance result.
"""

import json
from datetime import timedelta
from uuid import UUID, uuid4

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import select, update

from loom.db.schema import TaskImageExecutionStart, TaskImageMaterialization, Trial, TrialTaskImageMaterialization, Worker
from loom.models.task import TaskConfig
from loom.pipeline.keys import canonical_digest
from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_image_materialization import ensure_task_image_materializations
from loom_task_image_authority import execution_store as store
from loom_task_image_authority.publication_keyset import ExecutionGrantTrustRoot
from loom_task_image_authority.publication_keyset_store import finalize_keyset, prepare_keyset
from loom_worker.task_image_execution import WorkerTaskImageExecution
from tests.integration import test_task_image_publication_completion as completion
from tests.integration.test_task_bundle_source_admission import _task
from tests.integration.test_task_bundle_source_journal import _publish, _receipts, _upload
from tests.integration.test_task_image_execution_claim_authority import seed
from tests.integration.test_task_image_publication_jobs import registry_authority_session as registry_authority_session
from tests.integration.test_task_image_registry_credentials import NOW, registry_issuer as registry_issuer
from tests.integration.test_trial_legacy_claim_identity import _TOKEN_HASH
from tests.unit.test_task_bundle_registration import _bundle
from tests.unit.test_task_image_execution_grant import sign_grant
from tests.unit.test_task_image_publication_keyset import _sign, _time


async def ready(factory, issuer, tmp_path, monkeypatch):
    directory = _bundle(tmp_path)
    toml = directory / "task.toml"
    toml.write_text(toml.read_text().replace("[environment]", '[environment]\ncpu_arch="arm64"\nsidecars=[]'))
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
        await session.execute(update(Trial).where(Trial.id == UUID(claim.trial_id)).values(task_id=spec.catalog_task_id))
        image = (await session.scalars(select(TaskImageMaterialization))).one()
        session.add(TrialTaskImageMaterialization(trial_id=UUID(claim.trial_id), materialization_id=image.id))
        worker = await session.get(Worker, UUID(claim.worker_id))
        snapshot = dict(worker.capability_snapshot_json, cpu_arch="arm64")
        worker.capability_snapshot_json = snapshot
        worker.capability_snapshot_digest = canonical_digest(snapshot)
    return claim, root, private, now, directory


async def test_current_signed_grant_consumes_once_after_real_source_verification(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch,
):
    import pytest

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
        task_checksum=grant.task_checksum, cpu_arch="arm64", task_image=grant.components[0].image,
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
