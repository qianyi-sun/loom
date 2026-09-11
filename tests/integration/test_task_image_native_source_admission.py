"""Native lease operations retain registered inputs through fresh and replay paths."""

import hashlib
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
import rfc8785
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from loom.db.schema import (
    Task,
    TaskBundleSourceReference,
    TaskImageBuildProjection,
    TaskImageMaterialization,
)
from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_image_materialization import (
    ensure_task_image_materializations,
    task_image_materialization_key,
)
from loom_task_image_authority import materializations as native
from loom_task_image_authority.registry_credentials import (
    issue_session_registry_credential,
    record_session_publication_candidate,
)
from tests.integration.test_task_bundle_source_admission import _task
from tests.integration.test_task_bundle_source_admission import journal as journal
from tests.integration.test_task_bundle_source_journal import _module, _publish, _receipts, _upload
from tests.integration.test_task_image_authority_materializations import (
    CLAIM_ID,
    NOW,
    _active_authorization,
    _attempt,
)
from tests.integration.test_task_image_projection_store import _MemorySecretStore
from tests.integration.test_task_image_registry_credentials import (
    CREDENTIAL_ID,
    _candidate_request,
    _credential_request,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.unit.test_task_bundle_registration import _bundle


async def _setup(factory, tmp_path):
    directory = _bundle(tmp_path)
    config = directory / "task.toml"
    config.write_text(config.read_text().replace("[environment]", '[environment]\ncpu_arch = "arm64"'))
    spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(directory, task_id="benchmark/" + uuid4().hex),
        bucket="task-sources",
    )
    ticket = await _upload(factory, spec)
    await _receipts(factory, ticket)
    await _publish(factory, ticket)
    async with factory.begin() as session:
        authorization, *_ = await _active_authorization(session)
        image = (await ensure_task_image_materializations(session, task_row=_task(spec)))[0]
        image_id = image.id
    return authorization, spec, ticket, image_id


async def _claim(session, authorization):
    return await native.claim_session_materialization(
        session, authorization=authorization, claim_id=CLAIM_ID,
        now=NOW + timedelta(seconds=10), lease_seconds=300,
    )


async def _release_source(factory, spec, ticket, image_id, *, retire):
    async with factory.begin() as session:
        await session.scalar(select(TaskImageMaterialization).where(
            TaskImageMaterialization.id == image_id,
        ).with_for_update())
        await _module().release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="materialization", owner_id=str(image_id),
        )
        if retire:
            await _module().release_task_bundle_reference(
                session, source_id=spec.id, reference_kind="catalog", owner_id="catalog",
            )
            assert await _module().retire_task_bundle_source(
                session, incarnation_id=ticket.incarnation_id, now=NOW,
            )


@pytest.mark.parametrize("operation", [
    "claim", "claim-replay", "start", "start-replay", "heartbeat", "heartbeat-replay", "plan",
])
@pytest.mark.parametrize("retired", [False, True])
async def test_native_source_admission_on_fresh_and_replayed_authority(journal, tmp_path, operation, retired):
    authorization, spec, ticket, image_id = await _setup(journal, tmp_path)
    operation_id = uuid4()
    arguments = {}
    original_plan = None
    if operation != "claim":
        async with journal.begin() as session:
            _image, original_plan = await _claim(session, authorization)
            attempt = await _attempt(session)
            arguments = dict(
                authorization=authorization, materialization_id=image_id, attempt_id=attempt.id,
                lease_epoch=attempt.lease_epoch, now=NOW + timedelta(seconds=11),
            )
        if operation in {"start-replay", "heartbeat-replay"}:
            async with journal.begin() as session:
                method = native.start_session_materialization if operation == "start-replay" else native.heartbeat_session_materialization
                await method(session, operation_id=operation_id, **arguments)
    await _release_source(journal, spec, ticket, image_id, retire=retired)

    async def admit(session):
        if operation in {"claim", "claim-replay"}:
            result = await _claim(session, authorization)
            assert result[1].content_manifest_digest == spec.manifest.digest
            if original_plan is not None:
                assert result[1].model_dump_json() == original_plan.model_dump_json()
            return result
        if operation == "plan":
            return await native.get_session_materialization_build_plan(session, **arguments)
        method = native.start_session_materialization if operation.startswith("start") else native.heartbeat_session_materialization
        return await method(session, operation_id=operation_id, **arguments)

    async with journal() as session:
        if retired:
            with pytest.raises(native.TaskImageSessionMaterializationConflictError, match="source"):
                await admit(session)
            await session.rollback()
        else:
            assert await admit(session) is not None
            await session.commit()
    async with journal() as session:
        reference = await session.get(TaskBundleSourceReference, (spec.id, "materialization", str(image_id)))
        assert (reference is None) == retired


@pytest.mark.parametrize("operation", ["claim", "start", "heartbeat", "plan"])
@pytest.mark.parametrize("isolation", ["AUTOCOMMIT", "REPEATABLE READ", "SERIALIZABLE"])
async def test_native_preflight_rejects_unsafe_transaction_before_unrelated_autoflush(journal, tmp_path, operation, isolation):
    authorization, _spec, _ticket, image_id = await _setup(journal, tmp_path)
    arguments = {}
    if operation != "claim":
        async with journal.begin() as session:
            await _claim(session, authorization)
            attempt = await _attempt(session)
            arguments = dict(
                authorization=authorization, materialization_id=image_id, attempt_id=attempt.id,
                lease_epoch=attempt.lease_epoch, now=NOW + timedelta(seconds=11),
            )
    sessions = async_sessionmaker(journal.kw["bind"].execution_options(isolation_level=isolation), expire_on_commit=False)
    pending = Task(id="pending-" + uuid4().hex, checksum="f" * 64, config={})
    async with sessions() as session:
        session.add(pending)
        with pytest.raises((ValueError, native.TaskImageSessionMaterializationConflictError), match="READ COMMITTED"):
            if operation == "claim":
                await _claim(session, authorization)
            elif operation == "plan":
                await native.get_session_materialization_build_plan(session, **arguments)
            else:
                method = native.start_session_materialization if operation == "start" else native.heartbeat_session_materialization
                await method(session, operation_id=uuid4(), **arguments)
        assert pending in session.new
        await session.rollback()
    async with journal() as session:
        assert await session.get(Task, pending.id) is None


@pytest.mark.parametrize("containment", [False, True])
async def test_cleanup_remains_usable_after_source_retirement_in_serializable_transaction(journal, tmp_path, containment):
    authorization, spec, ticket, image_id = await _setup(journal, tmp_path)
    async with journal.begin() as session:
        await _claim(session, authorization)
        attempt = await _attempt(session)
    await _release_source(journal, spec, ticket, image_id, retire=True)
    sessions = async_sessionmaker(journal.kw["bind"].execution_options(isolation_level="SERIALIZABLE"), expire_on_commit=False)
    async with sessions.begin() as session:
        method = native.release_containment_failed_session_materialization if containment else native.release_session_materialization
        result = await method(
            session, authorization=authorization, materialization_id=image_id, attempt_id=attempt.id,
            lease_epoch=attempt.lease_epoch, operation_id=uuid4(), now=NOW + timedelta(seconds=12),
        )
        assert result.state == "queued"


@pytest.mark.parametrize("operation", ["credential", "candidate"])
@pytest.mark.parametrize("replay", [False, True])
@pytest.mark.parametrize("isolation", ["READ COMMITTED", "AUTOCOMMIT", "REPEATABLE READ", "SERIALIZABLE"])
async def test_registry_consumers_recheck_source_and_preflight_before_autoflush(
    journal, tmp_path, registry_issuer, operation, replay, isolation,
):
    authorization, spec, ticket, image_id = await _setup(journal, tmp_path)
    secrets = _MemorySecretStore()
    async with journal.begin() as session:
        image, _plan = await _claim(session, authorization)
        attempt = await _attempt(session)
    # The direct service receives already-authenticated authorization; this is the
    # same session token issued by _active_authorization, not a bypassed HTTP path.
    build_session = SimpleNamespace(session_token="loom_tibs_" + "B" * 64)
    credential = _credential_request(authorization, build_session, image, attempt)
    candidate = _candidate_request(authorization, build_session, image, attempt)

    async def issue(session):
        return await issue_session_registry_credential(
            session, authorization=authorization, request=credential, now=NOW + timedelta(seconds=11),
            issuer=registry_issuer, secret_store=secrets, credential_id_factory=lambda: CREDENTIAL_ID,
        )

    async def admit(session):
        if operation == "credential":
            return await issue(session)
        return await record_session_publication_candidate(
            session, authorization=authorization, request=candidate,
            now=NOW + timedelta(seconds=12), candidate_id_factory=uuid4,
        )

    if operation == "candidate":
        async with journal.begin() as session:
            await issue(session)
    if replay:
        async with journal.begin() as session:
            await admit(session)
    await _release_source(journal, spec, ticket, image_id, retire=True)
    sessions = async_sessionmaker(journal.kw["bind"].execution_options(isolation_level=isolation), expire_on_commit=False)
    pending = Task(id="pending-" + uuid4().hex, checksum="f" * 64, config={})
    async with sessions() as session:
        if isolation != "READ COMMITTED":
            session.add(pending)
        with pytest.raises(native.TaskImageSessionMaterializationConflictError, match="source" if isolation == "READ COMMITTED" else "READ COMMITTED"):
            await admit(session)
        if isolation != "READ COMMITTED":
            assert pending in session.new
        await session.rollback()
    async with journal() as session:
        assert await session.get(Task, pending.id) is None


@pytest.mark.parametrize("operation", ["claim", "start", "heartbeat", "start-replay", "heartbeat-replay", "plan"])
@pytest.mark.parametrize("drift", ["manifest", "component", "downgrade"])
async def test_validly_hashed_retained_receipt_must_match_admitted_frozen_inputs(journal, tmp_path, operation, drift):
    authorization, _spec, _ticket, image_id = await _setup(journal, tmp_path)
    async with journal.begin() as session:
        _image, plan = await _claim(session, authorization)
        attempt = await _attempt(session)
        arguments = dict(
            authorization=authorization, materialization_id=image_id, attempt_id=attempt.id,
            lease_epoch=attempt.lease_epoch, now=NOW + timedelta(seconds=11),
        )
    operation_id = uuid4()
    if operation.endswith("-replay"):
        async with journal.begin() as session:
            method = native.start_session_materialization if operation.startswith("start") else native.heartbeat_session_materialization
            await method(session, operation_id=operation_id, **arguments)
    payload = plan.model_dump(mode="json")
    if drift == "manifest":
        payload["bundle_content_manifest_sha256"] = "f" * 64
        payload["bundle_prefix"] = "replacement/" + "f" * 64 + "/"
    elif drift == "component":
        payload["components"][0]["dockerfile_path"] = "another-Dockerfile"
    else:
        del payload["bundle_content_manifest_sha256"]
        payload["schema_version"] = "loom.task-image-build-plan.v1"
    async with journal.begin() as session:
        stored = await _attempt(session)
        # These fields have no immutable trigger: exercise the actual retained
        # receipt boundary without disabling database protections.
        stored.claim_plan_json = payload
        stored.claim_plan_sha256 = hashlib.sha256(rfc8785.dumps(payload)).hexdigest()
    async with journal() as session:
        with pytest.raises(native.TaskImageSessionMaterializationConflictError, match="frozen"):
            if operation == "claim":
                await _claim(session, authorization)
            elif operation == "plan":
                await native.get_session_materialization_build_plan(session, **arguments)
            else:
                method = native.start_session_materialization if operation.startswith("start") else native.heartbeat_session_materialization
                await method(session, operation_id=operation_id, **arguments)
        await session.rollback()


async def test_native_claim_refreshes_cached_materialization_epoch(journal, tmp_path):
    authorization, _spec, _ticket, image_id = await _setup(journal, tmp_path)
    async with journal() as stale:
        cached = await stale.get(TaskImageMaterialization, image_id)
        assert cached.lease_epoch == 0
        async with journal.begin() as current:
            await current.execute(update(TaskImageMaterialization).where(
                TaskImageMaterialization.id == image_id,
            ).values(lease_epoch=3))
        claimed, _plan = await _claim(stale, authorization)
        assert claimed.lease_epoch == 4
        await stale.commit()
    async with journal() as session:
        assert (await session.get(TaskImageMaterialization, image_id)).lease_epoch == 4


async def test_native_claim_refuses_an_unregistered_source_without_creating_attempt(journal, tmp_path):
    directory = _bundle(tmp_path)
    config = directory / "task.toml"
    config.write_text(config.read_text().replace("[environment]", '[environment]\ncpu_arch = "arm64"'))
    spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(directory, task_id="benchmark/" + uuid4().hex),
        bucket="task-sources",
    )
    async with journal.begin() as session:
        authorization, *_ = await _active_authorization(session)
        image = TaskImageMaterialization(
            task_id=spec.catalog_task_id, task_checksum=spec.manifest.task_checksum,
            cpu_arch="arm64", task_config=spec.task_config, task_source=spec.source_uri,
            task_source_provenance=spec.provenance, bundle_content_manifest_sha256=spec.manifest.digest,
            materialization_key=task_image_materialization_key(
                task_id=spec.catalog_task_id, task_checksum=spec.manifest.task_checksum,
                cpu_arch="arm64", bundle_content_manifest_sha256=spec.manifest.digest,
            ),
        )
        session.add(image)
        await session.flush()
        image_id = image.id
    async with journal() as session:
        with pytest.raises(native.TaskImageSessionMaterializationConflictError, match=r"source.*absent"):
            await _claim(session, authorization)
        await session.rollback()
    async with journal() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        assert image.state == "queued" and image.lease_epoch == 0
        assert (await session.scalars(select(native.TaskImageMaterializationAttempt))).all() == []


@pytest.mark.parametrize("operation", ["claim", "start", "heartbeat", "plan"])
async def test_native_admission_rejects_pending_image_edits_without_discarding_them(journal, tmp_path, operation):
    authorization, _spec, _ticket, image_id = await _setup(journal, tmp_path)
    arguments = {}
    if operation != "claim":
        async with journal.begin() as session:
            await _claim(session, authorization)
            attempt = await _attempt(session)
            arguments = dict(
                authorization=authorization, materialization_id=image_id, attempt_id=attempt.id,
                lease_epoch=attempt.lease_epoch, now=NOW + timedelta(seconds=11),
            )
    async with journal() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        image.claimed_by = "unflushed-caller-edit"
        with pytest.raises(native.TaskImageSessionMaterializationConflictError, match="unflushed"):
            if operation == "claim":
                await _claim(session, authorization)
            elif operation == "plan":
                await native.get_session_materialization_build_plan(session, **arguments)
            else:
                method = native.start_session_materialization if operation == "start" else native.heartbeat_session_materialization
                await method(session, operation_id=uuid4(), **arguments)
        assert image in session.dirty and image.claimed_by == "unflushed-caller-edit"
        await session.rollback()


async def test_native_claim_rechecks_cached_parent_revocation(journal, tmp_path):
    authorization, _spec, _ticket, _image_id = await _setup(journal, tmp_path)
    async with journal() as stale:
        cached = await stale.scalar(select(TaskImageBuildProjection).where(
            TaskImageBuildProjection.grant_id == authorization.grant_id,
        ))
        assert cached.state == "exchanged"
        async with journal.begin() as current:
            await current.execute(update(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == authorization.grant_id,
            ).values(state="revoked", revoked_at=NOW + timedelta(seconds=10), revoke_reason="guard_attestation_lost"))
        with pytest.raises(native.TaskImageSessionMaterializationAuthorizationError):
            await _claim(stale, authorization)
        await stale.rollback()
