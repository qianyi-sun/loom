"""Trusted DB validation must retain the authenticated path's live authority checks."""

# ruff: noqa: F811

from __future__ import annotations

import asyncio
import hashlib
from datetime import timedelta

import pytest
import rfc8785
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import defer

from loom.db.schema import (
    TaskImageBuildContainmentAttestation,
    TaskImageBuildGrant,
    TaskImageBuildProjection,
    TaskImageBuildSessionGeneration,
)
from loom_task_image_authority import store
from loom_task_image_authority.contracts import TaskImageSessionRenewalV1
from tests.integration.test_task_image_projection_store import (
    GRANT_ID,
    NEXT_SESSION_ID,
    NOW,
    PROVIDER_RELEASE_SHA256,
    RENEWAL_ID,
    SUPERVISOR_SHA256,
    _attestation,
    _exchange,
    _MemorySecretStore,
    _project_grant,
    projection_session,  # noqa: F401
)


async def _initial(session: AsyncSession):
    secrets = _MemorySecretStore()
    _, principal, proof, receipt = await _project_grant(session, secret_store=secrets)
    initial = await store.exchange_task_image_bootstrap(
        session,
        principal=principal,
        request=_exchange(receipt),
        now=NOW + timedelta(seconds=8),
        secret_store=secrets,
        session_token_factory=lambda: "loom_tibs_" + "B" * 64,
    )
    return initial, principal, proof, secrets


@pytest.mark.parametrize("renewed", [False, True])
async def test_internal_current_session_matches_bearer_without_secret_access(
    projection_session: async_sessionmaker[AsyncSession],
    renewed: bool,
) -> None:
    async with projection_session() as session:
        initial, principal, proof, secrets = await _initial(session)
        current = initial
        if renewed:
            attestation = _attestation(proof, generation=2)
            current = await store.renew_task_image_build_session(
                session,
                principal=principal,
                request=TaskImageSessionRenewalV1(
                    renewal_id=RENEWAL_ID,
                    grant_id=GRANT_ID,
                    session_id=initial.session_id,
                    session_generation=1,
                    session_token=initial.session_token,
                    attestation=attestation,
                    observed_at=attestation.issued_at,
                ),
                now=NOW + timedelta(seconds=13),
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "C" * 64,
                session_id_factory=lambda: NEXT_SESSION_ID,
            )
        secrets.values.clear()
        now = NOW + timedelta(seconds=14)
        internal = await store.validate_current_task_image_build_session(
            session,
            grant_id=GRANT_ID,
            clock=lambda: now,
        )
        authenticated = await store.authorize_task_image_build_session(
            session,
            grant_id=GRANT_ID,
            session_id=current.session_id,
            session_generation=2 if renewed else 1,
            raw_session_token=current.session_token,
            now=now,
        )
        assert internal == authenticated
        assert internal.grant_id == GRANT_ID
        assert internal.session_id == current.session_id
        assert internal.session_generation == (2 if renewed else 1)
        assert internal.attestation_generation == (2 if renewed else 1)
        assert internal.authority_version == 2
        assert internal.builder_release_sha256 == PROVIDER_RELEASE_SHA256
        assert internal.supervisor_executable_sha256 == SUPERVISOR_SHA256
        assert internal.pool_id == "staging-gb10-task-image"
        assert current.session_token not in repr(internal)
        for session_id, generation, token in [
            (current.session_id, 2 if renewed else 1, "loom_tibs_" + "Z" * 64),
            (initial.session_id, 1 if renewed else 2, initial.session_token),
        ]:
            with pytest.raises(store.TaskImageProjectionAuthorizationError):
                await store.authorize_task_image_build_session(
                    session,
                    grant_id=GRANT_ID,
                    session_id=session_id,
                    session_generation=generation,
                    raw_session_token=token,
                    now=now,
                )


@pytest.mark.parametrize(
    "change",
    [
        "revoked",
        "nonreleased",
        "grant_expired",
        "session_expired",
        "attestation_expired",
        "projection",
        "receipt",
        "generation",
        "session_id",
        "pointer",
        "malformed",
        "extra_bearer",
        "pool",
        "token_hash",
        "noncanonical",
        "session_digest",
        "attestation_binding",
    ],
)
async def test_internal_current_session_rejects_changed_authority(
    projection_session: async_sessionmaker[AsyncSession],
    change: str,
) -> None:
    async with projection_session() as session:
        initial, _, _, _ = await _initial(session)
        grant = await session.get(TaskImageBuildGrant, GRANT_ID)
        row = await session.scalar(select(TaskImageBuildProjection))
        generation = await session.scalar(select(TaskImageBuildSessionGeneration))
        assert grant is not None and row is not None and generation is not None
        now = NOW + timedelta(seconds=14)
        if change == "revoked":
            grant.state = "revoked"
            grant.released_at = None
            grant.revoked_at = now
            grant.revoke_reason = "test_revocation"
        elif change == "nonreleased":
            grant.state = "bound"
            grant.released_at = None
        elif change == "grant_expired":
            now = grant.grant_expires_at
        elif change == "session_expired":
            now = initial.expires_at
        elif change == "attestation_expired":
            row.attestation_expires_at = now
        elif change == "projection":
            row.request_json = dict(row.request_json) | {"node_name": "changed-node"}
        elif change == "receipt":
            row.bootstrap_expires_at += timedelta(seconds=1)
        elif change == "generation":
            row.session_generation = 2
        elif change == "session_id":
            row.session_id = NEXT_SESSION_ID
        elif change == "pointer":
            generation.session_sha256 = "e" * 64
        else:
            binding = dict(row.session_json)
            if change == "malformed":
                binding["generation"] = True
            elif change == "extra_bearer":
                binding["session_token"] = initial.session_token
            elif change == "pool":
                binding["pool_id"] = "changed-pool"
            elif change == "token_hash":
                binding["session_token_sha256"] = "e" * 64
            elif change == "noncanonical":
                binding["issued_at"] = str(binding["issued_at"]).replace("Z", "+00:00")
            elif change == "attestation_binding":
                binding["attestation_sha256"] = "e" * 64
            digest = hashlib.sha256(rfc8785.dumps(binding)).hexdigest()
            row.session_json = generation.session_json = binding
            row.session_sha256 = generation.session_sha256 = (
                "e" * 64 if change == "session_digest" else digest
            )
        # Persist corruption so strict binding checks are exercised after reload.
        # Impossible FK overrides must instead fail closed as unflushed authority.
        if change not in {"generation", "session_id"}:
            await session.flush()
        with session.no_autoflush:
            with pytest.raises(store.TaskImageProjectionAuthorizationError) as error:
                await store.validate_current_task_image_build_session(
                    session,
                    grant_id=GRANT_ID,
                    clock=lambda: now,
                )
        assert initial.session_token not in str(error.value)


@pytest.mark.parametrize(
    "locked_model",
    [
        TaskImageBuildGrant,
        TaskImageBuildProjection,
        TaskImageBuildSessionGeneration,
        TaskImageBuildContainmentAttestation,
    ],
)
async def test_internal_current_session_samples_clock_after_database_lock_wait(
    projection_session: async_sessionmaker[AsyncSession],
    locked_model: type,
) -> None:
    async with projection_session() as session:
        initial, _, _, _ = await _initial(session)
        await session.commit()
    now = NOW + timedelta(seconds=14)
    async with projection_session() as blocker, projection_session() as worker:
        await blocker.scalar(select(locked_model).with_for_update())
        pid = await worker.scalar(text("SELECT pg_backend_pid()"))
        task = asyncio.create_task(
            store.validate_current_task_image_build_session(
                worker,
                grant_id=GRANT_ID,
                clock=lambda: now,
            )
        )
        try:
            async with asyncio.timeout(5):
                while not await blocker.scalar(
                    text("SELECT cardinality(pg_blocking_pids(:pid)) > 0"), {"pid": pid}
                ):
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)
            now = initial.expires_at
            await blocker.rollback()
            with pytest.raises(store.TaskImageProjectionExpiredError):
                await task
        finally:
            await blocker.rollback()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize(
    "changed_model",
    [
        TaskImageBuildGrant,
        TaskImageBuildProjection,
        TaskImageBuildSessionGeneration,
        TaskImageBuildContainmentAttestation,
    ],
)
async def test_current_session_rejects_committed_change_to_preloaded_authority(
    projection_session: async_sessionmaker[AsyncSession],
    changed_model: type,
) -> None:
    async with projection_session() as session:
        await _initial(session)
        await session.commit()
    async with projection_session() as worker, projection_session() as writer:
        cached = await worker.scalar(select(changed_model))
        assert cached is not None
        if changed_model is TaskImageBuildGrant:
            changes = {"state": "bound", "released_at": None}
        elif changed_model is TaskImageBuildContainmentAttestation:
            changes = {"attestation_sha256": "e" * 64}
        else:
            changes = {"session_sha256": "e" * 64}
        await writer.execute(update(changed_model).values(**changes))
        await writer.commit()
        with pytest.raises(store.TaskImageProjectionAuthorizationError):
            await store.validate_current_task_image_build_session(
                worker,
                grant_id=GRANT_ID,
                clock=lambda: NOW + timedelta(seconds=14),
            )


@pytest.mark.parametrize(
    "cached_model",
    [
        TaskImageBuildProjection,
        TaskImageBuildSessionGeneration,
        TaskImageBuildContainmentAttestation,
    ],
)
@pytest.mark.parametrize("identity_state", ["deferred", "expired"])
@pytest.mark.parametrize("dirty", [False, True])
async def test_current_session_refreshes_authority_with_unloaded_grant_id(
    projection_session: async_sessionmaker[AsyncSession],
    cached_model: type,
    identity_state: str,
    dirty: bool,
) -> None:
    async with projection_session() as session:
        await _initial(session)
        await session.commit()
    async with projection_session() as worker, projection_session() as writer:
        query = select(cached_model)
        if identity_state == "deferred":
            query = query.options(defer(cached_model.grant_id))
        cached = await worker.scalar(query)
        assert cached is not None
        if identity_state == "expired":
            worker.expire(cached, ["grant_id"])
        digest_field = (
            "attestation_sha256"
            if cached_model is TaskImageBuildContainmentAttestation
            else "session_sha256"
        )
        if dirty:
            setattr(cached, digest_field, "e" * 64)
        else:
            await writer.execute(update(cached_model).values(**{digest_field: "e" * 64}))
            await writer.commit()
        with worker.no_autoflush:
            with pytest.raises(
                store.TaskImageProjectionAuthorizationError,
                match="unflushed changes" if dirty else None,
            ):
                await store.validate_current_task_image_build_session(
                    worker,
                    grant_id=GRANT_ID,
                    clock=lambda: NOW + timedelta(seconds=14),
                )
        if dirty:
            assert worker.is_modified(cached)
            assert getattr(cached, digest_field) == "e" * 64


async def test_unloaded_unrelated_authority_edits_are_not_discarded_or_rejected(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    async with projection_session() as session:
        await _initial(session)
        await session.commit()
    async with projection_session() as worker:
        cached = await worker.scalar(
            select(TaskImageBuildProjection).options(defer(TaskImageBuildProjection.grant_id))
        )
        assert cached is not None
        cached.session_sha256 = "e" * 64
        with worker.no_autoflush:
            with pytest.raises(
                store.TaskImageProjectionAuthorizationError, match="grant is unavailable"
            ):
                await store.validate_current_task_image_build_session(
                    worker,
                    grant_id=NEXT_SESSION_ID,
                    clock=lambda: NOW + timedelta(seconds=14),
                )
        assert worker.is_modified(cached)
        assert cached.session_sha256 == "e" * 64


@pytest.mark.parametrize(
    "cached_model",
    [
        TaskImageBuildProjection,
        TaskImageBuildSessionGeneration,
        TaskImageBuildContainmentAttestation,
    ],
)
async def test_current_session_preserves_autoflush_for_unloaded_authority(
    projection_session: async_sessionmaker[AsyncSession],
    cached_model: type,
) -> None:
    async with projection_session() as session:
        await _initial(session)
        await session.commit()
    async with projection_session() as worker:
        cached = await worker.scalar(select(cached_model).options(defer(cached_model.grant_id)))
        assert cached is not None
        digest_field = (
            "attestation_sha256"
            if cached_model is TaskImageBuildContainmentAttestation
            else "session_sha256"
        )
        setattr(cached, digest_field, "e" * 64)
        with pytest.raises(store.TaskImageProjectionAuthorizationError):
            await store.validate_current_task_image_build_session(
                worker,
                grant_id=GRANT_ID,
                clock=lambda: NOW + timedelta(seconds=14),
            )
        assert not worker.is_modified(cached)
        assert await worker.scalar(select(getattr(cached_model, digest_field))) == "e" * 64


async def test_legacy_v1_binding_still_requires_exact_bearer_and_generation(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    async with projection_session() as session:
        initial, _, _, _ = await _initial(session)
        row = await session.scalar(select(TaskImageBuildProjection))
        generation = await session.scalar(select(TaskImageBuildSessionGeneration))
        assert row is not None and generation is not None
        binding = dict(row.session_json)
        binding.pop("generation")
        binding["schema_version"] = 1
        row.session_json = generation.session_json = binding
        row.session_sha256 = generation.session_sha256 = hashlib.sha256(
            rfc8785.dumps(binding)
        ).hexdigest()
        await session.flush()
        now = NOW + timedelta(seconds=14)
        internal = await store.validate_current_task_image_build_session(
            session,
            grant_id=GRANT_ID,
            clock=lambda: now,
        )
        external = await store.authorize_task_image_build_session(
            session,
            grant_id=GRANT_ID,
            session_id=initial.session_id,
            session_generation=1,
            raw_session_token=initial.session_token,
            now=now,
        )
        assert internal == external
        assert external.session_generation == 1
        for version, token in [(2, initial.session_token), (1, "loom_tibs_" + "Z" * 64)]:
            with pytest.raises(store.TaskImageProjectionAuthorizationError):
                await store.authorize_task_image_build_session(
                    session,
                    grant_id=GRANT_ID,
                    session_id=initial.session_id,
                    session_generation=version,
                    raw_session_token=token,
                    now=now,
                )


async def test_current_session_rejects_valid_but_stale_attestation_binding(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    async with projection_session() as session:
        initial, principal, proof, secrets = await _initial(session)
        attestation = _attestation(proof, generation=2)
        current = await store.renew_task_image_build_session(
            session,
            principal=principal,
            request=TaskImageSessionRenewalV1(
                renewal_id=RENEWAL_ID,
                grant_id=GRANT_ID,
                session_id=initial.session_id,
                session_generation=1,
                session_token=initial.session_token,
                attestation=attestation,
                observed_at=attestation.issued_at,
            ),
            now=NOW + timedelta(seconds=13),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "C" * 64,
            session_id_factory=lambda: NEXT_SESSION_ID,
        )
        row = await session.scalar(select(TaskImageBuildProjection))
        generation = await session.scalar(
            select(TaskImageBuildSessionGeneration).where(
                TaskImageBuildSessionGeneration.generation == 2
            )
        )
        assert row is not None and generation is not None
        stale = current.model_copy(
            update={
                "attestation_generation": 1,
                "attestation_sha256": initial.attestation_sha256,
                "expires_at": initial.expires_at,
            }
        )
        row.session_json = generation.session_json = stale.public_binding()
        row.session_sha256 = generation.session_sha256 = hashlib.sha256(
            rfc8785.dumps(stale.public_binding())
        ).hexdigest()
        row.session_expires_at = generation.expires_at = stale.expires_at
        await session.flush()
        with pytest.raises(store.TaskImageProjectionAuthorizationError):
            await store.validate_current_task_image_build_session(
                session,
                grant_id=GRANT_ID,
                clock=lambda: NOW + timedelta(seconds=14),
            )


async def test_current_session_reloads_successor_attestation_after_its_lock_wait(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    async with projection_session() as session:
        initial, principal, proof, secrets = await _initial(session)
        attestation = _attestation(proof, generation=2)
        await store.renew_task_image_build_session(
            session,
            principal=principal,
            request=TaskImageSessionRenewalV1(
                renewal_id=RENEWAL_ID,
                grant_id=GRANT_ID,
                session_id=initial.session_id,
                session_generation=1,
                session_token=initial.session_token,
                attestation=attestation,
                observed_at=attestation.issued_at,
            ),
            now=NOW + timedelta(seconds=13),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "C" * 64,
            session_id_factory=lambda: NEXT_SESSION_ID,
        )
        await session.commit()
    async with projection_session() as blocker, projection_session() as worker:
        changed = await blocker.scalar(
            select(TaskImageBuildContainmentAttestation)
            .where(TaskImageBuildContainmentAttestation.generation == 2)
            .with_for_update()
        )
        assert changed is not None
        cached = await worker.scalar(
            select(TaskImageBuildContainmentAttestation).where(
                TaskImageBuildContainmentAttestation.generation == 2
            )
        )
        assert cached is not None
        pid = await worker.scalar(text("SELECT pg_backend_pid()"))
        task = asyncio.create_task(
            store.validate_current_task_image_build_session(
                worker,
                grant_id=GRANT_ID,
                clock=lambda: NOW + timedelta(seconds=14),
            )
        )
        try:
            async with asyncio.timeout(5):
                while not await blocker.scalar(
                    text("SELECT cardinality(pg_blocking_pids(:pid)) > 0"), {"pid": pid}
                ):
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)
            changed.attestation_sha256 = "e" * 64
            await blocker.commit()
            with pytest.raises(store.TaskImageProjectionAuthorizationError):
                await task
        finally:
            await blocker.rollback()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
