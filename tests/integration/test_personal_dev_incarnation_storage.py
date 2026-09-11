"""Management persists storage identity before any external provisioning."""

import asyncio
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import DevInstance, DevLifecycleOperation, PersonalDevCandidate, Team, User
from loom.personal_dev_environment import (
    PersonalDevEnvironmentApplyRequest,
    PersonalDevEnvironmentDestroyRequest,
)
from loom.personal_dev_environment_store import SqlAlchemyPersonalDevEnvironmentAuthority
from loom_capacity_manager.contracts import canonical_digest
from tests.integration.test_personal_dev_membership_successor import _arguments, _row_values, _seed
from tests.unit.test_personal_dev_reconciler import _NOW, _claim


async def _candidate(sessions):
    claim = _claim(state="activating")
    candidate = claim.candidate
    async with sessions() as session:
        session.add(Team(id=candidate.owner_team_id, name="storage-owner"))
        session.add(User(id=candidate.owner_user_id, email="storage@example.test", username="storage-owner",
                         username_normalized="storage-owner", status="active"))
        await session.flush()
        values = _row_values(PersonalDevCandidate, candidate)
        values.update(manifest_json={"schema_version": 1}, object_key=(
            f"personal-dev/sources/{candidate.owner_team_id}/{candidate.owner_user_id}/"
            f"{candidate.candidate_sha}/{candidate.archive_sha256}.tar"
        ))
        session.add(PersonalDevCandidate(**values))
        await session.commit()
    request = PersonalDevEnvironmentApplyRequest(
        name="alice", owner_user_id=candidate.owner_user_id, owner_team_id=candidate.owner_team_id,
        candidate_id=candidate.id, candidate_sha=candidate.candidate_sha,
        min_slots=0, max_slots=1, expected_operation_epoch=0, idempotency_key=uuid4(),
    )
    return request, claim.attempt.access_binding


@pytest.mark.asyncio
async def test_storage_layout_is_reserved_once_and_replay_cannot_downgrade(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access = await _candidate(sessions)
        async with sessions() as session:
            result = await SqlAlchemyPersonalDevEnvironmentAuthority(
                session, storage_layout="incarnation-v1",
            ).apply(request, access_binding=access, now=_NOW)
        binding = result.operation.storage_binding
        assert binding is not None and binding.layout == "incarnation-v1"
        assert binding.subject_incarnation == result.operation.subject_incarnation
        assert binding.owner_user_id == request.owner_user_id
        assert binding.owner_team_id == request.owner_team_id
        assert result.environment.storage_binding == binding
        async with sessions() as session:
            environment = await session.get(DevInstance, request.name)
            operation = await session.get(DevLifecycleOperation, result.operation.id)
            assert environment.capacity_database == binding.identity.database
            assert environment.capacity_namespace == "loom-dev-alice"
            assert environment.storage_binding_sha256 == operation.storage_binding_sha256 == canonical_digest(binding)
            assert environment.storage_binding == operation.storage_binding == binding.model_dump(mode="json")
        async with sessions() as session:
            replay = await SqlAlchemyPersonalDevEnvironmentAuthority(session).apply(
                request, access_binding=access, now=_NOW,
            )
            assert not replay.acquired
            assert replay.operation.storage_binding == binding
            assert replay.operation.id == result.operation.id
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_database_pins_successor_storage_to_accepted_source(isolated_migration_postgres_url, monkeypatch):
    from loom import personal_dev_environment_store as store

    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        claim, binding = await _seed(sessions, "update", "terminal-not-committed",
                                     incarnation_storage=True, legacy_accepted_storage=True)
        original_validate = store.validate_membership_successor

        def bypass_application_comparison(*args, **kwargs):
            # Deliberately lie only to the pure validator: the DB must enforce
            # the actual retained source independently at child insertion.
            kwargs["accepted_operation"] = replace(
                kwargs["accepted_operation"], storage_binding=claim.operation.storage_binding,
            )
            return original_validate(*args, **kwargs)

        monkeypatch.setattr(store, "validate_membership_successor", bypass_application_comparison)
        async with sessions() as session:
            with pytest.raises(DBAPIError, match="storage"):
                await SqlAlchemyPersonalDevEnvironmentAuthority(session).create_membership_successor(
                    **_arguments(claim, binding),
                )
            await session.rollback()
            parent = await session.get(DevLifecycleOperation, claim.operation.id)
            assert parent.state != "superseded"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("table", ("dev_instances", "dev_lifecycle_operations"))
async def test_same_incarnation_storage_binding_is_immutable(isolated_migration_postgres_url, table):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access = await _candidate(sessions)
        async with sessions() as session:
            result = await SqlAlchemyPersonalDevEnvironmentAuthority(
                session, storage_layout="incarnation-v1",
            ).apply(request, access_binding=access, now=_NOW)
        async with sessions() as session:
            with pytest.raises(DBAPIError):
                await session.execute(text(
                    f"UPDATE {table} SET storage_binding = NULL, storage_binding_sha256 = NULL"
                ))
                await session.commit()
            await session.rollback()
            operation = (await session.scalars(select(DevLifecycleOperation).where(
                DevLifecycleOperation.id == result.operation.id,
            ))).one()
            assert operation.storage_binding == result.operation.storage_binding.model_dump(mode="json")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_database_rejects_storage_digest_owner_and_physical_target_drift(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access = await _candidate(sessions)
        async with sessions() as session:
            result = await SqlAlchemyPersonalDevEnvironmentAuthority(session, storage_layout="incarnation-v1").apply(
                request, access_binding=access, now=_NOW,
            )
        for table, amendment in (
            ("dev_instances", "storage_binding_sha256 = repeat('e', 64)"),
            ("dev_lifecycle_operations", "storage_binding_sha256 = repeat('e', 64)"),
            ("dev_instances", "capacity_database = 'loom_dev_alice'"),
            ("dev_instances", "storage_binding = storage_binding || '{\"owner_user_id\":\"00000000-0000-0000-0000-000000000099\"}'::jsonb"),
            ("dev_lifecycle_operations", "storage_binding = storage_binding || '{\"database\":\"loom_staging\"}'::jsonb"),
        ):
            async with sessions() as session:
                with pytest.raises(DBAPIError):
                    await session.execute(text(f"UPDATE {table} SET {amendment}"))
                    await session.commit()
                await session.rollback()
                row = await session.get(DevInstance, request.name)
                assert row.storage_binding == result.operation.storage_binding.model_dump(mode="json")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("layout", ("legacy-name-v1", "incarnation-v1"))
async def test_storage_migration_preserves_rows_and_rejects_lossy_downgrade(isolated_migration_postgres_url, layout):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "migrations/alembic.ini"))
    config.set_main_option("script_location", str(root / "migrations"))
    config.set_main_option("sqlalchemy.url", isolated_migration_postgres_url)

    async def snapshot():
        async with sessions() as session:
            return {
                table: (await session.execute(text(f"SELECT to_jsonb(row) FROM {table} row"))).scalars().all()
                for table in ("dev_instances", "dev_lifecycle_operations")
            }

    try:
        request, access = await _candidate(sessions)
        async with sessions() as session:
            await SqlAlchemyPersonalDevEnvironmentAuthority(session, storage_layout=layout).apply(
                request, access_binding=access, now=_NOW,
            )
        before = await snapshot()
        if layout == "incarnation-v1":
            with pytest.raises(DBAPIError, match="cannot downgrade 0141"):
                await asyncio.to_thread(command.downgrade, config, "0140")
        else:
            await asyncio.to_thread(command.downgrade, config, "0140")
            await asyncio.to_thread(command.upgrade, config, "0141")
        assert await snapshot() == before
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,outcome", (
    ("create", "committed"), ("update", "committed"), ("capacity", "committed"),
    ("create", "terminal-not-committed"), ("update", "terminal-not-committed"),
    ("capacity", "terminal-not-committed"), ("destroy", "terminal-not-committed"),
))
async def test_incarnation_storage_survives_every_successor_decision(
    isolated_migration_postgres_url, kind, outcome,
):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        claim, binding = await _seed(sessions, kind, outcome, incarnation_storage=True)
        storage = claim.operation.storage_binding
        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            result = await authority.create_membership_successor(**_arguments(claim, binding))
            assert result.operation.storage_binding == result.environment.storage_binding == storage
            assert result.operation.subject_incarnation == storage.subject_incarnation
            parent = await authority.get_operation(claim.operation.id)
            assert parent.storage_binding == storage
            if binding.accepted_operation_id is not None:
                accepted = await authority.get_operation(binding.accepted_operation_id)
                assert accepted.storage_binding == storage
            replay = await authority.create_membership_successor(**_arguments(claim, binding))
            assert not replay.acquired
            assert replay.operation.storage_binding == storage
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("history_attack", ("rewind", "delete"))
async def test_recreation_allocates_disjoint_storage_despite_layout_config_rollback(
    isolated_migration_postgres_url, history_attack,
):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access = await _candidate(sessions)
        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session, storage_layout="incarnation-v1")
            created = await authority.apply(request, access_binding=access, now=_NOW)
            claim = await authority.claim_next_reconciliation(
                reconciler_id="storage-test", now=_NOW, lease_seconds=60,
            )
            assert claim is not None
            await authority.fail_pre_activation(
                operation_id=created.operation.id, operation_epoch=created.operation.operation_epoch,
                attempt_id=claim.attempt.id, reconciler_id="storage-test",
                lease_epoch=claim.attempt.lease_epoch, failure_reason="candidate_build_failed", now=_NOW,
            )
            retired = await authority.destroy(PersonalDevEnvironmentDestroyRequest(
                name=request.name, owner_user_id=request.owner_user_id, owner_team_id=request.owner_team_id,
                expected_operation_epoch=created.operation.operation_epoch, idempotency_key=uuid4(), keep_data=False,
            ), access_binding=access, now=_NOW)
            assert retired.environment.status == "deleted"
            assert retired.operation.storage_binding == created.operation.storage_binding
        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            recreated = await authority.apply(replace(
                request, expected_operation_epoch=retired.operation.operation_epoch, idempotency_key=uuid4(),
            ), access_binding=access, now=_NOW)
            old, new = created.operation.storage_binding, recreated.operation.storage_binding
            assert new.layout == old.layout == "incarnation-v1"
            assert new.subject_incarnation != old.subject_incarnation
            assert new.identity.namespace == old.identity.namespace == "loom-dev-alice"
            for field in ("database", "db_role", "task_bucket", "trajectories_bucket", "artifacts_bucket"):
                assert getattr(new.identity, field) != getattr(old.identity, field)
            assert new.object_store_identity != old.object_store_identity
            assert (await authority.get_operation(created.operation.id)).storage_binding == old
            assert (await authority.get_operation(retired.operation.id)).storage_binding == old
            with pytest.raises(DBAPIError, match="storage"):
                if history_attack == "rewind":
                    await session.execute(update(DevInstance).where(DevInstance.name == request.name).values(
                        subject_incarnation=old.subject_incarnation,
                        storage_binding=old.model_dump(mode="json"), storage_binding_sha256=canonical_digest(old),
                        capacity_database=old.identity.database, operation_id=created.operation.id,
                        operation_epoch=created.operation.operation_epoch,
                    ))
                else:
                    # This non-current destroy remains an exact cleanup target.
                    await session.execute(text("DELETE FROM dev_lifecycle_operation_attempts WHERE operation_id = :id"),
                                          {"id": retired.operation.id})
                    await session.execute(text("DELETE FROM dev_lifecycle_operations WHERE id = :id"),
                                          {"id": retired.operation.id})
                await session.commit()
            await session.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ("dev_instances", "dev_lifecycle_operations"))
async def test_storage_digest_is_checked_on_initial_insert(isolated_migration_postgres_url, target, monkeypatch):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access = await _candidate(sessions)
        async with sessions() as session:
            original_flush = session.flush

            async def corrupt_flush(*args, **kwargs):
                for row in session.new:
                    if row.__tablename__ == target and row.storage_binding is not None:
                        row.storage_binding_sha256 = "e" * 64
                await original_flush(*args, **kwargs)

            monkeypatch.setattr(session, "flush", corrupt_flush)
            with pytest.raises(DBAPIError, match="personal storage binding digest is invalid"):
                await SqlAlchemyPersonalDevEnvironmentAuthority(session, storage_layout="incarnation-v1").apply(
                    request, access_binding=access, now=_NOW,
                )
            await session.rollback()
            assert await session.get(DevInstance, request.name) is None
            assert not (await session.scalars(select(DevLifecycleOperation))).all()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ("numeric_version", "subject_id", "subject_incarnation"))
async def test_storage_database_rejects_unparseable_binding(isolated_migration_postgres_url, monkeypatch, defect):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access = await _candidate(sessions)
        async with sessions() as session:
            original_flush = session.flush

            async def corrupt_flush(*args, **kwargs):
                for row in session.new:
                    if isinstance(row, (DevInstance, DevLifecycleOperation)) and row.storage_binding is not None:
                        if defect == "numeric_version":
                            row.storage_binding = row.storage_binding | {"schema_version": 1.0}
                        else:
                            setattr(row, defect, UUID(int=0))
                            row.storage_binding = row.storage_binding | {defect: str(UUID(int=0))}
                        row.storage_binding_sha256 = hashlib.sha256(json.dumps(
                            row.storage_binding, sort_keys=True, separators=(",", ":"),
                        ).encode()).hexdigest()
                await original_flush(*args, **kwargs)

            monkeypatch.setattr(session, "flush", corrupt_flush)
            with pytest.raises(DBAPIError, match="storage"):
                await SqlAlchemyPersonalDevEnvironmentAuthority(session, storage_layout="incarnation-v1").apply(
                    request, access_binding=access, now=_NOW,
                )
            await session.rollback()
            assert await session.get(DevInstance, request.name) is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_storage_database_rejects_malformed_new_bindings(isolated_migration_postgres_url, monkeypatch):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access = await _candidate(sessions)
        for defect in ("null", "array", "scalar", "missing", "extra", "layout", "version", "boolean",
                       "missing_digest", "uppercase_digest", "zero_digest", "malformed_digest", "reserved"):
            async with sessions() as session:
                original_flush = session.flush

                async def corrupt_flush(*args, defect=defect, original_flush=original_flush, **kwargs):
                    for row in session.new:
                        if not isinstance(row, (DevInstance, DevLifecycleOperation)):
                            continue
                        binding = dict(row.storage_binding)
                        if defect in {"null", "array", "scalar"}:
                            binding = {"null": None, "array": [], "scalar": "invalid"}[defect]
                        elif defect == "missing":
                            binding.pop("owner_team_id")
                        elif defect == "extra":
                            binding["database"] = "loom_staging"
                        elif defect == "layout":
                            binding["layout"] = "legacy-name-v1"
                        elif defect in {"version", "boolean"}:
                            binding["schema_version"] = 2 if defect == "version" else True
                        elif defect == "reserved":
                            binding["environment_name"] = "shared"
                            if isinstance(row, DevInstance):
                                row.name = "shared"
                                row.capacity_namespace = "loom-dev-shared"
                                row.capacity_database = f"ld_shared_{row.subject_incarnation.hex}"
                            else:
                                row.environment_name = "shared"
                        row.storage_binding = binding
                        digest = hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                        row.storage_binding_sha256 = {
                            "missing_digest": None, "uppercase_digest": digest.upper(),
                            "zero_digest": "0" * 64, "malformed_digest": "not-a-digest",
                        }.get(defect, digest)
                    await original_flush(*args, **kwargs)

                monkeypatch.setattr(session, "flush", corrupt_flush)
                with pytest.raises(DBAPIError, match=r"storage|non-object"):
                    await SqlAlchemyPersonalDevEnvironmentAuthority(session, storage_layout="incarnation-v1").apply(
                        request, access_binding=access, now=_NOW,
                    )
                await session.rollback()
                assert await session.get(DevInstance, request.name) is None, defect
    finally:
        await engine.dispose()
