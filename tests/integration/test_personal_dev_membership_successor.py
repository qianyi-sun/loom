"""Durable successor lineage preserves historical receipts across retry and takeover."""

import asyncio
import json
from dataclasses import fields, replace
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    DevInstance,
    DevLifecycleOperation,
    DevLifecycleOperationAttempt,
    PersonalDevCandidate,
    Team,
    User,
)
from loom.personal_dev_environment_store import (
    PersonalDevEnvironmentOperationFencedError,
    SqlAlchemyPersonalDevEnvironmentAuthority,
)
from loom.personal_dev_incarnation_storage import PersonalDevStorageBindingV1
from loom.personal_dev_membership_successor import PersonalDevMembershipSuccessorBindingV1
from loom_capacity_manager.contracts import canonical_digest
from tests.unit.test_personal_dev_membership_reconciler import _NOW
from tests.unit.test_personal_dev_membership_successor import successor_case


def _row_values(model, record):
    values = {
        field.name: getattr(record, field.name)
        for field in fields(record)
        if field.name in model.__table__.columns
    }
    for key, value in values.items():
        if hasattr(value, "model_dump"):
            values[key] = value.model_dump(mode="json")
        elif isinstance(value, tuple):
            values[key] = list(value)
    if values.get("storage_binding") is not None:
        values["storage_binding_sha256"] = canonical_digest(record.storage_binding)
    return values


async def _seed(sessions, kind, outcome, *, incarnation_storage=False, legacy_accepted_storage=False):
    claim, accepted, values = successor_case(kind, outcome)
    if incarnation_storage:
        storage = PersonalDevStorageBindingV1(
            layout="incarnation-v1", environment_name=claim.operation.environment_name,
            subject_id=claim.operation.subject_id,
            subject_incarnation=claim.operation.subject_incarnation,
            owner_user_id=claim.operation.owner_user_id,
            owner_team_id=claim.operation.owner_team_id,
        )
        claim = replace(claim, environment=replace(claim.environment, storage_binding=storage),
                        operation=replace(claim.operation, storage_binding=storage))
        if accepted is not None and not legacy_accepted_storage:
            accepted = replace(accepted, storage_binding=storage)
    # Match real durable rows, rather than the reconciler's lightweight fixtures.
    if kind in {"capacity", "destroy"}:
        claim = replace(
            claim,
            operation=replace(
                claim.operation, state="running", readiness_evidence_sha256=None,
                activation_acknowledgement_sha256=None,
            ),
            attempt=replace(claim.attempt, state="running"),
        )
    async with sessions() as session:
        session.add(Team(id=claim.operation.owner_team_id, name="successor-owner"))
        session.add(User(
            id=claim.operation.owner_user_id, email="successor@example.test",
            username="successor-owner", username_normalized="successor-owner", status="active",
        ))
        await session.flush()
        candidate_values = _row_values(PersonalDevCandidate, claim.candidate)
        candidate_values["manifest_json"] = {"schema_version": 1}
        candidate_values["object_key"] = (
            f"personal-dev/sources/{claim.operation.owner_team_id}/"
            f"{claim.operation.owner_user_id}/{claim.candidate.candidate_sha}/"
            f"{claim.candidate.archive_sha256}.tar"
        )
        session.add(PersonalDevCandidate(**candidate_values))
        await session.flush()
        env_values = _row_values(DevInstance, claim.environment)
        env_values.update(
            capacity_namespace="loom-dev-alice", capacity_database=(
                claim.environment.storage_binding.identity.database
                if claim.environment.storage_binding else "loom_dev_alice"
            ),
            operation_step="membership_outcome_resolved",
        )
        if accepted is not None:
            for key in (
                "capacity_reporter_incarnation", "capacity_reporter_token_sha256",
                "local_activation_sha256", "protected_admission_sha256",
                "capacity_agent_installation_sha256", "capacity_supported_pool_ids",
                "capacity_supported_architectures",
            ):
                value = getattr(accepted, key)
                env_values[key] = list(value) if isinstance(value, tuple) else value
        session.add(DevInstance(**env_values))
        await session.flush()
        if accepted is not None:
            accepted_values = _row_values(DevLifecycleOperation, accepted)
            accepted_values["finished_at"] = _NOW
            session.add(DevLifecycleOperation(**accepted_values))
        session.add(DevLifecycleOperation(**_row_values(DevLifecycleOperation, claim.operation)))
        await session.flush()
        attempt_values = _row_values(DevLifecycleOperationAttempt, claim.attempt)
        attempt_values.update(
            credential_binding_version=1,
            bootstrap_auth_kind=claim.attempt.access_binding.auth_kind,
            bootstrap_credential_hash=claim.attempt.access_binding.credential_hash,
        )
        session.add(DevLifecycleOperationAttempt(**attempt_values))
        if accepted is not None:
            session.add(DevLifecycleOperationAttempt(
                **(attempt_values | {
                    "id": accepted.attempt_id, "operation_id": accepted.id,
                    "operation_epoch": accepted.operation_epoch, "state": "succeeded",
                    "checkpoint": "complete", "claimed_by": None, "lease_expires_at": None,
                    "finished_at": _NOW,
                })
            ))
        await session.commit()
    binding = PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    return claim, binding


@pytest.mark.parametrize(
    "kind,outcome,effective",
    (
        ("create", "committed", "update"),
        ("update", "committed", "update"),
        ("capacity", "committed", "update"),
        ("create", "terminal-not-committed", "create"),
        ("update", "terminal-not-committed", "update"),
        ("capacity", "terminal-not-committed", "update"),
        ("destroy", "terminal-not-committed", "destroy"),
    ),
)
@pytest.mark.asyncio
async def test_successor_is_one_lease_fenced_linked_operation(
    isolated_migration_postgres_url, kind, outcome, effective,
):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        claim, binding = await _seed(sessions, kind, outcome)
        original = claim.operation.capacity_membership_envelope.model_dump(mode="json")
        args = dict(
            operation_id=claim.operation.id,
            operation_epoch=claim.operation.operation_epoch,
            attempt_id=claim.attempt.id,
            reconciler_id=claim.attempt.claimed_by,
            lease_epoch=claim.attempt.lease_epoch,
            binding=binding,
            expected_binding_sha256=canonical_digest(binding),
            current_checkpoint=claim.operation.capacity_membership_envelope.expected_checkpoint.model_copy(
                update={"execution": binding.authority.execution, "namespace_id": binding.authority.namespace_id}
            ),
            now=_NOW,
        )
        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            for invalid in (
                {"lease_epoch": claim.attempt.lease_epoch - 1},
                {"reconciler_id": "other-reconciler"},
                {"attempt_id": uuid4()},
                {"operation_epoch": claim.operation.operation_epoch + 1},
                {"expected_binding_sha256": "e" * 64},
                {"current_checkpoint": args["current_checkpoint"].model_copy(update={"namespace_id": uuid4()})},
            ):
                with pytest.raises((PersonalDevEnvironmentOperationFencedError, ValueError)):
                    await authority.create_membership_successor(**(args | invalid))
                await session.rollback()
            result = await authority.create_membership_successor(**args)
            child = result.operation
            assert result.acquired
            assert child.kind == effective
            assert child.id != claim.operation.id
            assert child.idempotency_key != claim.operation.idempotency_key
            assert child.attempt_id != claim.attempt.id
            assert child.operation_epoch == claim.operation.operation_epoch + 1
            assert child.membership_predecessor_operation_id == claim.operation.id
            assert child.membership_successor_binding_sha256 == canonical_digest(binding)
            assert child.capacity_membership_envelope is None
            assert child.candidate_id == claim.operation.candidate_id
            assert (child.min_slots, child.max_slots) == (
                claim.operation.min_slots, claim.operation.max_slots,
            )
            assert child.checkpoint == (
                "capacity_retirement_requested" if effective == "destroy" else "candidate_build"
            )
            assert result.environment.operation_id == child.id
            if effective != "destroy":
                assert child.deployment_generation > claim.operation.deployment_generation
                assert child.capacity_reporter_incarnation is None
                assert child.local_activation_sha256 is None
            replay = await authority.create_membership_successor(**args)
            assert not replay.acquired and replay.operation.id == child.id
        async with sessions() as session:
            parent = await session.get(DevLifecycleOperation, claim.operation.id)
            assert parent.state == "superseded"
            assert parent.checkpoint == "membership_successor_created"
            assert parent.capacity_membership_envelope == original
            attempt = await session.get(DevLifecycleOperationAttempt, claim.attempt.id)
            assert attempt.state == "superseded"
            assert attempt.claimed_by is None and attempt.lease_expires_at is None
            children = (await session.scalars(select(DevLifecycleOperation).where(
                DevLifecycleOperation.membership_predecessor_operation_id == claim.operation.id
            ))).all()
            assert len(children) == 1
            status = await SqlAlchemyPersonalDevEnvironmentAuthority(session).get_operation(parent.id)
            assert status.membership_successor_operation_id == child.id
            child_status = await SqlAlchemyPersonalDevEnvironmentAuthority(session).get_operation(child.id)
            assert child_status.membership_successor_operation_id is None
            for statement, row_id in (
                ("UPDATE dev_lifecycle_operations SET min_slots = 1 WHERE id = :id", child.id),
                ("UPDATE dev_lifecycle_operations SET checkpoint = 'complete' WHERE id = :id", parent.id),
                ("DELETE FROM dev_lifecycle_operations WHERE id = :id", parent.id),
                ("DELETE FROM dev_lifecycle_operations WHERE id = :id", child.id),
                ("UPDATE dev_lifecycle_operation_attempts SET lease_epoch = lease_epoch + 1 WHERE id = :id", attempt.id),
            ):
                with pytest.raises(DBAPIError):
                    async with session.begin_nested():
                        await session.execute(text(statement), {"id": row_id})
    finally:
        await engine.dispose()


def _arguments(claim, binding):
    return dict(
        operation_id=claim.operation.id, operation_epoch=claim.operation.operation_epoch,
        attempt_id=claim.attempt.id, reconciler_id=claim.attempt.claimed_by,
        lease_epoch=claim.attempt.lease_epoch, binding=binding,
        expected_binding_sha256=canonical_digest(binding),
        current_checkpoint=claim.operation.capacity_membership_envelope.expected_checkpoint.model_copy(
            update={"execution": binding.authority.execution, "namespace_id": binding.authority.namespace_id}
        ),
        now=_NOW,
    )


@pytest.mark.asyncio
async def test_concurrent_successor_creation_has_one_winner(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        claim, binding = await _seed(sessions, "create", "terminal-not-committed")
        arguments = _arguments(claim, binding)

        async def create():
            async with sessions() as session:
                return await SqlAlchemyPersonalDevEnvironmentAuthority(session).create_membership_successor(**arguments)

        first, second = await asyncio.wait_for(asyncio.gather(create(), create()), timeout=15)
        assert sorted((first.acquired, second.acquired)) == [False, True]
        assert first.operation.id == second.operation.id
        assert first.operation.attempt_id == second.operation.attempt_id
        assert first.operation.idempotency_key == second.operation.idempotency_key
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_successor_retains_independent_accepted_history(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        claim, binding = await _seed(sessions, "update", "terminal-not-committed")
        async with sessions() as session:
            await SqlAlchemyPersonalDevEnvironmentAuthority(session).create_membership_successor(
                **_arguments(claim, binding)
            )
            for statement in (
                "UPDATE dev_lifecycle_operations SET local_activation_sha256 = repeat('e', 64) WHERE id = :id",
                "UPDATE dev_lifecycle_operation_attempts SET lease_epoch = lease_epoch + 1 WHERE operation_id = :id",
                "DELETE FROM dev_lifecycle_operation_attempts WHERE operation_id = :id",
            ):
                with pytest.raises(DBAPIError):
                    async with session.begin_nested():
                        await session.execute(text(statement), {"id": binding.accepted_operation_id})
    finally:
        await engine.dispose()


@pytest.mark.parametrize("defect", (
    "child_attempt_state", "child_attempt_checkpoint", "child_checkpoint",
    "environment_step", "environment_status", "destroy_evidence", "effective_kind",
))
@pytest.mark.asyncio
async def test_database_rejects_corrupt_successor_handoff(
    isolated_migration_postgres_url, monkeypatch, defect,
):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        kind = "destroy" if defect == "destroy_evidence" else "create"
        claim, binding = await _seed(sessions, kind, "terminal-not-committed")
        async with sessions() as session:
            real_flush = session.flush

            async def corrupt_flush(*args, **kwargs):
                for row in tuple(session.new):
                    if isinstance(row, DevLifecycleOperation) and row.membership_predecessor_operation_id:
                        if defect == "destroy_evidence":
                            row.local_activation_sha256 = None
                        elif defect == "effective_kind":
                            row.kind = "update"
                        elif defect == "child_checkpoint":
                            row.checkpoint = "requested"
                    if isinstance(row, DevLifecycleOperationAttempt):
                        if defect == "child_attempt_state":
                            row.state = "failed"
                            row.finished_at = _NOW
                            row.failure_reason = "injected corruption"
                        elif defect == "child_attempt_checkpoint":
                            row.checkpoint = "requested"
                for row in tuple(session.dirty):
                    if isinstance(row, DevInstance) and row.operation_id != claim.operation.id:
                        if defect == "environment_step":
                            row.operation_step = "requested"
                        elif defect == "environment_status":
                            row.status = "deleting"
                return await real_flush(*args, **kwargs)

            monkeypatch.setattr(session, "flush", corrupt_flush)
            with pytest.raises(DBAPIError):
                await SqlAlchemyPersonalDevEnvironmentAuthority(session).create_membership_successor(
                    **_arguments(claim, binding)
                )
            await session.rollback()
    finally:
        await engine.dispose()


@pytest.mark.parametrize("fail_after_flush", (1, 2))
@pytest.mark.asyncio
async def test_partial_successor_transition_rolls_back_without_losing_history(
    isolated_migration_postgres_url, monkeypatch, fail_after_flush,
):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        claim, binding = await _seed(sessions, "create", "committed")
        arguments = _arguments(claim, binding)
        async with sessions() as session:
            real_flush = session.flush
            flush_count = 0

            async def fail_flush(*args, **kwargs):
                nonlocal flush_count
                await real_flush(*args, **kwargs)
                flush_count += 1
                if flush_count == fail_after_flush:
                    raise RuntimeError("injected transition crash")

            monkeypatch.setattr(session, "flush", fail_flush)
            with pytest.raises(RuntimeError, match="injected transition crash"):
                await SqlAlchemyPersonalDevEnvironmentAuthority(session).create_membership_successor(**arguments)
            await session.rollback()
        async with sessions() as session:
            parent = await session.get(DevLifecycleOperation, claim.operation.id)
            assert parent.state == claim.operation.state
            assert parent.capacity_membership_envelope == claim.operation.capacity_membership_envelope.model_dump(mode="json")
            assert parent.checkpoint == "membership_outcome_resolved"
            env = await session.get(DevInstance, claim.environment.name)
            assert env.operation_id == parent.id
            retry = await SqlAlchemyPersonalDevEnvironmentAuthority(session).create_membership_successor(**arguments)
            assert retry.acquired
    finally:
        await engine.dispose()


@pytest.mark.parametrize("with_successor", (False, True))
@pytest.mark.asyncio
async def test_successor_migration_preserves_history_and_rejects_lossy_downgrade(
    isolated_migration_postgres_url, with_successor,
):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    repo = Path(__file__).resolve().parents[2]
    cfg = Config(str(repo / "migrations/alembic.ini"))
    cfg.set_main_option("script_location", str(repo / "migrations"))
    cfg.set_main_option("sqlalchemy.url", isolated_migration_postgres_url)

    async def snapshot():
        async with sessions() as session:
            return {
                table: (await session.execute(text(
                    f"SELECT to_jsonb(row) FROM {table} row ORDER BY to_jsonb(row)::text"
                ))).scalars().all()
                for table in ("dev_lifecycle_operations", "dev_lifecycle_operation_attempts", "dev_instances")
            }

    try:
        claim, binding = await _seed(sessions, "update", "terminal-not-committed")
        if with_successor:
            async with sessions() as session:
                await SqlAlchemyPersonalDevEnvironmentAuthority(session).create_membership_successor(
                    **_arguments(claim, binding)
                )
        before = await snapshot()
        if with_successor:
            with pytest.raises(DBAPIError, match="cannot downgrade 0140 with membership successor history"):
                await asyncio.to_thread(command.downgrade, cfg, "0139")
        else:
            await asyncio.to_thread(command.downgrade, cfg, "0139")
            await asyncio.to_thread(command.upgrade, cfg, "0140")
            await asyncio.to_thread(command.upgrade, cfg, "0141")
        assert await snapshot() == before
        async with sessions() as session:
            assert (await session.execute(text("SELECT version_num FROM alembic_version"))).scalar_one() == "0141"
            # Existing historical outcomes never gain invented successor authority.
            parent = await SqlAlchemyPersonalDevEnvironmentAuthority(session).get_operation(claim.operation.id)
            assert (parent.membership_successor_operation_id is not None) is with_successor
    finally:
        await engine.dispose()
