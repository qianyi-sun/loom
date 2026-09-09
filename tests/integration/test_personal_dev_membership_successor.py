"""Durable successor lineage preserves historical receipts across retry and takeover."""

import json
from dataclasses import fields, replace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    DevInstance,
    DevLifecycleOperation,
    DevLifecycleOperationAttempt,
    PersonalDevCandidate,
    Team,
    User,
)
from loom.personal_dev_environment_store import SqlAlchemyPersonalDevEnvironmentAuthority
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
    return values


async def _seed(sessions, kind, outcome):
    claim, accepted, values = successor_case(kind, outcome)
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
            capacity_namespace="loom-dev-alice", capacity_database="loom_dev_alice",
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
    finally:
        await engine.dispose()
