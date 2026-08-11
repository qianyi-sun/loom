"""Database invariants for the inert legacy-authority fence."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.legacy_fence import (
    LEGACY_MUTATION_INVENTORY_DIGEST,
    LEGACY_MUTATION_PATH_IDS,
    LegacyCompatibilityFreezeV1,
    LegacyCompatibilityPreparationV1,
    LegacyWriterCursorV1,
    LegacyWriterFreezeCursorV1,
)
from loom_capacity_agent.legacy_fence_store import LegacyCompatibilityFenceStore
from loom_capacity_agent.prepared_store import CapacityPreparedAdmissionStore
from loom_capacity_guard.contracts import canonical_digest
from tests.integration.test_capacity_agent_store import (
    _agent_session,
    _initialize_and_register,
    _initialize_prepared_plan,
    _owner_session,
)


def _preparation(
    registration: AgentRegistrationV1,
    *,
    preparation_id=None,
    compatibility_incarnation=None,
    high_water: int = 3,
) -> LegacyCompatibilityPreparationV1:
    cursors = tuple(
        LegacyWriterCursorV1(
            mutation_path_id=path_id,
            writer_domain="environment-local",
            writer_incarnation=uuid4(),
            writer_epoch=2,
            high_water=high_water,
            authority_digest="b" * 64,
        )
        for path_id in LEGACY_MUTATION_PATH_IDS
    )
    return LegacyCompatibilityPreparationV1(
        **registration.model_dump(mode="python"),
        preparation_id=preparation_id or uuid4(),
        compatibility_incarnation=compatibility_incarnation or uuid4(),
        fleet_migration_epoch=1,
        compatibility_not_after=datetime.now(UTC) + timedelta(hours=1),
        mutation_inventory_digest=LEGACY_MUTATION_INVENTORY_DIGEST,
        writer_cursors=cursors,
    )


def _freeze(
    registration: AgentRegistrationV1,
    preparation: LegacyCompatibilityPreparationV1,
) -> LegacyCompatibilityFreezeV1:
    return LegacyCompatibilityFreezeV1(
        **registration.model_dump(mode="python"),
        freeze_id=uuid4(),
        preparation_id=preparation.preparation_id,
        compatibility_incarnation=preparation.compatibility_incarnation,
        fleet_migration_epoch=preparation.fleet_migration_epoch,
        mutation_inventory_digest=preparation.mutation_inventory_digest,
        preparation_digest=canonical_digest(preparation),
        writer_cursors=tuple(
            LegacyWriterFreezeCursorV1(
                schema_version=cursor.schema_version,
                mutation_path_id=cursor.mutation_path_id,
                writer_domain=cursor.writer_domain,
                writer_incarnation=cursor.writer_incarnation,
                writer_epoch=cursor.writer_epoch,
                high_water=cursor.high_water,
                authority_digest=cursor.authority_digest,
                freeze_acknowledgement_digest="c" * 64,
            )
            for cursor in preparation.writer_cursors
        ),
    )


@pytest.mark.asyncio
async def test_preparation_exact_replay_converges_and_is_audited_once(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    preparation = _preparation(registration)

    async def prepare_once() -> LegacyCompatibilityPreparationV1:
        async with _agent_session(capacity_guard_database) as session:
            return await LegacyCompatibilityFenceStore(
                session,
                registration=registration,
            ).prepare(preparation)

    assert await asyncio.gather(prepare_once(), prepare_once()) == [
        preparation,
        preparation,
    ]

    async with _owner_session(capacity_guard_database) as (_, _, session):
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.legacy_compatibility_preparations) "
                        "AS preparations, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.legacy_writer_cursors) AS cursors, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events "
                        "WHERE event_type = 'legacy_compatibility_prepared.v1') AS audits"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {
        "preparations": 1,
        "cursors": len(LEGACY_MUTATION_PATH_IDS),
        "audits": 1,
    }


@pytest.mark.asyncio
async def test_preparation_preserves_distinct_oldlab_and_gb10_writer_domains(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    preparation = _preparation(registration)
    path_id = "slurm-job-launch-registry-release"
    other = tuple(
        cursor for cursor in preparation.writer_cursors if cursor.mutation_path_id != path_id
    )
    source = next(
        cursor for cursor in preparation.writer_cursors if cursor.mutation_path_id == path_id
    )
    pool_cursors = (
        source.model_copy(update={"writer_domain": "pool-gb10"}),
        source.model_copy(update={"writer_domain": "pool-oldlab", "writer_incarnation": uuid4()}),
    )
    expanded = tuple(
        sorted(
            (*other, *pool_cursors),
            key=lambda cursor: (cursor.mutation_path_id, cursor.writer_domain),
        )
    )
    preparation = LegacyCompatibilityPreparationV1.model_validate(
        {**preparation.model_dump(mode="python"), "writer_cursors": expanded}
    )
    async with _agent_session(capacity_guard_database) as session:
        await LegacyCompatibilityFenceStore(
            session,
            registration=registration,
        ).prepare(preparation)

    async with _owner_session(capacity_guard_database) as (_, _, session):
        domains = tuple(
            (
                await session.execute(
                    text(
                        "SELECT writer_domain FROM "
                        "loom_capacity_guard.legacy_writer_cursors "
                        "WHERE mutation_path_id = :path_id ORDER BY writer_domain"
                    ),
                    {"path_id": path_id},
                )
            ).scalars()
        )
    assert domains == ("pool-gb10", "pool-oldlab")


@pytest.mark.asyncio
async def test_only_one_exact_legacy_preparation_can_exist(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    first = _preparation(registration)
    second = _preparation(registration)

    async def prepare(value: LegacyCompatibilityPreparationV1):
        async with _agent_session(capacity_guard_database) as session:
            return await LegacyCompatibilityFenceStore(
                session,
                registration=registration,
            ).prepare(value)

    results = await asyncio.gather(prepare(first), prepare(second), return_exceptions=True)
    assert sum(isinstance(value, LegacyCompatibilityPreparationV1) for value in results) == 1
    assert sum(isinstance(value, DBAPIError) for value in results) == 1


@pytest.mark.asyncio
async def test_database_rejects_expired_or_shape_extended_preparation_payloads(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    expired = _preparation(registration).model_copy(
        update={"compatibility_not_after": datetime.now(UTC) - timedelta(seconds=1)}
    )
    with pytest.raises(DBAPIError, match="has expired"):
        async with _agent_session(capacity_guard_database) as session:
            await LegacyCompatibilityFenceStore(
                session,
                registration=registration,
            ).prepare(expired)

    preparation = _preparation(registration)
    for mutate in (
        lambda payload: payload.update({"unexpected_authority": False}),
        lambda payload: payload["writer_cursors"][0].update({"unexpected_cursor": 0}),
    ):
        payload = preparation.model_dump(mode="json")
        mutate(payload)
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        with pytest.raises(DBAPIError, match=r"invalid or incomplete|cursor is invalid"):
            async with _agent_session(capacity_guard_database) as session:
                await session.execute(
                    text(
                        "SELECT loom_capacity_guard.prepare_inert_legacy_compatibility("
                        ":agent_incarnation, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :payload_digest)"
                    ),
                    {
                        "agent_incarnation": registration.agent_incarnation,
                        "payload": encoded.decode("ascii"),
                        "canonical_payload": encoded,
                        "payload_digest": hashlib.sha256(encoded).hexdigest(),
                    },
                )


@pytest.mark.asyncio
async def test_legacy_and_global_preparations_cannot_coexist_in_either_order(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, global_plan = await _initialize_prepared_plan(capacity_guard_database)
    legacy = _preparation(registration)
    async with _agent_session(capacity_guard_database) as session:
        await CapacityPreparedAdmissionStore(
            session,
            registration=registration,
        ).prepare_plan(global_plan)
    with pytest.raises(DBAPIError, match="global admission preparation already exists"):
        async with _agent_session(capacity_guard_database) as session:
            await LegacyCompatibilityFenceStore(
                session,
                registration=registration,
            ).prepare(legacy)


@pytest.mark.asyncio
async def test_legacy_preparation_blocks_later_global_preparation(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, global_plan = await _initialize_prepared_plan(capacity_guard_database)
    legacy = _preparation(registration)
    async with _agent_session(capacity_guard_database) as session:
        await LegacyCompatibilityFenceStore(
            session,
            registration=registration,
        ).prepare(legacy)
    with pytest.raises(DBAPIError, match="legacy compatibility preparation already exists"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityPreparedAdmissionStore(
                session,
                registration=registration,
            ).prepare_plan(global_plan)


@pytest.mark.asyncio
async def test_freeze_requires_and_exactly_matches_the_preparation(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    preparation = _preparation(registration)
    freeze = _freeze(registration, preparation)

    with pytest.raises(DBAPIError, match="prepared legacy compatibility"):
        async with _agent_session(capacity_guard_database) as session:
            await LegacyCompatibilityFenceStore(
                session,
                registration=registration,
            ).freeze(freeze)

    async with _agent_session(capacity_guard_database) as session:
        store = LegacyCompatibilityFenceStore(session, registration=registration)
        assert await store.prepare(preparation) == preparation

    for mutate in (
        lambda payload: payload.update({"unexpected_freeze_authority": False}),
        lambda payload: payload["writer_cursors"][0].update({"unexpected_freeze_cursor": 0}),
    ):
        payload = freeze.model_dump(mode="json")
        mutate(payload)
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        with pytest.raises(DBAPIError, match=r"freeze.*differs"):
            async with _agent_session(capacity_guard_database) as session:
                await session.execute(
                    text(
                        "SELECT loom_capacity_guard.freeze_inert_legacy_compatibility("
                        ":agent_incarnation, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :payload_digest)"
                    ),
                    {
                        "agent_incarnation": registration.agent_incarnation,
                        "payload": encoded.decode("ascii"),
                        "canonical_payload": encoded,
                        "payload_digest": hashlib.sha256(encoded).hexdigest(),
                    },
                )

    async with _agent_session(capacity_guard_database) as session:
        store = LegacyCompatibilityFenceStore(session, registration=registration)
        assert await store.freeze(freeze) == freeze
        assert await store.freeze(freeze) == freeze

    changed_cursor = freeze.writer_cursors[0].model_copy(update={"high_water": 4})
    conflicting = LegacyCompatibilityFreezeV1.model_validate(
        {
            **freeze.model_dump(mode="python"),
            "freeze_id": uuid4(),
            "writer_cursors": (changed_cursor, *freeze.writer_cursors[1:]),
        }
    )
    with pytest.raises(DBAPIError, match="differs from prepared legacy compatibility"):
        async with _agent_session(capacity_guard_database) as session:
            await LegacyCompatibilityFenceStore(
                session,
                registration=registration,
            ).freeze(conflicting)

    async with _owner_session(capacity_guard_database) as (_, _, session):
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.legacy_compatibility_freezes) AS freezes, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events "
                        "WHERE event_type = 'legacy_compatibility_frozen.v1') AS audits"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"freezes": 1, "audits": 1}


@pytest.mark.asyncio
async def test_concurrent_exact_freeze_replay_converges(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    preparation = _preparation(registration)
    freeze = _freeze(registration, preparation)
    async with _agent_session(capacity_guard_database) as session:
        await LegacyCompatibilityFenceStore(
            session,
            registration=registration,
        ).prepare(preparation)

    async def freeze_once() -> LegacyCompatibilityFreezeV1:
        async with _agent_session(capacity_guard_database) as session:
            return await LegacyCompatibilityFenceStore(
                session,
                registration=registration,
            ).freeze(freeze)

    assert await asyncio.gather(freeze_once(), freeze_once()) == [freeze, freeze]


@pytest.mark.asyncio
async def test_frozen_records_are_append_only_and_do_not_activate_authority(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    preparation = _preparation(registration)
    freeze = _freeze(registration, preparation)
    async with _agent_session(capacity_guard_database) as session:
        store = LegacyCompatibilityFenceStore(session, registration=registration)
        await store.prepare(preparation)
        await store.freeze(freeze)

    for statement in (
        "UPDATE loom_capacity_guard.legacy_compatibility_preparations SET executable = true",
        "DELETE FROM loom_capacity_guard.legacy_writer_cursors",
        "TRUNCATE loom_capacity_guard.legacy_compatibility_freezes CASCADE",
    ):
        with pytest.raises(DBAPIError, match=r"append-only|check constraint"):
            async with _owner_session(capacity_guard_database) as (_, _, session):
                await session.execute(text(statement))

    async with _owner_session(capacity_guard_database) as (_, _, session):
        state = (
            (
                await session.execute(
                    text(
                        "SELECT a.authority_mode, a.allocation_epoch, "
                        "c.activation_state, c.activation_epoch, "
                        "c.executable_new_capacity_ceiling, c.live_claim_entry_enabled, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.protected_claim_leases) AS live_leases "
                        "FROM loom_capacity_guard.authority_state AS a "
                        "JOIN loom_capacity_guard.claim_guard_activation AS c "
                        "ON c.singleton_id = a.singleton_id"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(state) == {
        "authority_mode": "disabled",
        "allocation_epoch": 0,
        "activation_state": "disabled",
        "activation_epoch": 0,
        "executable_new_capacity_ceiling": 0,
        "live_claim_entry_enabled": False,
        "live_leases": 0,
    }
