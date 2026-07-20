"""Migration 0066 adds fail-closed staging lifecycle authority."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from loom.data_lifecycle import (
    DataClass,
    OwnerKind,
    StagingCapacity,
    staging_capacity_policy_digest,
)
from loom.data_lifecycle_capacity import StagingCapacityEvidence
from loom.data_lifecycle_capacity_sql import SqlAlchemyStagingCapacityStore
from loom.data_lifecycle_gc import (
    AuthorityInventory,
    GcScope,
    LifecycleGcExecutionError,
    RegisteredObject,
    build_gc_plan,
    execute_gc,
    resume_gc,
)
from loom.data_lifecycle_gc_sql import SqlAlchemyGcJournal
from loom.data_lifecycle_inventory_sql import SqlAlchemyLifecycleInventory
from loom.data_lifecycle_legacy_sql import SqlAlchemyLegacyClassifier
from loom.data_lifecycle_registry import RuntimeLifecycleScope, ensure_lifecycle_authority
from loom.staging_mutation_epoch import (
    MutationEpochAdvance,
    ProtectedMutationClass,
    advance_mutation_epoch,
)
from loom.staging_mutation_epoch_sql import SqlAlchemyMutationEpochStore


def _cfg(postgres_url: str) -> Config:
    repo_root = Path(__file__).resolve().parents[2]
    cfg = Config(str(repo_root / "migrations" / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", postgres_url)
    return cfg


@pytest.fixture(scope="module")
def postgres_url_at_0065() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://",
            "postgresql+psycopg://",
        )
        command.upgrade(_cfg(url), "0065")
        yield url


def test_upgrade_adds_authority_journal_and_nullable_execution_links(
    postgres_url_at_0065: str,
) -> None:
    cfg = _cfg(postgres_url_at_0065)
    engine = create_engine(postgres_url_at_0065)
    try:
        command.upgrade(cfg, "head")
        with engine.connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='public' AND table_name LIKE 'data_lifecycle_%'"
                    )
                )
            }
            linked = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.columns "
                        "WHERE table_schema='public' "
                        "AND column_name='lifecycle_authority_id'"
                    )
                )
            }
            mutation_tables = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='public' AND table_name LIKE 'staging_mutation_%'"
                    )
                )
            }
    finally:
        engine.dispose()

    assert tables == {
        "data_lifecycle_authorities",
        "data_lifecycle_objects",
        "data_lifecycle_gc_runs",
        "data_lifecycle_gc_items",
    }
    assert linked == {"batches", "trials", "llm_calls", "trial_events", "artifacts"}
    assert mutation_tables == {
        "staging_mutation_epochs",
        "staging_mutation_epoch_events",
    }


def test_constraints_reject_unbounded_or_cross_environment_authority(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    try:
        with engine.begin() as conn:
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        "INSERT INTO data_lifecycle_authorities "
                        "(environment, namespace, data_class, owner_kind, owner_id, pinned) "
                        "VALUES ('staging','loom-staging','trial','trial','bad',false)"
                    )
                )
        with engine.begin() as conn:
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        "INSERT INTO staging_mutation_epochs "
                        "(environment, namespace, epoch, reason) "
                        "VALUES ('production','loom-prod',0,'bad')"
                    )
                )
    finally:
        engine.dispose()


def test_capacity_store_publishes_only_newer_exact_evidence(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    newer_capacity = StagingCapacity(12, 34, 56, 78)
    older_capacity = StagingCapacity(99, 99, 99, 99)
    newer_at = datetime(2026, 7, 20, 0, 5, tzinfo=UTC)
    try:
        store = SqlAlchemyStagingCapacityStore(engine)
        store.publish(
            StagingCapacityEvidence(
                namespace="loom-staging",
                capacity=newer_capacity,
                policy_sha256=staging_capacity_policy_digest(),
                evidence_sha256=newer_capacity.evidence_digest,
                observed_at=newer_at,
            )
        )
        with pytest.raises(RuntimeError, match="lost freshness authority"):
            store.publish(
                StagingCapacityEvidence(
                    namespace="loom-staging",
                    capacity=older_capacity,
                    policy_sha256=staging_capacity_policy_digest(),
                    evidence_sha256=older_capacity.evidence_digest,
                    observed_at=newer_at - timedelta(minutes=1),
                )
            )
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT namespace,object_count,bytes_used,disk_free_percent,"
                    "inode_free_percent,evidence_sha256,observed_at "
                    "FROM staging_lifecycle_capacity WHERE environment='staging'"
                )
            ).one()
        assert tuple(row) == (
            "loom-staging",
            12,
            34,
            56,
            78,
            newer_capacity.evidence_digest,
            newer_at,
        )
    finally:
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM staging_lifecycle_capacity"))
        engine.dispose()


def test_epoch_compare_and_swap_updates_authority_and_appends_exact_event(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    occurred_at = datetime(2026, 7, 20, tzinfo=UTC)
    advance = MutationEpochAdvance(
        environment="staging",
        namespace="loom-staging",
        expected_epoch=0,
        mutation_class=ProtectedMutationClass.LIFECYCLE_GC,
        request_id="req-gc0000000",
        evidence_sha256="a" * 64,
        occurred_at=occurred_at,
    )
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO staging_mutation_epochs "
                    "(environment, namespace, epoch, reason) "
                    "VALUES ('staging','loom-staging',0,'bootstrap') "
                    "ON CONFLICT (environment) DO NOTHING"
                )
            )
            state = advance_mutation_epoch(
                SqlAlchemyMutationEpochStore(conn),
                advance,
            )
        with engine.connect() as conn:
            epoch_row = conn.execute(
                text(
                    "SELECT epoch, reason, request_id, evidence_sha256 "
                    "FROM staging_mutation_epochs WHERE environment='staging'"
                )
            ).one()
            event_row = conn.execute(
                text(
                    "SELECT epoch, mutation_class, request_id, evidence_sha256 "
                    "FROM staging_mutation_epoch_events WHERE environment='staging'"
                )
            ).one()
        assert state.epoch == 1
        assert tuple(epoch_row) == (1, "lifecycle_gc", "req-gc0000000", "a" * 64)
        assert tuple(event_row) == (1, "lifecycle_gc", "req-gc0000000", "a" * 64)

        with engine.begin() as conn:
            with pytest.raises(RuntimeError, match="stale"):
                advance_mutation_epoch(SqlAlchemyMutationEpochStore(conn), advance)
    finally:
        engine.dispose()


def test_sql_gc_journal_commits_exact_phases_and_mutation_epoch(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    authority_id = uuid4()
    object_id = uuid4()
    now = datetime(2026, 7, 20, 1, tzinfo=UTC)
    with engine.connect() as connection:
        epoch = connection.execute(
            text("SELECT epoch FROM staging_mutation_epochs WHERE environment='staging'")
        ).scalar_one()
    plan = build_gc_plan(
        scope=GcScope(environment="staging", namespace="loom-staging"),
        mutation_epoch=epoch,
        now=now,
        authorities=[
            AuthorityInventory(
                id=authority_id,
                environment="staging",
                namespace="loom-staging",
                owner_kind="trial",
                owner_id="integration-trial",
                expires_at=now - timedelta(days=1),
                pinned=False,
                state="active",
            )
        ],
        objects=[
            RegisteredObject(
                id=object_id,
                authority_id=authority_id,
                environment="staging",
                namespace="loom-staging",
                bucket="loom-staging-artifacts",
                object_key="integration/trial.json",
                version_id="version-1",
                content_sha256="b" * 64,
                size_bytes=7,
                state="active",
            )
        ],
    )

    class NoBusinessRows:
        def delete_exact(self, connection, authority_ids) -> None:
            assert authority_ids == (authority_id,)

    class Deleter:
        deleted = False
        verify = False

        def delete_exact(self, item) -> None:
            assert item.id == object_id
            self.deleted = True

        def exact_absent(self, item) -> bool:
            return self.verify and self.deleted and item.id == object_id

    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_authorities "
                    "(id, environment, namespace, data_class, owner_kind, owner_id, "
                    "created_at, expires_at, pinned, state) VALUES "
                    "(:id,'staging','loom-staging','trial','trial','integration-trial',"
                    ":created_at,:expires_at,false,'active')"
                ),
                {
                    "id": authority_id,
                    "created_at": now - timedelta(days=8),
                    "expires_at": now - timedelta(days=1),
                },
            )
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_objects "
                    "(id, authority_id, environment, namespace, bucket, object_key, "
                    "version_id, content_sha256, size_bytes, created_at, state) VALUES "
                    "(:id,:authority_id,'staging','loom-staging','loom-staging-artifacts',"
                    "'integration/trial.json','version-1',:sha,7,:created_at,'active')"
                ),
                {
                    "id": object_id,
                    "authority_id": authority_id,
                    "sha": "b" * 64,
                    "created_at": now - timedelta(days=8),
                },
            )
        journal = SqlAlchemyGcJournal(engine, metadata_purger=NoBusinessRows())
        deleter = Deleter()
        with pytest.raises(LifecycleGcExecutionError, match="still present"):
            execute_gc(
                plan=plan,
                requested_by="qianyi",
                journal=journal,
                object_deleter=deleter,
                dry_run=False,
                request_id="req-gcinteg00",
                completed_at=now,
            )
        with engine.connect() as connection:
            run_id = connection.execute(
                text(
                    "SELECT id FROM data_lifecycle_gc_runs "
                    "WHERE inventory->>'inventory_digest'=:digest"
                ),
                {"digest": plan.inventory_digest},
            ).scalar_one()
        deleter.verify = True
        result = resume_gc(
            run_id=run_id,
            request_id="req-gcinteg00",
            completed_at=now,
            journal=journal,
            object_deleter=deleter,
        )
        with engine.connect() as connection:
            run = connection.execute(
                text(
                    "SELECT state, mutation_epoch_before, mutation_epoch_after "
                    "FROM data_lifecycle_gc_runs WHERE id=:id"
                ),
                {"id": result.run_id},
            ).one()
            item_state = connection.execute(
                text(
                    "SELECT state FROM data_lifecycle_gc_items "
                    "WHERE gc_run_id=:id AND object_id=:object_id"
                ),
                {"id": result.run_id, "object_id": object_id},
            ).scalar_one()
            authority_count = connection.execute(
                text("SELECT count(*) FROM data_lifecycle_authorities WHERE id=:id"),
                {"id": authority_id},
            ).scalar_one()
        assert tuple(run) == ("completed", epoch, epoch + 1)
        assert item_state == "metadata_deleted"
        assert authority_count == 0
    finally:
        engine.dispose()


def test_sql_inventory_is_read_only_and_binds_unclassified_counts(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    scope = GcScope(environment="staging", namespace="loom-staging")
    try:
        with engine.connect() as connection:
            gc_runs_before = connection.execute(
                text("SELECT count(*) FROM data_lifecycle_gc_runs")
            ).scalar_one()
        snapshot = SqlAlchemyLifecycleInventory(engine).load(scope=scope)
        plan = snapshot.build_plan(now=datetime(2026, 7, 20, 2, tzinfo=UTC))
        with engine.connect() as connection:
            gc_runs_after = connection.execute(
                text("SELECT count(*) FROM data_lifecycle_gc_runs")
            ).scalar_one()
    finally:
        engine.dispose()

    assert snapshot.scope == scope
    assert dict(snapshot.unclassified_rows) == {
        "artifacts": 0,
        "batches": 0,
        "llm_calls": 0,
        "trial_events": 0,
        "trials": 0,
    }
    assert plan.mutation_epoch == snapshot.mutation_epoch
    assert gc_runs_after == gc_runs_before


def test_execution_authority_registration_is_transactional_and_idempotent(
    postgres_url_at_0065: str,
) -> None:
    team_id = uuid4()
    owner_id = f"trial-{uuid4()}"
    created_at = datetime(2026, 7, 20, 3, tzinfo=UTC)
    engine = create_engine(postgres_url_at_0065)
    async_engine = create_async_engine(postgres_url_at_0065)
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
            {"id": team_id, "name": f"lifecycle-{team_id}"},
        )

    async def _register() -> tuple[object, object]:
        sessions = async_sessionmaker(async_engine, expire_on_commit=False)
        spec = RuntimeLifecycleScope(
            environment="staging", namespace="loom-staging"
        ).authority_spec(
            team_id=team_id,
            data_class=DataClass.TRIAL,
            owner_kind=OwnerKind.TRIAL,
            owner_id=owner_id,
            created_at=created_at,
        )
        async with sessions.begin() as session:
            first = await ensure_lifecycle_authority(session, spec=spec)
            second = await ensure_lifecycle_authority(session, spec=spec)
        return first, second

    try:
        first, second = asyncio.run(_register())
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT environment,namespace,team_id,data_class,owner_kind,owner_id,"
                    "created_at,expires_at,pinned,state FROM data_lifecycle_authorities "
                    "WHERE id=:id"
                ),
                {"id": first},
            ).one()
        assert first == second
        assert tuple(row) == (
            "staging",
            "loom-staging",
            team_id,
            "trial",
            "trial",
            owner_id,
            created_at,
            created_at + timedelta(days=7),
            False,
            "active",
        )
    finally:
        asyncio.run(async_engine.dispose())
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM data_lifecycle_authorities WHERE owner_id=:owner_id"),
                {"owner_id": owner_id},
            )
            connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
        engine.dispose()


def test_legacy_artifact_classification_is_digest_approved_and_epoch_bound(
    postgres_url_at_0065: str,
) -> None:
    team_id = uuid4()
    artifact_id = uuid4()
    created_at = datetime(2026, 7, 12, 4, tzinfo=UTC)
    object_body = b"legacy artifact body"
    object_sha = hashlib.sha256(object_body).hexdigest()
    engine = create_engine(postgres_url_at_0065)

    class Inspector:
        def inspect(self, *, bucket, object_key, version_id):
            assert bucket == "loom-staging-artifacts"
            assert object_key == f"teams/{team_id}/artifacts/{artifact_id}.json"
            assert version_id is None
            return None, object_sha, len(object_body)

    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
                {"id": team_id, "name": f"legacy-{team_id}"},
            )
            connection.execute(
                text(
                    "INSERT INTO artifacts "
                    "(id, artifact_type, name, team_id, content_hash, storage, created_at) "
                    "VALUES (:id,'benchmark','legacy',:team_id,:content_hash,"
                    "CAST(:storage AS jsonb),:created_at)"
                ),
                {
                    "id": artifact_id,
                    "team_id": team_id,
                    "content_hash": f"sha256:{object_sha}",
                    "storage": json.dumps(
                        {
                            "bucket": "loom-staging-artifacts",
                            "key": f"teams/{team_id}/artifacts/{artifact_id}.json",
                            "size_bytes": len(object_body),
                        }
                    ),
                    "created_at": created_at,
                },
            )
        classifier = SqlAlchemyLegacyClassifier(engine, Inspector())
        scope = GcScope(environment="staging", namespace="loom-staging")
        plan = classifier.inventory(
            scope=scope,
            planned_at=datetime(2026, 7, 20, 4, tzinfo=UTC),
        )
        plan.require_applicable()
        assert [row.row_id for row in plan.rows] == [artifact_id]
        assert plan.objects[0].content_sha256 == object_sha
        assert plan.authorities[0].data_class is DataClass.BENCHMARK
        assert plan.authorities[0].pinned is True
        assert plan.authorities[0].expires_at is None

        with pytest.raises(RuntimeError, match="digest"):
            classifier.apply(
                plan=plan,
                approved_inventory_digest="0" * 64,
                request_id="req-legacybad0",
                applied_at=datetime(2026, 7, 20, 4, 1, tzinfo=UTC),
            )

        state = classifier.apply(
            plan=plan,
            approved_inventory_digest=plan.inventory_digest,
            request_id="req-legacygood",
            applied_at=datetime(2026, 7, 20, 4, 2, tzinfo=UTC),
        )
        with engine.connect() as connection:
            artifact_authority = connection.execute(
                text("SELECT lifecycle_authority_id FROM artifacts WHERE id=:id"),
                {"id": artifact_id},
            ).scalar_one()
            registered = connection.execute(
                text(
                    "SELECT authority_id, content_sha256, size_bytes "
                    "FROM data_lifecycle_objects WHERE authority_id=:id"
                ),
                {"id": artifact_authority},
            ).one()
            pinned_authority = connection.execute(
                text(
                    "SELECT data_class, owner_kind, pinned, expires_at "
                    "FROM data_lifecycle_authorities WHERE id=:id"
                ),
                {"id": artifact_authority},
            ).one()
        assert state.epoch == plan.mutation_epoch + 1
        assert tuple(registered) == (artifact_authority, object_sha, len(object_body))
        assert tuple(pinned_authority) == ("benchmark", "benchmark", True, None)
    finally:
        with engine.begin() as connection:
            authority_ids = list(
                connection.execute(
                    text(
                        "SELECT lifecycle_authority_id FROM artifacts "
                        "WHERE id=:id AND lifecycle_authority_id IS NOT NULL"
                    ),
                    {"id": artifact_id},
                ).scalars()
            )
            connection.execute(text("DELETE FROM artifacts WHERE id=:id"), {"id": artifact_id})
            if authority_ids:
                connection.execute(
                    text(
                        "DELETE FROM data_lifecycle_objects WHERE authority_id IN :ids"
                    ).bindparams(bindparam("ids", expanding=True)),
                    {"ids": authority_ids},
                )
                connection.execute(
                    text("DELETE FROM data_lifecycle_authorities WHERE id IN :ids").bindparams(
                        bindparam("ids", expanding=True)
                    ),
                    {"ids": authority_ids},
                )
            connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
        engine.dispose()


def test_legacy_absent_object_is_bound_as_explicit_authority_evidence(
    postgres_url_at_0065: str,
) -> None:
    team_id = uuid4()
    artifact_id = uuid4()
    created_at = datetime(2026, 7, 12, 5, tzinfo=UTC)
    engine = create_engine(postgres_url_at_0065)

    class Inspector:
        def inspect(self, *, bucket, object_key, version_id):
            assert bucket == "loom-staging-artifacts"
            assert object_key == f"teams/{team_id}/artifacts/{artifact_id}.json"
            assert version_id is None
            return None

    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
                {"id": team_id, "name": f"legacy-absent-{team_id}"},
            )
            connection.execute(
                text(
                    "INSERT INTO artifacts "
                    "(id, artifact_type, name, team_id, content_hash, storage, created_at) "
                    "VALUES (:id,'evidence_bundle','legacy-absent',:team_id,"
                    "'pending:legacy-unhashed',CAST(:storage AS jsonb),:created_at)"
                ),
                {
                    "id": artifact_id,
                    "team_id": team_id,
                    "storage": json.dumps(
                        {
                            "bucket": "artifacts",
                            "key": f"teams/{team_id}/artifacts/{artifact_id}.json",
                            "size_bytes": 0,
                        }
                    ),
                    "created_at": created_at,
                },
            )
        classifier = SqlAlchemyLegacyClassifier(
            engine,
            Inspector(),
            bucket_aliases={"artifacts": "loom-staging-artifacts"},
        )
        scope = GcScope(environment="staging", namespace="loom-staging")
        plan = classifier.inventory(
            scope=scope,
            planned_at=datetime(2026, 7, 20, 5, tzinfo=UTC),
        )
        plan.require_applicable()
        assert plan.objects == ()
        assert [item.row_id for item in plan.absent_objects] == [artifact_id]

        state = classifier.apply(
            plan=plan,
            approved_inventory_digest=plan.inventory_digest,
            request_id="req-legacyabsent",
            applied_at=datetime(2026, 7, 20, 5, 1, tzinfo=UTC),
        )
        with engine.connect() as connection:
            artifact_authority = connection.execute(
                text("SELECT lifecycle_authority_id FROM artifacts WHERE id=:id"),
                {"id": artifact_id},
            ).scalar_one()
            registered_count = connection.execute(
                text("SELECT count(*) FROM data_lifecycle_objects WHERE authority_id=:id"),
                {"id": artifact_authority},
            ).scalar_one()
            authority = connection.execute(
                text(
                    "SELECT data_class, pinned, metadata FROM data_lifecycle_authorities "
                    "WHERE id=:id"
                ),
                {"id": artifact_authority},
            ).one()
        assert state.epoch == plan.mutation_epoch + 1
        assert registered_count == 0
        assert authority.data_class == "artifact"
        assert authority.pinned is False
        assert authority.metadata == {
            "classification": "legacy-staging-v1",
            "expire_created_before": None,
            "inventory_digest": plan.inventory_digest,
            "object_state": "verified_absent",
        }
    finally:
        with engine.begin() as connection:
            authority_ids = list(
                connection.execute(
                    text(
                        "SELECT lifecycle_authority_id FROM artifacts "
                        "WHERE id=:id AND lifecycle_authority_id IS NOT NULL"
                    ),
                    {"id": artifact_id},
                ).scalars()
            )
            connection.execute(text("DELETE FROM artifacts WHERE id=:id"), {"id": artifact_id})
            if authority_ids:
                connection.execute(
                    text(
                        "DELETE FROM data_lifecycle_objects WHERE authority_id IN :ids"
                    ).bindparams(bindparam("ids", expanding=True)),
                    {"ids": authority_ids},
                )
                connection.execute(
                    text("DELETE FROM data_lifecycle_authorities WHERE id IN :ids").bindparams(
                        bindparam("ids", expanding=True)
                    ),
                    {"ids": authority_ids},
                )
            connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
        engine.dispose()


def test_downgrade_refuses_to_discard_lifecycle_data(postgres_url_at_0065: str) -> None:
    cfg = _cfg(postgres_url_at_0065)
    engine = create_engine(postgres_url_at_0065)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO staging_mutation_epochs "
                    "(environment, namespace, epoch, reason) "
                    "VALUES ('staging','loom-staging',0,'bootstrap') "
                    "ON CONFLICT (environment) DO NOTHING"
                )
            )
        with pytest.raises(Exception, match="deployment data remains"):
            command.downgrade(cfg, "0065")
    finally:
        engine.dispose()
