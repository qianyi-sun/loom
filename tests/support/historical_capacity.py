"""Synthetic pre-retirement claim rows for disposable database lineage tests.

Captured from 020a6e928. No retired issuer or worker runtime is retained.
Restore bypass applies only to test setup; migration assertions run with guards on.
"""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial

FIXTURES = Path(__file__).parents[1] / "fixtures" / "historical"


def _value(database, key):
    return str(database[key])


def restore_claim(database, *, delegated=False):
    name = "delegated_capacity_claim" if delegated else "capacity_claim"
    document = json.loads((FIXTURES / f"{name}.json").read_text())
    rows_json = json.dumps(document["rows"])
    for key, old_role in document["role_names"].items():
        rows_json = rows_json.replace(old_role, _value(database, key))
    rows = json.loads(rows_json)
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(text("SET LOCAL session_replication_role = replica"))
            for name, values in rows.items():
                schema, table = name.split(".")
                assert schema in {"public", "loom_capacity_guard"}
                assert table.replace("_", "").isalnum()
                relation = f'"{schema}"."{table}"'
                connection.execute(text(f"DELETE FROM {relation}"))
                columns = connection.execute(text(
                    "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass(:relation) "
                    "AND attnum > 0 AND NOT attisdropped AND attgenerated='' ORDER BY attnum"
                ), {"relation": name}).scalars().all()
                projection = ", ".join('"' + column + '"' for column in columns)
                connection.execute(text(
                    f"INSERT INTO {relation} ({projection}) SELECT {projection} FROM "
                    f"json_populate_recordset(NULL::{relation}, :rows)"
                ), {"rows": json.dumps(values)})
            connection.execute(text("SET LOCAL session_replication_role = origin"))
    finally:
        engine.dispose()
    attempt = document["first_attempt"]
    attempt["protected_attempt_id"] = UUID(attempt["protected_attempt_id"])
    return SimpleNamespace(
        trial_id=UUID(document["trial_id"]), first_attempt=attempt,
        worker=SimpleNamespace(
            worker=SimpleNamespace(worker_id=UUID(document["worker_id"])),
            registration=SimpleNamespace(agent_incarnation=UUID(document["agent_incarnation"])),
        ),
        payload=document["payload"],
    )


def _seed_claimed_protected_trial(database, monkeypatch, tmp_path):
    return restore_claim(database)


def typed_payload(seeded, *, owner=None):
    return deepcopy(seeded.payload)


async def _import_terminal_inventory_payload(database, seeded, payload):
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=True, allow_nan=False).encode("ascii")
    engine = create_async_engine(_value(database, "agent_url"), isolation_level="SERIALIZABLE")
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            returned = (await session.execute(text(
                "SELECT loom_capacity_guard.import_executable_terminal_inventory_evidence("
                ":agent, :attempt, CAST(:payload AS jsonb), CAST(:canonical AS bytea), :digest)"
            ), {"agent": seeded.worker.registration.agent_incarnation,
                "attempt": seeded.first_attempt["protected_attempt_id"],
                "payload": json.dumps(payload), "canonical": canonical,
                "digest": hashlib.sha256(canonical).hexdigest()})).scalar_one()
            await session.commit()
            return returned
    finally:
        await engine.dispose()


def seed_unprotected_trial(database: dict[str, object], *, priority: int = 100) -> UUID:
    engine = create_engine(_value(database, "admin_url"))
    team_id = uuid4()
    trial_id = uuid4()
    task_id = f"guard-agent-task-{uuid4().hex}"
    try:
        with engine.begin() as connection:
            connection.execute(Team.__table__.insert().values(id=team_id, name=f"agent-{team_id}"))
            connection.execute(TeamQuota.__table__.insert().values(team_id=team_id))
            connection.execute(
                Task.__table__.insert().values(
                    id=task_id,
                    checksum="0" * 64,
                    config={"schema_version": "1"},
                )
            )
            connection.execute(
                Trial.__table__.insert().values(
                    id=trial_id,
                    team_id=team_id,
                    task_id=task_id,
                    config={},
                    requires_caps={
                        "os": "linux",
                        "cpu_arch": "x86_64",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                    },
                    state="queued",
                    submit_priority=priority,
                )
            )
    finally:
        engine.dispose()
    return trial_id


def seed_historical_agent(database, fence, registration):
    """Insert synthetic historical registration rows into a disposable lineage DB."""
    engine = create_engine(_value(database, "migrator_url"))
    owner = engine.dialect.identifier_preparer.quote(_value(database, "owner_role"))
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"SET LOCAL ROLE {owner}")
            connection.execute(text("""
                INSERT INTO loom_capacity_guard.authority_state
                (singleton_id, schema_version, environment_id, subject_id, subject_incarnation,
                 authority_mode, authority_incarnation, reporter_incarnation, reporter_high_water,
                 allocation_epoch, deployment_generation, configuration_generation, candidate_digest)
                VALUES (1, 1, :environment_id, :subject_id, :subject_incarnation, 'disabled',
                        :authority_incarnation, :reporter_incarnation, 0, 0,
                        :deployment_generation, :configuration_generation, :candidate_digest)
            """), fence)
            connection.execute(text("""
                INSERT INTO loom_capacity_guard.agent_registrations
                (agent_incarnation, singleton_id, schema_version, environment_id, subject_id,
                 subject_incarnation, authority_incarnation, reporter_incarnation, authority_mode,
                 allocation_epoch, candidate_digest, candidate_identity_algorithm, candidate_identity,
                 candidate_publication_sha256, deployment_generation, configuration_generation,
                 registration_state)
                VALUES (:agent_incarnation, 1, 1, :environment_id, :subject_id, :subject_incarnation,
                        :authority_incarnation, :reporter_incarnation, 'disabled', 0, :candidate_digest,
                        :candidate_identity_algorithm, :candidate_identity, :candidate_publication_sha256,
                        :deployment_generation, :configuration_generation, 'registered')
            """), registration)
            connection.execute(text("""
                INSERT INTO loom_capacity_guard.agent_reporter_state (agent_incarnation, high_water)
                VALUES (:agent_incarnation, 0)
            """), registration)
            for kind, payload in (("authority_initialized.v1", fence), ("agent_registered.v1", registration)):
                wire = json.dumps(payload, default=str, sort_keys=True, separators=(",", ":"),
                                  ensure_ascii=True, allow_nan=False)
                connection.execute(text("""
                    INSERT INTO loom_capacity_guard.audit_events (event_type, payload, payload_digest)
                    VALUES (:kind, CAST(:payload AS jsonb), :digest)
                """), {"kind": kind, "payload": wire, "digest": hashlib.sha256(wire.encode()).hexdigest()})
    finally:
        engine.dispose()


async def historical_agent_rows(database):
    """Minimal historical authority and agent rows for published trigger tests."""
    fence = dict(schema_version=1, environment_id="dev-alice", subject_id=uuid4(),
                 subject_incarnation=uuid4(), authority_mode="disabled", authority_incarnation=uuid4(),
                 reporter_incarnation=uuid4(), reporter_high_water=0, allocation_epoch=0,
                 deployment_generation=7, configuration_generation=11, candidate_digest="a" * 64)
    registration = dict(fence, agent_incarnation=uuid4(), candidate_identity_algorithm="source-sha256",
                        candidate_identity="a" * 64, candidate_publication_sha256="a" * 64)
    seed_historical_agent(database, fence, registration)
    return SimpleNamespace(**fence), SimpleNamespace(**registration)
