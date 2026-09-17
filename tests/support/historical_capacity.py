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
from uuid import UUID

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

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
