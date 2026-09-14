"""Protected SQL preserves typed application cleanup and refuses build proofs."""

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from uuid import UUID

import pytest
from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.typed_inventory_contracts import parse_terminal_inventory_evidence
from tests.integration.test_capacity_guard_migrations import _guard_config
from tests.integration.test_capacity_protected_worker_session import (
    _import_terminal_inventory_payload,
    _seed_claimed_protected_trial,
    _terminal_inventory_evidence,
    _value,
)


def typed_payload(seeded, *, owner=None):
    value = _terminal_inventory_evidence(seeded).model_dump(mode="json")
    record = value["record"]
    proof = record["ownership_proof"]
    metadata = proof["metadata"]
    for node in (value, record, proof, metadata):
        node["schema_version"] = 3
    binding = value["binding"]
    metadata["launch_profile_sha256"] = "a" * 64
    metadata["subject_authority"] = dict(
        schema_version=3,
        source="immutable-base",
        purpose="application-worker",
        configuration=dict(
            schema_version=1,
            scope="subject",
            subject_id=binding["subject_id"],
            subject_incarnation=binding["subject_incarnation"],
            generation=1,
            digest="b" * 64,
        ),
        acknowledgement_sha256="c" * 64,
        membership=None,
    )
    if owner is not None:
        metadata["subject_authority"].update(
            source="personal-membership",
            membership=dict(
                schema_version=3,
                namespace_id=str(UUID(int=990012)),
                owner_id=str(owner),
                revision=1,
                head_sha256="d" * 64,
                execution_manifest_sha256=binding["execution"]["execution_manifest_sha256"],
            ),
        )
    return parse_terminal_inventory_evidence(json.dumps(value)).model_dump(mode="json")


def seed_delegated_claim(database, monkeypatch, tmp_path, *, owner):
    from tests.integration import test_capacity_protected_worker_session as worker_module

    # Change fixture inputs before its real protected registration/claim writes;
    # never mutate a live claim or manufacture an admitted database row.
    original_bootstrap = worker_module._bootstrap
    original_seed = worker_module._seed_protected_worker
    original_projection = worker_module._public_registration_payload

    def bootstrap(*args):
        value = original_bootstrap(*args)
        return value.model_copy(
            update={
                "binding": value.binding.model_copy(
                    update={
                        "account_id": f"dev-owner-{owner.hex}",
                    }
                )
            }
        )

    async def seed(database):
        return await original_seed(database, environment_id="dev-alice", tier_id="development")

    monkeypatch.setattr(worker_module, "_bootstrap", bootstrap)
    monkeypatch.setattr(worker_module, "_seed_protected_worker", seed)
    monkeypatch.setattr(
        worker_module,
        "_public_registration_payload",
        lambda: original_projection(sandbox_identity="loom-dev-alice"),
    )
    return _seed_claimed_protected_trial(database, monkeypatch, tmp_path)


@pytest.mark.parametrize("delegated", (False, True))
def test_typed_terminal_sql_import_preserves_exact_bytes_and_restart_idempotence(
    capacity_guard_database,
    monkeypatch,
    tmp_path,
    delegated,
):
    owner = UUID(int=990011) if delegated else None
    seeded = (
        seed_delegated_claim(capacity_guard_database, monkeypatch, tmp_path, owner=owner)
        if delegated
        else _seed_claimed_protected_trial(capacity_guard_database, monkeypatch, tmp_path)
    )
    payload = typed_payload(seeded, owner=owner)
    first = asyncio.run(
        _import_terminal_inventory_payload(capacity_guard_database, seeded, payload)
    )
    replay = asyncio.run(
        _import_terminal_inventory_payload(capacity_guard_database, seeded, payload)
    )
    assert first == replay
    evidence = parse_terminal_inventory_evidence(json.dumps(payload))
    assert first["evidence_digest"] == canonical_executable_digest(evidence)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT evidence_payload FROM loom_capacity_guard.executable_terminal_inventory_evidence"
                    )
                ).scalar_one()
                == payload
            )
            assert (
                connection.execute(
                    text("SELECT draining FROM loom_capacity_guard.executable_claim_state")
                ).scalar_one()
                is True
            )
    finally:
        engine.dispose()


def test_typed_terminal_import_migration_preserves_authority_and_reverses_empty_database(
    capacity_guard_database,
):
    config = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    signature = "loom_capacity_guard.import_executable_terminal_inventory_evidence(uuid,uuid,jsonb,bytea,text)"
    query = text(
        "SELECT pg_get_functiondef(oid), proowner, proacl, prosecdef, proconfig "
        "FROM pg_proc WHERE oid = CAST(:signature AS regprocedure)"
    )
    try:
        with engine.connect() as connection:
            before = connection.execute(query, {"signature": signature}).one()
        command.downgrade(config, "guard_0030")
        with engine.connect() as connection:
            legacy = connection.execute(query, {"signature": signature}).one()
            assert "guard_0032: exact" not in legacy[0]
            assert legacy[1:] == before[1:]
        command.upgrade(config, "head")
        with engine.connect() as connection:
            after = connection.execute(query, {"signature": signature}).one()
            assert after == before
    finally:
        engine.dispose()


def test_typed_terminal_import_refuses_downgrade_with_retained_evidence(
    capacity_guard_database,
    monkeypatch,
    tmp_path,
):
    seeded = _seed_claimed_protected_trial(capacity_guard_database, monkeypatch, tmp_path)
    payload = typed_payload(seeded)
    asyncio.run(_import_terminal_inventory_payload(capacity_guard_database, seeded, payload))
    with pytest.raises(RuntimeError, match="cannot downgrade guard_0032"):
        command.downgrade(_guard_config(capacity_guard_database), "guard_0030")
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0033"
            )
            assert (
                connection.execute(
                    text(
                        "SELECT evidence_payload FROM loom_capacity_guard.executable_terminal_inventory_evidence"
                    )
                ).scalar_one()
                == payload
            )
    finally:
        engine.dispose()


def test_downgrade_fences_an_import_already_executing_the_old_function_body(
    capacity_guard_database,
    monkeypatch,
    tmp_path,
):
    seeded = _seed_claimed_protected_trial(capacity_guard_database, monkeypatch, tmp_path)
    payload = typed_payload(seeded)
    config = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with ThreadPoolExecutor(max_workers=1) as workers:
            with engine.begin() as lock:
                # The importer has already validated the typed payload before
                # waiting here, but has not reached the evidence table yet.
                lock.execute(
                    text(
                        "SELECT 1 FROM loom_capacity_guard.agent_runtime_authority "
                        "WHERE singleton_id=1 FOR UPDATE"
                    )
                )
                future = workers.submit(
                    asyncio.run,
                    _import_terminal_inventory_payload(capacity_guard_database, seeded, payload),
                )
                deadline = time.monotonic() + 10
                while True:
                    with engine.connect() as observer:
                        waiting = observer.execute(
                            text(
                                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE "
                                "usename=:agent AND wait_event_type='Lock' AND "
                                "query LIKE '%import_executable_terminal_inventory_evidence%')"
                            ),
                            {"agent": _value(capacity_guard_database, "agent_role")},
                        ).scalar_one()
                    if waiting:
                        break
                    assert not future.done(), "importer completed before the intended lock fence"
                    assert time.monotonic() < deadline, "importer never reached the lock fence"
                    time.sleep(0.02)
                command.downgrade(config, "guard_0030")
            # Replacing a function does not cancel an invocation already inside
            # it. The physical table must reject its now-obsolete schema.
            with pytest.raises(DBAPIError, match="guard_terminal_inventory_schema_check"):
                future.result(timeout=10)
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM loom_capacity_guard.executable_terminal_inventory_evidence"
                    )
                ).scalar_one()
                == 0
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize("delegated", (False, True))
def test_typed_terminal_sql_rejects_purpose_and_provenance_substitution_without_writes(
    capacity_guard_database,
    monkeypatch,
    tmp_path,
    delegated,
):
    owner = UUID(int=990011) if delegated else None
    seeded = (
        seed_delegated_claim(capacity_guard_database, monkeypatch, tmp_path, owner=owner)
        if delegated
        else _seed_claimed_protected_trial(capacity_guard_database, monkeypatch, tmp_path)
    )
    original = typed_payload(seeded, owner=owner)
    for tamper in (
        "build",
        "record-version",
        "proof-version",
        "metadata-version",
        "extra-authority",
        "subject",
        "incarnation",
        "empty-profile",
        "member-absent",
        "string-version",
        "string-proof-version",
        "string-generation",
        "numeric-digest",
        "member-owner",
        "member-manifest",
        "member-revision",
    ):
        if tamper.startswith("member-") and tamper != "member-absent" and not delegated:
            continue
        payload = deepcopy(original)
        metadata = payload["record"]["ownership_proof"]["metadata"]
        authority = metadata["subject_authority"]
        if tamper == "build":
            authority["purpose"] = "personal-build-worker"
        elif tamper == "record-version":
            payload["record"]["schema_version"] = 2
        elif tamper == "proof-version":
            payload["record"]["ownership_proof"]["schema_version"] = 2
        elif tamper == "metadata-version":
            metadata["schema_version"] = 2
        elif tamper == "extra-authority":
            authority["unexpected"] = True
        elif tamper in ("subject", "incarnation"):
            field = "subject_id" if tamper == "subject" else "subject_incarnation"
            authority["configuration"][field] = "00000000-0000-0000-0000-000000000001"
        elif tamper == "empty-profile":
            metadata["launch_profile_sha256"] = "0" * 64
        elif tamper == "string-version":
            for node in (
                payload,
                payload["record"],
                payload["record"]["ownership_proof"],
                metadata,
            ):
                node["schema_version"] = "3"
        elif tamper == "string-proof-version":
            payload["record"]["ownership_proof"]["schema_version"] = "3"
        elif tamper == "string-generation":
            authority["configuration"]["generation"] = "1"
        elif tamper == "numeric-digest":
            metadata["launch_profile_sha256"] = int("1" * 64)
        elif tamper == "member-owner":
            authority["membership"]["owner_id"] = str(UUID(int=990013))
        elif tamper == "member-manifest":
            authority["membership"]["execution_manifest_sha256"] = "f" * 64
        elif tamper == "member-revision":
            authority["membership"]["revision"] = "1"
        else:
            authority["source"] = "personal-membership"
            authority["membership"] = None
        with pytest.raises(DBAPIError):
            asyncio.run(
                _import_terminal_inventory_payload(capacity_guard_database, seeded, payload)
            )
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM loom_capacity_guard.executable_terminal_inventory_evidence"
                    )
                ).scalar_one()
                == 0
            )
            assert (
                connection.execute(
                    text("SELECT draining FROM loom_capacity_guard.executable_claim_state")
                ).scalar_one()
                is False
            )
    finally:
        engine.dispose()
