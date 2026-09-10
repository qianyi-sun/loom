"""Protected SQL preserves typed application cleanup and refuses build proofs."""

import asyncio
import json
from copy import deepcopy

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.typed_inventory_contracts import parse_terminal_inventory_evidence
from tests.integration.test_capacity_protected_worker_session import (
    _import_terminal_inventory_payload,
    _seed_claimed_protected_trial,
    _terminal_inventory_evidence,
    _value,
)


def typed_payload(seeded):
    value = _terminal_inventory_evidence(seeded).model_dump(mode="json")
    record = value["record"]
    proof = record["ownership_proof"]
    metadata = proof["metadata"]
    for node in (value, record, proof, metadata):
        node["schema_version"] = 3
    binding = value["binding"]
    metadata["launch_profile_sha256"] = "a" * 64
    metadata["subject_authority"] = dict(
        schema_version=3, source="immutable-base", purpose="application-worker",
        configuration=dict(schema_version=1, scope="subject",
            subject_id=binding["subject_id"], subject_incarnation=binding["subject_incarnation"],
            generation=1, digest="b" * 64),
        acknowledgement_sha256="c" * 64, membership=None,
    )
    return parse_terminal_inventory_evidence(json.dumps(value)).model_dump(mode="json")


def test_typed_terminal_sql_import_preserves_exact_bytes_and_restart_idempotence(
    capacity_guard_database, monkeypatch, tmp_path,
):
    seeded = _seed_claimed_protected_trial(capacity_guard_database, monkeypatch, tmp_path)
    payload = typed_payload(seeded)
    first = asyncio.run(_import_terminal_inventory_payload(capacity_guard_database, seeded, payload))
    replay = asyncio.run(_import_terminal_inventory_payload(capacity_guard_database, seeded, payload))
    assert first == replay
    evidence = parse_terminal_inventory_evidence(json.dumps(payload))
    assert first["evidence_digest"] == canonical_executable_digest(evidence)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(text(
                "SELECT evidence_payload FROM loom_capacity_guard.executable_terminal_inventory_evidence"
            )).scalar_one() == payload
            assert connection.execute(text(
                "SELECT draining FROM loom_capacity_guard.executable_claim_state"
            )).scalar_one() is True
    finally:
        engine.dispose()


def test_typed_terminal_sql_rejects_purpose_and_provenance_substitution_without_writes(
    capacity_guard_database, monkeypatch, tmp_path,
):
    seeded = _seed_claimed_protected_trial(capacity_guard_database, monkeypatch, tmp_path)
    original = typed_payload(seeded)
    for tamper in ("build", "record-version", "proof-version", "metadata-version",
                   "extra-authority", "subject", "incarnation", "empty-profile", "member-absent"):
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
        else:
            authority["source"] = "personal-membership"
        with pytest.raises(DBAPIError):
            asyncio.run(_import_terminal_inventory_payload(capacity_guard_database, seeded, payload))
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(text(
                "SELECT count(*) FROM loom_capacity_guard.executable_terminal_inventory_evidence"
            )).scalar_one() == 0
            assert connection.execute(text(
                "SELECT draining FROM loom_capacity_guard.executable_claim_state"
            )).scalar_one() is False
    finally:
        engine.dispose()
