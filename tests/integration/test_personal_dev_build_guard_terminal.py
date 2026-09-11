"""Native terminal import retains exact evidence without releasing capacity."""

import json
from datetime import UTC, datetime
from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.executable_contracts import (
    ExecutionContextV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.ownership import sign_typed_executable_ownership
from loom_capacity_manager.typed_inventory_contracts import ExecutableTerminalInventoryEvidenceV3
from tests.integration.test_personal_dev_build_guard_execution import admitted, physical, store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions
from tests.unit.test_capacity_agent_typed_terminal import typed_terminal
from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context


def terminal_store(session, installation):
    return import_module("loom_capacity_build_guard.terminal_store").BuildGuardTerminalStore(
        session, installation=installation)


async def terminal_input(values, *, bind=True, metadata_changes=None):
    factory, _engine, installation, *_ = values
    registration, digest = await admitted(values)
    async with factory.begin() as session:
        await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    binding = registration.binding
    _, template = typed_terminal(purpose="personal-build-worker", pool=binding.pool_id)
    metadata = template.record.ownership_proof.metadata
    authority = metadata.subject_authority
    authority = authority.model_copy(update={
        "configuration": authority.configuration.model_copy(update={
            "subject_id": binding.subject_id, "subject_incarnation": binding.subject_incarnation}),
        "membership": authority.membership.model_copy(update={
            "owner_id": installation.document.owner_user_id,
            "execution_manifest_sha256": binding.execution.execution_manifest_sha256})})
    pool = next(item for item in installation.document.runtime.pools if item.pool_id == binding.pool_id)
    metadata = metadata.model_copy(update={"binding": binding, "subject_authority": authority,
        "launch_profile_sha256": pool.launch_profile_sha256,
        "controller_authority_sha256": pool.controller_authority_sha256,
        "trusted_launcher_sha256": binding.execution.trusted_fleet_release_sha256,
        **(metadata_changes or {})})
    key = typed_context(pool=binding.pool_id).ownership_key
    proof = sign_typed_executable_ownership(key.private_key, signing_key_id=key.signing_key_id, metadata=metadata)
    request = physical(registration).model_copy(update={"ownership_evidence_sha256": canonical_executable_digest(proof)})
    evidence = ExecutableTerminalInventoryEvidenceV3(binding=binding,
        inventory_execution=ExecutionContextV2.model_validate(binding.execution.model_dump(exclude={"allocation_epoch", "executable"})),
        inventory_sequence=1, inventory_digest="a"*64, journal_sequence=0, journal_digest="0"*64,
        record=template.record.model_copy(update={"ownership_proof": proof, "physical_identity": request.slurm_job_id,
            "resources": binding.resources, "node_ids": binding.node_ids}), observed_at=datetime.now(UTC))
    if bind:
        async with factory.begin() as session:
            await store(session, installation).bind_slurm_job(request)
    return evidence, request


async def test_terminal_import_replays_after_cancellation_without_release(prepared_input):
    factory, engine, installation, _plan, _source, request = prepared_input
    evidence, physical_request = await terminal_input(prepared_input)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
    async with factory.begin() as session:
        receipt = await terminal_store(session, installation).import_evidence(evidence)
    assert receipt.binding == evidence.binding
    assert receipt.installation_id == installation.id
    assert receipt.evidence_digest == canonical_executable_digest(evidence)
    assert receipt.physical_job_id == physical_request.slurm_job_id
    assert receipt.executable is False
    async with factory.begin() as session:
        assert await terminal_store(session, installation).import_evidence(evidence) == receipt
        assert (await store(session, installation).observe_intent(evidence.binding)).release is None
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.terminal_inventory")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0


async def test_terminal_import_requires_prior_physical_commit(prepared_input):
    factory, _engine, installation, *_ = prepared_input
    evidence, request = await terminal_input(prepared_input, bind=False)
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="physical"):
            await terminal_store(session, installation).import_evidence(evidence)
        await store(session, installation).bind_slurm_job(request)
        with pytest.raises(DBAPIError, match="committed"):
            await terminal_store(session, installation).import_evidence(evidence)


@pytest.mark.parametrize("boundary", ["job", "proof", "binding", "replay"])
async def test_terminal_import_rejects_identity_or_replay_substitution(prepared_input, boundary):
    factory, engine, installation, *_ = prepared_input
    evidence, _ = await terminal_input(prepared_input)
    if boundary == "replay":
        async with factory.begin() as session:
            await terminal_store(session, installation).import_evidence(evidence)
        changed = evidence.model_copy(update={"inventory_sequence": 2})
    elif boundary == "binding":
        changed = evidence.model_copy(update={"binding": evidence.binding.model_copy(update={"intent_id": uuid4()})})
    else:
        record = evidence.record
        changes = {"physical_identity": "9999"} if boundary == "job" else {
            "ownership_proof": record.ownership_proof.model_copy(update={"signing_key_id": "foreign"})}
        changed = evidence.model_copy(update={"record": record.model_copy(update=changes)})
    async with factory.begin() as session:
        with pytest.raises((DBAPIError, ValueError)):
            await terminal_store(session, installation).import_evidence(changed)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.terminal_inventory")) == (boundary == "replay")


@pytest.mark.parametrize("pin", ["launch_profile_sha256", "controller_authority_sha256"])
async def test_terminal_import_checks_installed_pins_even_when_proof_digest_matches(prepared_input, pin):
    factory, _engine, installation, *_ = prepared_input
    evidence, _ = await terminal_input(prepared_input, metadata_changes={pin: "f"*64})
    async with factory.begin() as session:
        with pytest.raises((DBAPIError, ValueError), match="installation"):
            await terminal_store(session, installation).import_evidence(evidence)


async def test_terminal_corrupt_receipt_rolls_back_inside_outer_commit(prepared_input, monkeypatch):
    factory, engine, installation, *_ = prepared_input
    evidence, _ = await terminal_input(prepared_input)
    async with factory.begin() as session:
        original = session.scalar

        async def corrupt(*args, **kwargs):
            payload = json.loads(await original(*args, **kwargs))
            payload["evidence_digest"] = "f"*64
            return json.dumps(payload, sort_keys=True, separators=(",", ":"))

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError, match="receipt"):
            await terminal_store(session, installation).import_evidence(evidence)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.terminal_inventory")) == 0


async def test_terminal_retention_is_private_immutable_and_blocks_downgrade(prepared_input, build_guard_database):
    from alembic import command

    factory, engine, installation, *_ = prepared_input
    evidence, _ = await terminal_input(prepared_input)
    async with factory.begin() as session:
        await terminal_store(session, installation).import_evidence(evidence)
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(text("SELECT * FROM loom_capacity_build_guard.terminal_inventory"))
    for statement in ("UPDATE loom_capacity_build_guard.terminal_inventory SET payload=payload",
        "DELETE FROM loom_capacity_build_guard.terminal_inventory", "TRUNCATE loom_capacity_build_guard.terminal_inventory"):
        with pytest.raises(DBAPIError):
            with engine.begin() as connection:
                connection.execute(text(statement))
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(build_guard_database[0], "build_guard_0012")


@pytest.mark.parametrize("boundary", ["schema", "string-schema", "record-string-schema", "extra", "state", "kind", "purpose", "resources", "execution", "sequence", "noncanonical", "journal", "nested"])
async def test_terminal_direct_sql_rejects_forged_evidence(prepared_input, boundary):
    from hashlib import sha256

    factory, _engine, installation, *_ = prepared_input
    evidence, _ = await terminal_input(prepared_input)
    payload = json.loads(canonical_executable_bytes(evidence))
    if boundary == "schema":
        payload["schema_version"] = 3.0
    elif boundary == "string-schema":
        payload["schema_version"] = "3"
    elif boundary == "record-string-schema":
        payload["record"]["schema_version"] = "3"
    elif boundary == "extra":
        payload["release_capacity"] = True
    elif boundary == "state":
        payload["record"]["state"] = "active"
    elif boundary == "kind":
        payload["record"]["physical_kind"] = "worker"
    elif boundary == "purpose":
        payload["record"]["ownership_proof"]["metadata"]["subject_authority"]["purpose"] = "application-worker"
    elif boundary == "resources":
        payload["record"]["resources"]["slots"] = 2
    elif boundary == "execution":
        payload["inventory_execution"]["writer_epoch"] += 1
    elif boundary == "sequence":
        payload["inventory_sequence"] = True
    elif boundary == "journal":
        payload["journal_digest"] = "f"*64
    elif boundary == "nested":
        payload["record"]["ownership_proof"]["metadata"]["subject_authority"]["membership"] = None
    wire = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    if boundary == "noncanonical":
        wire += b" "
    async with factory.begin() as session:
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.scalar(text("""SELECT loom_capacity_build_guard.import_terminal_inventory(
                    :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                    {"installation": installation.id, "payload": wire.decode("ascii"), "wire": wire, "digest": sha256(wire).hexdigest()})


async def test_terminal_import_survives_withdrawal_and_expired_source(prepared_input):
    from tests.integration.test_personal_dev_build_guard_withdrawal import withdrawal

    factory, engine, installation, _plan, source, _request = prepared_input
    evidence, request = await terminal_input(prepared_input)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"),
            {"id": source.build_attempt.id})
    async with factory.begin() as session:
        await store(session, installation).withdraw_unregistered_worker(withdrawal(request))
    async with factory.begin() as session:
        receipt = await terminal_store(session, installation).import_evidence(evidence)
    async with factory.begin() as session:
        assert await terminal_store(session, installation).import_evidence(evidence) == receipt
        observed = await store(session, installation).observe_intent(evidence.binding)
        assert observed.withdrawal is not None and observed.release is None
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_concurrent_terminal_import_retains_one_exact_receipt(prepared_input):
    import asyncio

    factory, engine, installation, *_ = prepared_input
    evidence, _ = await terminal_input(prepared_input)

    async def compete():
        try:
            async with factory.begin() as session:
                return await terminal_store(session, installation).import_evidence(evidence)
        except DBAPIError as exc:
            assert exc.orig.sqlstate == "40001"
            return None

    receipts = [item for item in await asyncio.gather(compete(), compete()) if item is not None]
    assert receipts and all(item == receipts[0] for item in receipts)
    async with factory.begin() as session:
        assert await terminal_store(session, installation).import_evidence(evidence) == receipts[0]
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.terminal_inventory")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
