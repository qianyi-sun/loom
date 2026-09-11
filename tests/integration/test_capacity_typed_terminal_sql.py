"""Typed terminal evidence follows real reservation and protected launch authority."""

import json
from copy import deepcopy
from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import select, text

from loom_capacity_manager.executable_contracts import (
    ExecutableIntentCloseV2,
    ExecutableLaunchPermitV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableProtectedReleaseV2,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.membership_launch_authority import resolve_allocation_launch_subject
from loom_capacity_manager.models import CapacityAllocationEpoch, CapacityExecutionEpoch
from loom_capacity_manager.ownership import OwnershipKeyring, sign_typed_executable_ownership
from loom_capacity_manager.typed_inventory_contracts import (
    ExecutableExecutorInventoryV3,
    ExecutableInventoryRecordV3,
)
from loom_capacity_manager.typed_ownership_contracts import ExecutableOwnershipMetadataV3
from tests.capacity_execution_fixtures import CONTROLLER_DIGESTS, EXECUTOR_KEYS, executor_binding
from tests.integration.test_capacity_manager_execution_store import (
    _admission_acknowledgement,
    _executor_state,
    _inventory_execution,
)
from tests.integration.test_capacity_typed_execution_guards import typed_admission_plan
from tests.integration.test_capacity_typed_membership_execution import typed_management


async def launched_application(session):
    store, executor, preparation, execution, member, plan = await typed_admission_plan(session)
    await store.acknowledge_admission_plan(session, _admission_acknowledgement(plan),
        actor="owner-agent", idempotency_key=UUID(int=123001))
    permit = await store.next_pool_work(session, executor)
    assert isinstance(permit, ExecutableLaunchPermitV2)
    await store.consume_launch_permit(session, ExecutablePermitConsumptionV2(
        permit_id=permit.permit_id, permit_digest=store.contract_digest(permit), binding=permit.binding, command_sequence=3))
    return preparation, execution, member, permit.binding


async def terminal_inventory(session):
    preparation, execution, member, binding = await launched_application(session)
    epoch = await session.get(CapacityExecutionEpoch, execution.execution_epoch)
    allocation = await session.scalar(select(CapacityAllocationEpoch).where(
        CapacityAllocationEpoch.allocation_epoch == binding.execution.allocation_epoch))
    subject = await resolve_allocation_launch_subject(session, epoch, allocation,
        subject_id=member.configuration.subject_id, require_current=False)
    key = EXECUTOR_KEYS[binding.pool_id]
    metadata = ExecutableOwnershipMetadataV3(binding=binding, subject_authority=subject.authority,
        launch_profile_sha256="a" * 64, controller_authority_sha256=CONTROLLER_DIGESTS[binding.pool_id],
        trusted_launcher_sha256=execution.trusted_fleet_release_sha256,
        slurm_cluster=f"{binding.pool_id}-controller", submitter_identity="loom", association="loom", submitted_at=datetime.now(UTC))
    proof = sign_typed_executable_ownership(key, signing_key_id=f"{binding.pool_id}-key", metadata=metadata)
    store = CapacityExecutionStore(ownership_keyring=OwnershipKeyring({f"{binding.pool_id}-key": key.public_key()}))
    state = await _executor_state(session, execution, pool_id=binding.pool_id)
    inventory = ExecutableExecutorInventoryV3(execution=_inventory_execution(binding),
        executor_id=binding.executor_id, executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id, pool_generation=binding.pool_generation,
        inventory_sequence=state.inventory_high_water + 1, journal_sequence=state.journal_high_water,
        journal_digest=state.journal_digest, journal_checkpoint_sequence=state.journal_high_water,
        journal_checkpoint_digest=state.journal_digest, records=(ExecutableInventoryRecordV3(
            physical_identity="job-typed-terminal", physical_kind="slurm-job", authority_scope="dedicated-loom-association",
            state="terminal", resources=binding.resources, node_ids=binding.node_ids,
            controller_evidence_sha256="9" * 64, ownership_proof=proof, terminal_evidence_sha256="8" * 64),))
    return store, preparation, member, binding, inventory


@pytest.mark.parametrize("supersede", (False, True))
async def test_typed_terminal_proof_survives_later_empty_inventory(capacity_session, supersede):
    store, preparation, member, binding, inventory = await terminal_inventory(capacity_session)
    management = typed_management(preparation)
    await store.ingest_typed_executor_inventory(capacity_session, inventory, management=management)
    evidence = await store.subject_terminal_inventory_evidence(capacity_session,
        subject_id=member.configuration.subject_id, subject_incarnation=member.configuration.subject_incarnation,
        reporter_incarnation=member.acknowledgement.reporter_incarnation, intent_id=binding.intent_id)
    assert evidence is not None and evidence.schema_version == 3
    assert evidence.record.ownership_proof.metadata.subject_authority.purpose == "application-worker"
    later = inventory.model_copy(update={"inventory_sequence": inventory.inventory_sequence + 1, "records": ()})
    await store.ingest_typed_executor_inventory(capacity_session, later, management=management)
    if supersede:
        from tests.capacity_build_membership_fixtures import application_request
        from tests.integration.test_capacity_mixed_membership_store import apply, transition

        execution = await management.execution_authority(capacity_session)
        original = application_request(preparation, execution, owner=member.owner_id.int, revision=member.revision - 1)
        await apply(capacity_session, transition(original, "update", revision=4), key=123004)
    assert await store.subject_terminal_inventory_evidence(capacity_session,
        subject_id=member.configuration.subject_id, subject_incarnation=member.configuration.subject_incarnation,
        reporter_incarnation=member.acknowledgement.reporter_incarnation, intent_id=binding.intent_id) == evidence


@pytest.mark.parametrize("cleanup_only", (False, True))
async def test_typed_terminal_release_matches_python_and_sql_before_recreation(capacity_session, cleanup_only):
    from tests.integration.test_capacity_membership_release import assert_sql_release_matches

    store, preparation, member, binding, inventory = await terminal_inventory(capacity_session)
    await store.ingest_typed_executor_inventory(capacity_session, inventory, management=typed_management(preparation))
    await store.begin_intent_close(capacity_session, ExecutableIntentCloseV2(binding=binding, command_sequence=4))
    await store.acknowledge_protected_release(capacity_session, ExecutableProtectedReleaseV2(
        binding=binding, reporter_incarnation=member.acknowledgement.reporter_incarnation,
        bootstrap_registration_epoch=1, protected_registration_epoch=2, bootstrap_revoked=True,
        protected_release_sha256="b" * 64), actor="owner-agent", idempotency_key=UUID(int=123002))
    release = await store.next_pool_work(capacity_session, executor_binding(binding.pool_id), cleanup_only=cleanup_only)
    assert isinstance(release, ExecutablePartialReleaseV2)
    await store.release_shapes(capacity_session, release)
    await assert_sql_release_matches(capacity_session, member.configuration)


@pytest.mark.parametrize("tamper", ("purpose", "owner", "member-head", "configuration", "ack", "version", "decimal-version", "decimal-revision"))
async def test_sql_terminal_subject_authentication_rejects_provenance_substitution(capacity_session, tamper):
    _store, _preparation, _member, binding, inventory = await terminal_inventory(capacity_session)
    proof = inventory.records[0].ownership_proof.model_dump(mode="json")
    query = text("SELECT public.capacity_typed_terminal_subject_matches(CAST(:binding AS jsonb),CAST(:proof AS jsonb))")
    parameters = {"binding": binding.model_dump_json(), "proof": json.dumps(proof)}
    assert await capacity_session.scalar(query, parameters) is True
    changed = deepcopy(proof)
    authority = changed["metadata"]["subject_authority"]
    if tamper == "purpose":
        authority["purpose"] = "personal-build-worker"
    elif tamper == "owner":
        authority["membership"]["owner_id"] = str(UUID(int=123003))
    elif tamper == "member-head":
        authority["membership"]["head_sha256"] = "f" * 64
    elif tamper == "configuration":
        authority["configuration"]["digest"] = "f" * 64
    elif tamper == "ack":
        authority["acknowledgement_sha256"] = "f" * 64
    elif tamper == "decimal-version":
        changed["metadata"]["schema_version"] = 3.0
    elif tamper == "decimal-revision":
        authority["membership"]["revision"] = float(authority["membership"]["revision"])
    else:
        changed["metadata"]["schema_version"] = 2
    assert await capacity_session.scalar(query, parameters | {"proof": json.dumps(changed)}) is False


@pytest.mark.parametrize("retained", (False, True))
async def test_typed_terminal_downgrade_preserves_evidence_and_fences_late_writes(capacity_session, retained):
    from alembic import command

    from tests.integration.test_capacity_build_membership_sql import _config

    if retained:
        store, preparation, _member, _binding, inventory = await terminal_inventory(capacity_session)
        await store.ingest_typed_executor_inventory(capacity_session, inventory, management=typed_management(preparation))
        with pytest.raises(RuntimeError, match="retained typed terminal evidence"):
            async with capacity_session.begin_nested():
                connection = await capacity_session.connection()
                await connection.run_sync(lambda sync: command.downgrade(_config(sync), "capacity_0020"))
        assert await capacity_session.scalar(text("SELECT version_num FROM alembic_version")) == "capacity_0023"
    else:
        connection = await capacity_session.connection()
        await connection.run_sync(lambda sync: command.downgrade(_config(sync), "capacity_0020"))
        assert await capacity_session.scalar(text("SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='capacity_terminal_legacy_evidence_check')"))
        await connection.run_sync(lambda sync: command.upgrade(_config(sync), "capacity_0021"))
    rows = (await capacity_session.execute(text("""
        SELECT proconfig, EXISTS (SELECT 1 FROM aclexplode(coalesce(proacl,acldefault('f',proowner)))
          WHERE grantee=0 AND privilege_type='EXECUTE') AS public_execute
        FROM pg_proc WHERE pronamespace='public'::regnamespace
          AND (proname='capacity_typed_terminal_subject_matches' OR proname LIKE 'capacity_0021_prior_%')
    """))).all()
    assert len(rows) == 3 and all("search_path=pg_catalog" in row.proconfig and not row.public_execute for row in rows)
