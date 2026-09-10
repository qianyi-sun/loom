"""Typed terminal evidence follows real reservation and protected launch authority."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select

from loom_capacity_manager.executable_contracts import (
    ExecutableLaunchPermitV2,
    ExecutablePermitConsumptionV2,
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
from tests.capacity_execution_fixtures import CONTROLLER_DIGESTS, EXECUTOR_KEYS
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


async def test_typed_terminal_proof_survives_later_empty_inventory(capacity_session):
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
    assert await store.subject_terminal_inventory_evidence(capacity_session,
        subject_id=member.configuration.subject_id, subject_incarnation=member.configuration.subject_incarnation,
        reporter_incarnation=member.acknowledgement.reporter_incarnation, intent_id=binding.intent_id) == evidence
