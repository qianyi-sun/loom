"""Read-only source graph preserves all owners through repeated empty epochs."""

from importlib import import_module
from uuid import UUID

import pytest
from sqlalchemy import select

from loom_capacity_manager.contracts import ConfigurationActivationV1, ConfigurationGenerationRefV1
from loom_capacity_manager.executable_contracts import ExecutableExecutorHeartbeatV2
from loom_capacity_manager.models import CapacityConfigGeneration, CapacityExecutionEpoch
from loom_capacity_manager.retired_application_import import _reference
from loom_capacity_manager.retired_member_export import _origins_from_authenticated_history
from loom_capacity_manager.retired_member_origin_contracts import (
    RetiredMembershipSnapshotReferenceV1,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    ConfigurationConflictError,
    WriterFence,
)
from loom_capacity_manager.typed_membership_store import _load_base_configurations
from tests.capacity_build_membership_fixtures import seed_typed_sql_execution
from tests.capacity_execution_fixtures import PreparedExecutionFixture, execution_policy
from tests.integration.test_capacity_manager_execution_epoch import (
    _drain_request,
    _publish_final_safe_evidence,
    _retirement_request,
)
from tests.integration.test_capacity_successor_source_verification import successor, verify


async def seed_empty_successor(session, candidate, *, epoch):
    """Use ordinary configuration activation, then the existing SQL-only harness."""
    source = await session.get(CapacityExecutionEpoch, candidate.retired_source.execution_epoch)
    subjects = await _load_base_configurations(session, source)
    subjects.update({origin.configuration.subject_id: origin.configuration for origin in
        (*candidate.managed_application_origins, *candidate.managed_build_origins)})
    management = CapacityManagementStore(execution_policy=execution_policy())
    for subject in subjects.values():
        existing = await session.scalar(select(CapacityConfigGeneration.id).where(
            CapacityConfigGeneration.scope == "subject", CapacityConfigGeneration.subject_id == subject.subject_id,
            CapacityConfigGeneration.subject_incarnation == subject.subject_incarnation,
            CapacityConfigGeneration.scope_generation == subject.configuration_generation))
        if existing is None:
            await management.propose_subject_configuration(session, subject, actor="test-operator",
                idempotency_key=UUID(int=981000 + subject.subject_id.int % 10000 + epoch * 10000))
    configuration = await management.activate_configuration(session, ConfigurationActivationV1(
        expected_configuration_epoch=candidate.configuration_epoch - 1,
        fleet=ConfigurationGenerationRefV1(scope="fleet", generation=candidate.fleet_generation, digest=candidate.fleet_digest),
        subjects=tuple(_reference(subject) for subject in subjects.values())),
        actor="test-operator", idempotency_key=UUID(int=982000 + epoch))
    assert configuration.configuration_epoch == candidate.configuration_epoch
    candidate = candidate.model_copy(update={"executors": tuple(binding.model_copy(update={
        "executor_incarnation": UUID(int=983000 + epoch * 10 + index)}) for index, binding in enumerate(candidate.executors))})
    management = CapacityManagementStore(execution_policy=execution_policy().model_copy(update={"executors": candidate.executors}))
    writer = WriterFence(authority_incarnation=candidate.authority_incarnation, writer_epoch=candidate.expected_writer_epoch)
    fixture = PreparedExecutionFixture(store=management, writer=writer, request=candidate)
    active = await seed_typed_sql_execution(session, fixture, candidate, execution_epoch=epoch)
    from loom_capacity_manager.execution_store import CapacityExecutionStore
    store = CapacityExecutionStore()
    for binding in candidate.executors:
        await store.heartbeat_executor(session, ExecutableExecutorHeartbeatV2(execution=active,
            executor_id=binding.executor_id, executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id, pool_generation=binding.pool_generation, heartbeat_sequence=1,
            journal_sequence=0, journal_digest="0" * 64))
    drained = await management.begin_execution_drain(session, _drain_request(active),
        actor="test-retirement", idempotency_key=UUID(int=984000 + epoch * 10))
    checkpoints = await _publish_final_safe_evidence(session, drained, bindings=candidate.executors)
    await management.retire_execution_epoch(session, _retirement_request(drained, checkpoints),
        actor="test-retirement", idempotency_key=UUID(int=984001 + epoch * 10))
    return candidate, RetiredMembershipSnapshotReferenceV1(namespace_id=candidate.personal_membership.namespace_id,
        execution_epoch=epoch, execution_manifest_sha256=active.execution_manifest_sha256,
        revision=0, head_sha256="0" * 64)


async def load(session, source):
    module = import_module("loom_capacity_manager.retired_source_graph")
    return await module.load_retired_source_graph(session, source)


@pytest.mark.parametrize("empty", (False, True))
async def test_graph_authenticates_repeated_empty_rollovers_without_losing_owners(capacity_session, empty):
    candidate, initial = await successor(capacity_session, empty=empty, resized=not empty)
    for epoch in (43, 44, 45):
        candidate, source = await seed_empty_successor(capacity_session, candidate, epoch=epoch)
        history = await load(capacity_session, source)
        exported = _origins_from_authenticated_history(history)
        assert exported.source == source
        assert len(exported.applications) == len(initial.applications)
        assert len(exported.builds) == len(initial.builds)
        for origin, first in zip(exported.builds, initial.builds, strict=True):
            assert origin.inherited.original_origin == first.inherited.original_origin
            assert origin.inherited.anchor == first.inherited.anchor
            assert origin.readiness_state == "pending"
        candidate = candidate.model_copy(update={"configuration_epoch": candidate.configuration_epoch + 1,
            "retired_source": source, "managed_application_origins": exported.applications,
            "managed_build_origins": exported.builds})
        assert await verify(capacity_session, candidate, epoch=epoch + 1) == exported


async def test_graph_rejects_forged_reference_even_when_epoch_exists(capacity_session):
    candidate, _initial = await successor(capacity_session)
    candidate, source = await seed_empty_successor(capacity_session, candidate, epoch=43)
    with pytest.raises(ConfigurationConflictError):
        await load(capacity_session, source.model_copy(update={"execution_manifest_sha256": "f" * 64}))


@pytest.mark.parametrize("omission", ("application", "build", "all"))
async def test_graph_rejects_self_consistent_omission_in_an_intermediate_epoch(capacity_session, omission):
    from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
    candidate, _initial = await successor(capacity_session)
    apps = () if omission in {"application", "all"} else candidate.managed_application_origins
    builds = () if omission in {"build", "all"} else candidate.managed_build_origins
    kept = {origin.configuration.subject_id for origin in (*apps, *builds)}
    removed = set(candidate.personal_membership.managed_base_subject_ids) - kept
    candidate = ExecutionPreparationV4.model_validate(candidate.model_dump(mode="python") | {
        "managed_application_origins": apps, "managed_build_origins": builds,
        "personal_membership": candidate.personal_membership.model_copy(update={"managed_base_subject_ids": tuple(kept)}),
        "subject_acknowledgements": tuple(ack for ack in candidate.subject_acknowledgements if ack.subject_id not in removed)})
    _candidate, source = await seed_empty_successor(capacity_session, candidate, epoch=43)
    with pytest.raises(ConfigurationConflictError, match="complete retired source"):
        await load(capacity_session, source)


@pytest.mark.parametrize("bound", ("MAX_RETIRED_SOURCE_EPOCHS", "MAX_RETIRED_SOURCE_BYTES",
    "MAX_RETIRED_SOURCE_EVENTS", "MAX_RETIRED_SOURCE_EVENT_BYTES"))
async def test_graph_work_is_bounded_before_loading_unbounded_history(capacity_session, monkeypatch, bound):
    candidate, _initial = await successor(capacity_session)
    _candidate, source = await seed_empty_successor(capacity_session, candidate, epoch=43)
    module = import_module("loom_capacity_manager.retired_source_graph")
    monkeypatch.setattr(module, bound, 1)
    with pytest.raises(ConfigurationConflictError, match="work bound"):
        await load(capacity_session, source)


async def test_graph_historical_installations_do_not_require_current_reporter_rows(capacity_session):
    from sqlalchemy import text
    candidate, initial = await successor(capacity_session)
    candidate, source = await seed_empty_successor(capacity_session, candidate, epoch=43)
    subject = initial.builds[0].configuration
    await capacity_session.execute(text("UPDATE capacity_demand_reporters SET state='fenced', configuration_generation=configuration_generation+1 WHERE reporter_incarnation=:reporter"),
        {"reporter": subject.demand_reporter_incarnation})
    history = await load(capacity_session, source)
    assert _origins_from_authenticated_history(history).builds[0].configuration == subject
