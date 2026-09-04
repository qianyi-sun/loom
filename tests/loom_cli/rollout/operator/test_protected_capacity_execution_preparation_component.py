from __future__ import annotations

import copy
import hashlib
import json
import os
import stat
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from loom_capacity_manager.executable_contracts import (
    ExecutionContextV2,
    ExecutionPreparationAbortV2,
    ExecutionPreparationV2,
    canonical_executable_digest,
)
from loom_capacity_manager.preparation_readiness import (
    PreparedExecutionReadinessV2,
    PreparedExecutorReadinessV1,
    canonical_prepared_readiness_digest,
)
from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from loom_cli.rollout.operator.protected_capacity_execution_preparation_component import (
    KubernetesProtectedCapacityExecutionPreparationComponent,
    PreparedControllerEvidence,
    PreparedControllerRequest,
    PreparedExecutorProfileStore,
)
from loom_cli.rollout.operator.protected_capacity_manager_client import (
    ProtectedCapacityManagerClientError,
    ProtectedExecutionPreparationAbortResult,
    ProtectedExecutionPreparationStatus,
)
from loom_cli.rollout.operator.protected_execution_preparation_journal import (
    ExecutionPreparationOperationJournal,
    ExecutionPreparationRecoveryState,
)
from tests.loom_cli.rollout.operator.test_protected_controller_prerequisite_component import (
    _evidence as _prerequisite_evidence,
)
from tests.loom_cli.rollout.operator.test_protected_controller_prerequisite_component import (
    _plan_and_artifact,
)

_UNITS = (
    "loom-capacity-pool-executor.service",
    "loom-capacity-pool-executor-prepared.service",
    "loom-capacity-pool-executor-prepared.timer",
    "loom-capacity-pool-executor-active.service",
    "loom-capacity-pool-executor-active.timer",
)


class _PrerequisiteTransport:
    def __init__(self, evidence) -> None:  # type: ignore[no-untyped-def]
        self.authority_sha256 = evidence.transport_authority_sha256
        self.evidence = evidence

    def observe(self, _request):  # type: ignore[no-untyped-def]
        return self.evidence

    def converge(self, _request):  # type: ignore[no-untyped-def]
        raise AssertionError("exact controller prerequisite was unexpectedly converged")


def _controller_evidence(request: PreparedControllerRequest, *, timer: bool, tick: bool):
    return PreparedControllerEvidence(
        schema_version=1,
        pool_id=request.pool_id,
        transport_authority_sha256=request.transport_authority_sha256,
        request_sha256=request.request_sha256,
        file_sha256={
            path: hashlib.sha256(payload).hexdigest() for path, payload in request.files.items()
        },
        unit_active_state={
            unit: (
                "active"
                if unit == "loom-capacity-pool-executor-prepared.timer" and timer
                else "inactive"
            )
            for unit in _UNITS
        },
        unit_file_state={
            unit: (
                "enabled"
                if unit == "loom-capacity-pool-executor-prepared.timer" and timer
                else "disabled"
                if unit.endswith(".timer")
                else "static"
            )
            for unit in _UNITS
        },
        successful_tick=tick,
        tick_evidence_sha256="e" * 64 if tick else None,
    )


class _PreparedTransport:
    def __init__(
        self,
        manager: _Manager,
        pool_id: str,
        *,
        fail_converge: bool = False,
        drift_before_enable: bool = False,
    ) -> None:
        self.manager = manager
        self.pool_id = pool_id
        self.fail_converge = fail_converge
        self.drift_before_enable = drift_before_enable
        self.evidence: PreparedControllerEvidence | None = None
        self.requests: list[tuple[str, PreparedControllerRequest]] = []
        self.observe_count = 0

    def observe(self, request: PreparedControllerRequest):  # type: ignore[no-untyped-def]
        self.requests.append(("observe", request))
        self.observe_count += 1
        if self.drift_before_enable and self.observe_count == 3 and self.evidence is not None:
            file_sha256 = dict(self.evidence.file_sha256)
            file_sha256[next(iter(file_sha256))] = "a" * 64
            return replace(self.evidence, file_sha256=file_sha256)
        return self.evidence

    def converge_files(self, request: PreparedControllerRequest):  # type: ignore[no-untyped-def]
        self.requests.append(("converge-files", request))
        if self.fail_converge:
            raise RuntimeError("prepared controller file convergence failed safely")
        self.evidence = _controller_evidence(request, timer=False, tick=False)
        return self.evidence

    def enable_timer(self, request: PreparedControllerRequest):  # type: ignore[no-untyped-def]
        self.requests.append(("enable-timer", request))
        self.evidence = _controller_evidence(request, timer=True, tick=False)
        return self.evidence

    def run_tick(self, request: PreparedControllerRequest):  # type: ignore[no-untyped-def]
        self.requests.append(("run-tick", request))
        self.evidence = _controller_evidence(request, timer=True, tick=True)
        self.manager.ready_pools.add(self.pool_id)
        return self.evidence

    def disable_timer(self, request: PreparedControllerRequest):  # type: ignore[no-untyped-def]
        self.requests.append(("disable-timer", request))
        self.evidence = _controller_evidence(request, timer=False, tick=False)
        self.manager.ready_pools.discard(self.pool_id)
        return self.evidence


class _Manager:
    def __init__(
        self,
        artifact,
        *,
        first_prepare_fails_without_mutation: bool = False,
        first_prepare_fails_after_mutation: bool = False,
        corrupt_final_readiness_digest: bool = False,
        expired_final_lease: bool = False,
        first_abort_fails_after_mutation: bool = False,
    ) -> None:  # type: ignore[no-untyped-def]
        self.artifact = artifact
        self.policy_sha256 = canonical_executable_digest(artifact.execution_policy)
        self.writer_epoch = 11
        self.execution: ExecutionContextV2 | None = None
        self.ready_pools: set[str] = set()
        self.first_prepare_fails_without_mutation = first_prepare_fails_without_mutation
        self.first_prepare_fails_after_mutation = first_prepare_fails_after_mutation
        self.corrupt_final_readiness_digest = corrupt_final_readiness_digest
        self.expired_final_lease = expired_final_lease
        self.first_abort_fails_after_mutation = first_abort_fails_after_mutation
        self.prepare_calls: list[tuple[ExecutionPreparationV2, UUID]] = []
        self.abort_calls: list[tuple[ExecutionPreparationAbortV2, UUID]] = []

    def get_status(self) -> dict[str, object]:
        return {
            "account_slots": {},
            "authority_incarnation": self.artifact.executor_profile_seed.authority_incarnation,
            "blocker_counts": {},
            "configuration_digest": self.artifact.source_configuration_sha256,
            "configuration_epoch": self.artifact.source_configuration_epoch,
            "executable_new_capacity_ceiling": 0,
            "execution_epoch": 0 if self.execution is None else self.execution.execution_epoch,
            "execution_manifest_sha256": (
                None if self.execution is None else self.execution.execution_manifest_sha256
            ),
            "execution_state": "shadow" if self.execution is None else "prepared",
            "increase_freeze": True,
            "latest_shadow_epoch": None,
            "latest_shadow_input_digest": None,
            "observer_principal_id": "execution-reader",
            "pool_slots": {},
            "report_freshness_counts": {},
            "schema_version": 1,
            "tier_slots": {},
            "writer_epoch": self.writer_epoch,
        }

    def prepare_execution(
        self,
        request: ExecutionPreparationV2,
        idempotency_key: UUID,
    ) -> ExecutionContextV2:
        self.prepare_calls.append((request, idempotency_key))
        if self.first_prepare_fails_without_mutation and len(self.prepare_calls) == 1:
            raise ProtectedCapacityManagerClientError("transport")
        context = ExecutionContextV2(
            authority_incarnation=request.authority_incarnation,
            writer_epoch=request.expected_writer_epoch,
            configuration_epoch=request.configuration_epoch,
            execution_epoch=1,
            execution_manifest_sha256=canonical_executable_digest(request),
            execution_state="prepared",
            executable_new_capacity_ceiling=0,
            executable_new_capacity_rate_per_minute=0,
            trusted_fleet_release_sha256=request.trusted_fleet_release_sha256,
        )
        if self.execution is not None and self.execution != context:
            raise AssertionError("manager replay request changed")
        self.execution = context
        if self.first_prepare_fails_after_mutation and len(self.prepare_calls) == 1:
            raise ProtectedCapacityManagerClientError("transport")
        return context

    def get_execution_preparation_status(self) -> ProtectedExecutionPreparationStatus:
        if self.execution is None:
            readiness = PreparedExecutionReadinessV2(
                ready=False,
                policy_mode="pinned",
                policy_sha256=self.policy_sha256,
                execution=None,
                expected_subject_count=0,
                acknowledged_subject_count=0,
                executors=(),
                blockers=("manager-shadow",),
            )
        elif self.ready_pools == {"gb10", "oldlab"}:
            now = datetime.now(UTC)
            expected = {item.pool_id: item for item in self.artifact.execution_policy.executors}
            executors = tuple(
                PreparedExecutorReadinessV1(
                    pool_id=pool_id,
                    expected_executor_id=expected[pool_id].executor_id,
                    expected_executor_incarnation=expected[pool_id].executor_incarnation,
                    expected_pool_generation=expected[pool_id].pool_generation,
                    registered=True,
                    registered_executor_id=expected[pool_id].executor_id,
                    registered_executor_incarnation=expected[pool_id].executor_incarnation,
                    registered_pool_generation=expected[pool_id].pool_generation,
                    current=True,
                    lease_expires_at=(
                        now - timedelta(seconds=1)
                        if self.expired_final_lease
                        else now + timedelta(minutes=2)
                    ),
                    lease_fresh=True,
                    last_heartbeat_at=now,
                    heartbeat_sequence=2,
                    journal_sequence=1,
                    journal_digest="d" * 64,
                    inventory_sequence=1,
                    inventory_digest="c" * 64,
                    inventory_observed_at=now - timedelta(seconds=1),
                    inventory_fresh=True,
                    post_inventory_heartbeat=True,
                    inventory_record_count=0,
                    foreign_record_count=0,
                    unknown_record_count=0,
                    ownership_missing_record_count=0,
                    quarantined_record_count=0,
                    blockers=(),
                )
                for pool_id in ("gb10", "oldlab")
            )
            readiness = PreparedExecutionReadinessV2(
                ready=True,
                policy_mode="pinned",
                policy_sha256=self.policy_sha256,
                execution=self.execution,
                expected_subject_count=len(self.artifact.execution_policy.subject_acknowledgements),
                acknowledged_subject_count=len(
                    self.artifact.execution_policy.subject_acknowledgements
                ),
                executors=executors,
                blockers=(),
            )
        else:
            readiness = PreparedExecutionReadinessV2(
                ready=False,
                policy_mode="pinned",
                policy_sha256=self.policy_sha256,
                execution=self.execution,
                expected_subject_count=len(self.artifact.execution_policy.subject_acknowledgements),
                acknowledged_subject_count=len(
                    self.artifact.execution_policy.subject_acknowledgements
                ),
                executors=(),
                blockers=("executor-registration-missing",),
            )
        readiness_sha256 = canonical_prepared_readiness_digest(readiness)
        if self.corrupt_final_readiness_digest and readiness.ready:
            readiness_sha256 = "a" * 64
        return ProtectedExecutionPreparationStatus(
            readiness=readiness,
            readiness_sha256=readiness_sha256,
        )

    def abort_execution_preparation(
        self,
        request: ExecutionPreparationAbortV2,
        idempotency_key: UUID,
    ) -> ProtectedExecutionPreparationAbortResult:
        self.abort_calls.append((request, idempotency_key))
        assert self.execution is not None
        assert request.execution_epoch == self.execution.execution_epoch
        assert request.execution_manifest_sha256 == self.execution.execution_manifest_sha256
        self.execution = None
        self.ready_pools.clear()
        self.writer_epoch += 1
        if self.first_abort_fails_after_mutation and len(self.abort_calls) == 1:
            raise ProtectedCapacityManagerClientError("transport")
        return ProtectedExecutionPreparationAbortResult(
            execution_epoch=request.execution_epoch,
            execution_manifest_sha256=request.execution_manifest_sha256,
            retired_at=datetime.now(UTC),
            replayed=False,
        )


def _component(
    tmp_path: Path,
    *,
    replay_prepare: bool = False,
    fail_oldlab: bool = False,
    drift_oldlab_before_enable: bool = False,
    corrupt_final_readiness_digest: bool = False,
    mutate_before_prepare_timeout: bool = False,
    expired_final_lease: bool = False,
    abort_response_lost: bool = False,
    dependency_failure_call: int | None = None,
):
    plan, artifact, executables, configurations, bindings = _plan_and_artifact(tmp_path)
    manager = _Manager(
        artifact,
        first_prepare_fails_without_mutation=replay_prepare,
        first_prepare_fails_after_mutation=mutate_before_prepare_timeout,
        corrupt_final_readiness_digest=corrupt_final_readiness_digest,
        expired_final_lease=expired_final_lease,
        first_abort_fails_after_mutation=abort_response_lost,
    )
    prerequisite_transports = {}
    prepared_transports = {}
    for pool_id in ("gb10", "oldlab"):
        prerequisite = _prerequisite_evidence(
            plan=plan,
            artifact=artifact,
            binding=bindings[pool_id],
            executable_sha256=executables,
            configuration_sha256=configurations,
            transport_authority_sha256="7" * 64,
        )
        prerequisite_transports[pool_id] = _PrerequisiteTransport(prerequisite)
        prepared_transports[pool_id] = _PreparedTransport(
            manager,
            pool_id,
            fail_converge=fail_oldlab and pool_id == "oldlab",
            drift_before_enable=drift_oldlab_before_enable and pool_id == "oldlab",
        )
    guard_calls: list[str] = []

    def dependency_guard(bound_plan, bound_artifact):  # type: ignore[no-untyped-def]
        assert bound_plan == plan
        assert bound_artifact == artifact
        guard_calls.append(bound_artifact.artifact_sha256)
        if dependency_failure_call == len(guard_calls):
            raise RuntimeError("protected execution dependency rotated")
        return "f" * 64

    @contextmanager
    def client_context():
        yield manager

    component = KubernetesProtectedCapacityExecutionPreparationComponent(
        state_root=(tmp_path / "prepared-state").resolve(),
        service_uid=os.geteuid(),
        client_context=client_context,
        prerequisite_reader=lambda _plan: artifact,
        dependency_guard=dependency_guard,
        controller_prerequisite_transports=prerequisite_transports,
        prepared_controller_transports=prepared_transports,
    )
    return component, manager, prepared_transports, plan, artifact, guard_calls


def test_component_replays_only_the_same_preparation_and_converges_both_pools(
    tmp_path: Path,
) -> None:
    component, manager, transports, plan, artifact, guard_calls = _component(
        tmp_path,
        replay_prepare=True,
    )

    assert component.classify(plan)[0] is ComponentState.READY
    component.apply(plan)

    assert len(manager.prepare_calls) == 2
    assert manager.prepare_calls[0] == manager.prepare_calls[1]
    request, idempotency_key = manager.prepare_calls[0]
    assert idempotency_key.int != 0
    assert request.requested_ceiling == artifact.execution_policy.executable_new_capacity_ceiling
    assert request.requested_rate_per_minute == (
        artifact.execution_policy.executable_new_capacity_rate_per_minute
    )
    assert request.executors == artifact.execution_policy.executors
    assert request.subject_acknowledgements == artifact.execution_policy.subject_acknowledgements
    assert request.legacy_writer_fences == artifact.execution_policy.legacy_writer_fences
    assert manager.execution is not None
    profile = artifact.executor_profile_seed.realize(manager.execution)
    store = PreparedExecutorProfileStore(
        (tmp_path / "prepared-state").resolve(),
        service_uid=os.geteuid(),
    )
    publication = store.observe(profile)
    assert publication is not None
    assert store.read(publication) == profile
    for pool_id, transport in transports.items():
        operations = [operation for operation, _request in transport.requests]
        assert "converge-files" in operations
        assert "enable-timer" in operations
        assert "run-tick" in operations
        final_request = transport.requests[-1][1]
        assert set(final_request.files) == {
            f"/etc/loom-capacity-executor/{pool_id}.json",
            f"/etc/loom-capacity-executor/{pool_id}-inventory-policy.json",
            "/etc/loom-capacity-executor/service.env",
        }
        other_pool = "oldlab" if pool_id == "gb10" else "gb10"
        assert other_pool.encode("ascii") not in b"".join(final_request.files.values())
    assert len(guard_calls) >= 3
    assert component.classify(plan)[0] is ComponentState.EXACT
    assert not manager.abort_calls
    journal = ExecutionPreparationOperationJournal(
        component.state_root,
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    assert (
        journal.recovery_state(
            plan,
            artifact_sha256=artifact.artifact_sha256,
        )
        is ExecutionPreparationRecoveryState.FORWARD_COMPLETE
    )


def test_prepare_timeout_after_manager_mutation_resumes_without_replay(
    tmp_path: Path,
) -> None:
    component, manager, _transports, plan, _artifact, _guard_calls = _component(
        tmp_path,
        mutate_before_prepare_timeout=True,
    )

    component.apply(plan)

    assert len(manager.prepare_calls) == 1
    assert manager.execution is not None
    assert not manager.abort_calls


def test_exact_prepared_reapply_does_not_mutate_manager_or_controllers(
    tmp_path: Path,
) -> None:
    component, manager, transports, plan, _artifact, _guard_calls = _component(tmp_path)
    component.apply(plan)
    manager_calls = len(manager.prepare_calls)
    controller_mutations = {
        pool_id: sum(operation != "observe" for operation, _request in transport.requests)
        for pool_id, transport in transports.items()
    }

    component.apply(plan)

    assert len(manager.prepare_calls) == manager_calls
    assert {
        pool_id: sum(operation != "observe" for operation, _request in transport.requests)
        for pool_id, transport in transports.items()
    } == controller_mutations
    assert not manager.abort_calls


def test_terminal_proven_controller_drift_compensates_without_reconvergence(
    tmp_path: Path,
) -> None:
    component, manager, transports, plan, _artifact, _guard_calls = _component(tmp_path)
    component.apply(plan)
    oldlab = transports["oldlab"]
    assert oldlab.evidence is not None
    file_sha256 = dict(oldlab.evidence.file_sha256)
    file_sha256[next(iter(file_sha256))] = "a" * 64
    oldlab.evidence = replace(oldlab.evidence, file_sha256=file_sha256)
    convergences = sum(operation == "converge-files" for operation, _request in oldlab.requests)

    with pytest.raises(RuntimeError, match="terminal state drifted"):
        component.apply(plan)

    assert (
        sum(operation == "converge-files" for operation, _request in oldlab.requests)
        == convergences
    )
    assert len(manager.abort_calls) == 1


def test_unrelated_prepared_epoch_fails_closed_and_is_never_aborted(tmp_path: Path) -> None:
    component, manager, _transports, plan, artifact, _guard_calls = _component(tmp_path)
    manager.execution = ExecutionContextV2(
        authority_incarnation=UUID(artifact.executor_profile_seed.authority_incarnation),
        writer_epoch=manager.writer_epoch,
        configuration_epoch=artifact.source_configuration_epoch,
        execution_epoch=9,
        execution_manifest_sha256="a" * 64,
        execution_state="prepared",
        executable_new_capacity_ceiling=0,
        executable_new_capacity_rate_per_minute=0,
        trusted_fleet_release_sha256=(artifact.execution_policy.trusted_fleet_release_sha256),
    )

    with pytest.raises(ValueError, match="manifest"):
        component.apply(plan)

    assert not manager.prepare_calls
    assert not manager.abort_calls
    assert manager.execution.execution_epoch == 9


def test_controller_request_and_evidence_are_canonical_pool_local_documents(
    tmp_path: Path,
) -> None:
    component, _manager, transports, plan, _artifact, _guard_calls = _component(tmp_path)
    component.apply(plan)
    request = transports["gb10"].requests[-1][1]
    evidence = _controller_evidence(request, timer=True, tick=True)

    assert PreparedControllerRequest.from_bytes(request.to_bytes()) == request
    assert PreparedControllerEvidence.from_bytes(evidence.to_bytes()) == evidence
    assert request.to_bytes().endswith(b"\n")
    assert evidence.to_bytes().endswith(b"\n")
    changed = json.loads(request.to_bytes())
    changed["unexpected"] = True
    with pytest.raises(ValueError, match="request"):
        PreparedControllerRequest.from_bytes(
            (json.dumps(changed, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        )


def test_prepared_failure_disables_both_timers_and_aborts_only_the_exact_epoch(
    tmp_path: Path,
) -> None:
    component, manager, transports, plan, artifact, _guard_calls = _component(
        tmp_path,
        fail_oldlab=True,
    )

    with pytest.raises(RuntimeError, match="failed safely"):
        component.apply(plan)

    assert manager.execution is None
    assert len(manager.abort_calls) == 1
    abort, abort_key = manager.abort_calls[0]
    assert abort_key.int != 0
    assert abort.execution_epoch == 1
    assert abort.execution_manifest_sha256 == canonical_executable_digest(
        manager.prepare_calls[0][0]
    )
    assert all(
        "disable-timer" in [operation for operation, _request in transport.requests]
        for transport in transports.values()
    )
    journal = ExecutionPreparationOperationJournal(
        component.state_root,
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    assert (
        journal.recovery_state(
            plan,
            artifact_sha256=artifact.artifact_sha256,
        )
        is ExecutionPreparationRecoveryState.COMPENSATED
    )


def test_lost_abort_response_closes_from_exact_authenticated_shadow_readback(
    tmp_path: Path,
) -> None:
    component, manager, _transports, plan, artifact, _guard_calls = _component(
        tmp_path,
        fail_oldlab=True,
        abort_response_lost=True,
    )

    with pytest.raises(RuntimeError, match="file convergence failed safely"):
        component.apply(plan)

    assert manager.execution is None
    assert len(manager.abort_calls) == 1
    journal = ExecutionPreparationOperationJournal(
        component.state_root,
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    assert (
        journal.recovery_state(
            plan,
            artifact_sha256=artifact.artifact_sha256,
        )
        is ExecutionPreparationRecoveryState.COMPENSATED
    )


def test_restart_closes_open_abort_intent_from_exact_shadow_without_new_mutation(
    tmp_path: Path,
) -> None:
    component, manager, transports, plan, artifact, _guard_calls = _component(
        tmp_path,
        fail_oldlab=True,
    )
    with pytest.raises(RuntimeError, match="file convergence failed safely"):
        component.apply(plan)
    journal = ExecutionPreparationOperationJournal(
        component.state_root,
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    (journal.root / "manager-abort.terminal.json").unlink()
    manager_mutations = (len(manager.prepare_calls), len(manager.abort_calls))
    controller_mutations = sum(
        operation != "observe"
        for transport in transports.values()
        for operation, _request in transport.requests
    )

    with pytest.raises(RuntimeError, match="compensated and requires fresh authority"):
        component.apply(plan)

    assert (len(manager.prepare_calls), len(manager.abort_calls)) == manager_mutations
    assert (
        sum(
            operation != "observe"
            for transport in transports.values()
            for operation, _request in transport.requests
        )
        == controller_mutations
    )
    assert (
        journal.recovery_state(
            plan,
            artifact_sha256=artifact.artifact_sha256,
        )
        is ExecutionPreparationRecoveryState.COMPENSATED
    )


def test_both_controllers_must_remain_exact_before_either_timer_is_enabled(
    tmp_path: Path,
) -> None:
    component, manager, transports, plan, _artifact, _guard_calls = _component(
        tmp_path,
        drift_oldlab_before_enable=True,
    )

    with pytest.raises(RuntimeError, match="controller files"):
        component.apply(plan)

    assert not any(
        operation == "enable-timer"
        for transport in transports.values()
        for operation, _request in transport.requests
    )
    assert len(manager.abort_calls) == 1


def test_readiness_digest_drift_stops_both_timers_and_aborts_exact_epoch(
    tmp_path: Path,
) -> None:
    component, manager, transports, plan, _artifact, _guard_calls = _component(
        tmp_path,
        corrupt_final_readiness_digest=True,
    )

    with pytest.raises(RuntimeError, match="readiness"):
        component.apply(plan)

    assert len(manager.abort_calls) == 1
    assert all(
        transport.evidence is not None
        and transport.evidence.unit_file_state["loom-capacity-pool-executor-prepared.timer"]
        == "disabled"
        for transport in transports.values()
    )


def test_expired_readiness_lease_stops_both_timers_and_aborts_exact_epoch(
    tmp_path: Path,
) -> None:
    component, manager, transports, plan, _artifact, _guard_calls = _component(
        tmp_path,
        expired_final_lease=True,
    )

    with pytest.raises(RuntimeError, match="readiness"):
        component.apply(plan)

    assert len(manager.abort_calls) == 1
    assert all(
        transport.evidence is not None
        and transport.evidence.unit_file_state["loom-capacity-pool-executor-prepared.timer"]
        == "disabled"
        for transport in transports.values()
    )


def test_dependency_rotation_before_timer_enablement_compensates_exact_epoch(
    tmp_path: Path,
) -> None:
    component, manager, transports, plan, _artifact, guard_calls = _component(
        tmp_path,
        dependency_failure_call=3,
    )

    with pytest.raises(RuntimeError, match="dependency rotated"):
        component.apply(plan)

    assert len(guard_calls) == 3
    assert len(manager.abort_calls) == 1
    assert not any(
        operation == "enable-timer"
        for transport in transports.values()
        for operation, _request in transport.requests
    )


def test_profile_store_rejects_replacement_and_symlink_tamper(tmp_path: Path) -> None:
    _component_value, manager, _transports, _plan, artifact, _guard_calls = _component(tmp_path)
    manager.prepare_execution(
        ExecutionPreparationV2(
            authority_incarnation=UUID(artifact.executor_profile_seed.authority_incarnation),
            expected_writer_epoch=manager.writer_epoch,
            configuration_epoch=artifact.source_configuration_epoch,
            fleet_generation=artifact.desired_fleet_generation,
            fleet_digest=artifact.desired_fleet_sha256,
            trusted_fleet_release_sha256=artifact.execution_policy.trusted_fleet_release_sha256,
            requested_ceiling=artifact.execution_policy.executable_new_capacity_ceiling,
            requested_rate_per_minute=(
                artifact.execution_policy.executable_new_capacity_rate_per_minute
            ),
            executors=artifact.execution_policy.executors,
            subject_acknowledgements=artifact.execution_policy.subject_acknowledgements,
            legacy_writer_fences=artifact.execution_policy.legacy_writer_fences,
            rollback_evidence_sha256=artifact.execution_policy.rollback_evidence_sha256,
        ),
        UUID(int=999),
    )
    assert manager.execution is not None
    profile = artifact.executor_profile_seed.realize(manager.execution)
    root = (tmp_path / "profile-store").resolve()
    store = PreparedExecutorProfileStore(root, service_uid=os.geteuid())
    publication = store.publish(profile)
    replacement = copy.deepcopy(json.loads(publication.path.read_text(encoding="ascii")))
    replacement["writer_epoch"] += 1
    publication.path.write_text(json.dumps(replacement) + "\n", encoding="ascii")

    with pytest.raises(RuntimeError, match="profile"):
        store.read(publication)

    outside = tmp_path / "outside-profile"
    outside.write_text("{}\n", encoding="ascii")
    outside.chmod(0o600)
    publication.path.unlink()
    publication.path.symlink_to(outside)
    with pytest.raises(RuntimeError, match="profile"):
        store.read(publication)


def test_profile_store_never_exposes_a_partial_final_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component, manager, _transports, plan, artifact, _guard_calls = _component(tmp_path)
    component.apply(plan)
    assert manager.execution is not None
    profile = artifact.executor_profile_seed.realize(manager.execution)
    root = (tmp_path / "atomic-profile-store").resolve()
    store = PreparedExecutorProfileStore(root, service_uid=os.geteuid())
    publication = store.publication_for(profile)
    original_write = os.write
    final_path_was_visible: list[bool] = []

    def observed_write(descriptor: int, payload: bytes) -> int:
        final_path_was_visible.append(publication.path.exists())
        return original_write(descriptor, payload)

    monkeypatch.setattr(
        "loom_cli.rollout.operator.protected_capacity_execution_preparation_component.os.write",
        observed_write,
    )

    store.publish(profile)

    assert final_path_was_visible and not any(final_path_was_visible)


def test_profile_store_rejects_symlinked_directory_without_mutating_target_mode(
    tmp_path: Path,
) -> None:
    component, manager, _transports, plan, artifact, _guard_calls = _component(tmp_path)
    component.apply(plan)
    assert manager.execution is not None
    profile = artifact.executor_profile_seed.realize(manager.execution)
    state_root = (tmp_path / "symlink-state").resolve()
    state_root.mkdir(mode=0o700)
    outside = tmp_path / "outside-directory"
    outside.mkdir(mode=0o755)
    (state_root / "protected-capacity").symlink_to(outside, target_is_directory=True)
    store = PreparedExecutorProfileStore(state_root, service_uid=os.geteuid())

    with pytest.raises(RuntimeError, match="directory"):
        store.publish(profile)

    assert stat.S_IMODE(outside.stat().st_mode) == 0o755
