"""Frozen protected-rollout proof across Kubernetes, manager, and both Slurm pools."""

from __future__ import annotations

from pathlib import Path
from typing import NoReturn, cast
from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.executable_contracts import ExecutionPreparationAbortV2
from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from loom_cli.rollout.operator.protected_capacity_execution_preparation_component import (
    KubernetesProtectedCapacityExecutionPreparationComponent,
)
from loom_cli.rollout.operator.protected_capacity_manager_client import (
    ProtectedCapacityManagerClient,
    ProtectedCapacityManagerClientError,
)
from loom_cli.rollout.operator.protected_execution_preparation_journal import (
    ExecutionPreparationOperationIntent,
)
from tests.support.protected_global_autoscaling_harness import (
    FrozenProtectedAutoscalingHarness,
)

pytestmark = pytest.mark.asyncio


class _SimulatedProcessCrashError(RuntimeError):
    pass


async def test_frozen_runtime_converges_absent_execution_path_to_prepared_zero_ceiling(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Catch bypassing any protected runtime layer or making prepared capacity executable."""

    harness = await FrozenProtectedAutoscalingHarness.create(
        tmp_path,
        capacity_postgres_url=capacity_postgres_url,
        capacity_session_factory=capacity_session_factory,
    )
    try:
        foreign_before = harness.foreign_job_snapshots()

        observations = harness.converge_frozen_execution_path()

        assert observations == {
            "oldlab-controller-prerequisite": ComponentState.EXACT,
            "gb10-controller-prerequisite": ComponentState.EXACT,
            "staging-capacity-execution-credentials": ComponentState.EXACT,
            "capacity-manager-runtime": ComponentState.EXACT,
            "capacity-manager-configuration": ComponentState.EXACT,
            "capacity-execution-preparation": ComponentState.EXACT,
        }
        status = harness.manager_status()
        assert status["execution_state"] == "prepared"
        assert status["execution_epoch"] == 1
        assert status["executable_new_capacity_ceiling"] == 0
        assert status["increase_freeze"] is True
        assert harness.pool_nodes == {
            "gb10": tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16))),
            "oldlab": tuple(f"trt-eai-oldlab-{index}" for index in range(3, 6)),
        }
        assert harness.prepared_timer_states() == {
            "gb10": ("enabled", True),
            "oldlab": ("enabled", True),
        }
        assert harness.active_executor_services() == {"gb10": (), "oldlab": ()}
        assert harness.manager_routes == {
            "192.168.50.103/32",
            "192.168.60.11/32",
        }
        assert harness.manager_certificate_has_router_ip
        assert harness.execution_credentials_are_separated
        assert harness.pool_credentials_are_separated
        assert harness.foreign_job_snapshots() == foreign_before

        mutations = harness.mutation_counts()
        replay = harness.converge_frozen_execution_path()

        assert set(replay.values()) == {ComponentState.EXACT}
        assert harness.mutation_counts() == mutations
        assert harness.foreign_job_snapshots() == foreign_before
    finally:
        await harness.aclose()


async def test_partial_controller_failure_aborts_exact_epoch_and_leaves_timers_inert(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Catch leaving a prepared manager or timer live after one pool fails."""

    harness = await FrozenProtectedAutoscalingHarness.create(
        tmp_path,
        capacity_postgres_url=capacity_postgres_url,
        capacity_session_factory=capacity_session_factory,
    )
    try:
        foreign_before = harness.foreign_job_snapshots()
        harness.fail_next_prepared_file_convergence("oldlab")

        with pytest.raises(RuntimeError, match="injected controller file failure"):
            harness.converge_frozen_execution_path()

        status = harness.manager_status()
        assert status["execution_state"] == "shadow"
        assert status["execution_epoch"] == 0
        assert status["executable_new_capacity_ceiling"] == 0
        assert status["increase_freeze"] is True
        assert harness.prepared_timer_states() == {
            "gb10": ("disabled", False),
            "oldlab": ("disabled", False),
        }
        assert harness.active_executor_services() == {"gb10": (), "oldlab": ()}
        assert harness.manager_mutation_paths().count("/v2/execution-preparations") == 1
        assert harness.manager_mutation_paths().count("/v2/execution-preparations/1/abort") == 1
        assert harness.foreign_job_snapshots() == foreign_before
    finally:
        await harness.aclose()


async def test_lost_abort_response_recovers_from_real_manager_shadow_readback(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catch treating a committed abort with a lost response as unresolved."""

    harness = await FrozenProtectedAutoscalingHarness.create(
        tmp_path,
        capacity_postgres_url=capacity_postgres_url,
        capacity_session_factory=capacity_session_factory,
    )
    try:
        foreign_before = harness.foreign_job_snapshots()
        assert set(harness.converge_frozen_prerequisites().values()) == {ComponentState.EXACT}
        harness.fail_next_prepared_file_convergence("oldlab")
        abort_execution_preparation = ProtectedCapacityManagerClient.abort_execution_preparation

        def lose_abort_response(
            client: ProtectedCapacityManagerClient,
            abort: ExecutionPreparationAbortV2,
            idempotency_key: UUID,
        ) -> NoReturn:
            abort_execution_preparation(client, abort, idempotency_key)
            raise ProtectedCapacityManagerClientError("transport")

        monkeypatch.setattr(
            ProtectedCapacityManagerClient,
            "abort_execution_preparation",
            lose_abort_response,
        )

        with pytest.raises(RuntimeError, match="injected controller file failure"):
            harness.apply_execution_preparation()

        status = harness.manager_status()
        assert status["execution_state"] == "shadow"
        assert status["execution_epoch"] == 0
        assert status["executable_new_capacity_ceiling"] == 0
        assert status["increase_freeze"] is True
        assert harness.prepared_timer_states() == {
            "gb10": ("disabled", False),
            "oldlab": ("disabled", False),
        }
        assert harness.manager_mutation_paths().count("/v2/execution-preparations/1/abort") == 1
        assert harness.foreign_job_snapshots() == foreign_before
    finally:
        await harness.aclose()


@pytest.mark.parametrize("drift", ("coexistence-witness", "legacy-high-water"))
async def test_stale_external_execution_authority_refuses_before_preparation_mutation(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
    drift: str,
) -> None:
    """Catch preparing capacity after the live legacy-writer fence has changed."""

    harness = await FrozenProtectedAutoscalingHarness.create(
        tmp_path,
        capacity_postgres_url=capacity_postgres_url,
        capacity_session_factory=capacity_session_factory,
    )
    try:
        foreign_before = harness.foreign_job_snapshots()
        if drift == "coexistence-witness":
            harness.stale_coexistence_witness("gb10")
        else:
            harness.stale_legacy_writer_high_water()

        with pytest.raises(RuntimeError, match="capacity-execution-preparation drifted"):
            harness.converge_frozen_execution_path()

        assert "/v2/execution-preparations" not in harness.manager_mutation_paths()
        assert harness.mutation_counts()["prepared-controllers"] == 0
        status = harness.manager_status()
        assert status["execution_state"] == "shadow"
        assert status["execution_epoch"] == 0
        assert status["executable_new_capacity_ceiling"] == 0
        assert harness.prepared_timer_states() == {
            "gb10": ("disabled", False),
            "oldlab": ("disabled", False),
        }
        assert harness.foreign_job_snapshots() == foreign_before
    finally:
        await harness.aclose()


@pytest.mark.parametrize("crossover", ("route", "certificate"))
async def test_manager_route_or_certificate_crossover_refuses_preparation(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
    crossover: str,
) -> None:
    """Catch preparing through a route or certificate outside the bound channel."""

    harness = await FrozenProtectedAutoscalingHarness.create(
        tmp_path,
        capacity_postgres_url=capacity_postgres_url,
        capacity_session_factory=capacity_session_factory,
    )
    try:
        foreign_before = harness.foreign_job_snapshots()
        assert set(harness.converge_frozen_prerequisites().values()) == {ComponentState.EXACT}
        if crossover == "route":
            harness.cross_manager_route()
        else:
            harness.cross_manager_certificate()

        with pytest.raises(RuntimeError, match="capacity-manager-runtime drifted"):
            harness.converge_frozen_execution_path()

        assert "/v2/execution-preparations" not in harness.manager_mutation_paths()
        status = harness.manager_status()
        assert status["execution_state"] == "shadow"
        assert status["execution_epoch"] == 0
        assert harness.prepared_timer_states() == {
            "gb10": ("disabled", False),
            "oldlab": ("disabled", False),
        }
        assert harness.foreign_job_snapshots() == foreign_before
    finally:
        await harness.aclose()


@pytest.mark.parametrize("credential", ("bearer-token", "ownership-private-key"))
async def test_cross_pool_credential_substitution_refuses_preparation(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
    credential: str,
) -> None:
    """Catch accepting GB10 execution authority at the OLDLAB boundary."""

    harness = await FrozenProtectedAutoscalingHarness.create(
        tmp_path,
        capacity_postgres_url=capacity_postgres_url,
        capacity_session_factory=capacity_session_factory,
    )
    try:
        foreign_before = harness.foreign_job_snapshots()
        assert set(harness.converge_frozen_prerequisites().values()) == {ComponentState.EXACT}
        harness.cross_pool_credential(
            source_pool="gb10",
            target_pool="oldlab",
            credential=credential,
        )

        with pytest.raises(
            RuntimeError,
            match="staging-capacity-execution-credentials drifted",
        ):
            harness.converge_frozen_execution_path()

        assert "/v2/execution-preparations" not in harness.manager_mutation_paths()
        assert harness.mutation_counts()["prepared-controllers"] == 0
        status = harness.manager_status()
        assert status["execution_state"] == "shadow"
        assert status["execution_epoch"] == 0
        assert harness.prepared_timer_states() == {
            "gb10": ("disabled", False),
            "oldlab": ("disabled", False),
        }
        assert harness.foreign_job_snapshots() == foreign_before
    finally:
        await harness.aclose()


@pytest.mark.parametrize(
    "operation",
    (
        "manager-preparation",
        "controller-files-gb10",
        "controller-files-oldlab",
        "prepared-timer-gb10",
        "prepared-timer-oldlab",
        "prepared-tick-gb10",
        "prepared-tick-oldlab",
    ),
)
async def test_preparation_restarts_after_each_mutation_before_terminal_publication(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    """Catch replaying a completed side effect after a crash left its intent open."""

    harness = await FrozenProtectedAutoscalingHarness.create(
        tmp_path,
        capacity_postgres_url=capacity_postgres_url,
        capacity_session_factory=capacity_session_factory,
    )
    try:
        foreign_before = harness.foreign_job_snapshots()
        assert set(harness.converge_frozen_prerequisites().values()) == {ComponentState.EXACT}
        component_type = KubernetesProtectedCapacityExecutionPreparationComponent
        record_terminal = component_type._record_operation_terminal

        def crash_before_terminal(*args: object, **kwargs: object) -> None:
            intent = cast(ExecutionPreparationOperationIntent, kwargs["intent"])
            if intent.operation == operation:
                raise _SimulatedProcessCrashError(operation)
            record_terminal(*args, **kwargs)  # type: ignore[arg-type]

        def crash_during_compensation(*_args: object, **_kwargs: object) -> None:
            raise _SimulatedProcessCrashError("compensation interrupted by process exit")

        with monkeypatch.context() as faults:
            faults.setattr(
                component_type,
                "_record_operation_terminal",
                staticmethod(crash_before_terminal),
            )
            faults.setattr(component_type, "_abort_exact", crash_during_compensation)
            for transport in harness.prepared_transports.values():
                faults.setattr(transport, "disable_timer", crash_during_compensation)
            with pytest.raises(RuntimeError):
                harness.apply_execution_preparation()

        assert harness.manager_status()["execution_state"] == "prepared"
        assert harness.manager_mutation_paths().count("/v2/execution-preparations") == 1
        assert not any(path.endswith("/abort") for path in harness.manager_mutation_paths())
        assert harness.foreign_job_snapshots() == foreign_before

        observations = harness.converge_frozen_execution_path()

        assert set(observations.values()) == {ComponentState.EXACT}
        assert harness.manager_mutation_paths().count("/v2/execution-preparations") == 1
        assert not any(path.endswith("/abort") for path in harness.manager_mutation_paths())
        assert harness.prepared_timer_states() == {
            "gb10": ("enabled", True),
            "oldlab": ("enabled", True),
        }
        assert harness.foreign_job_snapshots() == foreign_before
    finally:
        await harness.aclose()
