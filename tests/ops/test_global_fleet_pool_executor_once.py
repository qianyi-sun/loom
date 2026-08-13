from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops.global_fleet_pool_executor_once import run_executor_once
from tests.unit.test_capacity_executor_config import executor_files

from loom_capacity_executor.config import PoolExecutorConfig
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorInventoryV2,
    ExecutionAuthorityV2,
)


@dataclass
class InventoryClient:
    inventory_sequence: int = 0

    async def executable_checkpoint(self) -> SimpleNamespace:
        return SimpleNamespace(
            journal_sequence=0,
            journal_digest="0" * 64,
            inventory_sequence=self.inventory_sequence,
            command_sequence=0,
        )

    async def ingest_executable_inventory(
        self, inventory: ExecutableExecutorInventoryV2
    ) -> SimpleNamespace:
        self.inventory_sequence = inventory.inventory_sequence
        return SimpleNamespace()


class MutatingBackendMustNotConstruct:
    def __init__(self) -> None:
        raise AssertionError("mutating Slurm backend must remain unconstructed at zero ceiling")

    async def tick(self) -> object:
        raise AssertionError("mutating Slurm backend must remain unconstructed at zero ceiling")


class DrainOnlyExecutor:
    def __init__(self) -> None:
        self.ticks = 0

    async def tick(self) -> object:
        self.ticks += 1
        return object()


@pytest.mark.asyncio
async def test_zero_ceiling_never_constructs_mutating_backend(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    result = await run_executor_once(
        config,
        client=InventoryClient(),
        backend_factory=MutatingBackendMustNotConstruct,
    )
    assert result.mode == "inventory-only"
    assert result.scheduler_mutations == 0


@pytest.mark.asyncio
async def test_validate_only_does_not_construct_mutating_backend(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    result = await run_executor_once(
        config,
        client=InventoryClient(),
        validate_only=True,
        backend_factory=MutatingBackendMustNotConstruct,
    )
    assert result.mode == "validate-only"
    assert result.scheduler_mutations == 0


@pytest.mark.asyncio
async def test_pool_argument_rejects_cross_loaded_configuration(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path, pool_id="gb10").config)
    with pytest.raises(Exception, match="pool binding"):
        await run_executor_once(config, pool_id="oldlab", client=InventoryClient())


@pytest.mark.asyncio
async def test_current_drain_only_authority_exposes_the_existing_executor_only(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    executor = DrainOnlyExecutor()
    authority = ExecutionAuthorityV2(
        **config.execution.model_dump(exclude={"execution_state"}),
        execution_state="drain-only",
    )
    result = await run_executor_once(
        config,
        client=InventoryClient(),
        authority=authority,
        backend_factory=lambda: executor,
    )
    assert result.mode == "drain-only"
    assert executor.ticks == 1


@pytest.mark.asyncio
async def test_positive_current_active_authority_constructs_existing_executor(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    executor = DrainOnlyExecutor()
    authority = ExecutionAuthorityV2(
        **config.execution.model_dump(
            exclude={
                "execution_state",
                "executable_new_capacity_ceiling",
                "executable_new_capacity_rate_per_minute",
            }
        ),
        execution_state="active",
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    result = await run_executor_once(
        config,
        client=InventoryClient(),
        authority=authority,
        backend_factory=lambda: executor,
    )
    assert result.mode == "scale-up"
    assert executor.ticks == 1
