#!/usr/bin/env python3
"""Run one inert, controller-local executable pool executor tick."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from loom_capacity_executor.config import ExecutorConfigError, PoolExecutorConfig
from loom_capacity_executor.journal import ExecutorJournal
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorInventoryV2,
    ExecutionAuthorityV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)


class _ExecutableTick(Protocol):
    async def tick(self) -> object: ...


@dataclass(frozen=True, slots=True)
class ExecutorOnceResult:
    mode: Literal["drain-only", "inventory-only", "scale-up", "validate-only"]
    scheduler_mutations: Literal[0] = 0


async def run_executor_once(
    config: PoolExecutorConfig,
    *,
    pool_id: str | None = None,
    client: Any,
    validate_only: bool = False,
    authority: ExecutionAuthorityV2 | None = None,
    backend_factory: Callable[[], _ExecutableTick] | None = None,
) -> ExecutorOnceResult:
    """Validate a zero-ceiling binding and durably publish its empty inventory.

    Prepared/shadow and validate-only states publish inventory without creating a
    scheduler. A current, exact active or drain-only authority is the sole path
    that may construct the existing executable-v2 runtime.
    """

    if not isinstance(config, PoolExecutorConfig):
        raise TypeError("executor config must be PoolExecutorConfig")
    config.assert_pool(pool_id or config.pool_id)
    if authority is not None:
        expected = config.execution.model_dump(
            exclude={
                "execution_state",
                "executable_new_capacity_ceiling",
                "executable_new_capacity_rate_per_minute",
            }
        )
        actual = authority.model_dump(
            exclude={
                "execution_state",
                "executable_new_capacity_ceiling",
                "executable_new_capacity_rate_per_minute",
                "executable",
            }
        )
        if actual != expected:
            raise ExecutorConfigError("current execution authority differs from local binding")
    current = authority or config.execution
    if validate_only or current.execution_state == "prepared":
        # Deliberately do not evaluate backend_factory: no mutating scheduler object is
        # available in shadow, prepared, or validate-only operation.
        return await _publish_inert_inventory(config, client, validate_only=validate_only)
    if not isinstance(authority, ExecutionAuthorityV2):
        raise ExecutorConfigError("mutating executor requires current execution authority")
    if backend_factory is None:
        raise ExecutorConfigError("current authority requires an executable runtime")
    executor = backend_factory()
    await executor.tick()
    return ExecutorOnceResult("scale-up" if authority.execution_state == "active" else "drain-only")


async def _publish_inert_inventory(
    config: PoolExecutorConfig,
    client: Any,
    *,
    validate_only: bool,
) -> ExecutorOnceResult:
    with ExecutorJournal(config.journal_file) as journal:
        checkpoint = await client.executable_checkpoint()
        expected_sequence = getattr(checkpoint, "inventory_sequence", None)
        if type(expected_sequence) is not int or expected_sequence < 0:
            raise ExecutorConfigError("manager checkpoint inventory high-water is invalid")
        inventory = ExecutableExecutorInventoryV2(
            execution=config.execution,
            executor_id=config.executor_id,
            executor_incarnation=config.executor_incarnation,
            pool_id=config.pool_id,
            pool_generation=config.pool_generation,
            inventory_sequence=expected_sequence + 1,
            journal_sequence=journal.head.sequence,
            journal_digest=journal.head.digest,
        )
        payload = canonical_executable_bytes(inventory)
        digest = canonical_executable_digest(inventory)
        journal.append(
            "inventory-publish-requested",
            digest,
            object_kind="inventory",
            object_id=str(config.executor_incarnation),
            payload=payload,
        )
        await client.ingest_executable_inventory(inventory)
        journal.append(
            "inventory-publish-confirmed",
            digest,
            object_kind="inventory",
            object_id=str(config.executor_incarnation),
            payload=payload,
        )
    return ExecutorOnceResult("validate-only" if validate_only else "inventory-only")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--pool", choices=("oldlab", "gb10"))
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    # Startup only validates file authority. Real manager transport construction is
    # intentionally unavailable until a reviewed active authority exists.
    config = PoolExecutorConfig.from_files(args.config)
    config.assert_pool(args.pool or config.pool_id)
    if not args.validate_only:
        raise ExecutorConfigError(
            "zero-ceiling daemon requires --validate-only outside a service harness"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
