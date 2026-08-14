#!/usr/bin/env python3
"""Run one inert, controller-local executable pool executor tick."""

from __future__ import annotations

import argparse
import asyncio
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import httpx

from loom_capacity_agent.client import (
    DemandReporterTLSFiles,
    build_reporter_tls_context,
    read_owner_only_bearer_token,
)
from loom_capacity_executor.client import ExecutableCapacityExecutorClient
from loom_capacity_executor.config import ExecutorConfigError, PoolExecutorConfig
from loom_capacity_executor.executable import ExecutablePoolExecutor
from loom_capacity_executor.journal import ExecutorJournal
from loom_capacity_executor.slurm_backend import AsyncSlurmBackend
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorInventoryV2,
    ExecutionAuthorityV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)


@dataclass(frozen=True, slots=True)
class ExecutorOnceResult:
    mode: Literal["drain-only", "inventory-only", "scale-up", "validate-only"]


def build_executable_client(config: PoolExecutorConfig) -> ExecutableCapacityExecutorClient:
    """Construct the exact executable-v2 manager transport from local credentials."""

    tls = build_reporter_tls_context(
        DemandReporterTLSFiles(
            ca_file=config.tls_ca_file,
            certificate_file=config.tls_certificate_file,
            private_key_file=config.tls_private_key_file,
        )
    )
    return ExecutableCapacityExecutorClient(
        config.registration,
        manager_origin=config.manager_origin,
        bearer_token=read_owner_only_bearer_token(config.bearer_token_file),
        http_client=httpx.AsyncClient(
            verify=tls,
            timeout=httpx.Timeout(10.0),
            follow_redirects=False,
            trust_env=False,
        ),
        owns_http_client=True,
    )


async def run_daemon_once(
    config: PoolExecutorConfig,
    *,
    pool_id: str | None = None,
    validate_only: bool = False,
) -> ExecutorOnceResult:
    """Run the production inert daemon path and close the mTLS client reliably."""

    async with build_executable_client(config) as client:
        return await run_executor_once(
            config,
            pool_id=pool_id,
            client=client,
            validate_only=validate_only,
        )


async def run_executor_once(
    config: PoolExecutorConfig,
    *,
    pool_id: str | None = None,
    client: Any,
    validate_only: bool = False,
    authority: ExecutionAuthorityV2 | None = None,
    executor: ExecutablePoolExecutor | None = None,
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
    if not isinstance(executor, ExecutablePoolExecutor):
        raise ExecutorConfigError("current authority requires an executable runtime")
    _assert_executable_runtime(config, authority, executor)
    if authority.execution_state == "drain-only":
        # Task 8 has one `tick()` that can consume all manager work, including
        # reservation/permit scale-up. It has no drain-only command selector;
        # invoking it here would accidentally expose capacity increase.
        raise ExecutorConfigError("executable runtime has no drain-only command boundary")
    await executor.tick()
    return ExecutorOnceResult("scale-up")


async def _publish_inert_inventory(
    config: PoolExecutorConfig,
    client: Any,
    *,
    validate_only: bool,
) -> ExecutorOnceResult:
    with ExecutorJournal(config.journal_file) as journal:
        checkpoint = await client.executable_checkpoint()
        journal_sequence = getattr(checkpoint, "journal_sequence", None)
        journal_digest = getattr(checkpoint, "journal_digest", None)
        expected_sequence = getattr(checkpoint, "inventory_sequence", None)
        if (
            type(expected_sequence) is not int
            or expected_sequence < 0
            or type(journal_sequence) is not int
            or not isinstance(journal_digest, str)
        ):
            raise ExecutorConfigError("manager checkpoint inventory high-water is invalid")
        journal.assert_covers(journal_sequence, journal_digest)
        object_id = str(config.executor_incarnation)
        latest = journal.latest("inventory", object_id)
        if latest is not None and latest.event_kind == "inventory-publish-requested":
            payload = latest.durable_payload()
            if payload is None:
                raise ExecutorConfigError("inventory replay payload is absent")
            inventory = ExecutableExecutorInventoryV2.model_validate_json(payload)
            _assert_inventory_binding(config, inventory)
            if expected_sequence not in {
                inventory.inventory_sequence - 1,
                inventory.inventory_sequence,
            }:
                raise ExecutorConfigError("manager inventory high-water changed during replay")
            await client.ingest_executable_inventory(inventory)
            journal.append(
                "inventory-publish-confirmed",
                canonical_executable_digest(inventory),
                object_kind="inventory",
                object_id=object_id,
                payload=payload,
            )
            return ExecutorOnceResult("validate-only" if validate_only else "inventory-only")
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
            object_id=object_id,
            payload=payload,
        )
        await client.ingest_executable_inventory(inventory)
        journal.append(
            "inventory-publish-confirmed",
            digest,
            object_kind="inventory",
            object_id=object_id,
            payload=payload,
        )
    return ExecutorOnceResult("validate-only" if validate_only else "inventory-only")


def _assert_inventory_binding(
    config: PoolExecutorConfig,
    inventory: ExecutableExecutorInventoryV2,
) -> None:
    if (
        inventory.execution != config.execution
        or inventory.executor_id != config.executor_id
        or inventory.executor_incarnation != config.executor_incarnation
        or inventory.pool_id != config.pool_id
        or inventory.pool_generation != config.pool_generation
    ):
        raise ExecutorConfigError("inventory replay binding differs from local authority")


def _assert_executable_runtime(
    config: PoolExecutorConfig,
    authority: ExecutionAuthorityV2,
    executor: ExecutablePoolExecutor,
) -> None:
    expected_registration = config.registration.model_dump(exclude={"execution"})
    actual_registration = executor.registration.model_dump(exclude={"execution"})
    backend = executor.slurm
    if not isinstance(backend, AsyncSlurmBackend):
        raise ExecutorConfigError("executable runtime requires typed Slurm backend")
    slurm = backend.authority
    executable_paths = tuple(
        sorted(
            (name, Path(getattr(slurm.executables, name).path))
            for name in ("scontrol", "sacctmgr", "squeue", "sbatch", "scancel", "sacct")
        )
    )
    if (
        actual_registration != expected_registration
        or executor.registration.execution.model_dump()
        != authority.model_dump(exclude={"executable"})
        or executor.journal.path != config.journal_file
        or executor.profile.pool_id != config.pool_id
        or executor.profile.pool_generation != config.pool_generation
        or executor.profile.profile_id != config.profile_id
        or executor.profile.profile_generation != config.profile_generation
        or executor.profile.profile_digest != config.profile_digest
        or executor.profile.slurm_cluster != config.slurm_cluster
        or executor.profile.controller_host != config.controller_host
        or executor.profile.partition != config.partition
        or executor.profile.association != config.association
        or executor.profile.submitter != config.submitter
        or executor.profile.qos != config.qos
        or executor.controller_authority != config.controller_authority
        or executor.ownership_key != config.ownership_key
        or getattr(executor.client, "registration", None) != executor.registration
        or slurm.cluster != config.manifest.slurm_cluster
        or slurm.controller_host != config.manifest.controller_host
        or slurm.partition != config.manifest.partition
        or slurm.account != config.manifest.association
        or slurm.submitter != config.manifest.submitter
        or slurm.qos != config.manifest.qos
        or slurm.local_uid != config.manifest.local_uid
        or executable_paths != config.manifest.slurm_executables
    ):
        raise ExecutorConfigError("executable runtime differs from exact controller-local binding")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--expected-manifest-sha256", required=True)
    parser.add_argument("--pool", choices=("oldlab", "gb10"))
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    config = PoolExecutorConfig.from_files(
        args.config, expected_manifest_sha256=args.expected_manifest_sha256
    )
    asyncio.run(_run_with_signals(config, pool_id=args.pool, validate_only=args.validate_only))
    return 0


async def _run_with_signals(
    config: PoolExecutorConfig,
    *,
    pool_id: str | None,
    validate_only: bool,
) -> ExecutorOnceResult:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def request_stop() -> None:
        stop.set()

    installed: list[signal.Signals] = []
    for value in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(value, request_stop)
            installed.append(value)
        except (NotImplementedError, RuntimeError):
            # Windows and non-main-thread tests cannot install asyncio handlers;
            # regular cancellation still leaves the requested journal record durable.
            pass
    task = asyncio.create_task(
        run_daemon_once(config, pool_id=pool_id, validate_only=validate_only)
    )
    stop_task = asyncio.create_task(stop.wait())
    try:
        complete, _ = await asyncio.wait((task, stop_task), return_when=asyncio.FIRST_COMPLETED)
        if stop_task in complete and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise asyncio.CancelledError("executor daemon interrupted")
        return await task
    finally:
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        for value in installed:
            loop.remove_signal_handler(value)


if __name__ == "__main__":
    raise SystemExit(main())
