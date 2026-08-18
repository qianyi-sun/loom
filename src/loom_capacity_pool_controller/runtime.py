#!/usr/bin/env python3
"""Run one explicit controller-local pool executor mode."""

from __future__ import annotations

import argparse
import asyncio
import signal
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid5

import httpx

from loom_capacity_agent.client import (
    DemandReporterTLSFiles,
    build_reporter_tls_context,
    read_owner_only_bearer_token,
)
from loom_capacity_executor.client import ExecutableCapacityExecutorClient
from loom_capacity_executor.config import ExecutorConfigError, PoolExecutorConfig
from loom_capacity_executor.executable import ExecutablePoolExecutor
from loom_capacity_executor.heartbeat import ExecutableHeartbeatLoop
from loom_capacity_executor.journal import ExecutorJournal, JournalRecord
from loom_capacity_executor.runtime import (
    build_executable_runtime,
    load_activation_runtime_artifact,
    retained_drain_execution_matches,
)
from loom_capacity_executor.slurm_backend import AsyncSlurmBackend
from loom_capacity_executor.slurm_contracts import SlurmAuthorityV2
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorInventoryV2,
    ExecutionAuthorityV2,
    ExecutionContextV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_pool_executor.config import load_slurm_inventory_policy
from loom_capacity_pool_executor.slurm_inventory import (
    ReadOnlySlurmCommandRunner,
    SlurmInventoryPolicy,
    SlurmReportBinding,
    SubprocessReadOnlySlurmCommandRunner,
    capture_slurm_capacity_reports,
)

_PREPARED_REGISTRATION_NAMESPACE = UUID("0dbdb949-f40e-5ae4-92ac-ee986992a3a2")


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
    prepared_only: bool = False,
    inventory_policy: SlurmInventoryPolicy | None = None,
    activation_runtime_artifact: Path | None = None,
) -> ExecutorOnceResult:
    """Run one explicit executor mode and close the mTLS client reliably."""

    if not isinstance(config, PoolExecutorConfig):
        raise TypeError("executor config must be PoolExecutorConfig")
    if pool_id is not None and not isinstance(pool_id, str):
        raise TypeError("pool argument must be a string or none")
    if type(validate_only) is not bool or type(prepared_only) is not bool:
        raise TypeError("executor mode arguments must be booleans")
    if inventory_policy is not None and not isinstance(inventory_policy, SlurmInventoryPolicy):
        raise TypeError("inventory policy argument must be SlurmInventoryPolicy")
    if activation_runtime_artifact is not None and not isinstance(
        activation_runtime_artifact, Path
    ):
        raise TypeError("activation runtime artifact argument must be Path")
    config.assert_pool(config.pool_id if pool_id is None else pool_id)
    if validate_only and prepared_only:
        raise ExecutorConfigError("validate-only and prepared-only modes are mutually exclusive")
    if prepared_only:
        if inventory_policy is None:
            raise ExecutorConfigError("prepared-only mode requires an inventory policy")
        if activation_runtime_artifact is not None:
            raise ExecutorConfigError("prepared-only mode refuses an activation runtime artifact")
        config.assert_inventory_policy_binding(
            pool_id=inventory_policy.pool_id,
            pool_generation=inventory_policy.pool_generation,
            query_uid=inventory_policy.query_uid,
            controller_cluster=inventory_policy.controller_cluster,
            relevant_partitions=inventory_policy.relevant_partitions,
        )
    elif validate_only:
        if inventory_policy is not None or activation_runtime_artifact is not None:
            raise ExecutorConfigError("validate-only mode refuses runtime authority artifacts")
    else:
        if inventory_policy is not None:
            raise ExecutorConfigError("executable mode refuses a prepared inventory policy")
        if activation_runtime_artifact is None:
            raise ExecutorConfigError("executable mode requires an activation runtime artifact")

    async with build_executable_client(config) as client:
        if validate_only:
            return await run_executor_once(
                config,
                pool_id=pool_id,
                client=client,
                validate_only=True,
            )
        if prepared_only:
            assert inventory_policy is not None
            return await run_prepared_inventory_once(
                config,
                inventory_policy,
                client=client,
            )
        current_context = ExecutionContextV2.model_validate(
            await client.current_execution_context()
        )
        if current_context.execution_state == "prepared":
            raise ExecutorConfigError("prepared authority requires prepared-only inventory runtime")
        assert activation_runtime_artifact is not None
        artifact = load_activation_runtime_artifact(activation_runtime_artifact)
        executor = build_executable_runtime(
            config,
            artifact,
            manager_client=client,
            current_context=current_context,
        )
        try:
            return await run_executor_once(
                config,
                pool_id=pool_id,
                client=client,
                authority=ExecutionAuthorityV2.model_validate(
                    current_context.model_dump(mode="python")
                ),
                executor=executor,
            )
        finally:
            close = getattr(getattr(executor, "journal", None), "close", None)
            if callable(close):
                close()


async def run_executor_once(
    config: PoolExecutorConfig,
    *,
    pool_id: str | None = None,
    client: Any,
    validate_only: bool = False,
    current_context: ExecutionContextV2 | None = None,
    authority: ExecutionAuthorityV2 | None = None,
    executor: ExecutablePoolExecutor | None = None,
) -> ExecutorOnceResult:
    """Validate inert artifacts or execute one exact active/drain authority tick.

    Validate-only publishes a synthetic no-query inventory without creating a
    scheduler. Prepared physical inventory uses ``run_prepared_inventory_once``.
    A current, exact active or drain-only authority is the sole path that accepts
    the existing executable-v2 runtime.
    """

    if not isinstance(config, PoolExecutorConfig):
        raise TypeError("executor config must be PoolExecutorConfig")
    config.assert_pool(config.pool_id if pool_id is None else pool_id)
    if current_context is not None and authority is not None:
        raise ExecutorConfigError("current context and execution authority are mutually exclusive")
    if authority is not None:
        current_context = authority
    if current_context is not None:
        expected = config.execution.model_dump(
            exclude={
                "execution_state",
                "executable_new_capacity_ceiling",
                "executable_new_capacity_rate_per_minute",
            }
        )
        actual = current_context.model_dump(
            exclude={
                "execution_state",
                "executable_new_capacity_ceiling",
                "executable_new_capacity_rate_per_minute",
                "executable",
            }
        )
        if actual != expected and not retained_drain_execution_matches(
            config.execution,
            current_context,
        ):
            raise ExecutorConfigError("current execution authority differs from local binding")
    current = current_context or config.execution
    if validate_only:
        # Deliberately do not evaluate backend_factory: no mutating scheduler object is
        # available in validate-only operation.
        return await _publish_inert_inventory(config, client, validate_only=validate_only)
    if current.execution_state == "prepared":
        raise ExecutorConfigError("prepared authority requires prepared-only inventory runtime")
    authority = ExecutionAuthorityV2.model_validate(current.model_dump(mode="python"))
    if not isinstance(executor, ExecutablePoolExecutor):
        raise ExecutorConfigError("current authority requires an executable runtime")
    _assert_executable_runtime(config, authority, executor)
    await executor.slurm.validate_authority()
    heartbeats = ExecutableHeartbeatLoop(executor.registration, executor.journal, executor.client)
    await heartbeats.heartbeat()
    if authority.execution_state == "drain-only":
        result = await executor.tick_drain_only()
        mode: Literal["drain-only", "scale-up"] = "drain-only"
    else:
        result = await executor.tick()
        mode = "scale-up"
    if result.status == "inventory-published":
        await heartbeats.heartbeat()
    return ExecutorOnceResult(mode)


async def run_prepared_inventory_once(
    config: PoolExecutorConfig,
    policy: SlurmInventoryPolicy,
    *,
    client: Any,
    runner_factory: Callable[..., ReadOnlySlurmCommandRunner] = (
        SubprocessReadOnlySlurmCommandRunner
    ),
) -> ExecutorOnceResult:
    """Register and publish one journaled read-only prepared inventory."""

    if not isinstance(config, PoolExecutorConfig):
        raise TypeError("executor config must be PoolExecutorConfig")
    if not isinstance(policy, SlurmInventoryPolicy):
        raise TypeError("prepared inventory policy must be SlurmInventoryPolicy")
    config.assert_inventory_policy_binding(
        pool_id=policy.pool_id,
        pool_generation=policy.pool_generation,
        query_uid=policy.query_uid,
        controller_cluster=policy.controller_cluster,
        relevant_partitions=policy.relevant_partitions,
    )
    runner = runner_factory(policy=policy)
    try:
        current = ExecutionContextV2.model_validate(await client.current_execution_context())
    except (TypeError, ValueError) as exc:
        raise ExecutorConfigError("prepared-only manager context is invalid") from exc
    if current != config.execution or current.execution_state != "prepared":
        raise ExecutorConfigError("prepared-only manager context differs from local binding")
    registration_key = uuid5(
        _PREPARED_REGISTRATION_NAMESPACE,
        canonical_executable_digest(config.registration),
    )
    registered = await client.register_execution_executor(idempotency_key=registration_key)
    if registered != current:
        raise ExecutorConfigError("prepared executor registration changed its execution context")
    return await _publish_prepared_inventory(config, policy, client, runner)


async def _publish_prepared_inventory(
    config: PoolExecutorConfig,
    policy: SlurmInventoryPolicy,
    client: Any,
    runner: ReadOnlySlurmCommandRunner,
) -> ExecutorOnceResult:
    with ExecutorJournal(config.journal_file) as journal:
        heartbeats = ExecutableHeartbeatLoop(config.registration, journal, client)
        object_id = str(config.executor_incarnation)
        latest = journal.latest("inventory", object_id)
        durable_inventory = (
            None if latest is None else _inventory_from_journal_record(config, latest)
        )
        if latest is None or latest.event_kind != "inventory-publish-requested":
            await heartbeats.heartbeat()
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
        if latest is not None and latest.event_kind == "inventory-publish-requested":
            assert durable_inventory is not None
            payload = latest.durable_payload()
            assert payload is not None
            inventory = durable_inventory
            if expected_sequence not in {
                inventory.inventory_sequence - 1,
                inventory.inventory_sequence,
            }:
                raise ExecutorConfigError("manager inventory high-water changed during replay")
        else:
            if durable_inventory is not None:
                if expected_sequence < durable_inventory.inventory_sequence:
                    raise ExecutorConfigError(
                        "manager inventory high-water regressed behind durable confirmation"
                    )
                if expected_sequence > durable_inventory.inventory_sequence:
                    raise ExecutorConfigError(
                        "manager inventory high-water advanced beyond durable confirmation"
                    )
            binding = SlurmReportBinding(
                pool_sequence=expected_sequence + 1,
                inventory_sequence=expected_sequence + 1,
                execution=config.execution,
                executor_id=config.executor_id,
                executor_incarnation=config.executor_incarnation,
                journal_sequence=journal.head.sequence,
                journal_digest=journal.head.digest,
                journal_checkpoint_sequence=journal_sequence,
                journal_checkpoint_digest=journal_digest,
            )
            reports = await capture_slurm_capacity_reports(
                runner,
                policy=policy,
                binding=binding,
                source_observed_at=datetime.now(UTC),
            )
            inventory = reports.executable_inventory
            payload = canonical_executable_bytes(inventory)
            digest = canonical_executable_digest(inventory)
            journal.append(
                "inventory-publish-requested",
                digest,
                object_kind="inventory",
                object_id=object_id,
                payload=payload,
            )
        digest = canonical_executable_digest(inventory)
        await client.ingest_executable_inventory(inventory)
        journal.append(
            "inventory-publish-confirmed",
            digest,
            object_kind="inventory",
            object_id=object_id,
            payload=payload,
        )
        await heartbeats.heartbeat()
    return ExecutorOnceResult("inventory-only")


async def _publish_inert_inventory(
    config: PoolExecutorConfig,
    client: Any,
    *,
    validate_only: bool,
) -> ExecutorOnceResult:
    with ExecutorJournal(config.journal_file) as journal:
        heartbeats = ExecutableHeartbeatLoop(config.registration, journal, client)
        latest_inventory = journal.latest("inventory", str(config.executor_incarnation))
        if latest_inventory is None or latest_inventory.event_kind != "inventory-publish-requested":
            await heartbeats.heartbeat()
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
            await heartbeats.heartbeat()
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
        await heartbeats.heartbeat()
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


def _inventory_from_journal_record(
    config: PoolExecutorConfig,
    record: JournalRecord,
) -> ExecutableExecutorInventoryV2:
    if record.event_kind not in {
        "inventory-publish-requested",
        "inventory-publish-confirmed",
    }:
        raise ExecutorConfigError("inventory journal state is invalid")
    payload = record.durable_payload()
    if payload is None:
        raise ExecutorConfigError("inventory journal payload is absent")
    try:
        inventory = ExecutableExecutorInventoryV2.model_validate_json(payload)
    except ValueError:
        raise ExecutorConfigError("inventory journal payload is invalid") from None
    if (
        canonical_executable_bytes(inventory) != payload
        or canonical_executable_digest(inventory) != record.payload_digest
    ):
        raise ExecutorConfigError("inventory journal payload is not canonical")
    _assert_inventory_binding(config, inventory)
    return inventory


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
    expected_slurm_authority = getattr(executor, "expected_slurm_authority", None)
    executable_paths = tuple(
        sorted(
            (name, Path(getattr(slurm.executables, name).path))
            for name in ("scontrol", "sacctmgr", "squeue", "sbatch", "scancel", "sacct")
        )
    )
    registration_execution = executor.registration.execution.model_dump()
    authority_execution = authority.model_dump(exclude={"executable"})
    execution_matches = registration_execution == authority_execution or (
        retained_drain_execution_matches(executor.registration.execution, authority)
    )
    if (
        actual_registration != expected_registration
        or not execution_matches
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
        or not isinstance(expected_slurm_authority, SlurmAuthorityV2)
        or slurm != expected_slurm_authority
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
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--validate-only", action="store_true")
    modes.add_argument("--prepared-only", action="store_true")
    parser.add_argument("--activation-runtime-artifact")
    parser.add_argument("--inventory-policy")
    parser.add_argument("--expected-inventory-policy-sha256")
    args = parser.parse_args()
    policy_path_supplied = args.inventory_policy is not None
    policy_digest_supplied = args.expected_inventory_policy_sha256 is not None
    if policy_path_supplied != policy_digest_supplied:
        parser.error("inventory policy path and digest must be supplied together")
    if args.prepared_only:
        if not policy_path_supplied:
            parser.error("prepared-only mode requires an inventory policy path and digest")
        if args.activation_runtime_artifact is not None:
            parser.error("prepared-only mode refuses an activation runtime artifact")
    elif args.validate_only:
        if policy_path_supplied or args.activation_runtime_artifact is not None:
            parser.error("validate-only mode refuses runtime authority artifacts")
    else:
        if policy_path_supplied:
            parser.error("executable mode refuses a prepared inventory policy")
        if args.activation_runtime_artifact is None:
            parser.error("executable mode requires an activation runtime artifact")
    config = PoolExecutorConfig.from_files(
        args.config, expected_manifest_sha256=args.expected_manifest_sha256
    )
    inventory_policy = (
        load_slurm_inventory_policy(
            Path(args.inventory_policy),
            expected_sha256=args.expected_inventory_policy_sha256,
        )
        if args.prepared_only
        else None
    )
    asyncio.run(
        _run_with_signals(
            config,
            pool_id=args.pool,
            validate_only=args.validate_only,
            prepared_only=args.prepared_only,
            inventory_policy=inventory_policy,
            activation_runtime_artifact=(
                Path(args.activation_runtime_artifact)
                if args.activation_runtime_artifact is not None
                else None
            ),
        )
    )
    return 0


async def _run_with_signals(
    config: PoolExecutorConfig,
    *,
    pool_id: str | None,
    validate_only: bool,
    prepared_only: bool = False,
    inventory_policy: SlurmInventoryPolicy | None = None,
    activation_runtime_artifact: Path | None = None,
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
        run_daemon_once(
            config,
            pool_id=pool_id,
            validate_only=validate_only,
            prepared_only=prepared_only,
            inventory_policy=inventory_policy,
            activation_runtime_artifact=activation_runtime_artifact,
        )
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
