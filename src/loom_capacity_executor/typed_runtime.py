"""Distinct typed activation composition; public operation interlocks stay closed."""

from __future__ import annotations

import hmac
from collections.abc import Callable
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal

from pydantic import field_validator

from loom_capacity_executor.bootstrap_handoff import BootstrapHandoffStore
from loom_capacity_executor.build_admission_client import BuildAdmissionExecutorV1
from loom_capacity_executor.config import PoolExecutorConfig
from loom_capacity_executor.executable import ExecutablePoolExecutor
from loom_capacity_executor.journal import ExecutorJournal
from loom_capacity_executor.launch_policy_set import (
    PoolLaunchPolicyV3,
    validate_typed_runtime_profiles,
)
from loom_capacity_executor.pinned_admission_transport import PinnedAdmissionFileV1
from loom_capacity_executor.runtime import (
    RuntimeAssemblyError,
    _ActivationRuntimeArtifactBaseV2,
    _assert_config_artifact_binding,
    _execution_context_payload,
    _load_owner_runtime_payload,
    canonical_approved_profiles_digest,
    retained_drain_execution_matches,
)
from loom_capacity_executor.slurm_backend import AsyncSlurmBackend
from loom_capacity_executor.slurm_contracts import SlurmAuthorityV2
from loom_capacity_executor.typed_admission import (
    TypedAdmissionRouter,
    load_typed_admission_directory,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionContextV2,
    PoolControllerAuthorityV2,
    canonical_executable_bytes,
)


class ActivationRuntimeArtifactV3(_ActivationRuntimeArtifactBaseV2):
    schema_version: Literal[3] = 3  # type: ignore[assignment]
    admission: PinnedAdmissionFileV1
    policy: PoolLaunchPolicyV3

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 3:
            raise ValueError("typed runtime schema must be integer3")
        return value


def load_typed_activation_runtime_artifact(path: Path, *, expected_sha256: str) -> ActivationRuntimeArtifactV3:
    pin = PinnedAdmissionFileV1(path=str(path), sha256=expected_sha256)
    wire = _load_owner_runtime_payload(Path(pin.path), label="typed activation artifact")
    if not hmac.compare_digest(sha256(wire).hexdigest(), pin.sha256):
        raise RuntimeAssemblyError("typed activation artifact digest changed")
    artifact = ActivationRuntimeArtifactV3.model_validate_json(wire)
    if canonical_executable_bytes(artifact) != wire:
        raise RuntimeAssemblyError("typed activation artifact is not canonical")
    return artifact


def build_typed_executable_runtime(
    config: PoolExecutorConfig, artifact: ActivationRuntimeArtifactV3, *,
    manager_client: Any, current_context: ExecutionContextV2,
    slurm_backend_factory: Callable[[SlurmAuthorityV2], Any] = AsyncSlurmBackend,
) -> ExecutablePoolExecutor:
    """Bind real typed components without authorizing tick, recovery or launch."""
    if not isinstance(config, PoolExecutorConfig) or not isinstance(artifact, ActivationRuntimeArtifactV3):
        raise RuntimeAssemblyError("typed runtime inputs are invalid")
    artifact = ActivationRuntimeArtifactV3.model_validate_json(artifact.model_dump_json())
    if (_execution_context_payload(current_context) != _execution_context_payload(artifact.execution)
        and not retained_drain_execution_matches(artifact.execution, current_context)):
        raise RuntimeAssemblyError("current execution context differs from typed activation artifact")
    _assert_config_artifact_binding(config, artifact)
    if canonical_approved_profiles_digest(artifact.profiles) != artifact.approved_profiles_sha256:
        raise RuntimeAssemblyError("typed activation profile set digest changed")
    if artifact.policy.pool_id != artifact.pool_id or artifact.policy.pool_generation != artifact.pool_generation:
        raise RuntimeAssemblyError("typed activation policy pool changed")
    validate_typed_runtime_profiles(artifact.profiles, policy=artifact.policy,
        controller_authority_sha256=artifact.controller_authority_sha256)
    for profile in artifact.profiles:
        if (profile.trusted_launcher_release_sha256 != artifact.execution.trusted_fleet_release_sha256
            or any(getattr(profile, name) != getattr(config, name) for name in (
                "slurm_cluster", "controller_host", "partition", "association", "submitter", "qos",
            ))):
            raise RuntimeAssemblyError("typed activation profile differs from local authority")
    if not any(profile.profile_id == config.profile_id and profile.profile_generation == config.profile_generation
        and profile.profile_digest == config.profile_digest for profile in artifact.profiles):
        raise RuntimeAssemblyError("typed activation does not contain local runtime profile")
    identity = BuildAdmissionExecutorV1(pool_id=artifact.pool_id, pool_generation=artifact.pool_generation,
        executor_id=artifact.executor_id, executor_incarnation=artifact.executor_incarnation)
    directory = load_typed_admission_directory(Path(artifact.admission.path),
        expected_sha256=artifact.admission.sha256, executor=identity)
    purposes = {entry.purpose for entry in artifact.policy.entries}
    if any(entry.configuration_epoch != artifact.execution.configuration_epoch or entry.purpose not in purposes
        for entry in directory.entries):
        raise RuntimeAssemblyError("typed activation admission scope differs from policy")
    admission = TypedAdmissionRouter(Path(artifact.admission.path),
        expected_sha256=artifact.admission.sha256, executor=identity)
    handoff = BootstrapHandoffStore(Path(artifact.handoff_directory))
    journal = ExecutorJournal(config.journal_file)
    journal.__enter__()
    try:
        return ExecutablePoolExecutor(config.registration.model_copy(update={"execution":artifact.execution}),
            journal, manager_client, admission, slurm_backend_factory(artifact.slurm_authority),
            profile=artifact.profiles[0], profiles=artifact.profiles, typed_policy=artifact.policy,
            controller_authority=PoolControllerAuthorityV2(pool_id=artifact.pool_id,
                controller_authority_sha256=artifact.controller_authority_sha256),
            ownership_key=config.ownership_key, bootstrap_handoff_store=handoff)
    except BaseException:
        journal.close()
        raise
