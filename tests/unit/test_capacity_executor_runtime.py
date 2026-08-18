from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_capacity_executor.config import PoolExecutorConfig
from loom_capacity_executor.launch_renderer import (
    OperatorGenericTresMappingV2,
    OperatorLaunchProfileV2,
    canonical_launch_policy_digest,
)
from loom_capacity_executor.runtime import (
    ActivationRuntimeArtifactV2,
    AdmissionBindingEntryV2,
    AdmissionBindingResolutionError,
    ApprovedLaunchProfileSetV2,
    RoutedExecutableAdmissionClient,
    RuntimeAssemblyError,
    build_executable_runtime,
    canonical_admission_directory_digest,
    canonical_approved_profiles_digest,
    load_activation_runtime_artifact,
    load_approved_launch_profile_set,
    resolve_runtime_profile,
    write_admission_binding_directory,
)
from loom_capacity_executor.slurm_contracts import (
    SlurmAuthorityV2,
    SlurmExecutableIdentityV2,
    SlurmExecutablesV2,
    SlurmResourceV2,
)
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    canonical_executable_bytes,
)
from tests.unit.test_capacity_executor_config import executor_files
from tests.unit.test_capacity_executor_launch_renderer import (
    launch_context_fixture,
    operator_profile_fixture,
)


def _write_private(path: Path, value: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)
    return path


def _database_url(name: str) -> str:
    return (
        f"postgresql+psycopg://executor:{name}-secret@postgres.example.test/"
        f"loom_{name}?sslmode=verify-full"
    )


def _entry(root: Path, binding: ExecutableIntentBindingV2, name: str) -> AdmissionBindingEntryV2:
    url_file = _write_private(root / f"{name}.url", _database_url(name))
    return AdmissionBindingEntryV2(
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        configuration_generation=binding.execution.configuration_epoch,
        deployment_generation=binding.deployment_generation,
        candidate_generation=binding.candidate_generation,
        protected_admission_sha256=hashlib.sha256(name.encode("ascii")).hexdigest(),
        database_url_file=str(url_file),
        database_url_sha256=hashlib.sha256(url_file.read_bytes()).hexdigest(),
        environment_name=f"loom-dev-{name}",
    )


def _entry_with_environment(
    root: Path,
    binding: ExecutableIntentBindingV2,
    name: str,
) -> AdmissionBindingEntryV2:
    url_file = _write_private(root / f"{name}.url", _database_url(name.replace("-", "_")))
    return AdmissionBindingEntryV2(
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        configuration_generation=binding.execution.configuration_epoch,
        deployment_generation=binding.deployment_generation,
        candidate_generation=binding.candidate_generation,
        protected_admission_sha256=hashlib.sha256(name.encode("ascii")).hexdigest(),
        database_url_file=str(url_file),
        database_url_sha256=hashlib.sha256(url_file.read_bytes()).hexdigest(),
        environment_name=name,
    )


def _entry_name(entry: AdmissionBindingEntryV2) -> str:
    return f"{entry.subject_id.hex}-{entry.subject_incarnation.hex}.json"


class _FakeAdmission:
    def __init__(
        self,
        database_url: bytes,
        subject_id: UUID,
        subject_incarnation: UUID,
    ) -> None:
        self.database_url = database_url
        self.subject_id = subject_id
        self.subject_incarnation = subject_incarnation
        self.closed = False

    async def observe_intent(self, binding: ExecutableIntentBindingV2) -> dict[str, Any]:
        return {
            "database_url": self.database_url.decode("utf-8"),
            "subject_id": str(binding.subject_id),
            "subject_incarnation": str(binding.subject_incarnation),
        }

    async def aclose(self) -> None:
        self.closed = True


# Production break caught: a pool-local executor cannot retain one protected DB
# client and reuse it for other subjects or after generation/candidate drift.
async def test_routed_admission_resolver_uses_exact_subject_binding_per_operation(
    tmp_path: Path,
) -> None:
    base = launch_context_fixture().binding
    alice = base.model_copy(
        update={
            "subject_id": UUID(int=101),
            "subject_incarnation": UUID(int=102),
            "candidate_generation": 1,
            "deployment_generation": 1,
        }
    )
    bob = base.model_copy(
        update={
            "subject_id": UUID(int=201),
            "subject_incarnation": UUID(int=202),
            "candidate_generation": 1,
            "deployment_generation": 1,
        }
    )
    directory = tmp_path / "admission"
    directory.mkdir(mode=0o700)
    write_admission_binding_directory(
        directory, (_entry(tmp_path, alice, "alice"), _entry(tmp_path, bob, "bob"))
    )
    digest = canonical_admission_directory_digest(directory)
    opened: list[_FakeAdmission] = []

    def factory(
        database_url: bytes,
        *,
        subject_id: UUID,
        subject_incarnation: UUID,
    ) -> _FakeAdmission:
        client = _FakeAdmission(database_url, subject_id, subject_incarnation)
        opened.append(client)
        return client

    resolver = RoutedExecutableAdmissionClient(
        directory,
        expected_directory_sha256=digest,
        client_factory=factory,
    )

    first = await resolver.observe_intent(alice)
    second = await resolver.observe_intent(bob)

    assert json.loads(json.dumps(first))["database_url"] == _database_url("alice")
    assert json.loads(json.dumps(second))["database_url"] == _database_url("bob")
    assert len(opened) == 2
    assert all(client.closed for client in opened)
    with pytest.raises(AdmissionBindingResolutionError, match="generation"):
        await resolver.observe_intent(bob.model_copy(update={"deployment_generation": 2}))


# Production break caught: the routed client must pass the exact URL bytes it
# securely opened and hashed; passing the pathname lets an atomic replacement
# race swap credentials between validation and client construction.
async def test_routed_admission_resolver_uses_pinned_url_bytes_after_replacement_race(
    tmp_path: Path,
) -> None:
    binding = launch_context_fixture().binding
    directory = tmp_path / "admission-url-race"
    directory.mkdir(mode=0o700)
    entry = _entry(tmp_path, binding, "alice")
    write_admission_binding_directory(directory, (entry,))
    digest = canonical_admission_directory_digest(directory)
    original = _database_url("alice").encode("utf-8")
    changed = _database_url("mallory").encode("utf-8")
    observed: list[bytes] = []
    url_path = Path(entry.database_url_file)

    def factory(
        database_url: bytes | Path,
        *,
        subject_id: UUID,
        subject_incarnation: UUID,
    ) -> _FakeAdmission:
        replacement = _write_private(tmp_path / "replacement.url", changed.decode("utf-8"))
        replacement.replace(url_path)
        payload = database_url.read_bytes() if isinstance(database_url, Path) else database_url
        observed.append(payload)
        return _FakeAdmission(payload, subject_id, subject_incarnation)

    resolver = RoutedExecutableAdmissionClient(
        directory,
        expected_directory_sha256=digest,
        client_factory=factory,
    )

    result = await resolver.observe_intent(binding)

    assert observed == [original]
    assert result["database_url"] == original.decode("utf-8")


# Production break caught: admission binding publication must never follow a
# pre-existing symlink at the final entry path or partially overwrite its target.
def test_admission_binding_writer_rejects_symlink_entry_without_overwrite(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "admission-symlink"
    directory.mkdir(mode=0o700)
    entry = _entry(tmp_path, launch_context_fixture().binding, "alice")
    target = tmp_path / "victim.json"
    target.write_bytes(b"keep-me")
    target.chmod(0o600)
    (directory / _entry_name(entry)).symlink_to(target)

    with pytest.raises(AdmissionBindingResolutionError, match=r"nonsymlink|replace|exists"):
        write_admission_binding_directory(directory, (entry,))

    assert target.read_bytes() == b"keep-me"


# Production break caught: a racing or stale final entry must be treated as an
# existing immutable publication, not replaced in place by a new payload.
def test_admission_binding_writer_does_not_replace_existing_entry(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "admission-existing"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
    original = _entry(tmp_path, binding, "alice")
    changed = original.model_copy(
        update={"candidate_generation": original.candidate_generation + 1}
    )

    write_admission_binding_directory(directory, (original,))
    with pytest.raises(AdmissionBindingResolutionError, match=r"replace|exists|changed"):
        write_admission_binding_directory(directory, (changed,))

    assert (directory / _entry_name(original)).read_bytes() == canonical_executable_bytes(original)


# Production break caught: the routed admission directory must cover all global
# manager subjects, not only personal `loom-dev-*` development namespaces.
@pytest.mark.parametrize(
    "environment_name",
    ("production", "staging", "development", "loom-dev", "loom-dev-alice", "static-1"),
)
def test_admission_binding_accepts_global_and_static_subject_environments(
    tmp_path: Path,
    environment_name: str,
) -> None:
    entry = _entry_with_environment(tmp_path, launch_context_fixture().binding, environment_name)

    assert entry.environment_name == environment_name


def test_admission_binding_rejects_nonexistent_shared_dev_namespace(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="executable-scoped"):
        _entry_with_environment(
            tmp_path,
            launch_context_fixture().binding,
            "loom-dev-shared",
        )


def _executable(path: Path, name: str) -> SlurmExecutableIdentityV2:
    path.mkdir(parents=True, exist_ok=True)
    target = path / name
    target.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    target.chmod(0o755)
    return SlurmExecutableIdentityV2(
        path=str(target),
        sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
        owner_uid=target.stat().st_uid,
    )


def _slurm_authority(root: Path, profile: OperatorLaunchProfileV2) -> SlurmAuthorityV2:
    executables = {
        name: _executable(root, name)
        for name in ("scontrol", "sacctmgr", "squeue", "sbatch", "scancel", "sacct")
    }
    return SlurmAuthorityV2(
        cluster=profile.slurm_cluster,
        controller_host=profile.controller_host,
        partition=profile.partition,
        account=profile.association,
        submitter=profile.submitter,
        qos=profile.qos,
        local_uid=os.geteuid(),
        executables=SlurmExecutablesV2(**executables),
        resource_ceiling=SlurmResourceV2(
            cpus=64,
            memory_bytes=256 * 1024 * 1024 * 1024,
            gpus=8,
        ),
    )


def _slurm_authority_for_config(
    root: Path,
    profile: OperatorLaunchProfileV2,
    config: PoolExecutorConfig,
) -> SlurmAuthorityV2:
    authority = _slurm_authority(root, profile)
    configured_paths = dict(config.manifest.slurm_executables)
    executables = {
        name: getattr(authority.executables, name).model_copy(
            update={"path": str(configured_paths[name])}
        )
        for name in ("scontrol", "sacctmgr", "squeue", "sbatch", "scancel", "sacct")
    }
    return authority.model_copy(update={"executables": SlurmExecutablesV2(**executables)})


def _two_slot_profile(profile: OperatorLaunchProfileV2) -> OperatorLaunchProfileV2:
    changed = profile.model_copy(
        update={
            "profile_id": "oldlab-a100-two",
            "shape_id": "oldlab-a100-two-slot",
            "concurrency_slots": 2,
            "cpus": 32,
            "resources": ResourceVectorV1(
                slots=2,
                cpu_millicores=32_000,
                memory_bytes=68_719_476_736,
                gpu_count=2,
                generic={"fpga": 1, "gpu_a100": 2},
            ),
            "profile_digest": "9" * 64,
        }
    )
    return OperatorLaunchProfileV2.model_validate(changed.model_dump(mode="python"))


# Production break caught: a pool runtime with multiple approved profiles must
# not render using whichever singleton profile the daemon happened to hold.
def test_runtime_profile_resolver_selects_exact_profile_identity_and_resources() -> None:
    first = operator_profile_fixture()
    second = _two_slot_profile(first)
    binding = launch_context_fixture().binding.model_copy(
        update={
            "profile_id": second.profile_id,
            "profile_generation": second.profile_generation,
            "profile_digest": second.profile_digest,
            "shape_id": second.shape_id,
            "concurrency_slots": second.concurrency_slots,
            "resources": second.resources,
        }
    )

    resolved = resolve_runtime_profile(
        binding,
        (first, second),
        controller_authority_sha256=second.controller_authority_sha256,
    )

    assert resolved == second
    with pytest.raises(RuntimeAssemblyError, match="profile"):
        resolve_runtime_profile(
            binding.model_copy(update={"resources": first.resources}),
            (first, second),
            controller_authority_sha256=second.controller_authority_sha256,
        )


# Production break caught: the normal active daemon path had no immutable
# activation artifact capable of constructing the real manager/admission/Slurm
# executor runtime from exact local bindings.
def test_activation_runtime_artifact_builds_exact_executor_runtime(tmp_path: Path) -> None:
    files = executor_files(tmp_path)
    config = PoolExecutorConfig.from_files(files.config)
    context = launch_context_fixture()
    active = config.execution.model_copy(
        update={
            "execution_state": "active",
            "executable_new_capacity_ceiling": 1,
            "executable_new_capacity_rate_per_minute": 1,
        }
    )
    profile = context.profile.model_copy(
        update={
            "pool_generation": config.pool_generation,
            "profile_id": config.profile_id,
            "profile_generation": config.profile_generation,
            "profile_digest": config.profile_digest,
            "slurm_cluster": config.slurm_cluster,
            "controller_host": config.controller_host,
            "partition": config.partition,
            "association": config.association,
            "submitter": config.submitter,
            "qos": config.qos,
            "trusted_launcher_release_sha256": active.trusted_fleet_release_sha256,
            "controller_authority_sha256": "0" * 64,
        }
    )
    profile = profile.model_copy(
        update={"controller_authority_sha256": canonical_launch_policy_digest(profile)}
    )
    profile = OperatorLaunchProfileV2.model_validate(profile.model_dump(mode="python"))
    profiles = (profile, _two_slot_profile(profile))
    payload = json.loads(files.config.read_text(encoding="utf-8"))
    payload["controller_authority_sha256"] = profile.controller_authority_sha256
    payload["approved_profiles_sha256"] = canonical_approved_profiles_digest(profiles)
    _write_private(files.config, json.dumps(payload))
    config = PoolExecutorConfig.from_files(files.config)
    active = config.execution.model_copy(
        update={
            "execution_state": "active",
            "executable_new_capacity_ceiling": 1,
            "executable_new_capacity_rate_per_minute": 1,
        }
    )
    directory = tmp_path / "admission-runtime"
    directory.mkdir(mode=0o700)
    write_admission_binding_directory(directory, (_entry(tmp_path, context.binding, "alice"),))
    digest = canonical_admission_directory_digest(directory)
    handoff = tmp_path / "handoff"
    handoff.mkdir(mode=0o700)
    artifact = ActivationRuntimeArtifactV2(
        execution=active,
        pool_id=config.pool_id,
        pool_generation=config.pool_generation,
        executor_id=config.executor_id,
        executor_incarnation=config.executor_incarnation,
        controller_authority_sha256=profile.controller_authority_sha256,
        approved_profiles_sha256=canonical_approved_profiles_digest(profiles),
        local_authority_sha256=config.local_authority_sha256,
        signing_key_id=config.signing_key_id,
        signing_key_sha256=config.signing_key_sha256,
        immutable_manifest_sha256=config.manifest.sha256(),
        admission_directory=str(directory),
        admission_directory_sha256=digest,
        handoff_directory=str(handoff),
        journal_file=str(config.journal_file),
        state_directory=str(config.state_directory),
        slurm_authority=_slurm_authority_for_config(
            tmp_path / "slurm-bin",
            profile,
            config,
        ),
        profiles=profiles,
    )
    manager = object()
    admission = object()
    seen: list[SlurmAuthorityV2] = []

    def slurm_factory(authority: SlurmAuthorityV2) -> object:
        seen.append(authority)
        return object()

    executor = build_executable_runtime(
        config,
        artifact,
        manager_client=manager,
        current_context=active,
        admission_client_factory=lambda *_args, **_kwargs: admission,
        slurm_backend_factory=slurm_factory,
    )

    assert executor.registration.execution == active
    assert executor.client is manager
    assert executor.admission is admission
    assert seen == [artifact.slurm_authority]
    assert len(executor.profiles) == 2
    executor.journal.close()

    drain_only = active.model_copy(
        update={
            "writer_epoch": active.writer_epoch + 1,
            "execution_state": "drain-only",
            "executable_new_capacity_ceiling": 0,
            "executable_new_capacity_rate_per_minute": 0,
        }
    )
    retained = build_executable_runtime(
        config,
        artifact,
        manager_client=manager,
        current_context=drain_only,
        admission_client_factory=lambda *_args, **_kwargs: admission,
        slurm_backend_factory=slurm_factory,
    )
    try:
        assert retained.registration.execution == active
    finally:
        retained.journal.close()

    with pytest.raises(RuntimeAssemblyError, match="activation artifact"):
        build_executable_runtime(
            config,
            artifact,
            manager_client=manager,
            current_context=drain_only.model_copy(update={"execution_manifest_sha256": "f" * 64}),
            admission_client_factory=lambda *_args, **_kwargs: admission,
            slurm_backend_factory=slurm_factory,
        )

    seen.clear()
    changed_slurm = artifact.slurm_authority.model_copy(
        update={"controller_host": "other-controller.internal"}
    )
    with pytest.raises(RuntimeAssemblyError, match="Slurm"):
        build_executable_runtime(
            config,
            artifact.model_copy(update={"slurm_authority": changed_slurm}),
            manager_client=manager,
            current_context=active,
            admission_client_factory=lambda *_args, **_kwargs: admission,
            slurm_backend_factory=slurm_factory,
        )
    assert seen == []
    with pytest.raises(RuntimeAssemblyError, match="activation artifact"):
        build_executable_runtime(
            config,
            artifact,
            manager_client=manager,
            current_context=active.model_copy(update={"writer_epoch": active.writer_epoch + 1}),
            admission_client_factory=lambda *_args, **_kwargs: admission,
            slurm_backend_factory=slurm_factory,
        )
    with pytest.raises(RuntimeAssemblyError, match="activation artifact"):
        build_executable_runtime(
            config,
            artifact,
            manager_client=manager,
            current_context=drain_only.model_copy(update={"writer_epoch": active.writer_epoch + 2}),
            admission_client_factory=lambda *_args, **_kwargs: admission,
            slurm_backend_factory=slurm_factory,
        )


# Production break caught: per-profile manager-resource to Slurm-TRES mappings
# are profile-scoped and intentionally excluded from the pool-wide controller
# digest; positive runtime assembly must still reject a mutable mapping unless
# the controller-local approved-profile-set commitment changes too.
def test_activation_runtime_rejects_profile_set_not_pinned_by_local_manifest(
    tmp_path: Path,
) -> None:
    files = executor_files(tmp_path)
    config = PoolExecutorConfig.from_files(files.config)
    context = launch_context_fixture()
    active = config.execution.model_copy(
        update={
            "execution_state": "active",
            "executable_new_capacity_ceiling": 1,
            "executable_new_capacity_rate_per_minute": 1,
        }
    )
    profile = context.profile.model_copy(
        update={
            "pool_generation": config.pool_generation,
            "profile_id": config.profile_id,
            "profile_generation": config.profile_generation,
            "profile_digest": config.profile_digest,
            "slurm_cluster": config.slurm_cluster,
            "controller_host": config.controller_host,
            "partition": config.partition,
            "association": config.association,
            "submitter": config.submitter,
            "qos": config.qos,
            "trusted_launcher_release_sha256": active.trusted_fleet_release_sha256,
            "controller_authority_sha256": "0" * 64,
        }
    )
    profile = OperatorLaunchProfileV2.model_validate(
        profile.model_copy(
            update={"controller_authority_sha256": canonical_launch_policy_digest(profile)}
        ).model_dump(mode="python")
    )
    tampered = OperatorLaunchProfileV2.model_validate(
        profile.model_copy(
            update={
                "generic_tres": (
                    OperatorGenericTresMappingV2(
                        resource_name="fpga",
                        tres_name="gres/fpga-v2",
                    ),
                    profile.generic_tres[1],
                )
            }
        ).model_dump(mode="python")
    )
    assert canonical_launch_policy_digest(tampered) == profile.controller_authority_sha256
    assert canonical_approved_profiles_digest((tampered,)) != canonical_approved_profiles_digest(
        (profile,)
    )
    payload = json.loads(files.config.read_text(encoding="utf-8"))
    payload["controller_authority_sha256"] = profile.controller_authority_sha256
    payload["approved_profiles_sha256"] = canonical_approved_profiles_digest((profile,))
    _write_private(files.config, json.dumps(payload))
    config = PoolExecutorConfig.from_files(files.config)
    directory = tmp_path / "admission-profile-set"
    directory.mkdir(mode=0o700)
    write_admission_binding_directory(directory, (_entry(tmp_path, context.binding, "alice"),))
    handoff = tmp_path / "handoff-profile-set"
    handoff.mkdir(mode=0o700)
    artifact = ActivationRuntimeArtifactV2(
        execution=active,
        pool_id=config.pool_id,
        pool_generation=config.pool_generation,
        executor_id=config.executor_id,
        executor_incarnation=config.executor_incarnation,
        controller_authority_sha256=profile.controller_authority_sha256,
        approved_profiles_sha256=canonical_approved_profiles_digest((tampered,)),
        local_authority_sha256=config.local_authority_sha256,
        signing_key_id=config.signing_key_id,
        signing_key_sha256=config.signing_key_sha256,
        immutable_manifest_sha256=config.manifest.sha256(),
        admission_directory=str(directory),
        admission_directory_sha256=canonical_admission_directory_digest(directory),
        handoff_directory=str(handoff),
        journal_file=str(config.journal_file),
        state_directory=str(config.state_directory),
        slurm_authority=_slurm_authority_for_config(
            tmp_path / "slurm-bin-profile-set",
            profile,
            config,
        ),
        profiles=(tampered,),
    )

    with pytest.raises(RuntimeAssemblyError, match="profile set"):
        build_executable_runtime(
            config,
            artifact,
            manager_client=object(),
            current_context=active,
            admission_client_factory=lambda *_args, **_kwargs: object(),
            slurm_backend_factory=lambda _authority: object(),
        )


# Production break caught: the activation artifact journal binding must be
# owner-only; a group-readable file cannot become a positive runtime input.
def test_activation_runtime_artifact_rejects_group_readable_journal_file(
    tmp_path: Path,
) -> None:
    files = executor_files(tmp_path)
    config = PoolExecutorConfig.from_files(files.config)
    active = config.execution.model_copy(
        update={
            "execution_state": "active",
            "executable_new_capacity_ceiling": 1,
            "executable_new_capacity_rate_per_minute": 1,
        }
    )
    profile = launch_context_fixture().profile.model_copy(
        update={
            "pool_generation": config.pool_generation,
            "profile_id": config.profile_id,
            "profile_generation": config.profile_generation,
            "profile_digest": config.profile_digest,
            "slurm_cluster": config.slurm_cluster,
            "controller_host": config.controller_host,
            "partition": config.partition,
            "association": config.association,
            "submitter": config.submitter,
            "qos": config.qos,
            "trusted_launcher_release_sha256": active.trusted_fleet_release_sha256,
            "controller_authority_sha256": "0" * 64,
        }
    )
    profile = OperatorLaunchProfileV2.model_validate(
        profile.model_copy(
            update={"controller_authority_sha256": canonical_launch_policy_digest(profile)}
        ).model_dump(mode="python")
    )
    payload = json.loads(files.config.read_text(encoding="utf-8"))
    payload["controller_authority_sha256"] = profile.controller_authority_sha256
    _write_private(files.config, json.dumps(payload))
    config = PoolExecutorConfig.from_files(files.config)
    active = config.execution.model_copy(
        update={
            "execution_state": "active",
            "executable_new_capacity_ceiling": 1,
            "executable_new_capacity_rate_per_minute": 1,
        }
    )
    directory = tmp_path / "admission-journal"
    directory.mkdir(mode=0o700)
    write_admission_binding_directory(
        directory,
        (_entry(tmp_path, launch_context_fixture().binding, "alice"),),
    )
    handoff = tmp_path / "handoff-journal"
    handoff.mkdir(mode=0o700)
    config.journal_file.write_text("", encoding="utf-8")
    config.journal_file.chmod(0o640)

    with pytest.raises(ValidationError, match="journal file"):
        ActivationRuntimeArtifactV2(
            execution=active,
            pool_id=config.pool_id,
            pool_generation=config.pool_generation,
            executor_id=config.executor_id,
            executor_incarnation=config.executor_incarnation,
            controller_authority_sha256=profile.controller_authority_sha256,
            approved_profiles_sha256=canonical_approved_profiles_digest((profile,)),
            local_authority_sha256=config.local_authority_sha256,
            signing_key_id=config.signing_key_id,
            signing_key_sha256=config.signing_key_sha256,
            immutable_manifest_sha256=config.manifest.sha256(),
            admission_directory=str(directory),
            admission_directory_sha256=canonical_admission_directory_digest(directory),
            handoff_directory=str(handoff),
            journal_file=str(config.journal_file),
            state_directory=str(config.state_directory),
            slurm_authority=_slurm_authority(tmp_path / "slurm-bin-journal", profile),
            profiles=(profile,),
        )


def test_activation_runtime_artifact_loader_rejects_group_readable_file(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "activation-runtime.json"
    artifact.write_text("{}", encoding="utf-8")
    artifact.chmod(0o640)

    with pytest.raises(RuntimeAssemblyError, match="0600"):
        load_activation_runtime_artifact(artifact)


def test_approved_profile_set_loader_requires_owner_only_bounded_regular_file(
    tmp_path: Path,
) -> None:
    profiles = ApprovedLaunchProfileSetV2(profiles=(operator_profile_fixture(),))
    source = tmp_path / "approved-profiles.json"
    source.write_bytes(canonical_executable_bytes(profiles))
    source.chmod(0o600)

    assert load_approved_launch_profile_set(source) == profiles

    source.chmod(0o640)
    with pytest.raises(RuntimeAssemblyError, match="0600"):
        load_approved_launch_profile_set(source)
