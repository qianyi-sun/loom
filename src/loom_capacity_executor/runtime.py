"""Production assembly helpers for executable pool runtimes."""

from __future__ import annotations

import errno
import hashlib
import hmac
import os
import secrets
import stat
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import Field, field_validator

from loom_capacity_agent.admission import (
    ExecutableDrainRequestV2,
    ExecutableReleaseRequestV2,
    ExecutableWorkerRegistrationV2,
    ExecutableWorkerWithdrawalRequestV2,
    PhysicalJobBindingV2,
)
from loom_capacity_agent.claim_guard import ExecutableClaimProposalV2
from loom_capacity_executor.admission_client import DatabaseExecutableAdmissionClient
from loom_capacity_executor.bootstrap_handoff import BootstrapHandoffStore
from loom_capacity_executor.config import PoolExecutorConfig
from loom_capacity_executor.executable import ExecutablePoolExecutor
from loom_capacity_executor.journal import ExecutorJournal
from loom_capacity_executor.launch_renderer import (
    OperatorLaunchProfileV2,
    canonical_launch_policy_digest,
)
from loom_capacity_executor.slurm_backend import AsyncSlurmBackend
from loom_capacity_executor.slurm_contracts import SlurmAuthorityV2
from loom_capacity_manager.contracts import MAX_SUBJECTS, Digest
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    ExecutableIntentBindingV2,
    ExecutionContextV2,
    PoolControllerAuthorityV2,
    StrictV2Model,
    canonical_executable_bytes,
    canonical_executable_digest,
)

_MAX_ADMISSION_ENTRY_BYTES = 16 * 1024
_MAX_RUNTIME_ARTIFACT_BYTES = 1024 * 1024
_MAX_DIRECTORY_ENTRIES = MAX_SUBJECTS
_ClientFactory = Callable[..., Any]
_SlurmFactory = Callable[[SlurmAuthorityV2], Any]


class AdmissionBindingResolutionError(RuntimeError):
    """A protected admission binding directory failed closed."""


class RuntimeAssemblyError(RuntimeError):
    """A positive executor runtime artifact failed exact immutable validation."""


class AdmissionBindingEntryV2(StrictV2Model):
    """One owner-only protected admission binding for an exact subject incarnation."""

    subject_id: UUID
    subject_incarnation: UUID
    configuration_generation: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    deployment_generation: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    candidate_generation: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    protected_admission_sha256: Digest
    database_url_file: Annotated[str, Field(min_length=1, max_length=4096)]
    database_url_sha256: Digest
    environment_name: Annotated[str, Field(min_length=1, max_length=128)]

    @field_validator("database_url_file")
    @classmethod
    def _absolute_url_file(cls, value: str) -> str:
        path = Path(value)
        if (
            "\0" in value
            or not path.is_absolute()
            or path == Path("/")
            or path.is_symlink()
            or ".." in path.parts
        ):
            raise ValueError("database URL file must be a canonical absolute path")
        return value

    @field_validator("environment_name")
    @classmethod
    def _environment_name(cls, value: str) -> str:
        allowed_static = value.startswith("static-") and len(value) > len("static-")
        if value == "loom-dev-shared" or not (
            value in {"production", "staging", "development", "loom-dev"}
            or value.startswith("loom-dev-")
            or allowed_static
        ):
            raise ValueError("admission environment name is not executable-scoped")
        return value


class AdmissionBindingDirectoryV2(StrictV2Model):
    """Canonical digest input for the complete protected admission directory."""

    entries: Annotated[
        tuple[AdmissionBindingEntryV2, ...],
        Field(max_length=_MAX_DIRECTORY_ENTRIES),
    ]

    @field_validator("entries")
    @classmethod
    def _canonical_entries(
        cls,
        value: tuple[AdmissionBindingEntryV2, ...],
    ) -> tuple[AdmissionBindingEntryV2, ...]:
        keys = [(item.subject_id, item.subject_incarnation) for item in value]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate protected admission subject binding")
        return tuple(
            sorted(value, key=lambda item: (item.subject_id.int, item.subject_incarnation.int))
        )


class ActivationRuntimeArtifactV2(StrictV2Model):
    """Operator-supplied immutable positive runtime artifact for one pool executor."""

    execution: ExecutionContextV2
    pool_id: Literal["gb10", "oldlab"]
    pool_generation: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    executor_id: Annotated[str, Field(min_length=1, max_length=128)]
    executor_incarnation: UUID
    controller_authority_sha256: Digest
    local_authority_sha256: Digest
    signing_key_id: Annotated[str, Field(min_length=1, max_length=128)]
    signing_key_sha256: Digest
    immutable_manifest_sha256: Digest
    admission_directory: Annotated[str, Field(min_length=1, max_length=4096)]
    admission_directory_sha256: Digest
    handoff_directory: Annotated[str, Field(min_length=1, max_length=4096)]
    journal_file: Annotated[str, Field(min_length=1, max_length=4096)]
    state_directory: Annotated[str, Field(min_length=1, max_length=4096)]
    slurm_authority: SlurmAuthorityV2
    profiles: Annotated[tuple[OperatorLaunchProfileV2, ...], Field(min_length=1)]

    @field_validator("admission_directory", "handoff_directory", "state_directory")
    @classmethod
    def _owner_private_directory(cls, value: str) -> str:
        path = _absolute_owner_path(value)
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise ValueError("runtime directory is unavailable") from exc
        if (
            path.is_symlink()
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise ValueError("runtime directory must be a current-UID-owned 0700 nonsymlink")
        return value

    @field_validator("journal_file")
    @classmethod
    def _owner_private_parent_file(cls, value: str) -> str:
        path = _absolute_owner_path(value)
        try:
            parent = path.parent.lstat()
        except OSError as exc:
            raise ValueError("journal parent is unavailable") from exc
        if (
            path.parent.is_symlink()
            or not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.geteuid()
            or stat.S_IMODE(parent.st_mode) != 0o700
        ):
            raise ValueError("journal parent must be a current-UID-owned 0700 nonsymlink")
        if path.exists():
            try:
                metadata = path.lstat()
            except OSError as exc:
                raise ValueError("journal file is unavailable") from exc
            if (
                path.is_symlink()
                or not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or stat.S_IMODE(metadata.st_mode) != 0o600
            ):
                raise ValueError("journal file must be a current-UID-owned 0600 nonsymlink")
        return value


def load_activation_runtime_artifact(path: Path) -> ActivationRuntimeArtifactV2:
    """Securely load one non-renderable, operator-supplied runtime artifact."""

    artifact_path = _absolute_owner_path(str(path))
    try:
        before = artifact_path.lstat()
    except OSError as exc:
        raise RuntimeAssemblyError("activation runtime artifact is unavailable") from exc
    if (
        artifact_path.is_symlink()
        or not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
    ):
        raise RuntimeAssemblyError(
            "activation runtime artifact must be a current-UID-owned 0600 nonsymlink"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(artifact_path, flags)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise RuntimeAssemblyError("activation runtime artifact must be a nonsymlink") from exc
        raise RuntimeAssemblyError("activation runtime artifact is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise RuntimeAssemblyError("activation runtime artifact changed while opening")
        payload = os.read(descriptor, _MAX_RUNTIME_ARTIFACT_BYTES + 1)
    finally:
        os.close(descriptor)
    if not payload or len(payload) > _MAX_RUNTIME_ARTIFACT_BYTES:
        raise RuntimeAssemblyError("activation runtime artifact exceeds its byte bound")
    try:
        return ActivationRuntimeArtifactV2.model_validate_json(payload)
    except ValueError as exc:
        raise RuntimeAssemblyError("activation runtime artifact is invalid") from exc


def _absolute_owner_path(value: str) -> Path:
    path = Path(value)
    if (
        "\0" in value
        or not path.is_absolute()
        or path == Path("/")
        or path.is_symlink()
        or ".." in path.parts
    ):
        raise ValueError("runtime path must be canonical and absolute")
    return path


def resolve_runtime_profile(
    binding: ExecutableIntentBindingV2,
    profiles: tuple[OperatorLaunchProfileV2, ...],
    *,
    controller_authority_sha256: str,
) -> OperatorLaunchProfileV2:
    """Select the one approved profile that exactly matches an intent binding."""

    if not isinstance(binding, ExecutableIntentBindingV2):
        raise RuntimeAssemblyError("runtime profile binding is not executable-v2")
    matches: list[OperatorLaunchProfileV2] = []
    for profile in profiles:
        if not isinstance(profile, OperatorLaunchProfileV2):
            raise RuntimeAssemblyError("runtime profile set contains an invalid profile")
        profile_digest = canonical_launch_policy_digest(profile)
        if (
            binding.pool_id == profile.pool_id
            and binding.pool_generation == profile.pool_generation
            and binding.profile_id == profile.profile_id
            and binding.profile_generation == profile.profile_generation
            and hmac.compare_digest(binding.profile_digest, profile.profile_digest)
            and binding.shape_id == profile.shape_id
            and binding.concurrency_slots == profile.concurrency_slots
            and binding.resources == profile.resources
            and hmac.compare_digest(
                profile.controller_authority_sha256, controller_authority_sha256
            )
            and hmac.compare_digest(profile_digest, controller_authority_sha256)
        ):
            matches.append(profile)
    if len(matches) != 1:
        raise RuntimeAssemblyError("runtime profile does not exactly match binding")
    return matches[0]


def build_executable_runtime(
    config: PoolExecutorConfig,
    artifact: ActivationRuntimeArtifactV2,
    *,
    manager_client: Any,
    current_context: ExecutionContextV2,
    admission_client_factory: _ClientFactory | None = None,
    slurm_backend_factory: _SlurmFactory = AsyncSlurmBackend,
) -> ExecutablePoolExecutor:
    """Assemble one positive pool executor from exact local and activation bindings."""

    if not isinstance(config, PoolExecutorConfig):
        raise RuntimeAssemblyError("executor config is invalid")
    if not isinstance(artifact, ActivationRuntimeArtifactV2):
        raise RuntimeAssemblyError("activation runtime artifact is invalid")
    if current_context != artifact.execution:
        raise RuntimeAssemblyError("current execution context differs from activation artifact")
    _assert_config_artifact_binding(config, artifact)
    _assert_profiles(config, artifact)
    admission_directory = Path(artifact.admission_directory)
    if (
        canonical_admission_directory_digest(admission_directory)
        != artifact.admission_directory_sha256
    ):
        raise RuntimeAssemblyError("admission directory digest differs from activation artifact")
    admission = (
        RoutedExecutableAdmissionClient(
            admission_directory,
            expected_directory_sha256=artifact.admission_directory_sha256,
        )
        if admission_client_factory is None
        else admission_client_factory(
            admission_directory,
            expected_directory_sha256=artifact.admission_directory_sha256,
        )
    )
    journal = ExecutorJournal(config.journal_file)
    journal.__enter__()
    try:
        return ExecutablePoolExecutor(
            config.registration.model_copy(update={"execution": artifact.execution}),
            journal,
            manager_client,
            admission,
            slurm_backend_factory(artifact.slurm_authority),
            profile=artifact.profiles[0],
            profiles=artifact.profiles,
            controller_authority=PoolControllerAuthorityV2(
                pool_id=artifact.pool_id,
                controller_authority_sha256=artifact.controller_authority_sha256,
            ),
            ownership_key=config.ownership_key,
            bootstrap_handoff_store=BootstrapHandoffStore(Path(artifact.handoff_directory)),
        )
    except Exception:
        journal.close()
        raise


def _assert_config_artifact_binding(
    config: PoolExecutorConfig,
    artifact: ActivationRuntimeArtifactV2,
) -> None:
    if (
        artifact.pool_id != config.pool_id
        or artifact.pool_generation != config.pool_generation
        or artifact.executor_id != config.executor_id
        or artifact.executor_incarnation != config.executor_incarnation
        or not hmac.compare_digest(
            artifact.controller_authority_sha256,
            config.controller_authority_sha256,
        )
        or not hmac.compare_digest(artifact.local_authority_sha256, config.local_authority_sha256)
        or artifact.signing_key_id != config.signing_key_id
        or not hmac.compare_digest(artifact.signing_key_sha256, config.signing_key_sha256)
        or not hmac.compare_digest(artifact.immutable_manifest_sha256, config.manifest.sha256())
        or Path(artifact.journal_file) != config.journal_file
        or Path(artifact.state_directory) != config.state_directory
    ):
        raise RuntimeAssemblyError("activation artifact differs from controller-local binding")


def _assert_profiles(config: PoolExecutorConfig, artifact: ActivationRuntimeArtifactV2) -> None:
    seen: set[tuple[str, int, str, str, int, str]] = set()
    for profile in artifact.profiles:
        policy_digest = canonical_launch_policy_digest(profile)
        key = (
            profile.profile_id,
            profile.profile_generation,
            profile.profile_digest,
            profile.shape_id,
            profile.concurrency_slots,
            str(profile.resources.model_dump(mode="json")),
        )
        if key in seen:
            raise RuntimeAssemblyError("activation artifact contains duplicate runtime profile")
        seen.add(key)
        if (
            profile.pool_id != artifact.pool_id
            or profile.pool_generation != artifact.pool_generation
            or not hmac.compare_digest(
                profile.controller_authority_sha256,
                artifact.controller_authority_sha256,
            )
            or not hmac.compare_digest(policy_digest, artifact.controller_authority_sha256)
            or profile.trusted_launcher_release_sha256
            != artifact.execution.trusted_fleet_release_sha256
        ):
            raise RuntimeAssemblyError("activation artifact profile differs from authority")
    if not any(
        profile.profile_id == config.profile_id
        and profile.profile_generation == config.profile_generation
        and profile.profile_digest == config.profile_digest
        for profile in artifact.profiles
    ):
        raise RuntimeAssemblyError("activation artifact does not contain local runtime profile")


def _private_directory(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise AdmissionBindingResolutionError("admission directory is unavailable") from exc
    if (
        path.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise AdmissionBindingResolutionError(
            "admission directory must be a current-UID-owned 0700 nonsymlink directory"
        )


def _private_regular(path: Path, *, label: str, maximum: int) -> bytes:
    try:
        before = path.lstat()
    except OSError as exc:
        raise AdmissionBindingResolutionError(f"{label} is unavailable") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
    ):
        raise AdmissionBindingResolutionError(
            f"{label} must be a current-UID-owned 0600 regular nonsymlink"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise AdmissionBindingResolutionError(f"{label} must be a nonsymlink") from exc
        raise AdmissionBindingResolutionError(f"{label} is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise AdmissionBindingResolutionError(f"{label} changed while opening")
        payload = os.read(descriptor, maximum + 1)
    finally:
        os.close(descriptor)
    if not payload or len(payload) > maximum:
        raise AdmissionBindingResolutionError(f"{label} exceeds its byte bound")
    return payload


def _write_private_new(path: Path, payload: bytes, *, label: str, maximum: int) -> None:
    if not payload or len(payload) > maximum:
        raise AdmissionBindingResolutionError(f"{label} exceeds its byte bound")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(temporary, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            existing = _private_regular(path, label=label, maximum=maximum)
            if existing != payload:
                raise AdmissionBindingResolutionError(
                    f"{label} already exists and cannot be replaced"
                ) from exc
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
    finally:
        descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _entry_filename(entry: AdmissionBindingEntryV2) -> str:
    return f"{entry.subject_id.hex}-{entry.subject_incarnation.hex}.json"


def write_admission_binding_directory(
    directory: Path,
    entries: tuple[AdmissionBindingEntryV2, ...],
) -> None:
    _private_directory(directory)
    document = AdmissionBindingDirectoryV2(entries=entries)
    for entry in document.entries:
        path = directory / _entry_filename(entry)
        _write_private_new(
            path,
            canonical_executable_bytes(entry),
            label="admission binding entry",
            maximum=_MAX_ADMISSION_ENTRY_BYTES,
        )
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def load_admission_binding_directory(directory: Path) -> AdmissionBindingDirectoryV2:
    _private_directory(directory)
    entries: list[AdmissionBindingEntryV2] = []
    for path in sorted(directory.iterdir(), key=lambda item: item.name):
        if not path.name.endswith(".json"):
            raise AdmissionBindingResolutionError("admission directory contains unknown entries")
        payload = _private_regular(
            path,
            label="admission binding entry",
            maximum=_MAX_ADMISSION_ENTRY_BYTES,
        )
        try:
            entry = AdmissionBindingEntryV2.model_validate_json(payload)
        except ValueError as exc:
            raise AdmissionBindingResolutionError("admission binding entry is invalid") from exc
        if path.name != _entry_filename(entry):
            raise AdmissionBindingResolutionError("admission binding filename changed")
        entries.append(entry)
    try:
        return AdmissionBindingDirectoryV2(entries=tuple(entries))
    except ValueError as exc:
        raise AdmissionBindingResolutionError("admission binding directory is invalid") from exc


def canonical_admission_directory_digest(directory: Path) -> str:
    return canonical_executable_digest(load_admission_binding_directory(directory))


def _default_client_factory(
    path: Path,
    *,
    subject_id: UUID,
    subject_incarnation: UUID,
) -> DatabaseExecutableAdmissionClient:
    return DatabaseExecutableAdmissionClient.from_database_url_file(
        path,
        subject_id=subject_id,
        subject_incarnation=subject_incarnation,
    )


class RoutedExecutableAdmissionClient:
    """Resolve protected admission per exact subject operation, then dispose it."""

    def __init__(
        self,
        directory: Path,
        *,
        expected_directory_sha256: str,
        client_factory: _ClientFactory = _default_client_factory,
    ) -> None:
        self._directory = directory
        self._expected_directory_sha256 = expected_directory_sha256
        self._client_factory = client_factory
        self._load_verified()

    def _load_verified(self) -> AdmissionBindingDirectoryV2:
        document = load_admission_binding_directory(self._directory)
        digest = canonical_executable_digest(document)
        if digest != self._expected_directory_sha256:
            raise AdmissionBindingResolutionError("admission directory digest changed")
        return document

    def _resolve(self, binding: ExecutableIntentBindingV2) -> AdmissionBindingEntryV2:
        if not isinstance(binding, ExecutableIntentBindingV2):
            raise AdmissionBindingResolutionError("admission binding is not executable-v2")
        matches = tuple(
            entry
            for entry in self._load_verified().entries
            if entry.subject_id == binding.subject_id
            and entry.subject_incarnation == binding.subject_incarnation
        )
        if len(matches) != 1:
            raise AdmissionBindingResolutionError("protected admission subject binding is absent")
        entry = matches[0]
        if (
            entry.configuration_generation != binding.execution.configuration_epoch
            or entry.deployment_generation != binding.deployment_generation
            or entry.candidate_generation != binding.candidate_generation
        ):
            raise AdmissionBindingResolutionError("protected admission generation binding changed")
        url_path = Path(entry.database_url_file)
        digest = hashlib.sha256(
            _private_regular(url_path, label="database URL file", maximum=16 * 1024)
        ).hexdigest()
        if digest != entry.database_url_sha256:
            raise AdmissionBindingResolutionError("protected admission URL binding changed")
        return entry

    async def _call(
        self,
        binding: ExecutableIntentBindingV2,
        method: str,
        *args: object,
        **kwargs: object,
    ) -> Any:
        entry = self._resolve(binding)
        client = self._client_factory(
            Path(entry.database_url_file),
            subject_id=entry.subject_id,
            subject_incarnation=entry.subject_incarnation,
        )
        try:
            return await getattr(client, method)(*args, **kwargs)
        finally:
            close = getattr(client, "aclose", None)
            if close is not None:
                await close()

    async def prepare_worker(
        self,
        request: ExecutableBootstrapRegistrationV2,
        *,
        bootstrap_sha256: str,
    ) -> Any:
        return await self._call(
            request.binding,
            "prepare_worker",
            request,
            bootstrap_sha256=bootstrap_sha256,
        )

    def bootstrap_handoff_route_sha256(self, binding: ExecutableIntentBindingV2) -> str:
        return canonical_executable_digest(self._resolve(binding))

    async def bind_slurm_job(self, request: PhysicalJobBindingV2) -> Any:
        return await self._call(request.binding, "bind_slurm_job", request)

    async def observe_intent(self, binding: ExecutableIntentBindingV2) -> Any:
        return await self._call(binding, "observe_intent", binding)

    async def begin_drain(self, request: ExecutableDrainRequestV2) -> Any:
        return await self._call(request.binding, "begin_drain", request)

    async def withdraw_unregistered_worker(
        self,
        request: ExecutableWorkerWithdrawalRequestV2,
    ) -> Any:
        return await self._call(request.binding, "withdraw_unregistered_worker", request)

    async def register_worker(
        self,
        request: ExecutableWorkerRegistrationV2,
        *,
        bootstrap_capability: str,
    ) -> Any:
        return await self._call(
            request.binding,
            "register_worker",
            request,
            bootstrap_capability=bootstrap_capability,
        )

    async def acknowledge_release(
        self,
        request: ExecutableReleaseRequestV2,
        *,
        current_worker_credential: str,
    ) -> Any:
        return await self._call(
            request.binding,
            "acknowledge_release",
            request,
            current_worker_credential=current_worker_credential,
        )

    async def admit_claim(
        self,
        binding: ExecutableIntentBindingV2,
        proposal: ExecutableClaimProposalV2,
    ) -> Any:
        return await self._call(binding, "admit_claim", proposal)


__all__ = [
    "ActivationRuntimeArtifactV2",
    "AdmissionBindingDirectoryV2",
    "AdmissionBindingEntryV2",
    "AdmissionBindingResolutionError",
    "RoutedExecutableAdmissionClient",
    "RuntimeAssemblyError",
    "build_executable_runtime",
    "canonical_admission_directory_digest",
    "load_activation_runtime_artifact",
    "load_admission_binding_directory",
    "resolve_runtime_profile",
    "write_admission_binding_directory",
]
