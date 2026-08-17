"""Fail-closed controller-local configuration for the executable pool daemon."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import stat
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal
from uuid import UUID

from loom_capacity_executor.keys import (
    ExecutorKeyError,
    ExecutorOwnershipKey,
    load_executor_ownership_key,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorRegistrationV2,
    ExecutionContextV2,
    PoolControllerAuthorityV2,
)

_MAX_CONFIG_BYTES = 64 * 1024
_POOL_IDS = frozenset({"gb10", "oldlab"})
_DIGEST_LENGTH = 64


class ExecutorConfigError(ValueError):
    """The controller-local daemon configuration is unsafe or incomplete."""


@dataclass(frozen=True, slots=True)
class ImmutablePoolManifest:
    """The retained pool-local facts consumed by Task 6/7/8 runtime binding."""

    pool_id: Literal["gb10", "oldlab"]
    pool_generation: int
    controller_authority_sha256: str
    approved_profiles_sha256: str
    executor_id: str
    executor_incarnation: UUID
    local_authority_sha256: str
    signing_key_id: str
    signing_key_sha256: str
    ownership_key_file: Path
    manager_origin: str
    bearer_token_file: Path
    tls_ca_file: Path
    tls_certificate_file: Path
    tls_private_key_file: Path
    state_directory: Path
    journal_file: Path
    local_uid: int
    slurm_cluster: str
    controller_host: str
    partition: str
    association: str
    submitter: str
    qos: str
    profile_id: str
    profile_generation: int
    profile_digest: str
    slurm_executables: tuple[tuple[str, Path], ...]
    executor_image: str
    service_user: str

    def sha256(self) -> str:
        value = asdict(self)
        for name, item in tuple(value.items()):
            if isinstance(item, Path):
                value[name] = str(item)
            elif isinstance(item, UUID):
                value[name] = str(item)
        value["slurm_executables"] = [(name, str(path)) for name, path in self.slurm_executables]
        return hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
                "ascii"
            )
        ).hexdigest()


def _absolute_path(value: object, *, label: str) -> Path:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ExecutorConfigError(f"{label} must be an absolute path")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ExecutorConfigError(f"{label} must be an absolute path")
    return path


def _private_regular(path: Path, *, label: str, maximum: int) -> bytes:
    try:
        before = path.lstat()
    except OSError as exc:
        raise ExecutorConfigError(f"{label} is unavailable") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
    ):
        raise ExecutorConfigError(f"{label} must be a current-UID-owned 0600 regular nonsymlink")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise ExecutorConfigError(f"{label} must be a regular nonsymlink") from exc
        raise ExecutorConfigError(f"{label} is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise ExecutorConfigError(f"{label} changed while opening")
        payload = os.read(descriptor, maximum + 1)
    finally:
        os.close(descriptor)
    if not payload or len(payload) > maximum:
        raise ExecutorConfigError(f"{label} exceeds its byte bound")
    return payload


def _one_line_text(path: Path, *, label: str, maximum: int) -> str:
    try:
        value = _private_regular(path, label=label, maximum=maximum).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ExecutorConfigError(f"{label} is not UTF-8") from exc
    if value.endswith("\n"):
        value = value[:-1]
    if not value or "\n" in value or "\r" in value or "\x00" in value:
        raise ExecutorConfigError(f"{label} must contain exactly one line")
    return value


def _digest(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _DIGEST_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ExecutorConfigError(f"{label} must be an exact SHA-256 fingerprint")
    return value


def _identifier(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 128
        or not value.replace("-", "").replace("_", "").replace(".", "").isalnum()
    ):
        raise ExecutorConfigError(f"{label} is invalid")
    return value


def _private_state_directory(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ExecutorConfigError("state directory is unavailable") from exc
    if (
        path.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise ExecutorConfigError("state directory must be a current-UID-owned 0700 directory")


@dataclass(frozen=True, slots=True)
class PoolExecutorConfig:
    """One immutable local binding; checked-in use is permanently zero ceiling."""

    pool_id: Literal["gb10", "oldlab"]
    pool_generation: int
    executor_id: str
    executor_incarnation: UUID
    controller_authority_sha256: str
    approved_profiles_sha256: str
    local_authority_sha256: str
    signing_key_id: str
    signing_key_sha256: str
    ownership_key_file: Path
    ownership_key: ExecutorOwnershipKey
    manager_origin: str
    local_uid: int
    bearer_token_file: Path
    tls_ca_file: Path
    tls_certificate_file: Path
    tls_private_key_file: Path
    state_directory: Path
    journal_file: Path
    slurm_cluster: str
    controller_host: str
    partition: str
    association: str
    submitter: str
    qos: str
    profile_id: str
    profile_generation: int
    profile_digest: str
    executor_image: str
    service_user: str
    manifest: ImmutablePoolManifest
    expected_manifest_sha256: str | None
    execution: ExecutionContextV2

    @classmethod
    def from_files(
        cls, config_file: Path, *, expected_manifest_sha256: str | None = None
    ) -> PoolExecutorConfig:
        config_path = _absolute_path(str(config_file), label="configuration path")
        raw = _one_line_text(config_path, label="configuration file", maximum=_MAX_CONFIG_BYTES)
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ExecutorConfigError("configuration file is not bounded JSON") from exc
        if not isinstance(value, dict):
            raise ExecutorConfigError("configuration file must be a JSON object")
        allowed = {
            "pool_id",
            "pool_generation",
            "executor_id",
            "executor_incarnation",
            "controller_authority_sha256",
            "approved_profiles_sha256",
            "local_authority_sha256",
            "signing_key_id",
            "signing_key_sha256",
            "manager_origin",
            "bearer_token_file",
            "tls_ca_file",
            "tls_certificate_file",
            "tls_private_key_file",
            "state_directory",
            "journal_file",
            "ownership_key_file",
            "authority_incarnation",
            "writer_epoch",
            "configuration_epoch",
            "execution_epoch",
            "execution_manifest_sha256",
            "trusted_fleet_release_sha256",
            "controller_host",
            "partition",
            "association",
            "local_uid",
            "slurm_executables",
            "slurm_cluster",
            "submitter",
            "qos",
            "profile_id",
            "profile_generation",
            "profile_digest",
            "executor_image",
            "service_user",
        }
        if set(value) - allowed:
            raise ExecutorConfigError("configuration file contains unknown fields")
        pool_id = value.get("pool_id")
        if pool_id not in _POOL_IDS:
            raise ExecutorConfigError("pool binding is invalid")
        pool = pool_id
        pool_generation = value.get("pool_generation")
        if type(pool_generation) is not int or pool_generation <= 0:
            raise ExecutorConfigError("pool generation is invalid")
        executor_id = _identifier(value.get("executor_id"), label="executor id")
        if not executor_id.startswith(f"{pool}-"):
            raise ExecutorConfigError("executor id differs from pool binding")
        try:
            incarnation = UUID(str(value.get("executor_incarnation")))
            authority_incarnation = UUID(str(value.get("authority_incarnation", UUID(int=1))))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ExecutorConfigError("executor authority identity is invalid") from exc
        controller_digest = _digest(
            value.get("controller_authority_sha256"), label="controller authority"
        )
        approved_profiles_digest = _digest(
            value.get("approved_profiles_sha256", "0" * 64),
            label="approved profiles",
        )
        local_digest = _digest(value.get("local_authority_sha256"), label="local authority")
        signing_key_id = _identifier(value.get("signing_key_id"), label="signing key id")
        if not signing_key_id.startswith(f"{pool}-"):
            raise ExecutorConfigError("signing key differs from pool binding")
        signing_key_sha = _digest(value.get("signing_key_sha256"), label="signing key")
        ownership_key = _absolute_path(value.get("ownership_key_file"), label="ownership key file")
        key_bytes = _private_regular(ownership_key, label="ownership key file", maximum=32)
        if len(key_bytes) != 32:
            raise ExecutorConfigError("ownership key file must contain exactly 32 raw bytes")
        try:
            loaded_key = load_executor_ownership_key(
                ownership_key,
                signing_key_id=signing_key_id,
                expected_public_key_sha256=signing_key_sha,
            )
        except ExecutorKeyError as exc:
            raise ExecutorConfigError(
                "ownership key fingerprint differs from registration"
            ) from exc
        manager_origin = value.get("manager_origin")
        if not isinstance(manager_origin, str) or not manager_origin.startswith("https://"):
            raise ExecutorConfigError("manager origin must be an HTTPS origin")
        local_uid = value.get("local_uid")
        if type(local_uid) is not int or local_uid != os.geteuid():
            raise ExecutorConfigError("local UID differs from controller-local identity")
        bearer = _absolute_path(value.get("bearer_token_file"), label="bearer token file")
        _one_line_text(bearer, label="bearer token file", maximum=16 * 1024)
        tls_ca = _absolute_path(value.get("tls_ca_file"), label="TLS CA file")
        tls_certificate = _absolute_path(
            value.get("tls_certificate_file"), label="TLS certificate file"
        )
        tls_private_key = _absolute_path(
            value.get("tls_private_key_file"), label="TLS private-key file"
        )
        for path, label in (
            (tls_ca, "TLS CA file"),
            (tls_certificate, "TLS certificate file"),
            (tls_private_key, "TLS private-key file"),
        ):
            _private_regular(path, label=label, maximum=_MAX_CONFIG_BYTES)
        state = _absolute_path(value.get("state_directory"), label="state directory")
        _private_state_directory(state)
        journal = _absolute_path(value.get("journal_file"), label="journal file")
        if journal.parent != state:
            raise ExecutorConfigError("journal file must be directly inside the state directory")
        # These explicit values preserve the controller/partition/association and executable
        # authority even while the checked-in daemon is deliberately unable to scale up.
        retained: dict[str, str] = {}
        for field in (
            "slurm_cluster",
            "controller_host",
            "partition",
            "association",
            "submitter",
            "qos",
            "profile_id",
        ):
            if field not in value:
                raise ExecutorConfigError(f"{field.replace('_', ' ')} is required")
            retained[field] = _identifier(value[field], label=field.replace("_", " "))
        profile_generation = value.get("profile_generation")
        if type(profile_generation) is not int or profile_generation <= 0:
            raise ExecutorConfigError("profile generation is invalid")
        profile_digest = _digest(value.get("profile_digest"), label="profile digest")
        executor_image = value.get("executor_image", "legacy-unbound")
        if executor_image != "legacy-unbound" and (
            not isinstance(executor_image, str) or "@sha256:" not in executor_image
        ):
            raise ExecutorConfigError("executor image must be immutable")
        service_user = value.get("service_user", "legacy-unbound")
        if not isinstance(service_user, str) or not service_user or len(service_user) > 32:
            raise ExecutorConfigError("service user is invalid")
        executables = value.get("slurm_executables", {})
        if not isinstance(executables, dict):
            raise ExecutorConfigError("Slurm executables must be an object")
        expected_executables = {"scontrol", "sacctmgr", "squeue", "sbatch", "scancel", "sacct"}
        if set(executables) != expected_executables:
            raise ExecutorConfigError("Slurm executable binding is incomplete")
        executable_paths: list[tuple[str, Path]] = []
        for name, executable in executables.items():
            _identifier(name, label="Slurm executable name")
            executable_paths.append((name, _absolute_path(executable, label="Slurm executable")))
        try:
            execution = ExecutionContextV2(
                authority_incarnation=authority_incarnation,
                writer_epoch=value.get("writer_epoch", 1),
                configuration_epoch=value.get("configuration_epoch", 1),
                execution_epoch=value.get("execution_epoch", 1),
                execution_manifest_sha256=_digest(
                    value.get("execution_manifest_sha256", "0" * 64), label="execution manifest"
                ),
                execution_state="prepared",
                executable_new_capacity_ceiling=0,
                executable_new_capacity_rate_per_minute=0,
                trusted_fleet_release_sha256=_digest(
                    value.get("trusted_fleet_release_sha256", "0" * 64), label="fleet release"
                ),
            )
        except (TypeError, ValueError) as exc:
            raise ExecutorConfigError("zero-ceiling execution authority is invalid") from exc
        manifest = ImmutablePoolManifest(
            pool_id=pool,
            pool_generation=pool_generation,
            controller_authority_sha256=controller_digest,
            approved_profiles_sha256=approved_profiles_digest,
            executor_id=executor_id,
            executor_incarnation=incarnation,
            local_authority_sha256=local_digest,
            signing_key_id=signing_key_id,
            signing_key_sha256=signing_key_sha,
            ownership_key_file=ownership_key,
            manager_origin=manager_origin,
            bearer_token_file=bearer,
            tls_ca_file=tls_ca,
            tls_certificate_file=tls_certificate,
            tls_private_key_file=tls_private_key,
            state_directory=state,
            journal_file=journal,
            local_uid=local_uid,
            slurm_cluster=retained["slurm_cluster"],
            controller_host=retained["controller_host"],
            partition=retained["partition"],
            association=retained["association"],
            submitter=retained["submitter"],
            qos=retained["qos"],
            profile_id=retained["profile_id"],
            profile_generation=profile_generation,
            profile_digest=profile_digest,
            slurm_executables=tuple(sorted(executable_paths)),
            executor_image=executor_image,
            service_user=service_user,
        )
        if expected_manifest_sha256 is not None and manifest.sha256() != _digest(
            expected_manifest_sha256, label="expected manifest"
        ):
            raise ExecutorConfigError("configuration differs from pinned immutable manifest")
        return cls(
            pool_id=pool,
            pool_generation=pool_generation,
            executor_id=executor_id,
            executor_incarnation=incarnation,
            controller_authority_sha256=controller_digest,
            approved_profiles_sha256=approved_profiles_digest,
            local_authority_sha256=local_digest,
            signing_key_id=signing_key_id,
            signing_key_sha256=signing_key_sha,
            ownership_key_file=ownership_key,
            ownership_key=loaded_key,
            manager_origin=manager_origin,
            local_uid=local_uid,
            bearer_token_file=bearer,
            tls_ca_file=tls_ca,
            tls_certificate_file=tls_certificate,
            tls_private_key_file=tls_private_key,
            state_directory=state,
            journal_file=journal,
            slurm_cluster=retained["slurm_cluster"],
            controller_host=retained["controller_host"],
            partition=retained["partition"],
            association=retained["association"],
            submitter=retained["submitter"],
            qos=retained["qos"],
            profile_id=retained["profile_id"],
            profile_generation=profile_generation,
            profile_digest=profile_digest,
            executor_image=executor_image,
            service_user=service_user,
            manifest=manifest,
            expected_manifest_sha256=expected_manifest_sha256,
            execution=execution,
        )

    def assert_pool(self, pool_id: str) -> None:
        if pool_id != self.pool_id:
            raise ExecutorConfigError("pool binding differs from controller-local configuration")

    def assert_inventory_policy_binding(
        self,
        *,
        pool_id: str,
        pool_generation: int,
        query_uid: int,
        controller_cluster: str,
        relevant_partitions: tuple[str, ...],
    ) -> None:
        """Require one read-only inventory policy to match this local identity."""

        if (
            pool_id != self.pool_id
            or type(pool_generation) is not int
            or pool_generation != self.pool_generation
            or type(query_uid) is not int
            or query_uid != self.local_uid
            or query_uid != os.geteuid()
            or controller_cluster != self.slurm_cluster
            or self.partition not in relevant_partitions
        ):
            raise ExecutorConfigError(
                "inventory policy differs from exact controller-local binding"
            )

    @property
    def registration(self) -> ExecutableExecutorRegistrationV2:
        return ExecutableExecutorRegistrationV2(
            execution=self.execution,
            executor_id=self.executor_id,
            executor_incarnation=self.executor_incarnation,
            pool_id=self.pool_id,
            pool_generation=self.pool_generation,
            signing_key_id=self.signing_key_id,
            signing_key_sha256=self.signing_key_sha256,
            local_authority_sha256=self.local_authority_sha256,
            controller_authority_sha256=self.controller_authority_sha256,
        )

    @property
    def controller_authority(self) -> PoolControllerAuthorityV2:
        return PoolControllerAuthorityV2(
            pool_id=self.pool_id,
            controller_authority_sha256=self.controller_authority_sha256,
        )


__all__ = ["ExecutorConfigError", "ImmutablePoolManifest", "PoolExecutorConfig"]
