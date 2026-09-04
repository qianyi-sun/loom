"""Protected zero-ceiling execution preparation and controller staging."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Literal, Protocol
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import ValidationError

from loom_capacity_manager.executable_contracts import (
    ExecutionContextV2,
    ExecutionPreparationAbortV2,
    ExecutionPreparationV2,
    canonical_executable_digest,
)
from loom_capacity_manager.preparation_readiness import (
    PreparedExecutionReadinessV2,
    canonical_prepared_readiness_digest,
)
from loom_cli.capacity_control_plane import (
    CapacityPoolExecutorProfile,
    render_capacity_pool_executor_configs,
    render_capacity_pool_executor_service_environment,
    render_capacity_pool_inventory_policies,
)

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import ComponentState
from .protected_capacity_manager_client import (
    ProtectedCapacityManagerClientError,
    ProtectedExecutionPreparationAbortResult,
    ProtectedExecutionPreparationStatus,
)
from .protected_controller_prerequisite_component import (
    ControllerPrerequisiteRequest,
    KubernetesProtectedControllerPrerequisiteComponent,
    ProtectedControllerPrerequisiteTransport,
)
from .protected_execution_preparation_journal import (
    ExecutionPreparationOperationIntent,
    ExecutionPreparationOperationJournal,
    ExecutionPreparationOperationTerminal,
    ExecutionPreparationRecoveryState,
)
from .protected_execution_prerequisites import ProtectedExecutionPrerequisiteArtifact

_POOL_ORDER: tuple[Literal["gb10", "oldlab"], Literal["gb10", "oldlab"]] = (
    "gb10",
    "oldlab",
)
_UNITS = (
    "loom-capacity-pool-executor.service",
    "loom-capacity-pool-executor-prepared.service",
    "loom-capacity-pool-executor-prepared.timer",
    "loom-capacity-pool-executor-active.service",
    "loom-capacity-pool-executor-active.timer",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_PROFILE_BYTES = 2 * 1024 * 1024
_MAX_CONTROLLER_REQUEST_BYTES = 4 * 1024 * 1024
_MAX_CONTROLLER_EVIDENCE_BYTES = 2 * 1024 * 1024


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")


def _digest(value: object, *, allow_zero: bool = False) -> str:
    if (
        not isinstance(value, str)
        or _SHA256_RE.fullmatch(value) is None
        or (not allow_zero and value == "0" * 64)
    ):
        raise ValueError("protected execution preparation digest is invalid")
    return value


def canonical_prepared_executor_profile_bytes(profile: CapacityPoolExecutorProfile) -> bytes:
    if not isinstance(profile, CapacityPoolExecutorProfile):
        raise TypeError("prepared executor profile is invalid")
    return _canonical_json(profile.model_dump(mode="json", exclude_none=False))


def prepared_executor_profile_sha256(profile: CapacityPoolExecutorProfile) -> str:
    return hashlib.sha256(canonical_prepared_executor_profile_bytes(profile)).hexdigest()


@dataclass(frozen=True, slots=True)
class PreparedExecutorProfilePublication:
    path: Path
    profile_sha256: str

    def __post_init__(self) -> None:
        _digest(self.profile_sha256)
        if (
            not self.path.is_absolute()
            or ".." in self.path.parts
            or self.path.name != f"{self.profile_sha256}.json"
        ):
            raise ValueError("prepared executor profile publication is invalid")


class PreparedExecutorProfileStore:
    """Durably publish one immutable, digest-addressed prepared profile."""

    def __init__(self, state_root: Path, *, service_uid: int) -> None:
        self.state_root = state_root
        self.root = state_root / "protected-capacity" / "prepared-executor-profiles"
        self.service_uid = service_uid
        if (
            not isinstance(state_root, Path)
            or not state_root.is_absolute()
            or ".." in state_root.parts
            or type(service_uid) is not int
            or service_uid < 0
        ):
            raise ValueError("prepared executor profile store authority is invalid")

    def publication_for(
        self,
        profile: CapacityPoolExecutorProfile,
    ) -> PreparedExecutorProfilePublication:
        digest = prepared_executor_profile_sha256(profile)
        return PreparedExecutorProfilePublication(
            path=self.root / f"{digest}.json",
            profile_sha256=digest,
        )

    def observe(
        self,
        profile: CapacityPoolExecutorProfile,
    ) -> PreparedExecutorProfilePublication | None:
        publication = self.publication_for(profile)
        try:
            publication.path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise RuntimeError("prepared executor profile is unavailable") from exc
        if self.read(publication) != profile:
            raise RuntimeError("prepared executor profile digest collision")
        return publication

    def publish(
        self,
        profile: CapacityPoolExecutorProfile,
    ) -> PreparedExecutorProfilePublication:
        payload = canonical_prepared_executor_profile_bytes(profile)
        if not 0 < len(payload) <= _MAX_PROFILE_BYTES:
            raise RuntimeError("prepared executor profile is too large")
        try:
            parsed = CapacityPoolExecutorProfile.model_validate_json(payload)
        except ValidationError as exc:
            raise RuntimeError("prepared executor profile is invalid") from exc
        if parsed != profile:
            raise RuntimeError("prepared executor profile round-trip drifted")
        self._ensure()
        publication = self.publication_for(profile)
        existing = self.observe(profile)
        if existing is not None:
            return existing
        temporary_name = f".{publication.path.name}.{uuid4().hex}.tmp"
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        directory = os.open(
            self.root,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        descriptor: int | None = None
        try:
            metadata = os.fstat(directory)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != self.service_uid
                or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
            ):
                raise RuntimeError("prepared executor profile directory is unsafe")
            descriptor = os.open(
                temporary_name,
                flags,
                _PRIVATE_FILE_MODE,
                dir_fd=directory,
            )
            os.fchmod(descriptor, _PRIVATE_FILE_MODE)
            offset = 0
            while offset < len(payload):
                written = os.write(descriptor, payload[offset:])
                if written <= 0:
                    raise RuntimeError("prepared executor profile write was incomplete")
                offset += written
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            try:
                os.link(
                    temporary_name,
                    publication.path.name,
                    src_dir_fd=directory,
                    dst_dir_fd=directory,
                    follow_symlinks=False,
                )
            except FileExistsError:
                existing = self.observe(profile)
                if existing is None:
                    raise RuntimeError("prepared executor profile publication raced") from None
            os.unlink(temporary_name, dir_fd=directory)
            os.fsync(directory)
        except RuntimeError:
            raise
        except OSError as exc:
            raise RuntimeError("prepared executor profile could not be published") from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                os.unlink(temporary_name, dir_fd=directory)
            except FileNotFoundError:
                pass
            finally:
                os.close(directory)
        if self.read(publication) != profile:
            raise RuntimeError("prepared executor profile did not converge")
        return publication

    def read(
        self,
        publication: PreparedExecutorProfilePublication,
    ) -> CapacityPoolExecutorProfile:
        if (
            not isinstance(publication, PreparedExecutorProfilePublication)
            or publication.path.parent != self.root
        ):
            raise RuntimeError("prepared executor profile publication is invalid")
        self._validate_directories()
        try:
            descriptor = os.open(
                publication.path,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
        except OSError as exc:
            raise RuntimeError("prepared executor profile is unavailable") from exc
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or stat.S_IMODE(before.st_mode) != _PRIVATE_FILE_MODE
                or before.st_uid != self.service_uid
                or before.st_nlink != 1
                or not 0 < before.st_size <= _MAX_PROFILE_BYTES
            ):
                raise RuntimeError("prepared executor profile metadata is unsafe")
            chunks: list[bytes] = []
            remaining = before.st_size
            while remaining:
                chunk = os.read(descriptor, min(64 * 1024, remaining))
                if not chunk:
                    raise RuntimeError("prepared executor profile changed while reading")
                chunks.append(chunk)
                remaining -= len(chunk)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        payload = b"".join(chunks)
        if _metadata_identity(before) != _metadata_identity(after):
            raise RuntimeError("prepared executor profile changed while reading")
        if hashlib.sha256(payload).hexdigest() != publication.profile_sha256:
            raise RuntimeError("prepared executor profile digest is invalid")
        try:
            profile = CapacityPoolExecutorProfile.model_validate_json(payload)
        except ValidationError as exc:
            raise RuntimeError("prepared executor profile is invalid") from exc
        if canonical_prepared_executor_profile_bytes(profile) != payload:
            raise RuntimeError("prepared executor profile is not canonical")
        return profile

    def _ensure(self) -> None:
        _ensure_private_directory(self.state_root, service_uid=self.service_uid, parents=True)
        _ensure_private_directory(
            self.state_root / "protected-capacity",
            service_uid=self.service_uid,
        )
        _ensure_private_directory(self.root, service_uid=self.service_uid)

    def _validate_directories(self) -> None:
        for path in (
            self.state_root,
            self.state_root / "protected-capacity",
            self.root,
        ):
            _validate_private_directory(path, service_uid=self.service_uid)


@dataclass(frozen=True, slots=True)
class PreparedControllerRequest:
    """One pool's non-secret prepared files bound to its inert prerequisite."""

    schema_version: Literal[1]
    pool_id: Literal["gb10", "oldlab"]
    transport_authority_sha256: str
    prerequisite: ControllerPrerequisiteRequest
    execution: ExecutionContextV2
    profile_sha256: str
    files: Mapping[str, bytes]

    def __post_init__(self) -> None:
        _digest(self.transport_authority_sha256)
        _digest(self.profile_sha256)
        if (
            self.schema_version != 1
            or self.pool_id not in set(_POOL_ORDER)
            or not isinstance(self.prerequisite, ControllerPrerequisiteRequest)
            or self.prerequisite.pool_id != self.pool_id
            or self.prerequisite.transport_authority_sha256 != self.transport_authority_sha256
            or not isinstance(self.execution, ExecutionContextV2)
            or self.execution.execution_state != "prepared"
            or self.execution.executable_new_capacity_ceiling != 0
            or self.execution.executable_new_capacity_rate_per_minute != 0
            or not isinstance(self.files, Mapping)
        ):
            raise ValueError("prepared controller request authority is invalid")
        config_path = self.prerequisite.binding.config_file
        expected_paths = {
            config_path,
            str(Path(config_path).with_name(f"{self.pool_id}-inventory-policy.json")),
            "/etc/loom-capacity-executor/service.env",
        }
        copied = dict(self.files)
        if set(copied) != expected_paths or any(
            not isinstance(path, str)
            or not isinstance(payload, bytes)
            or not payload
            or len(payload) > _MAX_CONTROLLER_REQUEST_BYTES
            for path, payload in copied.items()
        ):
            raise ValueError("prepared controller file set is invalid")
        try:
            config = json.loads(copied[config_path])
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("prepared controller configuration is invalid") from exc
        binding = self.prerequisite.binding
        if (
            not isinstance(config, dict)
            or config.get("pool_id") != self.pool_id
            or config.get("executor_id") != binding.executor_id
            or config.get("executor_incarnation") != binding.executor_incarnation
            or config.get("pool_generation") != binding.pool_generation
            or config.get("controller_authority_sha256") != binding.controller_authority_sha256
            or config.get("local_authority_sha256") != binding.local_authority_sha256
            or config.get("signing_key_sha256") != binding.signing_key_sha256
            or config.get("authority_incarnation") != str(self.execution.authority_incarnation)
            or config.get("writer_epoch") != self.execution.writer_epoch
            or config.get("configuration_epoch") != self.execution.configuration_epoch
            or config.get("execution_epoch") != self.execution.execution_epoch
            or config.get("execution_manifest_sha256") != self.execution.execution_manifest_sha256
            or config.get("trusted_fleet_release_sha256")
            != self.execution.trusted_fleet_release_sha256
            or config.get("approved_profiles_sha256") != "0" * 64
        ):
            raise ValueError("prepared controller configuration binding is invalid")
        object.__setattr__(self, "files", MappingProxyType(dict(sorted(copied.items()))))

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(self.to_bytes()).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "execution": self.execution.model_dump(mode="json", exclude_none=False),
            "files": {
                path: base64.b64encode(payload).decode("ascii")
                for path, payload in self.files.items()
            },
            "pool_id": self.pool_id,
            "prerequisite": json.loads(self.prerequisite.to_bytes()),
            "profile_sha256": self.profile_sha256,
            "schema_version": self.schema_version,
            "transport_authority_sha256": self.transport_authority_sha256,
        }

    def to_bytes(self) -> bytes:
        payload = _canonical_json(self.to_dict())
        if len(payload) > _MAX_CONTROLLER_REQUEST_BYTES:
            raise ValueError("prepared controller request is too large")
        return payload

    @classmethod
    def from_bytes(cls, payload: bytes) -> PreparedControllerRequest:
        if (
            not isinstance(payload, bytes)
            or not 0 < len(payload) <= _MAX_CONTROLLER_REQUEST_BYTES
            or not payload.endswith(b"\n")
        ):
            raise ValueError("prepared controller request bytes are invalid")
        try:
            value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("prepared controller request bytes are invalid") from exc
        if not isinstance(value, dict) or set(value) != {
            "execution",
            "files",
            "pool_id",
            "prerequisite",
            "profile_sha256",
            "schema_version",
            "transport_authority_sha256",
        }:
            raise ValueError("prepared controller request fields are invalid")
        files = value["files"]
        if not isinstance(files, dict) or any(
            not isinstance(path, str) or not isinstance(encoded, str)
            for path, encoded in files.items()
        ):
            raise ValueError("prepared controller request files are invalid")
        try:
            decoded = {
                path: base64.b64decode(encoded, validate=True) for path, encoded in files.items()
            }
            request = cls(
                schema_version=value["schema_version"],
                pool_id=value["pool_id"],
                transport_authority_sha256=value["transport_authority_sha256"],
                prerequisite=ControllerPrerequisiteRequest.from_bytes(
                    _canonical_json(value["prerequisite"])
                ),
                execution=ExecutionContextV2.model_validate_json(
                    json.dumps(value["execution"], sort_keys=True, separators=(",", ":"))
                ),
                profile_sha256=value["profile_sha256"],
                files=decoded,
            )
        except (binascii.Error, TypeError, ValidationError, ValueError) as exc:
            raise ValueError("prepared controller request is invalid") from exc
        if request.to_bytes() != payload:
            raise ValueError("prepared controller request is not canonical")
        return request


@dataclass(frozen=True, slots=True)
class PreparedControllerEvidence:
    """Exact controller readback for prepared files, units, and one safe tick."""

    schema_version: Literal[1]
    pool_id: Literal["gb10", "oldlab"]
    transport_authority_sha256: str
    request_sha256: str
    file_sha256: Mapping[str, str]
    unit_active_state: Mapping[str, str]
    unit_file_state: Mapping[str, str]
    successful_tick: bool
    tick_evidence_sha256: str | None

    def __post_init__(self) -> None:
        _digest(self.transport_authority_sha256)
        _digest(self.request_sha256)
        files = dict(self.file_sha256)
        active = dict(self.unit_active_state)
        enabled = dict(self.unit_file_state)
        if (
            type(self.schema_version) is not int
            or self.schema_version != 1
            or self.pool_id not in set(_POOL_ORDER)
            or not isinstance(self.file_sha256, Mapping)
            or not isinstance(self.unit_active_state, Mapping)
            or not isinstance(self.unit_file_state, Mapping)
            or not files
            or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in files.items()
            )
            or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in active.items()
            )
            or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in enabled.items()
            )
            or set(active) != set(_UNITS)
            or set(enabled) != set(_UNITS)
            or type(self.successful_tick) is not bool
            or (self.successful_tick != (self.tick_evidence_sha256 is not None))
        ):
            raise ValueError("prepared controller evidence is invalid")
        for value in files.values():
            _digest(value)
        if self.tick_evidence_sha256 is not None:
            _digest(self.tick_evidence_sha256)
        prepared_timer = "loom-capacity-pool-executor-prepared.timer"
        active_timer = "loom-capacity-pool-executor-active.timer"
        if (
            any(active[unit] != "inactive" for unit in _UNITS if not unit.endswith(".timer"))
            or active[active_timer] != "inactive"
            or enabled[active_timer] != "disabled"
            or any(enabled[unit] != "static" for unit in _UNITS if not unit.endswith(".timer"))
            or (active[prepared_timer], enabled[prepared_timer])
            not in {("inactive", "disabled"), ("active", "enabled")}
            or (
                self.successful_tick
                and (active[prepared_timer], enabled[prepared_timer]) != ("active", "enabled")
            )
        ):
            raise ValueError("prepared controller units are invalid")
        object.__setattr__(self, "file_sha256", MappingProxyType(dict(sorted(files.items()))))
        object.__setattr__(
            self,
            "unit_active_state",
            MappingProxyType(dict(sorted(active.items()))),
        )
        object.__setattr__(
            self,
            "unit_file_state",
            MappingProxyType(dict(sorted(enabled.items()))),
        )

    def to_bytes(self) -> bytes:
        payload = _canonical_json(
            {
                "file_sha256": dict(self.file_sha256),
                "pool_id": self.pool_id,
                "request_sha256": self.request_sha256,
                "schema_version": self.schema_version,
                "successful_tick": self.successful_tick,
                "tick_evidence_sha256": self.tick_evidence_sha256,
                "transport_authority_sha256": self.transport_authority_sha256,
                "unit_active_state": dict(self.unit_active_state),
                "unit_file_state": dict(self.unit_file_state),
            }
        )
        if len(payload) > _MAX_CONTROLLER_EVIDENCE_BYTES:
            raise ValueError("prepared controller evidence is too large")
        return payload

    @classmethod
    def from_bytes(cls, payload: bytes) -> PreparedControllerEvidence:
        if (
            not isinstance(payload, bytes)
            or not 0 < len(payload) <= _MAX_CONTROLLER_EVIDENCE_BYTES
            or not payload.endswith(b"\n")
        ):
            raise ValueError("prepared controller evidence bytes are invalid")
        try:
            value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("prepared controller evidence bytes are invalid") from exc
        if not isinstance(value, dict) or set(value) != {
            "file_sha256",
            "pool_id",
            "request_sha256",
            "schema_version",
            "successful_tick",
            "tick_evidence_sha256",
            "transport_authority_sha256",
            "unit_active_state",
            "unit_file_state",
        }:
            raise ValueError("prepared controller evidence fields are invalid")
        maps = (
            value["file_sha256"],
            value["unit_active_state"],
            value["unit_file_state"],
        )
        if any(
            not isinstance(item, dict)
            or any(
                not isinstance(key, str) or not isinstance(entry, str)
                for key, entry in item.items()
            )
            for item in maps
        ):
            raise ValueError("prepared controller evidence fields are invalid")
        try:
            evidence = cls(
                schema_version=value["schema_version"],
                pool_id=value["pool_id"],
                transport_authority_sha256=value["transport_authority_sha256"],
                request_sha256=value["request_sha256"],
                file_sha256=value["file_sha256"],
                unit_active_state=value["unit_active_state"],
                unit_file_state=value["unit_file_state"],
                successful_tick=value["successful_tick"],
                tick_evidence_sha256=value["tick_evidence_sha256"],
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("prepared controller evidence is invalid") from exc
        if evidence.to_bytes() != payload:
            raise ValueError("prepared controller evidence is not canonical")
        return evidence


class PreparedControllerTransport(Protocol):
    def observe(self, request: PreparedControllerRequest) -> PreparedControllerEvidence | None: ...

    def converge_files(self, request: PreparedControllerRequest) -> PreparedControllerEvidence: ...

    def enable_timer(self, request: PreparedControllerRequest) -> PreparedControllerEvidence: ...

    def run_tick(self, request: PreparedControllerRequest) -> PreparedControllerEvidence: ...

    def disable_timer(
        self,
        request: PreparedControllerRequest,
    ) -> PreparedControllerEvidence | None: ...


class ExecutionPreparationManagerClient(Protocol):
    def get_status(self) -> dict[str, object]: ...

    def prepare_execution(
        self,
        preparation: ExecutionPreparationV2,
        idempotency_key: UUID,
    ) -> ExecutionContextV2: ...

    def get_execution_preparation_status(self) -> ProtectedExecutionPreparationStatus: ...

    def abort_execution_preparation(
        self,
        abort: ExecutionPreparationAbortV2,
        idempotency_key: UUID,
    ) -> ProtectedExecutionPreparationAbortResult: ...


ExecutionPreparationClientContext = Callable[
    [], AbstractContextManager[ExecutionPreparationManagerClient]
]
ExecutionPreparationDependencyGuard = Callable[
    [FinalGatePlan, ProtectedExecutionPrerequisiteArtifact], str
]


@dataclass(frozen=True, slots=True)
class _ManagerExecutionStatus:
    authority_incarnation: UUID
    writer_epoch: int
    configuration_epoch: int
    configuration_digest: str
    execution_epoch: int
    execution_state: Literal["shadow", "prepared"]
    execution_manifest_sha256: str | None
    executable_new_capacity_ceiling: int
    increase_freeze: bool


@dataclass(frozen=True, slots=True)
class KubernetesProtectedCapacityExecutionPreparationComponent:
    """Prepare one zero-ceiling epoch and prove both prepared controllers."""

    state_root: Path
    service_uid: int
    client_context: ExecutionPreparationClientContext
    prerequisite_reader: Callable[[FinalGatePlan], ProtectedExecutionPrerequisiteArtifact]
    dependency_guard: ExecutionPreparationDependencyGuard
    controller_prerequisite_transports: Mapping[str, ProtectedControllerPrerequisiteTransport]
    prepared_controller_transports: Mapping[str, PreparedControllerTransport]

    def __post_init__(self) -> None:
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or type(self.service_uid) is not int
            or self.service_uid < 0
            or not callable(self.client_context)
            or not callable(self.prerequisite_reader)
            or not callable(self.dependency_guard)
            or set(self.controller_prerequisite_transports) != set(_POOL_ORDER)
            or set(self.prepared_controller_transports) != set(_POOL_ORDER)
        ):
            raise ValueError("protected execution preparation authority is invalid")
        object.__setattr__(
            self,
            "controller_prerequisite_transports",
            MappingProxyType(dict(self.controller_prerequisite_transports)),
        )
        object.__setattr__(
            self,
            "prepared_controller_transports",
            MappingProxyType(dict(self.prepared_controller_transports)),
        )

    def classify(self, plan: FinalGatePlan) -> tuple[ComponentState, str]:
        try:
            artifact = self._artifact(plan)
            dependency = self._dependency(plan, artifact)
            with self.client_context() as client:
                manager = _parse_manager_status(client.get_status(), artifact=artifact)
                status = client.get_execution_preparation_status()
            if manager.execution_state == "shadow":
                _require_shadow_readback(manager, status, artifact=artifact)
                return ComponentState.READY, _hash_json(
                    {"dependency": dependency, "state": "shadow"}
                )
            request = _preparation_request(manager, artifact=artifact)
            execution = _require_prepared_readback(
                manager,
                status,
                request=request,
                artifact=artifact,
            )
            profile = artifact.executor_profile_seed.realize(execution)
            store = self._profile_store()
            publication = store.observe(profile)
            if publication is None:
                return ComponentState.READY, _hash_json(
                    {"dependency": dependency, "state": "profile-missing"}
                )
            requests = self._controller_requests(plan, artifact, profile, publication)
            evidence = {
                pool_id: self.prepared_controller_transports[pool_id].observe(requests[pool_id])
                for pool_id in _POOL_ORDER
            }
            if not all(
                _controller_evidence_matches(
                    evidence[pool_id],
                    requests[pool_id],
                    require_timer=True,
                    require_tick=True,
                )
                for pool_id in _POOL_ORDER
            ) or not _readiness_is_exact(status, execution=execution, artifact=artifact):
                return ComponentState.READY, _hash_json(
                    {"dependency": dependency, "state": "prepared-incomplete"}
                )
            return ComponentState.EXACT, _hash_json(
                {
                    "dependency": dependency,
                    "profile_sha256": publication.profile_sha256,
                    "readiness_sha256": status.readiness_sha256,
                    "requests": {
                        pool_id: requests[pool_id].request_sha256 for pool_id in _POOL_ORDER
                    },
                    "state": "prepared-ready",
                }
            )
        except (
            KeyError,
            OSError,
            ProtectedCapacityManagerClientError,
            RuntimeError,
            TypeError,
            ValidationError,
            ValueError,
        ):
            return ComponentState.DRIFTED, _hash_json({"state": "observation-failed"})

    def apply(self, plan: FinalGatePlan) -> None:
        artifact = self._artifact(plan)
        journal = self._operation_journal(plan)
        recovery_state = journal.recovery_state(
            plan,
            artifact_sha256=artifact.artifact_sha256,
        )
        if recovery_state is ExecutionPreparationRecoveryState.UNRESOLVED:
            records = journal.records(
                plan,
                artifact_sha256=artifact.artifact_sha256,
            )
            abort_record = records.get("manager-abort")
            if abort_record is not None and abort_record[1] is None:
                self._dependency(plan, artifact)
                self._recover_open_abort(
                    plan=plan,
                    artifact=artifact,
                    journal=journal,
                    intent=abort_record[0],
                )
                recovery_state = journal.recovery_state(
                    plan,
                    artifact_sha256=artifact.artifact_sha256,
                )
        if recovery_state is ExecutionPreparationRecoveryState.COMPENSATED:
            raise RuntimeError(
                "protected execution preparation was compensated and requires fresh authority"
            )
        self._dependency(plan, artifact)
        execution: ExecutionContextV2 | None = None
        requests: dict[str, PreparedControllerRequest] = {}
        with self.client_context() as client:
            try:
                execution = self._prepare_or_resume(
                    client,
                    artifact=artifact,
                    plan=plan,
                    journal=journal,
                )
                self._dependency(plan, artifact)
                profile = artifact.executor_profile_seed.realize(execution)
                publication = self._profile_store().publish(profile)
                requests = self._controller_requests(
                    plan,
                    artifact,
                    profile,
                    publication,
                )
                for pool_id in _POOL_ORDER:
                    transport = self.prepared_controller_transports[pool_id]
                    intent = self._record_operation_intent(
                        journal,
                        plan=plan,
                        artifact=artifact,
                        operation=f"controller-files-{pool_id}",
                        request_sha256=requests[pool_id].request_sha256,
                        execution=execution,
                    )
                    observed = transport.observe(requests[pool_id])
                    terminal_recorded = self._operation_terminal_is_recorded(
                        journal,
                        intent=intent,
                        execution=execution,
                        result_state="prepared",
                    )
                    if terminal_recorded and not _controller_files_are_exact(
                        observed, requests[pool_id]
                    ):
                        raise RuntimeError(
                            "prepared controller terminal state drifted before file convergence"
                        )
                    if not terminal_recorded and not _controller_files_are_exact(
                        observed, requests[pool_id]
                    ):
                        transport.converge_files(requests[pool_id])
                    observed = transport.observe(requests[pool_id])
                    if not _controller_files_are_exact(observed, requests[pool_id]):
                        raise RuntimeError("prepared controller files did not converge exactly")
                    assert observed is not None
                    self._record_operation_terminal(
                        journal,
                        intent=intent,
                        evidence_sha256=hashlib.sha256(observed.to_bytes()).hexdigest(),
                        execution=execution,
                        result_state="prepared",
                    )
                self._dependency(plan, artifact)
                before_enable: dict[str, PreparedControllerEvidence] = {}
                for pool_id in _POOL_ORDER:
                    transport = self.prepared_controller_transports[pool_id]
                    observed = transport.observe(requests[pool_id])
                    if observed is None or not _controller_files_are_exact(
                        observed, requests[pool_id]
                    ):
                        raise RuntimeError(
                            "prepared controller files changed before timer enablement"
                        )
                    before_enable[pool_id] = observed
                for pool_id in _POOL_ORDER:
                    transport = self.prepared_controller_transports[pool_id]
                    observed = before_enable[pool_id]
                    intent = self._record_operation_intent(
                        journal,
                        plan=plan,
                        artifact=artifact,
                        operation=f"prepared-timer-{pool_id}",
                        request_sha256=requests[pool_id].request_sha256,
                        execution=execution,
                    )
                    terminal_recorded = self._operation_terminal_is_recorded(
                        journal,
                        intent=intent,
                        execution=execution,
                        result_state="prepared",
                    )
                    if terminal_recorded and not _controller_evidence_matches(
                        observed,
                        requests[pool_id],
                        require_timer=True,
                        require_tick=False,
                    ):
                        raise RuntimeError(
                            "prepared controller terminal state drifted before timer enablement"
                        )
                    if not terminal_recorded and not _controller_evidence_matches(
                        observed,
                        requests[pool_id],
                        require_timer=True,
                        require_tick=False,
                    ):
                        transport.enable_timer(requests[pool_id])
                    observed = transport.observe(requests[pool_id])
                    if not _controller_evidence_matches(
                        observed,
                        requests[pool_id],
                        require_timer=True,
                        require_tick=False,
                    ):
                        raise RuntimeError("prepared controller timer did not converge exactly")
                    assert observed is not None
                    self._record_operation_terminal(
                        journal,
                        intent=intent,
                        evidence_sha256=hashlib.sha256(observed.to_bytes()).hexdigest(),
                        execution=execution,
                        result_state="prepared",
                    )
                for pool_id in _POOL_ORDER:
                    transport = self.prepared_controller_transports[pool_id]
                    observed = transport.observe(requests[pool_id])
                    intent = self._record_operation_intent(
                        journal,
                        plan=plan,
                        artifact=artifact,
                        operation=f"prepared-tick-{pool_id}",
                        request_sha256=requests[pool_id].request_sha256,
                        execution=execution,
                    )
                    terminal_recorded = self._operation_terminal_is_recorded(
                        journal,
                        intent=intent,
                        execution=execution,
                        result_state="prepared",
                    )
                    if terminal_recorded and not _controller_evidence_matches(
                        observed,
                        requests[pool_id],
                        require_timer=True,
                        require_tick=True,
                    ):
                        raise RuntimeError("prepared controller terminal state drifted before tick")
                    if not terminal_recorded and not _controller_evidence_matches(
                        observed,
                        requests[pool_id],
                        require_timer=True,
                        require_tick=True,
                    ):
                        transport.run_tick(requests[pool_id])
                    observed = transport.observe(requests[pool_id])
                    if not _controller_evidence_matches(
                        observed,
                        requests[pool_id],
                        require_timer=True,
                        require_tick=True,
                    ):
                        raise RuntimeError("prepared controller tick did not converge exactly")
                    assert observed is not None
                    self._record_operation_terminal(
                        journal,
                        intent=intent,
                        evidence_sha256=hashlib.sha256(observed.to_bytes()).hexdigest(),
                        execution=execution,
                        result_state="prepared",
                    )
                final_status = client.get_execution_preparation_status()
                if not _readiness_is_exact(
                    final_status,
                    execution=execution,
                    artifact=artifact,
                ):
                    raise RuntimeError("prepared execution readiness did not converge exactly")
                self._dependency(plan, artifact)
            except BaseException as exc:
                if execution is None:
                    raise
                compensation_failures: list[BaseException] = []
                for pool_id in _POOL_ORDER:
                    request = requests.get(pool_id)
                    if request is None:
                        continue
                    try:
                        self.prepared_controller_transports[pool_id].disable_timer(request)
                    except BaseException as cleanup_exc:
                        compensation_failures.append(cleanup_exc)
                try:
                    self._abort_exact(
                        client,
                        execution=execution,
                        plan=plan,
                        artifact=artifact,
                        journal=journal,
                    )
                except BaseException as abort_exc:
                    compensation_failures.append(abort_exc)
                if compensation_failures:
                    raise RuntimeError(
                        "protected execution preparation compensation failed safely"
                    ) from exc
                raise

    def _prepare_or_resume(
        self,
        client: ExecutionPreparationManagerClient,
        *,
        artifact: ProtectedExecutionPrerequisiteArtifact,
        plan: FinalGatePlan,
        journal: ExecutionPreparationOperationJournal,
    ) -> ExecutionContextV2:
        manager = _parse_manager_status(client.get_status(), artifact=artifact)
        request = _preparation_request(manager, artifact=artifact)
        intent = self._record_operation_intent(
            journal,
            plan=plan,
            artifact=artifact,
            operation="manager-preparation",
            request_sha256=canonical_executable_digest(request),
            execution=None,
        )
        key = _idempotency_key(plan, "prepare", canonical_executable_digest(request))
        response: ExecutionContextV2 | None = None
        for attempt in range(2):
            if manager.execution_state == "prepared":
                status = client.get_execution_preparation_status()
                readback = _require_prepared_readback(
                    manager,
                    status,
                    request=request,
                    artifact=artifact,
                )
                self._record_operation_terminal(
                    journal,
                    intent=intent,
                    evidence_sha256=canonical_executable_digest(readback),
                    execution=readback,
                    result_state="prepared",
                )
                return readback
            try:
                response = client.prepare_execution(request, key)
            except ProtectedCapacityManagerClientError as exc:
                if exc.reason == "credential":
                    raise
                response = None
            manager = _parse_manager_status(client.get_status(), artifact=artifact)
            status = client.get_execution_preparation_status()
            if manager.execution_state == "prepared":
                readback = _require_prepared_readback(
                    manager,
                    status,
                    request=request,
                    artifact=artifact,
                )
                if response is not None and response != readback:
                    raise RuntimeError("prepared execution response and readback diverged")
                self._record_operation_terminal(
                    journal,
                    intent=intent,
                    evidence_sha256=canonical_executable_digest(readback),
                    execution=readback,
                    result_state="prepared",
                )
                return readback
            _require_shadow_readback(manager, status, artifact=artifact)
            if response is not None:
                raise RuntimeError("execution preparation response was not durable")
            if attempt == 1:
                break
        raise RuntimeError("execution preparation outcome is unresolved")

    def _abort_exact(
        self,
        client: ExecutionPreparationManagerClient,
        *,
        execution: ExecutionContextV2,
        plan: FinalGatePlan,
        artifact: ProtectedExecutionPrerequisiteArtifact,
        journal: ExecutionPreparationOperationJournal,
    ) -> None:
        abort = ExecutionPreparationAbortV2(
            authority_incarnation=execution.authority_incarnation,
            expected_writer_epoch=execution.writer_epoch,
            execution_epoch=execution.execution_epoch,
            execution_manifest_sha256=execution.execution_manifest_sha256,
        )
        intent = self._record_operation_intent(
            journal,
            plan=plan,
            artifact=artifact,
            operation="manager-abort",
            request_sha256=canonical_executable_digest(abort),
            execution=execution,
        )
        result: ProtectedExecutionPreparationAbortResult | None = None
        try:
            result = client.abort_execution_preparation(
                abort,
                _idempotency_key(plan, "abort", canonical_executable_digest(abort)),
            )
        except ProtectedCapacityManagerClientError as exc:
            if exc.reason == "credential":
                raise
        if result is not None and (
            result.execution_epoch != abort.execution_epoch
            or result.execution_manifest_sha256 != abort.execution_manifest_sha256
        ):
            raise RuntimeError("prepared execution abort response diverged")
        manager = _parse_manager_status(client.get_status(), artifact=artifact)
        status = client.get_execution_preparation_status()
        if manager.execution_state != "shadow":
            raise RuntimeError("prepared execution abort outcome is unresolved")
        _require_shadow_readback(manager, status, artifact=artifact)
        self._record_operation_terminal(
            journal,
            intent=intent,
            evidence_sha256=_hash_json(
                {
                    "execution_epoch": execution.execution_epoch,
                    "execution_manifest_sha256": execution.execution_manifest_sha256,
                    "result_state": "shadow",
                    "writer_epoch": manager.writer_epoch,
                }
            ),
            execution=execution,
            result_state="shadow",
        )

    def _recover_open_abort(
        self,
        *,
        plan: FinalGatePlan,
        artifact: ProtectedExecutionPrerequisiteArtifact,
        journal: ExecutionPreparationOperationJournal,
        intent: ExecutionPreparationOperationIntent,
    ) -> None:
        execution_epoch = intent.prepared_execution_epoch
        execution_manifest_sha256 = intent.prepared_execution_manifest_sha256
        if (
            intent.operation != "manager-abort"
            or type(execution_epoch) is not int
            or execution_epoch < 1
            or not isinstance(execution_manifest_sha256, str)
        ):
            raise RuntimeError("execution preparation abort recovery intent is invalid")
        with self.client_context() as client:
            manager = _parse_manager_status(client.get_status(), artifact=artifact)
            status = client.get_execution_preparation_status()
            if manager.execution_state == "shadow":
                _require_shadow_readback(manager, status, artifact=artifact)
                journal.record_terminal(
                    ExecutionPreparationOperationTerminal.build(
                        intent=intent,
                        evidence_sha256=_hash_json(
                            {
                                "execution_epoch": execution_epoch,
                                "execution_manifest_sha256": execution_manifest_sha256,
                                "result_state": "shadow",
                                "writer_epoch": manager.writer_epoch,
                            }
                        ),
                        prepared_execution_epoch=execution_epoch,
                        prepared_execution_manifest_sha256=(execution_manifest_sha256),
                        result_state="shadow",
                    )
                )
                return
            if (
                manager.execution_epoch != execution_epoch
                or manager.execution_manifest_sha256 != execution_manifest_sha256
            ):
                raise RuntimeError("execution preparation abort recovery authority drifted")
            execution = ExecutionContextV2(
                authority_incarnation=manager.authority_incarnation,
                writer_epoch=manager.writer_epoch,
                configuration_epoch=manager.configuration_epoch,
                execution_epoch=manager.execution_epoch,
                execution_manifest_sha256=execution_manifest_sha256,
                execution_state="prepared",
                executable_new_capacity_ceiling=0,
                executable_new_capacity_rate_per_minute=0,
                trusted_fleet_release_sha256=(
                    artifact.execution_policy.trusted_fleet_release_sha256
                ),
            )
            self._abort_exact(
                client,
                execution=execution,
                plan=plan,
                artifact=artifact,
                journal=journal,
            )

    def _operation_journal(
        self,
        plan: FinalGatePlan,
    ) -> ExecutionPreparationOperationJournal:
        return ExecutionPreparationOperationJournal(
            self.state_root,
            request_id=plan.request_id,
            attempt_number=plan.attempt_number,
            service_uid=self.service_uid,
        )

    @staticmethod
    def _record_operation_intent(
        journal: ExecutionPreparationOperationJournal,
        *,
        plan: FinalGatePlan,
        artifact: ProtectedExecutionPrerequisiteArtifact,
        operation: str,
        request_sha256: str,
        execution: ExecutionContextV2 | None,
    ) -> ExecutionPreparationOperationIntent:
        intent = ExecutionPreparationOperationIntent.build(
            plan=plan,
            artifact_sha256=artifact.artifact_sha256,
            operation=operation,
            request_sha256=request_sha256,
            prepared_execution_epoch=(None if execution is None else execution.execution_epoch),
            prepared_execution_manifest_sha256=(
                None if execution is None else execution.execution_manifest_sha256
            ),
        )
        journal.record_intent(intent)
        return intent

    @staticmethod
    def _record_operation_terminal(
        journal: ExecutionPreparationOperationJournal,
        *,
        intent: ExecutionPreparationOperationIntent,
        evidence_sha256: str,
        execution: ExecutionContextV2,
        result_state: Literal["prepared", "shadow"],
    ) -> None:
        if KubernetesProtectedCapacityExecutionPreparationComponent._operation_terminal_is_recorded(
            journal,
            intent=intent,
            execution=execution,
            result_state=result_state,
        ):
            return
        journal.record_terminal(
            ExecutionPreparationOperationTerminal.build(
                intent=intent,
                evidence_sha256=evidence_sha256,
                prepared_execution_epoch=execution.execution_epoch,
                prepared_execution_manifest_sha256=(execution.execution_manifest_sha256),
                result_state=result_state,
            )
        )

    @staticmethod
    def _operation_terminal_is_recorded(
        journal: ExecutionPreparationOperationJournal,
        *,
        intent: ExecutionPreparationOperationIntent,
        execution: ExecutionContextV2,
        result_state: Literal["prepared", "shadow"],
    ) -> bool:
        try:
            existing = journal.read_terminal(intent.operation)
        except FileNotFoundError:
            return False
        if (
            existing.intent_sha256 != intent.intent_sha256
            or existing.prepared_execution_epoch != execution.execution_epoch
            or existing.prepared_execution_manifest_sha256 != execution.execution_manifest_sha256
            or existing.result_state != result_state
        ):
            raise RuntimeError("execution preparation operation terminal drifted")
        return True

    def _controller_requests(
        self,
        plan: FinalGatePlan,
        artifact: ProtectedExecutionPrerequisiteArtifact,
        profile: CapacityPoolExecutorProfile,
        publication: PreparedExecutorProfilePublication,
    ) -> dict[str, PreparedControllerRequest]:
        configs = render_capacity_pool_executor_configs(profile)
        policies = render_capacity_pool_inventory_policies(profile)
        requests: dict[str, PreparedControllerRequest] = {}
        for pool_id in _POOL_ORDER:

            def prerequisite_reader(
                _plan: FinalGatePlan,
                bound_artifact: ProtectedExecutionPrerequisiteArtifact = artifact,
            ) -> ProtectedExecutionPrerequisiteArtifact:
                return bound_artifact

            component = KubernetesProtectedControllerPrerequisiteComponent(
                pool_id=pool_id,
                transport=self.controller_prerequisite_transports[pool_id],
                prerequisite_reader=prerequisite_reader,
            )
            if component.classify(plan)[0] is not ComponentState.EXACT:
                raise RuntimeError("controller prerequisite changed before preparation")
            prerequisite = component._request(plan, artifact)
            config_path = prerequisite.binding.config_file
            request = PreparedControllerRequest(
                schema_version=1,
                pool_id=pool_id,
                transport_authority_sha256=prerequisite.transport_authority_sha256,
                prerequisite=prerequisite,
                execution=profile_execution_context(profile),
                profile_sha256=publication.profile_sha256,
                files={
                    config_path: configs[pool_id].encode("ascii"),
                    str(Path(config_path).with_name(f"{pool_id}-inventory-policy.json")): policies[
                        pool_id
                    ].encode("ascii"),
                    "/etc/loom-capacity-executor/service.env": (
                        render_capacity_pool_executor_service_environment(
                            profile,
                            pool_id,
                        ).encode("ascii")
                    ),
                },
            )
            if self.prepared_controller_transports[pool_id] is None:
                raise RuntimeError("prepared controller transport is unavailable")
            requests[pool_id] = request
        return requests

    def _artifact(self, plan: FinalGatePlan) -> ProtectedExecutionPrerequisiteArtifact:
        artifact = self.prerequisite_reader(plan)
        if not isinstance(artifact, ProtectedExecutionPrerequisiteArtifact):
            raise ValueError("protected execution prerequisite is invalid")
        return artifact

    def _dependency(
        self,
        plan: FinalGatePlan,
        artifact: ProtectedExecutionPrerequisiteArtifact,
    ) -> str:
        return _digest(self.dependency_guard(plan, artifact))

    def _profile_store(self) -> PreparedExecutorProfileStore:
        return PreparedExecutorProfileStore(self.state_root, service_uid=self.service_uid)


def profile_execution_context(profile: CapacityPoolExecutorProfile) -> ExecutionContextV2:
    return ExecutionContextV2(
        authority_incarnation=UUID(profile.authority_incarnation),
        writer_epoch=profile.writer_epoch,
        configuration_epoch=profile.configuration_epoch,
        execution_epoch=profile.execution_epoch,
        execution_manifest_sha256=profile.execution_manifest_sha256,
        execution_state="prepared",
        executable_new_capacity_ceiling=0,
        executable_new_capacity_rate_per_minute=0,
        trusted_fleet_release_sha256=profile.trusted_fleet_release_sha256,
    )


def _preparation_request(
    status: _ManagerExecutionStatus,
    *,
    artifact: ProtectedExecutionPrerequisiteArtifact,
) -> ExecutionPreparationV2:
    policy = artifact.execution_policy
    request = ExecutionPreparationV2(
        authority_incarnation=status.authority_incarnation,
        expected_writer_epoch=status.writer_epoch,
        configuration_epoch=status.configuration_epoch,
        fleet_generation=artifact.desired_fleet_generation,
        fleet_digest=artifact.desired_fleet_sha256,
        trusted_fleet_release_sha256=policy.trusted_fleet_release_sha256,
        requested_ceiling=policy.executable_new_capacity_ceiling,
        requested_rate_per_minute=policy.executable_new_capacity_rate_per_minute,
        executors=policy.executors,
        subject_acknowledgements=policy.subject_acknowledgements,
        legacy_writer_fences=policy.legacy_writer_fences,
        rollback_evidence_sha256=policy.rollback_evidence_sha256,
    )
    if status.execution_state == "prepared" and (
        status.execution_manifest_sha256 != canonical_executable_digest(request)
    ):
        raise ValueError("prepared manager manifest differs from exact request")
    return request


def _parse_manager_status(
    value: object,
    *,
    artifact: ProtectedExecutionPrerequisiteArtifact,
) -> _ManagerExecutionStatus:
    expected = {
        "account_slots",
        "authority_incarnation",
        "blocker_counts",
        "configuration_digest",
        "configuration_epoch",
        "executable_new_capacity_ceiling",
        "execution_epoch",
        "execution_manifest_sha256",
        "execution_state",
        "increase_freeze",
        "latest_shadow_epoch",
        "latest_shadow_input_digest",
        "observer_principal_id",
        "pool_slots",
        "report_freshness_counts",
        "schema_version",
        "tier_slots",
        "writer_epoch",
    }
    if not isinstance(value, dict) or set(value) != expected or value.get("schema_version") != 1:
        raise ValueError("capacity manager execution status is invalid")
    try:
        authority = UUID(str(value["authority_incarnation"]))
    except ValueError as exc:
        raise ValueError("capacity manager execution authority is invalid") from exc
    writer_epoch = value["writer_epoch"]
    configuration_epoch = value["configuration_epoch"]
    execution_epoch = value["execution_epoch"]
    ceiling = value["executable_new_capacity_ceiling"]
    state = value["execution_state"]
    manifest = value["execution_manifest_sha256"]
    if (
        str(authority) != artifact.executor_profile_seed.authority_incarnation
        or type(writer_epoch) is not int
        or writer_epoch <= 0
        or type(configuration_epoch) is not int
        or configuration_epoch
        not in {
            artifact.source_configuration_epoch,
            artifact.source_configuration_epoch + 1,
        }
        or type(execution_epoch) is not int
        or execution_epoch < 0
        or type(ceiling) is not int
        or ceiling != 0
        or state not in {"shadow", "prepared"}
        or type(value["increase_freeze"]) is not bool
        or value["increase_freeze"] is not True
        or not isinstance(value["observer_principal_id"], str)
        or not value["observer_principal_id"]
        or any(
            not isinstance(value[name], dict)
            for name in (
                "account_slots",
                "blocker_counts",
                "pool_slots",
                "report_freshness_counts",
                "tier_slots",
            )
        )
    ):
        raise ValueError("capacity manager execution status is invalid")
    configuration_digest = _digest(value["configuration_digest"])
    if (
        configuration_epoch == artifact.source_configuration_epoch
        and configuration_digest != artifact.source_configuration_sha256
    ):
        raise ValueError("capacity manager source configuration changed")
    if state == "shadow":
        if execution_epoch != 0 or manifest is not None:
            raise ValueError("capacity manager shadow execution status is invalid")
    elif execution_epoch <= 0:
        raise ValueError("capacity manager prepared execution epoch is invalid")
    else:
        _digest(manifest)
    return _ManagerExecutionStatus(
        authority_incarnation=authority,
        writer_epoch=writer_epoch,
        configuration_epoch=configuration_epoch,
        configuration_digest=configuration_digest,
        execution_epoch=execution_epoch,
        execution_state=state,
        execution_manifest_sha256=manifest,
        executable_new_capacity_ceiling=ceiling,
        increase_freeze=True,
    )


def _require_shadow_readback(
    manager: _ManagerExecutionStatus,
    status: ProtectedExecutionPreparationStatus,
    *,
    artifact: ProtectedExecutionPrerequisiteArtifact,
) -> None:
    if not _readiness_digest_is_exact(status):
        raise RuntimeError("capacity manager shadow readiness digest is not exact")
    readiness = status.readiness
    if (
        manager.execution_state != "shadow"
        or readiness.execution is not None
        or readiness.policy_mode != "pinned"
        or readiness.policy_sha256 != canonical_executable_digest(artifact.execution_policy)
        or readiness.ready
        or "manager-shadow" not in readiness.blockers
    ):
        raise RuntimeError("capacity manager shadow readback is not exact")


def _require_prepared_readback(
    manager: _ManagerExecutionStatus,
    status: ProtectedExecutionPreparationStatus,
    *,
    request: ExecutionPreparationV2,
    artifact: ProtectedExecutionPrerequisiteArtifact,
) -> ExecutionContextV2:
    if not _readiness_digest_is_exact(status):
        raise RuntimeError("capacity manager prepared readiness digest is not exact")
    execution = status.readiness.execution
    manifest = canonical_executable_digest(request)
    if (
        manager.execution_state != "prepared"
        or execution is None
        or execution.authority_incarnation != request.authority_incarnation
        or execution.writer_epoch != request.expected_writer_epoch
        or execution.configuration_epoch != request.configuration_epoch
        or execution.execution_epoch != manager.execution_epoch
        or execution.execution_manifest_sha256 != manifest
        or manager.execution_manifest_sha256 != manifest
        or execution.execution_state != "prepared"
        or execution.executable_new_capacity_ceiling != 0
        or execution.executable_new_capacity_rate_per_minute != 0
        or execution.trusted_fleet_release_sha256 != request.trusted_fleet_release_sha256
        or status.readiness.policy_mode != "pinned"
        or status.readiness.policy_sha256 != canonical_executable_digest(artifact.execution_policy)
    ):
        raise RuntimeError("capacity manager prepared readback is not exact")
    return execution


def _readiness_is_exact(
    status: ProtectedExecutionPreparationStatus,
    *,
    execution: ExecutionContextV2,
    artifact: ProtectedExecutionPrerequisiteArtifact,
) -> bool:
    if not _readiness_digest_is_exact(status):
        return False
    readiness: PreparedExecutionReadinessV2 = status.readiness
    expected = {item.pool_id: item for item in artifact.execution_policy.executors}
    if (
        not readiness.ready
        or readiness.blockers
        or readiness.policy_mode != "pinned"
        or readiness.policy_sha256 != canonical_executable_digest(artifact.execution_policy)
        or readiness.execution != execution
        or readiness.expected_subject_count
        != len(artifact.execution_policy.subject_acknowledgements)
        or readiness.acknowledged_subject_count != readiness.expected_subject_count
        or tuple(item.pool_id for item in readiness.executors) != _POOL_ORDER
    ):
        return False
    observed_at = datetime.now(UTC)
    for item in readiness.executors:
        binding = expected[item.pool_id]
        if (
            item.expected_executor_id != binding.executor_id
            or item.expected_executor_incarnation != binding.executor_incarnation
            or item.expected_pool_generation != binding.pool_generation
            or not item.registered
            or item.registered_executor_id != binding.executor_id
            or item.registered_executor_incarnation != binding.executor_incarnation
            or item.registered_pool_generation != binding.pool_generation
            or not item.current
            or not item.lease_fresh
            or item.lease_expires_at is None
            or item.lease_expires_at <= observed_at
            or not item.inventory_fresh
            or not item.post_inventory_heartbeat
            or item.last_heartbeat_at is None
            or item.inventory_observed_at is None
            or item.last_heartbeat_at <= item.inventory_observed_at
            or item.inventory_record_count != 0
            or item.foreign_record_count != 0
            or item.unknown_record_count != 0
            or item.ownership_missing_record_count != 0
            or item.quarantined_record_count != 0
            or item.blockers
        ):
            return False
    return True


def _readiness_digest_is_exact(status: object) -> bool:
    return bool(
        isinstance(status, ProtectedExecutionPreparationStatus)
        and isinstance(status.readiness, PreparedExecutionReadinessV2)
        and isinstance(status.readiness_sha256, str)
        and status.readiness_sha256 == canonical_prepared_readiness_digest(status.readiness)
    )


def _controller_evidence_matches(
    evidence: PreparedControllerEvidence | None,
    request: PreparedControllerRequest,
    *,
    require_timer: bool,
    require_tick: bool,
) -> bool:
    if not _controller_files_are_exact(evidence, request):
        return False
    assert isinstance(evidence, PreparedControllerEvidence)
    prepared_timer = "loom-capacity-pool-executor-prepared.timer"
    timer_state = (
        evidence.unit_active_state[prepared_timer],
        evidence.unit_file_state[prepared_timer],
    )
    return timer_state == (
        ("active", "enabled") if require_timer else ("inactive", "disabled")
    ) and (not require_tick or evidence.successful_tick)


def _controller_files_are_exact(
    evidence: PreparedControllerEvidence | None,
    request: PreparedControllerRequest,
) -> bool:
    if not isinstance(evidence, PreparedControllerEvidence):
        return False
    expected_files = {
        path: hashlib.sha256(payload).hexdigest() for path, payload in request.files.items()
    }
    return (
        evidence.pool_id == request.pool_id
        and evidence.transport_authority_sha256 == request.transport_authority_sha256
        and evidence.request_sha256 == request.request_sha256
        and dict(evidence.file_sha256) == expected_files
    )


def _idempotency_key(plan: FinalGatePlan, purpose: str, digest: str) -> UUID:
    _digest(digest)
    return uuid5(
        NAMESPACE_URL,
        f"loom:protected-execution:{plan.request_id}:{plan.attempt_number}:"
        f"{plan.plan_digest}:{purpose}:{digest}",
    )


def _hash_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).rstrip(b"\n")).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("prepared controller request contains duplicate fields")
        result[key] = value
    return result


def _validate_private_directory(path: Path, *, service_uid: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise RuntimeError("prepared executor profile directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise RuntimeError("prepared executor profile directory is unsafe")


def _ensure_private_directory(
    path: Path,
    *,
    service_uid: int,
    parents: bool = False,
) -> None:
    created = False
    try:
        path.mkdir(mode=_PRIVATE_DIRECTORY_MODE, parents=parents)
    except FileExistsError:
        pass
    except OSError as exc:
        raise RuntimeError("prepared executor profile directory could not be created") from exc
    else:
        created = True
    if created:
        try:
            descriptor = os.open(
                path,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
        except OSError as exc:
            raise RuntimeError(
                "prepared executor profile directory could not be finalized"
            ) from exc
        try:
            os.fchmod(descriptor, _PRIVATE_DIRECTORY_MODE)
        finally:
            os.close(descriptor)
    _validate_private_directory(path, service_uid=service_uid)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


__all__ = [
    "ExecutionPreparationClientContext",
    "ExecutionPreparationDependencyGuard",
    "KubernetesProtectedCapacityExecutionPreparationComponent",
    "PreparedControllerEvidence",
    "PreparedControllerRequest",
    "PreparedControllerTransport",
    "PreparedExecutorProfilePublication",
    "PreparedExecutorProfileStore",
    "canonical_prepared_executor_profile_bytes",
    "prepared_executor_profile_sha256",
    "profile_execution_context",
]
