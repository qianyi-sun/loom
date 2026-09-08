"""Owner-published authority for installed protected execution prerequisites."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4

from cryptography.hazmat.primitives.asymmetric import ed25519
from pydantic import ValidationError

from loom_capacity_agent.legacy_fence import LegacyCompatibilityFreezeV1
from loom_capacity_guard.contracts import canonical_digest
from loom_capacity_manager.contracts import canonical_digest as canonical_manager_digest
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    LegacyWriterFenceV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.ownership import public_key_fingerprint
from loom_capacity_pool_executor.config import SlurmInventoryNodeDocument
from loom_control_plane.global_execution_fence import (
    GlobalExecutionFenceError,
    assert_legacy_scale_up_allowed,
    parse_global_execution_witness_export,
)

from .protected_controller_discovery import (
    ControllerDiscoveryEvidence,
    ControllerDiscoveryRequest,
)
from .protected_controller_prerequisite_component import ControllerPrerequisiteRequest
from .protected_execution_prerequisite_source import (
    ProtectedExecutionPrerequisiteAuthority,
)
from .protected_execution_prerequisites import CapacityPoolExecutorProfileSeed
from .protected_staging_capacity_execution_credentials import ExecutionCredentialBundle
from .protected_staging_capacity_manager_configuration_component import (
    ProtectedStagingDesiredConfiguration,
)

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
_POOL_ORDER: tuple[Literal["gb10", "oldlab"], Literal["gb10", "oldlab"]] = (
    "gb10",
    "oldlab",
)
_POOLS = frozenset(_POOL_ORDER)
_ROUTES = frozenset({"gb10", "oldlab", "operator"})
_CREDENTIALS = frozenset(
    {
        "manager-abort",
        "manager-activate",
        "manager-drain",
        "manager-prepare",
        "manager-read",
        "manager-retire",
        "pool-executor-gb10",
        "pool-executor-oldlab",
        "pool-ownership-gb10",
        "pool-ownership-oldlab",
    }
)
_MAX_PUBLICATION_BYTES = 2 * 1024 * 1024
_MAX_CONFIG_MAP_BYTES = 256 * 1024
_MAX_WITNESS_EXPORT_BYTES = 64 * 1024
_MANAGER_ORIGIN = "https://192.168.50.103:31443"
_CONTROLLER_HOSTS = {"gb10": "gx10-01c7", "oldlab": "TRT-EAI-OLDLAB-1"}
_ARCHITECTURES = {"gb10": "arm64", "oldlab": "amd64"}
_SLURM_CLUSTERS = {"gb10": "trt-gb10", "oldlab": "trt-oldlab"}


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


def _digest(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None or value == "0" * 64:
        raise ValueError(f"installed execution authority {label} is invalid")
    return value


def _string_map(
    value: Mapping[str, str],
    *,
    label: str,
    expected_keys: frozenset[str] | None = None,
) -> Mapping[str, str]:
    copied = dict(value)
    if (
        (expected_keys is not None and set(copied) != set(expected_keys))
        or not copied
        or any(not isinstance(key, str) or not key for key in copied)
    ):
        raise ValueError(f"installed execution authority {label} is invalid")
    for digest in copied.values():
        _digest(digest, label=label)
    return MappingProxyType(dict(sorted(copied.items())))


def execution_subject_acknowledgement_sha256(
    freeze: LegacyCompatibilityFreezeV1,
    *,
    candidate: CandidateBindingV2,
    protected_admission_sha256: str,
) -> str:
    """Bind one subject acknowledgement to real freeze and admission evidence."""

    if not isinstance(freeze, LegacyCompatibilityFreezeV1) or not isinstance(
        candidate, CandidateBindingV2
    ):
        raise TypeError("installed execution subject acknowledgement is invalid")
    protected_admission = _digest(
        protected_admission_sha256,
        label="subject protected admission",
    )
    high_water = max(cursor.high_water for cursor in freeze.writer_cursors)
    return hashlib.sha256(
        _canonical_json(
            {
                "candidate": candidate.model_dump(mode="json"),
                "freeze_sha256": canonical_digest(freeze),
                "legacy_writer_high_water": high_water,
                "protected_admission_sha256": protected_admission,
                "schema_version": 1,
                "subject_id": str(freeze.subject_id),
            }
        ).rstrip(b"\n")
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class InstalledExecutionAuthorityPublication:
    """Canonical owner evidence admitted by the #906 protected cutover."""

    schema_version: Literal[1]
    authority_issue: Literal[906]
    candidate_sha: str
    core_artifact_bundle_sha256: str
    desired_fleet_sha256: str
    desired_subject_sha256: Mapping[str, str]
    subject_protected_admission_sha256: Mapping[str, str]
    staging_subject_id: UUID
    executor_profile_seed: CapacityPoolExecutorProfileSeed
    manager_client_cidrs: Mapping[str, str]
    credential_metadata_sha256: Mapping[str, str]
    controller_transport_authority_sha256: Mapping[str, str]
    manager_public_key_sha256: str
    manager_signing_key_id: str
    subject_acknowledgements: tuple[SubjectExecutionAcknowledgementV2, ...]
    subject_freezes: tuple[LegacyCompatibilityFreezeV1, ...]
    legacy_writer_fences: tuple[LegacyWriterFenceV2, ...]

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != 1
            or type(self.authority_issue) is not int
            or self.authority_issue != 906
            or not isinstance(self.candidate_sha, str)
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or not isinstance(self.staging_subject_id, UUID)
            or self.staging_subject_id.int == 0
            or not isinstance(self.executor_profile_seed, CapacityPoolExecutorProfileSeed)
            or not isinstance(self.manager_signing_key_id, str)
            or _IDENTIFIER_RE.fullmatch(self.manager_signing_key_id) is None
        ):
            raise ValueError("installed execution authority identity is invalid")
        for value, label in (
            (self.core_artifact_bundle_sha256, "core artifact bundle"),
            (self.desired_fleet_sha256, "desired fleet"),
            (self.manager_public_key_sha256, "manager public key"),
        ):
            _digest(value, label=label)
        desired_subjects = _string_map(
            self.desired_subject_sha256,
            label="desired subjects",
        )
        protected_admissions = _string_map(
            self.subject_protected_admission_sha256,
            label="subject protected admissions",
        )
        for subject_id in desired_subjects:
            try:
                parsed = UUID(subject_id)
            except ValueError as exc:
                raise ValueError(
                    "installed execution authority subject identity is invalid"
                ) from exc
            if parsed.int == 0 or str(parsed) != subject_id:
                raise ValueError("installed execution authority subject identity is invalid")
        if (
            set(desired_subjects) != set(protected_admissions)
            or str(self.staging_subject_id) not in desired_subjects
        ):
            raise ValueError("installed execution authority subject inventory is invalid")
        if (
            not isinstance(self.manager_client_cidrs, Mapping)
            or set(self.manager_client_cidrs) != set(_ROUTES)
            or any(
                not isinstance(name, str) or not isinstance(route, str)
                for name, route in self.manager_client_cidrs.items()
            )
        ):
            raise ValueError("installed execution authority manager routes are invalid")
        routes = MappingProxyType(dict(sorted(self.manager_client_cidrs.items())))
        credentials = _string_map(
            self.credential_metadata_sha256,
            label="credential metadata",
            expected_keys=_CREDENTIALS,
        )
        transports = _string_map(
            self.controller_transport_authority_sha256,
            label="controller transports",
            expected_keys=_POOLS,
        )
        acknowledgements = self.subject_acknowledgements
        freezes = self.subject_freezes
        fences = self.legacy_writer_fences
        if (
            not isinstance(acknowledgements, tuple)
            or not acknowledgements
            or any(
                not isinstance(item, SubjectExecutionAcknowledgementV2) for item in acknowledgements
            )
            or not isinstance(freezes, tuple)
            or not freezes
            or any(not isinstance(item, LegacyCompatibilityFreezeV1) for item in freezes)
            or not isinstance(fences, tuple)
            or not fences
            or any(not isinstance(item, LegacyWriterFenceV2) for item in fences)
        ):
            raise ValueError("installed execution authority typed evidence is invalid")
        acknowledgement_map = {str(item.subject_id): item for item in acknowledgements}
        freeze_map = {str(item.subject_id): item for item in freezes}
        if (
            len(acknowledgement_map) != len(acknowledgements)
            or len(freeze_map) != len(freezes)
            or set(acknowledgement_map) != set(desired_subjects)
            or set(freeze_map) != set(desired_subjects)
        ):
            raise ValueError("installed execution authority subject evidence is incomplete")
        for subject_id in sorted(desired_subjects):
            acknowledgement = acknowledgement_map[subject_id]
            freeze = freeze_map[subject_id]
            candidate = CandidateBindingV2(
                algorithm=freeze.candidate_identity_algorithm,
                identity=freeze.candidate_identity,
                publication_sha256=freeze.candidate_publication_sha256,
            )
            expected_high_water = max(cursor.high_water for cursor in freeze.writer_cursors)
            if (
                acknowledgement.subject_incarnation != freeze.subject_incarnation
                or acknowledgement.configuration_generation != freeze.configuration_generation
                or acknowledgement.deployment_generation != freeze.deployment_generation
                or acknowledgement.candidate != candidate
                or acknowledgement.reporter_incarnation != freeze.reporter_incarnation
                or acknowledgement.protected_admission_sha256 != protected_admissions[subject_id]
                or acknowledgement.legacy_writer_high_water != expected_high_water
                or acknowledgement.acknowledgement_sha256
                != execution_subject_acknowledgement_sha256(
                    freeze,
                    candidate=candidate,
                    protected_admission_sha256=protected_admissions[subject_id],
                )
                or str(freeze.authority_incarnation)
                != self.executor_profile_seed.authority_incarnation
            ):
                raise ValueError("installed execution authority subject acknowledgement is invalid")
        staging_freeze = freeze_map[str(self.staging_subject_id)]
        if (
            staging_freeze.candidate_identity_algorithm != "git-sha1"
            or staging_freeze.candidate_identity != self.candidate_sha
            or staging_freeze.candidate_publication_sha256 != self.core_artifact_bundle_sha256
        ):
            raise ValueError("installed execution authority staging candidate is invalid")
        fence_keys = tuple(
            (item.scope_kind, item.scope_id, item.writer_kind, item.writer_id) for item in fences
        )
        if len(fence_keys) != len(set(fence_keys)) or fence_keys != tuple(sorted(fence_keys)):
            raise ValueError("installed execution authority legacy writer fences are invalid")
        object.__setattr__(self, "desired_subject_sha256", desired_subjects)
        object.__setattr__(
            self,
            "subject_protected_admission_sha256",
            protected_admissions,
        )
        object.__setattr__(self, "manager_client_cidrs", routes)
        object.__setattr__(self, "credential_metadata_sha256", credentials)
        object.__setattr__(
            self,
            "controller_transport_authority_sha256",
            transports,
        )
        object.__setattr__(
            self,
            "subject_acknowledgements",
            tuple(sorted(acknowledgements, key=lambda item: item.subject_id.int)),
        )
        object.__setattr__(
            self,
            "subject_freezes",
            tuple(sorted(freezes, key=lambda item: item.subject_id.int)),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "authority_issue": self.authority_issue,
            "candidate_sha": self.candidate_sha,
            "controller_transport_authority_sha256": dict(
                self.controller_transport_authority_sha256
            ),
            "core_artifact_bundle_sha256": self.core_artifact_bundle_sha256,
            "credential_metadata_sha256": dict(self.credential_metadata_sha256),
            "desired_fleet_sha256": self.desired_fleet_sha256,
            "desired_subject_sha256": dict(self.desired_subject_sha256),
            "executor_profile_seed": self.executor_profile_seed.to_dict(),
            "legacy_writer_fences": [
                item.model_dump(mode="json", exclude_none=False)
                for item in self.legacy_writer_fences
            ],
            "manager_client_cidrs": dict(self.manager_client_cidrs),
            "manager_public_key_sha256": self.manager_public_key_sha256,
            "manager_signing_key_id": self.manager_signing_key_id,
            "schema_version": self.schema_version,
            "staging_subject_id": str(self.staging_subject_id),
            "subject_acknowledgements": [
                item.model_dump(mode="json", exclude_none=False)
                for item in self.subject_acknowledgements
            ],
            "subject_freezes": [
                item.model_dump(mode="json", exclude_none=False) for item in self.subject_freezes
            ],
            "subject_protected_admission_sha256": dict(self.subject_protected_admission_sha256),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> InstalledExecutionAuthorityPublication:
        expected = {
            "authority_issue",
            "candidate_sha",
            "controller_transport_authority_sha256",
            "core_artifact_bundle_sha256",
            "credential_metadata_sha256",
            "desired_fleet_sha256",
            "desired_subject_sha256",
            "executor_profile_seed",
            "legacy_writer_fences",
            "manager_client_cidrs",
            "manager_public_key_sha256",
            "manager_signing_key_id",
            "schema_version",
            "staging_subject_id",
            "subject_acknowledgements",
            "subject_freezes",
            "subject_protected_admission_sha256",
        }
        if set(value) != expected:
            raise ValueError("installed execution authority fields are invalid")

        def strings(name: str) -> dict[str, str]:
            found = value[name]
            if not isinstance(found, Mapping) or any(
                not isinstance(key, str) or not isinstance(item, str) for key, item in found.items()
            ):
                raise ValueError("installed execution authority fields are invalid")
            return dict(found)

        def objects(name: str) -> list[object]:
            found = value[name]
            if not isinstance(found, list):
                raise ValueError("installed execution authority fields are invalid")
            return found

        try:
            return cls(
                schema_version=value["schema_version"],  # type: ignore[arg-type]
                authority_issue=value["authority_issue"],  # type: ignore[arg-type]
                candidate_sha=value["candidate_sha"],  # type: ignore[arg-type]
                core_artifact_bundle_sha256=value["core_artifact_bundle_sha256"],  # type: ignore[arg-type]
                credential_metadata_sha256=strings("credential_metadata_sha256"),
                desired_fleet_sha256=value["desired_fleet_sha256"],  # type: ignore[arg-type]
                desired_subject_sha256=strings("desired_subject_sha256"),
                subject_protected_admission_sha256=strings("subject_protected_admission_sha256"),
                staging_subject_id=UUID(str(value["staging_subject_id"])),
                executor_profile_seed=CapacityPoolExecutorProfileSeed.from_dict(
                    value["executor_profile_seed"]  # type: ignore[arg-type]
                ),
                manager_client_cidrs=strings("manager_client_cidrs"),
                controller_transport_authority_sha256=strings(
                    "controller_transport_authority_sha256"
                ),
                manager_public_key_sha256=value["manager_public_key_sha256"],  # type: ignore[arg-type]
                manager_signing_key_id=value["manager_signing_key_id"],  # type: ignore[arg-type]
                subject_acknowledgements=tuple(
                    SubjectExecutionAcknowledgementV2.model_validate_json(
                        json.dumps(item, sort_keys=True, separators=(",", ":"))
                    )
                    for item in objects("subject_acknowledgements")
                ),
                subject_freezes=tuple(
                    LegacyCompatibilityFreezeV1.model_validate_json(
                        json.dumps(item, sort_keys=True, separators=(",", ":"))
                    )
                    for item in objects("subject_freezes")
                ),
                legacy_writer_fences=tuple(
                    LegacyWriterFenceV2.model_validate_json(
                        json.dumps(item, sort_keys=True, separators=(",", ":"))
                    )
                    for item in objects("legacy_writer_fences")
                ),
            )
        except (TypeError, ValidationError, ValueError) as exc:
            raise ValueError("installed execution authority is invalid") from exc


def canonical_installed_execution_authority_bytes(
    publication: InstalledExecutionAuthorityPublication,
) -> bytes:
    if not isinstance(publication, InstalledExecutionAuthorityPublication):
        raise TypeError("installed execution authority is invalid")
    return _canonical_json(publication.to_dict())


def parse_installed_execution_authority_bytes(
    payload: bytes,
) -> InstalledExecutionAuthorityPublication:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_PUBLICATION_BYTES:
        raise ValueError("installed execution authority is invalid")
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("installed execution authority is invalid") from exc
    if not isinstance(value, Mapping):
        raise ValueError("installed execution authority is invalid")
    publication = InstalledExecutionAuthorityPublication.from_dict(value)
    if canonical_installed_execution_authority_bytes(publication) != payload:
        raise ValueError("installed execution authority is not canonical")
    return publication


@dataclass(frozen=True, slots=True)
class InstalledExecutionAuthorityReader:
    """Read one stable canonical publication through an owner-only boundary."""

    path: Path
    expected_uid: int
    expected_gid: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.path, Path)
            or not self.path.is_absolute()
            or ".." in self.path.parts
            or type(self.expected_uid) is not int
            or type(self.expected_gid) is not int
            or min(self.expected_uid, self.expected_gid) < 0
        ):
            raise ValueError("installed execution authority reader is invalid")

    def __call__(self) -> InstalledExecutionAuthorityPublication:
        try:
            self._validate_parent()
            flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0)
            )
            descriptor = os.open(self.path, flags)
        except OSError as exc:
            raise ValueError("installed execution authority file is unavailable") from exc
        try:
            try:
                before = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(before.st_mode)
                    or stat.S_IMODE(before.st_mode) != 0o600
                    or before.st_uid != self.expected_uid
                    or before.st_gid != self.expected_gid
                    or before.st_nlink != 1
                    or not 0 < before.st_size <= _MAX_PUBLICATION_BYTES
                ):
                    raise ValueError("installed execution authority file is unsafe")
                chunks: list[bytes] = []
                remaining = _MAX_PUBLICATION_BYTES + 1
                while remaining > 0:
                    chunk = os.read(descriptor, min(64 * 1024, remaining))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                payload = b"".join(chunks)
                after = os.fstat(descriptor)
            except OSError as exc:
                raise ValueError("installed execution authority file is unavailable") from exc
        finally:
            os.close(descriptor)
        stable_fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_gid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if (
            len(payload) != before.st_size
            or len(payload) > _MAX_PUBLICATION_BYTES
            or any(getattr(before, field) != getattr(after, field) for field in stable_fields)
        ):
            raise ValueError("installed execution authority file changed while reading")
        try:
            self._validate_parent()
        except OSError as exc:
            raise ValueError("installed execution authority file is unavailable") from exc
        return parse_installed_execution_authority_bytes(payload)

    def _validate_parent(self) -> None:
        metadata = self.path.parent.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or metadata.st_uid != self.expected_uid
            or metadata.st_gid != self.expected_gid
        ):
            raise ValueError("installed execution authority directory is unsafe")


@dataclass(frozen=True, slots=True)
class InstalledExecutionAuthorityPublisher:
    """Create one canonical owner publication without replacing prior evidence."""

    path: Path
    expected_uid: int
    expected_gid: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.path, Path)
            or not self.path.is_absolute()
            or ".." in self.path.parts
            or self.path.name != "issue-906.json"
            or type(self.expected_uid) is not int
            or type(self.expected_gid) is not int
            or min(self.expected_uid, self.expected_gid) < 0
        ):
            raise ValueError("installed execution authority publisher is invalid")

    def __call__(
        self,
        publication: InstalledExecutionAuthorityPublication,
    ) -> InstalledExecutionAuthorityPublication:
        payload = canonical_installed_execution_authority_bytes(publication)
        if len(payload) > _MAX_PUBLICATION_BYTES:
            raise ValueError("installed execution authority is invalid")
        directory = self._open_directory()
        temporary = f".{self.path.name}.{uuid4().hex}.tmp"
        temporary_created = False
        try:
            try:
                descriptor = os.open(
                    temporary,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=directory,
                )
                temporary_created = True
            except OSError as exc:
                raise ValueError("installed execution authority publication failed") from exc
            try:
                os.fchmod(descriptor, 0o600)
                written = 0
                while written < len(payload):
                    count = os.write(descriptor, payload[written:])
                    if count <= 0:
                        raise OSError("authority publication write made no progress")
                    written += count
                os.fsync(descriptor)
                metadata = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or stat.S_IMODE(metadata.st_mode) != 0o600
                    or metadata.st_uid != self.expected_uid
                    or metadata.st_gid != self.expected_gid
                    or metadata.st_nlink != 1
                    or metadata.st_size != len(payload)
                ):
                    raise ValueError("installed execution authority temporary file is unsafe")
            except OSError as exc:
                raise ValueError("installed execution authority publication failed") from exc
            finally:
                os.close(descriptor)
            try:
                os.link(
                    temporary,
                    self.path.name,
                    src_dir_fd=directory,
                    dst_dir_fd=directory,
                    follow_symlinks=False,
                )
            except FileExistsError:
                existing = InstalledExecutionAuthorityReader(
                    path=self.path,
                    expected_uid=self.expected_uid,
                    expected_gid=self.expected_gid,
                )()
                if existing != publication:
                    raise ValueError(
                        "installed execution authority already exists with different evidence"
                    ) from None
                return existing
            except OSError as exc:
                raise ValueError("installed execution authority publication failed") from exc
            os.unlink(temporary, dir_fd=directory)
            temporary_created = False
            os.fsync(directory)
        finally:
            if temporary_created:
                try:
                    os.unlink(temporary, dir_fd=directory)
                except FileNotFoundError:
                    pass
            os.close(directory)
        admitted = InstalledExecutionAuthorityReader(
            path=self.path,
            expected_uid=self.expected_uid,
            expected_gid=self.expected_gid,
        )()
        if admitted != publication:
            raise ValueError("installed execution authority publication readback changed")
        return admitted

    def _open_directory(self) -> int:
        try:
            before = self.path.parent.stat(follow_symlinks=False)
            descriptor = os.open(
                self.path.parent,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
        except OSError as exc:
            raise ValueError("installed execution authority directory is unsafe") from exc
        opened = os.fstat(descriptor)
        identity_fields = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid")
        if (
            not stat.S_ISDIR(before.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o700
            or opened.st_uid != self.expected_uid
            or opened.st_gid != self.expected_gid
            or any(getattr(before, field) != getattr(opened, field) for field in identity_fields)
        ):
            os.close(descriptor)
            raise ValueError("installed execution authority directory is unsafe")
        return descriptor


class ControllerDiscoveryTransport(Protocol):
    @property
    def authority_sha256(self) -> str: ...

    def discover(self, request: ControllerDiscoveryRequest) -> ControllerDiscoveryEvidence: ...


class KubernetesWitnessCommandRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...


@dataclass(frozen=True, slots=True)
class KubernetesExecutionWitnessExportsSource:
    """Read only the fixed manager-published two-pool witness ConfigMap."""

    runner: KubernetesWitnessCommandRunner

    def __post_init__(self) -> None:
        environment = getattr(self.runner, "environment", None)
        if (
            not callable(getattr(self.runner, "capture_stdout", None))
            or not isinstance(environment, Mapping)
            or environment.get("KUBECONFIG") != "/var/lib/loom-staging-rollout/kubeconfig"
        ):
            raise ValueError("installed execution witness source is invalid")

    def __call__(self) -> Mapping[str, bytes]:
        payload = self.runner.capture_stdout(
            (
                "kubectl",
                "--namespace",
                "loom-dev",
                "get",
                "configmap",
                "loom-global-execution-witness-v1",
                "--output=json",
            ),
            env=self.runner.environment,
            timeout_seconds=10.0,
        )
        if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_CONFIG_MAP_BYTES:
            raise ValueError("installed execution witness ConfigMap is invalid")
        try:
            value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("installed execution witness ConfigMap is invalid") from exc
        if not isinstance(value, Mapping):
            raise ValueError("installed execution witness ConfigMap is invalid")
        metadata = value.get("metadata")
        data = value.get("data")
        if (
            value.get("apiVersion") != "v1"
            or value.get("kind") != "ConfigMap"
            or not isinstance(metadata, Mapping)
            or metadata.get("name") != "loom-global-execution-witness-v1"
            or metadata.get("namespace") != "loom-dev"
            or not isinstance(metadata.get("uid"), str)
            or not metadata.get("uid")
            or not isinstance(metadata.get("resourceVersion"), str)
            or not metadata.get("resourceVersion")
            or not isinstance(data, Mapping)
            or set(data) != {"gb10.json", "oldlab.json"}
        ):
            raise ValueError("installed execution witness ConfigMap is invalid")
        exports: dict[str, bytes] = {}
        for pool_id in _POOL_ORDER:
            exported = data[f"{pool_id}.json"]
            if not isinstance(exported, str):
                raise ValueError("installed execution witness ConfigMap is invalid")
            try:
                encoded = exported.encode("ascii")
            except UnicodeEncodeError as exc:
                raise ValueError("installed execution witness ConfigMap is invalid") from exc
            if not encoded.endswith(b"\n") or not 0 < len(encoded) <= _MAX_WITNESS_EXPORT_BYTES:
                raise ValueError("installed execution witness ConfigMap is invalid")
            exports[pool_id] = encoded
        return MappingProxyType(exports)


PublicationSource = Callable[[], InstalledExecutionAuthorityPublication]
CredentialBundleSource = Callable[[], ExecutionCredentialBundle]
WitnessExportsSource = Callable[[], Mapping[str, bytes]]


@dataclass(frozen=True, slots=True)
class InstalledExecutionAuthoritySource:
    """Combine owner evidence with live controller, credential, and witness authority."""

    publication_reader: PublicationSource
    controller_transports: Mapping[str, ControllerDiscoveryTransport]
    credential_bundle_reader: CredentialBundleSource
    witness_exports_source: WitnessExportsSource
    now: Callable[[], datetime] = lambda: datetime.now(UTC)

    def __post_init__(self) -> None:
        if (
            not callable(self.publication_reader)
            or not callable(self.credential_bundle_reader)
            or not callable(self.witness_exports_source)
            or not callable(self.now)
            or not isinstance(self.controller_transports, Mapping)
            or set(self.controller_transports) != set(_POOLS)
            or any(
                not callable(getattr(transport, "discover", None))
                for transport in self.controller_transports.values()
            )
        ):
            raise ValueError("installed execution authority source is invalid")
        object.__setattr__(
            self,
            "controller_transports",
            MappingProxyType(dict(sorted(self.controller_transports.items()))),
        )

    def __call__(
        self,
        desired: ProtectedStagingDesiredConfiguration,
    ) -> ProtectedExecutionPrerequisiteAuthority:
        if not isinstance(desired, ProtectedStagingDesiredConfiguration):
            raise TypeError("installed execution desired configuration is invalid")
        first_publication = self.publication_reader()
        if not isinstance(first_publication, InstalledExecutionAuthorityPublication):
            raise ValueError("installed execution owner publication is invalid")
        first_credentials = self.credential_bundle_reader()
        if not isinstance(first_credentials, ExecutionCredentialBundle):
            raise ValueError("installed execution credential authority is invalid")
        discoveries = self._discover_controllers(first_publication)
        first_witnesses = self._witness_semantics(first_publication)
        second_witnesses = self._witness_semantics(first_publication)
        second_discoveries = self._discover_controllers(first_publication)
        second_credentials = self.credential_bundle_reader()
        second_publication = self.publication_reader()
        if (
            second_publication != first_publication
            or second_credentials != first_credentials
            or second_discoveries != discoveries
            or second_witnesses != first_witnesses
        ):
            raise ValueError("installed execution authority changed during capture")
        self._validate_desired(first_publication, desired=desired)
        self._validate_credentials_and_controllers(
            first_publication,
            desired=desired,
            credentials=first_credentials,
            discoveries=discoveries,
        )
        return ProtectedExecutionPrerequisiteAuthority(
            executor_profile_seed=first_publication.executor_profile_seed,
            subject_acknowledgements=first_publication.subject_acknowledgements,
            manager_client_cidrs=first_publication.manager_client_cidrs,
            credential_metadata_sha256=first_credentials.metadata_sha256,
            coexistence_witness_sha256=first_witnesses,
            legacy_writer_fences=first_publication.legacy_writer_fences,
        )

    def _discover_controllers(
        self,
        publication: InstalledExecutionAuthorityPublication,
    ) -> Mapping[str, ControllerDiscoveryEvidence]:
        discoveries: dict[str, ControllerDiscoveryEvidence] = {}
        for pool_id in _POOL_ORDER:
            transport = self.controller_transports[pool_id]
            expected_authority = publication.controller_transport_authority_sha256[pool_id]
            if transport.authority_sha256 != expected_authority:
                raise ValueError("installed execution controller transport drifted")
            evidence = transport.discover(
                ControllerDiscoveryRequest(
                    schema_version=1,
                    pool_id=pool_id,
                    transport_authority_sha256=expected_authority,
                )
            )
            if (
                not isinstance(evidence, ControllerDiscoveryEvidence)
                or evidence.pool_id != pool_id
                or evidence.transport_authority_sha256 != expected_authority
                or transport.authority_sha256 != expected_authority
                or ControllerDiscoveryEvidence.from_bytes(evidence.to_bytes()) != evidence
            ):
                raise ValueError("installed execution controller discovery drifted")
            discoveries[pool_id] = evidence
        return MappingProxyType(discoveries)

    def _witness_semantics(
        self,
        publication: InstalledExecutionAuthorityPublication,
    ) -> Mapping[str, str]:
        exports = self.witness_exports_source()
        if (
            not isinstance(exports, Mapping)
            or set(exports) != set(_POOLS)
            or any(not isinstance(payload, bytes) for payload in exports.values())
        ):
            raise ValueError("installed execution witness exports are invalid")
        observed_at = self.now()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("installed execution witness clock is invalid")
        semantics: dict[str, str] = {}
        for pool_id in _POOL_ORDER:
            try:
                witness = parse_global_execution_witness_export(
                    exports[pool_id],
                    expected_manager_public_key_sha256=(publication.manager_public_key_sha256),
                )
                assert_legacy_scale_up_allowed(
                    witness,
                    expected_authority="global-capacity-manager",
                    expected_pool_id=pool_id,
                    now=observed_at,
                )
            except GlobalExecutionFenceError as exc:
                raise ValueError("installed execution witness is invalid") from exc
            if witness.signing_key_id != publication.manager_signing_key_id:
                raise ValueError("installed execution witness signer drifted")
            semantics[pool_id] = hashlib.sha256(
                _canonical_json(
                    {
                        "authority": witness.authority,
                        "executable_new_capacity_ceiling": (
                            witness.executable_new_capacity_ceiling
                        ),
                        "execution_epoch": witness.execution_epoch,
                        "execution_state": witness.execution_state,
                        "manager_public_key_sha256": (publication.manager_public_key_sha256),
                        "pool_id": witness.pool_id,
                        "schema_version": 1,
                        "signing_key_id": witness.signing_key_id,
                    }
                ).rstrip(b"\n")
            ).hexdigest()
        return MappingProxyType(semantics)

    @staticmethod
    def _validate_desired(
        publication: InstalledExecutionAuthorityPublication,
        *,
        desired: ProtectedStagingDesiredConfiguration,
    ) -> None:
        desired_subjects = {
            str(subject.subject_id): _manager_digest(subject) for subject in desired.subjects
        }
        if publication.manager_client_cidrs.get("operator") != "192.168.50.103/32":
            raise ValueError("installed execution operator route drifted")
        if (
            publication.desired_fleet_sha256 != _manager_digest(desired.fleet)
            or dict(publication.desired_subject_sha256) != desired_subjects
            or publication.staging_subject_id != desired.staging_subject.subject_id
            or str(publication.staging_subject_id)
            not in publication.subject_protected_admission_sha256
            or publication.executor_profile_seed.authority_incarnation
            != str(desired.fleet.authority_incarnation)
            or publication.executor_profile_seed.manager_origin != _MANAGER_ORIGIN
        ):
            raise ValueError("installed execution owner publication differs from desired state")

    @staticmethod
    def _validate_credentials_and_controllers(
        publication: InstalledExecutionAuthorityPublication,
        *,
        desired: ProtectedStagingDesiredConfiguration,
        credentials: ExecutionCredentialBundle,
        discoveries: Mapping[str, ControllerDiscoveryEvidence],
    ) -> None:
        bindings = {binding.pool_id: binding for binding in publication.executor_profile_seed.pools}
        desired_pools = {pool.pool_id: pool for pool in desired.fleet.pools}
        desired_profiles = {
            profile.pool_id: profile for profile in desired.staging_subject.profiles
        }
        if dict(credentials.metadata_sha256) != dict(publication.credential_metadata_sha256):
            raise ValueError("installed execution credential metadata drifted")
        if (
            set(bindings) != set(_POOLS)
            or set(desired_pools) != set(_POOLS)
            or set(desired_profiles) != set(_POOLS)
        ):
            raise ValueError("installed execution pool authority is incomplete")
        for pool_id in _POOL_ORDER:
            discovery = discoveries[pool_id]
            binding = bindings[pool_id]
            pool = desired_pools[pool_id]
            profile = desired_profiles[pool_id]
            one_slot_shapes = tuple(
                shape for shape in profile.worker_shapes if shape.concurrency_slots == 1
            )
            if len(one_slot_shapes) != 1:
                raise ValueError("installed execution one-slot profile is ambiguous")
            shape = one_slot_shapes[0]
            ownership_private_key = credentials.ownership_private_keys.get(pool_id)
            if not isinstance(ownership_private_key, bytes):
                raise ValueError("installed execution ownership key is unavailable")
            try:
                signing_key = ed25519.Ed25519PrivateKey.from_private_bytes(ownership_private_key)
            except ValueError as exc:
                raise ValueError("installed execution ownership key is invalid") from exc
            expected_nodes = tuple(
                SlurmInventoryNodeDocument(
                    pool_id=pool_id,
                    node_id=node.node_id,
                    allocatable=node.allocatable,
                    features=(domain.architecture,),
                )
                for domain in pool.resource_domains
                for node in domain.nodes
            )
            inventory = binding.inventory
            ControllerPrerequisiteRequest(
                pool_id=pool_id,
                source_sha=publication.candidate_sha,
                architecture=_ARCHITECTURES[pool_id],
                image=publication.executor_profile_seed.executor_image,
                service_user=publication.executor_profile_seed.service_user,
                binding=binding,
                credential_metadata_sha256={
                    name: credentials.metadata_sha256[name]
                    for name in (
                        f"pool-executor-{pool_id}",
                        f"pool-ownership-{pool_id}",
                    )
                },
                transport_authority_sha256=(
                    publication.controller_transport_authority_sha256[pool_id]
                ),
            )
            if (
                binding.controller_host != _CONTROLLER_HOSTS[pool_id]
                or binding.controller_host != discovery.controller_hostname
                or binding.local_uid != discovery.service_uid
                or binding.slurm_cluster != _SLURM_CLUSTERS[pool_id]
                or binding.slurm_cluster != discovery.slurm_cluster
                or binding.partition != discovery.partition
                or binding.partition != pool.partition
                or binding.association != pool.association
                or binding.pool_generation != pool.pool_generation
                or binding.local_authority_sha256 != discovery.local_authority_sha256
                or binding.signing_key_sha256 != public_key_fingerprint(signing_key.public_key())
                or binding.profile_id != shape.shape_id
                or binding.profile_generation != profile.profile_generation
                or binding.profile_digest != profile.profile_digest
                or inventory.nodes != expected_nodes
                or inventory.slot_resources != shape.total_resources
                or inventory.reporter_incarnation != str(pool.pool_reporter_incarnation)
                or inventory.controller_cluster != discovery.slurm_cluster
                or inventory.slurm_version != discovery.slurm_version
                or inventory.data_parser != discovery.data_parser
                or inventory.query_principal != discovery.query_principal
                or inventory.query_uid != discovery.service_uid
                or tuple(inventory.relevant_partitions) != (discovery.partition,)
                or inventory.job_visibility_evidence_sha256
                != discovery.job_visibility_evidence_sha256
                or inventory.scontrol_sha256 != discovery.executable_sha256["scontrol"]
                or inventory.squeue_sha256 != discovery.executable_sha256["squeue"]
                or inventory.slurm_conf_sha256 != discovery.configuration_sha256["slurm.conf"]
                or publication.manager_client_cidrs[pool_id] != discovery.manager_client_cidr
            ):
                raise ValueError("installed execution controller profile drifted")


def _manager_digest(value: object) -> str:
    return canonical_manager_digest(value)  # type: ignore[arg-type]


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("installed execution authority contains duplicate fields")
        value[key] = item
    return value


__all__ = [
    "ControllerDiscoveryTransport",
    "InstalledExecutionAuthorityPublication",
    "InstalledExecutionAuthorityPublisher",
    "InstalledExecutionAuthorityReader",
    "InstalledExecutionAuthoritySource",
    "KubernetesExecutionWitnessExportsSource",
    "KubernetesWitnessCommandRunner",
    "canonical_installed_execution_authority_bytes",
    "execution_subject_acknowledgement_sha256",
    "parse_installed_execution_authority_bytes",
]
