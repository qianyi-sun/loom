"""Protected inert convergence for one controller-local capacity executor."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit

from loom_cli.capacity_control_plane import CapacityPoolExecutorBinding

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import ComponentState
from .protected_execution_prerequisites import ProtectedExecutionPrerequisiteArtifact

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_RE = re.compile(
    r"^(?P<registry>[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[1-9][0-9]{0,4})?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*)"
    r"/loom-capacity-executor@sha256:(?P<digest>[0-9a-f]{64})$"
)
_POOL_IDS = frozenset({"gb10", "oldlab"})
_ARCHITECTURES = {"gb10": "arm64", "oldlab": "amd64"}
_CONTROLLER_HOSTS = {"gb10": "gx10-01c7", "oldlab": "TRT-EAI-OLDLAB-1"}
_SLURM_CLUSTERS = {"gb10": "trt-gb10", "oldlab": "trt-oldlab"}
_PARTITION = "loom-staging"
_TARGET_NODES = {
    "gb10": tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16))),
    "oldlab": tuple(f"trt-eai-oldlab-{index}" for index in range(3, 6)),
}
_EXECUTABLE_NAMES = frozenset({"sacct", "sacctmgr", "sbatch", "scancel", "scontrol", "squeue"})
_CONFIGURATION_NAMES = frozenset({"slurm.conf"})
_UNITS = (
    "loom-capacity-pool-executor.service",
    "loom-capacity-pool-executor-prepared.service",
    "loom-capacity-pool-executor-prepared.timer",
    "loom-capacity-pool-executor-active.service",
    "loom-capacity-pool-executor-active.timer",
)
_MAX_EVIDENCE_BYTES = 2 * 1024 * 1024


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


def _hash_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).rstrip(b"\n")).hexdigest()


def _digest(value: object, *, allow_zero: bool = False) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError("controller prerequisite digest is invalid")
    if not allow_zero and value == "0" * 64:
        raise ValueError("controller prerequisite digest is a placeholder")
    return value


def _digest_map(
    value: Mapping[str, str],
    *,
    keys: frozenset[str] | set[str],
) -> Mapping[str, str]:
    copied = dict(value)
    if set(copied) != set(keys):
        raise ValueError("controller prerequisite digest inventory is invalid")
    for digest in copied.values():
        _digest(digest)
    return MappingProxyType(dict(sorted(copied.items())))


def _safe_absolute(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("controller prerequisite path is invalid")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts or str(path) != value:
        raise ValueError("controller prerequisite path is invalid")
    return value


def capacity_executor_image_digest(image: object) -> str:
    """Return the digest of one immutable executor image in a validated registry."""

    matched = _IMAGE_RE.fullmatch(image) if isinstance(image, str) else None
    if matched is None or matched.group("digest") == "0" * 64:
        raise ValueError("capacity executor image is invalid")
    registry_host = matched.group("registry").split("/", 1)[0]
    try:
        parsed = urlsplit(f"//{registry_host}")
        port = parsed.port
    except ValueError as exc:
        raise ValueError("capacity executor image is invalid") from exc
    if parsed.hostname is None or (port is not None and port > 65535):
        raise ValueError("capacity executor image is invalid")
    return matched.group("digest")


@dataclass(frozen=True, slots=True)
class ControllerDirectoryEvidence:
    path: str
    mode: int
    uid: int
    gid: int

    def __post_init__(self) -> None:
        _safe_absolute(self.path)
        if self.mode != 0o700 or type(self.uid) is not int or type(self.gid) is not int:
            raise ValueError("controller prerequisite directory evidence is invalid")
        if self.uid <= 0 or self.gid <= 0:
            raise ValueError("controller prerequisite directory authority is invalid")

    def to_dict(self) -> dict[str, object]:
        return {"gid": self.gid, "mode": self.mode, "path": self.path, "uid": self.uid}

    @classmethod
    def from_dict(cls, value: object) -> ControllerDirectoryEvidence:
        if not isinstance(value, Mapping) or set(value) != {"gid", "mode", "path", "uid"}:
            raise ValueError("controller prerequisite directory evidence is invalid")
        return cls(
            path=value["path"],
            mode=value["mode"],
            uid=value["uid"],
            gid=value["gid"],
        )


def controller_local_authority_sha256(
    *,
    pool_id: str,
    architecture: str,
    controller_hostname: str,
    service_uid: int,
    service_gid: int,
    slurm_cluster: str,
    partition: str,
    target_nodes: tuple[str, ...],
    executable_sha256: Mapping[str, str],
    configuration_sha256: Mapping[str, str],
    job_visibility_evidence_sha256: str,
) -> str:
    """Derive the pool binding from the exact local scheduler authority."""

    return _hash_json(
        {
            "architecture": architecture,
            "configuration_sha256": dict(configuration_sha256),
            "controller_hostname": controller_hostname,
            "executable_sha256": dict(executable_sha256),
            "job_visibility_evidence_sha256": _digest(job_visibility_evidence_sha256),
            "partition": partition,
            "pool_id": pool_id,
            "service_gid": service_gid,
            "service_uid": service_uid,
            "slurm_cluster": slurm_cluster,
            "target_nodes": list(target_nodes),
        }
    )


@dataclass(frozen=True, slots=True)
class ControllerPrerequisiteEvidence:
    """Secret-free evidence for one exact and wholly inert controller."""

    schema_version: Literal[1]
    pool_id: str
    controller_hostname: str
    transport_authority_sha256: str
    image: str
    source_sha: str
    architecture: str
    release_root: str
    release_manifest_sha256: str
    service_user: str
    service_uid: int
    service_gid: int
    slurm_cluster: str
    partition: str
    target_nodes: tuple[str, ...]
    executable_sha256: Mapping[str, str]
    configuration_sha256: Mapping[str, str]
    job_visibility_evidence_sha256: str
    directories: Mapping[str, ControllerDirectoryEvidence]
    unit_sha256: Mapping[str, str]
    unit_active_state: Mapping[str, str]
    unit_file_state: Mapping[str, str]
    prerequisite_input_path: str
    prerequisite_input_sha256: str
    credential_metadata_sha256: Mapping[str, str]
    controller_authority_sha256: str
    local_authority_sha256: str

    def __post_init__(self) -> None:
        try:
            image_digest = capacity_executor_image_digest(self.image)
        except ValueError as exc:
            raise ValueError("controller prerequisite evidence identity is invalid") from exc
        if (
            self.schema_version != 1
            or self.pool_id not in _POOL_IDS
            or self.controller_hostname != _CONTROLLER_HOSTS.get(self.pool_id)
            or self.architecture != _ARCHITECTURES.get(self.pool_id)
            or not isinstance(self.source_sha, str)
            or _SHA_RE.fullmatch(self.source_sha) is None
            or self.service_user != "loom_capacity_executor"
            or type(self.service_uid) is not int
            or type(self.service_gid) is not int
            or self.service_uid <= 0
            or self.service_gid <= 0
            or self.slurm_cluster != _SLURM_CLUSTERS.get(self.pool_id)
            or self.partition != _PARTITION
            or self.target_nodes != _TARGET_NODES.get(self.pool_id)
        ):
            raise ValueError("controller prerequisite evidence identity is invalid")
        expected_release = (
            f"/opt/loom-capacity-executor-releases/{self.source_sha}-{self.architecture}-"
            f"{image_digest}"
        )
        if self.release_root != expected_release:
            raise ValueError("controller prerequisite release identity is invalid")
        for value in (
            self.transport_authority_sha256,
            self.release_manifest_sha256,
            self.prerequisite_input_sha256,
            self.controller_authority_sha256,
            self.local_authority_sha256,
            self.job_visibility_evidence_sha256,
        ):
            _digest(value)
        executables = _digest_map(self.executable_sha256, keys=_EXECUTABLE_NAMES)
        configurations = _digest_map(self.configuration_sha256, keys=_CONFIGURATION_NAMES)
        units = _digest_map(self.unit_sha256, keys=set(_UNITS))
        active_states = dict(self.unit_active_state)
        file_states = dict(self.unit_file_state)
        if active_states != {unit: "inactive" for unit in _UNITS} or file_states != {
            unit: "disabled" if unit.endswith(".timer") else "static" for unit in _UNITS
        }:
            raise ValueError("controller prerequisite units are not exactly inert")
        directory_values = dict(self.directories)
        expected_directories = {
            "/etc/loom-capacity-executor",
            "/run/loom-capacity-executor",
            f"/run/loom-capacity-executor/{self.pool_id}",
            "/var/lib/loom-capacity-executor",
            f"/var/lib/loom-capacity-executor/{self.pool_id}",
        }
        if set(directory_values) != expected_directories or any(
            not isinstance(item, ControllerDirectoryEvidence)
            or item.path != path
            or item.uid != self.service_uid
            or item.gid != self.service_gid
            for path, item in directory_values.items()
        ):
            raise ValueError("controller prerequisite directory inventory is invalid")
        input_path = f"/etc/loom-capacity-executor/{self.pool_id}-prerequisite.json"
        if self.prerequisite_input_path != input_path:
            raise ValueError("controller prerequisite input path is invalid")
        credential_keys = {
            f"pool-executor-{self.pool_id}",
            f"pool-ownership-{self.pool_id}",
        }
        credentials = _digest_map(self.credential_metadata_sha256, keys=credential_keys)
        local_authority = controller_local_authority_sha256(
            pool_id=self.pool_id,
            architecture=self.architecture,
            controller_hostname=self.controller_hostname,
            service_uid=self.service_uid,
            service_gid=self.service_gid,
            slurm_cluster=self.slurm_cluster,
            partition=self.partition,
            target_nodes=self.target_nodes,
            executable_sha256=executables,
            configuration_sha256=configurations,
            job_visibility_evidence_sha256=self.job_visibility_evidence_sha256,
        )
        if local_authority != self.local_authority_sha256:
            raise ValueError("controller prerequisite local authority is invalid")
        object.__setattr__(self, "executable_sha256", executables)
        object.__setattr__(self, "configuration_sha256", configurations)
        object.__setattr__(
            self, "directories", MappingProxyType(dict(sorted(directory_values.items())))
        )
        object.__setattr__(self, "unit_sha256", units)
        object.__setattr__(
            self, "unit_active_state", MappingProxyType(dict(sorted(active_states.items())))
        )
        object.__setattr__(
            self, "unit_file_state", MappingProxyType(dict(sorted(file_states.items())))
        )
        object.__setattr__(self, "credential_metadata_sha256", credentials)

    def to_dict(self) -> dict[str, object]:
        return {
            "architecture": self.architecture,
            "configuration_sha256": dict(self.configuration_sha256),
            "controller_authority_sha256": self.controller_authority_sha256,
            "controller_hostname": self.controller_hostname,
            "credential_metadata_sha256": dict(self.credential_metadata_sha256),
            "directories": {path: item.to_dict() for path, item in self.directories.items()},
            "executable_sha256": dict(self.executable_sha256),
            "image": self.image,
            "job_visibility_evidence_sha256": self.job_visibility_evidence_sha256,
            "local_authority_sha256": self.local_authority_sha256,
            "partition": self.partition,
            "pool_id": self.pool_id,
            "prerequisite_input_path": self.prerequisite_input_path,
            "prerequisite_input_sha256": self.prerequisite_input_sha256,
            "release_manifest_sha256": self.release_manifest_sha256,
            "release_root": self.release_root,
            "schema_version": self.schema_version,
            "service_gid": self.service_gid,
            "service_uid": self.service_uid,
            "service_user": self.service_user,
            "slurm_cluster": self.slurm_cluster,
            "source_sha": self.source_sha,
            "target_nodes": list(self.target_nodes),
            "transport_authority_sha256": self.transport_authority_sha256,
            "unit_active_state": dict(self.unit_active_state),
            "unit_file_state": dict(self.unit_file_state),
            "unit_sha256": dict(self.unit_sha256),
        }

    def to_bytes(self) -> bytes:
        return _canonical_json(self.to_dict())

    @classmethod
    def from_bytes(cls, payload: bytes) -> ControllerPrerequisiteEvidence:
        if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_EVIDENCE_BYTES:
            raise ValueError("controller prerequisite evidence bytes are invalid")
        try:
            value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("controller prerequisite evidence bytes are invalid") from exc
        if not isinstance(value, Mapping):
            raise ValueError("controller prerequisite evidence is invalid")
        expected = {
            "architecture",
            "configuration_sha256",
            "controller_authority_sha256",
            "controller_hostname",
            "credential_metadata_sha256",
            "directories",
            "executable_sha256",
            "image",
            "job_visibility_evidence_sha256",
            "local_authority_sha256",
            "partition",
            "pool_id",
            "prerequisite_input_path",
            "prerequisite_input_sha256",
            "release_manifest_sha256",
            "release_root",
            "schema_version",
            "service_gid",
            "service_uid",
            "service_user",
            "slurm_cluster",
            "source_sha",
            "target_nodes",
            "transport_authority_sha256",
            "unit_active_state",
            "unit_file_state",
            "unit_sha256",
        }
        directories = value.get("directories")
        if set(value) != expected or not isinstance(directories, Mapping):
            raise ValueError("controller prerequisite evidence fields are invalid")

        def strings(name: str) -> dict[str, str]:
            found = value[name]
            if not isinstance(found, Mapping) or not all(
                isinstance(key, str) and isinstance(item, str) for key, item in found.items()
            ):
                raise ValueError("controller prerequisite evidence fields are invalid")
            return dict(found)

        targets = value["target_nodes"]
        if not isinstance(targets, list) or not all(isinstance(item, str) for item in targets):
            raise ValueError("controller prerequisite evidence fields are invalid")
        try:
            evidence = cls(
                schema_version=value["schema_version"],
                pool_id=value["pool_id"],
                controller_hostname=value["controller_hostname"],
                transport_authority_sha256=value["transport_authority_sha256"],
                image=value["image"],
                job_visibility_evidence_sha256=value["job_visibility_evidence_sha256"],
                source_sha=value["source_sha"],
                architecture=value["architecture"],
                release_root=value["release_root"],
                release_manifest_sha256=value["release_manifest_sha256"],
                service_user=value["service_user"],
                service_uid=value["service_uid"],
                service_gid=value["service_gid"],
                slurm_cluster=value["slurm_cluster"],
                partition=value["partition"],
                target_nodes=tuple(targets),
                executable_sha256=strings("executable_sha256"),
                configuration_sha256=strings("configuration_sha256"),
                directories={
                    str(path): ControllerDirectoryEvidence.from_dict(item)
                    for path, item in directories.items()
                },
                unit_sha256=strings("unit_sha256"),
                unit_active_state=strings("unit_active_state"),
                unit_file_state=strings("unit_file_state"),
                prerequisite_input_path=value["prerequisite_input_path"],
                prerequisite_input_sha256=value["prerequisite_input_sha256"],
                credential_metadata_sha256=strings("credential_metadata_sha256"),
                controller_authority_sha256=value["controller_authority_sha256"],
                local_authority_sha256=value["local_authority_sha256"],
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("controller prerequisite evidence is invalid") from exc
        if evidence.to_bytes() != payload:
            raise ValueError("controller prerequisite evidence is not canonical")
        return evidence


@dataclass(frozen=True, slots=True)
class ControllerPrerequisiteRequest:
    """One immutable, secret-free controller convergence request."""

    pool_id: str
    source_sha: str
    architecture: str
    image: str
    service_user: str
    binding: CapacityPoolExecutorBinding
    credential_metadata_sha256: Mapping[str, str]
    transport_authority_sha256: str

    def __post_init__(self) -> None:
        try:
            capacity_executor_image_digest(self.image)
        except ValueError as exc:
            raise ValueError("controller prerequisite request is invalid") from exc
        if (
            self.pool_id not in _POOL_IDS
            or not isinstance(self.binding, CapacityPoolExecutorBinding)
            or self.binding.pool_id != self.pool_id
            or self.binding.controller_host != _CONTROLLER_HOSTS[self.pool_id]
            or self.binding.slurm_cluster != _SLURM_CLUSTERS[self.pool_id]
            or self.binding.partition != _PARTITION
            or self.architecture != _ARCHITECTURES[self.pool_id]
            or self.service_user != "loom_capacity_executor"
            or _SHA_RE.fullmatch(self.source_sha) is None
        ):
            raise ValueError("controller prerequisite request is invalid")
        expected_nodes = {item.node_id for item in self.binding.inventory.nodes}
        expected_paths = {name: f"/usr/bin/{name}" for name in _EXECUTABLE_NAMES}
        if (
            expected_nodes != set(_TARGET_NODES[self.pool_id])
            or self.binding.inventory.query_uid != self.binding.local_uid
            or self.binding.inventory.controller_cluster != self.binding.slurm_cluster
            or tuple(self.binding.inventory.relevant_partitions) != (_PARTITION,)
            or self.binding.slurm_executables.model_dump() != expected_paths
            or Path(self.binding.state_directory)
            != Path("/var/lib/loom-capacity-executor") / self.pool_id
            or Path(self.binding.config_file)
            != Path("/etc/loom-capacity-executor") / f"{self.pool_id}.json"
        ):
            raise ValueError("controller prerequisite request authority is invalid")
        credential_keys = {
            f"pool-executor-{self.pool_id}",
            f"pool-ownership-{self.pool_id}",
        }
        credentials = _digest_map(self.credential_metadata_sha256, keys=credential_keys)
        _digest(self.transport_authority_sha256)
        object.__setattr__(self, "credential_metadata_sha256", credentials)

    @property
    def prerequisite_input_path(self) -> str:
        return f"/etc/loom-capacity-executor/{self.pool_id}-prerequisite.json"

    @property
    def target_nodes(self) -> tuple[str, ...]:
        return _TARGET_NODES[self.pool_id]

    def prerequisite_input_value(self) -> dict[str, object]:
        return {
            "binding": self.binding.model_dump(mode="json"),
            "credential_metadata_sha256": dict(self.credential_metadata_sha256),
            "executor_image": self.image,
            "schema_version": 1,
            "service_user": self.service_user,
            "source_sha": self.source_sha,
        }

    @property
    def prerequisite_input_sha256(self) -> str:
        return hashlib.sha256(_canonical_json(self.prerequisite_input_value())).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "architecture": self.architecture,
            "binding": self.binding.model_dump(mode="json"),
            "credential_metadata_sha256": dict(self.credential_metadata_sha256),
            "image": self.image,
            "pool_id": self.pool_id,
            "schema_version": 1,
            "service_user": self.service_user,
            "source_sha": self.source_sha,
            "transport_authority_sha256": self.transport_authority_sha256,
        }

    def to_bytes(self) -> bytes:
        return _canonical_json(self.to_dict())

    @classmethod
    def from_bytes(cls, payload: bytes) -> ControllerPrerequisiteRequest:
        if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_EVIDENCE_BYTES:
            raise ValueError("controller prerequisite request bytes are invalid")
        try:
            value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("controller prerequisite request bytes are invalid") from exc
        expected = {
            "architecture",
            "binding",
            "credential_metadata_sha256",
            "image",
            "pool_id",
            "schema_version",
            "service_user",
            "source_sha",
            "transport_authority_sha256",
        }
        credentials = (
            value.get("credential_metadata_sha256") if isinstance(value, Mapping) else None
        )
        if (
            not isinstance(value, Mapping)
            or set(value) != expected
            or value.get("schema_version") != 1
            or not isinstance(credentials, Mapping)
            or not all(
                isinstance(name, str) and isinstance(digest, str)
                for name, digest in credentials.items()
            )
        ):
            raise ValueError("controller prerequisite request fields are invalid")
        try:
            request = cls(
                pool_id=value["pool_id"],
                source_sha=value["source_sha"],
                architecture=value["architecture"],
                image=value["image"],
                service_user=value["service_user"],
                binding=CapacityPoolExecutorBinding.model_validate(value["binding"]),
                credential_metadata_sha256=dict(credentials),
                transport_authority_sha256=value["transport_authority_sha256"],
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("controller prerequisite request is invalid") from exc
        if request.to_bytes() != payload:
            raise ValueError("controller prerequisite request is not canonical")
        return request


class ProtectedControllerPrerequisiteTransport(Protocol):
    @property
    def authority_sha256(self) -> str: ...

    def observe(
        self,
        request: ControllerPrerequisiteRequest,
    ) -> ControllerPrerequisiteEvidence | None: ...

    def converge(
        self,
        request: ControllerPrerequisiteRequest,
    ) -> ControllerPrerequisiteEvidence: ...


@dataclass(frozen=True, slots=True)
class KubernetesProtectedControllerPrerequisiteComponent:
    """Converge only the inert controller prerequisite for one pool."""

    pool_id: str
    transport: ProtectedControllerPrerequisiteTransport
    prerequisite_reader: Callable[[FinalGatePlan], ProtectedExecutionPrerequisiteArtifact]

    def __post_init__(self) -> None:
        if (
            self.pool_id not in _POOL_IDS
            or not callable(self.prerequisite_reader)
            or not callable(getattr(self.transport, "observe", None))
            or not callable(getattr(self.transport, "converge", None))
        ):
            raise ValueError("controller prerequisite component authority is invalid")
        _digest(self.transport.authority_sha256)

    def classify(self, plan: FinalGatePlan) -> tuple[ComponentState, str]:
        try:
            artifact = self.prerequisite_reader(plan)
            request = self._request(plan, artifact)
            evidence = self.transport.observe(request)
            second = self.prerequisite_reader(plan)
            if (
                second != artifact
                or self.transport.authority_sha256 != request.transport_authority_sha256
            ):
                raise ValueError("controller prerequisite source changed during observation")
            if evidence is None:
                return ComponentState.READY, _hash_json(
                    {"pool_id": self.pool_id, "status": "absent"}
                )
            self._validate_evidence(request, evidence)
        except (OSError, RuntimeError, UnicodeError, ValueError, KeyError):
            return ComponentState.DRIFTED, _hash_json(
                {"pool_id": self.pool_id, "status": "observation-failed"}
            )
        return ComponentState.EXACT, hashlib.sha256(evidence.to_bytes()).hexdigest()

    def apply(self, plan: FinalGatePlan) -> None:
        artifact = self.prerequisite_reader(plan)
        request = self._request(plan, artifact)
        before = self.transport.observe(request)
        if before is not None:
            self._validate_evidence(request, before)
            raise RuntimeError("controller prerequisite state changed before apply")
        self._require_source_unchanged(plan, artifact, request)
        converged = self.transport.converge(request)
        self._validate_evidence(request, converged)
        self._require_source_unchanged(plan, artifact, request)
        readback = self.transport.observe(request)
        if readback is None:
            raise RuntimeError("controller prerequisite convergence was incomplete")
        self._validate_evidence(request, readback)
        if readback != converged:
            raise RuntimeError("controller prerequisite readback differed from convergence")
        self._require_source_unchanged(plan, artifact, request)

    def _require_source_unchanged(
        self,
        plan: FinalGatePlan,
        expected_artifact: ProtectedExecutionPrerequisiteArtifact,
        expected_request: ControllerPrerequisiteRequest,
    ) -> None:
        current = self.prerequisite_reader(plan)
        if (
            current != expected_artifact
            or self.transport.authority_sha256 != expected_request.transport_authority_sha256
        ):
            raise RuntimeError("controller prerequisite source changed before mutation")

    def _request(
        self,
        plan: FinalGatePlan,
        artifact: ProtectedExecutionPrerequisiteArtifact,
    ) -> ControllerPrerequisiteRequest:
        _validate_prerequisite_binding(plan, artifact)
        bindings: dict[str, CapacityPoolExecutorBinding] = {
            binding.pool_id: binding for binding in artifact.executor_profile_seed.pools
        }
        if set(bindings) != _POOL_IDS:
            raise ValueError("controller prerequisite pool authority is incomplete")
        binding = bindings[self.pool_id]
        return ControllerPrerequisiteRequest(
            pool_id=self.pool_id,
            source_sha=plan.candidate_sha,
            architecture=_ARCHITECTURES[self.pool_id],
            image=artifact.executor_profile_seed.executor_image,
            service_user=artifact.executor_profile_seed.service_user,
            binding=binding,
            credential_metadata_sha256={
                name: artifact.credential_metadata_sha256[name]
                for name in (
                    f"pool-executor-{self.pool_id}",
                    f"pool-ownership-{self.pool_id}",
                )
            },
            transport_authority_sha256=self.transport.authority_sha256,
        )

    @staticmethod
    def _validate_evidence(
        request: ControllerPrerequisiteRequest,
        evidence: ControllerPrerequisiteEvidence,
    ) -> None:
        if not isinstance(evidence, ControllerPrerequisiteEvidence):
            raise ValueError("controller prerequisite evidence differs from its binding")
        validated = ControllerPrerequisiteEvidence.from_bytes(evidence.to_bytes())
        if validated != evidence:
            raise ValueError("controller prerequisite evidence differs from its binding")
        binding = request.binding
        if (
            evidence.pool_id != request.pool_id
            or evidence.controller_hostname != binding.controller_host
            or evidence.transport_authority_sha256 != request.transport_authority_sha256
            or evidence.image != request.image
            or evidence.source_sha != request.source_sha
            or evidence.architecture != request.architecture
            or evidence.service_user != request.service_user
            or evidence.service_uid != binding.local_uid
            or evidence.slurm_cluster != binding.slurm_cluster
            or evidence.partition != binding.partition
            or set(evidence.target_nodes) != {item.node_id for item in binding.inventory.nodes}
            or evidence.executable_sha256["scontrol"] != binding.inventory.scontrol_sha256
            or evidence.executable_sha256["squeue"] != binding.inventory.squeue_sha256
            or evidence.configuration_sha256["slurm.conf"] != binding.inventory.slurm_conf_sha256
            or evidence.credential_metadata_sha256 != request.credential_metadata_sha256
            or evidence.controller_authority_sha256 != binding.controller_authority_sha256
            or evidence.local_authority_sha256 != binding.local_authority_sha256
            or evidence.prerequisite_input_path != request.prerequisite_input_path
            or evidence.prerequisite_input_sha256 != request.prerequisite_input_sha256
        ):
            raise ValueError("controller prerequisite evidence differs from its binding")


def _validate_prerequisite_binding(
    plan: FinalGatePlan,
    artifact: ProtectedExecutionPrerequisiteArtifact,
) -> None:
    if (
        not isinstance(plan, FinalGatePlan)
        or plan.schema_version != 7
        or not isinstance(artifact, ProtectedExecutionPrerequisiteArtifact)
        or plan.execution_prerequisite_artifact_sha256 != artifact.artifact_sha256
        or plan.executor_profile_seed_sha256 != artifact.executor_profile_seed_sha256
        or plan.execution_access_metadata_sha256 != artifact.credential_metadata_manifest_sha256
        or plan.candidate_sha != artifact.candidate_sha
    ):
        raise ValueError("controller prerequisite plan binding drifted")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("controller prerequisite evidence contains duplicate fields")
        value[key] = item
    return value


__all__ = [
    "ControllerDirectoryEvidence",
    "ControllerPrerequisiteEvidence",
    "ControllerPrerequisiteRequest",
    "KubernetesProtectedControllerPrerequisiteComponent",
    "ProtectedControllerPrerequisiteTransport",
    "capacity_executor_image_digest",
    "controller_local_authority_sha256",
]
