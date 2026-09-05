"""Stateful frozen-fleet integration harness for protected global autoscaling."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import ClassVar, Self
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

import httpx
import yaml
from cryptography import x509
from cryptography.hazmat.primitives.asymmetric import ec, ed25519
from fastapi.testclient import TestClient
from sqlalchemy import delete, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tests.capacity_fixtures import AUTHORITY_ID, configuration_activation
from tests.loom_cli.rollout.operator.test_final_gate_plan import (
    NOW,
    _artifacts,
    _baseline,
    _envelope,
    _plan,
    _predecessor_evidence,
    _systemd_evidence,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import (
    _attestation as _base_attestation,
)
from tests.loom_cli.rollout.operator.test_protected_execution_prerequisite_source import (
    _execution_fleet,
    _executor_seed,
    _lease,
)
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_manager_policy_component import (
    _server_certificate,
)
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_manager_runtime_component import (
    _SECRET_KEYS,
    _candidate,
)
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_runtime import (
    _write_bootstrap,
)
from tests.support.fake_slurm import FakeSlurm

from loom_capacity_manager.api import create_app
from loom_capacity_manager.config import CapacityManagerSettings
from loom_capacity_manager.contracts import (
    ConfigurationGenerationRefV1,
    ConfigurationSnapshotV1,
    FleetManifestV1,
    canonical_digest,
    canonical_digest_excluding,
)
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutableExecutorRegistrationV2,
    LegacyWriterFenceV2,
    SubjectExecutionAcknowledgementV2,
    canonical_executable_bytes,
    canonical_executable_digest,
    canonical_inventory_confirmation_journal_head,
)
from loom_capacity_manager.grant_contracts import DryRunExecutorRegistrationV1
from loom_capacity_manager.grant_store import CapacityGrantStore
from loom_capacity_manager.models import Base, CapacityAuthorityState
from loom_capacity_manager.ownership import OwnershipKeyring, public_key_fingerprint
from loom_capacity_manager.store import CapacityManagementStore
from loom_cli.capacity_control_plane import (
    CapacityPoolExecutorBinding,
    load_capacity_control_plane_profile,
    render_capacity_control_plane_manifests,
)
from loom_cli.rollout.operator.backup_lease import BackupLease, component_set_digest
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.operator.protected_capacity_execution_preparation_component import (
    PreparedControllerEvidence,
    PreparedControllerRequest,
)
from loom_cli.rollout.operator.protected_capacity_manager_client import (
    ProtectedCapacityManagerClient,
)
from loom_cli.rollout.operator.protected_controller_prerequisite_component import (
    ControllerDirectoryEvidence,
    ControllerPrerequisiteEvidence,
    ControllerPrerequisiteRequest,
    capacity_executor_image_digest,
    controller_local_authority_sha256,
)
from loom_cli.rollout.operator.protected_execution_prerequisite_source import (
    ProtectedExecutionPrerequisiteAuthority,
    ProtectedExecutionPrerequisiteRuntimeSource,
)
from loom_cli.rollout.operator.protected_execution_prerequisite_store import (
    ProtectedExecutionPrerequisiteStore,
)
from loom_cli.rollout.operator.protected_execution_prerequisites import (
    CapacityPoolExecutorProfileSeed,
    ProtectedExecutionPrerequisiteArtifact,
)
from loom_cli.rollout.operator.protected_pool_credential_transport import (
    FixedLocalPoolCredentialTransport,
)
from loom_cli.rollout.operator.protected_staging_capacity_execution_credentials import (
    ExecutionCredentialBundle,
    build_execution_ownership_keyring,
    build_execution_principal_registry,
)
from loom_cli.rollout.operator.protected_staging_capacity_manager_configuration_component import (
    ProtectedStagingDesiredConfiguration,
    derive_protected_staging_capacity_configuration,
)
from loom_cli.rollout.operator.protected_staging_capacity_manager_runtime_component import (
    _principal_registry_with_staging_reporter,
)
from loom_cli.rollout.operator.protected_staging_capacity_runtime import (
    KubernetesProtectedStagingCapacityRuntime,
)
from loom_cli.rollout.preflight_contract import (
    CheckContext,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    PreflightAttestation,
    PreflightDag,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
)

_CONTAINER_REGISTRY = "registry.example.test/loom"
_MANAGER_IMAGE_DIGEST = "sha256:" + "9" * 64
_EXECUTOR_IMAGE_DIGEST = "8" * 64
_PROTECTED_ADMISSION = "2" * 64
_POOL_ORDER = ("gb10", "oldlab")
_POOL_NODES = {
    "gb10": tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16))),
    "oldlab": tuple(f"trt-eai-oldlab-{index}" for index in range(3, 6)),
}
_POOL_ARCHITECTURE = {"gb10": "arm64", "oldlab": "amd64"}
_POOL_RESOURCE_ARCHITECTURE = {"gb10": "arm64", "oldlab": "x86_64"}
_POOL_CONTROLLER = {"gb10": "gx10-01c7", "oldlab": "TRT-EAI-OLDLAB-1"}
_POOL_CLUSTER = {"gb10": "trt-gb10", "oldlab": "trt-oldlab"}
_PARTITION = "loom-staging"
_UNITS = (
    "loom-capacity-pool-executor.service",
    "loom-capacity-pool-executor-prepared.service",
    "loom-capacity-pool-executor-prepared.timer",
    "loom-capacity-pool-executor-active.service",
    "loom-capacity-pool-executor-active.timer",
)
_CONFIGURATION_SCOPES = {
    "configuration-read": "capacity:read",
    "configuration-fleet": "capacity:configure:fleet",
    "configuration-subject": "capacity:configure:subject",
    "configuration-activate": "capacity:configure:activate",
}
_COMPONENT_LABEL = "loom.carin.dev/protected-component"
_COMPONENT_VALUE = "staging-capacity-manager-policy"
_FIELD_MANAGER = "loom-staging-capacity-manager-runtime"
_EXECUTION_FIELD_MANAGER = "loom-staging-capacity-execution-credentials"
_FROZEN_PREREQUISITE_COMPONENTS = frozenset(
    {
        "oldlab-controller-prerequisite",
        "gb10-controller-prerequisite",
        "staging-capacity-execution-credentials",
        "capacity-manager-runtime",
        "capacity-manager-configuration",
    }
)
_FROZEN_EXECUTION_COMPONENTS = _FROZEN_PREREQUISITE_COMPONENTS | {"capacity-execution-preparation"}


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _hash_json(value: object) -> str:
    return _sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii"))


def _owner_file(path: Path, payload: str | bytes) -> Path:
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_bytes(payload)
    path.chmod(0o600)
    return path


def _principal(principal_id: str, token: bytes, scope: str) -> dict[str, object]:
    return {
        "demand_reporter_incarnation": None,
        "executor_id": None,
        "executor_incarnation": None,
        "executor_pool_generation": None,
        "pool_id": None,
        "pool_reporter_incarnation": None,
        "principal_id": principal_id,
        "scopes": [scope],
        "subject_id": None,
        "subject_incarnation": None,
        "token_sha256": _sha256(token),
    }


def _base_registry(credentials_root: Path) -> bytes:
    principals = [
        {
            **_principal(
                "existing-operator",
                b"frozen-existing-operator-token-0000000000000001",
                "capacity:read",
            ),
            "scopes": ["capacity:read", "capacity:reconcile"],
        }
    ]
    for principal_id, scope in _CONFIGURATION_SCOPES.items():
        principals.append(
            _principal(
                principal_id,
                (credentials_root / principal_id / "bearer-token").read_bytes(),
                scope,
            )
        )
    return (
        json.dumps(
            {"principals": principals, "schema_version": 1},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("ascii")


def _identity(document: Mapping[str, object]) -> tuple[str, str, str]:
    metadata = document["metadata"]
    assert isinstance(metadata, Mapping)
    return (
        str(document["kind"]),
        str(metadata.get("namespace", "")),
        str(metadata["name"]),
    )


def _projection(document: Mapping[str, object]) -> dict[str, object]:
    value = deepcopy(dict(document))
    value.pop("status", None)
    metadata = value["metadata"]
    assert isinstance(metadata, dict)
    for metadata_field in (
        "creationTimestamp",
        "generation",
        "managedFields",
        "resourceVersion",
        "uid",
    ):
        metadata.pop(metadata_field, None)
    spec = value.get("spec")
    if isinstance(spec, dict):
        template = spec.get("template")
        if isinstance(template, dict) and isinstance(template.get("metadata"), dict):
            template["metadata"].pop("creationTimestamp", None)
    return value


class _FrozenKubernetes:
    """Stateful kubectl boundary shared by credential and policy components."""

    environment: ClassVar[dict[str, str]] = {"KUBECONFIG": "/fake/protected-kubeconfig"}

    def __init__(
        self,
        candidate: Path,
        *,
        authority_incarnation: UUID,
        principal_registry: bytes,
    ) -> None:
        profile = load_capacity_control_plane_profile(
            candidate / "deploy" / "dev-fleet" / "capacity-control-plane.toml"
        )
        rendered = render_capacity_control_plane_manifests(
            profile,
            manager_image=(f"{_CONTAINER_REGISTRY}/loom-capacity-manager@sha256:" + "7" * 64),
            authority_incarnation=authority_incarnation,
        )
        self.resources: dict[tuple[str, str, str], dict[str, object]] = {}
        for document in yaml.safe_load_all(rendered):
            if not isinstance(document, dict):
                continue
            key = _identity(document)
            if key in {
                ("Deployment", "loom-dev", "loom-capacity-manager"),
                ("NetworkPolicy", "loom-dev", "capacity-manager-ingress"),
            }:
                self.resources[key] = self._stored(document, existing=None)
        self.secret_uid = "34044ac3-1a1a-4fbe-ac27-05d03312cfe2"
        self.secret_resource_version = 17
        self.secret_owner = "kubectl-create"
        self.secret_data = {
            key: base64.b64encode(f"unchanged-{key}".encode("ascii")).decode("ascii")
            for key in _SECRET_KEYS
        }
        self.secret_data["principals.json"] = base64.b64encode(principal_registry).decode("ascii")
        self.secret_data["ownership-public-keys.json"] = base64.b64encode(
            b'{"keys":[],"schema_version":1}\n'
        ).decode("ascii")
        self.secret_data["server-certificate.pem"] = base64.b64encode(_server_certificate()).decode(
            "ascii"
        )
        self.backup_secrets: dict[str, dict[str, object]] = {}
        self.calls: list[tuple[tuple[str, ...], bytes | None]] = []
        self.rollouts: list[tuple[str, str]] = []
        self.mutations = 0

    def _manager_secret(self) -> dict[str, object]:
        return {
            "apiVersion": "v1",
            "data": dict(self.secret_data),
            "kind": "Secret",
            "metadata": {
                "managedFields": [
                    {
                        "apiVersion": "v1",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {
                            "f:data": {
                                "f:ownership-public-keys.json": {},
                                "f:principals.json": {},
                            }
                        },
                        "manager": self.secret_owner,
                        "operation": "Update",
                    }
                ],
                "name": "loom-capacity-manager",
                "namespace": "loom-dev",
                "resourceVersion": str(self.secret_resource_version),
                "uid": self.secret_uid,
            },
            "type": "Opaque",
        }

    def _stored(
        self,
        desired: Mapping[str, object],
        *,
        existing: Mapping[str, object] | None,
    ) -> dict[str, object]:
        value = deepcopy(dict(desired))
        metadata = value["metadata"]
        assert isinstance(metadata, dict)
        if existing is None:
            uid = f"11111111-1111-4111-8111-{len(self.resources) + 1:012d}"
            resource_version = str(len(self.resources) + 1)
            generation = 1
        else:
            old_metadata = existing["metadata"]
            assert isinstance(old_metadata, Mapping)
            uid = str(old_metadata["uid"])
            resource_version = str(int(str(old_metadata["resourceVersion"])) + 1)
            generation = int(str(old_metadata.get("generation", 0))) + 1
        metadata.update(
            {
                "managedFields": [
                    {
                        "apiVersion": value["apiVersion"],
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {"f:spec": {}},
                        "manager": _FIELD_MANAGER,
                        "operation": "Update",
                    }
                ],
                "resourceVersion": resource_version,
                "uid": uid,
            }
        )
        if value["kind"] == "Deployment":
            spec = value["spec"]
            assert isinstance(spec, dict)
            spec.setdefault("progressDeadlineSeconds", 600)
            replicas = spec["replicas"]
            assert isinstance(replicas, int)
            metadata["generation"] = generation
            managed = metadata["managedFields"]
            assert isinstance(managed, list)
            managed.append(
                {
                    "apiVersion": "apps/v1",
                    "fieldsType": "FieldsV1",
                    "fieldsV1": {"f:status": {}},
                    "manager": "k3s",
                    "operation": "Update",
                    "subresource": "status",
                }
            )
            value["status"] = {
                "availableReplicas": replicas,
                "observedGeneration": generation,
                "readyReplicas": replicas,
                "replicas": replicas,
                "updatedReplicas": replicas,
            }
        return value

    def _stored_backup(self, document: Mapping[str, object]) -> dict[str, object]:
        value = deepcopy(dict(document))
        metadata = value["metadata"]
        data = value["data"]
        assert isinstance(metadata, dict) and isinstance(data, dict)
        sequence = len(self.backup_secrets) + 1
        metadata.update(
            {
                "managedFields": [
                    {
                        "apiVersion": "v1",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {"f:data": {f"f:{key}": {} for key in data}},
                        "manager": _EXECUTION_FIELD_MANAGER,
                        "operation": "Update",
                    }
                ],
                "resourceVersion": str(sequence),
                "uid": f"22222222-2222-4222-8222-{sequence:012d}",
            }
        )
        return value

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes:
        assert env == self.environment
        command = tuple(argv)
        self.calls.append((command, None))
        if "secret/loom-capacity-manager" in command:
            assert timeout_seconds == 60.0
            return json.dumps(self._manager_secret(), sort_keys=True).encode("ascii")
        for name in (
            "loom-capacity-execution-operator",
            "loom-capacity-executor-gb10",
            "loom-capacity-executor-oldlab",
        ):
            if f"secret/{name}" in command:
                assert timeout_seconds == 60.0
                value = self.backup_secrets.get(name)
                return b"" if value is None else json.dumps(value, sort_keys=True).encode()
        assert timeout_seconds == 30.0
        if any(item.startswith("--selector=") for item in command):
            namespaced = "--all-namespaces" in command
            items = []
            for (_kind, namespace, _name), resource in self.resources.items():
                metadata = resource["metadata"]
                assert isinstance(metadata, Mapping)
                labels = metadata.get("labels", {})
                if not isinstance(labels, Mapping):
                    continue
                if labels.get(_COMPONENT_LABEL) != _COMPONENT_VALUE:
                    continue
                if namespaced != bool(namespace):
                    continue
                items.append(resource)
            return json.dumps({"apiVersion": "v1", "items": items, "kind": "List"}).encode()
        target = next(item for item in command if "/" in item and not item.startswith("--"))
        kind_name, name = target.split("/", 1)
        kind = {
            "configmap": "ConfigMap",
            "deployment": "Deployment",
            "namespace": "Namespace",
            "networkpolicy": "NetworkPolicy",
        }[kind_name]
        namespace = command[command.index("--namespace") + 1] if "--namespace" in command else ""
        value = self.resources.get((kind, namespace, name))
        return b"" if value is None else json.dumps(value, sort_keys=True).encode()

    def run_status(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> int:
        assert env == self.environment and timeout_seconds == 60.0
        assert input_payload is not None
        command = tuple(argv)
        self.calls.append((command, input_payload))
        desired = next(item for item in yaml.safe_load_all(input_payload) if item)
        observed = self.resources.get(_identity(desired))
        normalized = deepcopy(desired)
        if normalized["kind"] == "Deployment":
            spec = normalized["spec"]
            assert isinstance(spec, dict)
            spec.setdefault("progressDeadlineSeconds", 600)
        return 1 if observed is None or _projection(observed) != _projection(normalized) else 0

    def capture_stdout_with_input(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes,
        timeout_seconds: float,
    ) -> bytes:
        assert env == self.environment and timeout_seconds == 60.0
        command = tuple(argv)
        self.calls.append((command, input_payload))
        if "patch" in command and "secret/loom-capacity-manager" in command:
            operations = json.loads(input_payload)
            tests = {
                (operation["path"], json.dumps(operation["value"], sort_keys=True))
                for operation in operations
                if operation["op"] == "test"
            }
            assert ("/metadata/uid", json.dumps(self.secret_uid)) in tests
            assert (
                "/metadata/resourceVersion",
                json.dumps(str(self.secret_resource_version)),
            ) in tests
            for operation in operations:
                if operation["op"] == "replace":
                    self.secret_data[operation["path"].removeprefix("/data/")] = operation["value"]
            self.secret_owner = (
                _EXECUTION_FIELD_MANAGER
                if f"--field-manager={_EXECUTION_FIELD_MANAGER}" in command
                else _FIELD_MANAGER
            )
            self.secret_resource_version += 1
            self.mutations += 1
            return json.dumps(self._manager_secret(), sort_keys=True).encode("ascii")

        desired = next(item for item in yaml.safe_load_all(input_payload) if item)
        if desired["kind"] == "Secret":
            name = desired["metadata"]["name"]
            assert name not in self.backup_secrets
            stored_backup = self._stored_backup(desired)
            self.backup_secrets[name] = stored_backup
            self.mutations += 1
            return json.dumps(stored_backup, sort_keys=True).encode("ascii")

        key = _identity(desired)
        existing = self.resources.get(key)
        metadata = desired["metadata"]
        assert isinstance(metadata, dict)
        if "replace" in command:
            assert existing is not None
            existing_metadata = existing["metadata"]
            assert isinstance(existing_metadata, Mapping)
            assert metadata["uid"] == existing_metadata["uid"]
            assert metadata["resourceVersion"] == existing_metadata["resourceVersion"]
            metadata.pop("uid")
            metadata.pop("resourceVersion")
        else:
            assert "create" in command and existing is None
        stored = self._stored(desired, existing=existing)
        if "--dry-run=server" not in command:
            self.resources[key] = stored
            self.mutations += 1
        return json.dumps(stored, sort_keys=True).encode("ascii")

    def run_checked(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> None:
        assert env == self.environment and input_payload is None and timeout_seconds == 660.0
        command = tuple(argv)
        self.calls.append((command, None))
        name = next(item.removeprefix("deployment/") for item in command if "deployment/" in item)
        namespace = command[command.index("--namespace") + 1]
        self.rollouts.append((namespace, name))


class _HTTPStream:
    def __init__(self, response: httpx.Response) -> None:
        self.status_code = response.status_code
        self.headers = response.headers
        self._content = response.content

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback

    def iter_bytes(self, *, chunk_size: int) -> Iterator[bytes]:
        for offset in range(0, len(self._content), chunk_size):
            yield self._content[offset : offset + chunk_size]


@dataclass(slots=True)
class _ManagerMutationCounter:
    count: int = 0
    paths: list[str] = field(default_factory=list)

    def record(self, path: str) -> None:
        self.count += 1
        self.paths.append(path)


@dataclass(slots=True)
class _ExternalExecutionAuthority:
    artifact: ProtectedExecutionPrerequisiteArtifact
    legacy_writer_fences: tuple[LegacyWriterFenceV2, ...]
    coexistence_witness_sha256: dict[str, str]

    def evidence(self, observed_artifact: ProtectedExecutionPrerequisiteArtifact) -> str:
        if (
            observed_artifact != self.artifact
            or self.legacy_writer_fences != self.artifact.execution_policy.legacy_writer_fences
            or self.coexistence_witness_sha256 != dict(self.artifact.coexistence_witness_sha256)
        ):
            raise RuntimeError("frozen external execution authority drifted")
        return _hash_json(
            {
                "fences": [item.model_dump(mode="json") for item in self.legacy_writer_fences],
                "witnesses": dict(self.coexistence_witness_sha256),
            }
        )


class _TestClientHTTP:
    def __init__(self, client: TestClient, counter: _ManagerMutationCounter) -> None:
        self._client = client
        self._counter = counter

    def stream(self, method: str, url: str, **kwargs: object) -> _HTTPStream:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != "127.0.0.1":
            raise RuntimeError("frozen manager client escaped its localhost boundary")
        if method != "GET":
            self._counter.record(parsed.path)
        response = self._client.request(
            method,
            parsed.path,
            headers=kwargs.get("headers"),  # type: ignore[arg-type]
            content=kwargs.get("content"),  # type: ignore[arg-type]
            follow_redirects=False,
        )
        return _HTTPStream(response)

    def close(self) -> None:
        return None


def _scoped_slurm_job_ids(
    slurm: FakeSlurm,
    binding: CapacityPoolExecutorBinding,
) -> tuple[str, ...]:
    result = subprocess.run(
        (
            str(slurm.bin / "squeue"),
            f"--clusters={binding.slurm_cluster}",
            f"--user={binding.submitter}",
            f"--account={binding.association}",
        ),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"},
        check=True,
        timeout=1,
    )
    return tuple(
        line.split("|", 1)[0] for line in result.stdout.decode("utf-8").splitlines() if line
    )


class _ControllerPrerequisiteTransport:
    def __init__(self, pool_id: str, slurm: FakeSlurm, *, gid: int) -> None:
        self.pool_id = pool_id
        self.slurm = slurm
        self.gid = gid
        self.authority_sha256 = _hash_json(
            {"boundary": "frozen-controller", "pool_id": pool_id, "schema_version": 1}
        )
        self._request: ControllerPrerequisiteRequest | None = None
        self._evidence: ControllerPrerequisiteEvidence | None = None
        self.converge_calls = 0

    @property
    def executable_sha256(self) -> dict[str, str]:
        return {
            name: _sha256((self.slurm.bin / name).read_bytes())
            for name in ("sacct", "sacctmgr", "sbatch", "scancel", "scontrol", "squeue")
        }

    @property
    def configuration_sha256(self) -> dict[str, str]:
        return {"slurm.conf": _sha256(f"{self.pool_id}-slurm-conf\n".encode("ascii"))}

    def observe(
        self,
        request: ControllerPrerequisiteRequest,
    ) -> ControllerPrerequisiteEvidence | None:
        if self._request is not None and self._request != request:
            raise RuntimeError("frozen controller prerequisite request drifted")
        return self._evidence

    def converge(self, request: ControllerPrerequisiteRequest) -> ControllerPrerequisiteEvidence:
        if self._evidence is not None:
            raise RuntimeError("frozen controller prerequisite was already converged")
        if _scoped_slurm_job_ids(self.slurm, request.binding):
            raise RuntimeError("frozen controller found a scoped pre-existing job")
        self._request = request
        self._evidence = self._build_evidence(request)
        self.converge_calls += 1
        return self._evidence

    def _build_evidence(
        self,
        request: ControllerPrerequisiteRequest,
    ) -> ControllerPrerequisiteEvidence:
        binding = request.binding
        image_digest = capacity_executor_image_digest(request.image)
        directories = {
            path: ControllerDirectoryEvidence(
                path=path,
                mode=0o700,
                uid=binding.local_uid,
                gid=self.gid,
            )
            for path in (
                "/etc/loom-capacity-executor",
                "/run/loom-capacity-executor",
                f"/run/loom-capacity-executor/{self.pool_id}",
                "/var/lib/loom-capacity-executor",
                f"/var/lib/loom-capacity-executor/{self.pool_id}",
            )
        }
        return ControllerPrerequisiteEvidence(
            schema_version=1,
            pool_id=self.pool_id,
            controller_hostname=_POOL_CONTROLLER[self.pool_id],
            transport_authority_sha256=self.authority_sha256,
            image=request.image,
            source_sha=request.source_sha,
            architecture=_POOL_ARCHITECTURE[self.pool_id],
            release_root=(
                f"/opt/loom-capacity-executor-releases/{request.source_sha}-"
                f"{_POOL_ARCHITECTURE[self.pool_id]}-{image_digest}"
            ),
            release_manifest_sha256=_hash_json(
                {"image": request.image, "pool_id": self.pool_id, "source": request.source_sha}
            ),
            service_user=request.service_user,
            service_uid=binding.local_uid,
            service_gid=self.gid,
            slurm_cluster=_POOL_CLUSTER[self.pool_id],
            partition=_PARTITION,
            target_nodes=_POOL_NODES[self.pool_id],
            executable_sha256=self.executable_sha256,
            configuration_sha256=self.configuration_sha256,
            job_visibility_evidence_sha256=binding.inventory.job_visibility_evidence_sha256,
            directories=directories,
            unit_sha256={name: _sha256(name.encode("ascii")) for name in _UNITS},
            unit_active_state={name: "inactive" for name in _UNITS},
            unit_file_state={
                name: "disabled" if name.endswith(".timer") else "static" for name in _UNITS
            },
            prerequisite_input_path=request.prerequisite_input_path,
            prerequisite_input_sha256=request.prerequisite_input_sha256,
            credential_metadata_sha256=request.credential_metadata_sha256,
            controller_authority_sha256=binding.controller_authority_sha256,
            local_authority_sha256=binding.local_authority_sha256,
        )


class _PreparedControllerTransport:
    def __init__(
        self,
        pool_id: str,
        slurm: FakeSlurm,
        *,
        manager_client: TestClient,
        executor_token: bytes,
        mutation_counter: _ManagerMutationCounter,
    ) -> None:
        self.pool_id = pool_id
        self.slurm = slurm
        self.manager_client = manager_client
        self.executor_token = executor_token.decode("ascii")
        self.mutation_counter = mutation_counter
        self.request: PreparedControllerRequest | None = None
        self.files_exact = False
        self.timer_enabled = False
        self.tick_succeeded = False
        self.fail_next_files = False
        self.mutations = 0

    def _evidence(self, request: PreparedControllerRequest) -> PreparedControllerEvidence | None:
        if self.request is not None and self.request != request:
            raise RuntimeError("frozen prepared controller request drifted")
        if not self.files_exact:
            return None
        prepared_timer = "loom-capacity-pool-executor-prepared.timer"
        active_state = {name: "inactive" for name in _UNITS}
        file_state = {name: "disabled" if name.endswith(".timer") else "static" for name in _UNITS}
        if self.timer_enabled:
            active_state[prepared_timer] = "active"
            file_state[prepared_timer] = "enabled"
        return PreparedControllerEvidence(
            schema_version=1,
            pool_id=self.pool_id,  # type: ignore[arg-type]
            transport_authority_sha256=request.transport_authority_sha256,
            request_sha256=request.request_sha256,
            file_sha256={path: _sha256(payload) for path, payload in request.files.items()},
            unit_active_state=active_state,
            unit_file_state=file_state,
            successful_tick=self.tick_succeeded,
            tick_evidence_sha256=(
                _hash_json(
                    {
                        "execution": request.execution.execution_manifest_sha256,
                        "pool_id": self.pool_id,
                        "scoped_inventory": [],
                    }
                )
                if self.tick_succeeded
                else None
            ),
        )

    def observe(self, request: PreparedControllerRequest) -> PreparedControllerEvidence | None:
        return self._evidence(request)

    def converge_files(self, request: PreparedControllerRequest) -> PreparedControllerEvidence:
        if self.fail_next_files:
            self.fail_next_files = False
            raise RuntimeError("injected controller file failure")
        if self.files_exact:
            raise RuntimeError("frozen prepared controller files were already converged")
        self.request = request
        self.files_exact = True
        self.mutations += 1
        evidence = self._evidence(request)
        assert evidence is not None
        return evidence

    def enable_timer(self, request: PreparedControllerRequest) -> PreparedControllerEvidence:
        if not self.files_exact or self.timer_enabled:
            raise RuntimeError("frozen prepared controller timer state changed")
        self.timer_enabled = True
        self.mutations += 1
        evidence = self._evidence(request)
        assert evidence is not None
        return evidence

    def run_tick(self, request: PreparedControllerRequest) -> PreparedControllerEvidence:
        if not self.timer_enabled or self.tick_succeeded:
            raise RuntimeError("frozen prepared controller tick state changed")
        if _scoped_slurm_job_ids(self.slurm, request.prerequisite.binding):
            raise RuntimeError("frozen prepared controller inventory was not empty")
        binding = request.prerequisite.binding
        execution = request.execution
        headers = {"Authorization": f"Bearer {self.executor_token}"}
        registration = ExecutableExecutorRegistrationV2(
            execution=execution,
            executor_id=binding.executor_id,
            executor_incarnation=UUID(binding.executor_incarnation),
            pool_id=self.pool_id,
            pool_generation=binding.pool_generation,
            signing_key_id=binding.signing_key_id,
            signing_key_sha256=binding.signing_key_sha256,
            local_authority_sha256=binding.local_authority_sha256,
            controller_authority_sha256=binding.controller_authority_sha256,
        )
        self._put(
            f"/v2/executors/{self.pool_id}/registration",
            headers
            | {
                "Idempotency-Key": str(
                    uuid5(
                        NAMESPACE_URL,
                        f"frozen-registration:{self.pool_id}:{execution.execution_manifest_sha256}",
                    )
                )
            },
            registration.model_dump(mode="json"),
        )
        heartbeat = ExecutableExecutorHeartbeatV2(
            execution=execution,
            executor_id=binding.executor_id,
            executor_incarnation=UUID(binding.executor_incarnation),
            pool_id=self.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
        )
        self._put(
            f"/v2/executors/{self.pool_id}/heartbeat",
            headers,
            heartbeat.model_dump(mode="json"),
        )
        inventory = ExecutableExecutorInventoryV2(
            execution=execution,
            executor_id=binding.executor_id,
            executor_incarnation=UUID(binding.executor_incarnation),
            pool_id=self.pool_id,
            pool_generation=binding.pool_generation,
            inventory_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
            records=(),
        )
        self._put(
            f"/v2/executors/{self.pool_id}/inventory",
            headers,
            inventory.model_dump(mode="json"),
        )
        confirmation_sequence, confirmation_digest = canonical_inventory_confirmation_journal_head(
            inventory
        )
        self._put(
            f"/v2/executors/{self.pool_id}/heartbeat",
            headers,
            heartbeat.model_copy(
                update={
                    "heartbeat_sequence": 2,
                    "journal_sequence": confirmation_sequence,
                    "journal_digest": confirmation_digest,
                }
            ).model_dump(mode="json"),
        )
        self.tick_succeeded = True
        self.mutations += 1
        evidence = self._evidence(request)
        assert evidence is not None
        return evidence

    def _put(
        self,
        path: str,
        headers: Mapping[str, str],
        payload: Mapping[str, object],
    ) -> None:
        self.mutation_counter.record(path)
        response = self.manager_client.put(path, headers=dict(headers), json=dict(payload))
        if response.status_code != 200:
            raise RuntimeError(
                f"frozen prepared controller manager publication failed with {response.status_code}"
            )

    def disable_timer(
        self,
        request: PreparedControllerRequest,
    ) -> PreparedControllerEvidence | None:
        if self.request is not None and self.request != request:
            raise RuntimeError("frozen prepared controller request drifted")
        if self.timer_enabled:
            self.timer_enabled = False
            self.tick_succeeded = False
            self.mutations += 1
        evidence = self._evidence(request)
        return evidence


def _source_fleet(authority_incarnation: UUID) -> FleetManifestV1:
    source = _execution_fleet()
    pools = []
    for pool in source.pools:
        domains = tuple(
            domain.model_copy(update={"partition": _PARTITION}) for domain in pool.resource_domains
        )
        changed = pool.model_copy(
            update={
                "partition": _PARTITION,
                "pool_digest": "0" * 64,
                "resource_domains": domains,
            }
        )
        pools.append(
            changed.model_copy(
                update={"pool_digest": canonical_digest_excluding(changed, "pool_digest")}
            )
        )
    pool_by_id = {pool.pool_id: pool for pool in pools}
    template = source.development_subject_template
    if template is not None:
        profiles = []
        for profile in template.profiles:
            changed_profile = profile.model_copy(
                update={
                    "pool_digest": pool_by_id[profile.pool_id].pool_digest,
                    "profile_digest": "0" * 64,
                }
            )
            profiles.append(
                changed_profile.model_copy(
                    update={
                        "profile_digest": canonical_digest_excluding(
                            changed_profile, "profile_digest"
                        )
                    }
                )
            )
        template = template.model_copy(update={"profiles": tuple(profiles)})
    changed_fleet = source.model_copy(
        update={
            "authority_incarnation": authority_incarnation,
            "development_subject_template": template,
            "fleet_digest": "0" * 64,
            "pools": tuple(pools),
        }
    )
    return changed_fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(changed_fleet, "fleet_digest")}
    )


def _executor_profile_seed(
    desired: ProtectedStagingDesiredConfiguration,
    *,
    bundle: ExecutionCredentialBundle,
    controller_transports: Mapping[str, _ControllerPrerequisiteTransport],
) -> CapacityPoolExecutorProfileSeed:
    image = f"{_CONTAINER_REGISTRY}/loom-capacity-executor@sha256:{_EXECUTOR_IMAGE_DIGEST}"
    original = _executor_seed(desired=desired, executor_image=image)
    bindings = []
    for binding in original.pools:
        pool_id = binding.pool_id
        transport = controller_transports[pool_id]
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(
            bundle.ownership_private_keys[pool_id]
        )
        signing_key_sha256 = public_key_fingerprint(private_key.public_key())
        inventory = binding.inventory.model_copy(
            update={
                "controller_cluster": _POOL_CLUSTER[pool_id],
                "query_uid": os.geteuid(),
                "relevant_partitions": (_PARTITION,),
                "scontrol_sha256": transport.executable_sha256["scontrol"],
                "squeue_sha256": transport.executable_sha256["squeue"],
                "slurm_conf_sha256": transport.configuration_sha256["slurm.conf"],
            }
        )
        local_authority = controller_local_authority_sha256(
            pool_id=pool_id,
            architecture=_POOL_ARCHITECTURE[pool_id],
            controller_hostname=_POOL_CONTROLLER[pool_id],
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            slurm_cluster=_POOL_CLUSTER[pool_id],
            partition=_PARTITION,
            target_nodes=_POOL_NODES[pool_id],
            executable_sha256=transport.executable_sha256,
            configuration_sha256=transport.configuration_sha256,
            job_visibility_evidence_sha256=inventory.job_visibility_evidence_sha256,
        )
        bindings.append(
            binding.model_copy(
                update={
                    "controller_host": _POOL_CONTROLLER[pool_id],
                    "inventory": inventory,
                    "local_authority_sha256": local_authority,
                    "local_uid": os.geteuid(),
                    "partition": _PARTITION,
                    "signing_key_sha256": signing_key_sha256,
                    "slurm_cluster": _POOL_CLUSTER[pool_id],
                }
            )
        )
    return replace(
        original,
        authority_incarnation=str(desired.fleet.authority_incarnation),
        pools=tuple(bindings),
    )


def _attestation(
    *,
    artifact: ProtectedExecutionPrerequisiteArtifact,
    publication_path: Path,
    lease: BackupLease,
    image_digests: Mapping[str, str],
) -> PreflightAttestation:
    check = RegisteredCheck(
        spec=CheckSpec(
            check_id="candidate.identity",
            failure_code="candidate.identity.drift",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha",),
            evidence_schema=(EvidenceField("ready", "boolean"),),
            timeout_seconds=5,
            freshness_ttl_seconds=600,
            remediation="restore the exact candidate identity",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="frozen-harness-v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={"ready": True},
            )
        },
    )
    executions = PreflightDag((check,)).run(
        CheckContext({"candidate.sha": "a" * 40}),
        now=lambda: NOW,
    )
    base = _base_attestation(artifact, execution_prerequisite_path=publication_path)
    bindings = replace(
        base.bindings,
        image_digests=dict(image_digests),
        backup_lease_id=lease.lease_id,
        backup_lease_digest=lease.evidence_digest,
        backup_manifest_sha256=lease.manifest_sha256,
        backup_component_set_digest=component_set_digest(lease.component_sha256),
        db_snapshot_identity=lease.db_snapshot_identity,
        schema_revision=lease.schema_revision,
        object_inventory_root=lease.object_inventory_root,
        checkpoint_schema_version=lease.checkpoint_schema_version,
        checkpoint_component_sha256=lease.component_sha256,
        database_authority_digest=lease.database_authority_digest,
        public_schema_revision=lease.public_schema_revision,
        capacity_guard_schema_revision=lease.capacity_guard_schema_revision,
        manager_configuration_epoch=lease.manager_configuration_epoch,
        manager_configuration_digest=lease.manager_configuration_digest,
        manager_authority_incarnation=str(lease.manager_authority_incarnation),
        manager_writer_epoch=lease.manager_writer_epoch,
        manager_execution_state=lease.manager_execution_state,
        manager_execution_epoch=lease.manager_execution_epoch,
        manager_execution_manifest_sha256=lease.manager_execution_manifest_sha256,
        manager_executable_new_capacity_ceiling=(lease.manager_executable_new_capacity_ceiling),
        manager_increase_freeze=lease.manager_increase_freeze,
        restore_report_sha256=lease.restore_report_sha256,
    )
    return PreflightAttestation.issue(
        bindings=bindings,
        executions=executions,
        issued_at=NOW,
        registry_digest="9" * 64,
        coverage_digest="a" * 64,
    )


async def _reset_capacity_database(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    authority_incarnation: UUID,
) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(
            text(
                "ALTER TABLE capacity_authority_state DISABLE TRIGGER "
                "capacity_authority_execution_transition_guard"
            )
        )
        try:
            await session.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(
                    authority_incarnation=authority_incarnation,
                    writer_epoch=0,
                    recovery_state="shadow",
                    increase_freeze=True,
                    increase_freeze_reason="frozen_integration_harness",
                    executable_new_capacity_ceiling=0,
                    execution_epoch=0,
                    execution_state="shadow",
                    execution_manifest_sha256=None,
                    global_pending_slot_ceiling=0,
                    global_pending_job_ceiling=0,
                    global_submission_rate_ceiling=0,
                )
            )
            for table in reversed(Base.metadata.sorted_tables):
                if table.name == CapacityAuthorityState.__tablename__:
                    continue
                await session.execute(text(f"ALTER TABLE {table.name} DISABLE TRIGGER USER"))
                try:
                    await session.execute(delete(table))
                finally:
                    await session.execute(text(f"ALTER TABLE {table.name} ENABLE TRIGGER USER"))
        finally:
            await session.execute(
                text(
                    "ALTER TABLE capacity_authority_state ENABLE TRIGGER "
                    "capacity_authority_execution_transition_guard"
                )
            )


async def _initialize_manager_database(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    fleet: FleetManifestV1,
    artifact: ProtectedExecutionPrerequisiteArtifact,
    bundle: ExecutionCredentialBundle,
) -> None:
    await _reset_capacity_database(
        session_factory,
        authority_incarnation=fleet.authority_incarnation,
    )
    store = CapacityManagementStore(
        execution_policy=artifact.execution_policy,
        execution_policy_sha256=artifact.execution_policy_sha256,
    )
    async with session_factory() as session:
        proposal = await store.propose_fleet_configuration(
            session,
            fleet,
            actor="frozen-harness",
            idempotency_key=UUID(int=9801),
        )
        active = await store.activate_configuration(
            session,
            configuration_activation(
                fleet=proposal,
                subjects=(),
                static_candidate_provenance=(),
            ),
            actor="frozen-harness",
            idempotency_key=UUID(int=9802),
        )
        if active.configuration_epoch != 1:
            raise RuntimeError("frozen manager source configuration epoch drifted")
        writer = await store.register_writer(
            session,
            fleet.authority_incarnation,
            expected_epoch=0,
        )
        public_keys = {}
        bindings = {item.pool_id: item for item in artifact.executor_profile_seed.pools}
        for pool_id in _POOL_ORDER:
            binding = bindings[pool_id]
            private_key = ed25519.Ed25519PrivateKey.from_private_bytes(
                bundle.ownership_private_keys[pool_id]
            )
            public_keys[binding.signing_key_id] = private_key.public_key()
        grant_store = CapacityGrantStore(ownership_keyring=OwnershipKeyring(public_keys))
        for index, pool_id in enumerate(_POOL_ORDER, start=1):
            binding = bindings[pool_id]
            await grant_store.register_executor(
                session,
                writer,
                DryRunExecutorRegistrationV1(
                    executor_id=binding.executor_id,
                    executor_incarnation=UUID(binding.executor_incarnation),
                    pool_id=pool_id,
                    pool_generation=binding.pool_generation,
                    signing_key_id=binding.signing_key_id,
                    signing_key_sha256=binding.signing_key_sha256,
                    local_authority_sha256=binding.local_authority_sha256,
                ),
                actor="frozen-harness",
                idempotency_key=UUID(int=9810 + index),
            )
        await session.commit()


@dataclass(slots=True)
class FrozenProtectedAutoscalingHarness:
    """Exercise the protected runtime against real contracts and frozen boundaries."""

    runtime: KubernetesProtectedStagingCapacityRuntime
    plan: FinalGatePlan
    artifact: ProtectedExecutionPrerequisiteArtifact
    kubernetes: _FrozenKubernetes
    manager_client: TestClient
    manager_mutations: _ManagerMutationCounter
    external_authority: _ExternalExecutionAuthority
    bundle: ExecutionCredentialBundle
    controller_transports: Mapping[str, _ControllerPrerequisiteTransport]
    prepared_transports: Mapping[str, _PreparedControllerTransport]
    pool_credential_root: Path
    slurm: Mapping[str, FakeSlurm]
    capacity_session_factory: async_sessionmaker[AsyncSession]
    _closed: bool = False

    @classmethod
    async def create(
        cls,
        tmp_path: Path,
        *,
        capacity_postgres_url: str,
        capacity_session_factory: async_sessionmaker[AsyncSession],
    ) -> FrozenProtectedAutoscalingHarness:
        candidate = _candidate(tmp_path)
        state_root = tmp_path / "state"

        class _BootstrapRunner:
            environment = _FrozenKubernetes.environment

        bootstrap_runtime = KubernetesProtectedStagingCapacityRuntime(
            runner=_BootstrapRunner(),  # type: ignore[arg-type]
            state_root=state_root,
            candidate_root=candidate,
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            container_registry=_CONTAINER_REGISTRY,
        )
        # These lifecycle scenarios exercise certificate validation on every
        # readback. Fresh EC identities retain that coverage without repeatedly
        # paying RSA private-key validation costs; the bootstrap unit tests also
        # retain explicit RSA coverage.
        _write_bootstrap(
            bootstrap_runtime,
            key_factory=lambda: ec.generate_private_key(ec.SECP256R1()),
        )
        bootstrap_runtime._create_credential_seed()
        seed = bootstrap_runtime.read_credential_seed()
        bundle = bootstrap_runtime.read_execution_credential_bundle()
        authority_incarnation = UUID(str(seed["authority_incarnation"]))
        source_fleet = _source_fleet(authority_incarnation)
        active_document = {
            "schema_version": 1,
            "configuration": ConfigurationSnapshotV1(
                configuration_epoch=1,
                fleet=ConfigurationGenerationRefV1(
                    scope="fleet",
                    generation=source_fleet.fleet_generation,
                    digest=canonical_digest(source_fleet),
                ),
                subjects=(),
            ).model_dump(mode="json"),
            "fleet": source_fleet.model_dump(mode="json"),
            "subjects": [],
        }
        desired = derive_protected_staging_capacity_configuration(
            active_document=active_document,
            seed_values=seed,
            target_generation=8,
        )
        slurm = {
            pool_id: FakeSlurm(
                tmp_path / "fake-slurm" / pool_id,
                cluster=_POOL_CLUSTER[pool_id],
                controller=_POOL_CONTROLLER[pool_id],
                partition=_PARTITION,
                account=next(
                    pool.association for pool in desired.fleet.pools if pool.pool_id == pool_id
                ),
                submitter="loom",
                qos="loom",
                next_job_id=1000 if pool_id == "gb10" else 2000,
            )
            for pool_id in _POOL_ORDER
        }
        slurm["gb10"].add_foreign_job("9001")
        slurm["oldlab"].add_foreign_job("9002")
        controller_transports = {
            pool_id: _ControllerPrerequisiteTransport(pool_id, slurm[pool_id], gid=os.getegid())
            for pool_id in _POOL_ORDER
        }
        executor_seed = _executor_profile_seed(
            desired,
            bundle=bundle,
            controller_transports=controller_transports,
        )
        staging = desired.staging_subject
        acknowledgement = SubjectExecutionAcknowledgementV2(
            subject_id=staging.subject_id,
            subject_incarnation=staging.subject_incarnation,
            configuration_generation=staging.configuration_generation,
            deployment_generation=staging.deployment_generation,
            candidate=CandidateBindingV2(
                algorithm="git-sha1",
                identity="a" * 40,
                publication_sha256="e" * 64,
            ),
            reporter_incarnation=staging.demand_reporter_incarnation,
            protected_admission_sha256=_PROTECTED_ADMISSION,
            legacy_writer_high_water=17,
            acknowledgement_sha256="3" * 64,
        )
        fence = LegacyWriterFenceV2(
            writer_id="global-dev-supervisor",
            writer_kind="allocation",
            scope_kind="global",
            scope_id="development",
            high_water=17,
            freeze_evidence_sha256="4" * 64,
            state="frozen",
        )
        authority = ProtectedExecutionPrerequisiteAuthority(
            executor_profile_seed=executor_seed,
            subject_acknowledgements=(acknowledgement,),
            manager_client_cidrs={
                "gb10": "192.168.60.11/32",
                "oldlab": "192.168.50.103/32",
                "operator": "192.168.50.103/32",
            },
            credential_metadata_sha256=bundle.metadata_sha256,
            coexistence_witness_sha256={"gb10": "5" * 64, "oldlab": "6" * 64},
            legacy_writer_fences=(fence,),
        )
        base_plan = _plan(tmp_path)
        lease = _lease(plan=base_plan, desired=desired)
        prerequisite_store = ProtectedExecutionPrerequisiteStore(
            state_root,
            service_uid=os.geteuid(),
        )
        source = ProtectedExecutionPrerequisiteRuntimeSource(
            store=prerequisite_store,
            candidate_sha=base_plan.candidate_sha,
            candidate_tree=base_plan.candidate_tree,
            core_artifact_bundle_sha256=base_plan.artifact_bundle_digest,
            mutation_epoch=base_plan.starting_mutation_epoch,
            executor_image_sha256=_EXECUTOR_IMAGE_DIGEST,
            container_registry=_CONTAINER_REGISTRY,
            manager_configuration_source=lambda: deepcopy(active_document),
            configuration_seed_source=lambda: deepcopy(seed),
            staging_protected_admission_source=lambda _seed: _PROTECTED_ADMISSION,
            authority_source=lambda _desired: authority,
            now=lambda: datetime(2026, 9, 3, 12, tzinfo=UTC),
        )
        publication = source.publish(lease)
        artifact = prerequisite_store.read(publication)
        image_digests = {
            "api": "sha256:" + "1" * 64,
            "loom-control-plane": "sha256:" + f"{1:064x}",
            "loom-staging-admin-browser-smoke": "sha256:" + "8" * 64,
            "loom-capacity-manager": _MANAGER_IMAGE_DIGEST,
            "loom-capacity-executor": "sha256:" + _EXECUTOR_IMAGE_DIGEST,
        }
        attestation = _attestation(
            artifact=artifact,
            publication_path=publication.path,
            lease=lease,
            image_digests=image_digests,
        )
        plan = FinalGatePlan.build(
            _envelope(attestation),
            attestation,
            _artifacts(tmp_path),
            lease,
            _baseline(),
            _systemd_evidence(),
            _predecessor_evidence(),
            execution_prerequisite_publication=publication,
            execution_prerequisite_store=prerequisite_store,
        )

        await _initialize_manager_database(
            capacity_session_factory,
            fleet=source_fleet,
            artifact=artifact,
            bundle=bundle,
        )
        base_registry = _base_registry(bootstrap_runtime.credentials_root)
        execution_registry = build_execution_principal_registry(
            base_registry,
            bundle=bundle,
            pools=artifact.executor_profile_seed.pools,
        )
        final_registry = _principal_registry_with_staging_reporter(
            execution_registry,
            seed=seed,
        )
        ownership_keyring = build_execution_ownership_keyring(
            b'{"keys":[],"schema_version":1}\n',
            bundle=bundle,
            pools=artifact.executor_profile_seed.pools,
        )
        settings_root = tmp_path / "manager-settings"
        settings_root.mkdir(mode=0o700)
        settings = CapacityManagerSettings(
            principals_file=_owner_file(settings_root / "principals.json", final_registry),
            db_url_file=_owner_file(settings_root / "database-url", capacity_postgres_url),
            expected_authority_incarnation=authority_incarnation,
            tls_cert_file=_owner_file(settings_root / "server.crt", "test"),
            tls_key_file=_owner_file(settings_root / "server.key", "test"),
            tls_client_ca_file=_owner_file(settings_root / "client-ca.crt", "test"),
            ownership_public_keys_file=_owner_file(
                settings_root / "ownership-public-keys.json", ownership_keyring
            ),
            execution_policy_file=_owner_file(
                settings_root / "execution-policy.json",
                canonical_executable_bytes(artifact.execution_policy),
            ),
            execution_policy_sha256=canonical_executable_digest(artifact.execution_policy),
            freshness_seconds=120,
        )
        app = create_app(settings)
        manager_client = TestClient(app)
        manager_client.__enter__()
        health = manager_client.get("/healthz")
        if health.status_code != 200:
            manager_client.__exit__(None, None, None)
            raise RuntimeError("frozen capacity manager failed to start")

        kubernetes = _FrozenKubernetes(
            candidate,
            authority_incarnation=authority_incarnation,
            principal_registry=base_registry,
        )
        mutation_counter = _ManagerMutationCounter()

        @contextmanager
        def manager_context(**kwargs: object) -> Iterator[ProtectedCapacityManagerClient]:
            yield ProtectedCapacityManagerClient(
                origin="https://127.0.0.1:18443",
                credentials_root=Path(str(kwargs["credentials_root"])),
                service_uid=int(str(kwargs["service_uid"])),
                service_gid=int(str(kwargs["service_gid"])),
                client_factory=lambda _context: _TestClientHTTP(
                    manager_client,
                    mutation_counter,
                ),
            )

        pool_credential_root = tmp_path / "controller-runtime"
        pool_credential_root.mkdir(mode=0o700)
        pool_credential_transports = {
            pool_id: FixedLocalPoolCredentialTransport(
                pool_id=pool_id,
                target_directory=pool_credential_root / pool_id,
                service_uid=os.geteuid(),
                service_gid=os.getegid(),
            )
            for pool_id in _POOL_ORDER
        }
        prepared_transports = {
            pool_id: _PreparedControllerTransport(
                pool_id,
                slurm[pool_id],
                manager_client=manager_client,
                executor_token=bundle.clients[f"pool-executor-{pool_id}"].bearer_token,
                mutation_counter=mutation_counter,
            )
            for pool_id in _POOL_ORDER
        }
        external_authority = _ExternalExecutionAuthority(
            artifact=artifact,
            legacy_writer_fences=artifact.execution_policy.legacy_writer_fences,
            coexistence_witness_sha256=dict(artifact.coexistence_witness_sha256),
        )

        def external_dependency_guard(
            _plan: FinalGatePlan,
            observed_artifact: ProtectedExecutionPrerequisiteArtifact,
        ) -> str:
            return external_authority.evidence(observed_artifact)

        runtime = KubernetesProtectedStagingCapacityRuntime(
            runner=kubernetes,
            state_root=state_root,
            candidate_root=candidate,
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            container_registry=_CONTAINER_REGISTRY,
            manager_configuration_client_context=manager_context,
            controller_prerequisite_transports=controller_transports,
            pool_credential_transports=pool_credential_transports,
            prepared_controller_transports=prepared_transports,
            execution_preparation_dependency_guard=external_dependency_guard,
        )
        return cls(
            runtime=runtime,
            plan=plan,
            artifact=artifact,
            kubernetes=kubernetes,
            manager_client=manager_client,
            manager_mutations=mutation_counter,
            external_authority=external_authority,
            bundle=bundle,
            controller_transports=controller_transports,
            prepared_transports=prepared_transports,
            pool_credential_root=pool_credential_root,
            slurm=slurm,
            capacity_session_factory=capacity_session_factory,
        )

    @property
    def pool_nodes(self) -> dict[str, tuple[str, ...]]:
        return {
            binding.pool_id: tuple(
                sorted(
                    (node.node_id for node in binding.inventory.nodes),
                    key=lambda node_id: int(node_id.rsplit("-", 1)[1]),
                )
            )
            for binding in self.artifact.executor_profile_seed.pools
        }

    def _epoch_guard(self, plan: FinalGatePlan) -> ComponentObservation:
        return ComponentObservation(
            state=ComponentState.EXACT,
            evidence_digest=_hash_json(
                {
                    "attempt": plan.attempt_number,
                    "epoch": plan.starting_mutation_epoch + 1,
                    "request": plan.request_id,
                }
            ),
            observed_epoch=plan.starting_mutation_epoch + 1,
        )

    def converge_frozen_execution_path(self) -> dict[str, ComponentState]:
        return self._converge_components(_FROZEN_EXECUTION_COMPONENTS)

    def converge_frozen_prerequisites(self) -> dict[str, ComponentState]:
        return self._converge_components(_FROZEN_PREREQUISITE_COMPONENTS)

    def apply_execution_preparation(self) -> None:
        component = next(
            item
            for item in self.runtime.components(self.plan, epoch_guard=self._epoch_guard)
            if item.component_id == "capacity-execution-preparation"
        )
        component.apply(self.plan)

    def _converge_components(
        self,
        wanted: frozenset[str],
    ) -> dict[str, ComponentState]:
        observations: dict[str, ComponentState] = {}
        for component in self.runtime.components(self.plan, epoch_guard=self._epoch_guard):
            if component.component_id not in wanted:
                continue
            before = component.classify(self.plan)
            if before.state is ComponentState.DRIFTED:
                raise RuntimeError(f"frozen component {component.component_id} drifted")
            if before.state is ComponentState.READY:
                component.apply(self.plan)
            after = component.classify(self.plan)
            if after.state is not ComponentState.EXACT:
                raise RuntimeError(f"frozen component {component.component_id} did not converge")
            observations[component.component_id] = after.state
        if set(observations) != wanted:
            raise RuntimeError("frozen protected runtime component coverage is incomplete")
        return observations

    def _manager(self) -> ProtectedCapacityManagerClient:
        return ProtectedCapacityManagerClient(
            origin="https://127.0.0.1:18443",
            credentials_root=self.runtime.credentials_root,
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            client_factory=lambda _context: _TestClientHTTP(
                self.manager_client,
                self.manager_mutations,
            ),
        )

    def manager_status(self) -> dict[str, object]:
        return self._manager().get_status()

    def fail_next_prepared_file_convergence(self, pool_id: str) -> None:
        if pool_id not in _POOL_ORDER:
            raise ValueError("frozen prepared-controller pool is invalid")
        self.prepared_transports[pool_id].fail_next_files = True

    def stale_coexistence_witness(self, pool_id: str) -> None:
        if pool_id not in _POOL_ORDER:
            raise ValueError("frozen coexistence-witness pool is invalid")
        self.external_authority.coexistence_witness_sha256[pool_id] = "7" * 64

    def stale_legacy_writer_high_water(self) -> None:
        first, *remaining = self.external_authority.legacy_writer_fences
        self.external_authority.legacy_writer_fences = (
            first.model_copy(update={"high_water": first.high_water + 1}),
            *remaining,
        )

    def cross_manager_route(self) -> None:
        ingress = self.kubernetes.resources[
            ("NetworkPolicy", "loom-capacity-router", "capacity-manager-router-ingress")
        ]
        spec = ingress["spec"]
        assert isinstance(spec, dict)
        rules = spec["ingress"]
        assert isinstance(rules, list)
        sources = rules[0]["from"]
        sources[0]["ipBlock"]["cidr"] = "192.168.60.12/32"

    def cross_manager_certificate(self) -> None:
        self.kubernetes.secret_data["server-certificate.pem"] = base64.b64encode(
            _server_certificate(include_router_ip=False)
        ).decode("ascii")

    def cross_pool_credential(
        self,
        *,
        source_pool: str,
        target_pool: str,
        credential: str,
    ) -> None:
        if (
            source_pool not in _POOL_ORDER
            or target_pool not in _POOL_ORDER
            or source_pool == target_pool
            or credential not in {"bearer-token", "ownership-private-key"}
        ):
            raise ValueError("frozen cross-pool credential request is invalid")
        source = self.pool_credential_root / source_pool / credential
        target = self.pool_credential_root / target_pool / credential
        target.write_bytes(source.read_bytes())
        target.chmod(0o600)

    def manager_mutation_paths(self) -> tuple[str, ...]:
        return tuple(self.manager_mutations.paths)

    def foreign_job_snapshots(self) -> dict[str, tuple[dict[str, object], ...]]:
        return {pool_id: self.slurm[pool_id].foreign_jobs() for pool_id in _POOL_ORDER}

    def prepared_timer_states(self) -> dict[str, tuple[str, bool]]:
        return {
            pool_id: (
                "enabled" if transport.timer_enabled else "disabled",
                transport.timer_enabled,
            )
            for pool_id, transport in self.prepared_transports.items()
        }

    def active_executor_services(self) -> dict[str, tuple[str, ...]]:
        return {
            pool_id: ()
            if transport.request is None
            else tuple(
                name
                for name, state in (
                    transport.observe(transport.request).unit_active_state.items()  # type: ignore[union-attr]
                )
                if state == "active" and not name.endswith(".timer")
            )
            for pool_id, transport in self.prepared_transports.items()
        }

    @property
    def manager_routes(self) -> set[str]:
        ingress = self.kubernetes.resources.get(
            ("NetworkPolicy", "loom-capacity-router", "capacity-manager-router-ingress")
        )
        router = self.kubernetes.resources.get(
            ("Deployment", "loom-capacity-router", "loom-capacity-manager-router")
        )
        if ingress is None or router is None:
            return set()
        routes = {
            item["ipBlock"]["cidr"] for rule in ingress["spec"]["ingress"] for item in rule["from"]
        }
        args = router["spec"]["template"]["spec"]["containers"][0]["args"]
        routed_addresses = set(args[1::2])
        return (
            routes if routed_addresses == {route.removesuffix("/32") for route in routes} else set()
        )

    @property
    def manager_certificate_has_router_ip(self) -> bool:
        payload = base64.b64decode(
            self.kubernetes.secret_data["server-certificate.pem"], validate=True
        )
        certificate = x509.load_pem_x509_certificate(payload)
        sans = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        return "192.168.50.103" in {str(item) for item in sans.get_values_for_type(x509.IPAddress)}

    @property
    def execution_credentials_are_separated(self) -> bool:
        registry = json.loads(
            base64.b64decode(self.kubernetes.secret_data["principals.json"], validate=True)
        )
        principals = {item["principal_id"]: item for item in registry["principals"]}
        expected = {
            "manager-read": "capacity:read",
            "manager-prepare": "capacity:execution:prepare",
            "manager-activate": "capacity:execution:activate",
            "manager-drain": "capacity:execution:drain",
            "manager-retire": "capacity:execution:retire",
            "manager-abort": "capacity:execution:abort",
        }
        return all(
            principals[name]["scopes"] == [scope]
            and principals[name]["token_sha256"] == _sha256(self.bundle.clients[name].bearer_token)
            for name, scope in expected.items()
        ) and len({principals[name]["token_sha256"] for name in expected}) == len(expected)

    @property
    def pool_credentials_are_separated(self) -> bool:
        gb10 = self.pool_credential_root / "gb10"
        oldlab = self.pool_credential_root / "oldlab"
        return (
            gb10.is_dir()
            and oldlab.is_dir()
            and (gb10 / "bearer-token").read_bytes() != (oldlab / "bearer-token").read_bytes()
            and (gb10 / "ownership-private-key").read_bytes()
            != (oldlab / "ownership-private-key").read_bytes()
        )

    def mutation_counts(self) -> dict[str, int]:
        return {
            "controller-prerequisites": sum(
                transport.converge_calls for transport in self.controller_transports.values()
            ),
            "kubernetes": self.kubernetes.mutations,
            "manager-http": self.manager_mutations.count,
            "prepared-controllers": sum(
                transport.mutations for transport in self.prepared_transports.values()
            ),
            "pool-credential-files": sum(
                len(tuple((self.pool_credential_root / pool_id).iterdir()))
                for pool_id in _POOL_ORDER
            ),
        }

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.manager_client.__exit__(None, None, None)
        await _reset_capacity_database(
            self.capacity_session_factory,
            authority_incarnation=AUTHORITY_ID,
        )


__all__ = ["FrozenProtectedAutoscalingHarness"]
