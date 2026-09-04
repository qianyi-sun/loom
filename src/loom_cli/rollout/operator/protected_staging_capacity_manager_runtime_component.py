"""Protected convergence of the staging capacity-manager runtime."""

from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, cast
from uuid import UUID

from pydantic import ValidationError

from loom_capacity_manager.auth import _RegistryDocument
from loom_cli.capacity_control_plane import (
    _manager_deployment,
    load_capacity_control_plane_profile,
)

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import ComponentState
from .protected_execution_prerequisites import ProtectedExecutionPrerequisiteArtifact
from .protected_staging_capacity_manager_policy_component import (
    KubernetesProtectedStagingCapacityManagerPolicyComponent,
    ManagerPolicyRuntimeAuthority,
)

_PRINCIPAL_ID = "staging-demand-reporter"
_NAMESPACE = "loom-dev"
_NAME = "loom-capacity-manager"
_FIELD_MANAGER = "loom-staging-capacity-manager-runtime"
_EXECUTION_CREDENTIAL_FIELD_MANAGER = "loom-staging-capacity-execution-credentials"
_PROFILE_PATH = Path("deploy/dev-fleet/capacity-control-plane.toml")
_REGISTRY_ANNOTATION = "loom.yylx.dev/principal-registry-sha256"
_REQUEST_TIMEOUT = "60s"
_COMMAND_TIMEOUT_SECONDS = 60.0
_ROLLOUT_TIMEOUT_SECONDS = 660.0
_MAX_REGISTRY_BYTES = 1024 * 1024
_MAX_SECRET_FIELD_BYTES = 4 * 1024 * 1024
_ROLLBACK_SCOPE = "capacity:configure:rollback"
_RESOURCE_VERSION_RE = re.compile(r"^[1-9][0-9]{0,31}$")
_UID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_SECRET_KEYS = frozenset(
    {
        "client-ca.pem",
        "database-url",
        "global-execution-signing-key",
        "health-certificate.pem",
        "health-private-key.pem",
        "ownership-public-keys.json",
        "postgres-database",
        "postgres-password",
        "postgres-user",
        "principals.json",
        "server-ca.pem",
        "server-certificate.pem",
        "server-private-key.pem",
    }
)
_SECRET_MANAGER_CONTRACTS = frozenset(
    {
        ("kubectl-create", "Update", "v1", None),
        ("kubectl-replace", "Update", "v1", None),
        ("kubectl-patch", "Update", "v1", None),
        (_FIELD_MANAGER, "Update", "v1", None),
        (_EXECUTION_CREDENTIAL_FIELD_MANAGER, "Update", "v1", None),
    }
)
_DEPLOYMENT_MANAGER_CONTRACTS = frozenset(
    {
        ("loom-capacity-control-plane", "Apply", "apps/v1", None),
        ("kubectl-client-side-apply", "Update", "apps/v1", None),
        ("kubectl-rollout", "Update", "apps/v1", None),
        (_FIELD_MANAGER, "Update", "apps/v1", None),
        ("k3s", "Update", "apps/v1", "status"),
    }
)
_REMOVABLE_DEPLOYMENT_ANNOTATIONS = frozenset(
    {
        "deployment.kubernetes.io/revision",
        "kubectl.kubernetes.io/last-applied-configuration",
    }
)
_REMOVABLE_TEMPLATE_ANNOTATIONS = frozenset({"kubectl.kubernetes.io/restartedAt"})


class ProtectedStagingCapacityManagerCommandRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...

    def capture_stdout_with_input(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes,
        timeout_seconds: float,
    ) -> bytes: ...

    def run_checked(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> None: ...

    def run_status(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> int: ...


@dataclass(frozen=True, slots=True)
class _SecretSnapshot:
    uid: str
    resource_version: str
    current_registry: str
    desired_registry: str
    desired_registry_bytes: bytes
    dedicated_owner: bool
    exact: bool
    evidence_digest: str
    server_certificate: bytes | None = None


@dataclass(frozen=True, slots=True)
class _DeploymentSnapshot:
    uid: str
    resource_version: str
    exact: bool
    healthy: bool
    dedicated_owner: bool
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class _SeedSnapshot:
    values: Mapping[str, object]
    identity_digest: str


@dataclass(frozen=True, slots=True)
class _Snapshot:
    seed: _SeedSnapshot
    secret: _SecretSnapshot
    deployment: _DeploymentSnapshot
    desired_deployment: dict[str, object]
    state: ComponentState


@dataclass(frozen=True, slots=True)
class KubernetesProtectedStagingCapacityManagerRuntimeComponent:
    runner: ProtectedStagingCapacityManagerCommandRunner
    candidate_root: Path
    container_registry: str
    seed_reader: Callable[[], dict[str, object]]
    prerequisite_reader: (
        Callable[[FinalGatePlan], ProtectedExecutionPrerequisiteArtifact] | None
    ) = None
    manager_status_reader: Callable[[], Mapping[str, object]] | None = None

    def __post_init__(self) -> None:
        if (
            not self.candidate_root.is_absolute()
            or ".." in self.candidate_root.parts
            or not self.container_registry
            or any(item in self.container_registry for item in ("\r", "\n", "\x00"))
            or not callable(self.seed_reader)
            or (self.prerequisite_reader is not None and not callable(self.prerequisite_reader))
            or (self.manager_status_reader is not None and not callable(self.manager_status_reader))
            or "KUBECONFIG" not in self.runner.environment
        ):
            raise ValueError("protected staging capacity manager authority is invalid")

    def classify(self, plan: FinalGatePlan) -> tuple[ComponentState, str]:
        if plan.schema_version == 7:
            return self._classify_policy_runtime(plan)
        try:
            snapshot = self._snapshot(plan)
        except (OSError, RuntimeError, UnicodeError, ValueError, KeyError):
            return ComponentState.DRIFTED, _hash_json({"status": "observation-failed"})
        return snapshot.state, _hash_json(
            {
                "deployment": snapshot.deployment.evidence_digest,
                "secret": snapshot.secret.evidence_digest,
                "state": snapshot.state.value,
            }
        )

    def apply(self, plan: FinalGatePlan) -> None:
        if plan.schema_version == 7:
            self._apply_policy_runtime(plan)
            return
        try:
            snapshot = self._snapshot(plan)
        except (OSError, RuntimeError, UnicodeError, ValueError, KeyError) as exc:
            raise RuntimeError("protected capacity manager state changed before apply") from exc
        if snapshot.state is not ComponentState.READY:
            raise RuntimeError("protected capacity manager state changed before apply")
        applied_secret: _SecretSnapshot | None = None
        if not snapshot.secret.exact:
            self._require_seed_unchanged(snapshot.seed)
            applied_secret_payload = self.runner.capture_stdout_with_input(
                self._secret_patch_argv(),
                env=self.runner.environment,
                input_payload=_secret_patch(snapshot.secret),
                timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
            )
            secret = _parse_secret(applied_secret_payload, seed=snapshot.seed.values)
            if not secret.exact:
                raise RuntimeError("protected capacity manager Secret did not converge")
            applied_secret = secret
            if not self._seed_is_unchanged(snapshot.seed):
                self._restore_secret_after_seed_change(
                    before=snapshot.secret,
                    after=secret,
                    seed=snapshot.seed,
                )
                raise RuntimeError(
                    "protected capacity manager Secret was restored after credential seed changed"
                )
        if not snapshot.deployment.exact or not snapshot.deployment.dedicated_owner:
            if not self._seed_is_unchanged(snapshot.seed):
                if applied_secret is not None:
                    self._restore_secret_after_seed_change(
                        before=snapshot.secret,
                        after=applied_secret,
                        seed=snapshot.seed,
                    )
                    raise RuntimeError(
                        "protected capacity manager Secret was restored after credential seed changed"
                    )
                raise RuntimeError(
                    "protected capacity manager credential seed changed before mutation"
                )
            desired = copy.deepcopy(snapshot.desired_deployment)
            desired_metadata = desired["metadata"]
            assert isinstance(desired_metadata, dict)
            desired_metadata["uid"] = snapshot.deployment.uid
            desired_metadata["resourceVersion"] = snapshot.deployment.resource_version
            applied_deployment = self.runner.capture_stdout_with_input(
                self._deployment_replace_argv(dry_run=False),
                env=self.runner.environment,
                input_payload=_json_bytes(desired),
                timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
            )
            applied_identity = _parse_deployment_identity(applied_deployment)
            if (
                applied_identity[0] != snapshot.deployment.uid
                or applied_identity[1] == snapshot.deployment.resource_version
            ):
                raise RuntimeError("protected capacity manager Deployment identity changed")
        self.runner.run_checked(
            (
                "kubectl",
                "--namespace",
                _NAMESPACE,
                "rollout",
                "status",
                f"deployment/{_NAME}",
                "--timeout=600s",
                f"--request-timeout={_REQUEST_TIMEOUT}",
            ),
            env=self.runner.environment,
            input_payload=None,
            timeout_seconds=_ROLLOUT_TIMEOUT_SECONDS,
        )
        after = self._snapshot(plan, seed=snapshot.seed)
        if after.state is not ComponentState.EXACT:
            raise RuntimeError("protected capacity manager runtime did not converge")

    def _classify_policy_runtime(
        self,
        plan: FinalGatePlan,
    ) -> tuple[ComponentState, str]:
        try:
            seed = self._read_seed_snapshot()
            secret = self._read_secret(seed=seed, require_router_certificate=True)
            policy_state, policy_evidence = self._policy_component().classify(plan)
        except (OSError, RuntimeError, UnicodeError, ValueError, KeyError):
            return ComponentState.DRIFTED, _hash_json({"status": "observation-failed"})
        state = (
            ComponentState.DRIFTED
            if policy_state is ComponentState.DRIFTED
            else (
                ComponentState.EXACT
                if secret.exact and policy_state is ComponentState.EXACT
                else ComponentState.READY
            )
        )
        return state, _hash_json(
            {
                "policy": policy_evidence,
                "secret": secret.evidence_digest,
                "state": state.value,
            }
        )

    def _apply_policy_runtime(self, plan: FinalGatePlan) -> None:
        seed = self._read_seed_snapshot()
        secret = self._read_secret(seed=seed, require_router_certificate=True)
        if not secret.exact:
            self._require_seed_unchanged(seed)
            applied_payload = self.runner.capture_stdout_with_input(
                self._secret_patch_argv(),
                env=self.runner.environment,
                input_payload=_secret_patch(secret),
                timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
            )
            applied = _parse_secret(
                applied_payload,
                seed=seed.values,
                require_router_certificate=True,
            )
            if not applied.exact:
                raise RuntimeError("protected capacity manager Secret did not converge")
            if not self._seed_is_unchanged(seed):
                self._restore_secret_after_seed_change(
                    before=secret,
                    after=applied,
                    seed=seed,
                )
                raise RuntimeError(
                    "protected capacity manager Secret was restored after credential seed changed"
                )
        policy = self._policy_component()
        state, _evidence = policy.classify(plan)
        if state is ComponentState.DRIFTED:
            raise RuntimeError("protected capacity manager policy state drifted")
        if state is ComponentState.READY:
            policy.apply(plan)
        after_state, _after_evidence = self._classify_policy_runtime(plan)
        if after_state is not ComponentState.EXACT:
            raise RuntimeError("protected capacity manager policy runtime did not converge")

    def _read_secret(
        self,
        *,
        seed: _SeedSnapshot,
        require_router_certificate: bool,
    ) -> _SecretSnapshot:
        payload = self.runner.capture_stdout(
            self._secret_get_argv(),
            env=self.runner.environment,
            timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
        )
        return _parse_secret(
            payload,
            seed=seed.values,
            require_router_certificate=require_router_certificate,
        )

    def _policy_component(
        self,
    ) -> KubernetesProtectedStagingCapacityManagerPolicyComponent:
        if self.prerequisite_reader is None or self.manager_status_reader is None:
            raise ValueError("protected capacity manager policy authority is unavailable")
        return KubernetesProtectedStagingCapacityManagerPolicyComponent(
            runner=self.runner,
            candidate_root=self.candidate_root,
            container_registry=self.container_registry,
            prerequisite_reader=self.prerequisite_reader,
            runtime_authority_reader=self._read_policy_runtime_authority,
            manager_status_reader=self.manager_status_reader,
        )

    def _read_policy_runtime_authority(self) -> ManagerPolicyRuntimeAuthority:
        seed = self._read_seed_snapshot()
        secret = self._read_secret(seed=seed, require_router_certificate=True)
        if secret.server_certificate is None:
            raise ValueError("protected capacity manager policy certificate is unavailable")
        return ManagerPolicyRuntimeAuthority(
            authority_incarnation=UUID(str(seed.values["authority_incarnation"])),
            principal_registry=secret.desired_registry_bytes,
            server_certificate=secret.server_certificate,
        )

    def _snapshot(self, plan: FinalGatePlan, *, seed: _SeedSnapshot | None = None) -> _Snapshot:
        bound_seed = seed or self._read_seed_snapshot()
        secret_payload = self.runner.capture_stdout(
            self._secret_get_argv(),
            env=self.runner.environment,
            timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
        )
        secret = _parse_secret(secret_payload, seed=bound_seed.values)
        desired_deployment = self._desired_deployment(
            plan,
            bound_seed.values,
            registry=secret.desired_registry_bytes,
        )
        desired_for_server = copy.deepcopy(desired_deployment)
        desired_metadata = desired_for_server["metadata"]
        assert isinstance(desired_metadata, dict)
        deployment_payload = self.runner.capture_stdout(
            self._deployment_get_argv(),
            env=self.runner.environment,
            timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
        )
        deployment_identity = _parse_deployment_identity(deployment_payload)
        desired_metadata["uid"] = deployment_identity[0]
        desired_metadata["resourceVersion"] = deployment_identity[1]
        normalized_desired = self.runner.capture_stdout_with_input(
            self._deployment_replace_argv(dry_run=True),
            env=self.runner.environment,
            input_payload=_json_bytes(desired_for_server),
            timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
        )
        deployment = _parse_deployment(
            deployment_payload,
            normalized_desired=normalized_desired,
        )
        state = (
            ComponentState.EXACT
            if secret.exact
            and deployment.exact
            and deployment.healthy
            and deployment.dedicated_owner
            else ComponentState.READY
        )
        return _Snapshot(
            seed=bound_seed,
            secret=secret,
            deployment=deployment,
            desired_deployment=desired_deployment,
            state=state,
        )

    def _read_seed_snapshot(self) -> _SeedSnapshot:
        seed = self.seed_reader()
        if not isinstance(seed, dict):
            raise ValueError("protected capacity manager seed is invalid")
        values = copy.deepcopy(seed)
        return _SeedSnapshot(
            values=MappingProxyType(values),
            identity_digest=_hash_json(values),
        )

    def _require_seed_unchanged(self, expected: _SeedSnapshot) -> None:
        if not self._seed_is_unchanged(expected):
            raise RuntimeError("protected capacity manager credential seed changed before mutation")

    def _seed_is_unchanged(self, expected: _SeedSnapshot) -> bool:
        try:
            observed = self._read_seed_snapshot()
        except (OSError, RuntimeError, UnicodeError, ValueError, KeyError):
            return False
        return observed.identity_digest == expected.identity_digest

    def _restore_secret_after_seed_change(
        self,
        *,
        before: _SecretSnapshot,
        after: _SecretSnapshot,
        seed: _SeedSnapshot,
    ) -> None:
        try:
            restored_payload = self.runner.capture_stdout_with_input(
                self._secret_patch_argv(),
                env=self.runner.environment,
                input_payload=_secret_patch(after, desired_registry=before.current_registry),
                timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
            )
            restored = _parse_secret(restored_payload, seed=seed.values)
        except (OSError, RuntimeError, UnicodeError, ValueError, KeyError) as exc:
            raise RuntimeError(
                "protected capacity manager Secret compensation lost its fence"
            ) from exc
        if (
            restored.uid != after.uid
            or restored.resource_version == after.resource_version
            or restored.current_registry != before.current_registry
        ):
            raise RuntimeError("protected capacity manager Secret compensation is invalid")

    def _desired_deployment(
        self,
        plan: FinalGatePlan,
        seed: Mapping[str, object],
        *,
        registry: bytes,
    ) -> dict[str, object]:
        authority_incarnation = UUID(str(seed["authority_incarnation"]))
        if (
            authority_incarnation.int == 0
            or str(authority_incarnation) != seed["authority_incarnation"]
        ):
            raise ValueError("protected capacity manager authority incarnation is invalid")
        digest = plan.image_digests["loom-capacity-manager"]
        manager_image = f"{self.container_registry}/loom-capacity-manager@{digest}"
        profile = load_capacity_control_plane_profile(self.candidate_root / _PROFILE_PATH)
        desired = _manager_deployment(
            profile,
            manager_image=manager_image,
            authority_incarnation=authority_incarnation,
        )
        template = desired["spec"]["template"]
        template_metadata = template["metadata"]
        template_metadata["annotations"] = {
            _REGISTRY_ANNOTATION: hashlib.sha256(registry).hexdigest()
        }
        return cast(dict[str, object], desired)

    @staticmethod
    def _secret_get_argv() -> tuple[str, ...]:
        return (
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "get",
            f"secret/{_NAME}",
            "--show-managed-fields",
            "--output=json",
            f"--request-timeout={_REQUEST_TIMEOUT}",
        )

    @staticmethod
    def _deployment_get_argv() -> tuple[str, ...]:
        return (
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "get",
            f"deployment/{_NAME}",
            "--show-managed-fields",
            "--output=json",
            f"--request-timeout={_REQUEST_TIMEOUT}",
        )

    @staticmethod
    def _secret_patch_argv() -> tuple[str, ...]:
        return (
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "patch",
            f"secret/{_NAME}",
            "--type=json",
            f"--field-manager={_FIELD_MANAGER}",
            "--patch-file=/dev/stdin",
            "--show-managed-fields",
            "--output=json",
            f"--request-timeout={_REQUEST_TIMEOUT}",
        )

    @staticmethod
    def _deployment_replace_argv(*, dry_run: bool) -> tuple[str, ...]:
        argv = [
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "replace",
            f"--field-manager={_FIELD_MANAGER}",
            "--show-managed-fields",
            "--output=json",
            "--validate=strict",
            f"--request-timeout={_REQUEST_TIMEOUT}",
            "-f",
            "-",
        ]
        if dry_run:
            argv.insert(argv.index("--show-managed-fields"), "--dry-run=server")
        return tuple(argv)


def _parse_secret(
    payload: bytes,
    *,
    seed: Mapping[str, object],
    require_router_certificate: bool = False,
) -> _SecretSnapshot:
    value = _json_object(payload, label="capacity manager Secret")
    metadata = value.get("metadata")
    data = value.get("data")
    if not isinstance(metadata, dict) or not isinstance(data, dict):
        raise ValueError("capacity manager Secret is invalid")
    uid, resource_version = _object_identity(
        value,
        api_version="v1",
        kind="Secret",
        metadata=metadata,
    )
    if (
        value.get("type") != "Opaque"
        or value.get("immutable") not in {None, False}
        or not _SECRET_KEYS.issubset(data)
        or not isinstance(data.get("principals.json"), str)
        or (require_router_certificate and not isinstance(data.get("server-certificate.pem"), str))
    ):
        raise ValueError("capacity manager Secret is invalid")
    current_registry = data["principals.json"]
    assert isinstance(current_registry, str)
    current_registry_bytes = _decode_secret_field(current_registry)
    server_certificate = (
        _decode_secret_field(cast(str, data["server-certificate.pem"]))
        if require_router_certificate
        else None
    )
    desired_registry_bytes = _principal_registry_with_staging_reporter(
        current_registry_bytes,
        seed=seed,
    )
    desired_registry = base64.b64encode(desired_registry_bytes).decode("ascii")
    managed_fields = metadata.get("managedFields")
    if not isinstance(managed_fields, list) or not managed_fields:
        raise ValueError("capacity manager Secret ownership is invalid")
    dedicated_owner = False
    legacy_owner = False
    for entry in managed_fields:
        contract, fields = _managed_field_contract(entry)
        if contract not in _SECRET_MANAGER_CONTRACTS:
            raise ValueError("capacity manager Secret ownership is invalid")
        data_fields = fields.get("f:data", {})
        if not isinstance(data_fields, dict):
            raise ValueError("capacity manager Secret ownership is invalid")
        if "f:principals.json" not in data_fields:
            continue
        if data_fields["f:principals.json"] != {}:
            raise ValueError("capacity manager Secret ownership is invalid")
        if contract[0] in {_FIELD_MANAGER, _EXECUTION_CREDENTIAL_FIELD_MANAGER}:
            dedicated_owner = True
        elif contract[0] == "kubectl-patch":
            legacy_owner = True
        else:
            raise ValueError("capacity manager Secret principal ownership is invalid")
    if dedicated_owner == legacy_owner:
        raise ValueError("capacity manager Secret principal ownership is invalid")
    exact = current_registry == desired_registry and dedicated_owner
    return _SecretSnapshot(
        uid=uid,
        resource_version=resource_version,
        current_registry=current_registry,
        desired_registry=desired_registry,
        desired_registry_bytes=desired_registry_bytes,
        dedicated_owner=dedicated_owner,
        exact=exact,
        evidence_digest=hashlib.sha256(payload).hexdigest(),
        server_certificate=server_certificate,
    )


def _secret_patch(snapshot: _SecretSnapshot, *, desired_registry: str | None = None) -> bytes:
    return _json_bytes(
        [
            {"op": "test", "path": "/metadata/uid", "value": snapshot.uid},
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": snapshot.resource_version,
            },
            {
                "op": "test",
                "path": "/data/principals.json",
                "value": snapshot.current_registry,
            },
            {
                "op": "replace",
                "path": "/data/principals.json",
                "value": snapshot.desired_registry
                if desired_registry is None
                else desired_registry,
            },
        ]
    )


def _parse_deployment_identity(payload: bytes) -> tuple[str, str]:
    value = _json_object(payload, label="capacity manager Deployment")
    metadata = value.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("capacity manager Deployment is invalid")
    return _object_identity(
        value,
        api_version="apps/v1",
        kind="Deployment",
        metadata=metadata,
    )


def _parse_deployment(
    payload: bytes,
    *,
    normalized_desired: bytes,
) -> _DeploymentSnapshot:
    value = _json_object(payload, label="capacity manager Deployment")
    desired = _json_object(normalized_desired, label="desired capacity manager Deployment")
    metadata = value.get("metadata")
    spec = value.get("spec")
    status = value.get("status")
    if not isinstance(metadata, dict) or not isinstance(spec, dict) or not isinstance(status, dict):
        raise ValueError("capacity manager Deployment is invalid")
    uid, resource_version = _object_identity(
        value,
        api_version="apps/v1",
        kind="Deployment",
        metadata=metadata,
    )
    managed_fields = metadata.get("managedFields")
    if not isinstance(managed_fields, list) or not managed_fields:
        raise ValueError("capacity manager Deployment ownership is invalid")
    dedicated_owner = False
    trusted_owner = False
    for entry in managed_fields:
        contract, _fields = _managed_field_contract(entry)
        if contract not in _DEPLOYMENT_MANAGER_CONTRACTS:
            raise ValueError("capacity manager Deployment ownership is invalid")
        dedicated_owner = dedicated_owner or contract[0] == _FIELD_MANAGER
        trusted_owner = trusted_owner or contract[0] == "loom-capacity-control-plane"
    if not trusted_owner and not dedicated_owner:
        raise ValueError("capacity manager Deployment ownership is invalid")
    generation = metadata.get("generation")
    healthy = (
        type(generation) is int
        and generation > 0
        and status.get("observedGeneration") == generation
        and status.get("replicas") == 1
        and status.get("updatedReplicas") == 1
        and status.get("readyReplicas") == 1
        and status.get("availableReplicas") == 1
        and status.get("unavailableReplicas") in {None, 0}
        and status.get("terminatingReplicas") in {None, 0}
    )
    projection = _deployment_projection(value)
    desired_projection = _deployment_projection(desired)
    return _DeploymentSnapshot(
        uid=uid,
        resource_version=resource_version,
        exact=projection == desired_projection,
        healthy=healthy,
        dedicated_owner=dedicated_owner,
        evidence_digest=_hash_json(projection),
    )


def _deployment_projection(value: dict[str, object]) -> dict[str, object]:
    projected = copy.deepcopy(value)
    projected.pop("status", None)
    metadata = projected.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("capacity manager Deployment metadata is invalid")
    for key in (
        "creationTimestamp",
        "generation",
        "managedFields",
        "resourceVersion",
        "uid",
    ):
        metadata.pop(key, None)
    _remove_annotations(metadata, _REMOVABLE_DEPLOYMENT_ANNOTATIONS)
    spec = projected.get("spec")
    if not isinstance(spec, dict):
        raise ValueError("capacity manager Deployment spec is invalid")
    template = spec.get("template")
    if not isinstance(template, dict):
        raise ValueError("capacity manager Deployment template is invalid")
    template_metadata = template.get("metadata")
    if not isinstance(template_metadata, dict):
        raise ValueError("capacity manager Deployment template metadata is invalid")
    template_metadata.pop("creationTimestamp", None)
    _remove_annotations(template_metadata, _REMOVABLE_TEMPLATE_ANNOTATIONS)
    return projected


def _remove_annotations(metadata: dict[str, object], removable: frozenset[str]) -> None:
    annotations = metadata.get("annotations")
    if annotations is None:
        return
    if not isinstance(annotations, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in annotations.items()
    ):
        raise ValueError("capacity manager Deployment annotations are invalid")
    for key in removable:
        annotations.pop(key, None)
    if not annotations:
        metadata.pop("annotations", None)


def _managed_field_contract(
    entry: object,
) -> tuple[tuple[object, object, object, object], dict[str, object]]:
    if not isinstance(entry, dict):
        raise ValueError("capacity manager managed field is invalid")
    fields = entry.get("fieldsV1")
    if entry.get("fieldsType") != "FieldsV1" or not isinstance(fields, dict):
        raise ValueError("capacity manager managed field is invalid")
    return (
        (
            entry.get("manager"),
            entry.get("operation"),
            entry.get("apiVersion"),
            entry.get("subresource"),
        ),
        fields,
    )


def _object_identity(
    value: Mapping[str, object],
    *,
    api_version: str,
    kind: str,
    metadata: Mapping[str, object],
) -> tuple[str, str]:
    uid = metadata.get("uid")
    resource_version = metadata.get("resourceVersion")
    if (
        value.get("apiVersion") != api_version
        or value.get("kind") != kind
        or metadata.get("name") != _NAME
        or metadata.get("namespace") != _NAMESPACE
        or not isinstance(uid, str)
        or _UID_RE.fullmatch(uid) is None
        or not isinstance(resource_version, str)
        or _RESOURCE_VERSION_RE.fullmatch(resource_version) is None
    ):
        raise ValueError(f"capacity manager {kind} identity is invalid")
    return uid, resource_version


def _decode_secret_field(value: str) -> bytes:
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("capacity manager Secret data is invalid") from exc
    if (
        not decoded
        or len(decoded) > _MAX_SECRET_FIELD_BYTES
        or base64.b64encode(decoded).decode("ascii") != value
    ):
        raise ValueError("capacity manager Secret data is invalid")
    return decoded


def _json_object(payload: bytes, *, label: str) -> dict[str, object]:
    if not payload or len(payload) > _MAX_SECRET_FIELD_BYTES:
        raise ValueError(f"{label} is invalid")
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (json.JSONDecodeError, UnicodeError, ValueError) as exc:
        raise ValueError(f"{label} is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} is invalid")
    return value


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def _hash_json(value: object) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _principal_registry_with_staging_reporter(
    payload: bytes,
    *,
    seed: Mapping[str, object],
) -> bytes:
    if not payload or len(payload) > _MAX_REGISTRY_BYTES:
        raise ValueError("capacity principal registry is invalid")
    try:
        registry = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (json.JSONDecodeError, UnicodeError, ValueError) as exc:
        raise ValueError("capacity principal registry contains duplicate or invalid JSON") from exc
    if not isinstance(registry, dict) or not isinstance(registry.get("principals"), list):
        raise ValueError("capacity principal registry is invalid")
    try:
        reporter_token = seed["reporter_token"]
        reporter_incarnation = _canonical_uuid(seed["reporter_incarnation"])
        subject_id = _canonical_uuid(seed["subject_id"])
        subject_incarnation = _canonical_uuid(seed["subject_incarnation"])
        if not isinstance(reporter_token, str):
            raise ValueError("reporter token is not a string")
        token_bytes = reporter_token.encode("ascii")
    except (KeyError, UnicodeEncodeError, ValueError) as exc:
        raise ValueError("staging demand reporter seed is invalid") from exc
    if not 32 <= len(token_bytes) <= 1024 or any(not 0x21 <= byte <= 0x7E for byte in token_bytes):
        raise ValueError("staging demand reporter seed is invalid")
    desired: dict[str, object] = {
        "demand_reporter_incarnation": reporter_incarnation,
        "executor_id": None,
        "executor_incarnation": None,
        "executor_pool_generation": None,
        "pool_id": None,
        "pool_reporter_incarnation": None,
        "principal_id": _PRINCIPAL_ID,
        "scopes": ["capacity:report:demand"],
        "subject_id": subject_id,
        "subject_incarnation": subject_incarnation,
        "token_sha256": hashlib.sha256(token_bytes).hexdigest(),
    }
    principals = registry["principals"]
    assert isinstance(principals, list)
    _add_rollback_scope(principals)
    matching = [
        principal
        for principal in principals
        if isinstance(principal, dict) and principal.get("principal_id") == _PRINCIPAL_ID
    ]
    if matching:
        if len(matching) != 1 or matching[0] != desired:
            raise ValueError("staging demand reporter conflicts with the principal registry")
    else:
        for principal in principals:
            if not isinstance(principal, dict):
                raise ValueError("capacity principal registry is invalid")
            if (
                principal.get("token_sha256") == desired["token_sha256"]
                or principal.get("subject_id") == subject_id
                or principal.get("subject_incarnation") == subject_incarnation
                or principal.get("demand_reporter_incarnation") == reporter_incarnation
            ):
                raise ValueError("staging demand reporter conflicts with the principal registry")
        principals.append(desired)
    canonical = (
        json.dumps(registry, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")
    try:
        _RegistryDocument.model_validate_json(canonical)
    except (ValidationError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("capacity principal registry is invalid") from exc
    return canonical


def _add_rollback_scope(principals: list[object]) -> None:
    matches: list[dict[str, object]] = []
    for principal in principals:
        if not isinstance(principal, dict):
            raise ValueError("capacity principal registry is invalid")
        scopes = principal.get("scopes")
        exact_activate_principal = (
            principal.get("principal_id") == "configuration-activate"
            and principal.get("subject_id") is None
            and principal.get("subject_incarnation") is None
            and principal.get("demand_reporter_incarnation") is None
            and principal.get("pool_id") is None
            and principal.get("pool_reporter_incarnation") is None
            and principal.get("executor_id") is None
            and principal.get("executor_incarnation") is None
            and principal.get("executor_pool_generation") is None
            and isinstance(scopes, list)
            and "capacity:configure:activate" in scopes
        )
        if (
            principal.get("principal_id") == "configuration-activate"
            and not exact_activate_principal
        ):
            raise ValueError("capacity principal registry is invalid")
        if not isinstance(scopes, list):
            raise ValueError("capacity principal registry is invalid")
        if _ROLLBACK_SCOPE in scopes and not exact_activate_principal:
            raise ValueError("capacity principal registry is invalid")
        if exact_activate_principal:
            matches.append(principal)
    if not matches:
        raise ValueError("capacity principal registry is invalid")
    if len(matches) != 1:
        raise ValueError("capacity principal registry is invalid")
    scopes = matches[0]["scopes"]
    assert isinstance(scopes, list)
    if _ROLLBACK_SCOPE in scopes:
        scopes[:] = sorted(set(cast(list[str], scopes)))
        return
    scopes.append(_ROLLBACK_SCOPE)
    scopes[:] = sorted(set(cast(list[str], scopes)))


def _canonical_uuid(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("identity is not a string")
    parsed = UUID(value)
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError("identity is not canonical")
    return value


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


__all__: list[str] = []
