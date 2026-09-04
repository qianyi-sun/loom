"""Protected Kubernetes convergence for global execution credentials."""

from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import ComponentState
from .protected_execution_prerequisites import ProtectedExecutionPrerequisiteArtifact
from .protected_pool_credential_transport import (
    PoolExecutionCredentialPayload,
    ProtectedPoolCredentialTransport,
    pool_execution_credential_payload,
)
from .protected_staging_capacity_execution_credentials import (
    ExecutionCredentialBundle,
    build_execution_backup_secret_documents,
    build_execution_ownership_keyring,
    build_execution_principal_registry,
)

_NAMESPACE = "loom-dev"
_MANAGER_SECRET = "loom-capacity-manager"
_BACKUP_SECRETS = (
    "loom-capacity-execution-operator",
    "loom-capacity-executor-gb10",
    "loom-capacity-executor-oldlab",
)
_FIELD_MANAGER = "loom-staging-capacity-execution-credentials"
_TRUSTED_MANAGER_OWNERS = frozenset(
    {
        "kubectl-create",
        "kubectl-patch",
        "kubectl-replace",
        "loom-staging-capacity-manager-runtime",
        _FIELD_MANAGER,
    }
)
_MANAGER_FIELDS = ("ownership-public-keys.json", "principals.json")
_RESOURCE_VERSION_RE = re.compile(r"^[1-9][0-9]{0,31}$")
_UID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_REQUEST_TIMEOUT = "60s"
_COMMAND_TIMEOUT_SECONDS = 60.0
_MAX_SECRET_BYTES = 4 * 1024 * 1024


class ProtectedExecutionCredentialCommandRunner(Protocol):
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


@dataclass(frozen=True, slots=True)
class _ManagerSnapshot:
    uid: str
    resource_version: str
    current: Mapping[str, str]
    desired: Mapping[str, str]
    exact: bool
    evidence_sha256: str


@dataclass(frozen=True, slots=True)
class _BackupSnapshot:
    name: str
    desired_payload: bytes
    present: bool
    evidence_sha256: str


@dataclass(frozen=True, slots=True)
class _PoolSnapshot:
    pool_id: str
    payload: PoolExecutionCredentialPayload
    present: bool
    evidence_sha256: str


@dataclass(frozen=True, slots=True)
class _Snapshot:
    bundle: ExecutionCredentialBundle
    prerequisite: ProtectedExecutionPrerequisiteArtifact
    manager: _ManagerSnapshot
    backups: tuple[_BackupSnapshot, ...]
    pools: tuple[_PoolSnapshot, ...]
    state: ComponentState


@dataclass(frozen=True, slots=True)
class KubernetesProtectedStagingExecutionCredentialComponent:
    """Converge manager authority and immutable recovery copies without restart."""

    runner: ProtectedExecutionCredentialCommandRunner
    credential_bundle_reader: Callable[[], ExecutionCredentialBundle]
    prerequisite_reader: Callable[[FinalGatePlan], ProtectedExecutionPrerequisiteArtifact]
    pool_credential_transports: Mapping[str, ProtectedPoolCredentialTransport] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        if (
            "KUBECONFIG" not in self.runner.environment
            or not callable(self.credential_bundle_reader)
            or not callable(self.prerequisite_reader)
            or any(
                pool_id not in {"gb10", "oldlab"}
                or not callable(getattr(transport, "observe", None))
                or not callable(getattr(transport, "publish", None))
                for pool_id, transport in self.pool_credential_transports.items()
            )
        ):
            raise ValueError("protected execution credential component authority is invalid")
        object.__setattr__(
            self,
            "pool_credential_transports",
            MappingProxyType(dict(self.pool_credential_transports)),
        )

    def classify(self, plan: FinalGatePlan) -> tuple[ComponentState, str]:
        try:
            snapshot = self._snapshot(plan)
        except (OSError, RuntimeError, UnicodeError, ValueError, KeyError):
            return ComponentState.DRIFTED, _hash_json({"status": "observation-failed"})
        return snapshot.state, _hash_json(
            {
                "backups": {item.name: item.evidence_sha256 for item in snapshot.backups},
                "credential_metadata": dict(snapshot.bundle.metadata_sha256),
                "manager": snapshot.manager.evidence_sha256,
                "pools": {item.pool_id: item.evidence_sha256 for item in snapshot.pools},
                "prerequisite": snapshot.prerequisite.artifact_sha256,
                "state": snapshot.state.value,
            }
        )

    def apply(self, plan: FinalGatePlan) -> None:
        snapshot = self._snapshot(plan)
        if snapshot.state is not ComponentState.READY:
            raise RuntimeError("protected execution credential state changed before apply")
        if not snapshot.manager.exact:
            self._require_source_unchanged(plan, snapshot)
            applied = self.runner.capture_stdout_with_input(
                self._manager_patch_argv(),
                env=self.runner.environment,
                input_payload=_manager_patch(snapshot.manager),
                timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
            )
            manager = self._parse_manager_secret(
                applied,
                bundle=snapshot.bundle,
                prerequisite=snapshot.prerequisite,
            )
            if not manager.exact:
                raise RuntimeError("protected execution manager authority did not converge")
            self._require_source_unchanged(plan, snapshot)
        for backup in snapshot.backups:
            if backup.present:
                continue
            self._require_source_unchanged(plan, snapshot)
            applied = self.runner.capture_stdout_with_input(
                self._backup_create_argv(),
                env=self.runner.environment,
                input_payload=backup.desired_payload,
                timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
            )
            parsed = _parse_backup_secret(
                applied,
                name=backup.name,
                desired_payload=backup.desired_payload,
            )
            if not parsed.present:
                raise RuntimeError("protected execution backup Secret did not converge")
            self._require_source_unchanged(plan, snapshot)
        for pool in snapshot.pools:
            if pool.present:
                continue
            self._require_source_unchanged(plan, snapshot)
            transport = self.pool_credential_transports[pool.pool_id]
            evidence = transport.publish(pool.payload)
            if evidence.pool_id != pool.pool_id:
                raise RuntimeError("protected pool credential transport returned foreign evidence")
            self._require_source_unchanged(plan, snapshot)
        after = self._snapshot(plan)
        if after.state is not ComponentState.EXACT:
            raise RuntimeError("protected execution credential authority did not converge")

    def _snapshot(self, plan: FinalGatePlan) -> _Snapshot:
        prerequisite = self.prerequisite_reader(plan)
        _validate_prerequisite_binding(plan, prerequisite)
        bundle = self.credential_bundle_reader()
        if not isinstance(bundle, ExecutionCredentialBundle):
            raise ValueError("protected execution credential bundle is invalid")
        second = self.credential_bundle_reader()
        if second != bundle:
            raise ValueError("protected execution credential source changed during read")
        if dict(bundle.metadata_sha256) != dict(prerequisite.credential_metadata_sha256):
            raise ValueError("protected execution credential metadata drifted")
        manager_payload = self.runner.capture_stdout(
            self._manager_get_argv(),
            env=self.runner.environment,
            timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
        )
        manager = self._parse_manager_secret(
            manager_payload,
            bundle=bundle,
            prerequisite=prerequisite,
        )
        documents = build_execution_backup_secret_documents(bundle)
        backups: list[_BackupSnapshot] = []
        for name in _BACKUP_SECRETS:
            payload = self.runner.capture_stdout(
                self._backup_get_argv(name),
                env=self.runner.environment,
                timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
            )
            backups.append(
                _parse_backup_secret(
                    payload,
                    name=name,
                    desired_payload=documents[name],
                )
            )
        pools: list[_PoolSnapshot] = []
        for pool_id in ("gb10", "oldlab"):
            transport = self.pool_credential_transports.get(pool_id)
            if transport is None:
                raise ValueError("protected pool credential transport is unavailable")
            pool_payload = pool_execution_credential_payload(bundle, pool_id=pool_id)
            evidence = transport.observe(pool_payload)
            pools.append(
                _PoolSnapshot(
                    pool_id=pool_id,
                    payload=pool_payload,
                    present=evidence is not None,
                    evidence_sha256=(
                        _hash_json({"pool_id": pool_id, "status": "absent"})
                        if evidence is None
                        else _hash_json(
                            {
                                "credential_metadata_sha256": dict(
                                    evidence.credential_metadata_sha256
                                ),
                                "file_sha256": dict(evidence.file_sha256),
                                "gid": evidence.gid,
                                "pool_id": evidence.pool_id,
                                "uid": evidence.uid,
                            }
                        )
                    ),
                )
            )
        state = (
            ComponentState.EXACT
            if manager.exact
            and all(item.present for item in backups)
            and all(item.present for item in pools)
            else ComponentState.READY
        )
        return _Snapshot(
            bundle=bundle,
            prerequisite=prerequisite,
            manager=manager,
            backups=tuple(backups),
            pools=tuple(pools),
            state=state,
        )

    def _require_source_unchanged(self, plan: FinalGatePlan, expected: _Snapshot) -> None:
        prerequisite = self.prerequisite_reader(plan)
        bundle = self.credential_bundle_reader()
        if prerequisite != expected.prerequisite or bundle != expected.bundle:
            raise RuntimeError("protected execution credential source changed before mutation")

    @staticmethod
    def _parse_manager_secret(
        payload: bytes,
        *,
        bundle: ExecutionCredentialBundle,
        prerequisite: ProtectedExecutionPrerequisiteArtifact,
    ) -> _ManagerSnapshot:
        value = _json_object(payload, label="protected execution manager Secret")
        metadata = value.get("metadata")
        data = value.get("data")
        if (
            value.get("apiVersion") != "v1"
            or value.get("kind") != "Secret"
            or value.get("type") != "Opaque"
            or value.get("immutable") not in {None, False}
            or not isinstance(metadata, dict)
            or metadata.get("name") != _MANAGER_SECRET
            or metadata.get("namespace") != _NAMESPACE
            or not isinstance(data, dict)
            or any(
                not isinstance(key, str) or not isinstance(item, str) for key, item in data.items()
            )
            or not set(_MANAGER_FIELDS) <= set(data)
        ):
            raise ValueError("protected execution manager Secret is invalid")
        uid, resource_version = _identity(metadata)
        current = {name: data[name] for name in _MANAGER_FIELDS}
        decoded = {name: _decode(current[name]) for name in _MANAGER_FIELDS}
        pools = prerequisite.executor_profile_seed.pools
        desired_payloads = {
            "principals.json": build_execution_principal_registry(
                decoded["principals.json"],
                bundle=bundle,
                pools=pools,
            ),
            "ownership-public-keys.json": build_execution_ownership_keyring(
                decoded["ownership-public-keys.json"],
                bundle=bundle,
                pools=pools,
            ),
        }
        desired = {
            name: base64.b64encode(desired_payloads[name]).decode("ascii")
            for name in _MANAGER_FIELDS
        }
        _require_managed_data_fields(
            metadata,
            fields=frozenset(_MANAGER_FIELDS),
            allowed_managers=_TRUSTED_MANAGER_OWNERS,
        )
        return _ManagerSnapshot(
            uid=uid,
            resource_version=resource_version,
            current=current,
            desired=desired,
            exact=current == desired,
            evidence_sha256=hashlib.sha256(payload).hexdigest(),
        )

    @staticmethod
    def _manager_get_argv() -> tuple[str, ...]:
        return (
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "get",
            f"secret/{_MANAGER_SECRET}",
            "--show-managed-fields",
            "--output=json",
            f"--request-timeout={_REQUEST_TIMEOUT}",
        )

    @staticmethod
    def _backup_get_argv(name: str) -> tuple[str, ...]:
        return (
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "get",
            f"secret/{name}",
            "--ignore-not-found=true",
            "--show-managed-fields",
            "--output=json",
            f"--request-timeout={_REQUEST_TIMEOUT}",
        )

    @staticmethod
    def _manager_patch_argv() -> tuple[str, ...]:
        return (
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "patch",
            f"secret/{_MANAGER_SECRET}",
            "--type=json",
            f"--field-manager={_FIELD_MANAGER}",
            "--patch-file=/dev/stdin",
            "--show-managed-fields",
            "--output=json",
            f"--request-timeout={_REQUEST_TIMEOUT}",
        )

    @staticmethod
    def _backup_create_argv() -> tuple[str, ...]:
        return (
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "create",
            f"--field-manager={_FIELD_MANAGER}",
            "--output=json",
            "--validate=strict",
            f"--request-timeout={_REQUEST_TIMEOUT}",
            "-f",
            "-",
        )


def _validate_prerequisite_binding(
    plan: FinalGatePlan,
    prerequisite: ProtectedExecutionPrerequisiteArtifact,
) -> None:
    if (
        not isinstance(plan, FinalGatePlan)
        or plan.schema_version != 7
        or not isinstance(prerequisite, ProtectedExecutionPrerequisiteArtifact)
        or plan.execution_prerequisite_artifact_sha256 != prerequisite.artifact_sha256
        or plan.execution_access_metadata_sha256 != prerequisite.credential_metadata_manifest_sha256
        or plan.executor_profile_seed_sha256 != prerequisite.executor_profile_seed_sha256
    ):
        raise ValueError("protected execution prerequisite binding drifted")


def _parse_backup_secret(
    payload: bytes,
    *,
    name: str,
    desired_payload: bytes,
) -> _BackupSnapshot:
    if not payload:
        return _BackupSnapshot(
            name=name,
            desired_payload=desired_payload,
            present=False,
            evidence_sha256=_hash_json({"name": name, "status": "absent"}),
        )
    value = _json_object(payload, label="protected execution backup Secret")
    desired = _json_object(desired_payload, label="desired protected execution backup Secret")
    metadata = value.get("metadata")
    data = value.get("data")
    if (
        value.get("apiVersion") != "v1"
        or value.get("kind") != "Secret"
        or value.get("type") != "Opaque"
        or value.get("immutable") is not True
        or not isinstance(metadata, dict)
        or metadata.get("name") != name
        or metadata.get("namespace") != _NAMESPACE
        or not isinstance(data, dict)
        or not data
        or any(not isinstance(key, str) or not isinstance(item, str) for key, item in data.items())
    ):
        raise ValueError("protected execution backup Secret is invalid")
    _identity(metadata)
    _require_managed_data_fields(
        metadata,
        fields=frozenset(data),
        allowed_managers=frozenset({_FIELD_MANAGER}),
    )
    projection = copy.deepcopy(value)
    projection_metadata = projection["metadata"]
    assert isinstance(projection_metadata, dict)
    for metadata_field in ("creationTimestamp", "managedFields", "resourceVersion", "uid"):
        projection_metadata.pop(metadata_field, None)
    if projection != desired:
        raise ValueError("protected execution immutable backup Secret drifted")
    return _BackupSnapshot(
        name=name,
        desired_payload=desired_payload,
        present=True,
        evidence_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _manager_patch(snapshot: _ManagerSnapshot) -> bytes:
    operations: list[dict[str, object]] = [
        {"op": "test", "path": "/metadata/uid", "value": snapshot.uid},
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": snapshot.resource_version,
        },
    ]
    for name in _MANAGER_FIELDS:
        operations.append({"op": "test", "path": f"/data/{name}", "value": snapshot.current[name]})
        operations.append(
            {"op": "replace", "path": f"/data/{name}", "value": snapshot.desired[name]}
        )
    return json.dumps(operations, sort_keys=True, separators=(",", ":")).encode("ascii")


def _require_managed_data_fields(
    metadata: Mapping[str, object],
    *,
    fields: frozenset[str],
    allowed_managers: frozenset[str],
) -> None:
    managed = metadata.get("managedFields")
    if not isinstance(managed, list) or not managed:
        raise ValueError("protected execution Secret ownership is invalid")
    owners: dict[str, set[str]] = {data_field: set() for data_field in fields}
    for entry in managed:
        if not isinstance(entry, dict) or entry.get("fieldsType") != "FieldsV1":
            raise ValueError("protected execution Secret ownership is invalid")
        raw = entry.get("fieldsV1")
        data_fields = raw.get("f:data") if isinstance(raw, dict) else None
        if not isinstance(data_fields, dict):
            continue
        manager = entry.get("manager")
        for data_field in fields:
            if data_fields.get(f"f:{data_field}") == {}:
                if not isinstance(manager, str) or manager not in allowed_managers:
                    raise ValueError("protected execution Secret has foreign field ownership")
                owners[data_field].add(manager)
    if any(len(value) != 1 for value in owners.values()):
        raise ValueError("protected execution Secret ownership is incomplete")


def _identity(metadata: Mapping[str, object]) -> tuple[str, str]:
    uid = metadata.get("uid")
    resource_version = metadata.get("resourceVersion")
    if (
        not isinstance(uid, str)
        or _UID_RE.fullmatch(uid) is None
        or not isinstance(resource_version, str)
        or _RESOURCE_VERSION_RE.fullmatch(resource_version) is None
    ):
        raise ValueError("protected execution Secret identity is invalid")
    return uid, resource_version


def _decode(value: str) -> bytes:
    try:
        payload = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("protected execution manager Secret data is invalid") from exc
    if not payload or len(payload) > _MAX_SECRET_BYTES:
        raise ValueError("protected execution manager Secret data is invalid")
    return payload


def _json_object(payload: bytes, *, label: str) -> dict[str, object]:
    if not payload or len(payload) > _MAX_SECRET_BYTES:
        raise ValueError(f"{label} is invalid")

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{label} contains duplicate fields")
            result[key] = value
        return result

    try:
        value = json.loads(payload, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"{label} is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} is invalid")
    return value


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


__all__ = ["KubernetesProtectedStagingExecutionCredentialComponent"]
