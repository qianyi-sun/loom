"""Journaled ownership of the external-supervisor database Secret data."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)

_NAMESPACE = "loom-staging"
_SOURCE_SECRET = "loom-secrets"
_TARGET_SECRET = "loom-external-slurm-autoscaler-db"
_DATABASE_KEY = "cp-db-url"
_FIELD_MANAGER = "loom-staging-rollout-supervisor-database"
_LEGACY_FIELD_MANAGER = "kubectl-client-side-apply"
_REQUEST_TIMEOUT = "60s"
_COMMAND_TIMEOUT_SECONDS = 60.0
_MAX_DATABASE_BYTES = 64 * 1024
_RESOURCE_VERSION_RE = re.compile(r"^[1-9][0-9]{0,31}$")
_UID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_IMPLEMENTATION_DIGEST = hashlib.sha256(
    b"loom-protected-external-supervisor-database-secret-v2"
).hexdigest()


class ProtectedDatabaseSecretCommandRunner(Protocol):
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


EpochGuard = Callable[[FinalGatePlan], ComponentObservation]


@dataclass(frozen=True, slots=True)
class _Target:
    uid: str
    resource_version: str
    database_digest: str | None
    dedicated_owner: bool
    legacy_owner: bool
    safe: bool


@dataclass(frozen=True, slots=True)
class _Snapshot:
    source_value: str
    source_digest: str
    target: _Target
    state: ComponentState


@dataclass(frozen=True, slots=True)
class KubernetesExternalSupervisorDatabaseSecretComponent:
    """Own only the derived database field, independently of manifest shells."""

    runner: ProtectedDatabaseSecretCommandRunner
    environment: Mapping[str, str]
    epoch_guard: EpochGuard

    def __post_init__(self) -> None:
        if "KUBECONFIG" not in self.environment or not callable(self.epoch_guard):
            raise ValueError("protected external supervisor database authority is invalid")

    def component(self, plan: FinalGatePlan) -> ProtectedApplyComponent:
        return ProtectedApplyComponent(
            component_id="external-supervisor-database-secret",
            implementation_digest=_IMPLEMENTATION_DIGEST,
            input_fingerprint=_hash_json(
                {
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "database_key": _DATABASE_KEY,
                    "field_manager": _FIELD_MANAGER,
                    "legacy_field_manager": _LEGACY_FIELD_MANAGER,
                    "namespace": plan.namespace,
                    "source_secret": _SOURCE_SECRET,
                    "starting_epoch": plan.starting_mutation_epoch,
                    "target_secret": _TARGET_SECRET,
                }
            ),
            classify=self.classify,
            apply=self.apply,
        )

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            return self._observation(plan, epoch, ComponentState.DRIFTED, None)
        try:
            snapshot = self._snapshot()
        except (OSError, RuntimeError, UnicodeError, ValueError):
            return self._observation(plan, epoch, ComponentState.DRIFTED, None)
        return self._observation(plan, epoch, snapshot.state, snapshot)

    def apply(self, plan: FinalGatePlan) -> None:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            raise RuntimeError("protected external supervisor database epoch changed")
        try:
            snapshot = self._snapshot()
        except (OSError, RuntimeError, UnicodeError, ValueError) as exc:
            raise RuntimeError(
                "protected external supervisor database state changed before apply"
            ) from exc
        if snapshot.state is not ComponentState.READY:
            raise RuntimeError("protected external supervisor database state changed before apply")
        payload = json.dumps(
            {
                "apiVersion": "v1",
                "data": {_DATABASE_KEY: snapshot.source_value},
                "kind": "Secret",
                "metadata": {
                    "name": _TARGET_SECRET,
                    "namespace": _NAMESPACE,
                    "resourceVersion": snapshot.target.resource_version,
                    "uid": snapshot.target.uid,
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        result = self.runner.capture_stdout_with_input(
            self._apply_argv(force_conflicts=snapshot.target.legacy_owner),
            env=self.environment,
            input_payload=payload,
            timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
        )
        applied = _parse_target(result)
        if (
            not applied.safe
            or applied.database_digest != snapshot.source_digest
            or not applied.dedicated_owner
        ):
            raise RuntimeError("protected external supervisor database did not converge")

    def _snapshot(self) -> _Snapshot:
        source_value, source_digest = _parse_database_value(
            self.runner.capture_stdout(
                self._source_argv(),
                env=self.environment,
                timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
            )
        )
        target = _parse_target(
            self.runner.capture_stdout(
                self._target_argv(),
                env=self.environment,
                timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
            )
        )
        if not target.safe:
            state = ComponentState.DRIFTED
        elif target.database_digest == source_digest and target.dedicated_owner:
            state = ComponentState.EXACT
        else:
            state = ComponentState.READY
        return _Snapshot(
            source_value=source_value,
            source_digest=source_digest,
            target=target,
            state=state,
        )

    def _observation(
        self,
        plan: FinalGatePlan,
        epoch: ComponentObservation,
        state: ComponentState,
        snapshot: _Snapshot | None,
    ) -> ComponentObservation:
        return ComponentObservation(
            state=state,
            evidence_digest=_hash_json(
                {
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "dedicated_owner": (
                        None if snapshot is None else snapshot.target.dedicated_owner
                    ),
                    "epoch_evidence_digest": epoch.evidence_digest,
                    "legacy_owner": None if snapshot is None else snapshot.target.legacy_owner,
                    "source_digest": None if snapshot is None else snapshot.source_digest,
                    "state": state.value,
                    "target_digest": (
                        None if snapshot is None else snapshot.target.database_digest
                    ),
                    "target_resource_version": (
                        None if snapshot is None else snapshot.target.resource_version
                    ),
                    "target_uid": None if snapshot is None else snapshot.target.uid,
                }
            ),
            observed_epoch=plan.starting_mutation_epoch + 1,
        )

    @staticmethod
    def _source_argv() -> tuple[str, ...]:
        return (
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "get",
            f"secret/{_SOURCE_SECRET}",
            f"--output=jsonpath={{.data.{_DATABASE_KEY}}}",
            f"--request-timeout={_REQUEST_TIMEOUT}",
        )

    @staticmethod
    def _target_argv() -> tuple[str, ...]:
        return (
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "get",
            f"secret/{_TARGET_SECRET}",
            "--show-managed-fields",
            "--output=json",
            f"--request-timeout={_REQUEST_TIMEOUT}",
        )

    @staticmethod
    def _apply_argv(*, force_conflicts: bool) -> tuple[str, ...]:
        argv = [
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "apply",
            "--server-side=true",
            f"--field-manager={_FIELD_MANAGER}",
            "--show-managed-fields",
            "--output=json",
            "--validate=strict",
            f"--request-timeout={_REQUEST_TIMEOUT}",
            "-f",
            "-",
        ]
        if force_conflicts:
            argv.insert(argv.index("--show-managed-fields"), "--force-conflicts")
        return tuple(argv)


def _parse_database_value(payload: bytes | str) -> tuple[str, str]:
    try:
        value = payload.decode("ascii") if isinstance(payload, bytes) else payload
        decoded = base64.b64decode(value, validate=True)
    except (UnicodeError, binascii.Error, ValueError) as exc:
        raise ValueError("protected external supervisor database value is invalid") from exc
    if (
        not value
        or not decoded
        or len(decoded) > _MAX_DATABASE_BYTES
        or base64.b64encode(decoded).decode("ascii") != value
    ):
        raise ValueError("protected external supervisor database value is invalid")
    return value, hashlib.sha256(decoded).hexdigest()


def _parse_target(payload: bytes) -> _Target:
    try:
        value = json.loads(payload)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise ValueError("protected external supervisor database target is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("protected external supervisor database target is invalid")
    metadata = value.get("metadata")
    data = value.get("data", {})
    if not isinstance(metadata, dict) or not isinstance(data, dict):
        raise ValueError("protected external supervisor database target is invalid")
    uid = metadata.get("uid")
    resource_version = metadata.get("resourceVersion")
    raw_managed_fields = metadata.get("managedFields")
    managed_fields = raw_managed_fields if isinstance(raw_managed_fields, list) else None
    immutable = value.get("immutable")
    identity_safe = (
        value.get("apiVersion") == "v1"
        and value.get("kind") == "Secret"
        and value.get("type") == "Opaque"
        and (immutable is None or immutable is False)
        and metadata.get("name") == _TARGET_SECRET
        and metadata.get("namespace") == _NAMESPACE
        and isinstance(uid, str)
        and _UID_RE.fullmatch(uid) is not None
        and isinstance(resource_version, str)
        and _RESOURCE_VERSION_RE.fullmatch(resource_version) is not None
        and managed_fields is not None
    )
    if not isinstance(uid, str) or not isinstance(resource_version, str):
        raise ValueError("protected external supervisor database target is invalid")
    keys_safe = set(data) <= {_DATABASE_KEY} and all(
        isinstance(key, str) and isinstance(item, str) for key, item in data.items()
    )
    database_value = data.get(_DATABASE_KEY) if keys_safe else None
    database_digest = None
    if database_value is not None:
        try:
            _canonical, database_digest = _parse_database_value(database_value)
        except ValueError:
            keys_safe = False
    dedicated_owner = False
    legacy_owner = False
    unknown_owner = False
    fields_safe = managed_fields is not None
    if managed_fields is not None:
        for entry in managed_fields:
            if not isinstance(entry, dict):
                fields_safe = False
                break
            fields = entry.get("fieldsV1")
            if not isinstance(fields, dict):
                fields_safe = False
                break
            data_fields = fields.get("f:data", {})
            if not isinstance(data_fields, dict):
                fields_safe = False
                break
            if "f:cp-db-url" not in data_fields:
                continue
            if data_fields["f:cp-db-url"] != {}:
                fields_safe = False
                break
            manager = entry.get("manager")
            operation = entry.get("operation")
            owner_contract_exact = (
                entry.get("fieldsType") == "FieldsV1" and entry.get("apiVersion") == "v1"
            )
            if manager == _FIELD_MANAGER and operation == "Apply" and owner_contract_exact:
                dedicated_owner = True
            elif (
                manager == _LEGACY_FIELD_MANAGER and operation == "Update" and owner_contract_exact
            ):
                legacy_owner = True
            else:
                unknown_owner = True
    ownership_safe = not unknown_owner and not (dedicated_owner and legacy_owner)
    return _Target(
        uid=uid,
        resource_version=resource_version,
        database_digest=database_digest,
        dedicated_owner=dedicated_owner,
        legacy_owner=legacy_owner,
        safe=identity_safe and keys_safe and fields_safe and ownership_safe,
    )


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = ["KubernetesExternalSupervisorDatabaseSecretComponent"]
