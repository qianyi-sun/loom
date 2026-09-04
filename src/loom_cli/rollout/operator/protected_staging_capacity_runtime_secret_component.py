"""Exact protected runtime database Secret for the staging control plane."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from cryptography import x509
from sqlalchemy import URL

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import ComponentState

_NAMESPACE = "loom-staging"
_SOURCE_SECRET = "loom-postgres-ca"
_TARGET_SECRET = "loom-protected-worker-runtime"
_FIELD_MANAGER = "loom-staging-protected-runtime-secret"
_REQUEST_TIMEOUT = "60s"
_QUERY_TIMEOUT_SECONDS = 30.0
_MUTATION_TIMEOUT_SECONDS = 60.0
_MAX_CA_BYTES = 1024 * 1024
_RESOURCE_VERSION_RE = re.compile(r"^[1-9][0-9]{0,31}$")
_UID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


class ProtectedStagingCapacityRuntimeSecretCommandRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...

    def run_status(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> int: ...

    def capture_stdout_with_input(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes,
        timeout_seconds: float,
    ) -> bytes: ...


@dataclass(frozen=True, slots=True)
class _Target:
    state: ComponentState
    uid: str | None
    resource_version: str | None
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class KubernetesProtectedStagingCapacityRuntimeSecretComponent:
    runner: ProtectedStagingCapacityRuntimeSecretCommandRunner
    seed_reader: Callable[[], dict[str, object]]

    def classify(self, plan: FinalGatePlan) -> tuple[ComponentState, str]:
        try:
            ca_certificate = self._read_ca_certificate()
            desired = self._desired(plan, ca_certificate)
            target = self._target(desired)
        except (OSError, RuntimeError, UnicodeError, ValueError):
            return ComponentState.DRIFTED, _hash_json({"status": "observation-failed"})
        return target.state, _hash_json(
            {
                "ca_sha256": hashlib.sha256(ca_certificate).hexdigest(),
                "state": target.state.value,
                "target": target.evidence_digest,
            }
        )

    def apply(self, plan: FinalGatePlan) -> None:
        ca_certificate = self._read_ca_certificate()
        desired = self._desired(plan, ca_certificate)
        before = self._target(desired)
        if before.state is not ComponentState.READY:
            raise RuntimeError("protected staging runtime Secret state drifted")
        payload = json.loads(desired)
        if before.uid is not None and before.resource_version is not None:
            payload["metadata"]["uid"] = before.uid
            payload["metadata"]["resourceVersion"] = before.resource_version
        applied = self.runner.capture_stdout_with_input(
            (
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
            ),
            env=self.runner.environment,
            input_payload=json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii"),
            timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
        )
        parsed = _parse_target(applied)
        if not parsed[0]:
            raise RuntimeError("protected staging runtime Secret apply was not owned")
        after_ca = self._read_ca_certificate()
        after_desired = self._desired(plan, after_ca)
        after = self._target(after_desired)
        if after.state is not ComponentState.EXACT:
            raise RuntimeError("protected staging runtime Secret did not converge")

    def _read_ca_certificate(self) -> bytes:
        encoded = self.runner.capture_stdout(
            (
                "kubectl",
                "--namespace",
                _NAMESPACE,
                "get",
                f"secret/{_SOURCE_SECRET}",
                "--output=jsonpath={.data.ca\\.crt}",
                f"--request-timeout={_REQUEST_TIMEOUT}",
            ),
            env=self.runner.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )
        try:
            certificate_bytes = base64.b64decode(encoded, validate=True)
            certificate = x509.load_pem_x509_certificate(certificate_bytes)
            constraints = certificate.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
        except (binascii.Error, ValueError, x509.ExtensionNotFound) as exc:
            raise ValueError("protected staging Postgres CA is invalid") from exc
        now = datetime.now(UTC)
        if (
            not certificate_bytes
            or len(certificate_bytes) > _MAX_CA_BYTES
            or base64.b64encode(certificate_bytes) != encoded
            or not constraints.ca
            or not certificate.not_valid_before_utc <= now <= certificate.not_valid_after_utc
        ):
            raise ValueError("protected staging Postgres CA is invalid")
        try:
            certificate.verify_directly_issued_by(certificate)
        except (TypeError, ValueError) as exc:
            raise ValueError("protected staging Postgres CA is not self-issued") from exc
        return certificate_bytes

    def _desired(self, plan: FinalGatePlan, ca_certificate: bytes) -> bytes:
        del plan
        seed = self.seed_reader()
        raw_password = seed.get("runtime_database_password")
        if not isinstance(raw_password, str):
            raise ValueError("protected staging runtime database password is invalid")
        try:
            password = raw_password.encode("ascii")
        except (AttributeError, UnicodeEncodeError):
            raise ValueError("protected staging runtime database password is invalid") from None
        if not 32 <= len(password) <= 1024 or any(not 0x21 <= byte <= 0x7E for byte in password):
            raise ValueError("protected staging runtime database password is invalid")
        database_url = URL.create(
            "postgresql+psycopg",
            username="loom_cap_staging_runtime",
            password=raw_password,
            host="loom-postgres-rw.loom-staging.svc.cluster.local",
            port=5432,
            database="loom",
            query={
                "sslmode": "verify-full",
                "sslrootcert": "/run/loom/protected-worker-runtime/files/ca.crt",
            },
        ).render_as_string(hide_password=False)
        return json.dumps(
            {
                "apiVersion": "v1",
                "data": {
                    "ca.crt": base64.b64encode(ca_certificate).decode("ascii"),
                    "database-url": base64.b64encode(database_url.encode("ascii")).decode("ascii"),
                },
                "immutable": False,
                "kind": "Secret",
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/managed-by": "loom-staging-rollout",
                        "app.kubernetes.io/name": _TARGET_SECRET,
                        "loom.carin.dev/protected-component": ("staging-protected-runtime-secret"),
                    },
                    "name": _TARGET_SECRET,
                    "namespace": _NAMESPACE,
                },
                "type": "Opaque",
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")

    def _target(self, desired: bytes) -> _Target:
        payload = self.runner.capture_stdout(
            (
                "kubectl",
                "--namespace",
                _NAMESPACE,
                "get",
                f"secret/{_TARGET_SECRET}",
                "--ignore-not-found=true",
                "--show-managed-fields",
                "--output=json",
                f"--request-timeout={_REQUEST_TIMEOUT}",
            ),
            env=self.runner.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )
        if not payload:
            return _Target(
                state=ComponentState.READY,
                uid=None,
                resource_version=None,
                evidence_digest=_hash_json({"status": "absent"}),
            )
        safe, uid, resource_version = _parse_target(payload)
        if not safe:
            state = ComponentState.DRIFTED
        else:
            status = self.runner.run_status(
                (
                    "kubectl",
                    "diff",
                    "--server-side=true",
                    f"--field-manager={_FIELD_MANAGER}",
                    "--validate=strict",
                    f"--request-timeout={_REQUEST_TIMEOUT}",
                    "-f",
                    "-",
                ),
                env=self.runner.environment,
                input_payload=desired,
                timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
            )
            state = ComponentState.EXACT if status == 0 else ComponentState.READY
        return _Target(
            state=state,
            uid=uid,
            resource_version=resource_version,
            evidence_digest=hashlib.sha256(payload).hexdigest(),
        )


def _parse_target(payload: bytes) -> tuple[bool, str, str]:
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (json.JSONDecodeError, UnicodeError, ValueError) as exc:
        raise ValueError("protected staging runtime Secret is invalid") from exc
    if not isinstance(value, dict) or not isinstance(value.get("metadata"), dict):
        raise ValueError("protected staging runtime Secret is invalid")
    metadata = value["metadata"]
    uid = metadata.get("uid")
    resource_version = metadata.get("resourceVersion")
    data = value.get("data")
    managed_fields = metadata.get("managedFields")
    identity_safe = (
        value.get("apiVersion") == "v1"
        and value.get("kind") == "Secret"
        and value.get("type") == "Opaque"
        and value.get("immutable") in {None, False}
        and metadata.get("name") == _TARGET_SECRET
        and metadata.get("namespace") == _NAMESPACE
        and isinstance(uid, str)
        and _UID_RE.fullmatch(uid) is not None
        and isinstance(resource_version, str)
        and _RESOURCE_VERSION_RE.fullmatch(resource_version) is not None
        and isinstance(data, dict)
        and set(data) == {"ca.crt", "database-url"}
        and all(isinstance(item, str) for item in data.values())
        and isinstance(managed_fields, list)
    )
    dedicated_fields: set[str] = set()
    foreign_fields: set[str] = set()
    if isinstance(managed_fields, list):
        for entry in managed_fields:
            if not isinstance(entry, dict) or not isinstance(entry.get("fieldsV1"), dict):
                identity_safe = False
                continue
            raw_data_fields = entry["fieldsV1"].get("f:data", {})
            if not isinstance(raw_data_fields, dict):
                identity_safe = False
                continue
            fields = {
                key.removeprefix("f:")
                for key, item in raw_data_fields.items()
                if isinstance(key, str) and key.startswith("f:") and item == {}
            }
            exact_owner = (
                entry.get("manager") == _FIELD_MANAGER
                and entry.get("operation") == "Apply"
                and entry.get("apiVersion") == "v1"
                and entry.get("fieldsType") == "FieldsV1"
            )
            if exact_owner:
                dedicated_fields.update(fields)
            else:
                foreign_fields.update(fields)
    expected_fields = {"ca.crt", "database-url"}
    safe = (
        identity_safe
        and dedicated_fields == expected_fields
        and not (foreign_fields & expected_fields)
    )
    if not isinstance(uid, str) or not isinstance(resource_version, str):
        raise ValueError("protected staging runtime Secret identity is invalid")
    return safe, uid, resource_version


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("protected staging runtime Secret JSON is ambiguous")
        value[key] = item
    return value


__all__ = ["KubernetesProtectedStagingCapacityRuntimeSecretComponent"]
