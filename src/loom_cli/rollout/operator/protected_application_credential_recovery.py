"""Recover original CNPG application credentials inside a protected component.

This is not cutover admission. The installed caller must serialize backup,
credential and database writers, admit CNPG reconciliation and private guards,
and stop/restart owned clients. Initial admission still requires the plan's
restore-verified lease. Recovery reuses that source even after its lease ages.
No Secret, database role or workload is mutated here.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol
from urllib.parse import parse_qsl, unquote_to_bytes, urlsplit

from loom_cli.cluster_backup_guard import validate_backup_manifest
from loom_cli.rollout.credential_authority import TrustedFileRead, read_trusted_file

from .final_gate_plan import FinalGatePlan
from .protected_cnpg_writer_configuration import capture_cnpg_writer_configuration
from .protected_secret_inventory import canonical_secret_export, inspect_secret_inventory

if TYPE_CHECKING:
    from .protected_apply_journal import ProtectedApplyJournal

_NAMESPACE = "loom-staging"
_APPLICATION = "loom-secrets"
_CNPG = "loom-postgres-cnpg-credentials"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_UID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z")
_RESOURCE_VERSION = re.compile(r"[1-9][0-9]{0,31}\Z")
_DIRECT_KEYS = ("cp-db-url", "gw-db-url", "svc-db-url")
_DIRECT_HOSTS = frozenset(
    f"{name}{suffix}:5432"
    for name in ("loom-postgres", "loom-postgres-rw")
    for suffix in ("", ".loom-staging", ".loom-staging.svc", ".loom-staging.svc.cluster.local")
)
_POOL_HOSTS = frozenset(
    f"loom-pgbouncer{suffix}:6432"
    for suffix in ("", ".loom-staging", ".loom-staging.svc", ".loom-staging.svc.cluster.local")
)


class CredentialRecoveryRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(
        self, argv: Sequence[str], *, env: Mapping[str, str], timeout_seconds: float
    ) -> bytes: ...


@dataclass(frozen=True, slots=True)
class ApplicationCredentialRecoveryBinding:
    """Only non-secret source hashes and exact live identities may be journaled."""

    manifest_sha256: str
    component_sha256: str
    inventory_sha256: str
    application_sha256: str
    application_uid: str
    application_resource_version: str
    cnpg_sha256: str
    cnpg_uid: str
    cnpg_resource_version: str

    def __post_init__(self) -> None:
        for value, pattern in (
            (self.manifest_sha256, _SHA256),
            (self.component_sha256, _SHA256),
            (self.inventory_sha256, _SHA256),
            (self.application_sha256, _SHA256),
            (self.cnpg_sha256, _SHA256),
            (self.application_uid, _UID),
            (self.cnpg_uid, _UID),
            (self.application_resource_version, _RESOURCE_VERSION),
            (self.cnpg_resource_version, _RESOURCE_VERSION),
        ):
            if not isinstance(value, str) or pattern.fullmatch(value) is None:
                raise ValueError("application credential recovery binding is invalid")


@dataclass(frozen=True, slots=True)
class ApplicationRuntimeCredential:
    username: str
    password: str = field(repr=False)


def _object(payload: bytes) -> dict[str, object]:
    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("application credential JSON has duplicate fields")
            result[key] = value
        return result

    value = json.loads(payload, object_pairs_hook=unique)
    if not isinstance(value, dict):
        raise ValueError("application credential JSON is invalid")
    return value


def _private(path: Path, uid: int) -> TrustedFileRead:
    return read_trusted_file(
        path, service_uid=uid, private=True, max_bytes=1024 * 1024, require_nonempty=True
    )


def _backup_sources(plan: FinalGatePlan, uid: int) -> tuple[bytes, bytes, str, str]:
    path = Path(plan.backup_manifest_path)
    if path.name != "backup-manifest.json":
        raise ValueError("application credential backup path is invalid")
    manifest_read = _private(path, uid)
    if hashlib.sha256(manifest_read.payload).hexdigest() != plan.backup_manifest_sha256:
        raise ValueError("application credential backup manifest changed")
    manifest = _object(manifest_read.payload)
    components = manifest.get("components")
    if not isinstance(components, dict):
        raise ValueError("application credential backup components are invalid")
    hashes = {
        name: item.get("sha256") for name, item in components.items() if isinstance(item, dict)
    }
    secrets = components.get("k8s_secrets")
    root = path.parent / "secrets"
    if (
        type(manifest.get("schema_version")) is not int
        or manifest["schema_version"] != 3
        or hashes != plan.checkpoint_component_sha256
        or not isinstance(secrets, dict)
        or secrets.get("kind") != "directory"
        or secrets.get("path") != str(root)
    ):
        raise ValueError("application credential backup component binding changed")
    inventory_path = root / "protected-capacity-secret-inventory.json"
    inventory_read = _private(inventory_path, uid)
    inventory = inspect_secret_inventory(root, expected_owner_uid=uid)
    cnpg_filename = f"protected-{_NAMESPACE}-{_CNPG}.json"
    if (
        inventory.inventory_payload != inventory_read.payload
        or _object(inventory.inventory_payload)["schema_version"] != 2
        or cnpg_filename not in inventory.exported_objects
    ):
        raise ValueError("application credential backup lacks observed CNPG presence")
    app_path, cnpg_path = root / "loom-secrets.yaml", root / cnpg_filename
    app_read, cnpg_read = _private(app_path, uid), _private(cnpg_path, uid)
    if cnpg_read.payload != inventory.exported_objects[cnpg_filename]:
        raise ValueError("application credential CNPG export changed")
    # Full backup validation, including PostgreSQL, lies between the selected
    # inputs' two trusted reads. It is not replaced by Secret-only validation.
    if validate_backup_manifest(
        path,
        environment="staging",
        namespace=_NAMESPACE,
        expected_owner_uid=uid,
        require_private_files=True,
        enforce_freshness=False,
    ):
        raise ValueError("application credential backup validation failed")
    for source, before in (
        (path, manifest_read),
        (inventory_path, inventory_read),
        (app_path, app_read),
        (cnpg_path, cnpg_read),
    ):
        after = _private(source, uid)
        if (
            after.payload != before.payload
            or after.metadata_fingerprint != before.metadata_fingerprint
            or after.acl_fingerprint != before.acl_fingerprint
        ):
            raise ValueError("application credential backup changed during recovery")
    component_hash = hashes["k8s_secrets"]
    assert isinstance(component_hash, str)
    return app_read.payload, cnpg_read.payload, inventory.inventory_sha256, component_hash


def _value(data: Mapping[str, object], key: str) -> str:
    encoded = data.get(key)
    if not isinstance(encoded, str):
        raise ValueError("application credential field is missing")
    return base64.b64decode(encoded, validate=True).decode("utf-8", errors="strict")


def _require_url(value: str, *, pool: bool, username: str, password: str) -> None:
    if (
        not 1 <= len(value) <= 8192
        or any(ord(char) < 0x21 or ord(char) > 0x7E for char in value)
        or re.search(r"%(?![0-9a-fA-F]{2})", value)
        or "#" in value
    ):
        raise ValueError("application credential URL encoding is invalid")
    parsed = urlsplit(value)
    credentials, separator, endpoint = parsed.netloc.rpartition("@")
    user, colon, secret = credentials.partition(":")
    if (
        parsed.scheme not in {"postgresql", "postgresql+psycopg", "postgresql+asyncpg", "postgres"}
        or not separator
        or not colon
        or "@" in secret
        or endpoint not in (_POOL_HOSTS if pool else _DIRECT_HOSTS)
        or parsed.path != "/loom"
        or unquote_to_bytes(user).decode("utf-8") != username
        or unquote_to_bytes(secret).decode("utf-8") != password
    ):
        raise ValueError("application credential URL authority differs")
    query = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    if len({key for key, _ in query}) != len(query):
        raise ValueError("application credential URL query repeats fields")
    for key, item in query:
        # These options do not override routing, database, role or password.
        if key == "sslmode" and item in {
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        }:
            continue
        if key == "connect_timeout" and re.fullmatch(r"[1-9][0-9]?", item):
            continue
        if key == "application_name" and re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", item):
            continue
        raise ValueError("application credential URL query is not admitted")


def _credential(application: bytes, cnpg: bytes) -> ApplicationRuntimeCredential:
    app, pg = _object(application), _object(cnpg)
    app_data, pg_data = app.get("data"), pg.get("data")
    if (
        app.get("type") != "Opaque"
        or not isinstance(app_data, dict)
        or pg.get("type") != "kubernetes.io/basic-auth"
        or not isinstance(pg_data, dict)
        or set(pg_data) != {"username", "password"}
    ):
        raise ValueError("application credential Secret contract is invalid")
    username, password = _value(app_data, "postgres-user"), _value(app_data, "postgres-password")
    if (
        username != "loom"
        or not 1 <= len(password) <= 1024
        or any(not 0x21 <= ord(character) <= 0x7E for character in password)
        or _value(pg_data, "username") != username
        or _value(pg_data, "password") != password
    ):
        raise ValueError("application credential sources differ or cannot be restored")
    # CNPG 1.25.1 SetUserPassword sends this literal directly to ALTER ROLE.
    # PostgreSQL stores recognized verifiers verbatim, even when configured for
    # SCRAM; our generic restore instead hashes the original literal password.
    # Reserve the SCRAM grammar rather than copying PostgreSQL's permissive C
    # parser (strtok also skips leading '$'). MD5-shaped literals are reserved
    # case-insensitively. Do not normalize the original or silently rotate it.
    if re.fullmatch(r"md5[0-9a-fA-F]{32}", password) or password.lstrip("$").startswith(
        "SCRAM-SHA-256$"
    ):
        raise ValueError("application credential is incompatible with CNPG password refresh")
    for key in _DIRECT_KEYS:
        _require_url(_value(app_data, key), pool=False, username=username, password=password)
        if key + "-pool" in app_data:
            _require_url(
                _value(app_data, key + "-pool"), pool=True, username=username, password=password
            )
    return ApplicationRuntimeCredential(username, password)


def _live_identity(runner: CredentialRecoveryRunner, name: str, expected: bytes) -> tuple[str, str]:
    payload = runner.capture_stdout(
        [
            "kubectl",
            "--namespace",
            _NAMESPACE,
            "get",
            "secret",
            name,
            "--output=json",
            "--request-timeout=30s",
        ],
        env=runner.environment,
        timeout_seconds=30,
    )
    if canonical_secret_export(payload, namespace=_NAMESPACE, name=name) != expected:
        raise ValueError("application credential live Secret differs from backup")
    metadata = _object(payload)["metadata"]
    assert isinstance(metadata, dict)
    uid, version = metadata["uid"], metadata["resourceVersion"]
    if (
        not isinstance(uid, str)
        or _UID.fullmatch(uid) is None
        or not isinstance(version, str)
        or _RESOURCE_VERSION.fullmatch(version) is None
    ):
        raise ValueError("application credential live identity is invalid")
    return uid, version


def recover_application_runtime_credential(
    plan: FinalGatePlan, *, journal: ProtectedApplyJournal, runner: CredentialRecoveryRunner
) -> ApplicationRuntimeCredential:
    """Return originals only after source/live checks and durable intent binding.

    Caller must use the installed protected runner and keep all relevant writers
    serialized. Matching observations cannot prove that serialization themselves.
    Verifier-shaped originals refuse here; server-side SCRAM encryption settings
    for the admitted CNPG writer still require separate configuration admission.
    The returned password is sensitive and must not enter logs or SQL plaintext.
    """
    journal.require_application_credential_context(plan)
    try:
        app, cnpg, inventory_hash, component_hash = _backup_sources(plan, journal.service_uid)
        credential = _credential(app, cnpg)
        capture_cnpg_writer_configuration(plan, journal=journal, runner=runner)
        first = (_live_identity(runner, _APPLICATION, app), _live_identity(runner, _CNPG, cnpg))
        second = (_live_identity(runner, _APPLICATION, app), _live_identity(runner, _CNPG, cnpg))
        if first != second:
            raise ValueError("application credential live identity changed during recovery")
        binding = ApplicationCredentialRecoveryBinding(
            manifest_sha256=plan.backup_manifest_sha256,
            component_sha256=component_hash,
            inventory_sha256=inventory_hash,
            application_sha256=hashlib.sha256(app).hexdigest(),
            application_uid=first[0][0],
            application_resource_version=first[0][1],
            cnpg_sha256=hashlib.sha256(cnpg).hexdigest(),
            cnpg_uid=first[1][0],
            cnpg_resource_version=first[1][1],
        )
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError):
        # Never propagate parser/transport errors containing original credentials.
        raise ValueError("application credential recovery source validation failed") from None
    journal.record_application_credential_recovery(plan, binding=binding)
    return credential
