"""Canonical owner evidence for personal-development acceptance."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS, PERSONAL_DEV_PLATFORMS
from loom.personal_dev_control_plane_config import (
    PersonalDevAcceptancePlan,
    PersonalDevControlPlaneProfile,
    PersonalDevOperationalPlan,
    PersonalDevTrustedRelease,
)

_DIGEST = re.compile(r"[0-9a-f]{64}")
_GIT_IDENTITY = re.compile(r"[0-9a-f]{40}")
_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
_MAX_SOURCE_BYTES = 16 * 1024 * 1024
_LAUNCHER_SOURCE_FILES = (
    "src/loom_capacity_executor/bootstrap_handoff.py",
    "src/loom_capacity_executor/runtime.py",
    "src/loom_capacity_executor/trusted_launcher.py",
)
_SCANNER_SOURCE_FILE = "src/loom/personal_dev_builder_tools.py"
_MINIO_BUCKETS = ("artifacts", "trajectories")
_SCANNER_ARGV = (
    "image",
    "--input",
    "<verified-oci-archive>",
    "--format",
    "json",
    "--scanners",
    "vuln,secret",
    "--severity",
    "HIGH,CRITICAL",
    "--exit-code",
    "1",
    "--no-progress",
    "--offline-scan",
    "--skip-db-update",
    "--skip-java-db-update",
    "--cache-dir",
    "<release-bound-cache>",
)
_MANAGEMENT_SECRET_KEYS = (
    "admin-secrets.toml",
    "capacity-lifecycle-ca.pem",
    "capacity-lifecycle-certificate.pem",
    "capacity-lifecycle-private-key.pem",
    "capacity-lifecycle-token",
    "capacity-reporter-ca.pem",
    "capacity-reporter-certificate.pem",
    "capacity-reporter-private-key.pem",
    "config.json",
    "dev-instance-database-admin-url",
    "minio-access-key",
    "minio-secret-key",
    "postgres-database",
    "postgres-password",
    "postgres-user",
    "secret-store-master-key",
    "svc-db-url",
)


class PersonalDevAcceptanceEvidenceError(ValueError):
    """Personal-development acceptance evidence is invalid."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _SourceBinding(_StrictModel):
    commit: str
    tree: str

    @field_validator("commit", "tree")
    @classmethod
    def _git_identity(cls, value: str) -> str:
        if _GIT_IDENTITY.fullmatch(value) is None:
            raise ValueError("source identity is invalid")
        return value


class _PostgresRestore(_StrictModel):
    dump_sha256: str
    image: str
    source_schema_head: str
    restored_schema_head: str
    source_state_sha256: str
    restored_state_sha256: str

    @field_validator("dump_sha256", "source_state_sha256", "restored_state_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None or value == "0" * 64:
            raise ValueError("Postgres evidence digest is invalid")
        return value

    @field_validator("source_schema_head", "restored_schema_head")
    @classmethod
    def _schema_head(cls, value: str) -> str:
        if re.fullmatch(r"[0-9]{4}", value) is None:
            raise ValueError("Postgres schema head is invalid")
        return value

    @model_validator(mode="after")
    def _restored_state_matches(self) -> _PostgresRestore:
        if (
            self.source_schema_head != self.restored_schema_head
            or self.source_state_sha256 != self.restored_state_sha256
        ):
            raise ValueError("Postgres restore does not match the source snapshot")
        return self


class _MinioRestore(_StrictModel):
    backup_manifest_sha256: str
    image: str
    source_object_count: Literal[0]
    restored_object_count: Literal[0]
    restored_manifest_sha256: str

    @field_validator("backup_manifest_sha256", "restored_manifest_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None or value == "0" * 64:
            raise ValueError("MinIO evidence digest is invalid")
        return value

    @model_validator(mode="after")
    def _restored_state_matches(self) -> _MinioRestore:
        if (
            self.source_object_count != self.restored_object_count
            or self.backup_manifest_sha256 != self.restored_manifest_sha256
        ):
            raise ValueError("MinIO restore does not match the source snapshot")
        return self


class _SecretBoundary(_StrictModel):
    key_inventory_sha256: str
    values_included: Literal[False]

    @field_validator("key_inventory_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None or value == "0" * 64:
            raise ValueError("Secret inventory digest is invalid")
        return value


class _StorageBoundary(_StrictModel):
    postgres_pvc: Literal["data-loom-dev-postgres-0"]
    minio_pvc: Literal["data-loom-dev-minio-0"]
    storage_class: Literal["longhorn"]


class _ManagerBoundary(_StrictModel):
    executable_new_capacity_ceiling: Literal[0]
    personal_worker_count: Literal[0]


class _CleanupBoundary(_StrictModel):
    isolated_postgres_absent: Literal[True]
    isolated_minio_absent: Literal[True]
    isolated_network_absent: Literal[True]


class PersonalDevBackupRestoreEvidence(_StrictModel):
    schema_name: Literal["loom-personal-dev-backup-restore-evidence-v1"] = Field(alias="schema")
    source: _SourceBinding
    release_sha256: str
    namespace: Literal["loom-dev"]
    started_at: str
    completed_at: str
    postgres: _PostgresRestore
    minio: _MinioRestore
    secrets: _SecretBoundary
    storage: _StorageBoundary
    manager: _ManagerBoundary
    cleanup: _CleanupBoundary

    @field_validator("release_sha256")
    @classmethod
    def _release_digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None or value == "0" * 64:
            raise ValueError("release digest is invalid")
        return value

    @field_validator("started_at", "completed_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        if _TIMESTAMP.fullmatch(value) is None:
            raise ValueError("evidence timestamp is invalid")
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        return value

    @model_validator(mode="after")
    def _time_order(self) -> PersonalDevBackupRestoreEvidence:
        started = datetime.strptime(self.started_at, "%Y-%m-%dT%H:%M:%SZ")
        completed = datetime.strptime(self.completed_at, "%Y-%m-%dT%H:%M:%SZ")
        if completed < started:
            raise ValueError("backup/restore evidence time order is invalid")
        return self


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _duplicate_rejecting_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _identity(metadata: os.stat_result) -> tuple[int, ...]:
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


def _read_owner_only(path: Path) -> bytes:
    descriptor: int | None = None
    try:
        before_path = path.lstat()
        if (
            not stat.S_ISREG(before_path.st_mode)
            or stat.S_ISLNK(before_path.st_mode)
            or before_path.st_uid != os.geteuid()
            or stat.S_IMODE(before_path.st_mode) != 0o600
            or before_path.st_nlink != 1
            or not 0 < before_path.st_size <= _MAX_EVIDENCE_BYTES
        ):
            raise ValueError
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before_path):
            raise ValueError
        payload = bytearray()
        while len(payload) <= _MAX_EVIDENCE_BYTES:
            chunk = os.read(
                descriptor,
                min(64 * 1024, _MAX_EVIDENCE_BYTES + 1 - len(payload)),
            )
            if not chunk:
                break
            payload.extend(chunk)
        if (
            len(payload) != opened.st_size
            or _identity(os.fstat(descriptor)) != _identity(opened)
            or _identity(path.lstat()) != _identity(before_path)
        ):
            raise ValueError
        return bytes(payload)
    except (OSError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _load_json(path: Path, expected_sha256: str) -> tuple[bytes, dict[str, Any]]:
    if _DIGEST.fullmatch(expected_sha256) is None or expected_sha256 == "0" * 64:
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    payload = _read_owner_only(path)
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), expected_sha256):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_duplicate_rejecting_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        if not isinstance(value, dict) or _canonical_json(value) != payload:
            raise ValueError
    except (RecursionError, UnicodeError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    return payload, value


def _parse_json_document(path: Path, *, canonical: bool) -> tuple[bytes, Any]:
    payload = _read_owner_only(path)
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_duplicate_rejecting_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        if canonical and _canonical_json(value) != payload:
            raise ValueError
    except (RecursionError, UnicodeError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    return payload, value


def _sha256_owner_only_file(path: Path) -> str:
    descriptor: int | None = None
    try:
        before_path = path.lstat()
        if (
            not stat.S_ISREG(before_path.st_mode)
            or stat.S_ISLNK(before_path.st_mode)
            or before_path.st_uid != os.geteuid()
            or stat.S_IMODE(before_path.st_mode) != 0o600
            or before_path.st_nlink != 1
            or before_path.st_size <= 0
        ):
            raise ValueError
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before_path):
            raise ValueError
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
        if (
            total != opened.st_size
            or _identity(os.fstat(descriptor)) != _identity(opened)
            or _identity(path.lstat()) != _identity(before_path)
        ):
            raise ValueError
        return digest.hexdigest()
    except (OSError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_source_file(path: Path) -> bytes:
    descriptor: int | None = None
    try:
        before_path = path.lstat()
        if (
            not stat.S_ISREG(before_path.st_mode)
            or stat.S_ISLNK(before_path.st_mode)
            or before_path.st_uid != os.geteuid()
            or before_path.st_nlink != 1
            or not 0 < before_path.st_size <= _MAX_SOURCE_BYTES
        ):
            raise ValueError
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before_path):
            raise ValueError
        payload = bytearray()
        while len(payload) <= _MAX_SOURCE_BYTES:
            chunk = os.read(
                descriptor,
                min(64 * 1024, _MAX_SOURCE_BYTES + 1 - len(payload)),
            )
            if not chunk:
                break
            payload.extend(chunk)
        if (
            len(payload) != opened.st_size
            or _identity(os.fstat(descriptor)) != _identity(opened)
            or _identity(path.lstat()) != _identity(before_path)
        ):
            raise ValueError
        return bytes(payload)
    except (OSError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _git_output(
    source_root: Path,
    *arguments: str,
    maximum_bytes: int,
) -> bytes:
    environment = {
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_NO_LAZY_FETCH": "1",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_TERMINAL_PROMPT": "0",
        "LC_ALL": "C",
        "PATH": "/usr/bin:/bin",
    }
    try:
        result = subprocess.run(
            [
                "/usr/bin/git",
                "--no-replace-objects",
                "-C",
                str(source_root),
                *arguments,
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
            env=environment,
        )
        if result.returncode != 0 or len(result.stdout) > maximum_bytes:
            raise ValueError
        return result.stdout
    except (OSError, subprocess.SubprocessError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None


def _validate_source_root(
    source_root: Path,
    release: PersonalDevTrustedRelease,
    relative_files: tuple[str, ...],
) -> Path:
    try:
        if not source_root.is_absolute():
            raise ValueError
        root = source_root.resolve(strict=True)
        if root != source_root or not root.is_dir():
            raise ValueError
        top_level = Path(
            os.fsdecode(
                _git_output(
                    root,
                    "rev-parse",
                    "--show-toplevel",
                    maximum_bytes=4096,
                )
            ).strip()
        ).resolve(strict=True)
        head = _git_output(
            root,
            "rev-parse",
            "--verify",
            "HEAD^{commit}",
            maximum_bytes=128,
        ).decode("ascii").strip()
        tree = _git_output(
            root,
            "rev-parse",
            "--verify",
            "HEAD^{tree}",
            maximum_bytes=128,
        ).decode("ascii").strip()
        status = _git_output(
            root,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--",
            *relative_files,
            maximum_bytes=4096,
        )
        if (
            top_level != root
            or head != release.source_sha
            or tree != release.source_tree
            or status
        ):
            raise ValueError
        return root
    except (OSError, UnicodeError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None


def _source_file_sha256(source_root: Path, source_sha: str, relative: str) -> str:
    object_spec = f"{source_sha}:{relative}"
    try:
        raw_size = _git_output(
            source_root,
            "cat-file",
            "-s",
            object_spec,
            maximum_bytes=64,
        ).decode("ascii").strip()
        if re.fullmatch(r"[0-9]+", raw_size) is None:
            raise ValueError
        size = int(raw_size)
        if not 0 < size <= _MAX_SOURCE_BYTES:
            raise ValueError
        payload = _git_output(
            source_root,
            "cat-file",
            "blob",
            object_spec,
            maximum_bytes=size,
        )
        if len(payload) != size:
            raise ValueError
    except (UnicodeError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    if not hmac.compare_digest(payload, _read_source_file(source_root / relative)):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    return hashlib.sha256(payload).hexdigest()


def build_personal_dev_trusted_launcher_profile(
    *,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    source_root: Path,
) -> dict[str, object]:
    """Derive the exact launcher profile from the checked-out source and profile."""

    source_root = _validate_source_root(source_root, release, _LAUNCHER_SOURCE_FILES)
    value = {
        "contract": {
            "candidate_argv_absolute": True,
            "candidate_executable_identity": True,
            "candidate_image_digest": True,
            "immutable_candidate_snapshot": True,
            "release_digest": True,
            "single_use_bootstrap_handoff": True,
        },
        "files": {
            relative: _source_file_sha256(source_root, release.source_sha, relative)
            for relative in _LAUNCHER_SOURCE_FILES
        },
        "protocol_versions": dict(sorted(profile.protocol_versions.items())),
        "schema": "loom-personal-dev-trusted-launcher-profile-v1",
        "source": {
            "commit": release.source_sha,
            "tree": release.source_tree,
        },
    }
    _validate_source_root(source_root, release, _LAUNCHER_SOURCE_FILES)
    return value


def build_personal_dev_scanner_finding_policy(
    *,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    source_root: Path,
) -> dict[str, object]:
    """Derive the exact offline scanner policy from source and trusted release."""

    source_root = _validate_source_root(source_root, release, (_SCANNER_SOURCE_FILE,))
    scanner = release.scanner
    value: dict[str, object] = {
        "argv": list(_SCANNER_ARGV),
        "components": list(PERSONAL_DEV_COMPONENTS),
        "denied_finding_fields": [
            "Licenses",
            "Misconfigurations",
            "Secrets",
            "Vulnerabilities",
        ],
        "limits": {
            "max_report_bytes": 16 * 1024 * 1024,
            "timeout_seconds": 900,
        },
        "platforms": list(PERSONAL_DEV_PLATFORMS),
        "release_scanner": {
            "binary_platform": scanner.binary_platform,
            "binary_sha256": scanner.binary_sha256,
            "cache_identity_sha256": scanner.cache_identity_sha256,
            "database_metadata_sha256": scanner.database_metadata_sha256,
            "database_sha256": scanner.database_sha256,
            "java_database_metadata_sha256": scanner.java_database_metadata_sha256,
            "java_database_sha256": scanner.java_database_sha256,
            "lock_sha256": scanner.lock_sha256,
            "trivy_version": scanner.trivy_version,
        },
        "schema": "loom-personal-dev-scanner-finding-policy-v1",
        "source": {
            "commit": release.source_sha,
            "tree": release.source_tree,
        },
        "source_file_sha256": _source_file_sha256(
            source_root,
            release.source_sha,
            _SCANNER_SOURCE_FILE,
        ),
    }
    _validate_source_root(source_root, release, (_SCANNER_SOURCE_FILE,))
    return value


def validate_personal_dev_policy_evidence(
    *,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    plan: PersonalDevAcceptancePlan | PersonalDevOperationalPlan,
    source_root: Path,
    trusted_launcher_profile_path: Path,
    scanner_finding_policy_path: Path,
) -> None:
    """Require exact canonical policy artifacts and plan digest bindings."""

    expected_launcher = _canonical_json(
        build_personal_dev_trusted_launcher_profile(
            profile=profile,
            release=release,
            source_root=source_root,
        )
    )
    launcher_payload, _ = _load_json(
        trusted_launcher_profile_path,
        plan.builder.trusted_launcher_profile_sha256,
    )
    expected_scanner = _canonical_json(
        build_personal_dev_scanner_finding_policy(
            profile=profile,
            release=release,
            source_root=source_root,
        )
    )
    scanner_payload, _ = _load_json(
        scanner_finding_policy_path,
        plan.builder.scanner_finding_policy_sha256,
    )
    if not hmac.compare_digest(launcher_payload, expected_launcher) or not hmac.compare_digest(
        scanner_payload,
        expected_scanner,
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")


def _validate_postgres_state(payload: bytes) -> None:
    try:
        if not payload.endswith(b"\n") or b"\r" in payload:
            raise ValueError
        lines = payload.decode("ascii").splitlines()
        parsed: list[tuple[str, str, int, str]] = []
        for line in lines:
            record_type, name, numeric_value, state_value = line.split("\t")
            if not name or "\x00" in name:
                raise ValueError
            if record_type == "table":
                if (
                    re.fullmatch(r"[0-9]+", numeric_value) is None
                    or _DIGEST.fullmatch(state_value) is None
                    or state_value == "0" * 64
                ):
                    raise ValueError
            elif record_type == "sequence":
                if (
                    re.fullmatch(r"-?[0-9]+", numeric_value) is None
                    or state_value not in {"f", "t"}
                ):
                    raise ValueError
            else:
                raise ValueError
            parsed.append((record_type, name, int(numeric_value), state_value))
        identities = [(record_type, name) for record_type, name, _, _ in parsed]
        if (
            not parsed
            or identities != sorted(identities)
            or len(set(identities)) != len(identities)
            or not any(record_type == "table" for record_type, _, _, _ in parsed)
        ):
            raise ValueError
    except (UnicodeError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None


def _validate_minio_manifest(value: Any) -> int:
    if not isinstance(value, dict) or set(value) != {"buckets", "objects"}:
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    buckets = value["buckets"]
    objects = value["objects"]
    if (
        not isinstance(buckets, list)
        or tuple(buckets) != _MINIO_BUCKETS
        or not isinstance(objects, list)
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    identities: list[tuple[str, str]] = []
    for item in objects:
        if (
            not isinstance(item, dict)
            or set(item) != {"bucket", "key", "sha256", "size"}
            or not isinstance(item["bucket"], str)
            or item["bucket"] not in buckets
            or not isinstance(item["key"], str)
            or not item["key"]
            or not isinstance(item["sha256"], str)
            or _DIGEST.fullmatch(item["sha256"]) is None
            or item["sha256"] == "0" * 64
            or type(item["size"]) is not int
            or item["size"] < 0
        ):
            raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
        identities.append((item["bucket"], item["key"]))
    if identities != sorted(identities) or len(set(identities)) != len(identities):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    return len(objects)


def _validate_shadow_status(value: Any) -> None:
    if (
        not isinstance(value, dict)
        or value.get("mode") != "shadow"
        or value.get("ready") is not True
        or value.get("blockers") != []
        or type(value.get("manager_ceiling")) is not int
        or value.get("manager_ceiling") != 0
        or value.get("worker_available") is not False
        or not isinstance(value.get("components"), list)
        or not any(
            isinstance(component, dict)
            and component.get("name") == "personal-workers"
            and type(component.get("observed")) is int
            and component.get("observed") == 0
            for component in value["components"]
        )
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")


def _validate_storage_inventory(
    value: Any,
    *,
    release: PersonalDevTrustedRelease,
) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("items"), list):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    items = value["items"]
    pvcs = {
        item.get("metadata", {}).get("name"): item
        for item in items
        if isinstance(item, dict) and item.get("kind") == "PersistentVolumeClaim"
    }
    statefulsets = {
        item.get("metadata", {}).get("name"): item
        for item in items
        if isinstance(item, dict) and item.get("kind") == "StatefulSet"
    }
    if set(pvcs) != {"data-loom-dev-postgres-0", "data-loom-dev-minio-0"} or any(
        item.get("spec", {}).get("storageClassName") != "longhorn" for item in pvcs.values()
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")

    def image(name: str, container_name: str) -> str | None:
        item = statefulsets.get(name, {})
        containers = item.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        if not isinstance(containers, list):
            return None
        matches = [
            container.get("image")
            for container in containers
            if isinstance(container, dict) and container.get("name") == container_name
        ]
        return matches[0] if len(matches) == 1 and isinstance(matches[0], str) else None

    if (
        set(statefulsets) != {"loom-dev-postgres", "loom-dev-minio"}
        or image("loom-dev-postgres", "postgres") != release.images.postgres
        or image("loom-dev-minio", "minio") != release.images.minio
        or image("loom-dev-minio", "admin") != release.images.minio_client
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")


def build_personal_dev_backup_restore_evidence(
    *,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    release_sha256: str,
    started_at: str,
    completed_at: str,
    postgres_dump_path: Path,
    postgres_source_state_path: Path,
    postgres_restored_state_path: Path,
    source_schema_head: str,
    restored_schema_head: str,
    minio_source_manifest_path: Path,
    minio_restored_manifest_path: Path,
    secret_key_inventory_path: Path,
    pre_shadow_status_path: Path,
    post_shadow_status_path: Path,
    storage_inventory_path: Path,
) -> dict[str, object]:
    """Derive a canonical backup/restore record from exact supporting evidence."""

    if _DIGEST.fullmatch(release_sha256) is None or release_sha256 == "0" * 64:
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    source_state = _read_owner_only(postgres_source_state_path)
    restored_state = _read_owner_only(postgres_restored_state_path)
    _validate_postgres_state(source_state)
    _validate_postgres_state(restored_state)
    if not hmac.compare_digest(source_state, restored_state):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    source_manifest, source_manifest_value = _parse_json_document(
        minio_source_manifest_path, canonical=True
    )
    restored_manifest, restored_manifest_value = _parse_json_document(
        minio_restored_manifest_path, canonical=True
    )
    object_count = _validate_minio_manifest(source_manifest_value)
    if (
        object_count != 0
        or _validate_minio_manifest(restored_manifest_value) != object_count
        or not hmac.compare_digest(source_manifest, restored_manifest)
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")

    secret_payload, secret_value = _parse_json_document(secret_key_inventory_path, canonical=False)
    expected_secret_inventory = {
        "items": [
            {"keys": list(_MANAGEMENT_SECRET_KEYS), "name": profile.identities.management_secret},
            {"keys": ["private-key"], "name": profile.identities.activation_private_secret},
            {"keys": ["public-key"], "name": profile.identities.activation_public_secret},
        ]
    }
    expected_secret_inventory["items"].sort(key=lambda item: str(item["name"]))
    if secret_value != expected_secret_inventory:
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")

    _, pre_status = _parse_json_document(pre_shadow_status_path, canonical=False)
    _, post_status = _parse_json_document(post_shadow_status_path, canonical=False)
    _validate_shadow_status(pre_status)
    _validate_shadow_status(post_status)
    _, storage_inventory = _parse_json_document(storage_inventory_path, canonical=False)
    _validate_storage_inventory(storage_inventory, release=release)

    state_sha256 = hashlib.sha256(source_state).hexdigest()
    manifest_sha256 = hashlib.sha256(source_manifest).hexdigest()
    value: dict[str, object] = {
        "cleanup": {
            "isolated_minio_absent": True,
            "isolated_network_absent": True,
            "isolated_postgres_absent": True,
        },
        "completed_at": completed_at,
        "manager": {
            "executable_new_capacity_ceiling": 0,
            "personal_worker_count": 0,
        },
        "minio": {
            "backup_manifest_sha256": manifest_sha256,
            "image": release.images.minio,
            "restored_manifest_sha256": manifest_sha256,
            "restored_object_count": object_count,
            "source_object_count": object_count,
        },
        "namespace": "loom-dev",
        "postgres": {
            "dump_sha256": _sha256_owner_only_file(postgres_dump_path),
            "image": release.images.postgres,
            "restored_schema_head": restored_schema_head,
            "restored_state_sha256": state_sha256,
            "source_schema_head": source_schema_head,
            "source_state_sha256": state_sha256,
        },
        "release_sha256": release_sha256,
        "schema": "loom-personal-dev-backup-restore-evidence-v1",
        "secrets": {
            "key_inventory_sha256": hashlib.sha256(secret_payload).hexdigest(),
            "values_included": False,
        },
        "source": {"commit": release.source_sha, "tree": release.source_tree},
        "started_at": started_at,
        "storage": {
            "minio_pvc": "data-loom-dev-minio-0",
            "postgres_pvc": "data-loom-dev-postgres-0",
            "storage_class": "longhorn",
        },
    }
    try:
        parsed = PersonalDevBackupRestoreEvidence.model_validate(value)
    except ValueError:
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    return parsed.model_dump(mode="json", by_alias=True)


def load_personal_dev_backup_restore_evidence(
    path: Path,
    *,
    expected_sha256: str,
    release: PersonalDevTrustedRelease,
    release_sha256: str,
    expected_schema_head: str,
) -> PersonalDevBackupRestoreEvidence:
    """Load and semantically validate one canonical backup/restore drill record."""

    payload, value = _load_json(path, expected_sha256)
    try:
        parsed = PersonalDevBackupRestoreEvidence.model_validate(value)
    except ValueError:
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    if (
        parsed.source.commit != release.source_sha
        or parsed.source.tree != release.source_tree
        or parsed.release_sha256 != release_sha256
        or parsed.postgres.image != release.images.postgres
        or parsed.minio.image != release.images.minio
        or parsed.postgres.source_schema_head != expected_schema_head
        or _canonical_json(parsed.model_dump(mode="json", by_alias=True)) != payload
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    return parsed


__all__ = [
    "PersonalDevAcceptanceEvidenceError",
    "PersonalDevBackupRestoreEvidence",
    "build_personal_dev_backup_restore_evidence",
    "build_personal_dev_scanner_finding_policy",
    "build_personal_dev_trusted_launcher_profile",
    "load_personal_dev_backup_restore_evidence",
    "validate_personal_dev_policy_evidence",
]
