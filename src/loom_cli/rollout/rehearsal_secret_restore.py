"""Exact checkpoint Secret clone for an isolated rehearsal namespace."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

import yaml  # type: ignore[import-untyped]

from loom_cli.cluster_backup_guard import (
    backup_manifest_sha256,
    validate_backup_manifest,
)
from loom_cli.rollout.credential_authority import read_trusted_file

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_NAMESPACE_RE = re.compile(r"loom-rehearsal-[0-9a-f]{24}\Z")
_SECRET_NAMES = ("loom-admin-secret", "loom-secrets", "loom-staging-tls")
_MAX_MANIFEST_BYTES = 1024 * 1024
_MAX_SECRET_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class RehearsalSecretArtifact:
    """Sensitive apply bytes plus the non-sensitive identity safe for evidence."""

    payload: bytes
    secret_names: tuple[str, ...]
    source_component_sha256: str
    artifact_sha256: str

    def __post_init__(self) -> None:
        if (
            not self.payload
            or self.secret_names != _SECRET_NAMES
            or _SHA256_RE.fullmatch(self.source_component_sha256) is None
            or _SHA256_RE.fullmatch(self.artifact_sha256) is None
            or hashlib.sha256(self.payload).hexdigest() != self.artifact_sha256
        ):
            raise ValueError("rehearsal Secret artifact identity is invalid")


def build_rehearsal_secret_artifact(
    manifest_path: Path,
    *,
    manifest_sha256: str,
    namespace: str,
    plan_digest: str,
    service_uid: int | None = None,
) -> RehearsalSecretArtifact:
    """Revalidate and clone only the three allowlisted checkpoint Secrets."""
    uid = os.geteuid() if service_uid is None else service_uid
    if (
        uid < 0
        or _SHA256_RE.fullmatch(manifest_sha256) is None
        or _NAMESPACE_RE.fullmatch(namespace) is None
        or _SHA256_RE.fullmatch(plan_digest) is None
        or not manifest_path.is_absolute()
        or manifest_path.name != "backup-manifest.json"
        or ".." in manifest_path.parts
    ):
        raise ValueError("rehearsal Secret source authority is invalid")
    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        min_remaining_hours=0,
        expected_owner_uid=uid,
        require_private_files=True,
        enforce_freshness=False,
    )
    if (
        problems
        or backup_manifest_sha256(
            manifest_path,
            expected_owner_uid=uid,
            require_private_file=True,
        )
        != manifest_sha256
    ):
        raise ValueError("rehearsal checkpoint manifest failed exact validation")
    manifest_read = read_trusted_file(
        manifest_path,
        service_uid=uid,
        private=True,
        max_bytes=_MAX_MANIFEST_BYTES,
        require_nonempty=True,
    )
    try:
        manifest = json.loads(
            manifest_read.payload,
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("rehearsal checkpoint manifest is invalid") from exc
    secrets_path, component_sha256 = _secret_component(manifest, manifest_path=manifest_path)
    _require_private_directory(secrets_path, service_uid=uid)
    documents = [
        _read_secret(
            secrets_path / f"{name}.yaml",
            name=name,
            namespace=namespace,
            plan_digest=plan_digest,
            service_uid=uid,
        )
        for name in _SECRET_NAMES
    ]
    payload = yaml.safe_dump_all(documents, sort_keys=True).encode()
    return RehearsalSecretArtifact(
        payload=payload,
        secret_names=_SECRET_NAMES,
        source_component_sha256=component_sha256,
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _secret_component(
    manifest: object,
    *,
    manifest_path: Path,
) -> tuple[Path, str]:
    if not isinstance(manifest, Mapping):
        raise ValueError("rehearsal checkpoint manifest is invalid")
    components = manifest.get("components")
    secret_component = components.get("k8s_secrets") if isinstance(components, Mapping) else None
    if not isinstance(secret_component, Mapping):
        raise ValueError("rehearsal checkpoint Secret component is missing")
    path = secret_component.get("path")
    kind = secret_component.get("kind")
    digest = secret_component.get("sha256")
    expected_path = manifest_path.parent / "secrets"
    if (
        not isinstance(path, str)
        or Path(path) != expected_path
        or kind != "directory"
        or not isinstance(digest, str)
        or _SHA256_RE.fullmatch(digest) is None
    ):
        raise ValueError("rehearsal checkpoint Secret component authority drifted")
    return expected_path, digest


def _read_secret(
    path: Path,
    *,
    name: str,
    namespace: str,
    plan_digest: str,
    service_uid: int,
) -> dict[str, object]:
    trusted = read_trusted_file(
        path,
        service_uid=service_uid,
        private=True,
        max_bytes=_MAX_SECRET_BYTES,
        require_nonempty=True,
    )
    try:
        documents = list(yaml.safe_load_all(trusted.payload))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError("rehearsal checkpoint Secret payload is invalid") from exc
    if len(documents) != 1 or not isinstance(documents[0], Mapping):
        raise ValueError("rehearsal checkpoint Secret payload is invalid")
    source = documents[0]
    metadata = source.get("metadata")
    data = source.get("data")
    secret_type = source.get("type", "Opaque")
    if (
        source.get("apiVersion") != "v1"
        or source.get("kind") != "Secret"
        or not isinstance(metadata, Mapping)
        or metadata.get("name") != name
        or not isinstance(data, Mapping)
        or not data
        or not all(
            isinstance(key, str) and key and isinstance(value, str) and value
            for key, value in data.items()
        )
        or not isinstance(secret_type, str)
        or not secret_type
    ):
        raise ValueError("rehearsal checkpoint Secret identity is invalid")
    return {
        "apiVersion": "v1",
        "data": dict(data),
        "kind": "Secret",
        "metadata": {
            "annotations": {"loom.openai.dev/plan-sha256": plan_digest},
            "name": name,
            "namespace": namespace,
        },
        "type": secret_type,
    }


def _require_private_directory(path: Path, *, service_uid: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ValueError("rehearsal checkpoint Secret directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_nlink < 2
    ):
        raise ValueError("rehearsal checkpoint Secret directory authority is invalid")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> Mapping[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return MappingProxyType(result)


__all__ = ["RehearsalSecretArtifact", "build_rehearsal_secret_artifact"]
