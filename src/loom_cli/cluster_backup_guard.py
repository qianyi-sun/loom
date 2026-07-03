"""Backup manifest helpers for protected Loom cluster environments.

The manifest is intentionally metadata-only: it records paths, sizes, and
digests for operator-created backups without copying secret material into CLI
stdout or durable issue/PR evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REQUIRED_BACKUP_COMPONENTS: tuple[str, ...] = (
    "postgres",
    "minio",
    "k8s_secrets",
)
PROTECTED_ENVIRONMENTS: frozenset[str] = frozenset({
    "staging",
    "production",
})
DEFAULT_BACKUP_MAX_AGE_HOURS = 24


def infer_environment(
    *,
    environment: str | None,
    namespace: str,
) -> str:
    if environment:
        return environment
    lowered = namespace.lower()
    if "staging" in lowered:
        return "staging"
    if "staging" in lowered or "stage" in lowered:
        return "staging"
    if "prod" in lowered:
        return "production"
    return "development"


def is_protected_environment(
    *,
    environment: str | None,
    namespace: str,
) -> bool:
    return infer_environment(
        environment=environment,
        namespace=namespace,
    ) in PROTECTED_ENVIRONMENTS


def _parse_time(raw: object) -> datetime | None:
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _file_digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _directory_stats(path: Path) -> tuple[int, int, str]:
    h = hashlib.sha256()
    file_count = 0
    total_size = 0
    for child in sorted(p for p in path.rglob("*") if p.is_file()):
        rel = child.relative_to(path).as_posix().encode("utf-8")
        data = child.read_bytes()
        h.update(rel)
        h.update(b"\0")
        h.update(hashlib.sha256(data).digest())
        file_count += 1
        total_size += len(data)
    return file_count, total_size, h.hexdigest()


def _component_metadata(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"backup component does not exist: {resolved}")
    if resolved.is_dir():
        file_count, total_size, digest = _directory_stats(resolved)
        if file_count == 0:
            raise ValueError(f"backup component directory is empty: {resolved}")
        return {
            "path": str(resolved),
            "kind": "directory",
            "file_count": file_count,
            "size_bytes": total_size,
            "sha256": digest,
        }
    if resolved.is_file():
        size = resolved.stat().st_size
        if size <= 0:
            raise ValueError(f"backup component file is empty: {resolved}")
        return {
            "path": str(resolved),
            "kind": "file",
            "size_bytes": size,
            "sha256": _file_digest(resolved),
        }
    raise ValueError(f"backup component must be file or directory: {resolved}")


def write_backup_manifest(
    *,
    environment: str,
    namespace: str,
    output_path: Path,
    components: dict[str, Path],
    now: datetime | None = None,
) -> dict[str, Any]:
    missing = sorted(set(REQUIRED_BACKUP_COMPONENTS) - set(components))
    if missing:
        raise ValueError(
            "missing required backup component(s): " + ", ".join(missing),
        )
    created_at = (now or datetime.now(UTC)).astimezone(UTC)
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "environment": environment,
        "namespace": namespace,
        "created_at": created_at.isoformat(),
        "components": {
            name: _component_metadata(path)
            for name, path in sorted(components.items())
        },
        "verification": {
            "status": "verified",
            "checked_at": created_at.isoformat(),
            "required_components": list(REQUIRED_BACKUP_COMPONENTS),
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(output_path, 0o600)
    return manifest


def validate_backup_manifest(
    manifest_path: Path | None,
    *,
    environment: str,
    namespace: str,
    max_age_hours: int = DEFAULT_BACKUP_MAX_AGE_HOURS,
    now: datetime | None = None,
) -> list[str]:
    if manifest_path is None:
        return ["backup manifest is required"]
    path = Path(manifest_path)
    if not path.exists():
        return [f"backup manifest not found: {path}"]
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"backup manifest is not readable JSON: {type(exc).__name__}: {exc}"]
    problems: list[str] = []
    if manifest.get("schema_version") != 1:
        problems.append("backup manifest schema_version must be 1")
    if manifest.get("environment") != environment:
        problems.append(
            f"backup manifest environment {manifest.get('environment')!r} "
            f"does not match {environment!r}",
        )
    if manifest.get("namespace") != namespace:
        problems.append(
            f"backup manifest namespace {manifest.get('namespace')!r} "
            f"does not match {namespace!r}",
        )
    created_at = _parse_time(manifest.get("created_at"))
    if created_at is None:
        problems.append("backup manifest created_at is missing or invalid")
    else:
        age = (now or datetime.now(UTC)).astimezone(UTC) - created_at
        if age.total_seconds() < 0:
            problems.append("backup manifest created_at is in the future")
        elif age > timedelta(hours=max_age_hours):
            problems.append(
                f"backup manifest is stale: age exceeds {max_age_hours}h",
            )
    verification = manifest.get("verification")
    if not isinstance(verification, dict) or verification.get("status") != "verified":
        problems.append("backup manifest verification.status must be 'verified'")
    components = manifest.get("components")
    if not isinstance(components, dict):
        problems.append("backup manifest components must be an object")
        return problems
    for name in REQUIRED_BACKUP_COMPONENTS:
        component = components.get(name)
        if not isinstance(component, dict):
            problems.append(f"backup manifest missing component {name!r}")
            continue
        size = component.get("size_bytes")
        if not isinstance(size, int) or size <= 0:
            problems.append(f"backup component {name!r} has no recorded bytes")
        component_path = component.get("path")
        if not isinstance(component_path, str) or not component_path:
            problems.append(f"backup component {name!r} has no path")
            continue
        if not Path(component_path).exists():
            problems.append(
                f"backup component {name!r} path no longer exists: {component_path}",
            )
    return problems
