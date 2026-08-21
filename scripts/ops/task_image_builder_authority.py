#!/usr/bin/env python3
"""Load the exact Phase 1 producer/parser authority component binding."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
MANIFEST_RELATIVE_PATH = Path("deploy/task-image-builder/authority-components-v1.json")
DEFAULT_MANIFEST = ROOT / MANIFEST_RELATIVE_PATH
SCHEMA = "loom.task-image-builder-authority-components/v1"
COMPONENT_COUNT = 11
MAX_FILE_BYTES = 4 * 1024 * 1024
AUTHORITIES = {"slurm", "host", "maintenance", "collection", "conformance"}
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
NAME_RE = re.compile(r"^[a-z][a-z0-9_]{2,63}$")


class AuthorityError(ValueError):
    """The authority manifest or one of its listed components is unsafe."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _read_regular(path: Path, label: str) -> bytes:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | os.O_NOFOLLOW
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        initial = os.fstat(descriptor)
        if (
            not stat.S_ISREG(initial.st_mode)
            or initial.st_size > MAX_FILE_BYTES
            or initial.st_mode & 0o002
        ):
            raise AuthorityError(f"{label} metadata is unsafe")
        chunks: list[bytes] = []
        remaining = MAX_FILE_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        final = os.fstat(descriptor)
        if len(payload) != initial.st_size or len(payload) > MAX_FILE_BYTES or (
            initial.st_dev,
            initial.st_ino,
            initial.st_size,
            initial.st_mtime_ns,
            initial.st_ctime_ns,
        ) != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
            final.st_ctime_ns,
        ):
            raise AuthorityError(f"{label} changed while being read")
        return payload
    except AuthorityError:
        raise
    except OSError as exc:
        raise AuthorityError(f"{label} is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


@dataclass(frozen=True)
class AuthorityBinding:
    manifest_sha256: str
    component_digests: Mapping[str, str]

    @property
    def digest(self) -> str:
        return hashlib.sha256(_canonical(self.as_dict())).hexdigest()

    def as_dict(self) -> dict[str, object]:
        return {
            "authority_manifest_sha256": self.manifest_sha256,
            "authority_component_digests": dict(sorted(self.component_digests.items())),
        }


def load_authority_binding(
    candidate_root: Path,
    manifest_path: Path | None = None,
) -> AuthorityBinding:
    if not candidate_root.is_absolute() or candidate_root.is_symlink():
        raise AuthorityError("authority candidate root is unsafe")
    selected_manifest = manifest_path or candidate_root / MANIFEST_RELATIVE_PATH
    manifest_payload = _read_regular(selected_manifest, "authority manifest")
    try:
        manifest = json.loads(manifest_payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError("authority manifest is not valid JSON") from exc
    if (
        not isinstance(manifest, dict)
        or set(manifest) != {"schema", "version", "component_count", "components"}
        or manifest.get("schema") != SCHEMA
        or manifest.get("version") != 1
        or manifest.get("component_count") != COMPONENT_COUNT
        or not isinstance(manifest.get("components"), list)
        or len(manifest["components"]) != COMPONENT_COUNT
    ):
        raise AuthorityError("authority manifest contract is invalid")

    names: set[str] = set()
    paths: set[str] = set()
    covered: set[str] = set()
    digests: dict[str, str] = {}
    for raw in manifest["components"]:
        if not isinstance(raw, dict) or set(raw) != {"name", "path", "authorities"}:
            raise AuthorityError("authority component contract is invalid")
        name = raw.get("name")
        relative_raw = raw.get("path")
        component_authorities = raw.get("authorities")
        if (
            not isinstance(name, str)
            or NAME_RE.fullmatch(name) is None
            or name in names
            or not isinstance(relative_raw, str)
            or relative_raw in paths
            or not isinstance(component_authorities, list)
            or not component_authorities
            or not all(isinstance(item, str) for item in component_authorities)
            or len(component_authorities) != len(set(component_authorities))
            or not set(component_authorities).issubset(AUTHORITIES)
        ):
            raise AuthorityError("authority component is duplicate or invalid")
        relative = PurePosixPath(relative_raw)
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or len(relative.parts) < 3
            or relative.parts[:2] not in {("scripts", "ops"), ("deploy", "slurm")}
            or relative.suffix not in {".py", ".sh"}
        ):
            raise AuthorityError("authority component path is unsafe")
        names.add(name)
        paths.add(relative_raw)
        covered.update(component_authorities)
        payload = _read_regular(candidate_root / relative_raw, f"authority component {name}")
        digests[name] = hashlib.sha256(payload).hexdigest()
    if covered != AUTHORITIES:
        raise AuthorityError("authority manifest coverage is incomplete")
    return AuthorityBinding(
        manifest_sha256=hashlib.sha256(manifest_payload).hexdigest(),
        component_digests=digests,
    )


def validate_authority_binding(
    value: Mapping[str, object],
    expected: AuthorityBinding,
) -> None:
    manifest_digest = value.get("authority_manifest_sha256")
    components = value.get("authority_component_digests")
    if (
        not isinstance(manifest_digest, str)
        or SHA256_RE.fullmatch(manifest_digest) is None
        or manifest_digest != expected.manifest_sha256
        or not isinstance(components, dict)
        or not all(isinstance(key, str) and isinstance(item, str) for key, item in components.items())
        or components != dict(expected.component_digests)
    ):
        raise AuthorityError("authority component binding is invalid")
