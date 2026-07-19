"""Immutable private publication for build-once preflight artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict
from uuid import uuid4

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.image_readiness import ImageArtifactSet
from loom_cli.rollout.manifest_readiness import ManifestArtifact

_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REVISION_RE = re.compile(r"^[0-9]{4}(?:_[a-z0-9_]+)?$")
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_DESCRIPTOR_BYTES = 1024 * 1024
_MAX_MANIFEST_BYTES = 16 * 1024 * 1024


class PreflightArtifactStoreError(RuntimeError):
    """Raised when build-once artifact publication is unsafe or inconsistent."""


class _ParsedDescriptor(TypedDict):
    browser_report_schema_sha256: str
    candidate_sha: str
    candidate_tree: str
    image_artifact_sha256: str
    manifest_artifact_sha256: str
    migration_plan_sha256: str
    migration_target_revision: str
    mutation_epoch: int
    rendered_manifest_sha256: str
    resource_set_digest: str


@dataclass(frozen=True, slots=True)
class PreflightArtifactPublication:
    """Exact private files consumed by rehearsal and the final rollout."""

    candidate_sha: str
    candidate_tree: str
    mutation_epoch: int
    bundle_digest: str
    descriptor_path: Path
    rendered_manifest_path: Path
    image_artifact_sha256: str
    manifest_artifact_sha256: str
    rendered_manifest_sha256: str
    migration_plan_sha256: str
    migration_target_revision: str
    browser_report_schema_sha256: str

    def __post_init__(self) -> None:
        if (
            _SHA_RE.fullmatch(self.candidate_sha) is None
            or len(self.candidate_tree) != 40
            or any(character not in "0123456789abcdef" for character in self.candidate_tree)
            or self.mutation_epoch < 0
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.bundle_digest,
                    self.image_artifact_sha256,
                    self.manifest_artifact_sha256,
                    self.rendered_manifest_sha256,
                    self.migration_plan_sha256,
                    self.browser_report_schema_sha256,
                )
            )
            or not self.descriptor_path.is_absolute()
            or _REVISION_RE.fullmatch(self.migration_target_revision) is None
            or not self.rendered_manifest_path.is_absolute()
            or ".." in self.descriptor_path.parts
            or ".." in self.rendered_manifest_path.parts
            or self.descriptor_path.parent != self.rendered_manifest_path.parent
        ):
            raise ValueError("preflight artifact publication identity is invalid")


class PreflightArtifactStore:
    """Publish exact image and manifest artifacts once without replacement."""

    def __init__(self, state_root: Path | str, *, service_uid: int | None = None) -> None:
        self.state_root = Path(state_root)
        self.root = self.state_root / "preflight-artifacts"
        self.service_uid = os.geteuid() if service_uid is None else service_uid
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or self.service_uid < 0
        ):
            raise PreflightArtifactStoreError("preflight artifact store authority is invalid")

    def publish(
        self,
        *,
        candidate_sha: str,
        candidate_tree: str,
        mutation_epoch: int,
        images: ImageArtifactSet,
        manifests: ManifestArtifact,
        migration_plan_sha256: str,
        migration_target_revision: str,
        browser_report_schema_sha256: str,
    ) -> PreflightArtifactPublication:
        descriptor = _descriptor(
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            mutation_epoch=mutation_epoch,
            images=images,
            manifests=manifests,
            migration_plan_sha256=migration_plan_sha256,
            migration_target_revision=migration_target_revision,
            browser_report_schema_sha256=browser_report_schema_sha256,
        )
        bundle_digest = _hash_json(descriptor)
        directory = self.root / bundle_digest
        descriptor_path = directory / "artifact.json"
        rendered_path = directory / "rendered.yaml"
        publication = PreflightArtifactPublication(
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            mutation_epoch=mutation_epoch,
            bundle_digest=bundle_digest,
            descriptor_path=descriptor_path,
            rendered_manifest_path=rendered_path,
            image_artifact_sha256=images.artifact_digest,
            manifest_artifact_sha256=manifests.artifact_digest,
            rendered_manifest_sha256=manifests.rendered_sha256,
            migration_plan_sha256=migration_plan_sha256,
            migration_target_revision=migration_target_revision,
            browser_report_schema_sha256=browser_report_schema_sha256,
        )
        self._ensure_roots()
        _ensure_private_directory(directory, service_uid=self.service_uid)
        _publish_file(
            directory,
            "rendered.yaml",
            manifests.rendered_yaml.encode(),
            service_uid=self.service_uid,
            max_bytes=_MAX_MANIFEST_BYTES,
        )
        _publish_file(
            directory,
            "artifact.json",
            _json_bytes({**descriptor, "bundle_digest": bundle_digest}),
            service_uid=self.service_uid,
            max_bytes=_MAX_DESCRIPTOR_BYTES,
        )
        observed = self.read(bundle_digest)
        if observed != publication:
            raise PreflightArtifactStoreError("preflight artifact publication drifted")
        return publication

    def read(self, bundle_digest: str) -> PreflightArtifactPublication:
        if _SHA256_RE.fullmatch(bundle_digest) is None:
            raise PreflightArtifactStoreError("preflight artifact digest is invalid")
        _require_private_directory(self.state_root, service_uid=self.service_uid)
        _require_private_directory(self.root, service_uid=self.service_uid)
        directory = self.root / bundle_digest
        _require_private_directory(directory, service_uid=self.service_uid)
        descriptor_path = directory / "artifact.json"
        rendered_path = directory / "rendered.yaml"
        try:
            descriptor_read = read_trusted_file(
                descriptor_path,
                service_uid=self.service_uid,
                private=True,
                max_bytes=_MAX_DESCRIPTOR_BYTES,
                require_nonempty=True,
            )
            rendered_read = read_trusted_file(
                rendered_path,
                service_uid=self.service_uid,
                private=True,
                max_bytes=_MAX_MANIFEST_BYTES,
                require_nonempty=True,
            )
            raw = json.loads(
                descriptor_read.payload,
                object_pairs_hook=_reject_duplicate_keys,
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise PreflightArtifactStoreError("preflight artifact files are invalid") from exc
        if not isinstance(raw, dict):
            raise PreflightArtifactStoreError("preflight artifact descriptor is invalid")
        descriptor = _parse_descriptor(raw, bundle_digest=bundle_digest)
        if (
            hashlib.sha256(rendered_read.payload).hexdigest()
            != descriptor["rendered_manifest_sha256"]
            or _hash_json({key: value for key, value in raw.items() if key != "bundle_digest"})
            != bundle_digest
        ):
            raise PreflightArtifactStoreError("preflight artifact content drifted")
        return PreflightArtifactPublication(
            candidate_sha=descriptor["candidate_sha"],
            candidate_tree=descriptor["candidate_tree"],
            mutation_epoch=descriptor["mutation_epoch"],
            bundle_digest=bundle_digest,
            descriptor_path=descriptor_path,
            rendered_manifest_path=rendered_path,
            image_artifact_sha256=descriptor["image_artifact_sha256"],
            manifest_artifact_sha256=descriptor["manifest_artifact_sha256"],
            rendered_manifest_sha256=descriptor["rendered_manifest_sha256"],
            migration_plan_sha256=descriptor["migration_plan_sha256"],
            migration_target_revision=descriptor["migration_target_revision"],
            browser_report_schema_sha256=descriptor["browser_report_schema_sha256"],
        )

    def _ensure_roots(self) -> None:
        _ensure_private_directory(self.state_root, service_uid=self.service_uid, parents=True)
        _ensure_private_directory(self.root, service_uid=self.service_uid)


def _descriptor(
    *,
    candidate_sha: str,
    candidate_tree: str,
    mutation_epoch: int,
    images: ImageArtifactSet,
    manifests: ManifestArtifact,
    migration_plan_sha256: str,
    migration_target_revision: str,
    browser_report_schema_sha256: str,
) -> dict[str, object]:
    if (
        _SHA_RE.fullmatch(candidate_sha) is None
        or len(candidate_tree) != 40
        or any(character not in "0123456789abcdef" for character in candidate_tree)
        or mutation_epoch < 0
        or _SHA256_RE.fullmatch(migration_plan_sha256) is None
        or _REVISION_RE.fullmatch(migration_target_revision) is None
        or _SHA256_RE.fullmatch(browser_report_schema_sha256) is None
        or manifests.image_identities
        != {
            name: digest
            for name, digest in images.image_digests.items()
            if name in manifests.image_identities
        }
    ):
        raise PreflightArtifactStoreError("preflight artifact input binding is invalid")
    return {
        "browser_report_schema_sha256": browser_report_schema_sha256,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "image_artifact_sha256": images.artifact_digest,
        "image_digests": dict(images.image_digests),
        "manifest_artifact_sha256": manifests.artifact_digest,
        "migration_plan_sha256": migration_plan_sha256,
        "migration_target_revision": migration_target_revision,
        "mutation_epoch": mutation_epoch,
        "rendered_manifest_sha256": manifests.rendered_sha256,
        "resource_set_digest": manifests.resource_set_digest,
        "schema_version": 1,
    }


def _parse_descriptor(value: Mapping[str, object], *, bundle_digest: str) -> _ParsedDescriptor:
    expected = {
        "browser_report_schema_sha256",
        "bundle_digest",
        "candidate_sha",
        "candidate_tree",
        "image_artifact_sha256",
        "image_digests",
        "manifest_artifact_sha256",
        "migration_plan_sha256",
        "migration_target_revision",
        "mutation_epoch",
        "rendered_manifest_sha256",
        "resource_set_digest",
        "schema_version",
    }
    image_digests = value.get("image_digests")
    mutation_epoch = value.get("mutation_epoch")
    if (
        set(value) != expected
        or value.get("bundle_digest") != bundle_digest
        or value.get("schema_version") != 1
        or type(value.get("schema_version")) is not int
        or type(mutation_epoch) is not int
        or not isinstance(image_digests, Mapping)
        or not image_digests
        or any(
            not isinstance(key, str) or not isinstance(item, str) or not item.startswith("sha256:")
            for key, item in image_digests.items()
        )
    ):
        raise PreflightArtifactStoreError("preflight artifact descriptor is invalid")
    string_fields = (
        "browser_report_schema_sha256",
        "candidate_sha",
        "candidate_tree",
        "image_artifact_sha256",
        "manifest_artifact_sha256",
        "migration_plan_sha256",
        "migration_target_revision",
        "rendered_manifest_sha256",
        "resource_set_digest",
    )
    if any(not isinstance(value.get(field), str) for field in string_fields):
        raise PreflightArtifactStoreError("preflight artifact descriptor is invalid")
    return _ParsedDescriptor(
        browser_report_schema_sha256=str(value["browser_report_schema_sha256"]),
        candidate_sha=str(value["candidate_sha"]),
        candidate_tree=str(value["candidate_tree"]),
        image_artifact_sha256=str(value["image_artifact_sha256"]),
        manifest_artifact_sha256=str(value["manifest_artifact_sha256"]),
        migration_plan_sha256=str(value["migration_plan_sha256"]),
        migration_target_revision=str(value["migration_target_revision"]),
        mutation_epoch=mutation_epoch,
        rendered_manifest_sha256=str(value["rendered_manifest_sha256"]),
        resource_set_digest=str(value["resource_set_digest"]),
    )


def _json_bytes(value: Mapping[str, object]) -> bytes:
    return (json.dumps(dict(value), sort_keys=True, separators=(",", ":")) + "\n").encode()


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(_json_bytes(value).rstrip(b"\n")).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _ensure_private_directory(path: Path, *, service_uid: int, parents: bool = False) -> None:
    created = False
    try:
        path.mkdir(mode=_PRIVATE_DIRECTORY_MODE, parents=parents)
        created = True
    except FileExistsError:
        pass
    except OSError as exc:
        raise PreflightArtifactStoreError("could not create preflight artifact directory") from exc
    if created:
        path.chmod(_PRIVATE_DIRECTORY_MODE)
        _fsync_directory(path.parent)
    _require_private_directory(path, service_uid=service_uid)


def _require_private_directory(path: Path, *, service_uid: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise PreflightArtifactStoreError("preflight artifact directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise PreflightArtifactStoreError("preflight artifact directory authority is unsafe")


def _publish_file(
    directory: Path,
    name: str,
    payload: bytes,
    *,
    service_uid: int,
    max_bytes: int,
) -> None:
    if not payload or len(payload) > max_bytes or "/" in name or name in {".", ".."}:
        raise PreflightArtifactStoreError("preflight artifact payload is invalid")
    directory_fd = os.open(
        directory,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    temp = f".{name}.{uuid4().hex}.tmp"
    temp_exists = False
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        fd = os.open(temp, flags, _PRIVATE_FILE_MODE, dir_fd=directory_fd)
        temp_exists = True
        try:
            metadata = os.fstat(fd)
            if metadata.st_uid != service_uid or stat.S_IMODE(metadata.st_mode) != 0o600:
                raise PreflightArtifactStoreError(
                    "preflight artifact temporary authority is unsafe"
                )
            offset = 0
            while offset < len(payload):
                offset += os.write(fd, payload[offset:])
            os.fsync(fd)
        finally:
            os.close(fd)
        try:
            os.link(temp, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        except FileExistsError:
            existing = read_trusted_file(
                directory / name,
                service_uid=service_uid,
                private=True,
                max_bytes=max_bytes,
                require_nonempty=True,
            )
            if existing.payload != payload:
                raise PreflightArtifactStoreError(
                    "preflight artifact immutable collision"
                ) from None
        os.unlink(temp, dir_fd=directory_fd)
        temp_exists = False
        os.fsync(directory_fd)
    finally:
        if temp_exists:
            try:
                os.unlink(temp, dir_fd=directory_fd)
            except OSError:
                pass
        os.close(directory_fd)


def _fsync_directory(path: Path) -> None:
    fd = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


__all__ = [
    "PreflightArtifactPublication",
    "PreflightArtifactStore",
    "PreflightArtifactStoreError",
]
