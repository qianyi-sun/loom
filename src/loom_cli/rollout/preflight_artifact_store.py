"""Immutable private publication for build-once preflight artifacts."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import stat
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict
from uuid import uuid4

from loom_cli.cluster_config import validate_container_registry_prefix
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.image_readiness import (
    DockerRunner,
    ImageArtifactSet,
    verify_image_contract,
    verify_published_images,
)
from loom_cli.rollout.manifest_readiness import (
    ManifestArtifact,
    inspect_rendered_manifests,
)
from loom_cli.rollout.migration_manifest_readiness import (
    MigrationManifestArtifact,
    inspect_migration_manifest_artifact,
)
from loom_cli.rollout.production_defaults_readiness import ProductionDefaultsArtifact

_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_REVISION_RE = re.compile(r"^[0-9]{4}(?:_[a-z0-9_]+)?$")
_DNS_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
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
    container_registry: str
    image_artifact_sha256: str
    image_digests: dict[str, str]
    registry_digests: dict[str, str]
    manifest_artifact_sha256: str
    manifest_image_names: tuple[str, ...]
    migration_image_id: str
    migration_job_name: str
    migration_manifest_artifact_sha256: str
    migration_manifest_sha256: str
    migration_plan_sha256: str
    migration_target_revision: str
    mutation_epoch: int
    production_defaults_sha256: str
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
    migration_manifest_path: Path
    production_defaults_path: Path
    image_artifact_sha256: str
    manifest_artifact_sha256: str
    rendered_manifest_sha256: str
    migration_manifest_artifact_sha256: str
    migration_manifest_sha256: str
    migration_job_name: str
    migration_image_id: str
    migration_plan_sha256: str
    migration_target_revision: str
    browser_report_schema_sha256: str
    production_defaults_sha256: str
    container_registry: str = ""

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
                    self.migration_manifest_artifact_sha256,
                    self.migration_manifest_sha256,
                    self.migration_plan_sha256,
                    self.browser_report_schema_sha256,
                    self.production_defaults_sha256,
                )
            )
            or not self.descriptor_path.is_absolute()
            or _REVISION_RE.fullmatch(self.migration_target_revision) is None
            or not self.rendered_manifest_path.is_absolute()
            or not self.migration_manifest_path.is_absolute()
            or not self.production_defaults_path.is_absolute()
            or ".." in self.descriptor_path.parts
            or ".." in self.rendered_manifest_path.parts
            or ".." in self.migration_manifest_path.parts
            or ".." in self.production_defaults_path.parts
            or self.descriptor_path.parent != self.rendered_manifest_path.parent
            or self.descriptor_path.parent != self.migration_manifest_path.parent
            or self.descriptor_path.parent != self.production_defaults_path.parent
            or _DNS_RE.fullmatch(self.migration_job_name) is None
            or _IMAGE_ID_RE.fullmatch(self.migration_image_id) is None
        ):
            raise ValueError("preflight artifact publication identity is invalid")
        if self.container_registry:
            validate_container_registry_prefix(
                self.container_registry,
                name="container_registry",
            )


@dataclass(frozen=True, slots=True)
class LoadedPreflightArtifacts:
    publication: PreflightArtifactPublication
    images: ImageArtifactSet
    manifests: ManifestArtifact
    migration: MigrationManifestArtifact
    production_defaults: ProductionDefaultsArtifact

    def __post_init__(self) -> None:
        if (
            self.publication.image_artifact_sha256 != self.images.artifact_digest
            or self.publication.manifest_artifact_sha256 != self.manifests.artifact_digest
            or self.publication.rendered_manifest_sha256 != self.manifests.rendered_sha256
            or self.publication.migration_manifest_artifact_sha256 != self.migration.artifact_digest
            or self.publication.migration_manifest_sha256 != self.migration.rendered_sha256
            or self.publication.container_registry != self.migration.container_registry
            or self.publication.production_defaults_sha256
            != self.production_defaults.artifact_digest
        ):
            raise ValueError("loaded preflight artifact identity drifted")


class PreflightArtifactStore:
    """Publish exact image and manifest artifacts once without replacement."""

    def __init__(self, state_root: Path | str, *, service_uid: int | None = None) -> None:
        self.state_root = Path(state_root)
        self.root = self.state_root / "preflight-artifacts"
        self.lifecycle_lock_path = self.state_root / "preflight-artifacts.lock"
        self.service_uid = os.geteuid() if service_uid is None else service_uid
        self._held_lifecycle_lock: ContextVar[str | None] = ContextVar(
            f"preflight_artifact_lifecycle_lock_{id(self)}",
            default=None,
        )
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
        migration: MigrationManifestArtifact,
        production_defaults: ProductionDefaultsArtifact,
        migration_plan_sha256: str,
        migration_target_revision: str,
        browser_report_schema_sha256: str,
    ) -> PreflightArtifactPublication:
        _ensure_private_directory(self.state_root, service_uid=self.service_uid, parents=True)
        with self.exclusive_lifecycle_lock():
            return self._publish_locked(
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
                mutation_epoch=mutation_epoch,
                images=images,
                manifests=manifests,
                migration=migration,
                production_defaults=production_defaults,
                migration_plan_sha256=migration_plan_sha256,
                migration_target_revision=migration_target_revision,
                browser_report_schema_sha256=browser_report_schema_sha256,
            )

    def _publish_locked(
        self,
        *,
        candidate_sha: str,
        candidate_tree: str,
        mutation_epoch: int,
        images: ImageArtifactSet,
        manifests: ManifestArtifact,
        migration: MigrationManifestArtifact,
        production_defaults: ProductionDefaultsArtifact,
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
            migration=migration,
            production_defaults=production_defaults,
            migration_plan_sha256=migration_plan_sha256,
            migration_target_revision=migration_target_revision,
            browser_report_schema_sha256=browser_report_schema_sha256,
        )
        bundle_digest = _hash_json(descriptor)
        directory = self.root / bundle_digest
        descriptor_path = directory / "artifact.json"
        rendered_path = directory / "rendered.yaml"
        migration_path = directory / "migration.yaml"
        production_defaults_path = directory / "production-defaults.json"
        publication = PreflightArtifactPublication(
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            mutation_epoch=mutation_epoch,
            bundle_digest=bundle_digest,
            descriptor_path=descriptor_path,
            rendered_manifest_path=rendered_path,
            migration_manifest_path=migration_path,
            production_defaults_path=production_defaults_path,
            image_artifact_sha256=images.artifact_digest,
            manifest_artifact_sha256=manifests.artifact_digest,
            rendered_manifest_sha256=manifests.rendered_sha256,
            migration_manifest_artifact_sha256=migration.artifact_digest,
            migration_manifest_sha256=migration.rendered_sha256,
            migration_job_name=migration.job_name,
            migration_image_id=migration.image_id,
            migration_plan_sha256=migration_plan_sha256,
            migration_target_revision=migration_target_revision,
            browser_report_schema_sha256=browser_report_schema_sha256,
            production_defaults_sha256=production_defaults.artifact_digest,
            container_registry=migration.container_registry,
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
            "migration.yaml",
            migration.rendered_yaml.encode(),
            service_uid=self.service_uid,
            max_bytes=_MAX_MANIFEST_BYTES,
        )
        _publish_file(
            directory,
            "production-defaults.json",
            production_defaults.to_bytes(),
            service_uid=self.service_uid,
            max_bytes=_MAX_DESCRIPTOR_BYTES,
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
        with self.shared_lifecycle_lock():
            return self._read_locked(bundle_digest)

    def _read_locked(self, bundle_digest: str) -> PreflightArtifactPublication:
        if _SHA256_RE.fullmatch(bundle_digest) is None:
            raise PreflightArtifactStoreError("preflight artifact digest is invalid")
        _require_private_directory(self.state_root, service_uid=self.service_uid)
        _require_private_directory(self.root, service_uid=self.service_uid)
        directory = self.root / bundle_digest
        _require_private_directory(directory, service_uid=self.service_uid)
        descriptor_path = directory / "artifact.json"
        rendered_path = directory / "rendered.yaml"
        migration_path = directory / "migration.yaml"
        production_defaults_path = directory / "production-defaults.json"
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
            migration_read = read_trusted_file(
                migration_path,
                service_uid=self.service_uid,
                private=True,
                max_bytes=_MAX_MANIFEST_BYTES,
                require_nonempty=True,
            )
            production_defaults_read = read_trusted_file(
                production_defaults_path,
                service_uid=self.service_uid,
                private=True,
                max_bytes=_MAX_DESCRIPTOR_BYTES,
                require_nonempty=True,
            )
            raw = json.loads(
                descriptor_read.payload,
                object_pairs_hook=_reject_duplicate_keys,
            )
            production_defaults = ProductionDefaultsArtifact.from_bytes(
                production_defaults_read.payload
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise PreflightArtifactStoreError("preflight artifact files are invalid") from exc
        if not isinstance(raw, dict):
            raise PreflightArtifactStoreError("preflight artifact descriptor is invalid")
        descriptor = _parse_descriptor(raw, bundle_digest=bundle_digest)
        if (
            hashlib.sha256(rendered_read.payload).hexdigest()
            != descriptor["rendered_manifest_sha256"]
            or hashlib.sha256(migration_read.payload).hexdigest()
            != descriptor["migration_manifest_sha256"]
            or production_defaults.artifact_digest != descriptor["production_defaults_sha256"]
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
            migration_manifest_path=migration_path,
            production_defaults_path=production_defaults_path,
            image_artifact_sha256=descriptor["image_artifact_sha256"],
            manifest_artifact_sha256=descriptor["manifest_artifact_sha256"],
            rendered_manifest_sha256=descriptor["rendered_manifest_sha256"],
            migration_manifest_artifact_sha256=descriptor["migration_manifest_artifact_sha256"],
            migration_manifest_sha256=descriptor["migration_manifest_sha256"],
            migration_job_name=descriptor["migration_job_name"],
            migration_image_id=descriptor["migration_image_id"],
            migration_plan_sha256=descriptor["migration_plan_sha256"],
            migration_target_revision=descriptor["migration_target_revision"],
            browser_report_schema_sha256=descriptor["browser_report_schema_sha256"],
            production_defaults_sha256=descriptor["production_defaults_sha256"],
            container_registry=descriptor["container_registry"],
        )

    def load(
        self,
        *,
        bundle_digest: str,
        candidate_sha: str,
        candidate_tree: str,
        mutation_epoch: int,
        image_tag: str,
        namespace: str,
        image_run: DockerRunner,
        container_registry_push: str = "",
    ) -> LoadedPreflightArtifacts:
        with self.shared_lifecycle_lock():
            return self._load_locked(
                bundle_digest=bundle_digest,
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
                mutation_epoch=mutation_epoch,
                image_tag=image_tag,
                namespace=namespace,
                image_run=image_run,
                container_registry_push=container_registry_push,
            )

    def _load_locked(
        self,
        *,
        bundle_digest: str,
        candidate_sha: str,
        candidate_tree: str,
        mutation_epoch: int,
        image_tag: str,
        namespace: str,
        image_run: DockerRunner,
        container_registry_push: str = "",
    ) -> LoadedPreflightArtifacts:
        """Load one exact publication without rebuilding or rerendering outputs."""
        if (
            _SHA_RE.fullmatch(candidate_sha) is None
            or len(candidate_tree) != 40
            or any(character not in "0123456789abcdef" for character in candidate_tree)
            or mutation_epoch < 0
        ):
            raise PreflightArtifactStoreError("preflight artifact lookup identity is invalid")
        publication = self.read(bundle_digest)
        if (
            publication.candidate_sha != candidate_sha
            or publication.candidate_tree != candidate_tree
            or publication.mutation_epoch != mutation_epoch
        ):
            raise PreflightArtifactStoreError("preflight artifact lookup identity drifted")
        descriptor_read = read_trusted_file(
            publication.descriptor_path,
            service_uid=self.service_uid,
            private=True,
            max_bytes=_MAX_DESCRIPTOR_BYTES,
            require_nonempty=True,
        )
        rendered_read = read_trusted_file(
            publication.rendered_manifest_path,
            service_uid=self.service_uid,
            private=True,
            max_bytes=_MAX_MANIFEST_BYTES,
            require_nonempty=True,
        )
        migration_read = read_trusted_file(
            publication.migration_manifest_path,
            service_uid=self.service_uid,
            private=True,
            max_bytes=_MAX_MANIFEST_BYTES,
            require_nonempty=True,
        )
        production_defaults_read = read_trusted_file(
            publication.production_defaults_path,
            service_uid=self.service_uid,
            private=True,
            max_bytes=_MAX_DESCRIPTOR_BYTES,
            require_nonempty=True,
        )
        try:
            raw = json.loads(descriptor_read.payload, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise PreflightArtifactStoreError("preflight artifact descriptor is invalid") from exc
        if not isinstance(raw, dict):
            raise PreflightArtifactStoreError("preflight artifact descriptor is invalid")
        descriptor = _parse_descriptor(raw, bundle_digest=publication.bundle_digest)
        try:
            images = verify_image_contract(
                image_run,
                image_tag=image_tag,
                resolved_sha=candidate_sha,
                expected_digests=descriptor["image_digests"],
            )
            if descriptor["registry_digests"]:
                images = verify_published_images(
                    image_run,
                    artifact=images,
                    image_tag=image_tag,
                    container_registry_push=container_registry_push,
                    expected_registry_digests=descriptor["registry_digests"],
                )
            rendered = rendered_read.payload.decode("utf-8")
            manifests = inspect_rendered_manifests(
                rendered,
                image_tag=image_tag,
                namespace=namespace,
                image_digests=images.image_digests,
                expected_image_names=descriptor["manifest_image_names"],
                container_registry=descriptor["container_registry"],
                registry_digests=images.registry_digests,
            )
            migration = inspect_migration_manifest_artifact(
                migration_read.payload.decode("utf-8"),
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
                image_tag=image_tag,
                image_id=images.image_digests["loom-control-plane"],
                namespace=namespace,
                migration_plan_sha256=publication.migration_plan_sha256,
                migration_target_revision=publication.migration_target_revision,
                container_registry=descriptor["container_registry"],
                registry_digest=(
                    images.registry_digests["loom-control-plane"]
                    if descriptor["container_registry"]
                    else ""
                ),
            )
            production_defaults = ProductionDefaultsArtifact.from_bytes(
                production_defaults_read.payload
            )
        except (UnicodeDecodeError, ValueError) as exc:
            raise PreflightArtifactStoreError("preflight artifact reconstruction failed") from exc
        if (
            descriptor["image_artifact_sha256"] != images.artifact_digest
            or descriptor["manifest_artifact_sha256"] != manifests.artifact_digest
            or descriptor["rendered_manifest_sha256"] != manifests.rendered_sha256
            or descriptor["resource_set_digest"] != manifests.resource_set_digest
            or descriptor["migration_manifest_artifact_sha256"] != migration.artifact_digest
            or descriptor["migration_manifest_sha256"] != migration.rendered_sha256
            or descriptor["migration_job_name"] != migration.job_name
            or descriptor["migration_image_id"] != migration.image_id
            or production_defaults.candidate_sha != candidate_sha
            or production_defaults.candidate_tree != candidate_tree
            or descriptor["production_defaults_sha256"] != production_defaults.artifact_digest
        ):
            raise PreflightArtifactStoreError("preflight artifact reconstruction drifted")
        return LoadedPreflightArtifacts(
            publication=publication,
            images=images,
            manifests=manifests,
            migration=migration,
            production_defaults=production_defaults,
        )

    def _ensure_roots(self) -> None:
        _ensure_private_directory(self.state_root, service_uid=self.service_uid, parents=True)
        _ensure_private_directory(self.root, service_uid=self.service_uid)

    def shared_lifecycle_lock(self) -> AbstractContextManager[None]:
        """Hold one shared lock across a complete multi-file artifact read."""
        return self._lifecycle_lock("shared")

    def exclusive_lifecycle_lock(self) -> AbstractContextManager[None]:
        """Exclude readers and publishers during publication or retirement."""
        return self._lifecycle_lock("exclusive")

    @contextmanager
    def _lifecycle_lock(self, requested: str) -> Iterator[None]:
        held = self._held_lifecycle_lock.get()
        if held == "exclusive" or held == requested == "shared":
            yield
            return
        if held == "shared":
            raise PreflightArtifactStoreError(
                "preflight artifact lifecycle lock cannot be promoted"
            )
        _require_private_directory(self.state_root, service_uid=self.service_uid)
        created = False
        create_flags = (
            os.O_RDWR
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        existing_flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            try:
                descriptor = os.open(
                    self.lifecycle_lock_path,
                    create_flags,
                    _PRIVATE_FILE_MODE,
                )
                created = True
            except FileExistsError:
                descriptor = os.open(self.lifecycle_lock_path, existing_flags)
        except OSError as exc:
            raise PreflightArtifactStoreError(
                "preflight artifact lifecycle lock is unsafe"
            ) from exc
        locked = False
        token = None
        try:
            if created:
                os.fchmod(descriptor, _PRIVATE_FILE_MODE)
                os.fsync(descriptor)
                _fsync_directory(self.state_root)
            before = os.fstat(descriptor)
            _require_safe_lifecycle_lock(before, service_uid=self.service_uid)
            fcntl.flock(
                descriptor,
                fcntl.LOCK_SH if requested == "shared" else fcntl.LOCK_EX,
            )
            locked = True
            after = os.fstat(descriptor)
            _require_safe_lifecycle_lock(after, service_uid=self.service_uid)
            if _metadata_identity(after) != _metadata_identity(before):
                raise PreflightArtifactStoreError(
                    "preflight artifact lifecycle lock changed during acquisition"
                )
            token = self._held_lifecycle_lock.set(requested)
            yield
        except PreflightArtifactStoreError:
            raise
        except OSError as exc:
            raise PreflightArtifactStoreError("preflight artifact lifecycle lock failed") from exc
        finally:
            if token is not None:
                self._held_lifecycle_lock.reset(token)
            if locked:
                with contextlib.suppress(OSError):
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)


def _descriptor(
    *,
    candidate_sha: str,
    candidate_tree: str,
    mutation_epoch: int,
    images: ImageArtifactSet,
    manifests: ManifestArtifact,
    migration: MigrationManifestArtifact,
    production_defaults: ProductionDefaultsArtifact,
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
        or migration.candidate_sha != candidate_sha
        or migration.candidate_tree != candidate_tree
        or migration.image_id != images.image_digests["loom-control-plane"]
        or bool(migration.container_registry) != bool(images.registry_digests)
        or (
            bool(images.registry_digests)
            and migration.registry_digest != images.registry_digests["loom-control-plane"]
        )
        or migration.migration_plan_sha256 != migration_plan_sha256
        or migration.migration_target_revision != migration_target_revision
        or production_defaults.candidate_sha != candidate_sha
        or production_defaults.candidate_tree != candidate_tree
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
        "container_registry": migration.container_registry,
        "image_artifact_sha256": images.artifact_digest,
        "image_digests": dict(images.image_digests),
        "registry_digests": dict(images.registry_digests),
        "manifest_artifact_sha256": manifests.artifact_digest,
        "manifest_image_names": sorted(manifests.image_identities),
        "migration_image_id": migration.image_id,
        "migration_job_name": migration.job_name,
        "migration_manifest_artifact_sha256": migration.artifact_digest,
        "migration_manifest_sha256": migration.rendered_sha256,
        "migration_plan_sha256": migration_plan_sha256,
        "migration_target_revision": migration_target_revision,
        "mutation_epoch": mutation_epoch,
        "production_defaults_sha256": production_defaults.artifact_digest,
        "rendered_manifest_sha256": manifests.rendered_sha256,
        "resource_set_digest": manifests.resource_set_digest,
        "schema_version": 6,
    }


def _parse_descriptor(value: Mapping[str, object], *, bundle_digest: str) -> _ParsedDescriptor:
    historical_expected = {
        "browser_report_schema_sha256",
        "bundle_digest",
        "candidate_sha",
        "candidate_tree",
        "image_artifact_sha256",
        "image_digests",
        "manifest_artifact_sha256",
        "manifest_image_names",
        "migration_image_id",
        "migration_job_name",
        "migration_manifest_artifact_sha256",
        "migration_manifest_sha256",
        "migration_plan_sha256",
        "migration_target_revision",
        "mutation_epoch",
        "production_defaults_sha256",
        "rendered_manifest_sha256",
        "resource_set_digest",
        "schema_version",
    }
    current_expected = historical_expected | {"container_registry", "registry_digests"}
    schema_version = value.get("schema_version")
    image_digests = value.get("image_digests")
    registry_digests = value.get("registry_digests", {})
    container_registry = value.get("container_registry", "")
    manifest_image_names = value.get("manifest_image_names")
    mutation_epoch = value.get("mutation_epoch")
    if (
        (
            (schema_version == 4 and set(value) != historical_expected)
            or (schema_version == 6 and set(value) != current_expected)
            or schema_version not in {4, 6}
        )
        or value.get("bundle_digest") != bundle_digest
        or type(schema_version) is not int
        or type(mutation_epoch) is not int
        or not isinstance(image_digests, Mapping)
        or not image_digests
        or not isinstance(registry_digests, Mapping)
        or not isinstance(container_registry, str)
        or not isinstance(manifest_image_names, list)
        or not manifest_image_names
        or manifest_image_names != sorted(set(manifest_image_names))
        or any(
            not isinstance(name, str) or name not in image_digests for name in manifest_image_names
        )
        or any(
            not isinstance(key, str) or not isinstance(item, str) or not item.startswith("sha256:")
            for key, item in image_digests.items()
        )
        or any(
            not isinstance(key, str)
            or key not in image_digests
            or not isinstance(item, str)
            or _IMAGE_ID_RE.fullmatch(item) is None
            for key, item in registry_digests.items()
        )
        or (bool(container_registry) != bool(registry_digests))
        or (bool(registry_digests) and set(registry_digests) != set(image_digests))
    ):
        raise PreflightArtifactStoreError("preflight artifact descriptor is invalid")
    string_fields = (
        "browser_report_schema_sha256",
        "candidate_sha",
        "candidate_tree",
        "image_artifact_sha256",
        "manifest_artifact_sha256",
        "migration_image_id",
        "migration_job_name",
        "migration_manifest_artifact_sha256",
        "migration_manifest_sha256",
        "migration_plan_sha256",
        "migration_target_revision",
        "production_defaults_sha256",
        "rendered_manifest_sha256",
        "resource_set_digest",
    )
    if any(not isinstance(value.get(field), str) for field in string_fields):
        raise PreflightArtifactStoreError("preflight artifact descriptor is invalid")
    return _ParsedDescriptor(
        browser_report_schema_sha256=str(value["browser_report_schema_sha256"]),
        candidate_sha=str(value["candidate_sha"]),
        candidate_tree=str(value["candidate_tree"]),
        container_registry=container_registry,
        image_artifact_sha256=str(value["image_artifact_sha256"]),
        image_digests={str(key): str(item) for key, item in image_digests.items()},
        registry_digests={str(key): str(item) for key, item in registry_digests.items()},
        manifest_artifact_sha256=str(value["manifest_artifact_sha256"]),
        manifest_image_names=tuple(str(name) for name in manifest_image_names),
        migration_image_id=str(value["migration_image_id"]),
        migration_job_name=str(value["migration_job_name"]),
        migration_manifest_artifact_sha256=str(value["migration_manifest_artifact_sha256"]),
        migration_manifest_sha256=str(value["migration_manifest_sha256"]),
        migration_plan_sha256=str(value["migration_plan_sha256"]),
        migration_target_revision=str(value["migration_target_revision"]),
        mutation_epoch=mutation_epoch,
        production_defaults_sha256=str(value["production_defaults_sha256"]),
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


def _require_safe_lifecycle_lock(metadata: os.stat_result, *, service_uid: int) -> None:
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
        or metadata.st_nlink != 1
    ):
        raise PreflightArtifactStoreError("preflight artifact lifecycle lock is unsafe")


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


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
    "LoadedPreflightArtifacts",
    "PreflightArtifactPublication",
    "PreflightArtifactStore",
    "PreflightArtifactStoreError",
]
