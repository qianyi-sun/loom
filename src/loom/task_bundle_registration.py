"""Verified authored configuration and catalog identity for immutable bundles.

This preparation boundary performs no upload, database write or source admission.
Callers must publish the captured bytes and register their lifecycle separately.
"""

from __future__ import annotations

import hashlib
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import rfc8785

from loom.driver.task_image import dockerfile_text_uses_runtime_arm64_fallback_base
from loom.models.task import TaskConfig, normalize_steps
from loom.task_image_bundle_manifest import (
    TaskImageBundleContentManifestV1,
    capture_task_image_bundle_manifest,
    read_verified_task_image_bundle_file,
)
from loom.terminal_bench_normalize import normalize_terminal_bench_task_toml

MAX_REGISTRATION_TEXT_BYTES = 1024 * 1024


def _task_id(value: str) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 512
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError("registered bundle task identity is invalid")
    return value


@dataclass(frozen=True, slots=True)
class RegisteredTaskBundle:
    """Owned immutable preparation; mutable model/provenance views are copies."""

    manifest: TaskImageBundleContentManifestV1
    catalog_task_id: str
    bundle_task_id: str
    task_toml_sha256: str
    _task_config_bytes: bytes

    @property
    def task_config(self) -> TaskConfig:
        return TaskConfig.model_validate_json(self._task_config_bytes)

    @property
    def source_provenance(self) -> dict[str, Any]:
        return {
            "bundle_content_manifest_sha256": self.manifest.digest,
            "bundle_file_metadata_sha256": "sha256:" + self.manifest.bundle_file_metadata_sha256,
            "bundle_task_identity": {
                "schema_version": "loom.task-bundle-identity.v1",
                "catalog_task_id": self.catalog_task_id,
                "bundle_task_id": self.bundle_task_id,
                "bundle_task_toml_sha256": self.task_toml_sha256,
                "registered_config_sha256": hashlib.sha256(self._task_config_bytes).hexdigest(),
            },
        }


def _promote_registered_runtime_architecture(
    normalized: dict[str, Any],
    task_dir: Path,
    manifest: TaskImageBundleContentManifestV1,
) -> None:
    environment = normalized.get("environment")
    if not isinstance(environment, dict) or "cpu_arch" in environment:
        return
    relative = environment.get("dockerfile")
    if not isinstance(relative, str):
        return
    path = PurePosixPath(relative).as_posix()
    entry = next((item for item in manifest.files if item.path == path), None)
    if entry is None:
        return
    if entry.size_bytes > MAX_REGISTRATION_TEXT_BYTES:
        raise ValueError("registered Dockerfile exceeds text size limit")
    content = read_verified_task_image_bundle_file(task_dir, entry).decode("utf-8")
    if dockerfile_text_uses_runtime_arm64_fallback_base(content):
        environment["cpu_arch"] = "any"


def prepare_task_bundle_registration(
    task_dir: Path,
    *,
    task_id: str,
    promote_runtime_architecture: bool = False,
) -> RegisteredTaskBundle:
    """Bind normalized catalog config to exactly the captured authored file.

    The catalog ID is the execution identity; it does not replace the authored
    task.toml bytes. Their relationship is explicit in frozen provenance. A
    later upload must consume this manifest with the existing verified uploader.
    """
    catalog_id = _task_id(task_id)
    manifest = capture_task_image_bundle_manifest(task_dir)
    task_file = next((item for item in manifest.files if item.path == "task.toml"), None)
    if task_file is None:
        raise ValueError("registered bundle requires captured task.toml")
    if task_file.size_bytes > MAX_REGISTRATION_TEXT_BYTES:
        raise ValueError("registered task.toml exceeds text size limit")
    payload = read_verified_task_image_bundle_file(task_dir, task_file)
    normalized = normalize_terminal_bench_task_toml(tomllib.loads(payload.decode("utf-8")))
    if promote_runtime_architecture:
        _promote_registered_runtime_architecture(normalized, task_dir, manifest)
    authored = normalize_steps(TaskConfig.model_validate(normalized))
    bundle_id = _task_id(authored.task.id)
    registered = authored.model_dump(mode="json")
    registered["task"]["id"] = catalog_id
    config = TaskConfig.model_validate(registered)
    return RegisteredTaskBundle(
        manifest=manifest,
        catalog_task_id=catalog_id,
        bundle_task_id=bundle_id,
        task_toml_sha256=task_file.sha256,
        _task_config_bytes=rfc8785.dumps(config.model_dump(mode="json")),
    )
