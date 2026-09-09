"""Catalog identities must not rewrite or race the authored bundle snapshot."""

from __future__ import annotations

import hashlib
import importlib

import pytest
import rfc8785

from loom.models.task_checksum import task_checksum
from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest


def _module():
    return importlib.import_module("loom.task_bundle_registration")


def _bundle(tmp_path):
    root = tmp_path / "bundle"
    root.mkdir()
    (root / "task.toml").write_text(
        'schema_version = "1"\n'
        '[task]\nid = "local-id"\nname = "Source task"\n'
        '[environment]\nos = "linux"\ndockerfile = "Dockerfile"\n'
        '[agent]\nname = "oracle"\n'
        '[verifier]\nname = "pytest"\n'
        '[[steps]]\nname = "main"\n',
    )
    (root / "Dockerfile").write_text("FROM python:3.11-slim\n")
    return root


def test_registration_binds_catalog_and_authored_identity_without_rewriting_bytes(tmp_path):
    root = _bundle(tmp_path)
    before = (root / "task.toml").read_bytes()
    captured = capture_task_image_bundle_manifest(root)
    registered = _module().prepare_task_bundle_registration(
        root, task_id="benchmark/local-id",
    )
    assert registered.task_config.task.id == "benchmark/local-id"
    assert registered.task_config.task.name == "Source task"
    assert registered.manifest == captured
    assert registered.manifest.task_checksum == task_checksum(root)
    assert (root / "task.toml").read_bytes() == before
    provenance = registered.source_provenance
    assert provenance["bundle_content_manifest_sha256"] == captured.digest
    assert provenance["bundle_file_metadata_sha256"] == (
        "sha256:" + captured.bundle_file_metadata_sha256
    )
    identity = provenance["bundle_task_identity"]
    assert identity == {
        "schema_version": "loom.task-bundle-identity.v1",
        "catalog_task_id": "benchmark/local-id",
        "bundle_task_id": "local-id",
        "bundle_task_toml_sha256": hashlib.sha256(before).hexdigest(),
        "registered_config_sha256": hashlib.sha256(
            rfc8785.dumps(registered.task_config.model_dump(mode="json")),
        ).hexdigest(),
    }
    assert _module().prepare_task_bundle_registration(
        root, task_id="benchmark/local-id",
    ) == registered


@pytest.mark.parametrize("task_id", ["", "a" * 513, "bad\0id", "bad\nid"])
def test_registration_refuses_unrepresentable_catalog_identity(tmp_path, task_id):
    root = _bundle(tmp_path)
    with pytest.raises(ValueError):
        _module().prepare_task_bundle_registration(root, task_id=task_id)


def test_registration_rejects_task_toml_changed_after_manifest_capture(tmp_path, monkeypatch):
    root = _bundle(tmp_path)
    module = _module()
    original_capture = module.capture_task_image_bundle_manifest

    def capture_then_change(path):
        manifest = original_capture(path)
        task_toml = path / "task.toml"
        task_toml.write_bytes(task_toml.read_bytes().replace(b"local-id", b"other-id"))
        return manifest

    monkeypatch.setattr(module, "capture_task_image_bundle_manifest", capture_then_change)
    with pytest.raises(ValueError):
        module.prepare_task_bundle_registration(root, task_id="benchmark/local-id")


def test_registration_requires_the_captured_task_configuration(tmp_path):
    root = _bundle(tmp_path)
    (root / "task.toml").unlink()
    with pytest.raises(ValueError):
        _module().prepare_task_bundle_registration(root, task_id="benchmark/local-id")


def test_registration_normalizes_terminal_bench_shape_and_retains_source_identity(tmp_path):
    root = _bundle(tmp_path)
    path = root / "task.toml"
    path.write_text(path.read_text().replace("[task]", "[metadata]"))
    registered = _module().prepare_task_bundle_registration(root, task_id="bench/local-id")
    assert registered.task_config.task.id == "bench/local-id"
    assert registered.source_provenance["bundle_task_identity"]["bundle_task_id"] == "local-id"
    assert b"[metadata]" in path.read_bytes()


def test_registration_views_cannot_change_the_frozen_binding(tmp_path):
    root = _bundle(tmp_path)
    registered = _module().prepare_task_bundle_registration(root, task_id="bench/local-id")
    provenance = registered.source_provenance
    provenance["bundle_task_identity"]["catalog_task_id"] = "changed"
    config = registered.task_config
    config.task.labels.append("changed")
    assert registered.task_config.task.labels == []
    assert registered.source_provenance["bundle_task_identity"]["catalog_task_id"] == "bench/local-id"


@pytest.mark.parametrize("explicit_arch", [None, "x86_64", "arm64", "any"])
def test_registration_runtime_architecture_promotion_preserves_explicit_choices(
    tmp_path, explicit_arch,
):
    from loom.driver.task_image import TERMINUS_2_FULL_IMAGE

    root = _bundle(tmp_path)
    (root / "Dockerfile").write_text(f"FROM {TERMINUS_2_FULL_IMAGE}\n")
    if explicit_arch is not None:
        path = root / "task.toml"
        path.write_text(path.read_text().replace(
            '[environment]', f'[environment]\ncpu_arch = "{explicit_arch}"',
        ))
    unchanged = capture_task_image_bundle_manifest(root)
    registered = _module().prepare_task_bundle_registration(
        root, task_id="bench/local-id", promote_runtime_architecture=True,
    )
    assert registered.task_config.environment.cpu_arch == (explicit_arch or "any")
    assert registered.manifest == unchanged
    if explicit_arch is None:
        assert _module().prepare_task_bundle_registration(
            root, task_id="bench/local-id",
        ).task_config.environment.cpu_arch == "x86_64"


def test_architecture_promotion_rejects_dockerfile_changed_after_capture(tmp_path, monkeypatch):
    from loom.driver.task_image import TERMINUS_2_FULL_IMAGE

    root = _bundle(tmp_path)
    module = _module()
    original_capture = module.capture_task_image_bundle_manifest

    def capture_then_change(path):
        manifest = original_capture(path)
        (path / "Dockerfile").write_text(f"FROM {TERMINUS_2_FULL_IMAGE}\n")
        return manifest

    monkeypatch.setattr(module, "capture_task_image_bundle_manifest", capture_then_change)
    with pytest.raises(ValueError):
        module.prepare_task_bundle_registration(
            root, task_id="bench/local-id", promote_runtime_architecture=True,
        )
