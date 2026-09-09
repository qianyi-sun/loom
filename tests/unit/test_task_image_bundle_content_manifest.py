"""Registration-bound content identity cannot rely on delimiter-only hashing."""

import hashlib
import json

import pytest

from loom.models.task_checksum import task_checksum
from loom.trajectory.storage import bundle_file_metadata_sha256


def _legacy_collision(tmp_path):
    left, right = tmp_path / "left", tmp_path / "right"
    left.mkdir()
    right.mkdir()
    (left / "a").write_bytes(b"P")
    (left / "b").write_bytes(b"Q\x00b\x00R")
    (right / "a").write_bytes(b"P\x00b\x00Q")
    (right / "b").write_bytes(b"R")
    return left, right


def test_legacy_checksum_and_mode_metadata_do_not_authenticate_file_boundaries(tmp_path):
    left, right = _legacy_collision(tmp_path)
    assert (left / "a").read_bytes() != (right / "a").read_bytes()
    assert task_checksum(left) == task_checksum(right)
    assert bundle_file_metadata_sha256(left) == bundle_file_metadata_sha256(right)


def test_content_manifest_binds_file_sizes_hashes_and_modes(tmp_path):
    from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest

    left, right = _legacy_collision(tmp_path)
    first = capture_task_image_bundle_manifest(left)
    second = capture_task_image_bundle_manifest(right)
    assert first.task_checksum == second.task_checksum == task_checksum(left)
    assert first.bundle_file_metadata_sha256 == bundle_file_metadata_sha256(left).removeprefix("sha256:")
    assert first.digest != second.digest
    assert [(item.path, item.size_bytes, item.sha256, item.mode) for item in first.files] == [
        ("a", 1, hashlib.sha256(b"P").hexdigest(), "0644"),
        ("b", 5, hashlib.sha256(b"Q\x00b\x00R").hexdigest(), "0644"),
    ]


def test_manifest_is_canonical_and_verifies_expected_digest(tmp_path):
    from loom.task_image_bundle_manifest import (
        capture_task_image_bundle_manifest,
        parse_task_image_bundle_manifest,
    )

    (tmp_path / "run café.sh").write_bytes(b"#!/bin/sh\n")
    (tmp_path / "run café.sh").chmod(0o755)
    manifest = capture_task_image_bundle_manifest(tmp_path)
    assert parse_task_image_bundle_manifest(manifest.canonical_bytes, expected_sha256=manifest.digest) == manifest
    assert manifest.digest == hashlib.sha256(manifest.canonical_bytes).hexdigest()
    assert manifest.files[0].mode == "0755"
    assert manifest.bundle_file_metadata_sha256 == bundle_file_metadata_sha256(tmp_path).removeprefix("sha256:")
    with pytest.raises(ValueError):
        parse_task_image_bundle_manifest(manifest.canonical_bytes, expected_sha256="0" * 64)
    # Same semantic JSON is not the registered canonical byte artifact.
    noncanonical = json.dumps(json.loads(manifest.canonical_bytes), indent=2).encode()
    with pytest.raises(ValueError):
        parse_task_image_bundle_manifest(noncanonical, expected_sha256=hashlib.sha256(noncanonical).hexdigest())


@pytest.mark.parametrize("changed", ["content", "size", "mode", "path", "duplicate", "parent_conflict", "unknown", "unsafe_path", "bad_mode", "bad_hash"])
def test_manifest_rejects_digest_or_structural_mutation(tmp_path, changed):
    from loom.task_image_bundle_manifest import (
        capture_task_image_bundle_manifest,
        parse_task_image_bundle_manifest,
    )

    (tmp_path / "a").write_bytes(b"one")
    manifest = capture_task_image_bundle_manifest(tmp_path)
    data = json.loads(manifest.canonical_bytes)
    entry = data["files"][0]
    if changed == "content":
        entry["sha256"] = "f" * 64
    elif changed == "size":
        entry["size_bytes"] = 4
    elif changed == "mode":
        entry["mode"] = "0755"
    elif changed == "path":
        entry["path"] = "b"
    elif changed == "duplicate":
        data["files"].append(dict(entry))
    elif changed == "parent_conflict":
        data["files"].append(dict(entry, path="a/child"))
    elif changed == "unknown":
        entry["unknown"] = True
    elif changed == "unsafe_path":
        entry["path"] = "../outside"
    elif changed == "bad_mode":
        entry["mode"] = "04755"
    else:
        entry["sha256"] = "not-a-hash"
    payload = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    expected = manifest.digest if changed in {"content", "size"} else hashlib.sha256(payload).hexdigest()
    with pytest.raises(ValueError):
        parse_task_image_bundle_manifest(payload, expected_sha256=expected)


@pytest.mark.parametrize("kind", ["file_symlink", "directory_symlink", "hardlink", "fifo"])
def test_capture_refuses_non_owned_regular_tree_entries(tmp_path, kind):
    import os

    from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest

    (tmp_path / "data").write_bytes(b"safe")
    if kind == "file_symlink":
        (tmp_path / "link").symlink_to(tmp_path / "data")
    elif kind == "directory_symlink":
        (tmp_path / "link").symlink_to(tmp_path, target_is_directory=True)
    elif kind == "hardlink":
        os.link(tmp_path / "data", tmp_path / "link")
    else:
        os.mkfifo(tmp_path / "pipe")
    with pytest.raises(ValueError):
        capture_task_image_bundle_manifest(tmp_path)


@pytest.mark.parametrize("kind", ["read_mutation", "late_mutation", "count", "bytes", "entries", "depth", "empty", "root_symlink"])
def test_capture_checks_stability_and_whole_tree_budgets(tmp_path, monkeypatch, kind):
    import os

    from loom import task_image_bundle_manifest as module

    root = tmp_path / "tree"
    root.mkdir()
    if kind != "empty":
        (root / "a").write_bytes(b"one")
        (root / "b").write_bytes(b"two")
    if kind in {"read_mutation", "late_mutation"}:
        original_read = os.read
        calls = []

        def read(descriptor, size):
            payload = original_read(descriptor, size)
            if payload:
                calls.append(payload)
                if len(calls) == (1 if kind == "read_mutation" else 2):
                    (root / "a").write_bytes(b"mutated")
            return payload

        monkeypatch.setattr(os, "read", read)
    elif kind == "count":
        monkeypatch.setattr(module, "MAX_TASK_IMAGE_BUILD_BUNDLE_FILES", 1)
    elif kind == "bytes":
        monkeypatch.setattr(module, "MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES", 5)
    elif kind == "entries":
        monkeypatch.setattr(module, "_MAX_TREE_ENTRIES", 1)
    elif kind == "depth":
        (root / "nested" / "deeper").mkdir(parents=True)
        monkeypatch.setattr(module, "_MAX_TREE_DEPTH", 1)
    elif kind == "root_symlink":
        link = tmp_path / "alias"
        link.symlink_to(root, target_is_directory=True)
        root = link
    with pytest.raises(ValueError):
        module.capture_task_image_bundle_manifest(root)


def test_legacy_order_and_mode_sidecar_encoding_have_independent_unicode_vectors(tmp_path):
    from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest

    names = ("a/file", "a.ext", "astral-😀", "combining-é", "<&>", "private-\ue000")
    for name in names:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(name.encode())
    manifest = capture_task_image_bundle_manifest(tmp_path)
    assert manifest.task_checksum == task_checksum(tmp_path)
    assert manifest.bundle_file_metadata_sha256 == bundle_file_metadata_sha256(tmp_path).removeprefix("sha256:")
    assert b"\\ud83d\\ude00" in manifest.mode_metadata_bytes
    assert "😀".encode() in manifest.canonical_bytes


def test_manifest_and_legacy_sidecar_have_separate_byte_budgets(tmp_path, monkeypatch):
    from loom import task_image_bundle_manifest as module

    (tmp_path / "a").write_bytes(b"one")
    with monkeypatch.context() as patch:
        patch.setattr(module, "MAX_MODE_METADATA_BYTES", 1)
        with pytest.raises(ValueError):
            module.capture_task_image_bundle_manifest(tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(module, "MAX_CONTENT_MANIFEST_BYTES", 1)
        with pytest.raises(ValueError):
            module.capture_task_image_bundle_manifest(tmp_path)


def test_content_manifest_counts_pending_directory_entries_globally(tmp_path, monkeypatch):
    import os

    from loom import task_image_bundle_manifest as module

    (tmp_path / "a").mkdir()
    (tmp_path / "z").write_bytes(b"later sibling")
    (tmp_path / "a" / "child").write_bytes(b"nested")
    monkeypatch.setattr(module, "_MAX_TREE_ENTRIES", 2)

    def unexpected_read(*args):
        pytest.fail("entry enumeration exceeded quota before opening file content")

    monkeypatch.setattr(os, "read", unexpected_read)
    with pytest.raises(ValueError):
        module.capture_task_image_bundle_manifest(tmp_path)


@pytest.mark.parametrize("kind", ["parent_symlink", "file_symlink", "hardlink", "fifo", "mode", "same_size_bytes"])
def test_verified_upload_reader_refuses_replaced_descriptors(tmp_path, kind):
    import os

    from loom.task_image_bundle_manifest import (
        capture_task_image_bundle_manifest,
        read_verified_task_image_bundle_file,
    )

    root = tmp_path / "tree"
    (root / "dir").mkdir(parents=True)
    source = root / "dir" / "data"
    source.write_bytes(b"one")
    manifest = capture_task_image_bundle_manifest(root)
    if kind == "parent_symlink":
        (root / "dir").rename(root / "old")
        (root / "dir").symlink_to(root / "old", target_is_directory=True)
    elif kind in {"file_symlink", "hardlink", "fifo"}:
        original = root / "original"
        source.rename(original)
        if kind == "file_symlink":
            source.symlink_to(original)
        elif kind == "hardlink":
            os.link(original, source)
        else:
            os.mkfifo(source)
    elif kind == "mode":
        source.chmod(0o755)
    else:
        source.write_bytes(b"two")
    with pytest.raises(ValueError):
        read_verified_task_image_bundle_file(root, manifest.files[0])
