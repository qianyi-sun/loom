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
    from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest, parse_task_image_bundle_manifest

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
