from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from loom_cli.pipeline_cmd import PipelineCliError, _build_bundle, _validate_import_tree


def test_import_tree_and_bundle_are_deterministic(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    payload = root / "checkpoint" / "weights.bin"
    payload.parent.mkdir()
    payload.write_bytes(b"weights")
    manifest = {
        "files": [
            {
                "path": "checkpoint/weights.bin",
                "sha256": hashlib.sha256(b"weights").hexdigest(),
                "size_bytes": 7,
                "media_type": "application/octet-stream",
            }
        ]
    }
    assert _validate_import_tree(root, manifest)[0][0] == "checkpoint/weights.bin"
    first = tmp_path / "first.tar.zst"
    second = tmp_path / "second.tar.zst"
    _build_bundle(root, manifest, first)
    _build_bundle(root, manifest, second)
    assert first.read_bytes() == second.read_bytes()


def test_import_tree_rejects_undeclared_and_symlink_files(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "declared").write_bytes(b"ok")
    manifest = {
        "files": [
            {
                "path": "declared",
                "sha256": hashlib.sha256(b"ok").hexdigest(),
                "size_bytes": 2,
                "media_type": "application/octet-stream",
            }
        ]
    }
    (root / "extra").write_bytes(b"extra")
    with pytest.raises(PipelineCliError, match="exactly match"):
        _validate_import_tree(root, manifest)
