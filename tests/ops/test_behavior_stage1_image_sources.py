from __future__ import annotations

import hashlib
import io
import json
import tarfile
from pathlib import Path

import pytest
from scripts.prepare_behavior_stage1_image_sources import (
    SourceLockError,
    _apply_openpi_cache_patch,
    _extract_regular_archive,
    _reject_lfs_pointers,
    load_source_lock,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_checked_in_stage1_source_lock_is_closed_and_self_consistent() -> None:
    lock = load_source_lock(REPO_ROOT)
    assert [source.name for source in lock.sources] == [
        "b1k",
        "bddl",
        "curobo",
        "dlimp",
        "lerobot",
        "omnigibson",
        "openpi",
    ]
    vendored = [source for source in lock.sources if source.visibility == "vendored-runtime"]
    assert len(vendored) == 1
    assert vendored[0].name == "omnigibson"
    assert vendored[0].vendor_path == "third_party/behavior-stage1/omnigibson"
    curobo = next(source for source in lock.sources if source.name == "curobo")
    assert curobo.excluded_upstream_entries == (
        "images",
        "src/curobo/content/assets",
    )
    lerobot = next(source for source in lock.sources if source.name == "lerobot")
    assert lerobot.excluded_upstream_entries == ("tests",)
    assert lock.raw["base_image"]["platform"] == "linux/amd64"
    assert lock.raw["runtime_assets"] == [
        {
            "binary_version": "n7.1.5-12-g1fdbca85aa-20260813",
            "license": "LGPL-2.1-or-later",
            "name": "ffmpeg",
            "release_tag": "autobuild-2026-08-13-17-03",
            "sha256": "sha256:b33b9c56b28dbc709a7938e2461d34caefc897a6090ac02da8fc55f82d6d5451",
            "url": "https://github.com/BtbN/FFmpeg-Builds/releases/download/"
            "autobuild-2026-08-13-17-03/"
            "ffmpeg-n7.1.5-12-g1fdbca85aa-linux64-lgpl-shared-7.1.tar.xz",
            "version": "7.1.5-12-g1fdbca85aa",
        }
    ]


def _archive(path: Path, members: list[tuple[str, bytes, str]]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        root = tarfile.TarInfo("root")
        root.type = tarfile.DIRTYPE
        archive.addfile(root)
        for name, payload, kind in members:
            info = tarfile.TarInfo(name)
            if kind == "file":
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
            elif kind == "symlink":
                info.type = tarfile.SYMTYPE
                info.linkname = "target"
                archive.addfile(info)


def test_source_archive_extraction_accepts_only_regular_confined_entries(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "source.tar.gz"
    _archive(archive, [("root/pkg.py", b"ok\n", "file")])
    output = tmp_path / "out"
    _extract_regular_archive(archive, output, "root")
    assert (output / "pkg.py").read_bytes() == b"ok\n"
    assert (output / "pkg.py").stat().st_mode & 0o777 == 0o444

    bad = tmp_path / "bad.tar.gz"
    _archive(bad, [("root/link", b"", "symlink")])
    with pytest.raises(SourceLockError, match="regular"):
        _extract_regular_archive(bad, tmp_path / "bad-out", "root")


def test_source_projection_excludes_exact_prefix_and_rejects_lfs_pointers(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "source.tar.gz"
    _archive(
        archive,
        [
            ("root/src/curobo/__init__.py", b"ok\n", "file"),
            (
                "root/src/curobo/content/assets/unused.obj",
                b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\n",
                "file",
            ),
        ],
    )
    output = tmp_path / "out"
    _extract_regular_archive(
        archive,
        output,
        "root",
        excluded_prefixes=("src/curobo/content/assets",),
    )
    assert (output / "src/curobo/__init__.py").read_bytes() == b"ok\n"
    assert not (output / "src/curobo/content/assets").exists()
    _reject_lfs_pointers(output)

    pointer_root = tmp_path / "pointer"
    pointer_root.mkdir()
    (pointer_root / "bad").write_bytes(
        b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\n"
    )
    with pytest.raises(SourceLockError, match="Git LFS pointer"):
        _reject_lfs_pointers(pointer_root)


def test_source_archive_materializes_confined_symlinks_as_regular_files(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "source.tar.gz"
    _archive(
        archive,
        [
            ("root/target", b"locked\n", "file"),
            ("root/link", b"", "symlink"),
        ],
    )
    output = tmp_path / "out"
    _extract_regular_archive(archive, output, "root")
    assert (output / "link").read_bytes() == b"locked\n"
    assert not (output / "link").is_symlink()


def test_source_archive_extraction_allows_only_the_exact_locked_omitted_symlink(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "source.tar.gz"
    _archive(archive, [("root/unsafe.yaml", b"", "symlink")])
    output = tmp_path / "out"
    _extract_regular_archive(
        archive,
        output,
        "root",
        excluded_symlinks=(("root/unsafe.yaml", "target"),),
    )
    assert list(output.iterdir()) == []

    with pytest.raises(SourceLockError, match="target drift"):
        _extract_regular_archive(
            archive,
            tmp_path / "wrong-target",
            "root",
            excluded_symlinks=(("root/unsafe.yaml", "different"),),
        )


def test_source_evidence_preimage_is_canonical_jcs_plus_lf() -> None:
    lock = load_source_lock(REPO_ROOT)
    value = {
        "integration_patches": [
            {
                "name": "openpi-transformers-cache-type",
                "path": "openpi/src/openpi/models_pytorch/gemma_pytorch.py",
                "result_sha256": "sha256:4f75d3647fadb7d00c0fee884579cf5a3ef33a6af53a3908fc237358d9606cf5",
                "source_sha256": "sha256:08fd8d750519f0fb44fc5173311e50a30f4c8f32c02e51244b4f8e47b32cd52f",
            }
        ],
        "schema_version": "loom.behavior-stage1-image-source-evidence.v1",
        "source_lock_sha256": "sha256:" + "a" * 64,
        "sources": [
            {"commit": item.commit, "name": item.name, "tree": item.tree} for item in lock.sources
        ],
    }
    payload = (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )
    assert payload.endswith(b"\n") and not payload.endswith(b"\n\n")
    assert len(payload) == 1318
    assert (
        hashlib.sha256(payload).hexdigest()
        == "eedec0e240413281e62a6eee87555078092be729ec9cdae3fad176e241f3fbdf"
    )


def test_openpi_cache_patch_is_exact_and_removes_test_runtime_dependency(
    tmp_path: Path,
) -> None:
    target = tmp_path / "openpi/src/openpi/models_pytorch/gemma_pytorch.py"
    target.parent.mkdir(parents=True)
    original = REPO_ROOT / ".git"  # prove the fixture is not silently synthesized
    del original
    payload = (
        b"from typing import Literal\n\nimport pytest\n"
        + b"value: list[torch.FloatTensor] | pytest.Cache | None\n"
    )
    # The production patch is deliberately pinned to the complete upstream file,
    # so a reduced fixture must fail before it can be rewritten.
    target.write_bytes(payload)
    with pytest.raises(SourceLockError, match="source drift"):
        _apply_openpi_cache_patch(tmp_path)
