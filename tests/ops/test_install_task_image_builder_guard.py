"""Transactional safety tests for staging an inert node-guard release."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import struct
from pathlib import Path

import pytest
from scripts.ops.install_task_image_builder_guard import (
    GuardInstallError,
    InstallContext,
    stage_guard_release,
)
from scripts.ops.task_image_builder_guard_release import build_release

ROOT = Path(__file__).resolve().parents[2]


def _bpftool(path: Path) -> Path:
    ident = b"\x7fELF" + bytes((2, 1, 1, 0)) + bytes(8)
    path.write_bytes(
        struct.pack(
            "<16sHHIQQQIHHHHHH",
            ident,
            3,
            62,
            1,
            0,
            0,
            0,
            0,
            64,
            0,
            0,
            64,
            0,
            0,
        )
        + b"test-bpftool"
    )
    path.chmod(0o755)
    return path


def _bundle(tmp_path: Path) -> Path:
    result = build_release(
        ROOT,
        _bpftool(tmp_path / "bpftool"),
        tmp_path / "bundle-output",
        "x86_64",
    )
    return result.directory


def _context(tmp_path: Path, bundle: Path) -> InstallContext:
    root = tmp_path / "target-root"
    root.mkdir(exist_ok=True)
    return InstallContext(
        root=root,
        live=False,
        expected_release_sha256=bundle.name,
        architecture="x86_64",
    )


def _tree_digest(path: Path) -> dict[str, tuple[int, str]]:
    return {
        item.relative_to(path).as_posix(): (
            stat.S_IMODE(item.stat().st_mode),
            hashlib.sha256(item.read_bytes()).hexdigest(),
        )
        for item in sorted(path.iterdir())
    }


def test_staging_installs_only_an_immutable_release_and_durable_receipt(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)

    receipt = stage_guard_release(bundle, context)

    installed = (
        context.root
        / "opt/loom-task-image-builder-guard/releases"
        / receipt.release_sha256
    )
    receipt_path = (
        context.root
        / "var/lib/loom-task-image-builder-guard/staged"
        / f"{receipt.release_sha256}.json"
    )
    assert _tree_digest(installed) == _tree_digest(bundle)
    assert stat.S_IMODE(installed.stat().st_mode) == 0o555
    assert stat.S_IMODE(receipt_path.stat().st_mode) == 0o600
    assert json.loads(receipt_path.read_bytes()) == receipt.as_dict()
    assert receipt.activated is False
    assert receipt.production_ready is False
    for forbidden in (
        "etc/loom",
        "etc/systemd/system",
        "run/loom-task-image-builder-guard",
        "sys/fs/bpf/loom-task-image-builder",
        "opt/loom-task-image-builder-guard/current",
    ):
        assert not (context.root / forbidden).exists()


def test_repeated_byte_identical_staging_is_exactly_idempotent(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)
    first = stage_guard_release(bundle, context)
    installed = (
        context.root
        / "opt/loom-task-image-builder-guard/releases"
        / first.release_sha256
    )
    before_inode = installed.stat().st_ino
    receipt_path = (
        context.root
        / "var/lib/loom-task-image-builder-guard/staged"
        / f"{first.release_sha256}.json"
    )
    before_receipt = receipt_path.read_bytes()

    second = stage_guard_release(bundle, context)

    assert second == first
    assert installed.stat().st_ino == before_inode
    assert receipt_path.read_bytes() == before_receipt


def test_live_staging_requires_real_root_and_the_real_filesystem_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle(tmp_path)
    target = tmp_path / "not-root"
    target.mkdir()
    monkeypatch.setattr(os, "geteuid", lambda: 1000)

    with pytest.raises(GuardInstallError, match="root authority"):
        stage_guard_release(
            bundle,
            InstallContext(
                root=target,
                live=True,
                expected_release_sha256=bundle.name,
                architecture="x86_64",
            ),
        )


@pytest.mark.parametrize(
    "mutation",
    ("digest", "path-escape", "symlink", "hardlink"),
)
def test_staging_rejects_manifest_and_member_substitution(
    tmp_path: Path,
    mutation: str,
) -> None:
    bundle = _bundle(tmp_path)
    bundle.chmod(0o755)
    manifest_path = bundle / "release-manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    member = bundle / "guard-network-map-schema-v1.json"
    if mutation == "digest":
        member.chmod(0o644)
        member.write_bytes(member.read_bytes() + b"changed")
        member.chmod(0o444)
    elif mutation == "path-escape":
        manifest["files"][0]["path"] = "../bpftool"
        manifest_path.chmod(0o644)
        manifest_path.write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="ascii",
        )
        manifest_path.chmod(0o444)
    elif mutation == "symlink":
        member.unlink()
        member.symlink_to("guard-network-v1.bpf.build.json")
    else:
        member.unlink()
        os.link(bundle / "guard-network-v1.bpf.build.json", member)
    bundle.chmod(0o555)

    with pytest.raises(GuardInstallError):
        stage_guard_release(bundle, _context(tmp_path, bundle))


def test_same_digest_collision_preserves_existing_and_candidate_for_inspection(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)
    receipt = stage_guard_release(bundle, context)
    releases = context.root / "opt/loom-task-image-builder-guard/releases"
    installed = releases / receipt.release_sha256
    installed.chmod(0o755)
    member = installed / "guard-network-map-schema-v1.json"
    member.chmod(0o644)
    member.write_bytes(b"existing-drift\n")
    member.chmod(0o444)
    installed.chmod(0o555)

    with pytest.raises(GuardInstallError, match="collision"):
        stage_guard_release(bundle, context)

    assert member.read_bytes() == b"existing-drift\n"
    candidates = list(releases.glob(f".{receipt.release_sha256}.conflict.*"))
    assert len(candidates) == 1
    assert _tree_digest(candidates[0]) == _tree_digest(bundle)


def test_failed_publish_does_not_leave_a_partial_release_or_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)

    def fail_publish(_source: Path, _destination: Path) -> None:
        raise GuardInstallError("injected publish failure")

    monkeypatch.setattr(
        "scripts.ops.install_task_image_builder_guard._rename_noreplace",
        fail_publish,
    )

    with pytest.raises(GuardInstallError, match="injected publish failure"):
        stage_guard_release(bundle, context)

    releases = context.root / "opt/loom-task-image-builder-guard/releases"
    assert not [path for path in releases.iterdir() if not path.name.startswith(".stage-")]
    assert not list(releases.glob(".stage-*"))
    staged = context.root / "var/lib/loom-task-image-builder-guard/staged"
    assert not staged.exists()


def test_staging_rejects_a_world_writable_managed_parent(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)
    managed = context.root / "opt/loom-task-image-builder-guard"
    managed.mkdir(parents=True)
    managed.chmod(0o777)

    with pytest.raises(GuardInstallError, match="directory is unsafe"):
        stage_guard_release(bundle, context)

    assert not (managed / "releases").exists()


def test_staging_rejects_a_bundle_reached_through_a_symlinked_parent(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    linked_parent = tmp_path / "linked-bundles"
    linked_parent.symlink_to(bundle.parent, target_is_directory=True)
    linked_bundle = linked_parent / bundle.name
    context = _context(tmp_path, bundle)

    with pytest.raises(GuardInstallError, match="verification"):
        stage_guard_release(linked_bundle, context)
