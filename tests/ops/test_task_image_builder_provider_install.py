from __future__ import annotations

import hashlib
import json
import os
import stat
import struct
import subprocess
from pathlib import Path

import pytest
from scripts.ops.install_task_image_builder_provider_release import (
    InstallContext,
    ProviderInstallError,
    stage_provider_release,
)
from scripts.ops.task_image_builder_provider_release import build_release

ROOT = Path(__file__).resolve().parents[2]
_ELF_HEADER = struct.Struct("<16sHHIQQQIHHHHHH")


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _elf_payload(machine: int, label: str, *, elf_type: int = 3) -> bytes:
    ident = b"\x7fELF" + bytes((2, 1, 1, 0)) + bytes(8)
    return (
        _ELF_HEADER.pack(
            ident,
            elf_type,
            machine,
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
        + label.encode("ascii")
        + b"\n"
    )


def _write(path: Path, payload: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(mode)


def _guard_release(root: Path, *, release_spec_sha256: str) -> tuple[Path, str]:
    release_root = root / "guard-release"
    map_schema = _canonical(
        {
            "schema": "loom.task-image-builder-guard-bpf-maps/v1",
            "maps": [],
        }
    )
    bpf_object = _elf_payload(247, "bpf", elf_type=1)
    files = (
        ("bpftool", _elf_payload(62, "bpftool"), 0o555),
        (
            "guard-network-map-schema-v1.json",
            map_schema,
            0o444,
        ),
        (
            "guard-network-v1.bpf.build.json",
            _canonical(
                {
                    "schema": "loom.task-image-builder-guard-bpf-build/v1",
                    "builder_image": "docker.io/example/bpf@sha256:" + "1" * 64,
                    "builder_platform": "linux/amd64",
                    "clang_version": "clang version fixture",
                    "target": "bpfel",
                    "source_sha256": "2" * 64,
                    "object_sha256": _digest(bpf_object),
                    "object_size": len(bpf_object),
                    "map_schema_sha256": _digest(map_schema),
                    "program_sections": [],
                    "program_symbols": [],
                    "map_symbols": [],
                }
            ),
            0o444,
        ),
        (
            "guard-network-v1.bpf.o",
            bpf_object,
            0o444,
        ),
        (
            "loom-task-image-builder-node-guard.service",
            (
                ROOT / "deploy/task-image-builder/loom-task-image-builder-node-guard.service"
            ).read_bytes(),
            0o444,
        ),
        ("loom-task-image-builder-guard.pyz", _elf_payload(62, "guard"), 0o555),
    )
    identity = {
        "architecture": "x86_64",
        "files": [
            {
                "mode": f"{mode:04o}",
                "path": name,
                "sha256": _digest(payload),
                "size": len(payload),
            }
            for name, payload, mode in files
        ],
        "interpreter": "/usr/bin/python3 -I -B",
        "release_spec_sha256": release_spec_sha256,
        "schema": "loom.task-image-builder-guard-bundle/v1",
    }
    guard_digest = _digest(_canonical(identity))
    release = release_root / guard_digest
    for name, payload, mode in files:
        _write(release / name, payload, mode)
    manifest = {**identity, "release_sha256": guard_digest}
    _write(release / "release-manifest.json", _canonical(manifest), 0o444)
    release.chmod(0o555)
    return release, guard_digest


def _runtime_tree(root: Path) -> Path:
    runtime = root / "runtime-root" / "runtime"
    members = {
        "buildctl": _elf_payload(62, "buildctl"),
        "buildkitd": _elf_payload(62, "buildkitd"),
        "buildkit-runc": _elf_payload(62, "buildkit-runc"),
        "rootlesskit": _elf_payload(62, "rootlesskit"),
        "rootlessctl": _elf_payload(62, "rootlessctl"),
        "slirp4netns": _elf_payload(62, "slirp4netns"),
        "fuse-overlayfs": _elf_payload(62, "fuse-overlayfs"),
    }
    for name, payload in members.items():
        _write(runtime / name, payload, 0o555)
    runtime.chmod(0o555)
    return runtime.parent


def _source_bundle(tmp_path: Path) -> tuple[Path, Path]:
    source = tmp_path / "source"
    deploy = source / "deploy/task-image-builder"
    scripts = source / "scripts/ops"
    cmd = source / "cmd/loom-task-image-builder-supervisor"
    deploy.mkdir(parents=True)
    scripts.mkdir(parents=True)
    cmd.mkdir(parents=True)

    guard_spec_payload = _canonical(
        {
            "schema": "loom.task-image-builder-guard-release-spec/v1",
            "version": 1,
        }
    )
    guard_release, guard_digest = _guard_release(
        tmp_path,
        release_spec_sha256=_digest(guard_spec_payload),
    )
    runtime_root = _runtime_tree(tmp_path)
    runtime_members = {
        name: _digest((runtime_root / "runtime" / name).read_bytes())
        for name in (
            "buildctl",
            "buildkitd",
            "buildkit-runc",
            "rootlesskit",
            "rootlessctl",
            "slirp4netns",
            "fuse-overlayfs",
        )
    }
    host_release = {
        "schema": "loom.task-image-builder-host-release/v2",
        "release": "host-release-v2",
        "runtime_manifest": "rootless-runtime-v2.json",
    }
    runtime_manifest = {
        "schema": "loom.task-image-builder-rootless-runtime/v2",
        "release": "rootless-runtime-v2",
        "toolchain": {
            "go": "go1.26.7",
            "image": "golang:1.26-alpine3.23",
            "image_sha256": "3" * 64,
            "x_crypto": "v0.55.0",
            "reproducible_flags": ["-trimpath", "-buildvcs=false"],
        },
        "architectures": {
            "amd64": {
                "platform": "linux/amd64",
                "members": runtime_members,
            }
        },
    }
    authority = b"apiVersion: v1\nkind: Service\n"
    oldlab = _canonical(
        {
            "schema": "loom.task-image-builder-supervisor-config/v1",
            "release_sha256": "1" * 64,
            "cpu_arch": "x86_64",
        }
    )
    gb10 = _canonical(
        {
            "schema": "loom.task-image-builder-supervisor-config/v1",
            "release_sha256": "a" * 64,
            "cpu_arch": "arm64",
        }
    )
    installer = b"#!/usr/bin/env python3\nprint('install fixture')\n"
    conformance = b"#!/usr/bin/env python3\nprint('conformance fixture')\n"
    supervisor_main = b"package main\nfunc main() {}\n"

    _write(
        deploy / "guard-release-v1.json",
        guard_spec_payload,
        0o444,
    )
    _write(deploy / "host-release-v2.json", _canonical(host_release), 0o444)
    _write(deploy / "rootless-runtime-v2.json", _canonical(runtime_manifest), 0o444)
    _write(deploy / "authority-service-v1.yaml", authority, 0o444)
    _write(deploy / "supervisor-config-oldlab-v1.example.json", oldlab, 0o444)
    _write(deploy / "supervisor-config-gb10-v1.example.json", gb10, 0o444)
    _write(scripts / "install_task_image_builder_provider_release.py", installer, 0o555)
    _write(scripts / "task_image_builder_provider_conformance.py", conformance, 0o555)
    _write(cmd / "main.go", supervisor_main, 0o444)

    spec = {
        "schema": "loom.task-image-builder-provider-release-spec/v1",
        "version": 1,
        "authority_contract_version": 2,
        "provider_install_root": "/opt/loom-task-image-builder-provider/releases",
        "supervisor_relative_path": "bin/loom-task-builder-supervisor",
        "guard_release": {
            "path": "deploy/task-image-builder/guard-release-v1.json",
            "sha256": _digest((deploy / "guard-release-v1.json").read_bytes()),
            "bundle_sha256": {"x86_64": guard_digest, "aarch64": "6" * 64},
        },
        "host_release": {
            "path": "deploy/task-image-builder/host-release-v2.json",
            "sha256": _digest((deploy / "host-release-v2.json").read_bytes()),
        },
        "runtime_manifest": {
            "path": "deploy/task-image-builder/rootless-runtime-v2.json",
            "sha256": _digest((deploy / "rootless-runtime-v2.json").read_bytes()),
        },
        "supervisor": {
            "sources": [
                {
                    "path": "cmd/loom-task-image-builder-supervisor/main.go",
                    "sha256": _digest(supervisor_main),
                }
            ],
            "sha256": {
                "x86_64": _digest(_elf_payload(62, "supervisor")),
                "aarch64": "5" * 64,
            },
        },
        "configs": [
            {
                "path": "deploy/task-image-builder/authority-service-v1.yaml",
                "sha256": _digest(authority),
                "destination": "configs/authority-service-v1.yaml",
                "mode": "0444",
            },
            {
                "path": "deploy/task-image-builder/supervisor-config-gb10-v1.example.json",
                "sha256": _digest(gb10),
                "destination": "configs/supervisor-config-gb10-v1.example.json",
                "mode": "0444",
            },
            {
                "path": "deploy/task-image-builder/supervisor-config-oldlab-v1.example.json",
                "sha256": _digest(oldlab),
                "destination": "configs/supervisor-config-oldlab-v1.example.json",
                "mode": "0444",
            },
        ],
        "scripts": [
            {
                "path": "scripts/ops/install_task_image_builder_provider_release.py",
                "sha256": _digest(installer),
                "destination": "ops/install_task_image_builder_provider_release.py",
                "mode": "0555",
            },
            {
                "path": "scripts/ops/task_image_builder_provider_conformance.py",
                "sha256": _digest(conformance),
                "destination": "ops/task_image_builder_provider_conformance.py",
                "mode": "0555",
            },
        ],
    }
    _write(deploy / "provider-release-v1.json", _canonical(spec), 0o444)
    release = build_release(
        source,
        tmp_path / "bundle-output",
        "x86_64",
        guard_release_directory=guard_release,
        runtime_root=runtime_root,
        build_supervisor=lambda _src, _arch: _elf_payload(62, "supervisor"),
    )
    return source, release.directory


def _bundle(tmp_path: Path) -> Path:
    return _source_bundle(tmp_path)[1]


def _context(tmp_path: Path, bundle: Path) -> InstallContext:
    root = tmp_path / "target-root"
    root.mkdir(exist_ok=True)
    return InstallContext(
        root=root,
        live=False,
        expected_release_sha256=bundle.name,
        architecture="x86_64",
        source_root=tmp_path / "source",
    )


def _forge_self_consistent_bundle(bundle: Path, member: str, payload: bytes) -> Path:
    bundle.chmod(0o755)
    member_path = bundle / member
    member_path.parent.chmod(0o755)
    member_path.chmod(0o644)
    member_path.write_bytes(payload)
    member_path.chmod(0o444)
    member_path.parent.chmod(0o555)
    manifest_path = bundle / "release-manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    for record in manifest["files"]:
        if record["path"] == member:
            record["sha256"] = _digest(payload)
            break
    else:  # pragma: no cover - fixture invariant
        raise AssertionError(f"{member} missing from release manifest")
    identity = dict(manifest)
    identity.pop("release_sha256")
    manifest["release_sha256"] = _digest(_canonical(identity))
    manifest_path.chmod(0o644)
    manifest_path.write_bytes(_canonical(manifest))
    manifest_path.chmod(0o444)
    forged = bundle.with_name(manifest["release_sha256"])
    bundle.rename(forged)
    forged.chmod(0o555)
    return forged


def _file_digest_tree(path: Path) -> dict[str, tuple[int, str]]:
    return {
        item.relative_to(path).as_posix(): (
            stat.S_IMODE(item.stat().st_mode),
            hashlib.sha256(item.read_bytes()).hexdigest(),
        )
        for item in sorted(path.rglob("*"))
        if item.is_file()
    }


def test_documented_installer_cli_runs_without_ambient_pythonpath() -> None:
    completed = subprocess.run(
        (
            "/usr/bin/python3",
            "scripts/ops/install_task_image_builder_provider_release.py",
            "--help",
        ),
        cwd=ROOT,
        env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"},
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr.decode("utf-8", "replace")
    assert completed.stdout.startswith(b"usage:")
    assert completed.stderr == b""


def test_staging_installs_only_an_immutable_release_and_durable_receipt(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)

    receipt = stage_provider_release(bundle, context)

    installed = (
        context.root
        / "opt/loom-task-image-builder-provider/releases"
        / receipt.release_sha256
    )
    receipt_path = (
        context.root
        / "var/lib/loom-task-image-builder-provider/staged"
        / f"{receipt.release_sha256}.json"
    )
    assert _file_digest_tree(installed) == _file_digest_tree(bundle)
    assert stat.S_IMODE(installed.stat().st_mode) == 0o555
    assert stat.S_IMODE(receipt_path.stat().st_mode) == 0o600
    assert json.loads(receipt_path.read_bytes()) == receipt.as_dict()
    assert receipt.activated is False
    assert receipt.production_ready is False
    for forbidden in (
        "etc/loom",
        "etc/systemd/system",
        "run/loom-task-image-builder-provider",
        "opt/loom-task-image-builder-provider/current",
        "var/lib/loom-task-image-builder-provider/current",
    ):
        assert not (context.root / forbidden).exists()


def test_repeated_byte_identical_staging_is_exactly_idempotent(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)
    first = stage_provider_release(bundle, context)
    installed = (
        context.root
        / "opt/loom-task-image-builder-provider/releases"
        / first.release_sha256
    )
    before_inode = installed.stat().st_ino
    receipt_path = (
        context.root
        / "var/lib/loom-task-image-builder-provider/staged"
        / f"{first.release_sha256}.json"
    )
    before_receipt = receipt_path.read_bytes()

    second = stage_provider_release(bundle, context)

    assert second == first
    assert installed.stat().st_ino == before_inode
    assert receipt_path.read_bytes() == before_receipt


def test_idempotent_staging_rejects_writable_existing_release_directory(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)
    first = stage_provider_release(bundle, context)
    installed = (
        context.root
        / "opt/loom-task-image-builder-provider/releases"
        / first.release_sha256
    )
    (installed / "configs").chmod(0o755)

    with pytest.raises(ProviderInstallError, match="collision"):
        stage_provider_release(bundle, context)

    assert stat.S_IMODE((installed / "configs").stat().st_mode) == 0o755


def test_idempotent_staging_rejects_receipt_replacement_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)
    receipt = stage_provider_release(bundle, context)
    receipt_path = (
        context.root
        / "var/lib/loom-task-image-builder-provider/staged"
        / f"{receipt.release_sha256}.json"
    )
    replacement = receipt_path.parent / ".replacement"
    replacement.write_bytes(receipt_path.read_bytes())
    replacement.chmod(0o600)
    original_lstat = Path.lstat
    raced = False

    def replace_after_lstat(path: Path) -> os.stat_result:
        nonlocal raced
        metadata = original_lstat(path)
        if path == receipt_path and not raced:
            raced = True
            os.replace(replacement, receipt_path)
        return metadata

    monkeypatch.setattr(Path, "lstat", replace_after_lstat)

    with pytest.raises(ProviderInstallError, match="receipt"):
        stage_provider_release(bundle, context)

    assert raced is True


def test_live_staging_requires_real_root_and_the_real_filesystem_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle(tmp_path)
    target = tmp_path / "not-root"
    target.mkdir()
    monkeypatch.setattr(os, "geteuid", lambda: 1000)

    with pytest.raises(ProviderInstallError, match="root authority"):
        stage_provider_release(
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
    member = bundle / "configs" / "authority-service-v1.yaml"
    if mutation == "digest":
        member.chmod(0o644)
        member.write_bytes(member.read_bytes() + b"changed")
        member.chmod(0o444)
    elif mutation == "path-escape":
        manifest["files"][0]["path"] = "../escape"
        manifest_path.chmod(0o644)
        manifest_path.write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="ascii",
        )
        manifest_path.chmod(0o444)
    elif mutation == "symlink":
        member.parent.chmod(0o755)
        member.unlink()
        member.symlink_to("../release-manifest.json")
        member.parent.chmod(0o555)
    else:
        member.parent.chmod(0o755)
        member.unlink()
        os.link(bundle / "release-manifest.json", member)
        member.parent.chmod(0o555)
    bundle.chmod(0o555)

    with pytest.raises(ProviderInstallError):
        stage_provider_release(bundle, _context(tmp_path, bundle))


def test_staging_rejects_self_consistent_bundle_not_bound_to_reviewed_spec(
    tmp_path: Path,
) -> None:
    _source, bundle = _source_bundle(tmp_path)
    forged = _forge_self_consistent_bundle(
        bundle,
        "configs/authority-service-v1.yaml",
        b"apiVersion: v1\nkind: ForgedService\n",
    )

    with pytest.raises(ProviderInstallError, match="reviewed"):
        stage_provider_release(forged, _context(tmp_path, forged))


def test_same_digest_collision_preserves_existing_and_candidate_for_inspection(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)
    receipt = stage_provider_release(bundle, context)
    releases = context.root / "opt/loom-task-image-builder-provider/releases"
    installed = releases / receipt.release_sha256
    installed.chmod(0o755)
    member = installed / "runtime" / "buildkitd"
    member.chmod(0o755)
    member.write_bytes(b"existing-drift\n")
    member.chmod(0o555)
    installed.chmod(0o555)

    with pytest.raises(ProviderInstallError, match="collision"):
        stage_provider_release(bundle, context)

    assert member.read_bytes() == b"existing-drift\n"
    candidates = list(releases.glob(f".{receipt.release_sha256}.conflict.*"))
    assert len(candidates) == 1
    assert _file_digest_tree(candidates[0]) == _file_digest_tree(bundle)


def test_failed_publish_does_not_leave_a_partial_release_or_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)

    def fail_publish(_source: Path, _destination: Path) -> None:
        raise ProviderInstallError("injected publish failure")

    monkeypatch.setattr(
        "scripts.ops.install_task_image_builder_provider_release._rename_noreplace",
        fail_publish,
    )

    with pytest.raises(ProviderInstallError, match="injected publish failure"):
        stage_provider_release(bundle, context)

    releases = context.root / "opt/loom-task-image-builder-provider/releases"
    assert not [path for path in releases.iterdir() if not path.name.startswith(".stage-")]
    assert not list(releases.glob(".stage-*"))
    staged = context.root / "var/lib/loom-task-image-builder-provider/staged"
    assert not staged.exists()


def test_staging_rejects_a_world_writable_managed_parent(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    context = _context(tmp_path, bundle)
    managed = context.root / "opt/loom-task-image-builder-provider"
    managed.mkdir(parents=True)
    managed.chmod(0o777)

    with pytest.raises(ProviderInstallError, match="directory is unsafe"):
        stage_provider_release(bundle, context)

    assert not (managed / "releases").exists()


def test_staging_rejects_a_bundle_reached_through_a_symlinked_parent(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    linked_parent = tmp_path / "linked-bundles"
    linked_parent.symlink_to(bundle.parent, target_is_directory=True)
    linked_bundle = linked_parent / bundle.name
    context = _context(tmp_path, bundle)

    with pytest.raises(ProviderInstallError, match="verification"):
        stage_provider_release(linked_bundle, context)
