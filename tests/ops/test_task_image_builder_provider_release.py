"""Behavioral tests for the inert, content-addressed provider release."""

from __future__ import annotations

import hashlib
import json
import os
import struct
import subprocess
from pathlib import Path

import pytest
from scripts.ops.task_image_builder_provider_release import (
    Architecture,
    ProviderReleaseError,
    build_certified_releases,
    build_release,
    verify_release_directory,
)

_ELF_HEADER = struct.Struct("<16sHHIQQQIHHHHHH")
ROOT = Path(__file__).resolve().parents[2]


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _elf_payload(machine: int, label: str) -> bytes:
    ident = b"\x7fELF" + bytes((2, 1, 1, 0)) + bytes(8)
    return (
        _ELF_HEADER.pack(
            ident,
            3,
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


def _guard_release(root: Path) -> tuple[Path, str]:
    return _guard_release_for(root, architecture="x86_64", machine=62)


def _guard_release_for(root: Path, *, architecture: str, machine: int) -> tuple[Path, str]:
    release_root = root / "guard-release"
    files = {
        "loom-task-image-builder-guard.pyz": (_elf_payload(machine, f"guard-{architecture}"), 0o555),
        "guard-network-v1.bpf.o": (_elf_payload(247, "bpf"), 0o444),
        "guard-network-v1.bpf.build.json": (
            _canonical(
                {
                    "schema": "loom.task-image-builder-guard-bpf-build/v1",
                    "builder_image": "docker.io/example/bpf@sha256:" + "1" * 64,
                    "builder_platform": "linux/amd64",
                    "clang_version": "clang version fixture",
                    "target": "bpfel",
                    "source_sha256": "2" * 64,
                    "object_sha256": _digest(_elf_payload(247, "bpf")),
                    "object_size": len(_elf_payload(247, "bpf")),
                    "map_schema_sha256": _digest(
                        _canonical(
                            {
                                "schema": "loom.task-image-builder-guard-bpf-maps/v1",
                                "maps": [],
                            }
                        )
                    ),
                    "program_sections": [],
                    "program_symbols": [],
                    "map_symbols": [],
                }
            ),
            0o444,
        ),
        "guard-network-map-schema-v1.json": (
            _canonical(
                {
                    "schema": "loom.task-image-builder-guard-bpf-maps/v1",
                    "maps": [],
                }
            ),
            0o444,
        ),
        "loom-task-image-builder-node-guard.service": (b"[Unit]\n[Service]\n", 0o444),
    }
    identity = {
        "schema": "loom.task-image-builder-guard-bundle/v1",
        "architecture": architecture,
        "files": [
            {"path": name, "mode": f"{mode:04o}", "sha256": _digest(payload)}
            for name, (payload, mode) in sorted(files.items())
        ],
    }
    guard_digest = _digest(_canonical(identity))
    release = release_root / guard_digest
    for name, (payload, mode) in files.items():
        _write(release / name, payload, mode)
    manifest = {**identity, "release_sha256": guard_digest}
    _write(release / "release-manifest.json", _canonical(manifest), 0o444)
    return release, guard_digest


def _runtime_tree(root: Path) -> Path:
    return _runtime_tree_for(root, machine=62)


def _runtime_tree_for(root: Path, *, machine: int) -> Path:
    runtime = root / "runtime-root" / "runtime"
    members = {
        "buildctl": _elf_payload(machine, "buildctl"),
        "buildkitd": _elf_payload(machine, "buildkitd"),
        "buildkit-runc": _elf_payload(machine, "buildkit-runc"),
        "rootlesskit": _elf_payload(machine, "rootlesskit"),
        "rootlessctl": _elf_payload(machine, "rootlessctl"),
        "slirp4netns": _elf_payload(machine, "slirp4netns"),
        "fuse-overlayfs": _elf_payload(machine, "fuse-overlayfs"),
    }
    for name, payload in members.items():
        _write(runtime / name, payload, 0o555)
    return runtime.parent


def _source_tree(tmp_path: Path) -> tuple[Path, Path, Path]:
    source = tmp_path / "source"
    deploy = source / "deploy/task-image-builder"
    scripts = source / "scripts/ops"
    cmd = source / "cmd/loom-task-image-builder-supervisor"
    deploy.mkdir(parents=True)
    scripts.mkdir(parents=True)
    cmd.mkdir(parents=True)

    guard_release, guard_digest = _guard_release(tmp_path)
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

    _write(deploy / "guard-release-v1.json", _canonical({"guard_release_sha256": guard_digest}), 0o444)
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
    return source, guard_release, runtime_root


def _multi_arch_source_tree(
    tmp_path: Path,
) -> tuple[Path, dict[Architecture, Path], dict[Architecture, Path]]:
    source, x86_guard, x86_runtime = _source_tree(tmp_path)
    aarch_guard, aarch_guard_digest = _guard_release_for(
        tmp_path / "aarch64",
        architecture="aarch64",
        machine=183,
    )
    aarch_runtime = _runtime_tree_for(tmp_path / "aarch64", machine=183)
    deploy = source / "deploy/task-image-builder"
    runtime_path = deploy / "rootless-runtime-v2.json"
    runtime_manifest = json.loads(runtime_path.read_bytes())
    runtime_manifest["architectures"]["arm64"] = {
        "platform": "linux/arm64",
        "members": {
            name: _digest((aarch_runtime / "runtime" / name).read_bytes())
            for name in (
                "buildctl",
                "buildkitd",
                "buildkit-runc",
                "rootlesskit",
                "rootlessctl",
                "slirp4netns",
                "fuse-overlayfs",
            )
        },
    }
    runtime_path.chmod(0o644)
    runtime_path.write_bytes(_canonical(runtime_manifest))
    runtime_path.chmod(0o444)
    spec_path = deploy / "provider-release-v1.json"
    spec = json.loads(spec_path.read_bytes())
    spec["guard_release"]["bundle_sha256"]["aarch64"] = aarch_guard_digest
    spec["runtime_manifest"]["sha256"] = _digest(runtime_path.read_bytes())
    spec["supervisor"]["sha256"]["aarch64"] = _digest(_elf_payload(183, "supervisor"))
    spec_path.chmod(0o644)
    spec_path.write_bytes(_canonical(spec))
    spec_path.chmod(0o444)
    return (
        source,
        {"x86_64": x86_guard, "aarch64": aarch_guard},
        {"x86_64": x86_runtime, "aarch64": aarch_runtime},
    )


def test_release_is_content_addressed_and_binds_expected_members(tmp_path: Path) -> None:
    source, guard_release, runtime_root = _source_tree(tmp_path)

    result = build_release(
        source,
        tmp_path / "out",
        "x86_64",
        guard_release_directory=guard_release,
        runtime_root=runtime_root,
        build_supervisor=lambda _src, _arch: _elf_payload(62, "supervisor"),
    )

    manifest = json.loads((result.directory / "release-manifest.json").read_bytes())
    assert result.directory.name == result.release_sha256
    assert manifest["architecture"] == "x86_64"
    assert manifest["authority_contract_version"] == 2
    assert manifest["guard_release_sha256"] == guard_release.name
    assert manifest["runtime_release"] == "rootless-runtime-v2"
    assert manifest["runtime_x_crypto"] == "v0.55.0"
    assert manifest["provider_install_root"] == "/opt/loom-task-image-builder-provider/releases"
    assert manifest["supervisor_relative_path"] == "bin/loom-task-builder-supervisor"
    assert [record["path"] for record in manifest["files"]] == [
        "bin/fuse-overlayfs",
        "bin/loom-task-builder-supervisor",
        "bin/rootlessctl",
        "bin/rootlesskit",
        "bin/slirp4netns",
        "configs/authority-service-v1.yaml",
        "configs/supervisor-config-gb10-v1.example.json",
        "configs/supervisor-config-oldlab-v1.example.json",
        "guard-network-map-schema-v1.json",
        "guard-network-v1.bpf.build.json",
        "guard-network-v1.bpf.o",
        "loom-task-image-builder-guard.pyz",
        "loom-task-image-builder-node-guard.service",
        "ops/install_task_image_builder_provider_release.py",
        "ops/task_image_builder_provider_conformance.py",
        "runtime/buildctl",
        "runtime/buildkit-runc",
        "runtime/buildkitd",
    ]
    assert all(record["mode"] in {"0444", "0555"} for record in manifest["files"])
    assert not (result.directory / "current").exists()


def test_checked_in_release_spec_uses_inert_ops_script_destinations() -> None:
    spec = json.loads(
        (ROOT / "deploy/task-image-builder/provider-release-v1.json").read_text(
            encoding="utf-8"
        )
    )

    assert spec["schema"] == "loom.task-image-builder-provider-release-spec/v1"
    assert spec["provider_install_root"] == "/opt/loom-task-image-builder-provider/releases"
    assert spec["supervisor_relative_path"] == "bin/loom-task-builder-supervisor"
    assert [record["destination"] for record in spec["scripts"]] == [
        "ops/install_task_image_builder_provider_release.py",
        "ops/task_image_builder_provider_conformance.py",
    ]
    assert all(record["mode"] == "0555" for record in spec["scripts"])
    for section in ("configs", "scripts"):
        for record in spec[section]:
            assert _digest((ROOT / record["path"]).read_bytes()) == record["sha256"]
    for record in spec["supervisor"]["sources"]:
        assert _digest((ROOT / record["path"]).read_bytes()) == record["sha256"]
    for section in ("guard_release", "host_release", "runtime_manifest"):
        record = spec[section]
        assert _digest((ROOT / record["path"]).read_bytes()) == record["sha256"]
    serialized = json.dumps(spec, sort_keys=True)
    for forbidden in ("current", "systemctl", "activation", "credential"):
        assert forbidden not in serialized


def test_release_is_deterministic_across_source_metadata_noise(tmp_path: Path) -> None:
    first_source, first_guard, first_runtime = _source_tree(tmp_path / "first")
    second_source, second_guard, second_runtime = _source_tree(tmp_path / "second")
    for index, path in enumerate(sorted(second_source.rglob("*"), reverse=True)):
        if path.is_file():
            timestamp = 1_800_000_000 + index
            os.utime(path, (timestamp, timestamp))

    first = build_release(
        first_source,
        tmp_path / "out-one",
        "x86_64",
        guard_release_directory=first_guard,
        runtime_root=first_runtime,
        build_supervisor=lambda _src, _arch: _elf_payload(62, "supervisor"),
    )
    second = build_release(
        second_source,
        tmp_path / "out-two",
        "x86_64",
        guard_release_directory=second_guard,
        runtime_root=second_runtime,
        build_supervisor=lambda _src, _arch: _elf_payload(62, "supervisor"),
    )

    assert first.release_sha256 == second.release_sha256
    assert {
        item.relative_to(first.directory).as_posix(): item.read_bytes()
        for item in first.directory.rglob("*")
        if item.is_file()
    } == {
        item.relative_to(second.directory).as_posix(): item.read_bytes()
        for item in second.directory.rglob("*")
        if item.is_file()
    }


def test_release_builds_supervisor_twice_before_publishing(tmp_path: Path) -> None:
    source, guard_release, runtime_root = _source_tree(tmp_path)
    calls: list[tuple[Path, Architecture]] = []

    def build_supervisor(src: Path, arch: Architecture) -> bytes:
        calls.append((src, arch))
        return _elf_payload(62, "supervisor")

    result = build_release(
        source,
        tmp_path / "out",
        "x86_64",
        guard_release_directory=guard_release,
        runtime_root=runtime_root,
        build_supervisor=build_supervisor,
    )

    assert result.directory.exists()
    assert calls == [(source, "x86_64"), (source, "x86_64")]


def test_release_rejects_nondeterministic_supervisor_builder(tmp_path: Path) -> None:
    source, guard_release, runtime_root = _source_tree(tmp_path)
    payloads = iter(
        (
            _elf_payload(62, "supervisor"),
            _elf_payload(62, "supervisor-drift"),
        )
    )

    with pytest.raises(ProviderReleaseError, match="deterministic"):
        build_release(
            source,
            tmp_path / "out",
            "x86_64",
            guard_release_directory=guard_release,
            runtime_root=runtime_root,
            build_supervisor=lambda _src, _arch: next(payloads),
        )

    assert not any((tmp_path / "out").glob("*/release-manifest.json"))


def test_certified_release_builds_both_architectures_twice_all_or_nothing(
    tmp_path: Path,
) -> None:
    source, guard_releases, runtime_roots = _multi_arch_source_tree(tmp_path)
    calls: list[Architecture] = []

    def build_supervisor(_src: Path, arch: Architecture) -> bytes:
        calls.append(arch)
        return _elf_payload(62 if arch == "x86_64" else 183, "supervisor")

    result = build_certified_releases(
        source,
        tmp_path / "out",
        guard_release_directories=guard_releases,
        runtime_roots=runtime_roots,
        build_supervisor=build_supervisor,
    )

    assert sorted(result) == ["aarch64", "x86_64"]
    assert all(release.directory.exists() for release in result.values())
    assert calls == ["x86_64", "x86_64", "aarch64", "aarch64"]


def test_certified_release_refuses_whole_publication_when_any_architecture_drifts(
    tmp_path: Path,
) -> None:
    source, guard_releases, runtime_roots = _multi_arch_source_tree(tmp_path)
    aarch64_calls = 0

    def build_supervisor(_src: Path, arch: Architecture) -> bytes:
        nonlocal aarch64_calls
        if arch == "aarch64":
            aarch64_calls += 1
            return _elf_payload(183, "supervisor-drift" if aarch64_calls == 2 else "supervisor")
        return _elf_payload(62, "supervisor")

    with pytest.raises(ProviderReleaseError, match="deterministic"):
        build_certified_releases(
            source,
            tmp_path / "out",
            guard_release_directories=guard_releases,
            runtime_roots=runtime_roots,
            build_supervisor=build_supervisor,
        )

    assert not list((tmp_path / "out").glob("*/release-manifest.json"))


def test_certified_release_removes_partial_publication_when_second_move_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, guard_releases, runtime_roots = _multi_arch_source_tree(tmp_path)
    output_root = tmp_path / "out"
    original_rename = Path.rename
    published_directories = 0

    def fail_second_directory_publish(path: Path, target: Path) -> Path:
        nonlocal published_directories
        if path.is_dir() and target.parent == output_root and not target.name.startswith("."):
            published_directories += 1
            if published_directories == 2:
                raise OSError("injected second architecture publish failure")
        return original_rename(path, target)

    monkeypatch.setattr(Path, "rename", fail_second_directory_publish)

    with pytest.raises(ProviderReleaseError, match="publication failed"):
        build_certified_releases(
            source,
            output_root,
            guard_release_directories=guard_releases,
            runtime_roots=runtime_roots,
            build_supervisor=lambda _src, arch: _elf_payload(
                62 if arch == "x86_64" else 183,
                "supervisor",
            ),
        )

    assert not [
        item for item in output_root.iterdir() if item.is_dir() and not item.name.startswith(".")
    ]
    preserved = list(output_root.glob(".provider-release-set-conflict.*"))
    assert len(preserved) == 1
    assert list(preserved[0].rglob("release-manifest.json"))


def test_release_assembler_cli_requires_both_architecture_inputs() -> None:
    completed = subprocess.run(
        (
            "/usr/bin/python3",
            "scripts/ops/task_image_builder_provider_release.py",
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
    help_text = completed.stdout.decode("utf-8")
    assert "--architecture" not in help_text
    assert "--guard-release-directory-x86-64" in help_text
    assert "--guard-release-directory-aarch64" in help_text
    assert "--runtime-root-x86-64" in help_text
    assert "--runtime-root-aarch64" in help_text


@pytest.mark.parametrize(
    "mutation",
    ("wrong-gid", "writable-subdirectory", "foreign-directory"),
)
def test_verify_release_directory_rejects_directory_metadata_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    source, guard_release, runtime_root = _source_tree(tmp_path)
    result = build_release(
        source,
        tmp_path / "out",
        "x86_64",
        guard_release_directory=guard_release,
        runtime_root=runtime_root,
        build_supervisor=lambda _src, _arch: _elf_payload(62, "supervisor"),
    )
    expected_gid = os.getegid()
    if mutation == "wrong-gid":
        expected_gid += 1
    elif mutation == "writable-subdirectory":
        (result.directory / "configs").chmod(0o755)
    else:
        result.directory.chmod(0o755)
        foreign = result.directory / "foreign"
        foreign.mkdir()
        foreign.chmod(0o555)
        result.directory.chmod(0o555)

    with pytest.raises(ProviderReleaseError, match=r"metadata|inventory"):
        verify_release_directory(
            result.directory,
            expected_release_sha256=result.release_sha256,
            expected_architecture="x86_64",
            expected_uid=os.geteuid(),
            expected_gid=expected_gid,
        )


@pytest.mark.parametrize(
    "mutation",
    (
        "tampered-guard-member",
        "wrong-guard-member-mode",
        "wrong-guard-directory-basename",
    ),
)
def test_release_rejects_guard_bundle_identity_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    source, guard_release, runtime_root = _source_tree(tmp_path)
    if mutation == "tampered-guard-member":
        target = guard_release / "guard-network-map-schema-v1.json"
        target.chmod(0o644)
        target.write_bytes(
            _canonical(
                {
                    "schema": "loom.task-image-builder-guard-bpf-maps/v1",
                    "maps": [{"name": "drift"}],
                }
            )
        )
        target.chmod(0o444)
    elif mutation == "wrong-guard-member-mode":
        (guard_release / "guard-network-v1.bpf.o").chmod(0o555)
    else:
        renamed = guard_release.with_name("8" * 64)
        guard_release.rename(renamed)
        guard_release = renamed

    with pytest.raises(ProviderReleaseError):
        build_release(
            source,
            tmp_path / "out",
            "x86_64",
            guard_release_directory=guard_release,
            runtime_root=runtime_root,
            build_supervisor=lambda _src, _arch: _elf_payload(62, "supervisor"),
        )


@pytest.mark.parametrize(
    "mutation",
    (
        "runtime-x-crypto",
        "writable-script",
        "script-symlink",
        "missing-guard-member",
        "extra-runtime-member",
        "reordered-configs",
        "self-referential-destination",
    ),
)
def test_release_rejects_unsafe_or_nondeterministic_inputs(
    tmp_path: Path,
    mutation: str,
) -> None:
    source, guard_release, runtime_root = _source_tree(tmp_path)
    spec_path = source / "deploy/task-image-builder/provider-release-v1.json"
    spec = json.loads(spec_path.read_bytes())

    if mutation == "runtime-x-crypto":
        runtime_path = source / "deploy/task-image-builder/rootless-runtime-v2.json"
        runtime = json.loads(runtime_path.read_bytes())
        runtime["toolchain"]["x_crypto"] = "v0.54.0"
        runtime_path.chmod(0o644)
        runtime_path.write_bytes(_canonical(runtime))
        runtime_path.chmod(0o444)
    elif mutation == "writable-script":
        script = source / "scripts/ops/install_task_image_builder_provider_release.py"
        script.chmod(0o775)
    elif mutation == "script-symlink":
        script = source / "scripts/ops/task_image_builder_provider_conformance.py"
        target = script.read_bytes()
        script.unlink()
        shadow = script.with_name("shadow.py")
        _write(shadow, target, 0o555)
        script.symlink_to(shadow.name)
    elif mutation == "missing-guard-member":
        (guard_release / "loom-task-image-builder-guard.pyz").unlink()
    elif mutation == "extra-runtime-member":
        _write(runtime_root / "runtime/extra", b"unexpected\n", 0o555)
    elif mutation == "reordered-configs":
        spec["configs"] = list(reversed(spec["configs"]))
        spec_path.chmod(0o644)
        spec_path.write_bytes(_canonical(spec))
        spec_path.chmod(0o444)
    else:
        spec["scripts"][0]["destination"] = "release-manifest.json"
        spec_path.chmod(0o644)
        spec_path.write_bytes(_canonical(spec))
        spec_path.chmod(0o444)

    with pytest.raises(ProviderReleaseError):
        build_release(
            source,
            tmp_path / "out",
            "x86_64",
            guard_release_directory=guard_release,
            runtime_root=runtime_root,
            build_supervisor=lambda _src, _arch: _elf_payload(62, "supervisor"),
        )
