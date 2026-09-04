"""Behavioral tests for the inert, content-addressed provider release."""

from __future__ import annotations

import hashlib
import json
import os
import struct
from pathlib import Path

import pytest
from scripts.ops.task_image_builder_provider_release import (
    ProviderReleaseError,
    build_release,
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
    release = root / "guard-release" / ("7" * 64)
    files = {
        "loom-task-image-builder-guard.pyz": (_elf_payload(62, "guard"), 0o555),
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
    for name, (payload, mode) in files.items():
        _write(release / name, payload, mode)
    manifest = {
        "schema": "loom.task-image-builder-guard-bundle/v1",
        "architecture": "x86_64",
        "release_sha256": release.name,
        "files": [
            {"path": name, "mode": f"{mode:04o}", "sha256": _digest(payload)}
            for name, (payload, mode) in sorted(files.items())
        ],
    }
    _write(release / "release-manifest.json", _canonical(manifest), 0o444)
    return release, release.name


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
    assert manifest["guard_release_sha256"] == "7" * 64
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
