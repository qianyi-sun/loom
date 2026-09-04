"""Evidence tests for inert Phase 2C provider conformance."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import struct
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops.install_task_image_builder_provider_release import (
    InstallContext,
    stage_provider_release,
)
from scripts.ops.task_image_builder_provider_release import build_release

ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "docs/runbooks/task-image-builder-phase2c-supervisor.md"
_ELF_HEADER = struct.Struct("<16sHHIQQQIHHHHHH")
_REQUIRED_LIVE_CHECK_IDS = (
    "live_cleanup",
    "live_clone3_scratch_cgroup",
    "live_fail_closed_guard_restart",
    "live_native_static_supervisor",
    "live_network_denial",
    "live_no_cache_oci_fixture",
    "live_no_slurm_or_foreign_cgroup",
    "live_process_ancestry",
    "live_project_quota_readback",
    "live_rootlesskit_buildkit_flags",
    "live_runtime_transitive_provenance",
    "live_subuid_subgid",
    "live_supervisor_module_metadata",
)


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _elf_payload(machine: int, label: str, *markers: bytes) -> bytes:
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
        + b"\0".join(markers)
        + b"\n"
    )


def _write(path: Path, payload: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(mode)


def _guard_release(root: Path) -> tuple[Path, str]:
    release_root = root / "guard-release"
    files = {
        "loom-task-image-builder-guard.pyz": (
            _elf_payload(62, "guard", b"/run/loom-task-image-builder-guard/guard.sock"),
            0o555,
        ),
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
        "architecture": "x86_64",
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


def _source_tree(tmp_path: Path) -> tuple[Path, Path]:
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
    authority = (
        b"apiVersion: apps/v1\nkind: Deployment\nspec:\n  replicas: 0\n"
        b"  template:\n    metadata:\n      annotations:\n"
        b"        loom.qianyi.dev/activation: disabled-phase2b1\n"
        b"spec:\n  ingress: []\n  egress: []\n"
    )
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
        _canonical({"guard_release_sha256": guard_digest}),
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

    policy = """
schema = "loom.task-image-rootless-provider-policies/v1"

[[policies]]
schema = "loom.task-image-build-environment-policy/v1"
enabled = false
activation_blockers = [
  "allocation_executor_not_accepted",
  "node_guard_not_accepted",
  "publication_acceptance_not_complete",
  "renewable_registry_credential_broker_not_accepted",
]
slurm_cluster_id = "oldlab"
cpu_arch = "x86_64"
submitting_identity = "loom-builder"
partition = "loom-task-builder"
account = "loom-task-builder"
qos = "loom-task-image-builder-rootless-oldlab"
feature_constraint = "loom_rootless_buildkit"
provider_install_root = "/opt/loom-task-image-builder-provider/releases"
supervisor_relative_path = "bin/loom-task-builder-supervisor"
sbatch_path = "/usr/bin/sbatch"

[policies.resources]
cpus = 8
memory_mib = 32768
pids = 4096
scratch_bytes = 107374182400
scratch_inodes = 1000000
wall_time = "02:00:00"
swap_bytes = 0

[[policies]]
schema = "loom.task-image-build-environment-policy/v1"
enabled = false
activation_blockers = [
  "allocation_executor_not_accepted",
  "node_guard_not_accepted",
  "publication_acceptance_not_complete",
  "renewable_registry_credential_broker_not_accepted",
]
slurm_cluster_id = "gb10"
cpu_arch = "arm64"
submitting_identity = "loom-builder"
partition = "loom-task-builder"
account = "loom-task-builder"
qos = "loom-task-image-builder-rootless-gb10"
feature_constraint = "loom_rootless_buildkit"
provider_install_root = "/opt/loom-task-image-builder-provider/releases"
supervisor_relative_path = "bin/loom-task-builder-supervisor"
sbatch_path = "/usr/bin/sbatch"

[policies.resources]
cpus = 8
memory_mib = 32768
pids = 4096
scratch_bytes = 107374182400
scratch_inodes = 1000000
wall_time = "02:00:00"
swap_bytes = 0
"""
    prerequisites = """
schema = "loom.task-image-builder-prerequisites/v1"
policy_version = "task-image-builder-prerequisites-v1"
production_certification_allowed = false
certified_nodes = []
unconditional_blockers = ["phase2_guard_provider_release_missing"]
host_release_manifest = "host-release-v2.json"

[identity]
user = "loom-builder"
group = "loom-task-builder"
uid = 993
gid = 980
subid_start = 3000000
subid_count = 65536
home = "/nonexistent"
shell = "/usr/sbin/nologin"
forbidden_supplementary_groups = ["docker", "root", "sudo"]

[legacy_guard]
qos = "loom-task-image-builder"
reservation = "loom-task-image-builder"
account = "loom-staging"
user = "loom-rollout"
max_jobs_per_user = 1
max_submit_jobs_per_user = 1
max_wall = "04:00:00"
"""
    _write(deploy / "rootless-provider-v1.toml", policy.lstrip().encode("utf-8"), 0o444)
    _write(deploy / "prerequisites-v1.toml", prerequisites.lstrip().encode("utf-8"), 0o444)

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
                "x86_64": _digest(
                    _elf_payload(
                        62,
                        "supervisor",
                        b"/opt/loom-task-image-builder-provider/releases",
                        b"/run/loom-task-image-builder-guard/guard.sock",
                    )
                ),
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
        build_supervisor=lambda _src, _arch: _elf_payload(
            62,
            "supervisor",
            b"/opt/loom-task-image-builder-provider/releases",
            b"/run/loom-task-image-builder-guard/guard.sock",
        ),
    )
    return source, release.directory


def _staged(tmp_path: Path) -> tuple[Path, Path, Path]:
    source, bundle = _source_tree(tmp_path)
    target_root = tmp_path / "target-root"
    target_root.mkdir()
    stage_provider_release(
        bundle,
        InstallContext(
            root=target_root,
            live=False,
            expected_release_sha256=bundle.name,
            architecture="x86_64",
            source_root=source,
        ),
    )
    installed = (
        target_root
        / "opt/loom-task-image-builder-provider/releases"
        / bundle.name
    )
    return installed, target_root, source


def _rewrite_text(path: Path, text: str) -> None:
    path.chmod(0o644)
    path.write_text(text, encoding="utf-8")
    path.chmod(0o444)


def _forge_installed_release(
    installed: Path,
    target_root: Path,
    member: str,
    payload: bytes,
) -> Path:
    installed.chmod(0o755)
    member_path = installed / member
    member_path.parent.chmod(0o755)
    member_path.chmod(0o644)
    member_path.write_bytes(payload)
    member_path.chmod(0o444)
    member_path.parent.chmod(0o555)
    manifest_path = installed / "release-manifest.json"
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
    manifest_payload = _canonical(manifest)
    manifest_path.write_bytes(manifest_payload)
    manifest_path.chmod(0o444)
    forged = installed.with_name(manifest["release_sha256"])
    installed.rename(forged)
    forged.chmod(0o555)
    receipts = target_root / "var/lib/loom-task-image-builder-provider/staged"
    receipt_path = receipts / f"{installed.name}.json"
    receipt = {
        "activated": False,
        "architecture": manifest["architecture"],
        "installed_path": (
            f"/opt/loom-task-image-builder-provider/releases/{manifest['release_sha256']}"
        ),
        "manifest_sha256": _digest(manifest_payload),
        "production_ready": False,
        "release_sha256": manifest["release_sha256"],
        "schema": "loom.task-image-builder-provider-stage-receipt/v1",
    }
    receipt_path.chmod(0o600)
    receipt_path.unlink()
    _write(receipts / f"{manifest['release_sha256']}.json", _canonical(receipt), 0o600)
    return forged


def test_documented_conformance_cli_runs_without_ambient_pythonpath() -> None:
    completed = subprocess.run(
        (
            "/usr/bin/python3",
            "scripts/ops/task_image_builder_provider_conformance.py",
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


def test_runbook_conformance_flags_match_safe_cli() -> None:
    completed = subprocess.run(
        (
            "/usr/bin/python3",
            "scripts/ops/task_image_builder_provider_conformance.py",
            "--help",
        ),
        cwd=ROOT,
        env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"},
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=10,
    )
    help_text = completed.stdout.decode("utf-8")
    runbook = RUNBOOK.read_text(encoding="utf-8")
    command = re.search(
        r"python3 scripts/ops/task_image_builder_provider_conformance.py \\(?P<body>.*?)```",
        runbook,
        re.DOTALL,
    )

    assert completed.returncode == 0
    assert command is not None
    documented_flags = set(re.findall(r"--[a-z][a-z-]*", command.group("body")))
    actual_flags = set(re.findall(r"--[a-z][a-z-]*", help_text)) - {"--help", "--live"}
    assert documented_flags == actual_flags


def test_offline_conformance_emits_nonproduction_evidence(tmp_path: Path) -> None:
    from scripts.ops.task_image_builder_provider_conformance import conform

    installed, target_root, source = _staged(tmp_path)

    report = conform(installed, live=False, root=target_root, source_root=source)
    document = report.as_dict()

    assert document["schema"] == "loom.task-image-builder-provider-conformance/v1"
    assert document["production_ready"] is False
    assert document["blockers"] == ["phase2_guard_provider_release_missing"]
    assert document["live"] is False
    assert [item["id"] for item in document["checks"]] == [
        "authority_inert",
        "host_release",
        "inert_runtime",
        "provider_policy",
        "provider_release",
        "stage_receipt",
        "supervisor_binary",
    ]
    assert all(item["status"] == "pass" for item in document["checks"])


@pytest.mark.parametrize(
    "surface",
    ("activation", "current", "socket", "pins", "service", "socket-unit"),
)
def test_conformance_rejects_any_live_provider_surface(
    tmp_path: Path,
    surface: str,
) -> None:
    from scripts.ops.task_image_builder_provider_conformance import (
        ProviderConformanceError,
        conform,
    )

    installed, target_root, source = _staged(tmp_path)
    paths = {
        "activation": target_root / "etc/loom/task-image-builder/activation-v1.json",
        "current": target_root / "opt/loom-task-image-builder-provider/current",
        "socket": target_root / "run/loom-task-image-builder-provider/supervisor.sock",
        "pins": target_root / "sys/fs/bpf/loom-task-image-builder/grant",
        "service": target_root / "etc/systemd/system/loom-task-image-builder-provider.service",
        "socket-unit": target_root / "etc/systemd/system/loom-task-image-builder-provider.socket",
    }
    path = paths[surface]
    path.parent.mkdir(parents=True, exist_ok=True)
    if surface == "current":
        path.symlink_to(installed)
    elif surface == "pins":
        path.mkdir()
    else:
        path.write_bytes(b"unexpected-live-surface\n")

    with pytest.raises(ProviderConformanceError, match="inert"):
        conform(installed, live=False, root=target_root, source_root=source)


@pytest.mark.parametrize(
    "mutation",
    (
        "provider-enabled",
        "certification-enabled",
        "certified-node",
        "blocker-removed",
        "provider-root-changed",
        "host-release-changed",
        "authority-scaled",
        "authority-egress",
        "malformed-provider",
        "provider-extra",
    ),
)
def test_conformance_rejects_provider_or_authority_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    from scripts.ops.task_image_builder_provider_conformance import (
        ProviderConformanceError,
        conform,
    )

    installed, target_root, source = _staged(tmp_path)
    deploy = source / "deploy/task-image-builder"
    if mutation == "provider-enabled":
        path = deploy / "rootless-provider-v1.toml"
        _rewrite_text(
            path,
            path.read_text(encoding="utf-8").replace("enabled = false", "enabled = true", 1),
        )
    elif mutation == "certification-enabled":
        path = deploy / "prerequisites-v1.toml"
        _rewrite_text(
            path,
            path.read_text(encoding="utf-8").replace(
                "production_certification_allowed = false",
                "production_certification_allowed = true",
            ),
        )
    elif mutation == "certified-node":
        path = deploy / "prerequisites-v1.toml"
        _rewrite_text(
            path,
            path.read_text(encoding="utf-8").replace(
                "certified_nodes = []", 'certified_nodes = ["unexpected"]'
            ),
        )
    elif mutation == "blocker-removed":
        path = deploy / "prerequisites-v1.toml"
        _rewrite_text(
            path,
            path.read_text(encoding="utf-8").replace(
                '["phase2_guard_provider_release_missing"]', "[]"
            ),
        )
    elif mutation == "provider-root-changed":
        path = deploy / "rootless-provider-v1.toml"
        _rewrite_text(
            path,
            path.read_text(encoding="utf-8").replace(
                "/opt/loom-task-image-builder-provider/releases",
                "/opt/loom-task-image-builder-provider/current",
                1,
            ),
        )
    elif mutation == "host-release-changed":
        path = deploy / "host-release-v2.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["runtime_manifest"] = "rootless-runtime-v1.json"
        _rewrite_text(path, json.dumps(value, sort_keys=True))
    elif mutation == "authority-scaled":
        path = deploy / "authority-service-v1.yaml"
        _rewrite_text(
            path,
            path.read_text(encoding="utf-8").replace("replicas: 0", "replicas: 1"),
        )
    elif mutation == "authority-egress":
        path = deploy / "authority-service-v1.yaml"
        _rewrite_text(
            path,
            path.read_text(encoding="utf-8").replace("egress: []", "egress:\n  - {}"),
        )
    elif mutation == "malformed-provider":
        path = deploy / "rootless-provider-v1.toml"
        _rewrite_text(
            path,
            'schema = "loom.task-image-rootless-provider-policies/v1"\n'
            'policies = ["invalid", "invalid"]\n',
        )
    else:
        path = deploy / "rootless-provider-v1.toml"
        _rewrite_text(
            path,
            path.read_text(encoding="utf-8") + "\nunexpected = true\n",
        )

    with pytest.raises(ProviderConformanceError):
        conform(installed, live=False, root=target_root, source_root=source)


def test_conformance_rejects_installed_release_mode_drift(tmp_path: Path) -> None:
    from scripts.ops.task_image_builder_provider_conformance import (
        ProviderConformanceError,
        conform,
    )

    installed, target_root, source = _staged(tmp_path)
    member = installed / "bin" / "loom-task-builder-supervisor"
    member.chmod(0o755)

    with pytest.raises(ProviderConformanceError, match="release"):
        conform(installed, live=False, root=target_root, source_root=source)

    assert stat.S_IMODE(member.stat().st_mode) == 0o755
    assert os.geteuid() == member.stat().st_uid


def test_conformance_rejects_self_consistent_release_not_bound_to_reviewed_spec(
    tmp_path: Path,
) -> None:
    from scripts.ops.task_image_builder_provider_conformance import (
        ProviderConformanceError,
        conform,
    )

    installed, target_root, source = _staged(tmp_path)
    forged = _forge_installed_release(
        installed,
        target_root,
        "configs/authority-service-v1.yaml",
        b"apiVersion: v1\nkind: ForgedService\n",
    )

    with pytest.raises(ProviderConformanceError, match="reviewed"):
        conform(forged, live=False, root=target_root, source_root=source)


def test_conformance_rejects_a_preserved_release_collision(tmp_path: Path) -> None:
    from scripts.ops.task_image_builder_provider_conformance import (
        ProviderConformanceError,
        conform,
    )

    installed, target_root, source = _staged(tmp_path)
    conflict = installed.parent / f".{installed.name}.conflict.review"
    conflict.mkdir()

    with pytest.raises(ProviderConformanceError, match="inert"):
        conform(installed, live=False, root=target_root, source_root=source)


def test_live_conformance_requires_explicit_empty_scratch_roots(tmp_path: Path) -> None:
    from scripts.ops import task_image_builder_provider_conformance as conformance_module
    from scripts.ops.task_image_builder_provider_conformance import ProviderConformanceError

    scratch = tmp_path / "scratch"
    storage = tmp_path / "storage"
    scratch.mkdir()
    storage.mkdir()
    (scratch / "not-empty").write_text("x", encoding="utf-8")

    with pytest.raises(ProviderConformanceError, match="scratch"):
        conformance_module._validate_live_root(scratch, label="scratch")

    with pytest.raises(ProviderConformanceError, match="storage"):
        conformance_module._validate_live_root(storage / "missing", label="storage")


class _FakeLiveProbeRunner:
    def __init__(self, emitted_ids: tuple[str, ...]) -> None:
        self.emitted_ids = emitted_ids
        self.requests: list[object] = []

    def run(self, request: object) -> tuple[object, ...]:
        from scripts.ops.task_image_builder_provider_conformance import ConformanceCheck

        self.requests.append(request)
        return tuple(
            ConformanceCheck(item, "pass", _digest(item.encode("ascii")))
            for item in self.emitted_ids
        )


def _live_runner_request(tmp_path: Path) -> object:
    from scripts.ops.task_image_builder_provider_conformance import LiveProbeRequest

    source, release = _source_tree(tmp_path)
    scratch = tmp_path / "scratch"
    storage = tmp_path / "storage"
    cgroup = tmp_path / ".loom-task-image-builder-provider-conformance-test"
    scratch.mkdir()
    storage.mkdir()
    cgroup.mkdir()
    return LiveProbeRequest(
        staged_release=release,
        release_sha256=release.name,
        architecture="x86_64",
        source_root=source,
        scratch_root=scratch,
        storage_root=storage,
        scratch_cgroup_root=cgroup,
    )


def test_default_live_probe_runner_is_phase2c_fail_closed(tmp_path: Path) -> None:
    from scripts.ops.task_image_builder_provider_conformance import (
        DefaultLiveProbeRunner,
        ProviderConformanceError,
    )

    with pytest.raises(ProviderConformanceError, match="Phase 2C live conformance is unavailable"):
        DefaultLiveProbeRunner().run(_live_runner_request(tmp_path))


def _patch_live_conformance_verifiers(monkeypatch: pytest.MonkeyPatch) -> Path:
    from scripts.ops import task_image_builder_provider_conformance as conformance_module

    release_sha256 = "a" * 64
    release = SimpleNamespace(
        architecture="x86_64",
        manifest_payload=b"{}\n",
        release_sha256=release_sha256,
    )
    monkeypatch.setattr(conformance_module.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        conformance_module,
        "_release_from_path",
        lambda _path, *, live, source_root: release,
    )
    monkeypatch.setattr(conformance_module, "_verify_authority", lambda _root: _digest(b"authority"))
    monkeypatch.setattr(conformance_module, "_verify_host_release", lambda _root: _digest(b"host"))
    monkeypatch.setattr(conformance_module, "_verify_inert_paths", lambda _root: _digest(b"inert"))
    monkeypatch.setattr(conformance_module, "_verify_provider_policy", lambda _root: _digest(b"policy"))
    monkeypatch.setattr(
        conformance_module,
        "_verify_release_binding",
        lambda _root, _release: _digest(b"release"),
    )
    monkeypatch.setattr(
        conformance_module,
        "_verify_receipt",
        lambda _root, _release: _digest(b"receipt"),
    )
    monkeypatch.setattr(conformance_module, "_verify_supervisor", lambda _release: _digest(b"supervisor"))
    return Path("/opt/loom-task-image-builder-provider/releases") / release_sha256


def test_live_conformance_requires_every_task7_probe_and_stays_nonproduction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts.ops.task_image_builder_provider_conformance import (
        ProviderConformanceError,
        conform,
    )

    staged_release = _patch_live_conformance_verifiers(monkeypatch)
    scratch = tmp_path / "scratch"
    storage = tmp_path / "storage"
    scratch_cgroup = tmp_path / "scratch-cgroup"
    scratch.mkdir()
    storage.mkdir()
    scratch_cgroup.mkdir()
    runner = _FakeLiveProbeRunner(_REQUIRED_LIVE_CHECK_IDS)

    with pytest.raises(ProviderConformanceError, match="Phase 2C live conformance is unavailable"):
        conform(
            staged_release,
            live=True,
            root=Path("/"),
            source_root=ROOT,
            scratch_root=scratch,
            storage_root=storage,
            scratch_cgroup_root=scratch_cgroup,
            live_probe_runner=runner,
        )

    assert runner.requests == []


@pytest.mark.parametrize("missing", _REQUIRED_LIVE_CHECK_IDS)
def test_live_conformance_rejects_missing_task7_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing: str,
) -> None:
    from scripts.ops.task_image_builder_provider_conformance import (
        ProviderConformanceError,
        conform,
    )

    staged_release = _patch_live_conformance_verifiers(monkeypatch)
    scratch = tmp_path / "scratch"
    storage = tmp_path / "storage"
    scratch_cgroup = tmp_path / "scratch-cgroup"
    scratch.mkdir()
    storage.mkdir()
    scratch_cgroup.mkdir()
    runner = _FakeLiveProbeRunner(
        tuple(item for item in _REQUIRED_LIVE_CHECK_IDS if item != missing)
    )

    with pytest.raises(ProviderConformanceError, match="Phase 2C live conformance is unavailable"):
        conform(
            staged_release,
            live=True,
            root=Path("/"),
            source_root=ROOT,
            scratch_root=scratch,
            storage_root=storage,
            scratch_cgroup_root=scratch_cgroup,
            live_probe_runner=runner,
        )


def test_live_conformance_uses_the_default_production_probe_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts.ops import task_image_builder_provider_conformance as conformance_module
    from scripts.ops.task_image_builder_provider_conformance import (
        ConformanceCheck,
        ProviderConformanceError,
        conform,
    )

    staged_release = _patch_live_conformance_verifiers(monkeypatch)
    scratch = tmp_path / "scratch"
    storage = tmp_path / "storage"
    scratch_cgroup = tmp_path / "scratch-cgroup"
    scratch.mkdir()
    storage.mkdir()
    scratch_cgroup.mkdir()

    class RecordingDefaultRunner:
        called = False

        def run(self, request: object) -> tuple[ConformanceCheck, ...]:
            self.called = True
            assert request.scratch_cgroup_root == scratch_cgroup
            return tuple(
                ConformanceCheck(item, "pass", _digest(item.encode("ascii")))
                for item in _REQUIRED_LIVE_CHECK_IDS
            )

    runner = RecordingDefaultRunner()
    monkeypatch.setattr(
        conformance_module,
        "DefaultLiveProbeRunner",
        lambda: runner,
    )

    with pytest.raises(ProviderConformanceError, match="Phase 2C live conformance is unavailable"):
        conform(
            staged_release,
            live=True,
            root=Path("/"),
            source_root=ROOT,
            scratch_root=scratch,
            storage_root=storage,
            scratch_cgroup_root=scratch_cgroup,
        )

    assert runner.called is False


@pytest.mark.parametrize(
    "scratch_cgroup",
    (
        Path("/sys/fs/cgroup/slurm/uid_993/job_1"),
        Path("/sys/fs/cgroup/user.slice/user-993.slice/foreign.scope"),
    ),
)
def test_live_conformance_rejects_slurm_or_foreign_cgroup_targets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scratch_cgroup: Path,
) -> None:
    from scripts.ops.task_image_builder_provider_conformance import (
        ProviderConformanceError,
        conform,
    )

    staged_release = _patch_live_conformance_verifiers(monkeypatch)
    scratch = tmp_path / "scratch"
    storage = tmp_path / "storage"
    scratch.mkdir()
    storage.mkdir()

    with pytest.raises(ProviderConformanceError, match="Phase 2C live conformance is unavailable"):
        conform(
            staged_release,
            live=True,
            root=Path("/"),
            source_root=ROOT,
            scratch_root=scratch,
            storage_root=storage,
            scratch_cgroup_root=scratch_cgroup,
            live_probe_runner=_FakeLiveProbeRunner(_REQUIRED_LIVE_CHECK_IDS),
        )
