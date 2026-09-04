from __future__ import annotations

import grp
import hashlib
import json
import os
import pwd
import shutil
import stat
import struct
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest
from scripts.ops.task_image_builder_provider_release import build_release

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh"
_ELF_HEADER = struct.Struct("<16sHHIQQQIHHHHHH")


@dataclass(frozen=True)
class Fixture:
    root: Path
    policy: Path
    host_release: Path
    bundle: Path
    stage_root: Path
    passwd_file: Path
    group_file: Path
    subuid_file: Path
    subgid_file: Path
    fake_bin: Path


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")


def _sha(payload: bytes) -> str:
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
    release_root = root / "guard-release"
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
                    "object_sha256": _sha(_elf_payload(247, "bpf")),
                    "object_size": len(_elf_payload(247, "bpf")),
                    "map_schema_sha256": _sha(
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
            {"path": name, "mode": f"{mode:04o}", "sha256": _sha(payload)}
            for name, (payload, mode) in sorted(files.items())
        ],
    }
    guard_digest = _sha(_canonical(identity))
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


def _provider_bundle(tmp_path: Path) -> tuple[Path, Path]:
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
        name: _sha((runtime_root / "runtime" / name).read_bytes())
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

    spec = {
        "schema": "loom.task-image-builder-provider-release-spec/v1",
        "version": 1,
        "authority_contract_version": 2,
        "provider_install_root": "/opt/loom-task-image-builder-provider/releases",
        "supervisor_relative_path": "bin/loom-task-builder-supervisor",
        "guard_release": {
            "path": "deploy/task-image-builder/guard-release-v1.json",
            "sha256": _sha((deploy / "guard-release-v1.json").read_bytes()),
            "bundle_sha256": {"x86_64": guard_digest, "aarch64": "6" * 64},
        },
        "host_release": {
            "path": "deploy/task-image-builder/host-release-v2.json",
            "sha256": _sha((deploy / "host-release-v2.json").read_bytes()),
        },
        "runtime_manifest": {
            "path": "deploy/task-image-builder/rootless-runtime-v2.json",
            "sha256": _sha((deploy / "rootless-runtime-v2.json").read_bytes()),
        },
        "supervisor": {
            "sources": [
                {
                    "path": "cmd/loom-task-image-builder-supervisor/main.go",
                    "sha256": _sha(supervisor_main),
                }
            ],
            "sha256": {
                "x86_64": _sha(_elf_payload(62, "supervisor")),
                "aarch64": "5" * 64,
            },
        },
        "configs": [
            {
                "path": "deploy/task-image-builder/authority-service-v1.yaml",
                "sha256": _sha(authority),
                "destination": "configs/authority-service-v1.yaml",
                "mode": "0444",
            },
            {
                "path": "deploy/task-image-builder/supervisor-config-gb10-v1.example.json",
                "sha256": _sha(gb10),
                "destination": "configs/supervisor-config-gb10-v1.example.json",
                "mode": "0444",
            },
            {
                "path": "deploy/task-image-builder/supervisor-config-oldlab-v1.example.json",
                "sha256": _sha(oldlab),
                "destination": "configs/supervisor-config-oldlab-v1.example.json",
                "mode": "0444",
            },
        ],
        "scripts": [
            {
                "path": "scripts/ops/install_task_image_builder_provider_release.py",
                "sha256": _sha(installer),
                "destination": "ops/install_task_image_builder_provider_release.py",
                "mode": "0555",
            },
            {
                "path": "scripts/ops/task_image_builder_provider_conformance.py",
                "sha256": _sha(conformance),
                "destination": "ops/task_image_builder_provider_conformance.py",
                "mode": "0555",
            },
        ],
    }
    _write(deploy / "provider-release-v1.json", _canonical(spec), 0o444)
    release = build_release(
        source,
        tmp_path / "out",
        "x86_64",
        guard_release_directory=guard_release,
        runtime_root=runtime_root,
        build_supervisor=lambda _src, _arch: _elf_payload(62, "supervisor"),
    )
    return source / "deploy/task-image-builder/host-release-v2.json", release.directory


def _write_executable(path: Path, source: str) -> None:
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)


def _fixture(tmp_path: Path) -> Fixture:
    host_release, bundle = _provider_bundle(tmp_path)
    stage_root = tmp_path / "stage-root"
    stage_root.mkdir()
    policy = tmp_path / "prerequisites-v1.toml"
    policy.write_text(
        """
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

[[clusters]]
id = "oldlab"
architecture = "x86_64"
controller = "TRT-EAI-OLDLAB-1"
builder_nodes = ["node-1"]
""".lstrip(),
        encoding="utf-8",
    )
    passwd_file = tmp_path / "passwd"
    group_file = tmp_path / "group"
    subuid_file = tmp_path / "subuid"
    subgid_file = tmp_path / "subgid"
    passwd_file.write_text("root:x:0:0:root:/root:/bin/bash\n", encoding="utf-8")
    group_file.write_text("root:x:0:\ndocker:x:988:\n", encoding="utf-8")
    subuid_file.write_text("", encoding="utf-8")
    subgid_file.write_text("", encoding="utf-8")

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _write_executable(
        fake_bin / "groupadd",
        """#!/usr/bin/env bash
set -euo pipefail
printf 'loom-task-builder:x:980:\n' >> "$LOOM_GROUP_FILE"
""",
    )
    _write_executable(
        fake_bin / "useradd",
        """#!/usr/bin/env bash
set -euo pipefail
printf 'loom-builder:x:993:980::/nonexistent:/usr/sbin/nologin\n' >> "$LOOM_PASSWD_FILE"
""",
    )
    _write_executable(
        fake_bin / "scontrol",
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" != "show node node-1 -o" ]]; then exit 2; fi
case "${LOOM_SLURM_BINDING_MODE:-exact}" in
  wrong-node)
    printf 'NodeName=node-2 NodeAddr=192.0.2.10 NodeHostName=physical-node-1 State=IDLE\n'
    ;;
  foreign-hostname)
    printf 'NodeName=node-1 NodeAddr=192.0.2.10 NodeHostName=foreign-node State=IDLE\n'
    ;;
  *)
    printf 'NodeName=node-1 NodeAddr=192.0.2.10 NodeHostName=physical-node-1 State=IDLE\n'
    ;;
esac
""",
    )
    _write_executable(
        fake_bin / "getent",
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" != "ahosts 192.0.2.10" ]]; then exit 2; fi
case "${LOOM_SLURM_BINDING_MODE:-exact}" in
  unresolved) exit 2 ;;
  foreign-address) printf '192.0.2.11 STREAM foreign\n' ;;
  mixed-addresses)
    printf '192.0.2.10 STREAM local\n'
    printf '192.0.2.11 STREAM foreign\n'
    ;;
  *) printf '192.0.2.10 STREAM local\n' ;;
esac
""",
    )
    _write_executable(
        fake_bin / "hostname",
        """#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  -s) printf 'physical-node-1\n' ;;
  -f) printf 'physical-node-1.example.invalid\n' ;;
  -A) printf 'physical-node-1.example.invalid physical-node-1\n' ;;
  *) exit 2 ;;
esac
""",
    )
    _write_executable(
        fake_bin / "ip",
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" != "-o address show scope global" ]]; then exit 2; fi
printf '2: eth0 inet 192.0.2.10/24 brd 192.0.2.255 scope global eth0\n'
""",
    )
    return Fixture(
        root=tmp_path,
        policy=policy,
        host_release=host_release,
        bundle=bundle,
        stage_root=stage_root,
        passwd_file=passwd_file,
        group_file=group_file,
        subuid_file=subuid_file,
        subgid_file=subgid_file,
        fake_bin=fake_bin,
    )


def _run(
    fixture: Fixture,
    action: str,
    *,
    slurm_node: str = "node-1",
    binding_mode: str = "exact",
    host_checks_fail: bool = False,
) -> subprocess.CompletedProcess[str]:
    owner = pwd.getpwuid(os.getuid()).pw_name
    group = grp.getgrgid(os.getgid()).gr_name
    environment = {
        **os.environ,
        "PATH": f"{fixture.fake_bin}:{os.environ['PATH']}",
        "LOOM_POLICY_PATH": str(fixture.policy),
        "LOOM_HOST_RELEASE_MANIFEST": str(fixture.host_release),
        "LOOM_STAGE_ROOT": str(fixture.stage_root),
        "LOOM_PASSWD_FILE": str(fixture.passwd_file),
        "LOOM_GROUP_FILE": str(fixture.group_file),
        "LOOM_SUBUID_FILE": str(fixture.subuid_file),
        "LOOM_SUBGID_FILE": str(fixture.subgid_file),
        "LOOM_INSTALL_OWNER": owner,
        "LOOM_INSTALL_GROUP": group,
        "LOOM_HOST_ARCH": "x86_64",
        "LOOM_SKIP_HOST_CHECKS": "1",
        "LOOM_SLURM_BINDING_MODE": binding_mode,
        "LOOM_TEST_HOST_CHECKS_FAIL": "1" if host_checks_fail else "0",
    }
    return subprocess.run(
        [
            shutil.which("bash") or "bash",
            "-c",
            'source "$1"; '
            'if [[ "$LOOM_TEST_HOST_CHECKS_FAIL" == "1" ]]; then '
            'loom_node_host_checks() { loom_node_error "injected host prerequisite failure"; }; '
            'fi; "loom_node_$2" oldlab "$3" "$4"',
            "node-installer-test",
            str(INSTALLER),
            action,
            slurm_node,
            str(fixture.bundle),
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


def _staged_release_root(fixture: Fixture) -> Path:
    return fixture.stage_root / "opt/loom-task-image-builder-provider/releases"


def _staged_receipt_root(fixture: Fixture) -> Path:
    return fixture.stage_root / "var/lib/loom-task-image-builder-provider/staged"


def test_installer_parses_and_check_mode_never_mutates(tmp_path: Path) -> None:
    parsed = subprocess.run(
        [shutil.which("bash") or "bash", "-n", str(INSTALLER)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert parsed.returncode == 0, parsed.stderr
    fixture = _fixture(tmp_path)
    before = {
        path: path.read_bytes()
        for path in (
            fixture.passwd_file,
            fixture.group_file,
            fixture.subuid_file,
            fixture.subgid_file,
        )
    }

    checked = _run(fixture, "check")

    assert checked.returncode == 1
    assert "identity" in checked.stderr
    assert not _staged_release_root(fixture).exists()
    assert not _staged_receipt_root(fixture).exists()
    assert {path: path.read_bytes() for path in before} == before


def test_invalid_release_fails_before_identity_or_stage_mutation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    member = fixture.bundle / "bin" / "slirp4netns"
    member.chmod(0o755)
    member.write_bytes(b"tampered\n")
    member.chmod(0o555)

    result = _run(fixture, "apply")

    assert result.returncode == 1
    assert "release" in result.stderr
    assert "loom-builder" not in fixture.passwd_file.read_text(encoding="utf-8")
    assert "loom-task-builder" not in fixture.group_file.read_text(encoding="utf-8")
    assert not _staged_release_root(fixture).exists()
    assert not _staged_receipt_root(fixture).exists()


def test_host_prerequisite_failure_precedes_every_mutation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    before = {
        path: path.read_bytes()
        for path in (
            fixture.passwd_file,
            fixture.group_file,
            fixture.subuid_file,
            fixture.subgid_file,
        )
    }

    result = _run(fixture, "apply", host_checks_fail=True)

    assert result.returncode == 1
    assert "host prerequisite failure" in result.stderr
    assert {path: path.read_bytes() for path in before} == before
    assert not _staged_release_root(fixture).exists()
    assert not _staged_receipt_root(fixture).exists()


@pytest.mark.parametrize(
    ("slurm_node", "binding_mode"),
    [
        ("node-2", "exact"),
        ("node-1", "wrong-node"),
        ("node-1", "foreign-hostname"),
        ("node-1", "unresolved"),
        ("node-1", "foreign-address"),
        ("node-1", "mixed-addresses"),
    ],
)
def test_slurm_alias_binding_failure_precedes_host_mutation(
    tmp_path: Path, slurm_node: str, binding_mode: str
) -> None:
    fixture = _fixture(tmp_path)

    result = _run(
        fixture,
        "apply",
        slurm_node=slurm_node,
        binding_mode=binding_mode,
    )

    assert result.returncode == 1
    if slurm_node == "node-2":
        assert "outside the cluster builder inventory" in result.stderr
    else:
        assert "Slurm node identity does not match the local host" in result.stderr
    assert "loom-builder" not in fixture.passwd_file.read_text(encoding="utf-8")
    assert "loom-task-builder" not in fixture.group_file.read_text(encoding="utf-8")
    assert not _staged_release_root(fixture).exists()
    assert not _staged_receipt_root(fixture).exists()


def test_comma_joined_node_names_are_not_one_inventory_member(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.policy.write_text(
        fixture.policy.read_text(encoding="utf-8").replace(
            'builder_nodes = ["node-1"]',
            'builder_nodes = ["node-1", "node-2"]',
        ),
        encoding="utf-8",
    )

    result = _run(fixture, "apply", slurm_node="node-1,node-2")

    assert result.returncode == 1
    assert "outside the cluster builder inventory" in result.stderr
    assert "loom-builder" not in fixture.passwd_file.read_text(encoding="utf-8")


@pytest.mark.parametrize("conflict", ["uid", "gid", "subuid", "docker-group", "other-group"])
def test_identity_and_subid_conflicts_fail_closed(tmp_path: Path, conflict: str) -> None:
    fixture = _fixture(tmp_path)
    if conflict == "uid":
        fixture.passwd_file.write_text(
            fixture.passwd_file.read_text(encoding="utf-8")
            + "foreign:x:993:2000::/nonexistent:/usr/sbin/nologin\n",
            encoding="utf-8",
        )
    elif conflict == "gid":
        fixture.group_file.write_text(
            fixture.group_file.read_text(encoding="utf-8") + "foreign:x:980:\n",
            encoding="utf-8",
        )
    elif conflict == "subuid":
        fixture.subuid_file.write_text("foreign:3000000:65536\n", encoding="utf-8")
    elif conflict == "docker-group":
        fixture.group_file.write_text(
            "root:x:0:\ndocker:x:988:loom-builder\n",
            encoding="utf-8",
        )
    else:
        fixture.group_file.write_text(
            fixture.group_file.read_text(encoding="utf-8") + "researchers:x:2000:loom-builder\n",
            encoding="utf-8",
        )

    result = _run(fixture, "apply")

    assert result.returncode == 1
    assert "conflict" in result.stderr or "supplementary" in result.stderr
    assert not _staged_release_root(fixture).exists()
    assert not _staged_receipt_root(fixture).exists()


def test_apply_stages_exact_release_and_is_idempotent(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    first = _run(fixture, "apply")

    assert first.returncode == 0, first.stderr
    release = (
        fixture.stage_root
        / "opt/loom-task-image-builder-provider/releases"
        / fixture.bundle.name
    )
    assert release.is_dir()
    assert release.name == fixture.bundle.name
    assert not (fixture.stage_root / "opt/loom-task-image-builder-provider/current").exists()
    assert not (release / "current").exists()
    assert stat.S_IMODE(release.stat().st_mode) == 0o555
    assert fixture.subuid_file.read_text(encoding="utf-8") == "loom-builder:3000000:65536\n"
    assert fixture.subgid_file.read_text(encoding="utf-8") == "loom-builder:3000000:65536\n"
    receipt = (
        fixture.stage_root
        / "var/lib/loom-task-image-builder-provider/staged"
        / f"{fixture.bundle.name}.json"
    )
    assert stat.S_IMODE(receipt.stat().st_mode) == 0o600
    assert json.loads(receipt.read_bytes())["installed_path"] == (
        f"/opt/loom-task-image-builder-provider/releases/{fixture.bundle.name}"
    )
    before = {
        path: (path.stat().st_mtime_ns, path.read_bytes())
        for path in sorted(release.rglob("*"))
        if path.is_file()
    }
    before[receipt] = (receipt.stat().st_mtime_ns, receipt.read_bytes())

    second = _run(fixture, "apply")

    assert second.returncode == 0, second.stderr
    assert {path: (path.stat().st_mtime_ns, path.read_bytes()) for path in before} == before


def test_extra_or_symlinked_member_is_rejected(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.bundle.chmod(0o755)
    (fixture.bundle / "unexpected").write_text("extra", encoding="utf-8")

    extra = _run(fixture, "apply")

    assert extra.returncode == 1
    assert "inventory" in extra.stderr or "release" in extra.stderr
    (fixture.bundle / "unexpected").unlink()
    member = fixture.bundle / "bin" / "slirp4netns"
    (fixture.bundle / "bin").chmod(0o755)
    payload = member.read_bytes()
    member.unlink()
    shadow = fixture.bundle / "bin" / "shadow"
    shadow.write_bytes(payload)
    shadow.chmod(0o555)
    member.symlink_to(shadow.name)

    linked = _run(fixture, "apply")

    assert linked.returncode == 1
    assert "release" in linked.stderr


def test_group_or_world_writable_member_is_rejected(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    member = fixture.bundle / "bin" / "slirp4netns"
    member.chmod(0o775)

    result = _run(fixture, "apply")

    assert result.returncode == 1
    assert "release" in result.stderr
    assert not _staged_release_root(fixture).exists()
    assert not _staged_receipt_root(fixture).exists()


def test_staged_release_drift_is_not_silently_repaired(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    assert _run(fixture, "apply").returncode == 0
    member = (
        fixture.stage_root
        / "opt/loom-task-image-builder-provider/releases"
        / fixture.bundle.name
        / "runtime"
        / "buildkitd"
    )
    member.chmod(0o755)
    member.write_bytes(b"drift\n")
    member.chmod(0o555)

    repeated = _run(fixture, "apply")

    assert repeated.returncode == 1
    assert "collision" in repeated.stderr or "release" in repeated.stderr
    conflicts = list(
        (
            fixture.stage_root / "opt/loom-task-image-builder-provider/releases"
        ).glob(f".{fixture.bundle.name}.conflict.*")
    )
    assert len(conflicts) == 1
