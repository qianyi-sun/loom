from __future__ import annotations

import grp
import hashlib
import json
import os
import pwd
import shutil
import subprocess
import tarfile
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh"


@dataclass(frozen=True)
class Fixture:
    root: Path
    policy: Path
    manifest: Path
    artifacts: Path
    install_base: Path
    passwd_file: Path
    group_file: Path
    subuid_file: Path
    subgid_file: Path
    fake_bin: Path


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _archive(path: Path, files: dict[str, bytes]) -> None:
    source_root = path.parent.parent / f".{path.name}.source"
    for relative, payload in files.items():
        target = source_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    with tarfile.open(path, "w:gz") as archive:
        for relative in sorted(files):
            archive.add(source_root / relative, arcname=relative, recursive=False)


def _write_executable(path: Path, source: str) -> None:
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)


def _fixture(tmp_path: Path) -> Fixture:
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    buildkit_files = {
        "bin/buildctl": b"buildctl-fixture\n",
        "bin/buildkit-runc": b"buildkit-runc-fixture\n",
        "bin/buildkitd": b"buildkitd-fixture\n",
        "bin/buildkit-qemu-aarch64": b"must-not-install\n",
        "bin/buildkit-cni-loopback": b"must-not-install\n",
    }
    rootlesskit_files = {
        "rootlessctl": b"rootlessctl-fixture\n",
        "rootlesskit": b"rootlesskit-fixture\n",
        "rootlesskit-docker-proxy": b"must-not-install\n",
    }
    buildkit = artifacts / "buildkit-fixture.tar.gz"
    rootlesskit = artifacts / "rootlesskit-fixture.tar.gz"
    _archive(buildkit, buildkit_files)
    _archive(rootlesskit, rootlesskit_files)
    slirp = artifacts / "slirp4netns-fixture"
    fuse = artifacts / "fuse-overlayfs-fixture"
    slirp.write_bytes(b"slirp-fixture\n")
    fuse.write_bytes(b"fuse-fixture\n")
    for artifact in (buildkit, rootlesskit, slirp, fuse):
        artifact.chmod(0o644)

    manifest = tmp_path / "rootless-runtime-v1.json"
    manifest.write_text(
        json.dumps(
            {
                "schema": "loom.task-image-builder-rootless-runtime/v1",
                "release": "rootless-runtime-v1",
                "versions": {
                    "buildkit": "test",
                    "rootlesskit": "test",
                    "slirp4netns": "test",
                    "fuse-overlayfs": "test",
                },
                "architectures": {
                    "x86_64": {
                        "platform": "linux-amd64",
                        "artifacts": [
                            {
                                "name": buildkit.name,
                                "url": "https://example.invalid/buildkit",
                                "sha256": _sha(buildkit.read_bytes()),
                            },
                            {
                                "name": rootlesskit.name,
                                "url": "https://example.invalid/rootlesskit",
                                "sha256": _sha(rootlesskit.read_bytes()),
                            },
                            {
                                "name": slirp.name,
                                "url": "https://example.invalid/slirp",
                                "sha256": _sha(slirp.read_bytes()),
                            },
                            {
                                "name": fuse.name,
                                "url": "https://example.invalid/fuse",
                                "sha256": _sha(fuse.read_bytes()),
                            },
                        ],
                        "binaries": {
                            "buildctl": _sha(buildkit_files["bin/buildctl"]),
                            "buildkit-runc": _sha(buildkit_files["bin/buildkit-runc"]),
                            "buildkitd": _sha(buildkit_files["bin/buildkitd"]),
                            "fuse-overlayfs": _sha(fuse.read_bytes()),
                            "rootlessctl": _sha(rootlesskit_files["rootlessctl"]),
                            "rootlesskit": _sha(rootlesskit_files["rootlesskit"]),
                            "slirp4netns": _sha(slirp.read_bytes()),
                        },
                    }
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    policy = tmp_path / "prerequisites-v1.toml"
    policy.write_text(
        f"""
schema = "loom.task-image-builder-prerequisites/v1"
policy_version = "task-image-builder-prerequisites-v1"
production_certification_allowed = false
certified_nodes = []
unconditional_blockers = ["phase2_guard_provider_release_missing"]

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

[runtime]
manifest = "{manifest.name}"

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
        manifest=manifest,
        artifacts=artifacts,
        install_base=tmp_path / "install",
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
) -> subprocess.CompletedProcess[str]:
    owner = pwd.getpwuid(os.getuid()).pw_name
    group = grp.getgrgid(os.getgid()).gr_name
    environment = {
        **os.environ,
        "PATH": f"{fixture.fake_bin}:{os.environ['PATH']}",
        "LOOM_POLICY_PATH": str(fixture.policy),
        "LOOM_RUNTIME_MANIFEST": str(fixture.manifest),
        "LOOM_INSTALL_BASE": str(fixture.install_base),
        "LOOM_PASSWD_FILE": str(fixture.passwd_file),
        "LOOM_GROUP_FILE": str(fixture.group_file),
        "LOOM_SUBUID_FILE": str(fixture.subuid_file),
        "LOOM_SUBGID_FILE": str(fixture.subgid_file),
        "LOOM_INSTALL_OWNER": owner,
        "LOOM_INSTALL_GROUP": group,
        "LOOM_HOST_ARCH": "x86_64",
        "LOOM_SKIP_HOST_CHECKS": "1",
        "LOOM_SLURM_BINDING_MODE": binding_mode,
    }
    return subprocess.run(
        [
            shutil.which("bash") or "bash",
            "-c",
            'source "$1"; "loom_node_$2" oldlab "$3" "$4"',
            "node-installer-test",
            str(INSTALLER),
            action,
            slurm_node,
            str(fixture.artifacts),
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


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
    assert not fixture.install_base.exists()
    assert {path: path.read_bytes() for path in before} == before


def test_invalid_artifact_fails_before_identity_or_release_mutation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    (fixture.artifacts / "slirp4netns-fixture").write_bytes(b"tampered\n")

    result = _run(fixture, "apply")

    assert result.returncode == 1
    assert "artifact digest" in result.stderr
    assert "loom-builder" not in fixture.passwd_file.read_text(encoding="utf-8")
    assert "loom-task-builder" not in fixture.group_file.read_text(encoding="utf-8")
    assert not fixture.install_base.exists()


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
    assert not fixture.install_base.exists()


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
    assert not fixture.install_base.exists()


def test_apply_installs_exact_release_and_is_idempotent(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    first = _run(fixture, "apply")

    assert first.returncode == 0, first.stderr
    release = fixture.install_base / "releases/rootless-runtime-v1/bin"
    assert {path.name for path in release.iterdir()} == {
        "buildctl",
        "buildkit-runc",
        "buildkitd",
        "fuse-overlayfs",
        "rootlessctl",
        "rootlesskit",
        "slirp4netns",
    }
    assert not any("qemu" in path.name or "cni" in path.name for path in release.iterdir())
    assert (fixture.install_base / "current").resolve() == release.parent.resolve()
    assert fixture.subuid_file.read_text(encoding="utf-8") == "loom-builder:3000000:65536\n"
    assert fixture.subgid_file.read_text(encoding="utf-8") == "loom-builder:3000000:65536\n"
    before = {
        path: (path.stat().st_mtime_ns, path.read_bytes())
        for path in (*release.iterdir(), release.parent / "receipt.json")
    }

    second = _run(fixture, "apply")

    assert second.returncode == 0, second.stderr
    assert {path: (path.stat().st_mtime_ns, path.read_bytes()) for path in before} == before


def test_extra_or_symlinked_artifact_is_rejected(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    (fixture.artifacts / "unexpected").write_text("extra", encoding="utf-8")

    extra = _run(fixture, "apply")

    assert extra.returncode == 1
    assert "artifact set" in extra.stderr
    (fixture.artifacts / "unexpected").unlink()
    slirp = fixture.artifacts / "slirp4netns-fixture"
    slirp.unlink()
    slirp.symlink_to(fixture.manifest)

    linked = _run(fixture, "apply")

    assert linked.returncode == 1
    assert "symlink" in linked.stderr


def test_group_or_world_writable_artifact_is_rejected(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    artifact = fixture.artifacts / "slirp4netns-fixture"
    artifact.chmod(0o666)

    result = _run(fixture, "apply")

    assert result.returncode == 1
    assert "unsafe mode" in result.stderr
    assert not fixture.install_base.exists()


def test_installed_release_drift_is_not_silently_repaired(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    assert _run(fixture, "apply").returncode == 0
    binary = fixture.install_base / "releases/rootless-runtime-v1/bin/buildkitd"
    binary.write_bytes(b"drift\n")

    repeated = _run(fixture, "apply")

    assert repeated.returncode == 1
    assert "installed release drift" in repeated.stderr
    assert binary.read_bytes() == b"drift\n"


def test_wrong_cluster_architecture_is_rejected(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    environment_arch = fixture.policy.read_text(encoding="utf-8").replace(
        'architecture = "x86_64"', 'architecture = "aarch64"'
    )
    fixture.policy.write_text(environment_arch, encoding="utf-8")

    result = _run(fixture, "apply")

    assert result.returncode == 1
    assert "architecture" in result.stderr
    assert not fixture.install_base.exists()


def test_direct_cli_rejects_test_path_and_host_check_overrides(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    environment = {
        **os.environ,
        "LOOM_POLICY_PATH": str(fixture.policy),
        "LOOM_RUNTIME_MANIFEST": str(fixture.manifest),
        "LOOM_INSTALL_BASE": str(fixture.install_base),
        "LOOM_SKIP_HOST_CHECKS": "1",
    }

    result = subprocess.run(
        [
            shutil.which("bash") or "bash",
            str(INSTALLER),
            "check",
            "oldlab",
            "node-1",
            str(fixture.artifacts),
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "test overrides" in result.stderr


def test_direct_cli_rejects_old_three_argument_grammar(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    result = subprocess.run(
        [
            shutil.which("bash") or "bash",
            str(INSTALLER),
            "check",
            "oldlab",
            str(fixture.artifacts),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "<slurm-node-name>" in result.stderr
