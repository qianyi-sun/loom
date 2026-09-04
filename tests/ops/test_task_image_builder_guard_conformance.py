"""Evidence tests for offline and live-safe node-guard conformance."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import stat
import struct
import subprocess
from pathlib import Path
from types import SimpleNamespace

import jsonschema
import pytest
from scripts.ops import task_image_builder_guard_conformance as conformance_module
from scripts.ops.install_task_image_builder_guard import (
    InstallContext,
    stage_guard_release,
)
from scripts.ops.task_image_builder_guard_conformance import (
    GuardConformanceError,
    conform,
)
from scripts.ops.task_image_builder_guard_release import build_release

from loom_task_image_builder_guard.bpf import BpfAttachment
from loom_task_image_builder_guard.errors import GuardError

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "docs/evidence/task-image-builder-guard-conformance-v1.schema.json"


def test_documented_conformance_cli_runs_without_ambient_pythonpath() -> None:
    completed = subprocess.run(
        (
            "/usr/bin/python3",
            "scripts/ops/task_image_builder_guard_conformance.py",
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


def _source_copy(tmp_path: Path) -> Path:
    source = tmp_path / "reviewed-source"
    target = source / "deploy/task-image-builder"
    target.parent.mkdir(parents=True)
    shutil.copytree(ROOT / "deploy/task-image-builder", target)
    return source


def _staged(tmp_path: Path) -> tuple[Path, Path, Path]:
    release = build_release(
        ROOT,
        _bpftool(tmp_path / "bpftool"),
        tmp_path / "bundle",
        "x86_64",
    )
    target_root = tmp_path / "target-root"
    target_root.mkdir()
    stage_guard_release(
        release.directory,
        InstallContext(
            root=target_root,
            live=False,
            expected_release_sha256=release.release_sha256,
            architecture="x86_64",
        ),
    )
    installed = (
        target_root
        / "opt/loom-task-image-builder-guard/releases"
        / release.release_sha256
    )
    return installed, target_root, _source_copy(tmp_path)


def test_live_feature_contract_rejects_nonempty_but_incapable_bpftool_output() -> None:
    with pytest.raises(GuardConformanceError, match="feature"):
        conformance_module._validate_bpftool_features(b'{"unrelated":true}\n')


def test_live_feature_contract_accepts_every_required_bpf_surface() -> None:
    common = [
        "bpf_map_lookup_elem",
        "bpf_ktime_get_ns",
        "bpf_spin_lock",
        "bpf_spin_unlock",
    ]
    payload = {
        "syscall_config": {"have_bpf_syscall": True},
        "program_types": {
            "have_cgroup_skb_prog_type": True,
            "have_cgroup_sock_prog_type": True,
            "have_cgroup_sock_addr_prog_type": True,
        },
        "map_types": {
            "have_hash_map_type": True,
            "have_array_map_type": True,
            "have_percpu_array_map_type": True,
        },
        "helpers": {
            "cgroup_skb_available_helpers": common,
            "cgroup_sock_addr_available_helpers": common,
            "cgroup_sock_available_helpers": [
                *common,
                "bpf_map_update_elem",
                "bpf_map_delete_elem",
                "bpf_get_socket_cookie",
            ],
        },
    }

    conformance_module._validate_bpftool_features(
        json.dumps(payload, sort_keys=True).encode("ascii")
    )


def test_live_mount_contract_requires_every_runtime_cgroup_controller() -> None:
    mountinfo = (
        b"20 19 0:20 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
        b"21 19 0:21 / /sys/fs/bpf rw - bpf bpf rw\n"
    )

    required = {"cpu", "cpuset", "io", "memory", "pids"}
    for missing in sorted(required):
        controllers = " ".join(sorted(required - {missing})).encode("ascii")
        with pytest.raises(GuardConformanceError, match="cgroup or bpffs"):
            conformance_module._validate_live_mounts(mountinfo, controllers)

    conformance_module._validate_live_mounts(
        mountinfo,
        " ".join(sorted(required)).encode("ascii"),
    )


def test_live_bpf_link_probe_accepts_only_a_recognized_command() -> None:
    conformance_module._validate_bpf_link_probe(errno.EBADF)

    with pytest.raises(GuardConformanceError, match="link"):
        conformance_module._validate_bpf_link_probe(errno.EINVAL)


def test_live_pinned_link_probe_requires_all_three_exact_policy_scopes() -> None:
    attachment = BpfAttachment(
        Path("/sys/fs/bpf/probe/grant"),
        tuple(range(1, 25)),
        tuple(range(101, 125)),
        tuple(range(201, 219)),
    )

    conformance_module._validate_pinned_link_probe(attachment)

    with pytest.raises(GuardConformanceError, match="link"):
        conformance_module._validate_pinned_link_probe(
            BpfAttachment(
                attachment.pin_path,
                attachment.link_ids[:-1],
                attachment.program_ids,
                attachment.map_ids,
            )
        )


def test_live_pinned_link_probe_surfaces_cleanup_failure_after_partial_attach(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cgroup_base = tmp_path / "cgroup"
    bpffs_base = tmp_path / "bpffs"
    release_root = tmp_path / "release"
    cgroup_base.mkdir()
    bpffs_base.mkdir()
    release_root.mkdir()
    real_path = Path

    def mapped_path(value: object) -> Path:
        if value == "/sys/fs/cgroup":
            return cgroup_base
        if value == "/sys/fs/bpf":
            return bpffs_base
        return real_path(value)  # type: ignore[arg-type]

    opened = iter((101, 102, 103))
    closed: list[int] = []

    class PartialAttachLoader:
        def __init__(self, **values: object) -> None:
            self.bpffs_root = values["bpffs_root"]

        def attach(self, *_args: object) -> BpfAttachment:
            assert isinstance(self.bpffs_root, Path)
            partial = self.bpffs_root / "partial-scope"
            partial.mkdir()
            (partial / "partial-link").write_bytes(b"pinned")
            raise GuardError("bpf_attachment_failed")

    def fail_after_removing_partial_pins(path: Path) -> None:
        partial = path / "partial-scope"
        link = partial / "partial-link"
        assert link.is_file()
        link.unlink()
        partial.rmdir()
        raise GuardConformanceError("cleanup sentinel")

    monkeypatch.setattr(conformance_module, "Path", mapped_path)
    monkeypatch.setattr(conformance_module, "BpfLoader", PartialAttachLoader)
    monkeypatch.setattr(
        conformance_module,
        "_remove_probe_tree",
        fail_after_removing_partial_pins,
    )
    monkeypatch.setattr(conformance_module.os, "open", lambda *_args, **_kwargs: next(opened))
    monkeypatch.setattr(
        conformance_module.os,
        "fstat",
        lambda descriptor: SimpleNamespace(st_uid=0, st_mode=0o40700, st_ino=descriptor),
    )
    monkeypatch.setattr(conformance_module.os, "close", closed.append)
    release = SimpleNamespace(
        directory=release_root,
        members=(
            ("bpftool", 0o555, b"bpftool"),
            ("guard-network-v1.bpf.o", 0o444, b"object"),
            ("guard-network-map-schema-v1.json", 0o444, b"schema"),
        ),
    )

    with pytest.raises(
        GuardConformanceError,
        match="probe and cleanup failed",
    ) as caught:
        conformance_module._probe_pinned_bpf_links(release)

    assert isinstance(caught.value.__cause__, BaseExceptionGroup)
    assert [str(item) for item in caught.value.__cause__.exceptions] == [
        "bpf_attachment_failed",
        "cleanup sentinel",
    ]
    assert closed == [103, 102, 101]
    assert not any(cgroup_base.iterdir())
    assert not any(bpffs_base.iterdir())


def test_offline_conformance_emits_public_typed_nonproduction_evidence(
    tmp_path: Path,
) -> None:
    installed, target_root, source = _staged(tmp_path)

    report = conform(installed, live=False, root=target_root, source_root=source)
    document = report.as_dict()

    jsonschema.validate(document, json.loads(SCHEMA.read_bytes()))
    assert document["schema"] == "loom.task-image-builder-guard-conformance/v1"
    assert document["production_ready"] is False
    assert document["blockers"] == ["phase2_guard_provider_release_missing"]
    assert document["live"] is False
    assert [item["id"] for item in document["checks"]] == [
        "authority_inert",
        "bpf_artifacts",
        "guard_release",
        "inert_runtime",
        "phase1_rollback",
        "provider_policy",
        "stage_receipt",
        "systemd_unit",
        "zipapp_self_check",
    ]
    assert all(item["status"] == "pass" for item in document["checks"])
    serialized = json.dumps(document, sort_keys=True)
    assert "loom_tibp_" not in serialized
    assert "loom_tibs_" not in serialized


def test_conformance_validates_the_content_addressed_unit_not_checkout_state(
    tmp_path: Path,
) -> None:
    installed, target_root, source = _staged(tmp_path)
    checkout_unit = (
        source
        / "deploy/task-image-builder/loom-task-image-builder-node-guard.service"
    )
    checkout_unit.write_text(
        checkout_unit.read_text(encoding="utf-8").replace(
            "NoNewPrivileges=yes",
            "NoNewPrivileges=no",
        ),
        encoding="utf-8",
    )

    report = conform(installed, live=False, root=target_root, source_root=source)

    systemd_check = next(item for item in report.checks if item.id == "systemd_unit")
    assert systemd_check.evidence_sha256 == hashlib.sha256(
        (installed / "loom-task-image-builder-node-guard.service").read_bytes()
    ).hexdigest()


@pytest.mark.parametrize(
    "surface",
    (
        "config",
        "activation",
        "current",
        "socket",
        "pins",
        "installed-unit",
        "vendor-unit",
        "runtime-unit",
        "enabled-unit",
    ),
)
def test_conformance_rejects_any_live_guard_surface(
    tmp_path: Path,
    surface: str,
) -> None:
    installed, target_root, source = _staged(tmp_path)
    paths = {
        "config": target_root / "etc/loom/task-image-builder-guard/config-v1.json",
        "activation": (
            target_root / "etc/loom/task-image-builder-guard/activation-v1.json"
        ),
        "current": target_root / "opt/loom-task-image-builder-guard/current",
        "socket": target_root / "run/loom-task-image-builder-guard/guard.sock",
        "pins": target_root / "sys/fs/bpf/loom-task-image-builder/grant",
        "installed-unit": (
            target_root
            / "etc/systemd/system/loom-task-image-builder-node-guard.service"
        ),
        "vendor-unit": (
            target_root
            / "usr/lib/systemd/system/loom-task-image-builder-node-guard.service"
        ),
        "runtime-unit": (
            target_root
            / "run/systemd/system/loom-task-image-builder-node-guard.service"
        ),
        "enabled-unit": (
            target_root
            / "etc/systemd/system/multi-user.target.wants/"
            "loom-task-image-builder-node-guard.service"
        ),
    }
    path = paths[surface]
    path.parent.mkdir(parents=True, exist_ok=True)
    if surface == "current":
        path.symlink_to(installed)
    elif surface == "enabled-unit":
        path.symlink_to(installed / "loom-task-image-builder-node-guard.service")
    elif surface == "pins":
        path.mkdir()
    else:
        path.write_bytes(b"unexpected-live-surface\n")

    with pytest.raises(GuardConformanceError, match="inert"):
        conform(installed, live=False, root=target_root, source_root=source)


def test_conformance_rejects_a_running_guard_without_filesystem_surfaces(
    tmp_path: Path,
) -> None:
    installed, target_root, source = _staged(tmp_path)
    process = target_root / "proc/42100"
    process.mkdir(parents=True)
    (process / "cmdline").write_bytes(
        b"/usr/bin/python3\0-I\0-B\0"
        b"/opt/loom-task-image-builder-guard/releases/"
        + installed.name.encode("ascii")
        + b"/loom-task-image-builder-guard.pyz\0"
        b"--config\0/etc/loom/task-image-builder-guard/config-v1.json\0"
    )

    with pytest.raises(GuardConformanceError, match="inert"):
        conform(installed, live=False, root=target_root, source_root=source)


@pytest.mark.parametrize(
    "prefix",
    (
        (b"/usr/bin/python3.12", b"-I", b"-B"),
        (b"/usr/local/bin/python", b"-B"),
    ),
)
def test_conformance_finds_a_guard_archive_independent_of_interpreter_argv(
    tmp_path: Path,
    prefix: tuple[bytes, ...],
) -> None:
    installed, target_root, source = _staged(tmp_path)
    process = target_root / "proc/42100"
    process.mkdir(parents=True)
    archive = (
        b"/opt/loom-task-image-builder-guard/releases/"
        + installed.name.encode("ascii")
        + b"/loom-task-image-builder-guard.pyz"
    )
    arguments = (*prefix, archive, b"-I", b"--config", b"/tmp/config.json")
    (process / "cmdline").write_bytes(b"\0".join(arguments) + b"\0")

    with pytest.raises(GuardConformanceError, match="inert"):
        conform(installed, live=False, root=target_root, source_root=source)


@pytest.mark.parametrize(
    "suffix",
    (
        b"//loom-task-image-builder-guard.pyz",
        b"/./loom-task-image-builder-guard.pyz",
    ),
)
def test_conformance_finds_lexical_aliases_of_a_running_guard_archive(
    tmp_path: Path,
    suffix: bytes,
) -> None:
    installed, target_root, source = _staged(tmp_path)
    process = target_root / "proc/42100"
    process.mkdir(parents=True)
    archive = (
        b"/opt/loom-task-image-builder-guard/releases/"
        + installed.name.encode("ascii")
        + suffix
    )
    (process / "cmdline").write_bytes(
        b"/usr/bin/python3.12\0-B\0" + archive + b"\0-I\0"
    )

    with pytest.raises(GuardConformanceError, match="inert"):
        conform(installed, live=False, root=target_root, source_root=source)


@pytest.mark.parametrize(
    "unit_root",
    (
        "etc/systemd/system.control",
        "run/systemd/system.control",
        "run/systemd/transient",
        "run/systemd/generator.early",
        "etc/systemd/system",
        "etc/systemd/system.attached",
        "run/systemd/system",
        "run/systemd/system.attached",
        "run/systemd/generator",
        "usr/local/lib/systemd/system",
        "usr/lib/systemd/system",
        "run/systemd/generator.late",
    ),
)
def test_conformance_rejects_the_guard_in_every_systemd_load_path(
    tmp_path: Path,
    unit_root: str,
) -> None:
    installed, target_root, source = _staged(tmp_path)
    directory = target_root / unit_root
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "loom-task-image-builder-node-guard.service").write_bytes(
        b"unexpected-live-surface\n"
    )

    with pytest.raises(GuardConformanceError, match="inert"):
        conform(installed, live=False, root=target_root, source_root=source)


@pytest.mark.parametrize(
    "mutation",
    (
        "provider-enabled",
        "certification-enabled",
        "certified-node",
        "blocker-removed",
        "phase1-changed",
        "authority-scaled",
        "authority-egress",
        "malformed-provider",
        "provider-extra",
    ),
)
def test_conformance_rejects_provider_phase1_or_authority_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    installed, target_root, source = _staged(tmp_path)
    deploy = source / "deploy/task-image-builder"
    if mutation == "provider-enabled":
        path = deploy / "rootless-provider-v1.toml"
        path.write_text(
            path.read_text(encoding="utf-8").replace("enabled = false", "enabled = true", 1),
            encoding="utf-8",
        )
    elif mutation == "certification-enabled":
        path = deploy / "prerequisites-v1.toml"
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                "production_certification_allowed = false",
                "production_certification_allowed = true",
            ),
            encoding="utf-8",
        )
    elif mutation == "certified-node":
        path = deploy / "prerequisites-v1.toml"
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                "certified_nodes = []", 'certified_nodes = ["unexpected"]'
            ),
            encoding="utf-8",
        )
    elif mutation == "blocker-removed":
        path = deploy / "prerequisites-v1.toml"
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                '["phase2_guard_provider_release_missing"]', "[]"
            ),
            encoding="utf-8",
        )
    elif mutation == "phase1-changed":
        path = deploy / "prerequisites-v1.toml"
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                'reservation = "loom-task-image-builder"', 'reservation = "removed"'
            ),
            encoding="utf-8",
        )
    elif mutation == "authority-scaled":
        path = deploy / "authority-service-v1.yaml"
        path.write_text(
            path.read_text(encoding="utf-8").replace("  replicas: 0", "  replicas: 1"),
            encoding="utf-8",
        )
    elif mutation == "authority-egress":
        path = deploy / "authority-service-v1.yaml"
        path.write_text(
            path.read_text(encoding="utf-8").replace("  egress: []", "  egress:\n    - {}"),
            encoding="utf-8",
        )
    elif mutation == "malformed-provider":
        path = deploy / "rootless-provider-v1.toml"
        path.write_text(
            'schema = "loom.task-image-rootless-provider-policies/v1"\n'
            'policies = ["invalid", "invalid"]\n',
            encoding="utf-8",
        )
    else:
        path = deploy / "rootless-provider-v1.toml"
        path.write_text(
            path.read_text(encoding="utf-8") + "\nunexpected = true\n",
            encoding="utf-8",
        )

    with pytest.raises(GuardConformanceError):
        conform(installed, live=False, root=target_root, source_root=source)


def test_conformance_rejects_installed_release_mode_drift(tmp_path: Path) -> None:
    installed, target_root, source = _staged(tmp_path)
    member = installed / "loom-task-image-builder-guard.pyz"
    member.chmod(0o755)

    with pytest.raises(GuardConformanceError, match="release"):
        conform(installed, live=False, root=target_root, source_root=source)

    assert stat.S_IMODE(member.stat().st_mode) == 0o755
    assert os.geteuid() == member.stat().st_uid


def test_conformance_rejects_a_preserved_release_collision(tmp_path: Path) -> None:
    installed, target_root, source = _staged(tmp_path)
    conflict = installed.parent / f".{installed.name}.conflict.review"
    conflict.mkdir()

    with pytest.raises(GuardConformanceError, match="inert"):
        conform(installed, live=False, root=target_root, source_root=source)
