"""Evidence tests for offline and live-safe node-guard conformance."""

from __future__ import annotations

import json
import os
import shutil
import stat
import struct
from pathlib import Path

import jsonschema
import pytest
from scripts.ops.install_task_image_builder_guard import (
    InstallContext,
    stage_guard_release,
)
from scripts.ops.task_image_builder_guard_conformance import (
    GuardConformanceError,
    conform,
)
from scripts.ops.task_image_builder_guard_release import build_release

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "docs/evidence/task-image-builder-guard-conformance-v1.schema.json"


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


@pytest.mark.parametrize(
    "surface",
    ("config", "activation", "current", "socket", "pins", "installed-unit"),
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
    }
    path = paths[surface]
    path.parent.mkdir(parents=True, exist_ok=True)
    if surface == "current":
        path.symlink_to(installed)
    elif surface == "pins":
        path.mkdir()
    else:
        path.write_bytes(b"unexpected-live-surface\n")

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
