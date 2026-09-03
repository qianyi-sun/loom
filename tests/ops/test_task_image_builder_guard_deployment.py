"""Executable contract tests for the deliberately inert guard unit template."""

from __future__ import annotations

from pathlib import Path

import pytest
from scripts.ops.task_image_builder_guard_conformance import (
    GuardConformanceError,
    validate_unit,
)

ROOT = Path(__file__).resolve().parents[2]
UNIT = ROOT / "deploy/task-image-builder/loom-task-image-builder-node-guard.service"


def test_unit_template_is_hardened_content_addressed_and_not_installable() -> None:
    evidence_sha256 = validate_unit(UNIT.read_bytes())

    assert len(evidence_sha256) == 64
    assert evidence_sha256 != "0" * 64


@pytest.mark.parametrize(
    ("original", "replacement"),
    (
        (
            "ConditionPathExists=/etc/loom/task-image-builder-guard/activation-v1.json",
            "ConditionPathExists=/etc/loom/task-image-builder-guard/config-v1.json",
        ),
        (
            "/releases/@LOOM_GUARD_RELEASE_SHA256@/loom-task-image-builder-guard.pyz",
            "/current/loom-task-image-builder-guard.pyz",
        ),
        ("User=root", "User=loom-builder"),
        (
            "ReadWritePaths=/sys/fs/cgroup /sys/fs/bpf/loom-task-image-builder",
            "ReadWritePaths=/",
        ),
        ("NoNewPrivileges=yes", "NoNewPrivileges=no"),
        ("MemoryMax=512M", "MemoryMax=infinity"),
        ("WatchdogSec=30s", "WatchdogSec=0"),
    ),
)
def test_unit_validator_rejects_weakened_runtime_boundaries(
    original: str,
    replacement: str,
) -> None:
    payload = UNIT.read_text(encoding="utf-8")
    assert original in payload

    with pytest.raises(GuardConformanceError, match="unit"):
        validate_unit(payload.replace(original, replacement).encode("utf-8"))


def test_unit_has_no_enablement_or_implicit_activation_target() -> None:
    payload = UNIT.read_text(encoding="utf-8")

    assert "[Install]" not in payload
    assert "WantedBy=" not in payload
    assert "Alias=" not in payload
    assert "Also=" not in payload


def test_unit_validator_rejects_an_additional_executable_directive() -> None:
    payload = UNIT.read_bytes() + b"ExecStop=/bin/sh -c /tmp/unreviewed\n"

    with pytest.raises(GuardConformanceError, match="unit"):
        validate_unit(payload)
