"""Executable contract tests for the deliberately inert guard unit template."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from scripts.ops.task_image_builder_guard_conformance import (
    GuardConformanceError,
    validate_unit,
)

ROOT = Path(__file__).resolve().parents[2]
UNIT = ROOT / "deploy/task-image-builder/loom-task-image-builder-node-guard.service"


@pytest.mark.parametrize("cluster", ("gb10", "oldlab"))
def test_pinned_authority_address_is_allowed_by_trusted_service_policy(
    cluster: str,
) -> None:
    config = json.loads(
        (ROOT / f"deploy/task-image-builder/guard-config-{cluster}-v1.example.json").read_text(
            encoding="ascii"
        )
    )
    policy = json.loads(
        (
            ROOT
            / f"deploy/task-image-builder/guard-network-policy-{cluster}-v1.example.json"
        ).read_text(encoding="ascii")
    )
    authority = config["authority"]
    endpoint = {
        "address": authority["connect_ip"],
        "port": urlsplit(authority["base_url"]).port or 443,
        "protocol": "tcp",
    }

    assert endpoint in policy["scopes"]["trusted-service"]["ipv4"]


def test_unit_template_is_hardened_content_addressed_and_not_installable() -> None:
    evidence_sha256 = validate_unit(UNIT.read_bytes())

    assert len(evidence_sha256) == 64
    assert evidence_sha256 != "0" * 64


def test_unit_validator_requires_pidfd_stop_and_cross_uid_proc_inspection() -> None:
    desired_capabilities = (
        "CAP_BPF CAP_CHOWN CAP_DAC_OVERRIDE CAP_DAC_READ_SEARCH CAP_FOWNER "
        "CAP_KILL CAP_NET_ADMIN CAP_SYS_ADMIN CAP_SYS_PTRACE"
    )
    desired_filter = (
        "@system-service bpf kcmp memfd_create pidfd_getfd pidfd_open pidfd_send_signal"
    )
    rows = []
    for row in UNIT.read_text(encoding="utf-8").splitlines():
        key, _separator, _value = row.partition("=")
        if key in {"AmbientCapabilities", "CapabilityBoundingSet"}:
            row = f"{key}={desired_capabilities}"
        elif key == "SystemCallFilter":
            row = f"{key}={desired_filter}"
        rows.append(row)
    secured = ("\n".join(rows) + "\n").encode("utf-8")

    assert len(validate_unit(secured)) == 64
    for authority in (
        b"CAP_KILL ",
        b" CAP_SYS_PTRACE",
        b" kcmp",
        b" pidfd_getfd",
        b" pidfd_send_signal",
    ):
        with pytest.raises(GuardConformanceError, match="unit"):
            validate_unit(secured.replace(authority, b"", 1))

    address_families = b"RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK"
    assert address_families in secured
    with pytest.raises(GuardConformanceError, match="unit"):
        validate_unit(secured.replace(b" AF_NETLINK", b"", 1))


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
        ("RuntimeDirectoryMode=0711", "RuntimeDirectoryMode=0750"),
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
