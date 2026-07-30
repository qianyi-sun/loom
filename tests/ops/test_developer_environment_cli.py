from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_environment_cli as cli


def test_cli_exposes_no_identity_or_authority_resource_overrides() -> None:
    help_text = cli._parser().format_help()
    forbidden = (
        "--principal-id",
        "--username",
        "--uid",
        "--gid",
        "--socket",
        "--port",
        "--node",
        "--secret",
        "--state-root",
        "--runtime-root",
    )

    assert all(option not in help_text for option in forbidden)
    assert str(cli.SOCKET_PATH) == ("/run/loom-developer-environment-authority/authority.sock")


def test_cli_documents_compatible_digest_flags_as_worker_config_ids() -> None:
    parser = cli._parser()
    subcommands = next(
        action for action in parser._actions if isinstance(action, argparse._SubParsersAction)
    )

    for command in ("import", "create", "update"):
        help_text = subcommands.choices[command].format_help()
        assert "loom-worker Docker config ID" in help_text
        assert "--amd64-image-digest" in help_text
        assert "--arm64-image-digest" in help_text


def test_import_opens_bundle_but_never_sends_its_path(tmp_path: Path) -> None:
    bundle = tmp_path / "private-name.bundle"
    bundle.write_bytes(b"bundle content")
    amd64_archive = tmp_path / "worker-amd64.tar"
    arm64_archive = tmp_path / "worker-arm64.tar"
    amd64_archive.write_bytes(b"amd64 archive")
    arm64_archive.write_bytes(b"arm64 archive")
    arguments = cli._parser().parse_args(
        [
            "import",
            "--idempotency-key",
            "candidate-import-key-0001",
            "--env-id",
            "denv-12345678",
            "--bundle",
            str(bundle),
            "--candidate-sha",
            "a" * 40,
            "--candidate-tree",
            "b" * 40,
            "--amd64-image-digest",
            "sha256:" + "c" * 64,
            "--arm64-image-digest",
            "sha256:" + "d" * 64,
            "--amd64-image-archive",
            str(amd64_archive),
            "--arm64-image-archive",
            str(arm64_archive),
        ]
    )

    request, descriptors = cli._build_request(arguments)
    assert len(descriptors) == 3
    try:
        assert str(bundle) not in cli._canonical(request).decode("ascii")
        assert set(request) == {
            "schema_version",
            "kind",
            "idempotency_key",
            "env_id",
            "candidate_sha",
            "candidate_tree",
            "bundle_sha256",
            "bundle_size",
            "image_digests",
            "image_archives",
        }
        assert request["bundle_size"] == len(b"bundle content")
        assert request["bundle_sha256"] == hashlib.sha256(b"bundle content").hexdigest()
        assert os.fstat(descriptors[0]).st_ino == bundle.stat().st_ino
        assert os.fstat(descriptors[1]).st_ino == amd64_archive.stat().st_ino
        assert os.fstat(descriptors[2]).st_ino == arm64_archive.stat().st_ino
    finally:
        for descriptor in descriptors:
            os.close(descriptor)


def test_import_rejects_symlink_bundle(tmp_path: Path) -> None:
    bundle = tmp_path / "candidate.bundle"
    bundle.write_bytes(b"bundle content")
    link = tmp_path / "candidate-link.bundle"
    link.symlink_to(bundle)
    amd64_archive = tmp_path / "worker-amd64.tar"
    arm64_archive = tmp_path / "worker-arm64.tar"
    amd64_archive.write_bytes(b"amd64 archive")
    arm64_archive.write_bytes(b"arm64 archive")
    arguments = cli._parser().parse_args(
        [
            "import",
            "--idempotency-key",
            "candidate-import-key-0001",
            "--env-id",
            "denv-12345678",
            "--bundle",
            str(link),
            "--candidate-sha",
            "a" * 40,
            "--candidate-tree",
            "b" * 40,
            "--amd64-image-digest",
            "sha256:" + "c" * 64,
            "--arm64-image-digest",
            "sha256:" + "d" * 64,
            "--amd64-image-archive",
            str(amd64_archive),
            "--arm64-image-archive",
            str(arm64_archive),
        ]
    )

    with pytest.raises(cli.ClientError, match="artifact is unavailable"):
        cli._build_request(arguments)


def test_self_service_commands_never_accept_environment_or_resource_selection(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "candidate.bundle"
    bundle.write_bytes(b"bundle content")
    amd64_archive = tmp_path / "worker-amd64.tar"
    arm64_archive = tmp_path / "worker-arm64.tar"
    amd64_archive.write_bytes(b"amd64 archive")
    arm64_archive.write_bytes(b"arm64 archive")
    candidate_arguments = [
        "--idempotency-key",
        "self-service-key-0001",
        "--bundle",
        str(bundle),
        "--candidate-sha",
        "a" * 40,
        "--candidate-tree",
        "b" * 40,
        "--amd64-image-digest",
        "sha256:" + "c" * 64,
        "--arm64-image-digest",
        "sha256:" + "d" * 64,
        "--amd64-image-archive",
        str(amd64_archive),
        "--arm64-image-archive",
        str(arm64_archive),
    ]
    create, create_fd = cli._build_request(
        cli._parser().parse_args(
            ["create", "--display-name", "Developer", *candidate_arguments],
        )
    )
    update, update_fd = cli._build_request(
        cli._parser().parse_args(["update", *candidate_arguments])
    )
    try:
        assert create["kind"] == cli.CREATE_KIND
        assert create["display_name"] == "Developer"
        assert update["kind"] == cli.UPDATE_KIND
        for request in (create, update):
            assert not {
                "env_id",
                "principal_id",
                "uid",
                "gid",
                "ports",
                "slurm_account",
            }.intersection(request)
    finally:
        assert len(create_fd) == 3
        assert len(update_fd) == 3
        for descriptor in [*create_fd, *update_fd]:
            os.close(descriptor)

    for command, kind in (
        ("check", cli.CHECK_KIND),
        ("rollback", cli.ROLLBACK_KIND),
        ("destroy", cli.DESTROY_KIND),
    ):
        arguments = [command]
        if command != "check":
            arguments.extend(["--idempotency-key", f"{command}-key-0000001"])
        request, descriptor = cli._build_request(cli._parser().parse_args(arguments))
        assert request["kind"] == kind
        assert descriptor == []
        assert "env_id" not in request


def test_cli_sends_no_descriptor_for_non_import_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[dict[str, Any], list[int]]] = []

    def exchange(
        request: dict[str, Any],
        descriptor: list[int],
    ) -> dict[str, Any]:
        observed.append((request, descriptor))
        return {
            "schema_version": 1,
            "kind": f"{request['kind']}.response",
            "status": "succeeded",
            "result": {},
        }

    monkeypatch.setattr(cli, "_exchange", exchange)
    assert cli.main(["snapshot"]) == 0
    assert observed == [
        (
            {
                "schema_version": 1,
                "kind": cli.SNAPSHOT_KIND,
            },
            [],
        )
    ]


def test_systemd_assets_pin_seqpacket_permissions_and_hardening() -> None:
    root = Path(__file__).resolve().parents[2]
    socket_unit = (
        root / "deploy/developer-sandboxes/loom-developer-environment-authority.socket"
    ).read_text(encoding="utf-8")
    service_unit = (
        root / "deploy/developer-sandboxes/loom-developer-environment-authority.service"
    ).read_text(encoding="utf-8")
    tmpfiles = (
        root / "deploy/developer-sandboxes/loom-developer-environment-authority.tmpfiles.conf"
    ).read_text(encoding="utf-8")
    manifest = (
        root / "deploy/developer-sandboxes/developer-environment-authority.install.toml"
    ).read_text(encoding="utf-8")

    assert "ListenSequentialPacket=" in socket_unit
    assert "SocketMode=0660" in socket_unit
    assert "SocketGroup=loom-developers" in socket_unit
    assert "DirectoryMode=0755" in socket_unit
    assert "User=root" in service_unit
    # The fixed node transport deliberately drops to the operator and regains
    # only the installed node-authority command through sudo on the local node.
    assert "NoNewPrivileges=no" in service_unit
    assert "ProtectSystem=strict" in service_unit
    assert "ProtectHome=yes" in service_unit
    assert "PrivateDevices=yes" in service_unit
    assert "RestrictAddressFamilies=AF_UNIX" in service_unit
    assert "CapabilityBoundingSet=" in service_unit
    assert "RuntimeDirectoryMode=0755" in service_unit
    assert "d /run/loom-developer-environment-authority 0755 root root -" in tmpfiles
    assert "developer_environment_registry.py" in manifest
    assert "developer_environment_cli.py" in manifest
