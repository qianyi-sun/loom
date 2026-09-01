from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import personal_dev_native_runtime_authority as authority


class Result:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _policy() -> authority.AuthorityPolicy:
    return authority.AuthorityPolicy(
        source_sha="a" * 40,
        source_tree_sha="b" * 40,
        source_base_sha=authority.APPROVED_BASE_SHA,
        wrapper_sha256="c" * 64,
        validator_sha256="d" * 64,
        sudoers_sha256="e" * 64,
        installer_sha256="1" * 64,
        converger_sha256="2" * 64,
        conformance_sha256="3" * 64,
        profile_sha256=authority.RUNTIME_PROFILE_SHA256,
    )


def _common(action: str) -> dict[str, object]:
    return {
        "action": action,
        "request_id": "issue-1280-native",
        "schema": authority.SCHEMA,
        "source_sha": "a" * 40,
        "source_tree_sha": "b" * 40,
    }


def _prepare_header(archive: bytes) -> dict[str, object]:
    return {
        **_common("prepare"),
        "archive_sha512": hashlib.sha512(archive).hexdigest(),
        "archive_size": len(archive),
        "current_agent": (
            "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "1" * 64
        ),
        "current_builder": ("ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "2" * 64),
        "current_revision": "3" * 40,
        "previous_agent": "",
        "previous_builder": "",
        "previous_revision": "",
        "public_store_origin": "https://objects.example.com",
    }


def _stage_header(ca: bytes) -> dict[str, object]:
    return {
        **_common("stage-agent"),
        "agent_instance_id": "11111111-1111-4111-8111-111111111111",
        "ca_size": len(ca),
        "current_agent": (
            "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "1" * 64
        ),
        "current_builder": ("ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "2" * 64),
        "key_id": "native-builder-v1",
        "private_key_size": 32,
        "service_url": authority.MANAGEMENT_ORIGIN,
    }


def _pipe(payload: bytes) -> int:
    read_fd, write_fd = os.pipe()
    os.write(write_fd, payload)
    os.close(write_fd)
    return read_fd


def test_sudoers_exposes_one_fixed_no_argument_no_environment_command() -> None:
    payload = Path(
        "deploy/personal-dev-native-builder/loom-personal-dev-native-runtime-authority.sudoers"
    ).read_text(encoding="ascii")

    assert payload.splitlines() == [
        "qianyi ALL=(root) NOPASSWD:NOSETENV: "
        "/usr/local/libexec/loom-personal-dev-native-runtime-authority"
    ]
    assert "*" not in payload
    assert "SETENV" not in payload.replace("NOSETENV", "")


@pytest.mark.parametrize("argument", ["prepare", "activate", "shell", "--help"])
def test_installed_runtime_has_no_argument_surface(argument: str) -> None:
    with pytest.raises(SystemExit):
        authority._bootstrap_parser().parse_args(argument.split())


def test_policy_binds_every_executable_asset_and_fixed_profile() -> None:
    policy = _policy()
    value = json.loads(policy.payload())

    assert value["source_mode"] == "sealed-cumulative"
    assert value["source_base_sha"] == authority.APPROVED_BASE_SHA
    assert value["profile_sha256"] == authority.RUNTIME_PROFILE_SHA256
    assert {
        "wrapper_sha256",
        "validator_sha256",
        "sudoers_sha256",
        "installer_sha256",
        "converger_sha256",
        "conformance_sha256",
    } <= set(value)

    with pytest.raises(authority.AuthorityError, match="profile identity"):
        replace(policy, profile_sha256="f" * 64)


def test_invoker_requires_exact_sudo_identity_and_no_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(authority.os, "geteuid", lambda: 0)
    monkeypatch.setattr(authority.os, "getegid", lambda: 0)
    monkeypatch.setattr(
        authority.pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(pw_uid=1000, pw_gid=1000),
    )
    valid = {
        "SUDO_COMMAND": str(authority.LIBEXEC),
        "SUDO_GID": "1000",
        "SUDO_UID": "1000",
        "SUDO_USER": authority.OPERATOR,
    }
    authority._validate_invoker(valid)

    for key, invalid in (
        ("SUDO_COMMAND", f"{authority.LIBEXEC} prepare"),
        ("SUDO_USER", "root"),
        ("SUDO_UID", "0"),
        ("SUDO_GID", "0"),
    ):
        drifted = dict(valid)
        drifted[key] = invalid
        with pytest.raises(authority.AuthorityError, match="not approved"):
            authority._validate_invoker(drifted)


def test_request_schema_rejects_unknown_actions_fields_and_stale_source() -> None:
    policy = _policy()
    for value in (
        {**_common("shell")},
        {**_common("activate"), "argv": ["/bin/sh"]},
        {**_common("activate"), "source_sha": "f" * 40},
    ):
        with pytest.raises(authority.AuthorityError):
            authority._validate_request(value, policy)


def test_observation_requests_are_fixed_and_grant_bound() -> None:
    assert authority._validate_request(_common("observe-agent"), _policy()) == ("observe-agent")
    grants = [
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
    ]
    request = {**_common("observe-containers"), "grant_ids": grants}
    assert authority._validate_request(request, _policy()) == "observe-containers"

    for invalid in (
        grants[:1],
        list(reversed(grants)),
        [grants[0], grants[0]],
        [grants[0], "not-a-uuid"],
    ):
        with pytest.raises(authority.AuthorityError, match="grant identities"):
            authority._validate_request(
                {**request, "grant_ids": invalid},
                _policy(),
            )


def test_prepare_request_accepts_only_fixed_archive_and_release_repositories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = b"gvisor-archive"
    header = _prepare_header(archive)
    monkeypatch.setattr(authority, "GVISOR_ARCHIVE_SHA512", header["archive_sha512"])
    assert authority._validate_request(header, _policy()) == "prepare"

    for key, value in (
        ("archive_size", authority._MAX_ARCHIVE_BYTES + 1),
        ("archive_sha512", "0" * 128),
        ("current_agent", "ghcr.io/other/agent@sha256:" + "1" * 64),
        ("current_builder", "ghcr.io/other/builder@sha256:" + "2" * 64),
        ("current_revision", "main"),
    ):
        drifted = dict(header)
        drifted[key] = value
        with pytest.raises(authority.AuthorityError, match="prepare request"):
            authority._validate_request(drifted, _policy())

    for public_store_origin in (
        authority.MANAGEMENT_ORIGIN,
        "http://objects.example.com",
        "https://127.0.0.1",
        "https://objects.example.com/path",
        "https://objects..example.com",
        "https://objects.example.com:8443",
    ):
        drifted = dict(header)
        drifted["public_store_origin"] = public_store_origin
        with pytest.raises(authority.AuthorityError, match="public store"):
            authority._validate_request(drifted, _policy())


def test_previous_release_is_all_or_nothing_and_distinct(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = b"gvisor-archive"
    header = _prepare_header(archive)
    monkeypatch.setattr(authority, "GVISOR_ARCHIVE_SHA512", header["archive_sha512"])

    for previous in (
        (header["current_agent"], "", ""),
        (header["current_agent"], header["current_builder"], header["current_revision"]),
    ):
        drifted = dict(header)
        drifted["previous_agent"] = previous[0]
        drifted["previous_builder"] = previous[1]
        drifted["previous_revision"] = previous[2]
        with pytest.raises(authority.AuthorityError, match="previous release"):
            authority._validate_request(drifted, _policy())


def test_stage_agent_request_fixes_service_and_secret_bounds() -> None:
    ca = b"service-ca"
    header = _stage_header(ca)
    assert authority._validate_request(header, _policy()) == "stage-agent"

    for key, value in (
        ("private_key_size", 31),
        ("ca_size", 0),
        ("ca_size", authority._MAX_CA_BYTES + 1),
        ("service_url", "https://attacker.example"),
        ("key_id", "../../root"),
    ):
        drifted = dict(header)
        drifted[key] = value
        with pytest.raises(authority.AuthorityError, match="agent request"):
            authority._validate_request(drifted, _policy())


def test_header_and_payload_framing_rejects_truncation_and_trailing_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(authority.os, "fchown", lambda *_args: None)
    descriptor = _pipe(b'{"schema":"x"}\nsecret')
    try:
        assert authority._read_header(descriptor) == {"schema": "x"}
        path, _ = authority._stage_payload_file(
            descriptor,
            size=6,
            algorithm="sha256",
            expected_digest=None,
            directory=tmp_path,
            name="payload",
            mode=0o600,
        )
        assert path.read_bytes() == b"secret"
        authority._require_eof(descriptor)
    finally:
        os.close(descriptor)

    descriptor = _pipe(b"short")
    try:
        with pytest.raises(authority.AuthorityError, match="truncated"):
            authority._stage_payload_file(
                descriptor,
                size=6,
                algorithm="sha256",
                expected_digest=None,
                directory=tmp_path,
                name="truncated",
                mode=0o600,
            )
    finally:
        os.close(descriptor)

    descriptor = _pipe(b"short")
    try:
        with pytest.raises(authority.AuthorityError, match="truncated"):
            authority._read_header(descriptor)
    finally:
        os.close(descriptor)

    descriptor = _pipe(b"x")
    try:
        with pytest.raises(authority.AuthorityError, match="trailing"):
            authority._require_eof(descriptor)
    finally:
        os.close(descriptor)


def test_clean_environment_drops_every_caller_controlled_value() -> None:
    assert authority._clean_env() == {
        "HOME": "/root",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONPATH": str(authority.SOURCE_ROOT),
        "PYTHONSAFEPATH": "1",
    }


def test_atomic_set_supports_only_exact_fresh_install_or_upgrade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "authority"
    monkeypatch.setattr(
        authority,
        "_safe_root_directory",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        authority,
        "_optional_regular_root_file",
        lambda path, **_kwargs: path.read_bytes() if path.exists() else None,
    )
    monkeypatch.setattr(authority.os, "fchown", lambda *_args: None)

    assert authority._atomic_set(target, b"v1", 0o600, expected=None)
    assert target.read_bytes() == b"v1"
    assert authority._atomic_set(target, b"v2", 0o600, expected=b"v1")
    assert target.read_bytes() == b"v2"
    assert not authority._atomic_set(target, b"v2", 0o600, expected=b"v2")
    with pytest.raises(authority.AuthorityError, match="asset drifted"):
        authority._atomic_set(target, b"v3", 0o600, expected=b"v1")
    assert target.read_bytes() == b"v2"


def test_prepare_invokes_only_fixed_installer_converger_conformance_and_services(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = b"gvisor-archive"
    header = _prepare_header(archive)
    monkeypatch.setattr(authority, "STATE_ROOT", tmp_path)
    monkeypatch.setattr(authority, "GVISOR_ARCHIVE_SHA512", header["archive_sha512"])
    monkeypatch.setattr(authority.os, "fchown", lambda *_args: None)
    calls: list[tuple[str, ...]] = []

    def run(argv, _env):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        calls.append(command)
        if command[1:3] == ("is-active", "--quiet"):
            return Result(3)
        if command[1] == "is-enabled":
            return Result(1, "disabled\n")
        if command[:4] == ("/usr/sbin/nft", "list", "table", "inet"):
            return Result(1, stderr="No such file or directory\n")
        if command[0] == "/usr/bin/python3":
            return Result(stdout='{"status":"ok"}\n')
        if command[0] == "/bin/bash":
            return Result(
                stdout=(
                    "Runtime=runsc-personal-dev-native architecture=arm64 "
                    "platform=linux/arm64 kvm=/dev/kvm public_https=allowed "
                    "private=denied host_to_provider=denied "
                    "foreign_to_provider=denied cross_network=denied\n"
                )
            )
        return Result()

    descriptor = _pipe(archive)
    try:
        receipts = authority._prepare(header, descriptor, run)
    finally:
        os.close(descriptor)

    assert {
        "runtime-preflight",
        "runtime-install",
        "runtime-verify-staged",
        "release-plan",
        "release-apply",
        "release-verify",
        "two-container-conformance",
    } == set(receipts)
    rendered = "\n".join(" ".join(command) for command in calls)
    assert str(authority.SOURCE_ROOT / authority.INSTALLER_RELATIVE) in rendered
    assert str(authority.SOURCE_ROOT / authority.CONVERGER_RELATIVE) in rendered
    assert str(authority.SOURCE_ROOT / authority.CONFORMANCE_RELATIVE) in rendered
    assert authority.DAEMON_UNIT in rendered
    for forbidden in ("sbatch", "srun", "scontrol update", "loom run", "qemu", "runc"):
        assert forbidden not in rendered.casefold()


def test_stage_agent_keeps_secret_bytes_out_of_argv_and_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_key = b"x" * 32
    assert len(private_key) == 32
    ca = b"service-ca-secret"
    header = _stage_header(ca)
    monkeypatch.setattr(authority, "STATE_ROOT", tmp_path)
    monkeypatch.setattr(authority.os, "fchown", lambda *_args: None)
    calls: list[tuple[str, ...]] = []

    def run(argv, _env):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        calls.append(command)
        if command[1:3] == ("is-active", "--quiet"):
            return Result(3)
        if command[1] == "is-enabled":
            return Result(1, "disabled\n")
        return Result(stdout='{"status":"ok"}\n')

    descriptor = _pipe(private_key + ca)
    try:
        receipt = authority._stage_agent(header, descriptor, run)
    finally:
        os.close(descriptor)

    encoded_calls = repr(calls).encode()
    encoded_receipt = json.dumps(receipt).encode()
    assert private_key not in encoded_calls
    assert private_key not in encoded_receipt
    assert ca not in encoded_calls
    assert ca not in encoded_receipt
    assert not list(tmp_path.iterdir())


def test_activate_failure_recovers_only_fixed_units_and_nft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run(argv, _env):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        calls.append(command)
        if command[0] == "/usr/bin/python3":
            return Result(stdout='{"status":"ok"}\n')
        if command[:3] == ("/usr/bin/systemctl", "start", authority.AGENT_UNIT):
            return Result(1)
        if command[:4] == ("/usr/sbin/nft", "list", "table", "inet"):
            return Result(1, stderr="No such file or directory\n")
        return Result()

    descriptor = _pipe(b"")
    try:
        with pytest.raises(authority.AuthorityError, match="fixed command"):
            authority._activate(descriptor, run)
    finally:
        os.close(descriptor)

    assert ("/usr/bin/systemctl", "stop", authority.AGENT_UNIT) in calls
    assert ("/usr/bin/systemctl", "stop", authority.DAEMON_UNIT) in calls
    assert all("docker.service" not in " ".join(call) for call in calls)


def test_remove_uses_only_byte_verified_installer_and_fixed_host_assets() -> None:
    calls: list[tuple[str, ...]] = []

    def run(argv, _env):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        calls.append(command)
        if command[:4] == ("/usr/sbin/nft", "list", "table", "inet"):
            return Result(1, stderr="No such file or directory\n")
        if command[0] == "/usr/bin/python3":
            return Result(stdout='{"status":"ok"}\n')
        return Result()

    descriptor = _pipe(b"")
    try:
        authority._remove(descriptor, run)
    finally:
        os.close(descriptor)

    assert authority._installer_argv("remove") in calls
    rendered = "\n".join(" ".join(command) for command in calls)
    assert "prune" not in rendered
    assert "/var/run/docker.sock" not in rendered
    assert "slurm" not in rendered.casefold()


def test_observe_agent_returns_only_exact_fixed_service_evidence() -> None:
    calls: list[tuple[str, ...]] = []
    values = {
        "ActiveState": "active\n",
        "FragmentPath": ("/etc/systemd/system/loom-personal-dev-native-builder-agent.service\n"),
        "SubState": "running\n",
    }

    def run(argv, _env):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        calls.append(command)
        property_name = command[-2].removeprefix("--property=")
        return Result(stdout=values[property_name])

    descriptor = _pipe(b"")
    try:
        receipts, evidence = authority._observe_agent(descriptor, run)
    finally:
        os.close(descriptor)

    assert evidence == {
        "active_state": "active",
        "fragment_path": ("/etc/systemd/system/loom-personal-dev-native-builder-agent.service"),
        "sub_state": "running",
    }
    assert set(receipts) == {"agent-active-evidence"}
    assert all(authority.AGENT_UNIT in command for command in calls)


def test_observe_containers_returns_only_two_bound_grant_pairs() -> None:
    grants = [
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
    ]
    container_ids = [character * 64 for character in "abcd"]
    inspected = []
    grant_roles = [(grant_id, role) for grant_id in grants for role in ("buildkit", "client")]
    for index, (grant_id, role) in enumerate(grant_roles):
        inspected.append(
            {
                "Config": {
                    "Labels": {
                        "loom.personal-dev-native-builder.grant-id": grant_id,
                        "loom.personal-dev-native-builder.platform": "linux/arm64",
                        "loom.personal-dev-native-builder.role": role,
                    }
                },
                "HostConfig": {"Runtime": "runsc-personal-dev-native"},
                "Id": container_ids[index],
                "Image": "sha256:" + str(index + 1) * 64,
            }
        )
    calls: list[tuple[str, ...]] = []

    def run(argv, _env):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        calls.append(command)
        if "inspect" in command:
            return Result(stdout=json.dumps(inspected))
        return Result(stdout="\n".join(container_ids) + "\n")

    descriptor = _pipe(b"")
    try:
        receipts, evidence = authority._observe_containers({"grant_ids": grants}, descriptor, run)
    finally:
        os.close(descriptor)

    assert set(receipts) == {"container-evidence"}
    assert [(item["grant_id"], item["role"]) for item in evidence] == [
        (grants[0], "buildkit"),
        (grants[0], "client"),
        (grants[1], "buildkit"),
        (grants[1], "client"),
    ]
    rendered = "\n".join(" ".join(command) for command in calls)
    assert authority.DOCKER_ENDPOINT in rendered
    assert "/var/run/docker.sock" not in rendered


def test_main_never_echoes_sensitive_failure_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret = "do-not-echo-private-key"

    def fail(**_kwargs):  # type: ignore[no-untyped-def]
        raise authority.AuthorityError(secret)

    monkeypatch.setattr(authority, "dispatch", fail)
    assert authority.main([]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "error: personal-dev native runtime authority failed safely\n"
    assert secret not in captured.err
