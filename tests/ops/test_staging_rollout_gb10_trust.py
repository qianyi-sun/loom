from __future__ import annotations

import base64
import json
import os
import stat
import struct
import subprocess
import sys
from pathlib import Path

import pytest
from scripts.ops import staging_rollout_gb10_trust as trust


def _public_key(seed: int, comment: str = "test-only-public-key") -> bytes:
    algorithm = b"ssh-ed25519"
    key_bytes = bytes((seed + offset) % 256 for offset in range(32))
    blob = (
        struct.pack(">I", len(algorithm))
        + algorithm
        + struct.pack(">I", len(key_bytes))
        + key_bytes
    )
    encoded = base64.b64encode(blob).decode("ascii")
    return f"ssh-ed25519 {encoded} {comment}\n".encode("ascii")


def _ssh_config() -> str:
    entries = []
    for number in range(1, 16):
        hostname = "207.35.188.227" if number == 1 else f"192.168.20.{number + 10}"
        entries.extend([f"Host trt-gb10-{number}", f"  HostName {hostname}"])
        if number == 1:
            entries.append("  Port 2221")
        else:
            entries.append("  ProxyJump trt-gb10-1")
        entries.append("")
    entries.extend(
        [
            "Host trt-gb10-*",
            "  User qianyi",
            "  Port 22",
            "  IdentityFile /var/lib/loom-staging-rollout/gb10-deploy-ed25519",
            "  IdentitiesOnly yes",
            "  PubkeyAuthentication yes",
            "  PasswordAuthentication no",
            "",
        ]
    )
    return "\n".join(entries)


def _write_inputs(tmp_path: Path) -> tuple[Path, Path, Path, bytes]:
    config = tmp_path / "ssh_config"
    config.write_text(_ssh_config(), encoding="utf-8")
    config.chmod(0o644)
    public_key = _public_key(1)
    public_path = tmp_path / "gb10-deploy-ed25519.pub"
    public_path.write_bytes(public_key)
    public_path.chmod(0o644)
    identity = tmp_path / "bootstrap-ed25519"
    identity.write_bytes(b"test-only-placeholder-private-input")
    identity.chmod(0o600)
    return config, public_path, identity, public_key


def _run_remote(
    home: Path,
    operation: str,
    public_key: bytes,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [sys.executable, "-c", trust._REMOTE_SCRIPT, operation],
        input=public_key,
        capture_output=True,
        check=False,
        env={**os.environ, "HOME": str(home)},
    )


def test_inventory_expands_exactly_all_15_targets_and_checked_in_user() -> None:
    inventory = trust.parse_ssh_inventory(_ssh_config())

    assert inventory.hosts == tuple(f"trt-gb10-{number}" for number in range(1, 16))
    assert inventory.remote_user == "qianyi"
    assert inventory.hostnames == (
        "207.35.188.227",
        *(f"192.168.20.{number}" for number in range(12, 26)),
    )
    assert inventory.ports == (2221,) + (22,) * 14
    assert inventory.proxy_jumps == (None,) + ("trt-gb10-1",) * 14
    assert inventory.identity_file == trust.SERVICE_PRIVATE_KEY_PATH


def test_checked_in_inventory_contract_is_exact() -> None:
    checked_in = (
        Path(__file__).resolve().parents[2] / "deploy" / "worker-pools" / "gb10" / "ssh_config"
    )
    inventory = trust.parse_ssh_inventory(checked_in.read_text(encoding="utf-8"))

    assert inventory.hosts == tuple(f"trt-gb10-{number}" for number in range(1, 16))
    assert inventory.remote_user == "qianyi"
    assert inventory.hostnames == (
        "207.35.188.227",
        *(f"192.168.20.{number}" for number in range(12, 26)),
    )
    assert inventory.ports == (2221,) + (22,) * 14
    assert inventory.proxy_jumps == (None,) + ("trt-gb10-1",) * 14
    assert inventory.identity_file == trust.SERVICE_PRIVATE_KEY_PATH


@pytest.mark.parametrize(
    "bad_config",
    [
        _ssh_config().replace("Host trt-gb10-15\n", ""),
        _ssh_config().replace("  User qianyi", "  User qianyi\n  User root"),
        _ssh_config().replace("  User qianyi", "  User qianyi\n  Match all"),
        _ssh_config().replace(
            "/var/lib/loom-staging-rollout/gb10-deploy-ed25519",
            "/tmp/operator-key",
        ),
        _ssh_config().replace("  ProxyJump trt-gb10-1\n", "", 1),
        _ssh_config().replace("  Port 2221\n", "", 1),
        _ssh_config().replace("  Port 22\n", "", 1),
        _ssh_config().replace("  Port 22\n", "  Port 2200\n", 1),
        _ssh_config().replace("  User qianyi", "  User hongjian"),
        _ssh_config().replace("HostName 207.35.188.227", "HostName 203.0.113.99"),
        _ssh_config().replace("HostName 207.35.188.227", "HostName dynamic.example"),
    ],
)
def test_inventory_fails_closed_on_unapproved_topology_or_auth(
    bad_config: str,
) -> None:
    with pytest.raises(trust.TrustConfigurationError):
        trust.parse_ssh_inventory(bad_config)


@pytest.mark.parametrize("operation", ["check", "revoke"])
def test_nonbootstrap_commands_reject_target_and_identity_overrides(operation: str) -> None:
    with pytest.raises(SystemExit):
        trust._parser().parse_args([operation, "--host", "trt-gb10-1"])
    with pytest.raises(SystemExit):
        trust._parser().parse_args([operation, "--user", "root"])
    with pytest.raises(SystemExit):
        trust._parser().parse_args([operation, "--ref", "feature"])
    with pytest.raises(SystemExit):
        trust._parser().parse_args([operation, "--bootstrap-identity", "/tmp/key"])


def test_bootstrap_sends_public_key_only_over_stdin_to_every_host(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, identity, public_key = _write_inputs(tmp_path)
    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(argv, **kwargs):
        calls.append((list(argv), dict(kwargs)))
        return subprocess.CompletedProcess(argv, 0, b'{"status":"installed"}\n', b"")

    rc = trust.main(
        ["bootstrap", "--bootstrap-identity", str(identity)],
        run=fake_run,
        ssh_config_path=config,
        public_key_path=public_path,
    )

    assert rc == 0
    assert len(calls) == 15
    for number, (argv, kwargs) in enumerate(calls, start=1):
        assert argv[-2] == f"trt-gb10-{number}"
        assert argv[:3] == ["ssh", "-F", str(config)]
        assert argv[argv.index("-i") + 1] == str(identity)
        assert kwargs["input"] == public_key
        assert kwargs["text"] is False
        assert public_key.decode("ascii").strip() not in " ".join(argv)
    captured = capsys.readouterr()
    assert public_key.decode("ascii").strip() not in captured.out
    assert public_key.decode("ascii").strip() not in captured.err
    report = json.loads(captured.out)
    assert report["ok"] is True
    assert report["remote_user"] == "qianyi"
    assert len(report["hosts"]) == 15


def test_check_continues_after_partial_host_failure_without_echoing_remote_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, public_key = _write_inputs(tmp_path)
    calls: list[list[str]] = []
    sentinel = "DO_NOT_ECHO_REMOTE_OUTPUT"

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        assert kwargs["input"] == public_key
        assert "-i" not in argv
        if argv[-2] == "trt-gb10-7":
            return subprocess.CompletedProcess(argv, 255, b"", sentinel.encode())
        return subprocess.CompletedProcess(argv, 0, b'{"status":"present"}\n', b"")

    rc = trust.main(
        ["check"],
        run=fake_run,
        ssh_config_path=config,
        public_key_path=public_path,
    )

    assert rc == 1
    assert len(calls) == 15
    captured = capsys.readouterr()
    assert sentinel not in captured.out
    assert sentinel not in captured.err
    report = json.loads(captured.out)
    failed = [entry for entry in report["hosts"] if not entry["ok"]]
    assert failed == [{"host": "trt-gb10-7", "ok": False, "status": "remote-failed"}]


def test_revoke_processes_private_hosts_before_jump_host(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, public_key = _write_inputs(tmp_path)
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        assert kwargs["input"] == public_key
        return subprocess.CompletedProcess(argv, 0, b'{"status":"revoked"}\n', b"")

    rc = trust.main(
        ["revoke"],
        run=fake_run,
        ssh_config_path=config,
        public_key_path=public_path,
    )

    assert rc == 0
    expected_hosts = [f"trt-gb10-{number}" for number in range(2, 16)] + ["trt-gb10-1"]
    assert [argv[-2] for argv in calls] == expected_hosts
    report = json.loads(capsys.readouterr().out)
    assert [entry["host"] for entry in report["hosts"]] == expected_hosts


def test_revoke_preserves_jump_host_when_private_host_fails(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    calls: list[list[str]] = []

    def fake_run(argv, **_kwargs):
        calls.append(list(argv))
        if argv[-2] == "trt-gb10-7":
            return subprocess.CompletedProcess(argv, 255, b"", b"")
        return subprocess.CompletedProcess(argv, 0, b'{"status":"revoked"}\n', b"")

    rc = trust.main(
        ["revoke"],
        run=fake_run,
        ssh_config_path=config,
        public_key_path=public_path,
    )

    assert rc == 1
    assert [argv[-2] for argv in calls] == [f"trt-gb10-{number}" for number in range(2, 16)]
    report = json.loads(capsys.readouterr().out)
    assert report["hosts"][-1] == {
        "host": "trt-gb10-1",
        "ok": False,
        "status": "dependency-failed",
    }


def test_remote_bootstrap_is_idempotent_and_preserves_unrelated_lines(tmp_path: Path) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o755)
    target = _public_key(1, "old-comment")
    unrelated = _public_key(90, "unrelated").decode("ascii").strip()
    authorized_keys = ssh_dir / "authorized_keys"
    authorized_keys.write_text(
        f"# preserve this comment\n{unrelated}\n{target.decode('ascii').strip()}\n",
        encoding="utf-8",
    )
    authorized_keys.chmod(0o644)

    first = _run_remote(home, "bootstrap", _public_key(1))
    after_first = authorized_keys.read_text(encoding="utf-8")
    second = _run_remote(home, "bootstrap", _public_key(1))

    assert first.returncode == 0
    assert json.loads(first.stdout)["status"] == "updated"
    assert second.returncode == 0
    assert json.loads(second.stdout)["status"] == "already-present"
    assert authorized_keys.read_text(encoding="utf-8") == after_first
    assert "# preserve this comment" in after_first
    assert unrelated in after_first
    assert after_first.count("loom-staging-rollout") == 1
    assert stat.S_IMODE(ssh_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(authorized_keys.stat().st_mode) == 0o600


def test_remote_bootstrap_inserts_new_key_without_private_key_fixture(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    unrelated = _public_key(80, "unrelated").decode("ascii").strip()
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir()
    authorized_keys = ssh_dir / "authorized_keys"
    authorized_keys.write_text(unrelated + "\n", encoding="utf-8")

    result = _run_remote(home, "bootstrap", _public_key(1))

    assert result.returncode == 0
    assert json.loads(result.stdout)["status"] == "installed"
    rendered = authorized_keys.read_text(encoding="utf-8")
    assert unrelated in rendered
    assert rendered.count("loom-staging-rollout") == 1


def test_remote_check_requires_exact_marked_key_and_modes(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    bootstrap = _run_remote(home, "bootstrap", _public_key(1))
    check = _run_remote(home, "check", _public_key(1))

    assert bootstrap.returncode == 0
    assert check.returncode == 0
    assert json.loads(check.stdout) == {"status": "present"}

    (home / ".ssh" / "authorized_keys").chmod(0o644)
    wrong_mode = _run_remote(home, "check", _public_key(1))
    assert wrong_mode.returncode == 4
    assert json.loads(wrong_mode.stdout) == {"status": "incorrect-authorized-keys-mode"}


def test_remote_bootstrap_rejects_broken_authorized_keys_symlink(tmp_path: Path) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    (ssh_dir / "authorized_keys").symlink_to(ssh_dir / "missing")

    result = _run_remote(home, "bootstrap", _public_key(1))

    assert result.returncode == 2
    assert json.loads(result.stdout) == {"status": "unsafe-authorized-keys"}
    assert (ssh_dir / "authorized_keys").is_symlink()


def test_remote_revoke_removes_only_exact_decoded_fingerprint(tmp_path: Path) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    target = _public_key(1, "some-other-comment").decode("ascii").strip()
    unrelated_key = _public_key(2, "unrelated").decode("ascii").strip()
    lines = ["# operator key", unrelated_key, target, "# keep trailing comment"]
    authorized_keys = ssh_dir / "authorized_keys"
    authorized_keys.write_text("\n".join(lines) + "\n", encoding="utf-8")
    authorized_keys.chmod(0o600)

    result = _run_remote(home, "revoke", _public_key(1))

    assert result.returncode == 0
    assert json.loads(result.stdout) == {"status": "revoked"}
    rendered = authorized_keys.read_text(encoding="utf-8")
    assert target not in rendered
    assert unrelated_key in rendered
    assert "# operator key" in rendered
    assert "# keep trailing comment" in rendered


def test_remote_revoke_refuses_ambiguous_fingerprint_without_changing_file(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    first = _public_key(1, "first").decode("ascii").strip()
    second = _public_key(1, "second").decode("ascii").strip()
    authorized_keys = ssh_dir / "authorized_keys"
    original = first + "\n" + second + "\n"
    authorized_keys.write_text(original, encoding="utf-8")
    authorized_keys.chmod(0o600)

    result = _run_remote(home, "revoke", _public_key(1))

    assert result.returncode == 3
    assert json.loads(result.stdout) == {"status": "ambiguous-fingerprint"}
    assert authorized_keys.read_text(encoding="utf-8") == original


def test_remote_revoke_does_not_match_key_text_inside_quoted_option(tmp_path: Path) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    target = _public_key(1).decode("ascii").split()[1]
    other = _public_key(80, "other-key").decode("ascii").strip()
    authorized_keys = ssh_dir / "authorized_keys"
    original = f'command="echo ssh-ed25519 {target} marker" {other}\n'
    authorized_keys.write_text(original, encoding="utf-8")
    authorized_keys.chmod(0o600)

    result = _run_remote(home, "revoke", _public_key(1))

    assert result.returncode == 0
    assert json.loads(result.stdout) == {"status": "absent"}
    assert authorized_keys.read_text(encoding="utf-8") == original


def test_remote_refuses_stale_comment_marker_bound_to_another_key(tmp_path: Path) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    authorized_keys = ssh_dir / "authorized_keys"
    original = _public_key(80, "loom-staging-rollout").decode("ascii")
    authorized_keys.write_text(original, encoding="utf-8")
    authorized_keys.chmod(0o600)

    result = _run_remote(home, "bootstrap", _public_key(1))

    assert result.returncode == 3
    assert json.loads(result.stdout) == {"status": "ambiguous-marker"}
    assert authorized_keys.read_text(encoding="utf-8") == original


def test_remote_refuses_marker_on_non_ed25519_or_malformed_key(tmp_path: Path) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    authorized_keys = ssh_dir / "authorized_keys"
    original = "ssh-rsa AAAA loom-staging-rollout\n"
    authorized_keys.write_text(original, encoding="utf-8")
    authorized_keys.chmod(0o600)

    result = _run_remote(home, "bootstrap", _public_key(1))

    assert result.returncode == 3
    assert json.loads(result.stdout) == {"status": "ambiguous-marker"}
    assert authorized_keys.read_text(encoding="utf-8") == original
