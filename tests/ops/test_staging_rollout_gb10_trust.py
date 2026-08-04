from __future__ import annotations

import base64
import json
import os
import shutil
import stat
import struct
import subprocess
import sys
from dataclasses import replace
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
        hostname = (
            "207.35.188.227"
            if number == 1
            else "192.168.20.77"
            if number == 7
            else f"192.168.20.{number + 10}"
        )
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
            "  StrictHostKeyChecking yes",
            "  UserKnownHostsFile /etc/loom/staging-rollout-gb10-known-hosts",
            "  GlobalKnownHostsFile /dev/null",
            "  UpdateHostKeys no",
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


def _ledger_path(tmp_path: Path) -> Path:
    parent = tmp_path / "etc" / "loom"
    parent.mkdir(parents=True, exist_ok=True)
    parent.chmod(0o755)
    return parent / "staging-rollout-gb10-trust-revocation.json"


def _lock_path(tmp_path: Path) -> Path:
    _ledger_path(tmp_path)
    return tmp_path / "etc" / "loom" / "staging-rollout-gb10-trust.lock"


def _known_hosts_path(tmp_path: Path) -> Path:
    path = tmp_path / "etc" / "loom" / "staging-rollout-gb10-known-hosts"
    path.parent.mkdir(parents=True, exist_ok=True)
    entries = []
    for number in range(1, 16):
        target = (
            "[207.35.188.227]:2221,trt-gb10-1"
            if number == 1
            else "192.168.20.77,trt-gb10-7"
            if number == 7
            else f"192.168.20.{number + 10},trt-gb10-{number}"
        )
        fields = _public_key(number).decode("ascii").split()
        entries.append(f"{target} {fields[0]} {fields[1]}")
    path.write_text("\n".join(entries) + "\n", encoding="ascii")
    path.chmod(0o644)
    return path


def _main(
    argv: list[str],
    *,
    tmp_path: Path,
    config: Path,
    public_path: Path,
    run=trust._subprocess_runner,
    known_hosts_path: Path | None = None,
) -> int:
    return trust.main(
        argv,
        run=run,
        ssh_config_path=config,
        known_hosts_path=known_hosts_path or _known_hosts_path(tmp_path),
        public_key_path=public_path,
        ledger_path=_ledger_path(tmp_path),
        lock_path=_lock_path(tmp_path),
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )


def _initialize_ledger(tmp_path: Path, config: Path, public_path: Path) -> None:
    inventory = trust.parse_ssh_inventory(config.read_text(encoding="utf-8"))
    trust._initialize_ledger(
        trust.RevocationLedgerStore(
            path=_ledger_path(tmp_path),
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        ),
        inventory=inventory,
        key_fingerprint=trust._key_fingerprint(public_path.read_bytes()),
    )


def test_cli_requires_exact_candidate_ssh_config(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert trust.main(["check"]) == 2
    assert "exact-candidate GB10 SSH config is required" in capsys.readouterr().err

    legacy = "/opt/loom-staging-runner/repo/deploy/worker-pools/gb10/ssh_config"
    assert trust.main(["--ssh-config", legacy, "check"]) == 2
    assert "not bound to an exact candidate" in capsys.readouterr().err


def _register_ledger_hosts(
    tmp_path: Path,
    config: Path,
    public_path: Path,
    hosts: tuple[str, ...],
) -> None:
    _initialize_ledger(tmp_path, config, public_path)
    store = trust.RevocationLedgerStore(
        path=_ledger_path(tmp_path),
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    ledger = store.load(allow_absent=False)
    assert ledger is not None
    trust._register_revocation_hosts(store, ledger=ledger, hosts=hosts)


def _run_remote(
    home: Path,
    operation: str,
    public_key: bytes,
    *,
    bootstrap_public_key: bytes | None = None,
) -> subprocess.CompletedProcess[bytes]:
    payload = (
        trust._bootstrap_envelope(public_key, bootstrap_public_key)
        if operation == "rotate-bootstrap" and bootstrap_public_key is not None
        else public_key
    )
    return subprocess.run(
        [sys.executable, "-c", trust._REMOTE_SCRIPT, operation],
        input=payload,
        capture_output=True,
        check=False,
        env={**os.environ, "HOME": str(home)},
    )


def test_inventory_expands_exactly_all_15_targets_and_checked_in_user() -> None:
    inventory = trust.parse_ssh_inventory(_ssh_config())

    assert inventory.hosts == tuple(f"trt-gb10-{number}" for number in range(1, 16))
    assert inventory.active_hosts == inventory.hosts
    assert inventory.remote_user == "qianyi"
    assert inventory.hostnames == (
        "207.35.188.227",
        *(
            "192.168.20.77" if number == 7 else f"192.168.20.{number + 10}"
            for number in range(2, 16)
        ),
    )
    assert inventory.ports == (2221,) + (22,) * 14
    assert inventory.proxy_jumps == (None,) + ("trt-gb10-1",) * 14
    assert inventory.identity_file == trust.SERVICE_PRIVATE_KEY_PATH
    assert inventory.known_hosts_file == trust.KNOWN_HOSTS_PATH


def test_physical_topology_digest_is_stable_across_active_policy_transition() -> None:
    inventory = trust.parse_ssh_inventory(_ssh_config())

    changed_policy = replace(
        inventory,
        active_hosts=tuple(host for host in inventory.hosts if host != "trt-gb10-7"),
    )

    assert trust._topology_sha256(changed_policy) == trust._topology_sha256(inventory)
    assert trust._active_policy_sha256(changed_policy) != trust._active_policy_sha256(inventory)


def test_checked_in_inventory_contract_is_exact() -> None:
    checked_in = (
        Path(__file__).resolve().parents[2] / "deploy" / "worker-pools" / "gb10" / "ssh_config"
    )
    inventory = trust.parse_ssh_inventory(checked_in.read_text(encoding="utf-8"))

    assert inventory.hosts == tuple(f"trt-gb10-{number}" for number in range(1, 16))
    assert inventory.active_hosts == inventory.hosts
    assert inventory.remote_user == "qianyi"
    assert inventory.hostnames == (
        "207.35.188.227",
        *(
            "192.168.20.77" if number == 7 else f"192.168.20.{number + 10}"
            for number in range(2, 16)
        ),
    )
    assert inventory.ports == (2221,) + (22,) * 14
    assert inventory.proxy_jumps == (None,) + ("trt-gb10-1",) * 14
    assert inventory.identity_file == trust.SERVICE_PRIVATE_KEY_PATH
    assert inventory.known_hosts_file == trust.KNOWN_HOSTS_PATH


def test_checked_in_known_hosts_authority_is_exact() -> None:
    checked_in = (
        Path(__file__).resolve().parents[2] / "deploy" / "worker-pools" / "gb10" / "known_hosts"
    )
    payload = checked_in.read_bytes()

    trust._validate_known_hosts_authority(payload)

    entries = [
        line for line in payload.decode("ascii").splitlines() if line and not line.startswith("#")
    ]
    assert len(entries) == 15
    assert entries[0].startswith("[207.35.188.227]:2221,trt-gb10-1 ssh-ed25519 ")
    assert entries[6] == (
        "192.168.20.77,trt-gb10-7 ssh-ed25519 "
        "AAAAC3NzaC1lZDI1NTE5AAAAIEcui4I2Lhr2iFgLrvGWYjUEUqUmWPxUHFOkt7fyiOwi"
    )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda payload: payload.replace(b"192.168.20.77,trt-gb10-7", b"192.168.20.99,trt-gb10-7"),
        lambda payload: payload.replace(b" ssh-ed25519 ", b" ssh-rsa ", 1),
        lambda payload: b"\n".join(payload.splitlines()[:-1]) + b"\n",
    ],
)
def test_known_hosts_authority_rejects_drift(mutation) -> None:  # type: ignore[no-untyped-def]
    checked_in = (
        Path(__file__).resolve().parents[2] / "deploy" / "worker-pools" / "gb10" / "known_hosts"
    )

    with pytest.raises(trust.TrustConfigurationError):
        trust._validate_known_hosts_authority(mutation(checked_in.read_bytes()))


def test_known_hosts_authority_requires_fixed_metadata(tmp_path: Path) -> None:
    path = _known_hosts_path(tmp_path)
    path.chmod(0o664)

    with pytest.raises(trust.TrustConfigurationError, match="metadata"):
        trust._read_known_hosts_authority(
            path,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        )


def test_check_fails_before_ssh_when_known_hosts_authority_is_unsafe(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    _initialize_ledger(tmp_path, config, public_path)
    known_hosts = _known_hosts_path(tmp_path)
    known_hosts.chmod(0o666)
    called = False

    def unexpected_runner(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("SSH must not run")

    rc = _main(
        ["check"],
        tmp_path=tmp_path,
        run=unexpected_runner,
        config=config,
        public_path=public_path,
        known_hosts_path=known_hosts,
    )

    assert rc == 2
    assert called is False
    assert "known-hosts authority metadata is unsafe" in capsys.readouterr().err


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
        _ssh_config().replace("StrictHostKeyChecking yes", "StrictHostKeyChecking accept-new"),
        _ssh_config().replace(
            "/etc/loom/staging-rollout-gb10-known-hosts",
            "~/.ssh/known_hosts",
        ),
        _ssh_config().replace(
            "GlobalKnownHostsFile /dev/null", "GlobalKnownHostsFile /etc/ssh/ssh_known_hosts"
        ),
        _ssh_config().replace("UpdateHostKeys no", "UpdateHostKeys yes"),
    ],
)
def test_inventory_fails_closed_on_unapproved_topology_or_auth(
    bad_config: str,
) -> None:
    with pytest.raises(trust.TrustConfigurationError):
        trust.parse_ssh_inventory(bad_config)


@pytest.mark.parametrize(
    "bad_config",
    [
        "Host *\n  ProxyCommand none\n\n" + _ssh_config(),
        _ssh_config().replace(
            "Host trt-gb10-2\n",
            "Host trt-gb10-2\n  ProxyCommand /usr/bin/false blocked\n",
            1,
        ),
        _ssh_config().replace(
            "  User qianyi\n",
            "  User qianyi\n  ProxyCommand none\n",
            1,
        ),
    ],
)
def test_inventory_rejects_every_proxycommand_matching_a_gb10_alias(
    bad_config: str,
) -> None:
    with pytest.raises(trust.TrustConfigurationError, match="ProxyCommand"):
        trust.parse_ssh_inventory(bad_config)


@pytest.mark.skipif(shutil.which("ssh") is None, reason="OpenSSH client is unavailable")
def test_openssh_first_value_proxycommand_would_override_later_proxyjump(
    tmp_path: Path,
) -> None:
    config = tmp_path / "ssh_config"
    config.write_text(
        "Host trt-gb10-2\n  ProxyCommand /usr/bin/false first-value-sentinel\n\n" + _ssh_config(),
        encoding="utf-8",
    )

    completed = subprocess.run(
        ["ssh", "-G", "-F", str(config), "trt-gb10-2"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "proxycommand /usr/bin/false first-value-sentinel" in completed.stdout.lower()
    with pytest.raises(trust.TrustConfigurationError, match="ProxyCommand"):
        trust.parse_ssh_inventory(config.read_text(encoding="utf-8"))


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


def test_initialize_ledger_is_secret_free_bound_and_root_modeled(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, public_key = _write_inputs(tmp_path)

    rc = _main(
        ["initialize-ledger"],
        tmp_path=tmp_path,
        config=config,
        public_path=public_path,
    )

    assert rc == 0
    ledger_path = _ledger_path(tmp_path)
    assert stat.S_IMODE(ledger_path.stat().st_mode) == 0o600
    raw = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert raw["key_fingerprint"] == trust._key_fingerprint(public_key)
    assert raw["revocation_hosts"] == []
    assert len(raw["topology_sha256"]) == 64
    assert len(raw["active_policy_sha256"]) == 64
    assert raw["schema_version"] == 2
    assert public_key.decode("ascii").strip() not in ledger_path.read_text(encoding="utf-8")
    assert list(ledger_path.parent.glob(f".{ledger_path.name}.*")) == []
    report = json.loads(capsys.readouterr().out)
    assert report["ledger_hosts_remaining"] == 0


def test_register_legacy_ledger_records_full_topology_without_ssh(
    tmp_path: Path,
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)

    def reject_ssh(*_args, **_kwargs):
        raise AssertionError("ledger migration must not contact a host")

    rc = _main(
        ["register-legacy-ledger"],
        tmp_path=tmp_path,
        config=config,
        public_path=public_path,
        run=reject_ssh,
    )

    assert rc == 0
    ledger = trust.RevocationLedger.from_bytes(_ledger_path(tmp_path).read_bytes())
    assert ledger.revocation_hosts == tuple(f"trt-gb10-{number}" for number in range(1, 16))


def test_legacy_topology_validation_accepts_transport_policy_hardening_only(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    previous = tmp_path / "previous_ssh_config"
    previous.write_text(
        _ssh_config()
        .replace("  StrictHostKeyChecking yes", "  StrictHostKeyChecking accept-new")
        .replace(
            "  UserKnownHostsFile /etc/loom/staging-rollout-gb10-known-hosts\n",
            "",
        )
        .replace("  GlobalKnownHostsFile /dev/null\n", "")
        .replace("  UpdateHostKeys no\n", ""),
        encoding="utf-8",
    )
    previous.chmod(0o600)

    rc = _main(
        ["validate-legacy-topology", "--previous-config", str(previous)],
        tmp_path=tmp_path,
        config=config,
        public_path=public_path,
    )

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_legacy_topology_validation_rejects_previous_physical_drift(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    previous = tmp_path / "previous_ssh_config"
    previous.write_text(
        _ssh_config().replace("HostName 192.168.20.77", "HostName 192.168.20.99"),
        encoding="utf-8",
    )
    previous.chmod(0o600)

    rc = _main(
        ["validate-legacy-topology", "--previous-config", str(previous)],
        tmp_path=tmp_path,
        config=config,
        public_path=public_path,
    )

    assert rc == 2
    assert "HostName policy is not approved" in capsys.readouterr().err


def test_check_fails_closed_when_ledger_does_not_cover_active_hosts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    _initialize_ledger(tmp_path, config, public_path)
    calls: list[list[str]] = []

    def fake_run(argv, **_kwargs):
        calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, b'{"status":"present"}\n', b"")

    rc = _main(
        ["check"],
        tmp_path=tmp_path,
        config=config,
        public_path=public_path,
        run=fake_run,
    )

    assert rc == 2
    assert calls == []
    assert "missing active hosts" in capsys.readouterr().err


@pytest.mark.parametrize("failure", ["mode", "symlink", "oversized", "schema"])
def test_ledger_metadata_and_schema_fail_closed(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    failure: str,
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    _initialize_ledger(tmp_path, config, public_path)
    ledger_path = _ledger_path(tmp_path)
    if failure == "mode":
        ledger_path.chmod(0o644)
    elif failure == "symlink":
        target = tmp_path / "attacker-ledger"
        target.write_bytes(ledger_path.read_bytes())
        target.chmod(0o600)
        ledger_path.unlink()
        ledger_path.symlink_to(target)
    elif failure == "oversized":
        ledger_path.write_bytes(b"x" * (trust._LEDGER_MAX_BYTES + 1))
        ledger_path.chmod(0o600)
    else:
        raw = json.loads(ledger_path.read_text(encoding="utf-8"))
        raw["unexpected"] = True
        ledger_path.write_text(json.dumps(raw) + "\n", encoding="utf-8")
        ledger_path.chmod(0o600)

    rc = _main(
        ["initialize-ledger"],
        tmp_path=tmp_path,
        config=config,
        public_path=public_path,
    )

    assert rc == 2
    assert "revocation ledger" in capsys.readouterr().err


def test_ledger_rejects_key_and_topology_binding_drift(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    _initialize_ledger(tmp_path, config, public_path)
    public_path.write_bytes(_public_key(2))
    public_path.chmod(0o644)

    key_rc = _main(
        ["initialize-ledger"],
        tmp_path=tmp_path,
        config=config,
        public_path=public_path,
    )

    assert key_rc == 2
    assert "key binding" in capsys.readouterr().err

    public_path.write_bytes(_public_key(1))
    raw = json.loads(_ledger_path(tmp_path).read_text(encoding="utf-8"))
    raw["topology_sha256"] = "0" * 64
    _ledger_path(tmp_path).write_text(json.dumps(raw) + "\n", encoding="utf-8")
    _ledger_path(tmp_path).chmod(0o600)
    topology_rc = _main(
        ["initialize-ledger"],
        tmp_path=tmp_path,
        config=config,
        public_path=public_path,
    )

    assert topology_rc == 2
    assert "topology binding" in capsys.readouterr().err


def test_explicit_active14_to_active15_migration_preserves_revocation_hosts(
    tmp_path: Path,
) -> None:
    config, _public_path, _identity, public_key = _write_inputs(tmp_path)
    inventory15 = trust.parse_ssh_inventory(config.read_text(encoding="utf-8"))
    inventory14 = replace(
        inventory15,
        active_hosts=tuple(host for host in inventory15.hosts if host != "trt-gb10-7"),
    )
    store = trust.RevocationLedgerStore(
        path=_ledger_path(tmp_path),
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    ledger = trust._initialize_ledger(
        store,
        inventory=inventory14,
        key_fingerprint=trust._key_fingerprint(public_key),
    )
    ledger = trust._register_revocation_hosts(
        store,
        ledger=ledger,
        hosts=inventory14.active_hosts,
    )
    with pytest.raises(trust.TrustConfigurationError, match="active-policy binding"):
        trust._validate_ledger_binding(
            ledger,
            inventory=inventory15,
            key_fingerprint=trust._key_fingerprint(public_key),
        )

    migrated = trust._migrate_active_policy(
        store,
        ledger=ledger,
        inventory=inventory15,
        key_fingerprint=trust._key_fingerprint(public_key),
    )

    assert migrated.revocation_hosts == inventory14.active_hosts
    assert migrated.topology_sha256 == ledger.topology_sha256
    assert migrated.active_policy_sha256 == trust._active_policy_sha256(inventory15)
    bootstrapped = trust._register_revocation_hosts(
        store,
        ledger=migrated,
        hosts=inventory15.active_hosts,
    )
    assert bootstrapped.revocation_hosts == inventory15.hosts


def test_exact_host7_endpoint_migration_updates_topology_and_active_policy(
    tmp_path: Path,
) -> None:
    config, _public_path, _identity, public_key = _write_inputs(tmp_path)
    inventory15 = trust.parse_ssh_inventory(config.read_text(encoding="utf-8"))
    old_hostnames = tuple(
        "192.168.20.17" if host == "trt-gb10-7" else hostname
        for host, hostname in zip(inventory15.hosts, inventory15.hostnames, strict=True)
    )
    inventory14 = replace(
        inventory15,
        active_hosts=tuple(host for host in inventory15.hosts if host != "trt-gb10-7"),
        hostnames=old_hostnames,
    )
    store = trust.RevocationLedgerStore(
        path=_ledger_path(tmp_path),
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    ledger = trust._initialize_ledger(
        store,
        inventory=inventory14,
        key_fingerprint=trust._key_fingerprint(public_key),
    )
    ledger = trust._register_revocation_hosts(
        store,
        ledger=ledger,
        hosts=inventory14.active_hosts,
    )

    migrated = trust._migrate_active_policy(
        store,
        ledger=ledger,
        inventory=inventory15,
        key_fingerprint=trust._key_fingerprint(public_key),
    )

    assert migrated.topology_sha256 == trust._topology_sha256(inventory15)
    assert migrated.active_policy_sha256 == trust._active_policy_sha256(inventory15)
    assert migrated.revocation_hosts == inventory14.active_hosts

    unapproved = replace(
        inventory15,
        hostnames=tuple(
            "192.168.20.78" if host == "trt-gb10-7" else hostname
            for host, hostname in zip(inventory15.hosts, inventory15.hostnames, strict=True)
        ),
    )
    with pytest.raises(trust.TrustConfigurationError, match="topology binding"):
        trust._migrate_active_policy(
            store,
            ledger=ledger,
            inventory=unapproved,
            key_fingerprint=trust._key_fingerprint(public_key),
        )


def test_active_policy_migration_requires_inherited_installer_lock(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    _initialize_ledger(tmp_path, config, public_path)

    standalone_rc = _main(
        ["migrate-active-policy"],
        tmp_path=tmp_path,
        config=config,
        public_path=public_path,
    )

    assert standalone_rc == 2
    assert "merged installer authority" in capsys.readouterr().err

    lock_path = _lock_path(tmp_path)
    with trust._trust_lifecycle_lock(
        lock_path,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    ) as lock_fd:
        monkeypatch.setenv(trust._INHERITED_LOCK_FD_ENV, str(lock_fd))
        installer_rc = trust.main(
            ["migrate-active-policy"],
            ssh_config_path=config,
            known_hosts_path=_known_hosts_path(tmp_path),
            public_key_path=public_path,
            ledger_path=_ledger_path(tmp_path),
            lock_path=lock_path,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        )

    assert installer_rc == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_installer_reconciles_verified_new_active_host_into_revocation_ledger(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    inventory = trust.parse_ssh_inventory(config.read_text(encoding="utf-8"))
    _register_ledger_hosts(
        tmp_path,
        config,
        public_path,
        tuple(host for host in inventory.hosts if host != "trt-gb10-7"),
    )
    calls: list[str] = []

    def fake_run(argv, **_kwargs):
        calls.append(argv[-2])
        return subprocess.CompletedProcess(argv, 0, b'{"status":"present"}\n', b"")

    standalone_rc = _main(
        ["reconcile-active-hosts"],
        tmp_path=tmp_path,
        config=config,
        public_path=public_path,
        run=fake_run,
    )
    assert standalone_rc == 2
    assert calls == []
    assert "merged installer authority" in capsys.readouterr().err

    lock_path = _lock_path(tmp_path)
    with trust._trust_lifecycle_lock(
        lock_path,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    ) as lock_fd:
        monkeypatch.setenv(trust._INHERITED_LOCK_FD_ENV, str(lock_fd))
        installer_rc = trust.main(
            ["reconcile-active-hosts"],
            run=fake_run,
            ssh_config_path=config,
            known_hosts_path=_known_hosts_path(tmp_path),
            public_key_path=public_path,
            ledger_path=_ledger_path(tmp_path),
            lock_path=lock_path,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        )

    assert installer_rc == 0
    assert calls == list(inventory.active_hosts)
    report = json.loads(capsys.readouterr().out)
    assert report["ok"] is True
    assert report["ledger_hosts_remaining"] == 15
    ledger = trust.RevocationLedger.from_bytes(_ledger_path(tmp_path).read_bytes())
    assert ledger.revocation_hosts == inventory.hosts


def test_installer_does_not_record_new_active_host_when_service_check_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    inventory = trust.parse_ssh_inventory(config.read_text(encoding="utf-8"))
    prior_hosts = tuple(host for host in inventory.hosts if host != "trt-gb10-7")
    _register_ledger_hosts(tmp_path, config, public_path, prior_hosts)

    def fake_run(argv, **_kwargs):
        if argv[-2] == "trt-gb10-7":
            return subprocess.CompletedProcess(argv, 255, b"", b"transport failed")
        return subprocess.CompletedProcess(argv, 0, b'{"status":"present"}\n', b"")

    lock_path = _lock_path(tmp_path)
    with trust._trust_lifecycle_lock(
        lock_path,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    ) as lock_fd:
        monkeypatch.setenv(trust._INHERITED_LOCK_FD_ENV, str(lock_fd))
        installer_rc = trust.main(
            ["reconcile-active-hosts"],
            run=fake_run,
            ssh_config_path=config,
            known_hosts_path=_known_hosts_path(tmp_path),
            public_key_path=public_path,
            ledger_path=_ledger_path(tmp_path),
            lock_path=lock_path,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        )

    assert installer_rc == 1
    ledger = trust.RevocationLedger.from_bytes(_ledger_path(tmp_path).read_bytes())
    assert ledger.revocation_hosts == prior_hosts


def test_lifecycle_lock_rejects_unsafe_metadata_before_state_access(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    lock_path = _lock_path(tmp_path)
    lock_path.write_text("unsafe\n", encoding="utf-8")
    lock_path.chmod(0o666)

    rc = _main(
        ["initialize-ledger"],
        tmp_path=tmp_path,
        config=config,
        public_path=public_path,
    )

    assert rc == 2
    assert "lock metadata is unsafe" in capsys.readouterr().err
    assert not _ledger_path(tmp_path).exists()


def test_bootstrap_sends_public_key_only_over_stdin_to_every_host(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, identity, public_key = _write_inputs(tmp_path)
    _initialize_ledger(tmp_path, config, public_path)
    bootstrap_public_key = _public_key(80, "bootstrap")
    calls: list[tuple[list[str], dict[str, object]]] = []
    temporary_configs: list[Path] = []

    def fake_run(argv, **kwargs):
        if argv[0] == str(trust.SSH_KEYGEN_BINARY):
            assert argv == [str(trust.SSH_KEYGEN_BINARY), "-y", "-f", str(identity)]
            assert kwargs["stdin"] == subprocess.DEVNULL
            assert kwargs["env"]["SSH_ASKPASS_REQUIRE"] == "never"
            return subprocess.CompletedProcess(argv, 0, bootstrap_public_key, b"")
        calls.append((list(argv), dict(kwargs)))
        temporary_configs.append(Path(argv[2]))
        rendered = Path(argv[2]).read_text(encoding="utf-8")
        assert f'IdentityFile "{identity}"' in rendered
        assert f'IdentityFile "{trust.SERVICE_PRIVATE_KEY_PATH}"' in rendered
        assert "IdentityAgent none" in rendered
        assert "ProxyJump trt-gb10-1" in rendered
        return subprocess.CompletedProcess(argv, 0, b'{"status":"installed"}\n', b"")

    rc = _main(
        ["bootstrap", "--bootstrap-identity", str(identity)],
        tmp_path=tmp_path,
        run=fake_run,
        config=config,
        public_path=public_path,
    )

    assert rc == 0
    expected_hosts = [f"trt-gb10-{number}" for number in range(2, 16)]
    expected_hosts.append("trt-gb10-1")
    assert len(calls) == 15
    for host, (argv, kwargs) in zip(expected_hosts, calls, strict=True):
        assert argv[-2] == host
        assert argv[:2] == [str(trust.SSH_BINARY), "-F"]
        assert "StrictHostKeyChecking=yes" in argv
        assert f"UserKnownHostsFile={trust.KNOWN_HOSTS_PATH}" in argv
        assert "GlobalKnownHostsFile=/dev/null" in argv
        assert "UpdateHostKeys=no" in argv
        assert "-i" not in argv
        assert kwargs["input"] == public_key
        assert kwargs["text"] is False
        assert public_key.decode("ascii").strip() not in " ".join(argv)
        assert str(identity) not in " ".join(argv)
    assert temporary_configs
    assert all(path == temporary_configs[0] for path in temporary_configs)
    assert not temporary_configs[0].exists()
    captured = capsys.readouterr()
    assert public_key.decode("ascii").strip() not in captured.out
    assert public_key.decode("ascii").strip() not in captured.err
    report = json.loads(captured.out)
    assert report["ok"] is True
    assert report["remote_user"] == "qianyi"
    assert [entry["host"] for entry in report["hosts"]] == expected_hosts
    ledger = trust.RevocationLedger.from_bytes(_ledger_path(tmp_path).read_bytes())
    assert ledger.revocation_hosts == tuple(f"trt-gb10-{number}" for number in range(1, 16))


@pytest.mark.parametrize(
    ("operation", "success_status"),
    [("bootstrap", "installed"), ("rotate-bootstrap", "rotated")],
)
def test_bootstrap_operations_defer_jump_host_after_private_host_failure(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    operation: str,
    success_status: str,
) -> None:
    config, public_path, identity, _public_key_value = _write_inputs(tmp_path)
    _initialize_ledger(tmp_path, config, public_path)
    calls: list[str] = []

    def fake_run(argv, **_kwargs):
        if argv[0] == str(trust.SSH_KEYGEN_BINARY):
            return subprocess.CompletedProcess(
                argv,
                0,
                _public_key(80, "derived-bootstrap"),
                b"",
            )
        calls.append(argv[-2])
        if argv[-2] == "trt-gb10-8":
            return subprocess.CompletedProcess(argv, 255, b"", b"private failure")
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"status": success_status}).encode("ascii") + b"\n",
            b"",
        )

    rc = _main(
        [operation, "--bootstrap-identity", str(identity)],
        tmp_path=tmp_path,
        run=fake_run,
        config=config,
        public_path=public_path,
    )

    assert rc == 1
    assert calls == [f"trt-gb10-{number}" for number in range(2, 16)]
    report = json.loads(capsys.readouterr().out)
    assert report["hosts"][-1] == {
        "host": "trt-gb10-1",
        "ok": False,
        "status": "dependency-failed",
    }


def test_rotate_bootstrap_binds_service_and_derived_bootstrap_public_keys(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, identity, service_public_key = _write_inputs(tmp_path)
    _initialize_ledger(tmp_path, config, public_path)
    bootstrap_public_key = _public_key(80, "derived-bootstrap")
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_run(argv, **kwargs):
        if argv[0] == str(trust.SSH_KEYGEN_BINARY):
            return subprocess.CompletedProcess(argv, 0, bootstrap_public_key, b"")
        calls.append((argv[-2], dict(kwargs)))
        return subprocess.CompletedProcess(argv, 0, b'{"status":"rotated"}\n', b"")

    rc = _main(
        ["rotate-bootstrap", "--bootstrap-identity", str(identity)],
        tmp_path=tmp_path,
        run=fake_run,
        config=config,
        public_path=public_path,
    )

    assert rc == 0
    expected_hosts = [f"trt-gb10-{number}" for number in range(2, 16)]
    expected_hosts.append("trt-gb10-1")
    assert [host for host, _ in calls] == expected_hosts
    for _host, kwargs in calls:
        envelope = json.loads(kwargs["input"])
        assert envelope == {
            "bootstrap_public_key": " ".join(bootstrap_public_key.decode("ascii").split()[:2]),
            "schema_version": 1,
            "service_public_key": service_public_key.decode("ascii").strip(),
        }
    report = json.loads(capsys.readouterr().out)
    assert report["ok"] is True
    assert [entry["status"] for entry in report["hosts"]] == ["rotated"] * 15


@pytest.mark.skipif(shutil.which("ssh") is None, reason="OpenSSH client is unavailable")
def test_generated_bootstrap_config_applies_both_keys_to_jump_and_private_hosts(
    tmp_path: Path,
) -> None:
    config, _public_path, identity, _public_key_value = _write_inputs(tmp_path)
    inventory = trust.parse_ssh_inventory(config.read_text(encoding="utf-8"))

    with trust._bootstrap_ssh_config(
        inventory,
        bootstrap_identity=identity,
        parent=tmp_path,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    ) as temporary:
        assert stat.S_IMODE(temporary.stat().st_mode) == 0o600
        for host in ("trt-gb10-1", "trt-gb10-2"):
            completed = subprocess.run(
                ["ssh", "-G", "-F", str(temporary), host],
                capture_output=True,
                text=True,
                check=False,
            )
            assert completed.returncode == 0, completed.stderr
            identities = [
                line.split(maxsplit=1)[1]
                for line in completed.stdout.splitlines()
                if line.startswith("identityfile ")
            ]
            assert identities == [str(identity), str(trust.SERVICE_PRIVATE_KEY_PATH)]
            assert "identityagent none" in completed.stdout
            assert "batchmode yes" in completed.stdout
            assert "controlmaster false" in completed.stdout
            assert "gssapiauthentication no" in completed.stdout
            assert "hostbasedauthentication no" in completed.stdout
            assert "identitiesonly yes" in completed.stdout
            assert "kbdinteractiveauthentication no" in completed.stdout
            assert "passwordauthentication no" in completed.stdout
            assert "preferredauthentications publickey" in completed.stdout
            assert "pubkeyauthentication true" in completed.stdout
        private = subprocess.run(
            ["ssh", "-G", "-F", str(temporary), "trt-gb10-2"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert "proxyjump trt-gb10-1" in private.stdout

    assert not temporary.exists()


@pytest.mark.parametrize("value", ["/secure/key%h", "/secure/${HOME}/key", "/secure/key\nHost *"])
def test_bootstrap_config_rejects_ssh_expansion_and_directive_injection(value: str) -> None:
    with pytest.raises(trust.TrustConfigurationError, match="configuration value is unsafe"):
        trust._quote_ssh_config_value(value)


def test_bootstrap_key_derivation_failure_contacts_no_host(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, identity, _public_key_value = _write_inputs(tmp_path)
    _initialize_ledger(tmp_path, config, public_path)
    calls: list[list[str]] = []

    def fake_run(argv, **_kwargs):
        calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 1, b"", b"DO_NOT_ECHO_KEYGEN_ERROR")

    rc = _main(
        ["bootstrap", "--bootstrap-identity", str(identity)],
        tmp_path=tmp_path,
        run=fake_run,
        config=config,
        public_path=public_path,
    )

    assert rc == 2
    assert calls == [[str(trust.SSH_KEYGEN_BINARY), "-y", "-f", str(identity)]]
    captured = capsys.readouterr()
    assert "DO_NOT_ECHO_KEYGEN_ERROR" not in captured.out
    assert "DO_NOT_ECHO_KEYGEN_ERROR" not in captured.err
    assert "bootstrap identity public key is unavailable" in captured.err


def test_bootstrap_rejects_hard_linked_identity_before_key_derivation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, identity, _public_key_value = _write_inputs(tmp_path)
    _initialize_ledger(tmp_path, config, public_path)
    os.link(identity, tmp_path / "second-name")

    def forbidden_run(*_args, **_kwargs):
        raise AssertionError("bootstrap identity must fail before any subprocess")

    rc = _main(
        ["bootstrap", "--bootstrap-identity", str(identity)],
        tmp_path=tmp_path,
        run=forbidden_run,
        config=config,
        public_path=public_path,
    )

    assert rc == 2
    assert "bootstrap identity metadata is not approved" in capsys.readouterr().err


def test_check_continues_after_partial_host_failure_without_echoing_remote_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, public_key = _write_inputs(tmp_path)
    inventory = trust.parse_ssh_inventory(config.read_text(encoding="utf-8"))
    _register_ledger_hosts(
        tmp_path,
        config,
        public_path,
        inventory.active_hosts,
    )
    calls: list[list[str]] = []
    sentinel = "DO_NOT_ECHO_REMOTE_OUTPUT"

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        assert kwargs["input"] == public_key
        assert "-i" not in argv
        if argv[-2] == "trt-gb10-8":
            return subprocess.CompletedProcess(argv, 255, b"", sentinel.encode())
        return subprocess.CompletedProcess(argv, 0, b'{"status":"present"}\n', b"")

    rc = _main(
        ["check"],
        tmp_path=tmp_path,
        run=fake_run,
        config=config,
        public_path=public_path,
    )

    assert rc == 1
    assert len(calls) == 15
    assert any(argv[-2] == "trt-gb10-7" for argv in calls)
    captured = capsys.readouterr()
    assert sentinel not in captured.out
    assert sentinel not in captured.err
    report = json.loads(captured.out)
    failed = [entry for entry in report["hosts"] if not entry["ok"]]
    assert failed == [{"host": "trt-gb10-8", "ok": False, "status": "remote-failed"}]


def test_revoke_processes_private_hosts_before_jump_host(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, public_key = _write_inputs(tmp_path)
    inventory = trust.parse_ssh_inventory(config.read_text(encoding="utf-8"))
    _register_ledger_hosts(tmp_path, config, public_path, inventory.hosts)
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        assert kwargs["input"] == public_key
        return subprocess.CompletedProcess(argv, 0, b'{"status":"revoked"}\n', b"")

    rc = _main(
        ["revoke"],
        tmp_path=tmp_path,
        run=fake_run,
        config=config,
        public_path=public_path,
    )

    assert rc == 0
    expected_hosts = [f"trt-gb10-{number}" for number in range(2, 16)] + ["trt-gb10-1"]
    assert [argv[-2] for argv in calls] == expected_hosts
    report = json.loads(capsys.readouterr().out)
    assert [entry["host"] for entry in report["hosts"]] == expected_hosts
    assert report["ledger_hosts_remaining"] == 0
    assert _ledger_path(tmp_path).is_file()


def test_revoke_preserves_jump_host_when_private_host_fails(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    inventory = trust.parse_ssh_inventory(config.read_text(encoding="utf-8"))
    _register_ledger_hosts(tmp_path, config, public_path, inventory.hosts)
    calls: list[list[str]] = []
    fail_seven = True

    def fake_run(argv, **_kwargs):
        nonlocal fail_seven
        calls.append(list(argv))
        if argv[-2] == "trt-gb10-7" and fail_seven:
            return subprocess.CompletedProcess(argv, 255, b"", b"")
        return subprocess.CompletedProcess(argv, 0, b'{"status":"revoked"}\n', b"")

    rc = _main(
        ["revoke"],
        tmp_path=tmp_path,
        run=fake_run,
        config=config,
        public_path=public_path,
    )

    assert rc == 1
    assert [argv[-2] for argv in calls] == [f"trt-gb10-{number}" for number in range(2, 16)]
    report = json.loads(capsys.readouterr().out)
    assert report["hosts"][-1] == {
        "host": "trt-gb10-1",
        "ok": False,
        "status": "dependency-failed",
    }
    ledger = trust.RevocationLedger.from_bytes(_ledger_path(tmp_path).read_bytes())
    assert ledger.revocation_hosts == ("trt-gb10-1", "trt-gb10-7")

    calls.clear()
    fail_seven = False
    retry_rc = _main(
        ["revoke"],
        tmp_path=tmp_path,
        run=fake_run,
        config=config,
        public_path=public_path,
    )

    assert retry_rc == 0
    assert [argv[-2] for argv in calls] == ["trt-gb10-7", "trt-gb10-1"]
    retry_report = json.loads(capsys.readouterr().out)
    assert retry_report["ledger_hosts_remaining"] == 0


def test_revoke_retries_inert_remote_receipt_after_local_ledger_write_failure(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, public_path, _identity, _public_key_bytes = _write_inputs(tmp_path)
    _register_ledger_hosts(tmp_path, config, public_path, ("trt-gb10-2",))
    calls: list[str] = []

    def receipt_runner(argv, **_kwargs):
        calls.append(argv[-2])
        return subprocess.CompletedProcess(argv, 0, b'{"status":"revoked"}\n', b"")

    original_write = trust.RevocationLedgerStore.write
    fail_once = True

    def injected_write(
        store: trust.RevocationLedgerStore,
        ledger: trust.RevocationLedger,
    ) -> None:
        nonlocal fail_once
        if fail_once and "trt-gb10-2" not in ledger.revocation_hosts:
            fail_once = False
            raise trust.TrustConfigurationError("injected durable ledger write failure")
        original_write(store, ledger)

    monkeypatch.setattr(trust.RevocationLedgerStore, "write", injected_write)
    first_rc = _main(
        ["revoke"],
        tmp_path=tmp_path,
        run=receipt_runner,
        config=config,
        public_path=public_path,
    )

    assert first_rc == 2
    assert calls == ["trt-gb10-2"]
    assert trust.RevocationLedger.from_bytes(
        _ledger_path(tmp_path).read_bytes()
    ).revocation_hosts == ("trt-gb10-2",)
    assert "durable ledger write failure" in capsys.readouterr().err

    monkeypatch.setattr(trust.RevocationLedgerStore, "write", original_write)
    retry_rc = _main(
        ["revoke"],
        tmp_path=tmp_path,
        run=receipt_runner,
        config=config,
        public_path=public_path,
    )

    assert retry_rc == 0
    assert calls == ["trt-gb10-2", "trt-gb10-2"]
    assert (
        trust.RevocationLedger.from_bytes(_ledger_path(tmp_path).read_bytes()).revocation_hosts
        == ()
    )


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


def test_remote_rotation_replaces_only_canonical_bootstrap_marker_and_is_idempotent(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    bootstrap_key = _public_key(80, "loom-staging-rollout")
    service_key = _public_key(1, "service")
    unrelated = _public_key(90, "unrelated").decode("ascii").strip()
    authorized_keys = ssh_dir / "authorized_keys"
    original_prefix = f"# preserve\r\n{unrelated}\n".encode("ascii")
    authorized_keys.write_bytes(
        original_prefix + bootstrap_key.decode("ascii").strip().encode("ascii")
    )
    authorized_keys.chmod(0o600)

    first = _run_remote(
        home,
        "rotate-bootstrap",
        service_key,
        bootstrap_public_key=bootstrap_key,
    )
    after_first = authorized_keys.read_text(encoding="utf-8")
    metadata_after_first = authorized_keys.stat()
    second = _run_remote(
        home,
        "rotate-bootstrap",
        service_key,
        bootstrap_public_key=bootstrap_key,
    )
    metadata_after_second = authorized_keys.stat()

    assert first.returncode == 0
    assert json.loads(first.stdout) == {"status": "rotated"}
    assert second.returncode == 0
    assert json.loads(second.stdout) == {"status": "already-present"}
    assert authorized_keys.read_text(encoding="utf-8") == after_first
    assert bootstrap_key.decode("ascii").split()[1] not in after_first
    assert service_key.decode("ascii").split()[1] in after_first
    assert unrelated in after_first
    assert "# preserve" in after_first
    assert after_first.count("loom-staging-rollout") == 1
    service_fields = service_key.decode("ascii").split()
    assert authorized_keys.read_bytes() == (
        original_prefix
        + f"{service_fields[0]} {service_fields[1]} loom-staging-rollout".encode("ascii")
    )
    assert (
        metadata_after_second.st_ino,
        metadata_after_second.st_mode,
        metadata_after_second.st_uid,
        metadata_after_second.st_gid,
        metadata_after_second.st_size,
        metadata_after_second.st_mtime_ns,
        metadata_after_second.st_ctime_ns,
    ) == (
        metadata_after_first.st_ino,
        metadata_after_first.st_mode,
        metadata_after_first.st_uid,
        metadata_after_first.st_gid,
        metadata_after_first.st_size,
        metadata_after_first.st_mtime_ns,
        metadata_after_first.st_ctime_ns,
    )


@pytest.mark.parametrize(
    "existing",
    [
        "missing",
        "unrelated-marker",
        "noncanonical-bootstrap",
        "duplicate-bootstrap",
        "bootstrap-plus-service",
        "revoked-bootstrap",
    ],
)
def test_remote_rotation_rejects_ambiguous_authority_without_writing(
    tmp_path: Path,
    existing: str,
) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    bootstrap_key = _public_key(80, "loom-staging-rollout")
    service_key = _public_key(1, "service")
    third_key = _public_key(90, "loom-staging-rollout")
    bootstrap_line = bootstrap_key.decode("ascii").strip()
    service_line = _public_key(1, "unmanaged-service").decode("ascii").strip()
    if existing == "missing":
        original = _public_key(91, "unrelated").decode("ascii")
    elif existing == "unrelated-marker":
        original = third_key.decode("ascii")
    elif existing == "noncanonical-bootstrap":
        original = f'command="/usr/bin/true" {bootstrap_line}\n'
    elif existing == "duplicate-bootstrap":
        original = bootstrap_line + "\n" + _public_key(80, "duplicate").decode("ascii")
    elif existing == "bootstrap-plus-service":
        original = bootstrap_line + "\n" + service_line + "\n"
    else:
        bootstrap = _run_remote(home, "bootstrap", bootstrap_key)
        assert bootstrap.returncode == 0
        revoked = _run_remote(home, "revoke", bootstrap_key)
        assert revoked.returncode == 0
        original = (ssh_dir / "authorized_keys").read_text(encoding="utf-8")
    authorized_keys = ssh_dir / "authorized_keys"
    if existing != "revoked-bootstrap":
        authorized_keys.write_text(original, encoding="utf-8")
        authorized_keys.chmod(0o600)

    result = _run_remote(
        home,
        "rotate-bootstrap",
        service_key,
        bootstrap_public_key=bootstrap_key,
    )

    assert result.returncode == 3
    assert authorized_keys.read_text(encoding="utf-8") == original


def test_remote_rotation_rejects_duplicate_envelope_keys_without_writing(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    bootstrap_key = _public_key(80, "loom-staging-rollout")
    service_key = _public_key(1, "service")
    authorized_keys = ssh_dir / "authorized_keys"
    original = bootstrap_key.decode("ascii")
    authorized_keys.write_text(original, encoding="utf-8")
    authorized_keys.chmod(0o600)
    bootstrap_value = bootstrap_key.decode("ascii").strip()
    service_value = service_key.decode("ascii").strip()
    payload = (
        "{"
        f'"bootstrap_public_key":{json.dumps(bootstrap_value)},'
        f'"bootstrap_public_key":{json.dumps(bootstrap_value)},'
        '"schema_version":1,'
        f'"service_public_key":{json.dumps(service_value)}'
        "}\n"
    ).encode("ascii")

    result = subprocess.run(
        [sys.executable, "-c", trust._REMOTE_SCRIPT, "rotate-bootstrap"],
        input=payload,
        capture_output=True,
        check=False,
        env={**os.environ, "HOME": str(home)},
    )

    assert result.returncode == 2
    assert json.loads(result.stdout) == {"status": "invalid-rotation-envelope"}
    assert authorized_keys.read_text(encoding="utf-8") == original


def test_remote_rotation_rejects_invalid_envelope_semantics_without_writing(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    bootstrap_key = _public_key(80, "loom-staging-rollout")
    service_key = _public_key(1, "service")
    authorized_keys = ssh_dir / "authorized_keys"
    original = bootstrap_key.decode("ascii")
    authorized_keys.write_text(original, encoding="utf-8")
    authorized_keys.chmod(0o600)
    bootstrap_value = bootstrap_key.decode("ascii").strip()
    service_value = service_key.decode("ascii").strip()
    common = {
        "bootstrap_public_key": bootstrap_value,
        "schema_version": 1,
        "service_public_key": service_value,
    }
    payloads = [
        json.dumps({**common, "schema_version": True}).encode("ascii") + b"\n",
        json.dumps({**common, "unexpected": "field"}).encode("ascii") + b"\n",
        trust._bootstrap_envelope(service_key, service_key),
    ]

    for payload in payloads:
        result = subprocess.run(
            [sys.executable, "-c", trust._REMOTE_SCRIPT, "rotate-bootstrap"],
            input=payload,
            capture_output=True,
            check=False,
            env={**os.environ, "HOME": str(home)},
        )

        assert result.returncode == 2
        assert json.loads(result.stdout) == {"status": "invalid-rotation-envelope"}
        assert authorized_keys.read_text(encoding="utf-8") == original


def test_remote_rotation_rejects_insecure_authority_mode_without_writing(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    bootstrap_key = _public_key(80, "loom-staging-rollout")
    authorized_keys = ssh_dir / "authorized_keys"
    original = bootstrap_key.decode("ascii")
    authorized_keys.write_text(original, encoding="utf-8")
    authorized_keys.chmod(0o640)

    result = _run_remote(
        home,
        "rotate-bootstrap",
        _public_key(1, "service"),
        bootstrap_public_key=bootstrap_key,
    )

    assert result.returncode == 3
    assert json.loads(result.stdout) == {"status": "rotation-authority-mode-invalid"}
    assert authorized_keys.read_text(encoding="utf-8") == original
    assert stat.S_IMODE(authorized_keys.stat().st_mode) == 0o640


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


def test_remote_revoke_tombstones_only_exact_decoded_fingerprint(tmp_path: Path) -> None:
    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, mode=0o700)
    target = _public_key(1, "loom-staging-rollout").decode("ascii").strip()
    unrelated_key = _public_key(2, "unrelated").decode("ascii").strip()
    lines = ["# operator key", unrelated_key, target, "# keep trailing comment"]
    authorized_keys = ssh_dir / "authorized_keys"
    authorized_keys.write_text("\n".join(lines) + "\n", encoding="utf-8")
    authorized_keys.chmod(0o600)

    result = _run_remote(home, "revoke", _public_key(1))

    assert result.returncode == 0
    assert json.loads(result.stdout) == {"status": "revoked"}
    rendered = authorized_keys.read_text(encoding="utf-8")
    assert f"\n{target}\n" not in rendered
    assert "restrict,command=" in rendered
    assert "loom-staging-rollout-revoked" in rendered
    assert r"{\"status\":\"revoked\"}" in rendered
    assert unrelated_key in rendered
    assert "# operator key" in rendered
    assert "# keep trailing comment" in rendered

    repeated = _run_remote(home, "revoke", _public_key(1))
    assert repeated.returncode == 0
    assert json.loads(repeated.stdout) == {"status": "revoked"}

    restored = _run_remote(home, "bootstrap", _public_key(1))
    assert restored.returncode == 0
    assert json.loads(restored.stdout) == {"status": "restored"}
    restored_text = authorized_keys.read_text(encoding="utf-8")
    assert "loom-staging-rollout-revoked" not in restored_text
    assert "restrict,command=" not in restored_text
    assert "loom-staging-rollout" in restored_text


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
