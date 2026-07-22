from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.gb10_readiness import ACTIVE_GB10_HOSTS
from loom_cli.rollout.gb10_rehearsal import (
    FixedGB10RehearsalTransport,
    GB10RehearsalAuthority,
    _remote_source,
)
from loom_cli.rollout.systemd_readiness import RehearsalSystemdActivation


def _authority(tmp_path: Path) -> GB10RehearsalAuthority:
    return GB10RehearsalAuthority(
        hosts=ACTIVE_GB10_HOSTS,
        ssh_config=tmp_path / "ssh-config",
        identity=tmp_path / "identity",
        ssh_config_sha256=hashlib.sha256(b"fixed-config").hexdigest(),
        identity_metadata_fingerprint="b" * 64,
    )


def _contract() -> RehearsalSystemdActivation:
    return RehearsalSystemdActivation(
        unit="loom-preflight-" + "a" * 24 + ".service",
        plan_digest="c" * 64,
    )


def _record(
    contract: RehearsalSystemdActivation,
    *,
    mode: str = "execute",
    reason: str = "",
) -> dict[str, object]:
    return {
        "boot_id": "11111111-1111-4111-8111-111111111111",
        "cleanup_verified": True,
        "latency_ms": 125 if mode == "execute" else 0,
        "mode": mode,
        "properties": (
            {
                "LoadState": "loaded",
                "ActiveState": "active",
                "SubState": "exited",
                "Type": "oneshot",
                "Result": "success",
                "ExecMainStatus": "0",
                "NeedDaemonReload": "no",
                "Transient": "yes",
                "Description": contract.description,
            }
            if mode == "execute"
            else {}
        ),
        "reason": reason,
        "unit": contract.unit,
    }


def _trust_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    def read(path, **_kwargs):
        if Path(path).name == "ssh-config":
            return SimpleNamespace(payload=b"fixed-config", metadata_fingerprint="a" * 64)
        return SimpleNamespace(payload=b"private-key", metadata_fingerprint="b" * 64)

    monkeypatch.setattr("loom_cli.rollout.gb10_rehearsal.read_trusted_file", read)


def test_authority_round_trip_requires_the_exact_active_inventory(tmp_path: Path) -> None:
    authority = _authority(tmp_path)
    assert GB10RehearsalAuthority.from_record(authority.to_record()) == authority
    assert len(authority.hosts) == 14
    assert "trt-gb10-7" not in authority.hosts

    record = authority.to_record()
    record["hosts"] = list(ACTIVE_GB10_HOSTS[:-1])
    with pytest.raises(ValueError, match="authority is invalid"):
        GB10RehearsalAuthority.from_record(record)


def test_execute_uses_only_fixed_ssh_and_isolated_unit_on_all_hosts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _trust_inputs(monkeypatch)
    authority = _authority(tmp_path)
    contract = _contract()
    calls: list[tuple[str, ...]] = []

    def run(argv, timeout):
        calls.append(tuple(argv))
        assert timeout == 60
        return subprocess.CompletedProcess(argv, 0, json.dumps(_record(contract)), "")

    evidence = FixedGB10RehearsalTransport(authority, 501, run).execute(contract)

    assert evidence.cleanup_verified
    assert not evidence.blockers
    assert set(evidence.host_boot_ids) == set(ACTIVE_GB10_HOSTS)
    assert len(calls) == 14
    assert {call[-2] for call in calls} == set(ACTIVE_GB10_HOSTS)
    assert all(
        call[:6] == ("ssh", "-F", str(authority.ssh_config), "-i", str(authority.identity), "-o")
        for call in calls
    )
    commands = "\n".join(call[-1] for call in calls)
    assert contract.unit in commands
    assert "loom-gb10-node-agent" not in commands
    assert "loom-staging-rollout.service" not in commands
    assert "sudo" not in commands


def test_remote_activation_uses_exit_status_and_exact_readback_not_warning_text() -> None:
    """User systemd may warn while successfully creating the exact unit."""

    source = _remote_source(_contract(), mode="execute")

    assert "if result.returncode != 0:" in source
    assert "if result.returncode != 0 or result.stderr:" not in source
    assert "properties = show() or {}" in source
    compile(source, "<gb10-rehearsal>", "exec")


def test_remote_cleanup_accepts_only_verified_absence_after_stop() -> None:
    source = _remote_source(_contract(), mode="cleanup")

    assert 'stopped.returncode != 0 and load_state() != "not-found"' in source
    assert "reset-failed" not in source
    assert 'cleanup_verified = load_state() == "not-found"' in source
    compile(source, "<gb10-rehearsal-cleanup>", "exec")


def test_execute_aggregates_independent_host_blockers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _trust_inputs(monkeypatch)
    authority = _authority(tmp_path)
    contract = _contract()

    def run(argv, _timeout):
        host = argv[-2]
        if host == "trt-gb10-1":
            return subprocess.CompletedProcess(argv, 1, "", "")
        reason = "activation-failed" if host == "trt-gb10-2" else ""
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(_record(contract, reason=reason)),
            "",
        )

    evidence = FixedGB10RehearsalTransport(authority, 501, run).execute(contract)

    assert evidence.blockers == {
        "trt-gb10-1": "transport-unavailable",
        "trt-gb10-2": "activation-failed",
    }
    assert len(evidence.host_boot_ids) == 12
    assert not evidence.cleanup_verified


def test_cleanup_is_fleet_wide_and_rejects_nonempty_readback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _trust_inputs(monkeypatch)
    authority = _authority(tmp_path)
    contract = _contract()

    def run(argv, _timeout):
        record = _record(contract, mode="cleanup")
        if argv[-2] == "trt-gb10-3":
            record["properties"] = {"LoadState": "loaded"}
        return subprocess.CompletedProcess(argv, 0, json.dumps(record), "")

    evidence = FixedGB10RehearsalTransport(authority, 501, run).cleanup(contract)

    assert evidence.blockers == {"trt-gb10-3": "cleanup-readback-drift"}
    assert len(evidence.host_boot_ids) == 13


def test_local_authority_drift_fails_before_any_ssh(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    authority = _authority(tmp_path)
    calls = 0

    monkeypatch.setattr(
        "loom_cli.rollout.gb10_rehearsal.read_trusted_file",
        lambda *_args, **_kwargs: SimpleNamespace(
            payload=b"drift",
            metadata_fingerprint="d" * 64,
        ),
    )

    def run(_argv, _timeout):
        nonlocal calls
        calls += 1
        raise AssertionError("SSH must not run after local authority drift")

    with pytest.raises(ValueError, match="local authority drifted"):
        FixedGB10RehearsalTransport(authority, 501, run).execute(_contract())
    assert calls == 0
