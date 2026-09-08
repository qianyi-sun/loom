from __future__ import annotations

import argparse
import base64
import json
from contextlib import nullcontext
from pathlib import Path

import pytest
from scripts.ops import nebius_private_link as link

KEY = base64.b64encode(b"a" * 32).decode()


def args(**changes: object) -> argparse.Namespace:
    values = dict(
        action="configure",
        address="10.253.176.1/32",
        peer_address="10.253.176.2/32",
        peer_public_key=KEY,
        listen_port=51871,
        endpoint="192.0.2.20:51871",
    )
    values.update(changes)
    return argparse.Namespace(**values)


def test_render_only_peer_route_and_no_shell_hooks() -> None:
    rendered = link.render(args(), KEY)
    assert "AllowedIPs = 10.253.176.2/32" in rendered
    assert "PersistentKeepalive = 25" in rendered
    assert "SaveConfig" not in rendered
    assert all(value not in rendered for value in ("PostUp", "PostDown", "0.0.0.0/0", "DNS"))
    assert "Endpoint" not in link.render(args(endpoint=None), KEY)


@pytest.mark.parametrize(
    "changes",
    [
        {"address": "0.0.0.0/0"},
        {"peer_address": "10.0.0.0/8"},
        {"peer_address": "10.253.176.1/32"},
        {"peer_address": "127.0.0.1/32"},
        {"listen_port": 0},
        {"endpoint": "host.example:51871"},
        {"endpoint": "192.0.2.1:70000"},
        {"peer_public_key": KEY + "\nPostUp = bad"},
    ],
)
def test_reject_unscoped_or_injected_configuration(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        link.render(args(**changes), KEY)


def test_route_conflict_excludes_default_and_own_interface(monkeypatch: pytest.MonkeyPatch) -> None:
    routes = [{"dst": "default", "dev": "eth0"}, {"dst": "10.253.176.2/32", "dev": link.INTERFACE}]
    monkeypatch.setattr(link, "command", lambda _: json.dumps(routes))
    link.check_routes(["10.253.176.2/32"])
    routes.append({"dst": "10.253.0.0/16", "dev": "other-vpn"})
    with pytest.raises(ValueError, match="conflicts"):
        link.check_routes(["10.253.176.2/32"])


def setup_fake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    calls: list[list[str]] = []
    monkeypatch.setattr(link, "DIRECTORY", tmp_path)
    monkeypatch.setattr(link.os, "geteuid", lambda: 0)
    monkeypatch.setattr(link, "private_file", lambda path: path.read_text())
    monkeypatch.setattr(link, "check_routes", lambda _: None)
    monkeypatch.setattr(link, "exclusive_lock", nullcontext)
    original_stat = Path.stat

    def root_stat(path: Path, **kwargs: object) -> object:
        result = original_stat(path, **kwargs)

        class Info:
            st_uid = 0
            st_mode = result.st_mode

        return Info() if path == tmp_path else result

    monkeypatch.setattr(Path, "stat", root_stat)

    def fake(command: list[str], **_: object) -> str:
        calls.append(command)
        if command == ["wg", "genkey"]:
            return KEY
        if command == ["wg", "pubkey"]:
            return KEY
        if "is-active" in command:
            return "active"
        if "is-enabled" in command:
            return "enabled"
        return ""

    monkeypatch.setattr(link, "command", fake)
    return calls


def test_prepare_and_reconcile_are_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = setup_fake(tmp_path, monkeypatch)
    assert link.reconcile(args(action="prepare"))["public_key"] == KEY
    link.reconcile(args(action="prepare"))
    assert calls.count(["wg", "genkey"]) == 1
    assert link.reconcile(args())["changed"] is True
    assert link.reconcile(args())["changed"] is False
    assert sum("restart" in call for call in calls) == 1
    assert (tmp_path / f"{link.INTERFACE}.conf").stat().st_mode & 0o777 == 0o600


def test_unmanaged_configuration_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = setup_fake(tmp_path, monkeypatch)
    config = tmp_path / f"{link.INTERFACE}.conf"
    config.write_text("belongs to someone else")
    with pytest.raises(ValueError, match="unmanaged"):
        link.reconcile(args(action="prepare"))
    assert config.read_text() == "belongs to someone else"
    assert not calls


@pytest.mark.parametrize("suffix", ["conf", "key"])
def test_dangling_symlink_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, suffix: str
) -> None:
    calls = setup_fake(tmp_path, monkeypatch)
    path = tmp_path / f"{link.INTERFACE}.{suffix}"
    path.symlink_to(tmp_path / "absent")
    with pytest.raises(ValueError, match="symlink"):
        link.reconcile(args(action="prepare"))
    assert path.is_symlink()
    assert not calls


def test_writable_directory_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = setup_fake(tmp_path, monkeypatch)
    tmp_path.chmod(0o777)
    with pytest.raises(ValueError, match="writable"):
        link.reconcile(args(action="prepare"))
    assert not calls


@pytest.mark.parametrize("active", [True, False])
def test_failed_update_restores_config_and_unit_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, active: bool
) -> None:
    calls = setup_fake(tmp_path, monkeypatch)
    link.reconcile(args(action="prepare"))
    link.reconcile(args())
    config = tmp_path / f"{link.INTERFACE}.conf"
    before = config.read_text()
    previous_command = link.command
    restarts = 0

    def fail_once(argv: list[str], **kwargs: object) -> str:
        nonlocal restarts
        if "--property=ActiveState" in argv:
            return "active" if active else "inactive"
        if "--property=UnitFileState" in argv:
            return "enabled" if active else "disabled"
        if "restart" in argv:
            restarts += 1
            if restarts == 1:
                raise RuntimeError("activation failed")
        return previous_command(argv, **kwargs)

    monkeypatch.setattr(link, "command", fail_once)
    with pytest.raises(RuntimeError, match="activation failed"):
        link.reconcile(args(listen_port=51872))
    assert config.read_text() == before
    assert restarts == (2 if active else 1)
    unit = f"wg-quick@{link.INTERFACE}.service"
    assert ["systemctl", "enable" if active else "disable", unit] in calls
    if not active:
        assert ["systemctl", "stop", unit] in calls


def test_command_failure_does_not_relay_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    monkeypatch.setattr(
        link.subprocess,
        "run",
        lambda *a, **kw: subprocess.CompletedProcess(a, 1, "private key", "secret endpoint"),
    )
    with pytest.raises(RuntimeError) as failure:
        link.command(["wg-quick", "up", link.INTERFACE])
    assert str(failure.value) == "wg-quick operation failed (exit 1)"


@pytest.mark.parametrize("failure_step", ["restart", "enable"])
def test_first_install_failure_keeps_identity_but_removes_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_step: str
) -> None:
    calls = setup_fake(tmp_path, monkeypatch)
    link.reconcile(args(action="prepare"))
    previous_command = link.command

    def fail(argv: list[str], **kwargs: object) -> str:
        if failure_step in argv:
            raise RuntimeError("first activation failed")
        return previous_command(argv, **kwargs)

    monkeypatch.setattr(link, "command", fail)
    with pytest.raises(RuntimeError, match="first activation failed"):
        link.reconcile(args())
    assert not (tmp_path / f"{link.INTERFACE}.conf").exists()
    assert (tmp_path / f"{link.INTERFACE}.key").exists()
    assert ["systemctl", "disable", f"wg-quick@{link.INTERFACE}.service"] in calls


def test_unmanaged_active_interface_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_fake(tmp_path, monkeypatch)
    link.reconcile(args(action="prepare"))
    previous_command = link.command
    monkeypatch.setattr(
        link,
        "command",
        lambda argv, **kw: (
            link.INTERFACE if argv == ["wg", "show", "interfaces"] else previous_command(argv, **kw)
        ),
    )
    with pytest.raises(ValueError, match="unmanaged active"):
        link.reconcile(args())
    assert not (tmp_path / f"{link.INTERFACE}.conf").exists()


def test_unchanged_config_enable_failure_restores_stopped_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = setup_fake(tmp_path, monkeypatch)
    link.reconcile(args(action="prepare"))
    link.reconcile(args())
    before = (tmp_path / f"{link.INTERFACE}.conf").read_text()
    previous_command = link.command

    def fail_enable(argv: list[str], **kwargs: object) -> str:
        if "enable" in argv:
            raise RuntimeError("enable failed")
        return previous_command(argv, **kwargs)

    monkeypatch.setattr(link, "command", fail_enable)
    with pytest.raises(RuntimeError, match="enable failed"):
        link.reconcile(args())
    assert (tmp_path / f"{link.INTERFACE}.conf").read_text() == before
    unit = f"wg-quick@{link.INTERFACE}.service"
    assert calls[-2:] == [["systemctl", "stop", unit], ["systemctl", "disable", unit]]
