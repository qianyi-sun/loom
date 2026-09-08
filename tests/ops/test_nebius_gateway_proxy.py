from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from scripts.ops import nebius_gateway_proxy as proxy

LISTEN = "10.20.30.40"
PEER = "10.253.176.2"


def test_render_is_narrow_and_boot_restart_ordered() -> None:
    units = proxy.render(LISTEN, PEER)
    assert len(units) == 7
    for port in proxy.PORTS:
        name = f"loom-nebius-forward-{port}"
        socket = units[f"{name}.socket"]
        service = units[f"{name}.service"]
        assert f"ListenStream={LISTEN}:{port}" in socket
        assert "DefaultDependencies=no" in socket  # No late-network/sockets.target boot cycle.
        assert f"Requires={proxy.TRANSPORT} {proxy.ADDRESS_UNIT}" in socket
        assert f"WantedBy=multi-user.target {proxy.TRANSPORT}" in socket
        assert f"PartOf={proxy.TRANSPORT} {name}.socket" in service
        assert f"ExecStart=/usr/lib/systemd/systemd-socket-proxyd {PEER}:{port}" in service
    alias = units[proxy.ADDRESS_UNIT]
    assert f"address add {LISTEN}/32 dev lo label lo:loom-nb" in alias
    assert f"address del {LISTEN}/32 dev lo" in alias
    assert "RemainAfterExit=yes" in alias
    assert all(
        value not in "".join(units.values())
        for value in (
            "0.0.0.0",
            "iptables",
            "nft ",
            "sysctl",
            "route add",
            "socat",
            "PrivateNetwork",
        )
    )


@pytest.mark.parametrize(
    "value",
    [
        "0.0.0.0",
        "127.0.0.1",
        "192.0.2.1",
        "8.8.8.8",
        "::1",
        "10.0.0.1/32",
        "localhost",
        "10.0.0.1\nBAD",
    ],
)
@pytest.mark.parametrize("side", ["listen", "peer"])
def test_reject_non_rfc1918_literal(value: str, side: str) -> None:
    with pytest.raises(ValueError):
        proxy.render(value if side == "listen" else LISTEN, value if side == "peer" else PEER)


def test_reject_identical_endpoints() -> None:
    with pytest.raises(ValueError, match="differ"):
        proxy.render(LISTEN, LISTEN)


class FakeSystemd:
    def __init__(self, directory: Path):
        self.directory = directory
        self.calls: list[list[str]] = []
        self.active: dict[str, str] = {}
        self.enabled: dict[str, str] = {}
        self.fail_action: str | None = None
        self.fail_remaining = 0
        self.fragment = ""
        self.dropins = ""

    def __call__(self, argv: list[str]) -> str:
        self.calls.append(argv)
        action = argv[1]
        if action == self.fail_action and self.fail_remaining:
            self.fail_remaining -= 1
            raise RuntimeError("injected operation failure")
        if action == "show":
            prop = argv[2].split("=", 1)[1]
            name = argv[-1]
            return {
                "ActiveState": self.active.get(name, "inactive"),
                "UnitFileState": self.enabled.get(name, "disabled"),
                "FragmentPath": self.fragment,
                "DropInPaths": self.dropins,
            }[prop]
        for name in argv[2:]:
            if action in ("start", "restart", "stop"):
                self.active[name] = "inactive" if action == "stop" else "active"
            elif action in ("enable", "disable"):
                self.enabled[name] = "enabled" if action == "enable" else "disabled"
        return ""


@pytest.fixture
def system(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeSystemd:
    system = FakeSystemd(tmp_path)
    monkeypatch.setattr(proxy, "DIRECTORY", tmp_path)
    monkeypatch.setattr(proxy, "command", system)
    monkeypatch.setattr(proxy, "trusted", lambda path, **kwargs: None)
    return system


def test_reconcile_install_then_idempotent_no_restart(system: FakeSystemd) -> None:
    units = proxy.render(LISTEN, PEER)
    assert proxy.reconcile(units)["changed"] is True
    for name, content in units.items():
        assert (system.directory / name).read_text() == content
    system.calls.clear()
    assert proxy.reconcile(units)["changed"] is False
    assert not any(call[1] in ("restart", "daemon-reload", "stop") for call in system.calls)


@pytest.mark.parametrize("failure", ["daemon-reload", "start", "restart", "enable"])
def test_first_install_failure_removes_only_owned_files(system: FakeSystemd, failure: str) -> None:
    unrelated = system.directory / "another.service"
    unrelated.write_text("not Loom")
    system.fail_action = failure
    system.fail_remaining = 1
    units = proxy.render(LISTEN, PEER)
    with pytest.raises(RuntimeError, match="injected"):
        proxy.reconcile(units)
    assert all(not (system.directory / name).exists() for name in units)
    assert unrelated.read_text() == "not Loom"
    assert not any(state == "active" for state in system.active.values())
    assert not any(state == "enabled" for state in system.enabled.values())


@pytest.mark.parametrize("was_active", [False, True])
def test_failed_change_restores_config_active_and_enabled(
    system: FakeSystemd, was_active: bool
) -> None:
    old = proxy.render(LISTEN, PEER)
    proxy.reconcile(old)
    for name in old:
        system.active[name] = "active" if was_active else "inactive"
        system.enabled[name] = "enabled" if was_active and name.endswith(".socket") else "disabled"
    before_active = dict(system.active)
    before_enabled = dict(system.enabled)
    system.fail_action, system.fail_remaining = "restart", 1
    with pytest.raises(RuntimeError, match="injected"):
        proxy.reconcile(proxy.render("10.20.30.41", PEER))
    assert {name: (system.directory / name).read_text() for name in old} == old
    assert system.active == before_active
    assert system.enabled == before_enabled


def test_alias_change_stops_old_before_replacing_execstop(
    system: FakeSystemd, monkeypatch: pytest.MonkeyPatch
) -> None:
    proxy.reconcile(proxy.render(LISTEN, PEER))
    original = proxy.write_unit

    def check_write(path: Path, value: str) -> None:
        if path.name == proxy.ADDRESS_UNIT:
            assert system.active[proxy.ADDRESS_UNIT] == "inactive"
        original(path, value)

    monkeypatch.setattr(proxy, "write_unit", check_write)
    assert proxy.reconcile(proxy.render("10.20.30.41", PEER))["changed"]


def test_unchanged_enable_failure_restores_disabled_state(system: FakeSystemd) -> None:
    units = proxy.render(LISTEN, PEER)
    proxy.reconcile(units)
    for name in units:
        system.enabled[name] = "disabled"
    system.fail_action, system.fail_remaining = "enable", 1
    with pytest.raises(RuntimeError, match="injected"):
        proxy.reconcile(units)
    assert set(system.enabled.values()) == {"disabled"}
    assert all((system.directory / name).read_text() == value for name, value in units.items())


def test_unmanaged_file_preserved(system: FakeSystemd) -> None:
    path = system.directory / proxy.ADDRESS_UNIT
    path.write_text("belongs to someone else")
    with pytest.raises(ValueError, match="unmanaged"):
        proxy.reconcile(proxy.render(LISTEN, PEER))
    assert path.read_text() == "belongs to someone else"
    assert not system.calls


@pytest.mark.parametrize("property_name", ["fragment", "dropins"])
def test_unmanaged_runtime_fragment_preserved(system: FakeSystemd, property_name: str) -> None:
    setattr(system, property_name, "/vendor/config")
    with pytest.raises(ValueError, match="unmanaged"):
        proxy.reconcile(proxy.render(LISTEN, PEER))
    assert not list(system.directory.iterdir())


def test_trusted_path_rejects_symlink_and_writable(tmp_path: Path) -> None:
    target = tmp_path / "unit"
    target.write_text("keep")
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="root-owned"):
        proxy.trusted(link)
    target.chmod(0o666)
    with pytest.raises(ValueError, match="root-owned"):
        proxy.trusted(target)
    assert target.read_text() == "keep"


def test_network_unassigned_alias_validates_exact_wg_route(monkeypatch: pytest.MonkeyPatch) -> None:
    outputs = iter(["[]", f"public-key\t{PEER}/32", json.dumps([{"dev": proxy.INTERFACE}])])
    monkeypatch.setattr(proxy, "command", lambda _: next(outputs))
    proxy.check_network(LISTEN, PEER)


@pytest.mark.parametrize(
    "allowed",
    [f"key\t{PEER}/24", f"key\t{PEER}/32,10.0.0.0/8", "", f"key\t{PEER}/32\nsecond\t10.0.0.2/32"],
)
def test_network_rejects_wrong_or_broad_peer(monkeypatch: pytest.MonkeyPatch, allowed: str) -> None:
    outputs = iter(["[]", allowed])
    monkeypatch.setattr(proxy, "command", lambda _: next(outputs))
    with pytest.raises(ValueError, match="AllowedIPs"):
        proxy.check_network(LISTEN, PEER)


def test_network_rejects_wrong_peer_route(monkeypatch: pytest.MonkeyPatch) -> None:
    outputs = iter(["[]", f"key\t{PEER}/32", '[{"dev":"eth0"}]'])
    monkeypatch.setattr(proxy, "command", lambda _: next(outputs))
    with pytest.raises(ValueError, match="route"):
        proxy.check_network(LISTEN, PEER)


@pytest.mark.parametrize(
    "interface,label,prefix",
    [
        ("eth0", "eth0", 24),
        ("lo", "someone", 32),
        ("lo", proxy.ADDRESS_LABEL, 24),
        (proxy.INTERFACE, proxy.ADDRESS_LABEL, 32),
    ],
)
def test_network_rejects_existing_unowned_address(
    monkeypatch: pytest.MonkeyPatch, interface: str, label: str, prefix: int
) -> None:
    interfaces = [
        {"ifname": interface, "addr_info": [{"local": LISTEN, "label": label, "prefixlen": prefix}]}
    ]
    monkeypatch.setattr(proxy, "command", lambda _: json.dumps(interfaces))
    with pytest.raises(ValueError, match="outside"):
        proxy.check_network(LISTEN, PEER)


def test_command_sanitizes_output(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        proxy.subprocess,
        "run",
        lambda *a, **kw: subprocess.CompletedProcess(a, 1, "private endpoint", "credential"),
    )
    with pytest.raises(RuntimeError) as failure:
        proxy.command(["systemctl", "start", "unit"])
    assert str(failure.value) == "systemctl operation failed (exit 1)"


def test_install_requires_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(proxy.os, "geteuid", lambda: 1000)
    with pytest.raises(ValueError, match="root"):
        proxy.install(LISTEN, PEER)


def test_network_owned_alias_is_idempotent(
    system: FakeSystemd, monkeypatch: pytest.MonkeyPatch
) -> None:
    units = proxy.render(LISTEN, PEER)
    proxy.reconcile(units)
    interfaces = [
        {
            "ifname": "lo",
            "addr_info": [{"local": LISTEN, "label": proxy.ADDRESS_LABEL, "prefixlen": 32}],
        }
    ]
    outputs = iter(
        [
            json.dumps(interfaces),
            "active",
            f"key\t{PEER}/32",
            json.dumps([{"dev": proxy.INTERFACE}]),
        ]
    )
    monkeypatch.setattr(proxy, "command", lambda _: next(outputs))
    proxy.check_network(LISTEN, PEER)


def test_network_label_without_managed_unit_rejected(
    system: FakeSystemd, monkeypatch: pytest.MonkeyPatch
) -> None:
    (system.directory / proxy.ADDRESS_UNIT).write_text("unrelated")
    interfaces = [
        {
            "ifname": "lo",
            "addr_info": [{"local": LISTEN, "label": proxy.ADDRESS_LABEL, "prefixlen": 32}],
        }
    ]
    monkeypatch.setattr(proxy, "command", lambda _: json.dumps(interfaces))
    with pytest.raises(ValueError, match="managed address unit"):
        proxy.check_network(LISTEN, PEER)


def test_partial_write_failure_restores_all_prior_files(
    system: FakeSystemd, monkeypatch: pytest.MonkeyPatch
) -> None:
    units = proxy.render(LISTEN, PEER)
    proxy.reconcile(units)
    original = proxy.write_unit
    writes = 0

    def fail_once(path: Path, value: str) -> None:
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("disk full")
        original(path, value)

    monkeypatch.setattr(proxy, "write_unit", fail_once)
    with pytest.raises(OSError, match="disk full"):
        proxy.reconcile(proxy.render("10.20.30.41", PEER))
    assert {name: (system.directory / name).read_text() for name in units} == units


def test_rollback_failure_is_explicit_and_still_restores_files(system: FakeSystemd) -> None:
    units = proxy.render(LISTEN, PEER)
    proxy.reconcile(units)
    system.fail_action, system.fail_remaining = "daemon-reload", 2
    with pytest.raises(RuntimeError, match="rollback requires operator attention"):
        proxy.reconcile(proxy.render("10.20.30.41", PEER))
    assert {name: (system.directory / name).read_text() for name in units} == units


def test_render_cli_has_no_install_side_effects(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        proxy.sys, "argv", ["proxy", "render", "--listen-address", LISTEN, "--peer-address", PEER]
    )
    monkeypatch.setattr(proxy, "install", lambda *_: pytest.fail("render must not install"))
    assert proxy.main() == 0
    assert json.loads(capsys.readouterr().out) == proxy.render(LISTEN, PEER)


@pytest.mark.parametrize("property_name", ["ActiveState", "UnitFileState"])
def test_unsuccessful_readback_rolls_back_even_when_command_succeeded(
    system: FakeSystemd, monkeypatch: pytest.MonkeyPatch, property_name: str
) -> None:
    def stale_readback(argv: list[str]) -> str:
        result = system(argv)
        if f"--property={property_name}" in argv and any(
            call[1] == "start" for call in system.calls
        ):
            return "inactive" if property_name == "ActiveState" else "disabled"
        return result

    monkeypatch.setattr(proxy, "command", stale_readback)
    units = proxy.render(LISTEN, PEER)
    with pytest.raises(RuntimeError, match="readback"):
        proxy.reconcile(units)
    assert all(not (system.directory / name).exists() for name in units)
