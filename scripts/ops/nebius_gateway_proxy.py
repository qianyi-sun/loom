#!/usr/bin/env python3
"""Install three private TCP forwards from a Nebius VPC address to the staging WG peer."""

from __future__ import annotations

import argparse
import fcntl
import ipaddress
import json
import os
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

DIRECTORY = Path("/etc/systemd/system")
PORTS = (15432, 18443, 19443)
INTERFACE = "wg-loom-nb"
TRANSPORT = f"wg-quick@{INTERFACE}.service"
ADDRESS_UNIT = "loom-nebius-private-address.service"
ADDRESS_LABEL = "lo:loom-nb"
MARKER = "# Managed by Loom nebius_gateway_proxy.py\n"
PROXY_PATHS = ("/usr/lib/systemd/systemd-socket-proxyd", "/lib/systemd/systemd-socket-proxyd")


def command(argv: list[str]) -> str:
    result = subprocess.run(argv, capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError(f"{argv[0]} operation failed (exit {result.returncode})")
    return result.stdout.strip()


def address(value: str) -> str:
    try:
        parsed = ipaddress.IPv4Address(value)
    except ValueError as exc:
        raise ValueError("address must be an RFC1918 IPv4 literal") from exc
    if not any(
        parsed in ipaddress.ip_network(cidr)
        for cidr in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
    ):
        raise ValueError("address must be an RFC1918 IPv4 literal")
    return str(parsed)


def render(listen_address: str, peer_address: str, binary: str = PROXY_PATHS[0]) -> dict[str, str]:
    listen, peer = address(listen_address), address(peer_address)
    if listen == peer:
        raise ValueError("VPC listener and staging peer must differ")
    if binary not in PROXY_PATHS:
        raise ValueError("unsupported systemd proxy binary")
    units = {
        ADDRESS_UNIT: (
            MARKER + "[Unit]\nDescription=Loom reserved VPC alias address\n\n"
            "[Service]\nType=oneshot\nRemainAfterExit=yes\n"
            f"ExecStart=/usr/bin/ip address add {listen}/32 dev lo label {ADDRESS_LABEL}\n"
            f"ExecStop=/usr/bin/ip address del {listen}/32 dev lo\n"
        )
    }
    for port in PORTS:
        name = f"loom-nebius-forward-{port}"
        # Default socket Before=sockets.target conflicts with WG's late-network ordering.
        units[f"{name}.socket"] = (
            MARKER + "[Unit]\nDescription=Loom private staging TCP entry\n"
            "DefaultDependencies=no\nConflicts=shutdown.target\nBefore=shutdown.target\n"
            f"Requires={TRANSPORT} {ADDRESS_UNIT}\nAfter={TRANSPORT} {ADDRESS_UNIT} network-online.target\n"
            f"PartOf={TRANSPORT}\nWants=network-online.target\n\n"
            f"[Socket]\nListenStream={listen}:{port}\nAccept=no\nFreeBind=no\n\n"
            f"[Install]\nWantedBy=multi-user.target {TRANSPORT}\n"
        )
        units[f"{name}.service"] = (
            MARKER + "[Unit]\nDescription=Loom private staging TCP forwarder\n"
            f"Requires={TRANSPORT} {name}.socket\nAfter={TRANSPORT} {name}.socket\n"
            f"PartOf={TRANSPORT} {name}.socket\n\n"
            f"[Service]\nExecStart={binary} {peer}:{port}\n"
            "Restart=on-failure\nRestartSec=2s\n"
        )
    return units


def trusted(path: Path, *, directory: bool = False) -> None:
    info = path.lstat()
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if not expected(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
        raise ValueError("installer paths must be root-owned, non-writable, and not symlinks")


def proxy_binary() -> str:
    for value in PROXY_PATHS:
        path = Path(value)
        if path.exists():
            trusted(path)
            if not path.stat().st_mode & 0o111:
                raise ValueError("systemd proxy binary is not executable")
            return value
    raise ValueError("systemd-socket-proxyd is not installed")


def check_network(listen: str, peer: str) -> None:
    interfaces = json.loads(command(["ip", "-j", "-4", "address", "show"]))
    owners = [
        (item, info)
        for item in interfaces
        for info in item.get("addr_info", [])
        if info.get("local") == listen
    ]
    if owners:
        path = DIRECTORY / ADDRESS_UNIT
        if (
            len(owners) != 1
            or owners[0][0].get("ifname") != "lo"
            or (owners[0][1].get("label") != ADDRESS_LABEL or owners[0][1].get("prefixlen") != 32)
        ):
            raise ValueError("reserved alias already assigned outside the managed loopback label")
        trusted(path)
        config = path.read_text(encoding="utf-8")
        if (
            config != render(listen, peer)[ADDRESS_UNIT]
            or property_value(ADDRESS_UNIT, "ActiveState") != "active"
        ):
            raise ValueError("refusing an existing alias without its active managed address unit")
    allowed = command(["wg", "show", INTERFACE, "allowed-ips"]).splitlines()
    if len(allowed) != 1 or allowed[0].split()[1:] != [f"{peer}/32"]:
        raise ValueError("staging peer must match the sole WireGuard AllowedIPs /32")
    routes = json.loads(command(["ip", "-j", "-4", "route", "get", peer]))
    if len(routes) != 1 or routes[0].get("dev") != INTERFACE:
        raise ValueError("staging peer route must use the dedicated WireGuard interface")


def write_unit(path: Path, value: str) -> None:
    fd, name = tempfile.mkstemp(prefix=".loom-nebius-forward-", dir=DIRECTORY)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def property_value(unit: str, name: str) -> str:
    return command(["systemctl", "show", f"--property={name}", "--value", unit])


def install(listen_address: str, peer_address: str) -> dict[str, object]:
    if os.geteuid() != 0:
        raise ValueError("run through the authorized root installer")
    trusted(DIRECTORY, directory=True)
    trusted(Path("/usr/bin/ip"))
    units = render(listen_address, peer_address, proxy_binary())
    fd = os.open(
        DIRECTORY / ".loom-nebius-forward.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(fd, "w") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077:
            raise ValueError("installer lock must be a root-owned private regular file")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        check_network(address(listen_address), address(peer_address))
        return reconcile(units)


def reconcile(units: dict[str, str]) -> dict[str, object]:
    previous: dict[str, str | None] = {}
    active: dict[str, bool] = {}
    enabled: dict[str, bool] = {}
    for name in units:
        path = DIRECTORY / name
        if path.exists() or path.is_symlink():
            trusted(path)
            old = path.read_text(encoding="utf-8")
            previous[name] = old
            if not old.startswith(MARKER):
                raise ValueError("refusing an unmanaged forwarder unit")
        else:
            previous[name] = None
        fragment = property_value(name, "FragmentPath")
        if (fragment and fragment != str(path)) or property_value(name, "DropInPaths"):
            raise ValueError("refusing unmanaged forwarder fragments or drop-ins")
        state = property_value(name, "ActiveState")
        enable_state = property_value(name, "UnitFileState")
        if state not in ("", "active", "inactive", "failed") or enable_state not in (
            "",
            "enabled",
            "disabled",
            "static",
        ):
            raise ValueError("forwarder state is transitional or not managed by this installer")
        active[name] = state == "active"
        enabled[name] = enable_state == "enabled"
        if previous[name] is None and (active[name] or enabled[name]):
            raise ValueError("refusing an unmanaged active or enabled forwarder")
    sockets = [name for name in units if name.endswith(".socket")]
    services = [name for name in units if name.endswith(".service") and name != ADDRESS_UNIT]
    changed = [name for name in units if units[name] != previous[name]]
    try:
        if ADDRESS_UNIT in changed and active[ADDRESS_UNIT]:
            command(["systemctl", "stop", *services, *sockets])
            command(["systemctl", "stop", ADDRESS_UNIT])
        for name in changed:
            write_unit(DIRECTORY / name, units[name])
        if changed:
            command(["systemctl", "daemon-reload"])
        command(["systemctl", "start", ADDRESS_UNIT])
        if property_value(ADDRESS_UNIT, "ActiveState") != "active":
            raise RuntimeError("private address failed activation readback")
        for name in sockets:
            pair_changed = name in changed or name.replace(".socket", ".service") in changed
            command(["systemctl", "restart" if pair_changed else "start", name])
            if pair_changed or not enabled[name]:
                command(["systemctl", "enable", name])
            if (
                property_value(name, "ActiveState") != "active"
                or property_value(name, "UnitFileState") != "enabled"
            ):
                raise RuntimeError("forwarder socket failed active/enabled readback")
        return {"changed": bool(changed), "sockets_active": len(sockets), "ports": list(PORTS)}
    except (RuntimeError, OSError):
        # Restore every owned unit even if one rollback command fails. Never touch WG itself.
        failures = []

        def restore_command(argv: list[str]) -> None:
            try:
                command(argv)
            except (RuntimeError, OSError):
                failures.append(True)

        for name in services + sockets + [ADDRESS_UNIT]:
            restore_command(["systemctl", "stop", name])
        for name in sockets:
            restore_command(["systemctl", "enable" if enabled[name] else "disable", name])
        for name in changed:
            try:
                prior_content = previous[name]
                if prior_content is None:
                    (DIRECTORY / name).unlink(missing_ok=True)
                else:
                    write_unit(DIRECTORY / name, prior_content)
            except OSError:
                failures.append(True)
        restore_command(["systemctl", "daemon-reload"])
        for name in [ADDRESS_UNIT, *sockets, *services]:
            if active[name]:
                restore_command(["systemctl", "start", name])
        if failures:
            raise RuntimeError(
                "forwarder install failed; rollback requires operator attention"
            ) from None
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("render", "install"))
    parser.add_argument("--listen-address", required=True)
    parser.add_argument("--peer-address", required=True)
    args = parser.parse_args()
    try:
        result = (render if args.action == "render" else install)(
            args.listen_address, args.peer_address
        )
        print(json.dumps(result, sort_keys=True))
    except (ValueError, RuntimeError, OSError) as exc:
        message = str(exc) if isinstance(exc, (ValueError, RuntimeError)) else type(exc).__name__
        print(f"gateway-proxy: {message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
