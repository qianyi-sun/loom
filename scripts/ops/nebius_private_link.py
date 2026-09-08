#!/usr/bin/env python3
"""Reconcile the dedicated Loom host-to-host WireGuard transport (not service routes)."""

from __future__ import annotations

import argparse
import base64
import fcntl
import ipaddress
import json
import os
import stat
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

INTERFACE = "wg-loom-nb"
DIRECTORY = Path("/etc/wireguard")
MARKER = "# Managed by Loom nebius_private_link.py; host transport only\n"


def command(argv: list[str], *, data: str | None = None) -> str:
    result = subprocess.run(argv, input=data, text=True, capture_output=True, check=False)
    if result.returncode:
        # wg-quick diagnostics can contain configuration: never relay them.
        raise RuntimeError(f"{argv[0]} operation failed (exit {result.returncode})")
    return result.stdout.strip()


def private_file(path: Path) -> str:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077:
        raise ValueError("WireGuard file must be a root-owned private regular file")
    return path.read_text(encoding="utf-8")


def write_private(path: Path, value: str) -> None:
    fd, name = tempfile.mkstemp(prefix=".loom-nebius-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def key(value: str) -> str:
    try:
        decoded = base64.b64decode(value, validate=True)
    except ValueError as exc:
        raise ValueError("invalid WireGuard key") from exc
    if len(decoded) != 32 or base64.b64encode(decoded).decode() != value:
        raise ValueError("invalid WireGuard key")
    return value


def host_address(value: str) -> ipaddress.IPv4Interface:
    address = ipaddress.ip_interface(value)
    if not isinstance(address, ipaddress.IPv4Interface) or address.network.prefixlen != 32:
        raise ValueError("transport address must be an IPv4 /32")
    if not any(
        address.ip in network
        for network in (
            ipaddress.ip_network("10.0.0.0/8"),
            ipaddress.ip_network("172.16.0.0/12"),
            ipaddress.ip_network("192.168.0.0/16"),
        )
    ):
        raise ValueError("transport address must be RFC1918")
    return address


def render(args: argparse.Namespace, private_key: str) -> str:
    local, peer = host_address(args.address), host_address(args.peer_address)
    if local.ip == peer.ip:
        raise ValueError("peer and local transport addresses must differ")
    if not 1024 <= args.listen_port <= 65535:
        raise ValueError("listen port must be between 1024 and 65535")
    endpoint = ""
    if args.endpoint:
        host, port = args.endpoint.rsplit(":", 1)
        if not isinstance(ipaddress.ip_address(host), ipaddress.IPv4Address):
            raise ValueError("endpoint must use a fixed IPv4 allocation")
        if not 1024 <= int(port) <= 65535:
            raise ValueError("invalid endpoint port")
        endpoint = f"Endpoint = {host}:{int(port)}\nPersistentKeepalive = 25\n"
    return (
        MARKER
        + f"[Interface]\nAddress = {local}\nListenPort = {args.listen_port}\n"
        + f"PrivateKey = {key(private_key)}\nMTU = 1380\n\n"
        + f"[Peer]\nPublicKey = {key(args.peer_public_key)}\nAllowedIPs = {peer}\n"
        + endpoint
    )


def check_routes(addresses: list[str]) -> None:
    targets = [host_address(value).ip for value in addresses]
    routes = json.loads(command(["ip", "-j", "-4", "route", "show", "table", "all"]))
    for route in routes:
        destination = route.get("dst")
        if destination in (None, "default") or route.get("dev") == INTERFACE:
            continue
        network = ipaddress.ip_network(destination, strict=False)
        if any(target in network for target in targets):
            raise ValueError("transport address conflicts with an existing route")


def reconcile(args: argparse.Namespace) -> dict[str, object]:
    if os.geteuid() != 0:
        raise ValueError("run through the authorized root installer")
    DIRECTORY.mkdir(mode=0o700, parents=True, exist_ok=True)
    if DIRECTORY.is_symlink() or DIRECTORY.stat().st_uid != 0 or DIRECTORY.stat().st_mode & 0o022:
        raise ValueError(
            "WireGuard directory must be root-owned and not writable by others or a symlink"
        )
    with exclusive_lock():
        return reconcile_locked(args)


@contextmanager
def exclusive_lock() -> Iterator[None]:
    fd = os.open(DIRECTORY / f"{INTERFACE}.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077:
            raise ValueError("WireGuard lock must be a root-owned private regular file")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield


def reconcile_locked(args: argparse.Namespace) -> dict[str, object]:
    config_path = DIRECTORY / f"{INTERFACE}.conf"
    key_path = DIRECTORY / f"{INTERFACE}.key"
    if config_path.is_symlink() or key_path.is_symlink():
        raise ValueError("refusing to replace a WireGuard symlink")
    previous = private_file(config_path) if config_path.exists() else None
    if previous is not None and not previous.startswith(MARKER):
        raise ValueError("refusing to replace an unmanaged WireGuard configuration")
    if args.action == "prepare":
        if not key_path.exists():
            if previous is not None:
                raise ValueError("managed configuration exists but identity key is missing")
            write_private(key_path, key(command(["wg", "genkey"])) + "\n")
        private_key = key(private_file(key_path).strip())
        return {"interface": INTERFACE, "public_key": command(["wg", "pubkey"], data=private_key)}
    private_key = key(private_file(key_path).strip())
    desired = render(args, private_key)
    check_routes([args.address, args.peer_address])
    unit = f"wg-quick@{INTERFACE}.service"
    existing_interfaces = command(["wg", "show", "interfaces"]).split()
    if previous is None and INTERFACE in existing_interfaces:
        raise ValueError("refusing to adopt an unmanaged active WireGuard interface")
    was_active = (
        command(["systemctl", "show", "--property=ActiveState", "--value", unit]) == "active"
    )
    was_enabled = (
        command(["systemctl", "show", "--property=UnitFileState", "--value", unit]) == "enabled"
    )
    changed = desired != previous
    if changed:
        write_private(config_path, desired)
    try:
        command(["systemctl", "restart" if changed else "start", unit])
        command(["systemctl", "enable", unit])
    except RuntimeError:
        if changed and previous is not None:
            write_private(config_path, previous)
        command(["systemctl", "restart" if was_active else "stop", unit])
        command(["systemctl", "enable" if was_enabled else "disable", unit])
        if changed and previous is None:
            config_path.unlink()
        raise
    return {
        "interface": INTERFACE,
        "changed": changed,
        "active": command(["systemctl", "is-active", unit]) == "active",
        "enabled": command(["systemctl", "is-enabled", unit]) == "enabled",
        "service_routes_configured": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    commands.add_parser(
        "prepare", help="Create or reuse host-local identity; print public key only"
    )
    configure = commands.add_parser("configure", help="Reconcile the dedicated /32 peer transport")
    configure.add_argument("--address", required=True)
    configure.add_argument("--peer-address", required=True)
    configure.add_argument("--peer-public-key", required=True)
    configure.add_argument("--listen-port", type=int, default=51871)
    configure.add_argument("--endpoint", help="Peer fixed IPv4:port; omit on the public gateway")
    try:
        print(json.dumps(reconcile(parser.parse_args()), sort_keys=True))
    except (ValueError, RuntimeError, OSError) as exc:
        # OSError filenames may be sensitive; only validation errors are safe to relay.
        message = str(exc) if isinstance(exc, (ValueError, RuntimeError)) else type(exc).__name__
        print(f"private-link: {message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
