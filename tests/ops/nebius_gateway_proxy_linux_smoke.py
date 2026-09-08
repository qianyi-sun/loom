"""Run ONLY in a disposable Linux container with NET_ADMIN; no live/cloud resources.

Install python3, systemd, iproute2 and wireguard-tools without recommended packages.
Mount the repo read-only, set PYTHONPATH=/repo and
LOOM_NEBIUS_DISPOSABLE_SMOKE=1, then execute this file. The container must be removed
afterwards. This validates real unit parsing, alias binding and proxy byte forwarding,
not PID 1 restart behavior, a Nebius allocation, WAN transport, or staging acceptance.
"""

from __future__ import annotations

import json
import os
import shlex
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from scripts.ops import nebius_gateway_proxy as proxy

LISTEN = "10.20.30.40"
PEER = "10.253.176.2"


def roundtrip(port: int, service: str) -> None:
    upstream = socket.socket()
    upstream.settimeout(10)
    upstream.bind((PEER, port))
    upstream.listen(1)
    errors: list[Exception] = []
    message = b"loom-private-forwarder\x00\xff-byte-roundtrip"

    def echo() -> None:
        try:
            connection, _ = upstream.accept()
            with connection:
                connection.settimeout(5)
                payload = b""
                while len(payload) < len(message):
                    chunk = connection.recv(4096)
                    if not chunk:
                        raise RuntimeError("upstream received incomplete payload")
                    payload += chunk
                connection.sendall(payload)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=echo, daemon=True)
    thread.start()
    exec_start = next(
        line.removeprefix("ExecStart=")
        for line in service.splitlines()
        if line.startswith("ExecStart=")
    )
    process = subprocess.Popen(
        ["systemd-socket-activate", "-l", f"{LISTEN}:{port}", *shlex.split(exec_start)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                connection = socket.create_connection((LISTEN, port), timeout=2)
                break
            except ConnectionRefusedError:
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("socket activation failed") from None
                time.sleep(0.05)
        with connection:
            connection.sendall(message)
            received = b""
            while len(received) < len(message):
                chunk = connection.recv(4096)
                if not chunk:
                    break
                received += chunk
            assert received == message, "proxy byte roundtrip mismatch"
        thread.join(timeout=5)
        assert not thread.is_alive() and not errors, "upstream roundtrip failed"
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        upstream.close()
        thread.join(timeout=1)


def main() -> None:
    if os.environ.get("LOOM_NEBIUS_DISPOSABLE_SMOKE") != "1" or os.geteuid() != 0:
        raise RuntimeError("explicit disposable root Linux container required")
    proxy.trusted(Path("/usr/bin/ip"))
    units = proxy.render(LISTEN, PEER, proxy.proxy_binary())
    with tempfile.TemporaryDirectory(prefix="loom-proxy-unit-smoke-") as directory:
        paths = []
        for name, value in units.items():
            path = Path(directory) / name
            path.write_text(value, encoding="utf-8")
            paths.append(str(path))
        result = subprocess.run(["systemd-analyze", "verify", *paths], capture_output=True)
        assert result.returncode == 0, "systemd unit verification failed"
    added = []
    try:
        for address, label in ((LISTEN, proxy.ADDRESS_LABEL), (PEER, "lo:loom-test")):
            subprocess.run(
                ["ip", "address", "add", f"{address}/32", "dev", "lo", "label", label],
                check=True,
                capture_output=True,
            )
            added.append(address)
        for port in proxy.PORTS:
            roundtrip(port, units[f"loom-nebius-forward-{port}.service"])
    finally:
        for address in reversed(added):
            subprocess.run(
                ["ip", "address", "del", f"{address}/32", "dev", "lo"],
                check=True,
                capture_output=True,
            )
    print(
        json.dumps(
            {
                "units_verified": len(units),
                "tcp_roundtrips": len(proxy.PORTS),
                "aliases_removed": len(added),
            }
        )
    )


if __name__ == "__main__":
    main()
