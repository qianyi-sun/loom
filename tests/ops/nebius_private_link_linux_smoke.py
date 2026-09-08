"""Run only in a disposable Linux container with WireGuard tools and NET_ADMIN.

Exercises real key generation, concurrent preparation, wg-quick and kernel
configuration. No systemd, WAN handshake, service routing or staging claim.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from scripts.ops import nebius_private_link as link


def main() -> None:
    script = Path(link.__file__)

    def prepare(_: int) -> dict[str, str]:
        result = subprocess.run(
            ["python3", str(script), "prepare"], capture_output=True, text=True, check=True
        )
        return json.loads(result.stdout)

    with ThreadPoolExecutor(max_workers=4) as executor:
        identities = list(executor.map(prepare, range(8)))
    assert len({identity["public_key"] for identity in identities}) == 1
    private_key = link.private_file(link.DIRECTORY / f"{link.INTERFACE}.key").strip()
    peer_key = link.command(["wg", "genkey"])
    peer_public = link.command(["wg", "pubkey"], data=peer_key)
    args = argparse.Namespace(
        address="10.253.176.1/32",
        peer_address="10.253.176.2/32",
        peer_public_key=peer_public,
        listen_port=51871,
        endpoint=None,
    )
    config = link.DIRECTORY / f"{link.INTERFACE}.conf"
    link.write_private(config, link.render(args, private_key))
    link.command(["wg-quick", "up", link.INTERFACE])
    try:
        assert (
            link.command(["wg", "show", link.INTERFACE, "public-key"])
            == identities[0]["public_key"]
        )
        assert link.command(["wg", "show", link.INTERFACE, "allowed-ips"]).split() == [
            peer_public,
            args.peer_address,
        ]
        routes = json.loads(
            link.command(["ip", "-j", "-4", "route", "show", "dev", link.INTERFACE])
        )
        assert len(routes) == 1 and routes[0]["dst"] == "10.253.176.2"
    finally:
        link.command(["wg-quick", "down", link.INTERFACE])
    assert link.INTERFACE not in link.command(["wg", "show", "interfaces"]).split()
    print(
        json.dumps(
            {
                "concurrent_identity_stable": True,
                "kernel_peer_configured": True,
                "peer_route_only": True,
                "interface_cleanup_complete": True,
            }
        )
    )


if __name__ == "__main__":
    main()
