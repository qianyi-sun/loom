"""Private staging entry validation; no network, credential or root mutations."""

from __future__ import annotations

import ipaddress
import re

from loom_cli.cluster_config import ClusterConfig

_PRIVATE = tuple(
    ipaddress.ip_network(cidr)
    for cidr in (
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
    )
)
_LABEL = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
_DNS = re.compile(rf"{_LABEL}(?:\.{_LABEL})*\Z")
PRIVATE_ENTRY_PORTS = frozenset({15432, 18443, 19443})


def private_ipv4(value: object) -> bool:
    """RFC1918 only, excluding Python's broader 'private' reserved ranges."""
    if not isinstance(value, str):
        return False
    try:
        address = ipaddress.IPv4Address(value)
    except ipaddress.AddressValueError:
        return False
    return any(address in network for network in _PRIVATE)


def validate_private_entry(config: ClusterConfig) -> None:
    entry = config.nebius_private_entry
    if not entry.enabled:
        return
    if (
        config.runtime_environment != "staging"
        or config.namespace != "loom-staging"
        or not config.topology.multi_node
    ):
        raise ValueError("nebius_private_entry requires canonical multi-node staging")
    for field, value in (
        ("node_name", entry.node_name),
        ("ingress_host", config.ingress_host),
        ("ingress_tls_secret_name", config.ingress_tls_secret_name),
    ):
        limit = 63 if field == "node_name" else 253
        if not isinstance(value, str) or len(value) > limit or not _DNS.fullmatch(value):
            raise ValueError(f"nebius_private_entry requires a valid {field}")
    try:
        ipaddress.ip_address(config.ingress_host)
    except ValueError:
        pass
    else:
        raise ValueError("nebius_private_entry requires a certificate DNS name, not an IP")
    if (
        not private_ipv4(entry.wireguard_address)
        or not private_ipv4(entry.peer_address)
        or entry.wireguard_address == entry.peer_address
    ):
        raise ValueError("nebius_private_entry requires distinct RFC1918 IPv4 peer addresses")
    if not re.fullmatch(r"[A-Za-z0-9./_:-]+@sha256:[0-9a-f]{64}", entry.proxy_image):
        raise ValueError("nebius_private_entry.proxy_image must be digest-pinned")
