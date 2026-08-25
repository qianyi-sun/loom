"""NetworkPolicy → mutually exclusive Daytona firewall arguments.

Daytona supports native domain and IPv4 CIDR allowlist modes. A mixed Loom
policy is collapsed to CIDRs after trusted-worker DNS resolution because the
provider rejects domain and CIDR modes when they are supplied together.
"""

from __future__ import annotations

from dataclasses import dataclass

from loom.models.networking import (
    Allowlist,
    NetworkPolicy,
    NoNetwork,
    Public,
)


@dataclass(frozen=True)
class DaytonaNetworkArgs:
    network_block_all: bool | None
    network_allow_list: str | None
    domain_allow_list: str | None


def to_daytona_network_args(
    policy: NetworkPolicy,
    *,
    resolved_domain_ips: dict[str, tuple[str, ...]],
) -> DaytonaNetworkArgs:
    if isinstance(policy, Public):
        return DaytonaNetworkArgs(
            network_block_all=False,
            network_allow_list=None,
            domain_allow_list=None,
        )
    if isinstance(policy, NoNetwork):
        return DaytonaNetworkArgs(
            network_block_all=True,
            network_allow_list=None,
            domain_allow_list=None,
        )
    if isinstance(policy, Allowlist):
        if policy.domains and not policy.cidrs:
            return DaytonaNetworkArgs(
                network_block_all=None,
                network_allow_list=None,
                domain_allow_list=",".join(policy.domains),
            )
        entries: list[str] = list(policy.cidrs)
        for domain in policy.domains:
            ips = resolved_domain_ips.get(domain)
            if not ips:
                raise ValueError(
                    f"Allowlist domain {domain!r} did not resolve to any "
                    f"IPv4 address; Daytona network API requires CIDRs only",
                )
            entries.extend(f"{ip}/32" for ip in ips)
        if not entries:
            return DaytonaNetworkArgs(
                network_block_all=False,
                network_allow_list=None,
                domain_allow_list=None,
            )
        return DaytonaNetworkArgs(
            network_block_all=None,
            network_allow_list=",".join(entries),
            domain_allow_list=None,
        )
    raise TypeError(f"unsupported NetworkPolicy kind: {type(policy).__name__}")
