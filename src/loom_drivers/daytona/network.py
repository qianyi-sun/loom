"""NetworkPolicy → Daytona update_network_settings argument mapping.

Daytona's network API is CIDR-only; domain allowlists are resolved to
/32 entries by the driver before this mapping runs. If a domain fails
to resolve we raise ValueError; the caller (DaytonaDriver
.set_network_policy) converts it to DriverError.
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
    network_block_all: bool
    network_allow_list: str | None


def to_daytona_network_args(
    policy: NetworkPolicy,
    *,
    resolved_domain_ips: dict[str, tuple[str, ...]],
) -> DaytonaNetworkArgs:
    if isinstance(policy, Public):
        return DaytonaNetworkArgs(
            network_block_all=False, network_allow_list=None,
        )
    if isinstance(policy, NoNetwork):
        return DaytonaNetworkArgs(
            network_block_all=True, network_allow_list=None,
        )
    if isinstance(policy, Allowlist):
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
                network_block_all=False, network_allow_list=None,
            )
        return DaytonaNetworkArgs(
            network_block_all=False,
            network_allow_list=",".join(entries),
        )
    raise TypeError(f"unsupported NetworkPolicy kind: {type(policy).__name__}")
