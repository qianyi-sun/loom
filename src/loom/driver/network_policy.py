"""Translate a NetworkPolicy into iptables rules that DockerDriver applies
inside the container's network namespace (spec §2.2 + §2.6).

This module is pure data — no side effects. `DockerDriver.set_network_policy`
runs the resulting commands inside the container.

Rule model:
- `flush`: clear existing OUTPUT rules before applying
- `outbound_drops`: list of (chain, action) tuples for default drop policy
- `allowed_domains` / `allowed_cidrs`: targets to whitelist

Known limitation: Allowlist resolves domains at apply-time via `getent hosts`
and pins the resulting IPs. Subsequent DNS lookups inside the container go
through the default DROP and will fail. A future iteration can add an
explicit ACCEPT for UDP/53 to the host's resolvers, or pre-populate
/etc/hosts with the resolved entries.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from loom.models.networking import Allowlist, NetworkPolicy, NoNetwork, Public


@dataclass(frozen=True)
class IptablesRule:
    chain: str          # e.g. "OUTPUT"
    action: str         # "ACCEPT" | "DROP"
    target: str | None = None


@dataclass(frozen=True)
class IptablesPlan:
    flush: bool
    outbound_drops: list[IptablesRule] = field(default_factory=list)
    allowed_domains: list[str] = field(default_factory=list)
    allowed_cidrs: list[str] = field(default_factory=list)


def compute_iptables_rules(policy: NetworkPolicy) -> IptablesPlan:
    if isinstance(policy, Public):
        return IptablesPlan(flush=True)
    if isinstance(policy, NoNetwork):
        return IptablesPlan(
            flush=True,
            outbound_drops=[IptablesRule(chain="OUTPUT", action="DROP")],
        )
    if isinstance(policy, Allowlist):
        return IptablesPlan(
            flush=True,
            outbound_drops=[IptablesRule(chain="OUTPUT", action="DROP")],
            allowed_domains=list(policy.domains),
            allowed_cidrs=list(policy.cidrs),
        )
    raise ValueError(f"unknown NetworkPolicy kind: {type(policy).__name__}")


def render_iptables_commands(plan: IptablesPlan) -> list[str]:
    """Render a plan as a list of shell commands suitable for `sh -c` inside
    a container.

    For a Public-equivalent plan (no drops, no allows) we return an empty
    list so vanilla images without iptables installed still work. Containers
    running NoNetwork or Allowlist policies must have iptables present.
    """
    if not plan.outbound_drops and not plan.allowed_domains and not plan.allowed_cidrs:
        return []
    cmds: list[str] = []
    if plan.flush:
        cmds.append("iptables -F OUTPUT || true")
        cmds.append("iptables -F INPUT || true")
    cmds.append("iptables -A OUTPUT -o lo -j ACCEPT")
    cmds.append("iptables -A OUTPUT -m state --state ESTABLISHED,RELATED -j ACCEPT")
    for cidr in plan.allowed_cidrs:
        cmds.append(f"iptables -A OUTPUT -d {cidr} -j ACCEPT")
    for domain in plan.allowed_domains:
        # Resolve at apply-time, pin to /etc/hosts so subsequent connections
        # don't need DNS (which would be blocked by the default DROP), and
        # ACCEPT the resolved IPs. Use `getent ahosts` (vs `hosts`) so we
        # see all A and AAAA records, then filter to IPv4 only — ip6tables
        # is a separate tool; IPv6 enforcement is a future iteration.
        cmds.append(
            f'for ip in $(getent ahosts {domain} | awk \'$2 == "STREAM" && $1 !~ /:/ {{print $1}}\' | sort -u); do '
            f'grep -q "$ip {domain}" /etc/hosts || echo "$ip {domain}" >> /etc/hosts; '
            f'iptables -A OUTPUT -d "$ip" -j ACCEPT; '
            f"done",
        )
    for rule in plan.outbound_drops:
        if rule.action == "DROP":
            cmds.append(f"iptables -P {rule.chain} DROP")
    return cmds
