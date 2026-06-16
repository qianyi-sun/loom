"""Provider-connection service layer.

The route handlers are thin orchestrators; this module owns the
business logic:

- URL parsing + upstream-host derivation.
- DNS resolution + SSRF IP classification (RFC1918, ULA, loopback,
  link-local, multicast, broadcast, unspecified). The team-level
  `allow_private_endpoints` flag relaxes RFC1918 + ULA; loopback +
  link-local stay rejected unconditionally because no legitimate
  provider hosts on those.
- Pricing-source validation (the `operator-supplied` source requires
  `pricing_data` with numeric `{input_usd_per_1m, output_usd_per_1m}`).

Separated from the routes so each piece is testable without a
FastAPI request fixture; the routes file is a thin orchestrator.
"""

from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass
from urllib.parse import urlparse

# Public IP classifications that are always SSRF targets — rejected
# even when team.allow_private_endpoints=True. These are network
# constructs that are never legitimate provider hosts on any topology.
_UNCONDITIONALLY_REJECTED_IPS = (
    # IPv4
    ipaddress.IPv4Network("0.0.0.0/8"),       # this network / unspecified
    ipaddress.IPv4Network("127.0.0.0/8"),     # loopback
    ipaddress.IPv4Network("169.254.0.0/16"),  # link-local
    ipaddress.IPv4Network("224.0.0.0/4"),     # multicast
    ipaddress.IPv4Network("240.0.0.0/4"),     # reserved (incl. broadcast)
    # IPv6
    ipaddress.IPv6Network("::1/128"),         # loopback
    ipaddress.IPv6Network("::/128"),          # unspecified
    ipaddress.IPv6Network("fe80::/10"),       # link-local
    ipaddress.IPv6Network("ff00::/8"),        # multicast
)

# Private IP classifications — rejected by default but permitted when
# team.allow_private_endpoints=True. Operators with on-prem providers
# (vLLM in the lab, hosted Llama on internal subnet) need the opt-in.
_PRIVATE_IPS = (
    # IPv4 RFC1918
    ipaddress.IPv4Network("10.0.0.0/8"),
    ipaddress.IPv4Network("172.16.0.0/12"),
    ipaddress.IPv4Network("192.168.0.0/16"),
    # IPv4 CGNAT (RFC6598)
    ipaddress.IPv4Network("100.64.0.0/10"),
    # IPv6 ULA
    ipaddress.IPv6Network("fc00::/7"),
)


class InvalidBaseUrlError(Exception):
    """Base URL fails parsing, scheme check, or has no resolvable host."""


class SsrfRejectedError(Exception):
    """The resolved IPs include addresses the SSRF policy forbids."""

    def __init__(self, message: str, *, ips: list[str]) -> None:
        super().__init__(message)
        self.ips = ips


class InvalidPricingError(Exception):
    """pricing_source / pricing_data combination is invalid."""


@dataclass(frozen=True)
class ResolvedUpstream:
    """A `base_url` validated + resolved for provider_connection storage."""

    base_url: str
    upstream_host: str
    resolved_ips: list[str]


def derive_upstream_host(base_url: str) -> str:
    """Parse `base_url` and return the hostname portion.

    Used both for the `provider_connections.upstream_host` column (the
    string the egress proxy validates SNI against) and for the DNS
    resolution that populates `resolved_egress_ips`. Raises on missing
    scheme or host.
    """
    if not base_url:
        raise InvalidBaseUrlError("base_url must be non-empty")
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https"):
        raise InvalidBaseUrlError(
            f"base_url scheme must be http or https; got {parsed.scheme!r}",
        )
    if not parsed.hostname:
        raise InvalidBaseUrlError(
            f"base_url has no hostname: {base_url!r}",
        )
    return parsed.hostname


def classify_ip(
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
    *,
    allow_private: bool,
) -> str | None:
    """Return None if `ip` is allowed; otherwise a human-readable reason
    naming the rejection category. The `allow_private` flag relaxes the
    `_PRIVATE_IPS` set; the `_UNCONDITIONALLY_REJECTED_IPS` set stays
    rejected.
    """
    for net in _UNCONDITIONALLY_REJECTED_IPS:
        if ip.version == net.version and ip in net:
            return f"address {ip} is in {net} (never legitimate as a provider host)"
    if not allow_private:
        for net in _PRIVATE_IPS:
            if ip.version == net.version and ip in net:
                return (
                    f"address {ip} is in private range {net}; set "
                    f"team.allow_private_endpoints=True to permit on-prem providers"
                )
    return None


def resolve_and_validate(
    base_url: str, *, allow_private: bool,
    _resolver: object = None,
) -> ResolvedUpstream:
    """Parse + DNS-resolve `base_url`; validate every resolved IP against
    the SSRF policy.

    `_resolver`: optional override for testability. Must be a callable
    matching `socket.getaddrinfo(host, port=None)` — tests inject a
    stub to avoid hitting real DNS. Default uses stdlib `socket`.
    """
    upstream_host = derive_upstream_host(base_url)
    resolver = _resolver if _resolver is not None else socket.getaddrinfo

    try:
        # type=SOCK_STREAM filters down to TCP results so we don't see
        # the same address twice (UDP + TCP duplicates).
        infos = resolver(  # type: ignore[operator]
            upstream_host, None, type=socket.SOCK_STREAM,
        )
    except socket.gaierror as e:
        raise InvalidBaseUrlError(
            f"DNS resolution failed for {upstream_host!r}: {e}",
        ) from e

    ips: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    seen: set[str] = set()
    for info in infos:
        sockaddr = info[4]
        ip_str = sockaddr[0]
        # IPv6 results can include zone-id suffix (e.g. "fe80::1%eth0");
        # strip before parsing.
        ip_str_clean = ip_str.split("%", 1)[0]
        if ip_str_clean in seen:
            continue
        seen.add(ip_str_clean)
        ips.append(ipaddress.ip_address(ip_str_clean))

    if not ips:
        raise InvalidBaseUrlError(
            f"DNS resolution for {upstream_host!r} returned no addresses",
        )

    rejected_reasons: list[str] = []
    for ip in ips:
        reason = classify_ip(ip, allow_private=allow_private)
        if reason is not None:
            rejected_reasons.append(reason)
    if rejected_reasons:
        raise SsrfRejectedError(
            "; ".join(rejected_reasons),
            ips=[str(ip) for ip in ips],
        )

    return ResolvedUpstream(
        base_url=base_url,
        upstream_host=upstream_host,
        resolved_ips=[str(ip) for ip in ips],
    )


def validate_pricing(
    pricing_source: str, pricing_data: dict[str, float] | None,
) -> None:
    """Enforce the cost-source × pricing_data contract.

    - `rate-card` / `tokens-only`: pricing_data MUST be NULL (any value
      is ignored at cost-compute time, so accepting it would be a UX
      footgun — operators expect what they set to be used).
    - `operator-supplied`: pricing_data MUST contain non-negative
      numeric `input_usd_per_1m` AND `output_usd_per_1m`.
    """
    if pricing_source not in {"rate-card", "tokens-only", "operator-supplied"}:
        raise InvalidPricingError(
            f"pricing_source must be one of "
            f"{{'rate-card', 'tokens-only', 'operator-supplied'}}; "
            f"got {pricing_source!r}",
        )
    if pricing_source == "operator-supplied":
        if not isinstance(pricing_data, dict):
            raise InvalidPricingError(
                "pricing_source='operator-supplied' requires pricing_data "
                "with input_usd_per_1m + output_usd_per_1m",
            )
        for key in ("input_usd_per_1m", "output_usd_per_1m"):
            value = pricing_data.get(key)
            if value is None:
                raise InvalidPricingError(
                    f"pricing_data.{key} is required for "
                    f"pricing_source='operator-supplied'",
                )
            if not isinstance(value, int | float) or isinstance(value, bool):
                raise InvalidPricingError(
                    f"pricing_data.{key} must be a number; got {type(value).__name__}",
                )
            if value < 0:
                raise InvalidPricingError(
                    f"pricing_data.{key} must be >= 0; got {value}",
                )
    elif pricing_data is not None:
        raise InvalidPricingError(
            f"pricing_data must be NULL for pricing_source={pricing_source!r}; "
            f"got {pricing_data!r}",
        )


def default_pricing_source_for(provider_type: str) -> str:
    """Per cluster-deploy.md §Cost computation:
    - anthropic, google → rate-card
    - openai-compatible, custom → tokens-only
    """
    if provider_type in ("anthropic", "google"):
        return "rate-card"
    return "tokens-only"
