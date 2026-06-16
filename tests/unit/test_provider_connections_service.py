"""Unit tests for the provider-connection service layer.

These cover the pure-function pieces (URL parsing, IP classification,
pricing validation). The DNS resolution + route integration tests live
in tests/integration/test_provider_connections_routes.py and need a
real Postgres.
"""

from __future__ import annotations

import ipaddress
import socket

import pytest

from loom_service.provider_connections_service import (
    InvalidBaseUrlError,
    InvalidPricingError,
    ResolvedUpstream,
    SsrfRejectedError,
    classify_ip,
    default_pricing_source_for,
    derive_upstream_host,
    resolve_and_validate,
    validate_pricing,
)

# ──────────────────────────────────────────────────────────────────────
# derive_upstream_host
# ──────────────────────────────────────────────────────────────────────


def test_derive_upstream_host_happy_https() -> None:
    assert derive_upstream_host("https://api.openai.com/v1") == "api.openai.com"


def test_derive_upstream_host_happy_http() -> None:
    assert derive_upstream_host("http://vllm.lab.local:8000/v1") == "vllm.lab.local"


def test_derive_upstream_host_strips_port_and_path() -> None:
    assert derive_upstream_host("https://gw.example.com:8443/openai/v1/chat") == "gw.example.com"


def test_derive_upstream_host_rejects_empty() -> None:
    with pytest.raises(InvalidBaseUrlError, match="non-empty"):
        derive_upstream_host("")


def test_derive_upstream_host_rejects_non_http_scheme() -> None:
    with pytest.raises(InvalidBaseUrlError, match="scheme must be http"):
        derive_upstream_host("ftp://example.com/")


def test_derive_upstream_host_rejects_missing_scheme() -> None:
    with pytest.raises(InvalidBaseUrlError, match="scheme must be http"):
        derive_upstream_host("api.openai.com/v1")


def test_derive_upstream_host_rejects_missing_host() -> None:
    with pytest.raises(InvalidBaseUrlError, match="has no hostname"):
        derive_upstream_host("https:///path-only")


# ──────────────────────────────────────────────────────────────────────
# classify_ip — IPv4
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("ip_str,allow_private", [
    ("127.0.0.1", False),
    ("127.0.0.1", True),       # loopback rejected even with flag on
    ("127.255.255.255", True),
    ("169.254.169.254", False),  # AWS / GCP metadata IP
    ("169.254.169.254", True),
    ("0.0.0.0", False),
    ("0.1.2.3", True),
    ("224.0.0.1", True),       # multicast
    ("255.255.255.255", True), # broadcast (in 240.0.0.0/4)
])
def test_classify_ip_v4_unconditionally_rejected(
    ip_str: str, allow_private: bool,
) -> None:
    ip = ipaddress.IPv4Address(ip_str)
    reason = classify_ip(ip, allow_private=allow_private)
    assert reason is not None, f"{ip_str} should be rejected"
    assert "never legitimate" in reason


@pytest.mark.parametrize("ip_str", [
    "10.0.0.1",        # RFC1918 /8
    "10.255.255.255",
    "172.16.0.1",      # RFC1918 /12
    "172.31.255.255",
    "192.168.0.1",     # RFC1918 /16
    "192.168.1.100",
    "100.64.0.1",      # CGNAT /10
])
def test_classify_ip_v4_private_rejected_without_flag(ip_str: str) -> None:
    ip = ipaddress.IPv4Address(ip_str)
    reason = classify_ip(ip, allow_private=False)
    assert reason is not None
    assert "private range" in reason
    assert "allow_private_endpoints" in reason


@pytest.mark.parametrize("ip_str", [
    "10.0.0.1",
    "172.16.0.1",
    "192.168.1.100",
    "100.64.0.1",
])
def test_classify_ip_v4_private_allowed_with_flag(ip_str: str) -> None:
    ip = ipaddress.IPv4Address(ip_str)
    assert classify_ip(ip, allow_private=True) is None


@pytest.mark.parametrize("ip_str", [
    "1.1.1.1",
    "8.8.8.8",
    "104.18.0.1",      # cloudflare-fronted
    "199.7.83.42",
])
def test_classify_ip_v4_public_always_allowed(ip_str: str) -> None:
    ip = ipaddress.IPv4Address(ip_str)
    assert classify_ip(ip, allow_private=False) is None
    assert classify_ip(ip, allow_private=True) is None


# ──────────────────────────────────────────────────────────────────────
# classify_ip — IPv6
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("ip_str,allow_private", [
    ("::1", False),
    ("::1", True),
    ("fe80::1", False),
    ("fe80::1", True),
    ("ff02::1", True),
])
def test_classify_ip_v6_unconditionally_rejected(
    ip_str: str, allow_private: bool,
) -> None:
    ip = ipaddress.IPv6Address(ip_str)
    reason = classify_ip(ip, allow_private=allow_private)
    assert reason is not None
    assert "never legitimate" in reason


@pytest.mark.parametrize("ip_str", [
    "fc00::1",         # ULA
    "fdab:cd00::1",    # ULA
])
def test_classify_ip_v6_ula_rejected_without_flag(ip_str: str) -> None:
    ip = ipaddress.IPv6Address(ip_str)
    reason = classify_ip(ip, allow_private=False)
    assert reason is not None
    assert "private range" in reason


def test_classify_ip_v6_ula_allowed_with_flag() -> None:
    assert classify_ip(
        ipaddress.IPv6Address("fc00::1"), allow_private=True,
    ) is None


def test_classify_ip_v6_public_always_allowed() -> None:
    # 2606:4700:4700::1111 = cloudflare DNS
    ip = ipaddress.IPv6Address("2606:4700:4700::1111")
    assert classify_ip(ip, allow_private=False) is None


# ──────────────────────────────────────────────────────────────────────
# resolve_and_validate — uses _resolver override
# ──────────────────────────────────────────────────────────────────────


def _fake_resolver_returning(*ips: str):
    """Build a getaddrinfo-shaped fake resolver from a list of IP strings."""
    def _resolver(host, port, type=None):
        infos = []
        for ip in ips:
            if ":" in ip:
                infos.append((socket.AF_INET6, socket.SOCK_STREAM, 0, "", (ip, 0, 0, 0)))
            else:
                infos.append((socket.AF_INET, socket.SOCK_STREAM, 0, "", (ip, 0)))
        return infos
    return _resolver


def test_resolve_and_validate_happy_public_ipv4() -> None:
    result = resolve_and_validate(
        "https://api.openai.com/v1",
        allow_private=False,
        _resolver=_fake_resolver_returning("104.18.0.1", "104.18.0.2"),
    )
    assert isinstance(result, ResolvedUpstream)
    assert result.upstream_host == "api.openai.com"
    assert sorted(result.resolved_ips) == ["104.18.0.1", "104.18.0.2"]


def test_resolve_and_validate_happy_public_ipv6() -> None:
    result = resolve_and_validate(
        "https://api.openai.com/v1",
        allow_private=False,
        _resolver=_fake_resolver_returning("2606:4700:4700::1111"),
    )
    assert result.resolved_ips == ["2606:4700:4700::1111"]


def test_resolve_and_validate_deduplicates_repeated_ips() -> None:
    """getaddrinfo may return the same IP multiple times (TCP + UDP);
    even though we filter to SOCK_STREAM, the deduplication is
    defensive. Test asserts the contract."""
    result = resolve_and_validate(
        "https://api.example.com/",
        allow_private=False,
        _resolver=_fake_resolver_returning("1.1.1.1", "1.1.1.1", "2.2.2.2"),
    )
    assert sorted(result.resolved_ips) == ["1.1.1.1", "2.2.2.2"]


def test_resolve_and_validate_strips_ipv6_zone_id() -> None:
    """IPv6 results may include zone-id suffix; we strip before parsing
    so 'fe80::1%eth0' classifies correctly as link-local (not as a
    malformed address that bypasses checks)."""
    with pytest.raises(SsrfRejectedError):
        resolve_and_validate(
            "https://localhost.example/",
            allow_private=False,
            _resolver=_fake_resolver_returning("fe80::1%eth0"),
        )


def test_resolve_and_validate_rejects_mixed_public_private(
) -> None:
    """If ANY resolved IP is private (without flag), the whole URL is
    rejected — defense against split-DNS attacks where the public IP
    is resolved on the API side but private on the gateway side."""
    with pytest.raises(SsrfRejectedError, match="private range"):
        resolve_and_validate(
            "https://attacker.example/",
            allow_private=False,
            _resolver=_fake_resolver_returning("1.1.1.1", "10.0.0.1"),
        )


def test_resolve_and_validate_allows_private_with_flag(
) -> None:
    result = resolve_and_validate(
        "https://vllm.lab.local:8000/v1",
        allow_private=True,
        _resolver=_fake_resolver_returning("10.0.5.42"),
    )
    assert result.resolved_ips == ["10.0.5.42"]


def test_resolve_and_validate_still_rejects_loopback_with_flag(
) -> None:
    """allow_private opens RFC1918 + ULA only — loopback stays
    rejected because nothing legitimate runs as a provider on
    127.0.0.0/8 (vLLM-on-localhost case is single-box compose mode
    where the SDK dials gateway-on-bridge, not the host)."""
    with pytest.raises(SsrfRejectedError, match=r"127\.0\.0"):
        resolve_and_validate(
            "https://api.localhost/",
            allow_private=True,
            _resolver=_fake_resolver_returning("127.0.0.1"),
        )


def test_resolve_and_validate_dns_failure_raises(
) -> None:
    def _failing(host, port, type=None):
        raise socket.gaierror(-2, "Name or service not known")

    with pytest.raises(InvalidBaseUrlError, match="DNS resolution failed"):
        resolve_and_validate(
            "https://nonexistent.example/",
            allow_private=False,
            _resolver=_failing,
        )


def test_resolve_and_validate_empty_resolution_raises(
) -> None:
    def _empty(host, port, type=None):
        return []

    with pytest.raises(InvalidBaseUrlError, match="returned no addresses"):
        resolve_and_validate(
            "https://api.example/",
            allow_private=False,
            _resolver=_empty,
        )


# ──────────────────────────────────────────────────────────────────────
# validate_pricing
# ──────────────────────────────────────────────────────────────────────


def test_validate_pricing_rate_card_no_data() -> None:
    validate_pricing("rate-card", None)  # ok


def test_validate_pricing_tokens_only_no_data() -> None:
    validate_pricing("tokens-only", None)  # ok


def test_validate_pricing_rate_card_with_data_rejected() -> None:
    with pytest.raises(InvalidPricingError, match="must be NULL"):
        validate_pricing("rate-card", {"input_usd_per_1m": 1.0})


def test_validate_pricing_operator_supplied_happy() -> None:
    validate_pricing("operator-supplied", {
        "input_usd_per_1m": 2.5,
        "output_usd_per_1m": 10.0,
    })


def test_validate_pricing_operator_supplied_zero_allowed() -> None:
    """Free-tier endpoints (sometimes self-hosted on free GPUs)."""
    validate_pricing("operator-supplied", {
        "input_usd_per_1m": 0.0,
        "output_usd_per_1m": 0.0,
    })


def test_validate_pricing_operator_supplied_missing_data() -> None:
    with pytest.raises(InvalidPricingError, match="requires pricing_data"):
        validate_pricing("operator-supplied", None)


def test_validate_pricing_operator_supplied_missing_field() -> None:
    with pytest.raises(InvalidPricingError, match="output_usd_per_1m is required"):
        validate_pricing("operator-supplied", {"input_usd_per_1m": 1.0})


def test_validate_pricing_operator_supplied_negative_rejected() -> None:
    with pytest.raises(InvalidPricingError, match="must be >= 0"):
        validate_pricing("operator-supplied", {
            "input_usd_per_1m": -1.0, "output_usd_per_1m": 2.0,
        })


def test_validate_pricing_operator_supplied_bool_rejected() -> None:
    """bools are technically int subclasses in Python; the validator
    must reject them or operators could store True/False as price."""
    with pytest.raises(InvalidPricingError, match="must be a number"):
        validate_pricing("operator-supplied", {
            "input_usd_per_1m": True, "output_usd_per_1m": 1.0,
        })


def test_validate_pricing_operator_supplied_str_rejected() -> None:
    with pytest.raises(InvalidPricingError, match="must be a number"):
        validate_pricing("operator-supplied", {
            "input_usd_per_1m": "5.0", "output_usd_per_1m": 1.0,
        })


def test_validate_pricing_rejects_unknown_source() -> None:
    with pytest.raises(InvalidPricingError, match="must be one of"):
        validate_pricing("bogus", None)


# ──────────────────────────────────────────────────────────────────────
# default_pricing_source_for
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("provider_type,expected", [
    ("anthropic", "rate-card"),
    ("google", "rate-card"),
    ("openai-compatible", "tokens-only"),
    ("custom", "tokens-only"),
])
def test_default_pricing_source(provider_type: str, expected: str) -> None:
    assert default_pricing_source_for(provider_type) == expected
