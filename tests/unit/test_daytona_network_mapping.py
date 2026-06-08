import pytest

from loom.models.networking import Allowlist, NoNetwork, Public
from loom_drivers.daytona.network import (
    DaytonaNetworkArgs,
    to_daytona_network_args,
)


def test_public_maps_to_unblocked_empty_allowlist() -> None:
    args = to_daytona_network_args(Public(), resolved_domain_ips={})
    assert args == DaytonaNetworkArgs(
        network_block_all=False, network_allow_list=None,
    )


def test_no_network_maps_to_block_all() -> None:
    args = to_daytona_network_args(NoNetwork(), resolved_domain_ips={})
    assert args == DaytonaNetworkArgs(
        network_block_all=True, network_allow_list=None,
    )


def test_allowlist_cidr_only() -> None:
    # Allowlist.domains has min_length=1; bypass via model_construct for
    # cidr-only edge case.
    policy = Allowlist.model_construct(
        domains=(), cidrs=("10.0.0.0/8", "192.168.1.0/24"),
    )
    args = to_daytona_network_args(policy, resolved_domain_ips={})
    assert args.network_block_all is False
    assert args.network_allow_list == "10.0.0.0/8,192.168.1.0/24"


def test_allowlist_with_resolved_domains() -> None:
    policy = Allowlist(domains=("api.example.com",), cidrs=("10.0.0.0/8",))
    args = to_daytona_network_args(
        policy,
        resolved_domain_ips={"api.example.com": ("203.0.113.10", "203.0.113.11")},
    )
    assert args.network_block_all is False
    assert args.network_allow_list == (
        "10.0.0.0/8,203.0.113.10/32,203.0.113.11/32"
    )


def test_allowlist_with_unresolved_domain_raises() -> None:
    policy = Allowlist(domains=("nonresolving.invalid",), cidrs=())
    with pytest.raises(ValueError, match=r"nonresolving\.invalid"):
        to_daytona_network_args(policy, resolved_domain_ips={})
