import pytest
from pydantic import TypeAdapter, ValidationError

from loom.models.networking import (
    Allowlist,
    NetworkPolicy,
    NoNetwork,
    Public,
)


def test_public_serialises():
    p = Public()
    assert p.kind == "public"
    assert p.model_dump() == {"kind": "public"}


def test_no_network_serialises():
    p = NoNetwork()
    assert p.kind == "no-network"
    assert p.model_dump() == {"kind": "no-network"}


def test_allowlist_serialises():
    p = Allowlist(domains=("github.com", "pypi.org"), cidrs=("10.0.0.0/8",))
    assert p.kind == "allowlist"
    assert p.domains == ("github.com", "pypi.org")
    assert p.cidrs == ("10.0.0.0/8",)


def test_tagged_union_discriminates():
    adapter = TypeAdapter(NetworkPolicy)
    pub = adapter.validate_python({"kind": "public"})
    assert isinstance(pub, Public)
    none = adapter.validate_python({"kind": "no-network"})
    assert isinstance(none, NoNetwork)
    allow = adapter.validate_python(
        {"kind": "allowlist", "domains": ("a",), "cidrs": ("1.0.0.0/24",)},
    )
    assert isinstance(allow, Allowlist)
    assert allow.domains == ("a",)


def test_tagged_union_rejects_unknown_kind():
    adapter = TypeAdapter(NetworkPolicy)
    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "bogus"})


def test_allowlist_requires_domains():
    with pytest.raises(ValidationError):
        Allowlist()  # type: ignore[call-arg]


def test_immutability():
    p = Allowlist(domains=("example.com",))
    with pytest.raises(ValidationError):
        p.domains = ("changed",)  # type: ignore[misc]
