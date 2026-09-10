"""Closed keyset signing input; caller cannot choose signing authority or time."""

import copy
import importlib

import pytest
import rfc8785

from loom_task_image_authority.publication_keyset import verify_publication_keyset
from tests.unit.test_task_image_publication_keyset import _sign, fixture
from tests.unit.test_task_image_publication_signing import NOW
from tests.unit.test_task_image_publication_transport import signer_server


def module():
    name = "loom_task_image_authority.keyset_signing_request"
    assert importlib.util.find_spec(name) is not None, "closed keyset signer request is missing"
    return importlib.import_module(name)


def payload():
    return dict(
        schema="loom.task-image-keyset-signing-request/v1", environment="production",
        previous_keyset_version=0, proposed_keyset_version=1, revocation_epoch=0,
        keys=fixture()[2]["keys"],
    )


def test_bootstrap_request_roundtrip_has_no_caller_selected_key_domain_or_clock():
    m = module()
    wire = rfc8785.dumps(payload())
    request = m.decode_keyset_signing_request(wire)
    assert m.canonical_keyset_signing_request(request) == wire
    assert request.previous_keyset_version == 0
    assert request.proposed_keyset_version == 1
    assert request.keys[0].record().status == "active"


async def test_refresh_client_response_interoperates_with_existing_keyset_verifier(tmp_path):
    m = module()
    t = importlib.import_module("loom_task_image_authority.publication_transport")
    private, root, signed, state, *_ = fixture()
    data = payload()
    data.update(previous_keyset_version=2, proposed_keyset_version=3, revocation_epoch=2, keys=signed["keys"])
    wire = m.canonical_keyset_signing_request(m.decode_keyset_signing_request(rfc8785.dumps(data)))
    expected = _sign(signed, private)
    response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(expected)).encode() + b"\r\n\r\n" + expected
    async with signer_server(tmp_path, response=response) as (options, requests, _, _, _):
        async with t.HTTPSKeysetSigner(**options) as client:
            reply = await client.sign_keyset(wire, maximum_reply_bytes=128 * 1024)
    result = verify_publication_keyset(reply, trust_root=root, expected_state=state, now=NOW)
    assert reply == expected
    assert result.keyset.keys == m.decode_keyset_signing_request(requests[0][1]).keys


@pytest.mark.parametrize("change", [
    "key_id", "root_sha256", "issued_at", "expires_at", "domain", "null", "empty",
    "duplicate-id", "duplicate-bytes", "unordered", "too-many", "version-skip",
    "version-replay", "overflow", "bool", "float", "epoch-negative", "lifecycle",
    "duplicate-json", "noncanonical", "oversize", "not-bytes",
])
def test_request_refuses_ambiguous_or_caller_supplied_authority(change):
    m = module()
    data = payload()
    wire = None
    if change in {"key_id", "root_sha256", "issued_at", "expires_at", "domain"}:
        data[change] = "caller-selected"
    elif change == "null":
        data["keys"][0]["retired_at"] = None
    elif change == "empty":
        data["keys"] = []
    elif change in {"duplicate-id", "duplicate-bytes", "unordered", "too-many"}:
        other = copy.deepcopy(data["keys"][0])
        if change == "duplicate-bytes":
            other["key_id"] = "z-second"
        elif change == "unordered":
            other.update(key_id="a-first", public_key="A" * 43)
        data["keys"].append(other)
        if change == "too-many":
            data["keys"] *= 65
    elif change == "version-skip":
        data["proposed_keyset_version"] = 2
    elif change == "version-replay":
        data["previous_keyset_version"] = 1
    elif change == "overflow":
        data.update(previous_keyset_version=9007199254740991, proposed_keyset_version=9007199254740992)
    elif change == "bool":
        data["previous_keyset_version"] = False
    elif change == "float":
        wire = rfc8785.dumps(data).replace(b'"previous_keyset_version":0', b'"previous_keyset_version":0.0')
    elif change == "epoch-negative":
        data["revocation_epoch"] = -1
    elif change == "lifecycle":
        data["keys"][0]["status"] = "verify_only"
    elif change == "duplicate-json":
        wire = rfc8785.dumps(data).replace(b'"revocation_epoch":0', b'"revocation_epoch":0,"revocation_epoch":0')
    elif change == "noncanonical":
        wire = rfc8785.dumps(data) + b" "
    elif change == "oversize":
        wire = b"x" * (64 * 1024 + 1)
    elif change == "not-bytes":
        wire = rfc8785.dumps(data).decode()
    with pytest.raises(ValueError):
        m.decode_keyset_signing_request(rfc8785.dumps(data) if wire is None else wire)


async def test_keyset_client_uses_fixed_mtls_operation_and_rejects_before_network(tmp_path):
    m = module()
    t = importlib.import_module("loom_task_image_authority.publication_transport")
    response = b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}'
    wire = m.canonical_keyset_signing_request(m.decode_keyset_signing_request(rfc8785.dumps(payload())))
    async with signer_server(tmp_path, response=response) as (options, requests, _, _, _):
        async with t.HTTPSKeysetSigner(**options) as client:
            assert not hasattr(client, "sign_publication")
            with pytest.raises(ValueError):
                await client.sign_keyset(b'{}', maximum_reply_bytes=128 * 1024)
            assert not requests
            assert await client.sign_keyset(wire, maximum_reply_bytes=128 * 1024) == b'{}'
        assert len(requests) == 1
        assert requests[0][0].startswith(b"POST /v1/keysets/sign HTTP/1.1\r\n")
        assert requests[0][1] == wire


@pytest.mark.parametrize("maximum", [0, True, 128 * 1024 + 1])
async def test_keyset_client_rejects_reply_limit_before_network(tmp_path, maximum):
    module()
    t = importlib.import_module("loom_task_image_authority.publication_transport")
    async with signer_server(tmp_path) as (options, requests, _, _, _):
        async with t.HTTPSKeysetSigner(**options) as client:
            with pytest.raises(ValueError):
                await client.sign_keyset(rfc8785.dumps(payload()), maximum_reply_bytes=maximum)
        assert not requests
