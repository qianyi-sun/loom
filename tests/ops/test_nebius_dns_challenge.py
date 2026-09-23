"""DNS-01 provider hooks must not broaden credential or record authority."""
from __future__ import annotations

import importlib
import json
from datetime import date

import httpx
import pytest


def module():
    return importlib.import_module("scripts.ops.nebius_dns_challenge")


def record(**changes):
    return {"recordId": "record-1", "type": "TXT", "name": "_acme-challenge.dev.nebius",
            "data": "v" * 43, "ttl": 600, **changes}


def provider(handler):
    return module().GoDaddyDNS("yylx.world", "dev.nebius.yylx.world", "private-pat",
                               transport=httpx.MockTransport(handler))


def test_lists_only_exact_challenge_and_creates_one_record_without_replacement():
    requests = []

    def handler(request):
        requests.append(request)
        assert request.url.scheme == "https"
        assert request.url.host == "api.godaddy.com"
        assert request.url.path == "/v3/domains/zones/yylx.world/dns-records"
        assert request.headers["authorization"] == "Bearer private-pat"
        if request.method == "GET":
            assert dict(request.url.params) == {"type": "TXT", "name": "_acme-challenge.dev.nebius",
                                               "page": "1", "pageSize": "100"}
            return httpx.Response(200, json={"items": [record(data="foreign")]})
        assert request.method == "POST"
        assert json.loads(request.content) == {"type": "TXT", "name": "_acme-challenge.dev.nebius",
                                              "data": "v" * 43, "ttl": 600}
        return httpx.Response(201, json=record())

    with provider(handler) as dns:
        assert dns.records() == [record(data="foreign")]
        assert dns.create("v" * 43) == record()
    assert [request.method for request in requests] == ["GET", "POST"]


@pytest.mark.parametrize("response", [
    httpx.Response(302, headers={"location": "https://foreign.example/secret"}),
    httpx.Response(500, text="private-token"),
    httpx.Response(200, content=b"x" * 262145),
    httpx.Response(200, json={"items": "private-token"}),
    httpx.Response(200, json={"items": [record(), record()]}),
    httpx.Response(200, json={"items": [record(name="_acme-challenge.foreign")]}),
])
def test_invalid_inventory_fails_closed_without_following_redirect_or_retry(response):
    calls = []
    with provider(lambda request: calls.append(request) or response) as dns:
        with pytest.raises(module().DNSChallengeError) as error:
            dns.records()
    assert len(calls) == 1
    assert "private" not in str(error.value)


def test_full_pages_are_not_mistaken_for_complete_inventory():
    pages = []

    def handler(request):
        page = int(request.url.params["page"])
        pages.append(page)
        items = [record(recordId=f"id-{index}") for index in range(100)] if page == 1 else [record(recordId="last")]
        return httpx.Response(200, json={"items": items})

    with provider(handler) as dns:
        assert len(dns.records()) == 101
    assert pages == [1, 2]


def test_ambiguous_create_is_not_retried():
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ReadTimeout("private-token", request=request)

    with provider(handler) as dns:
        with pytest.raises(module().DNSChallengeError, match="ambiguous"):
            dns.create("v" * 43)
    assert len(calls) == 1


def test_cleanup_rechecks_exact_record_and_preserves_parallel_challenge():
    calls = []

    def handler(request):
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"items": [record(), record(recordId="foreign", data="f" * 43)]})
        assert request.method == "DELETE"
        assert request.url.path.endswith("/dns-records/record-1")
        return httpx.Response(204)

    with provider(handler) as dns:
        dns.delete_owned(record())
    assert [request.method for request in calls] == ["GET", "DELETE"]


@pytest.mark.parametrize("mutation", [{"data": "foreign"}, {"ttl": 1200}, {"type": "A"}])
def test_changed_record_is_never_deleted(mutation):
    calls = []
    with provider(lambda request: calls.append(request) or httpx.Response(200, json={"items": [record(**mutation)]})) as dns:
        with pytest.raises(module().DNSChallengeError):
            dns.delete_owned(record())
    assert [request.method for request in calls] == ["GET"]


@pytest.mark.parametrize("domain", ["yylx.world", "foreign.world", "dev.nebius.yylx.world.evil", "*.dev.nebius.yylx.world"])
def test_protected_certificate_scope_rejects_invalid_or_nonchild_domain(domain):
    with pytest.raises(module().DNSChallengeError):
        module().GoDaddyDNS("yylx.world", domain, "private-pat")


@pytest.mark.parametrize("token", ["", "has space", "header\ninjection"])
def test_invalid_bearer_value_is_rejected_before_any_request(token):
    with pytest.raises(module().DNSChallengeError):
        module().GoDaddyDNS("yylx.world", "dev.nebius.yylx.world", token)


def test_credential_requires_private_regular_file_and_unexpired_token(tmp_path):
    path = tmp_path / "credential.json"
    path.write_text(json.dumps({"token": "private-pat", "expires_on": "2026-10-14"}))
    path.chmod(0o600)
    assert module().load_token(path, today=date(2026, 9, 23)) == "private-pat"
    with pytest.raises(module().DNSChallengeError):
        module().load_token(path, today=date(2026, 10, 14))
    path.chmod(0o644)
    with pytest.raises(module().DNSChallengeError):
        module().load_token(path, today=date(2026, 9, 23))
    path.chmod(0o600)
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(module().DNSChallengeError):
        module().load_token(link, today=date(2026, 9, 23))
