"""Fixed DNS publication preserves foreign records and uncertain write history."""
from __future__ import annotations

import copy
import json
import socketserver
import struct
import threading

import dns.flags
import dns.message
import dns.rcode
import dns.rrset
import httpx
import pytest
from scripts.ops import nebius_dns_publication as publication


@pytest.fixture
def target():
    return {
        "installation_id": "11111111-1111-4111-8111-111111111111",
        "service_uid": "22222222-2222-4222-8222-222222222222",
        "candidate": "a" * 40,
        "fingerprint_sha256": "b" * 64,
        "zone": "example.test",
        "child_domain": "dev.nebius.example.test",
        "management_host": "management.nebius.example.test",
        "address": "8.8.8.8",
    }


class Provider:
    def __init__(self):
        self.rows = {"*.dev.nebius": [], "management.nebius": []}
        self.posts = []
        self.failure = None

    def records(self, name):
        return copy.deepcopy(self.rows[name])

    def create(self, name, address):
        self.posts.append(name)
        if self.failure == "before":
            raise RuntimeError("lost request")
        row = {"recordId": "record-" + str(len(self.posts)), "type": "A", "name": name, "data": address, "ttl": 600}
        self.rows[name].append(row)
        if self.failure == "after":
            raise RuntimeError("lost response")
        return row


def publish(provider, target, tmp_path, *, qualify=None, wait=None):
    return publication.publish_dns(provider, target=target, state_dir=tmp_path / "state",
                                   qualify=qualify or (lambda: copy.deepcopy(target)), wait=wait or (lambda value: None))


def test_publishes_only_pair_and_replays_without_writes(target, tmp_path):
    provider = Provider()
    first = publish(provider, target, tmp_path)
    second = publish(provider, target, tmp_path)
    assert first == second
    assert first["status"] == "dns_published"
    assert provider.posts == ["*.dev.nebius", "management.nebius"]
    assert all(row["origin"] == "created" for row in first["records"])


def test_lost_response_qualifies_value_without_claiming_ownership(target, tmp_path):
    provider = Provider()
    provider.failure = "after"
    first = publish(provider, target, tmp_path)
    assert all(row["origin"] == "uncertain" for row in first["records"])
    assert publish(provider, target, tmp_path) == first
    assert len(provider.posts) == 2


def test_lost_request_blocks_replacement_invocation_without_another_post(target, tmp_path):
    provider = Provider()
    provider.failure = "before"
    for _ in range(2):
        with pytest.raises(publication.PublicationError):
            publish(provider, target, tmp_path)
    assert provider.posts == ["*.dev.nebius"]


@pytest.mark.parametrize("kind,data", [("A", "1.1.1.1"), ("AAAA", "2606:4700:4700::1111"),
                                      ("CNAME", "other.example.test"), ("NS", "ns.example.test")])
def test_conflict_at_second_name_prevents_first_write(target, tmp_path, kind, data):
    provider = Provider()
    provider.rows["management.nebius"] = [{"recordId": "foreign", "type": kind, "name": "management.nebius", "data": data, "ttl": 600}]
    before = copy.deepcopy(provider.rows)
    with pytest.raises(publication.PublicationError):
        publish(provider, target, tmp_path)
    assert provider.rows == before and not provider.posts


def test_preexisting_exact_record_is_external_and_is_never_recreated(target, tmp_path):
    provider = Provider()
    provider.rows["*.dev.nebius"] = [{"recordId": "foreign", "type": "A", "name": "*.dev.nebius", "data": "8.8.8.8", "ttl": 3600}]
    result = publish(provider, target, tmp_path)
    assert result["records"][0]["origin"] == "external"
    assert provider.posts == ["management.nebius"]
    provider.rows["*.dev.nebius"] = []
    with pytest.raises(publication.PublicationError):
        publish(provider, target, tmp_path)
    assert provider.posts == ["management.nebius"]


def test_existing_txt_is_preserved_during_creation_and_replay(target, tmp_path):
    provider = Provider()
    text_record = {"recordId": "verification", "type": "TXT", "name": "*.dev.nebius", "data": "foreign-verification", "ttl": 600}
    provider.rows["*.dev.nebius"] = [text_record]
    publish(provider, target, tmp_path)
    assert publish(provider, target, tmp_path)["status"] == "dns_published"
    assert provider.posts == ["*.dev.nebius", "management.nebius"]
    assert provider.rows["*.dev.nebius"][0] == text_record


def test_duplicate_identical_a_records_are_not_treated_as_one(target, tmp_path):
    provider = Provider()
    provider.rows["*.dev.nebius"] = [
        {"recordId": identity, "type": "A", "name": "*.dev.nebius", "data": "8.8.8.8", "ttl": 600}
        for identity in ("first", "second")
    ]
    with pytest.raises(publication.PublicationError):
        publish(provider, target, tmp_path)
    assert not provider.posts


def test_target_drift_between_records_retains_partial_journal(target, tmp_path):
    provider = Provider()

    def qualify():
        return {**target, "address": "1.1.1.1"} if provider.posts else target

    with pytest.raises(publication.PublicationError):
        publish(provider, target, tmp_path, qualify=qualify)
    assert provider.posts == ["*.dev.nebius"]
    assert (tmp_path / "state/dns-publication.json").is_file()


def test_propagation_failure_retains_records_and_replay_does_not_post(target, tmp_path):
    provider = Provider()

    def unavailable(value):
        raise RuntimeError("propagation pending")

    with pytest.raises(publication.PublicationError):
        publish(provider, target, tmp_path, wait=unavailable)
    assert publish(provider, target, tmp_path)["status"] == "dns_published"
    assert len(provider.posts) == 2


@pytest.mark.parametrize("change", [{"address": "127.0.0.1"}, {"address": "::1"},
                                   {"child_domain": "evil.test"}, {"management_host": "alice.dev.nebius.example.test"},
                                   {"installation_id": "not-a-uuid"}, {"candidate": "dev"}])
def test_invalid_target_fails_before_provider_access(target, tmp_path, change):
    provider = Provider()
    with pytest.raises(publication.PublicationError):
        publish(provider, {**target, **change}, tmp_path)
    assert not provider.posts and not (tmp_path / "state").exists()


def test_corrupt_or_changed_journal_cannot_restart_publication(target, tmp_path):
    provider = Provider()
    publish(provider, target, tmp_path)
    journal = tmp_path / "state/dns-publication.json"
    value = json.loads(journal.read_text())
    value["target"]["service_uid"] = "33333333-3333-4333-8333-333333333333"
    journal.write_text(json.dumps(value))
    with pytest.raises(publication.PublicationError):
        publish(provider, target, tmp_path)
    assert len(provider.posts) == 2


def test_transport_limits_queries_and_creation_to_exact_pair(target):
    requests = []

    def respond(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"items": []})
        return httpx.Response(201, json={"recordId": "created", **json.loads(request.content)})

    with publication.GoDaddyPublication(target, "test-token", transport=httpx.MockTransport(respond)) as provider:
        assert provider.records("*.dev.nebius") == []
        row = provider.create("management.nebius", "8.8.8.8")
        assert row == {"recordId": "created", "type": "A", "name": "management.nebius", "data": "8.8.8.8", "ttl": 600}
        with pytest.raises(publication.PublicationError):
            provider.create("@", "8.8.8.8")
        with pytest.raises(publication.PublicationError):
            provider.create("management.nebius", "1.1.1.1")
    assert len(requests) == 2
    assert requests[0].url.path == "/v3/domains/zones/example.test/dns-records"
    assert dict(requests[0].url.params) == {"name": "*.dev.nebius", "page": "1", "pageSize": "100"}
    assert requests[0].headers["authorization"] == "Bearer test-token"


@pytest.mark.parametrize("response", [httpx.Response(302, headers={"location": "https://evil.test"}),
                                    httpx.Response(429, json={"token": "do-not-print"}),
                                    httpx.Response(200, json={"items": [], "nextPage": 2}),
                                    httpx.Response(200, json={"items": [{"recordId": "x"}]})])
def test_transport_rejects_ambiguous_inventory_without_leaking_payload(target, response):
    requests = []

    def respond(request):
        requests.append(request)
        return response

    with publication.GoDaddyPublication(target, "test-token", transport=httpx.MockTransport(respond)) as provider:
        with pytest.raises(publication.PublicationError) as error:
            provider.records("*.dev.nebius")
    assert "test-token" not in str(error.value) and "do-not-print" not in str(error.value)
    assert len(requests) == 1


@pytest.fixture
def dns_wire(monkeypatch):
    requests = []
    replies = {}
    clock = [0.0]
    monkeypatch.setattr(publication.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(publication.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(publication, "_authorities", lambda zone, deadline: ["1.1.1.1", "8.8.8.8"])

    def query(request, server, *, timeout):
        assert not request.flags & dns.flags.RD and 0 < timeout <= 3
        name, kind = str(request.question[0].name), request.question[0].rdtype
        requests.append((name, kind, server))
        response = dns.message.make_response(request)
        response.flags |= dns.flags.AA
        response.answer = replies.get((server, kind), [])
        if not response.answer and kind == 1:
            response.answer = [dns.rrset.from_text(name, 600, "IN", "A", "8.8.8.8")]
        return response

    monkeypatch.setattr(publication.dns.query, "udp", query)
    monkeypatch.setattr(publication.dns.query, "tcp", query)
    return requests, replies, clock, query


def test_authoritative_proof_covers_both_names_all_servers_and_ipv6_absence(target, dns_wire):
    requests, _, _, _ = dns_wire
    publication.wait_for_addresses(target, timeout=10)
    assert len(requests) == 8
    assert {kind for _, kind, _ in requests} == {1, 28}
    assert {server for _, _, server in requests} == {"1.1.1.1", "8.8.8.8"}
    names = {name for name, _, _ in requests}
    assert "management.nebius.example.test." in names
    assert len(names) == 2
    assert any(name.endswith(".dev.nebius.example.test.") and not name.startswith("*.") for name in names)


def test_wildcard_probe_respects_dns_name_length_limit(target, dns_wire):
    requests, _, _, _ = dns_wire
    child = ".".join(["a" * 63, "b" * 63, "c" * 63, "d" * 46, "example.test"])
    assert len(child) == 251
    publication.wait_for_addresses({**target, "child_domain": child}, timeout=10)
    assert len(requests) == 8 and all(len(name.rstrip(".")) <= 253 for name, _, _ in requests)


@pytest.mark.parametrize("problem", ["alias", "referral", "ipv6", "wrong_address", "nxdomain"])
def test_authoritative_proof_rejects_false_readiness(target, dns_wire, monkeypatch, problem):
    _, _, clock, original = dns_wire

    def query(request, server, *, timeout):
        response = original(request, server, timeout=timeout)
        if server == "8.8.8.8":
            name = str(request.question[0].name)
            if problem == "alias":
                response.answer = [dns.rrset.from_text(name, 600, "IN", "CNAME", "other.example.test.")]
            elif problem == "referral":
                response.flags &= ~dns.flags.AA
            elif problem == "ipv6" and request.question[0].rdtype == 28:
                response.answer = [dns.rrset.from_text(name, 600, "IN", "AAAA", "2606:4700:4700::1111")]
            elif problem == "wrong_address" and request.question[0].rdtype == 1:
                response.answer = [dns.rrset.from_text(name, 600, "IN", "A", "1.1.1.1")]
            elif problem == "nxdomain":
                response.set_rcode(dns.rcode.NXDOMAIN)
                response.answer = []
        return response

    monkeypatch.setattr(publication.dns.query, "udp", query)
    with pytest.raises(publication.PublicationError):
        publication.wait_for_addresses(target, timeout=10)
    assert clock[0] <= 10


def test_truncated_udp_requires_tcp_proof(target, dns_wire, monkeypatch):
    requests, _, _, original = dns_wire
    tcp_requests = []

    def udp(request, server, *, timeout):
        response = original(request, server, timeout=timeout)
        response.flags |= dns.flags.TC
        response.answer = []
        return response

    def tcp(request, server, *, timeout):
        tcp_requests.append(server)
        return original(request, server, timeout=timeout)

    monkeypatch.setattr(publication.dns.query, "udp", udp)
    monkeypatch.setattr(publication.dns.query, "tcp", tcp)
    publication.wait_for_addresses(target, timeout=10)
    assert len(tcp_requests) == 8 and len(requests) == 16


def test_prepublication_authority_allows_absent_names_but_not_referrals(target, dns_wire, monkeypatch):
    _, _, _, original = dns_wire

    def missing(request, server, *, timeout):
        response = original(request, server, timeout=timeout)
        response.set_rcode(dns.rcode.NXDOMAIN)
        response.answer = []
        return response

    monkeypatch.setattr(publication.dns.query, "udp", missing)
    publication.qualify_authority(target)

    def referral(request, server, *, timeout):
        response = missing(request, server, timeout=timeout)
        response.flags &= ~dns.flags.AA
        return response

    monkeypatch.setattr(publication.dns.query, "udp", referral)
    with pytest.raises(publication.PublicationError):
        publication.qualify_authority(target)


def test_actual_udp_tcp_authoritative_wildcard_propagation(target, monkeypatch):
    calls = []

    class UDP(socketserver.BaseRequestHandler):
        def handle(self):
            wire, stream = self.request
            request = dns.message.from_wire(wire)
            response = dns.message.make_response(request)
            response.flags |= dns.flags.AA | dns.flags.TC
            calls.append("udp")
            stream.sendto(response.to_wire(), self.client_address)

    class TCP(socketserver.StreamRequestHandler):
        def handle(self):
            size = struct.unpack("!H", self.rfile.read(2))[0]
            request = dns.message.from_wire(self.rfile.read(size))
            question = request.question[0]
            response = dns.message.make_response(request)
            response.flags |= dns.flags.AA
            if question.rdtype == 1:
                response.answer = [dns.rrset.from_text(question.name, 600, "IN", "A", "8.8.8.8")]
            calls.append("tcp")
            payload = response.to_wire()
            self.wfile.write(struct.pack("!H", len(payload)) + payload)

    real_udp, real_tcp = publication.dns.query.udp, publication.dns.query.tcp
    with socketserver.UDPServer(("127.0.0.1", 0), UDP) as udp, socketserver.TCPServer(("127.0.0.1", 0), TCP) as tcp:
        threads = [threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
                   for server in (udp, tcp)]
        for thread in threads:
            thread.start()
        try:
            monkeypatch.setattr(publication, "_authorities", lambda zone, deadline: ["127.0.0.1"])
            monkeypatch.setattr(publication.dns.query, "udp", lambda request, server, **kwargs:
                                real_udp(request, server, port=udp.server_address[1], **kwargs))
            monkeypatch.setattr(publication.dns.query, "tcp", lambda request, server, **kwargs:
                                real_tcp(request, server, port=tcp.server_address[1], **kwargs))
            publication.wait_for_addresses(target, timeout=5)
            assert calls == ["udp", "tcp"] * 4
        finally:
            for server in (udp, tcp):
                server.shutdown()
            for thread in threads:
                thread.join(timeout=2)
                assert not thread.is_alive()
