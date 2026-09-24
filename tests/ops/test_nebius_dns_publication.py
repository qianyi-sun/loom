"""Fixed DNS publication preserves foreign records and uncertain write history."""
from __future__ import annotations

import copy
import json

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
