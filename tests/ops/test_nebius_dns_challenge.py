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


class ProviderState:
    def __init__(self):
        self.rows = [record(recordId="foreign", data="f" * 43)]
        self.calls = []
        self.lose_create_reply = False

    def handle(self, request):
        self.calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json={"items": self.rows})
        if request.method == "POST":
            self.rows.append(record())
            if self.lose_create_reply:
                raise httpx.ReadTimeout("private-response", request=request)
            return httpx.Response(201, json=record())
        assert request.method == "DELETE"
        self.rows = [row for row in self.rows if row["recordId"] != "record-1"]
        return httpx.Response(204)


def hook(dns, root, action, wait=lambda *_args: None, domain="dev.nebius.yylx.world"):
    return module().run_hook(dns, state_dir=root, action=action, certbot_domain=domain,
                             validation="v" * 43, wait=wait)


def test_hook_replay_and_cleanup_preserve_foreign_records(tmp_path):
    state = ProviderState()
    waits = []
    root = tmp_path / "journal"
    with provider(state.handle) as dns:
        assert hook(dns, root, "auth", lambda *args: waits.append(args), domain="*.dev.nebius.yylx.world") == "present"
        assert hook(dns, root, "auth", lambda *args: waits.append(args)) == "present"
        assert state.calls.count("POST") == 1
        assert waits == [("yylx.world", "_acme-challenge.dev.nebius.yylx.world", "v" * 43)] * 2
        assert hook(dns, root, "cleanup") == "cleaned"
        assert hook(dns, root, "cleanup") == "cleaned"
        assert state.calls.count("DELETE") == 1
        assert state.rows == [record(recordId="foreign", data="f" * 43)]
        with pytest.raises(module().DNSChallengeError):
            hook(dns, root, "auth")
    assert root.stat().st_mode & 0o777 == 0o700
    for path in root.iterdir():
        assert path.stat().st_mode & 0o777 == 0o600
        assert "private-pat" not in path.read_text()


def test_hook_lost_create_reply_keeps_pending_intent_without_retry_or_guessed_delete(tmp_path):
    state = ProviderState()
    state.lose_create_reply = True
    with provider(state.handle) as dns:
        for action in ("auth", "auth", "cleanup"):
            with pytest.raises(module().DNSChallengeError):
                hook(dns, tmp_path / "journal", action)
    assert state.calls.count("POST") == 1
    assert state.calls.count("DELETE") == 0
    journal = json.loads(next((tmp_path / "journal").glob("*.json")).read_text())
    assert journal["stage"] == "pending"
    assert journal["before_record_ids"] == ["foreign"]


def test_hook_propagation_failure_keeps_record_for_safe_retry_and_cleanup(tmp_path):
    state = ProviderState()

    def unavailable(*_args):
        raise module().DNSChallengeError("propagation deadline")

    with provider(state.handle) as dns:
        with pytest.raises(module().DNSChallengeError, match="propagation"):
            hook(dns, tmp_path / "journal", "auth", unavailable)
        assert state.calls.count("DELETE") == 0
        assert hook(dns, tmp_path / "journal", "auth") == "present"
        assert state.calls.count("POST") == 1
        assert hook(dns, tmp_path / "journal", "cleanup") == "cleaned"


def test_hook_does_not_adopt_preexisting_validation_without_a_journal(tmp_path):
    state = ProviderState()
    state.rows.append(record())
    with provider(state.handle) as dns:
        with pytest.raises(module().DNSChallengeError):
            hook(dns, tmp_path / "journal", "auth")
    assert state.calls == ["GET"]


def test_hook_rejects_cross_scope_and_insecure_journal_before_provider_write(tmp_path):
    state = ProviderState()
    root = tmp_path / "journal"
    root.mkdir(mode=0o755)
    with provider(state.handle) as dns:
        with pytest.raises(module().DNSChallengeError):
            hook(dns, root, "auth")
        with pytest.raises(module().DNSChallengeError):
            hook(dns, root, "auth", domain="foreign.yylx.world")
    assert state.calls == []


def test_hook_rejects_symlink_and_malformed_journal_without_provider_writes(tmp_path):
    state = ProviderState()
    root = tmp_path / "journal"
    with provider(state.handle) as dns:
        hook(dns, root, "auth")
        journal = next(root.glob("*.json"))
        preserved = journal.read_text()
        journal.write_text('{"stage":"created"}')
        with pytest.raises(module().DNSChallengeError):
            hook(dns, root, "cleanup")
        target = tmp_path / "protected.json"
        target.write_text(preserved)
        target.chmod(0o600)
        journal.unlink()
        journal.symlink_to(target)
        with pytest.raises(module().DNSChallengeError):
            hook(dns, root, "cleanup")
    assert state.calls.count("POST") == 1
    assert state.calls.count("DELETE") == 0


def dns_reply(name, value=None, *, authoritative=True, alias=False):
    import dns.flags
    import dns.message
    import dns.rrset

    reply = dns.message.make_response(dns.message.make_query(name, "TXT"))
    if authoritative:
        reply.flags |= dns.flags.AA
    if alias:
        reply.answer.append(dns.rrset.from_text(name, 600, "IN", "CNAME", "foreign.example."))
    elif value:
        reply.answer.append(dns.rrset.from_text(name, 600, "IN", "TXT", '"' + value + '"'))
    return reply


def test_waits_for_every_authority_not_recursive_or_provider_success(monkeypatch):
    mod = module()
    monkeypatch.setattr(mod, "_authorities", lambda *_args: ["8.8.8.8", "1.1.1.1"])
    seen = []
    slept = []

    def query(message, server, **kwargs):
        seen.append(server)
        value = "v" * 43 if server == "8.8.8.8" or slept else None
        return dns_reply(message.question[0].name, value)

    monkeypatch.setattr(mod.dns.query, "udp", query)
    monkeypatch.setattr(mod.time, "sleep", lambda seconds: slept.append(seconds))
    mod.wait_for_txt("yylx.world", "_acme-challenge.dev.nebius.yylx.world", "v" * 43)
    assert set(seen) == {"8.8.8.8", "1.1.1.1"}
    assert len(slept) == 1


@pytest.mark.parametrize("kwargs", [{"authoritative": False}, {"alias": True}])
def test_delegated_or_aliased_challenge_is_rejected(monkeypatch, kwargs):
    mod = module()
    monkeypatch.setattr(mod, "_authorities", lambda *_args: ["8.8.8.8"])
    monkeypatch.setattr(mod.dns.query, "udp", lambda message, *_a, **_kw: dns_reply(message.question[0].name, "v" * 43, **kwargs))
    with pytest.raises(mod.DNSChallengeError, match=r"authoritative|alias"):
        mod.wait_for_txt("yylx.world", "_acme-challenge.dev.nebius.yylx.world", "v" * 43)


def test_propagation_has_a_bounded_deadline(monkeypatch):
    mod = module()
    now = [0.0]
    monkeypatch.setattr(mod, "_authorities", lambda *_args: ["8.8.8.8"])
    monkeypatch.setattr(mod.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(mod.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.setattr(mod.dns.query, "udp", lambda message, *_a, **_kw: dns_reply(message.question[0].name))
    with pytest.raises(mod.DNSChallengeError, match="deadline"):
        mod.wait_for_txt("yylx.world", "_acme-challenge.dev.nebius.yylx.world", "v" * 43, timeout=10)
    assert now[0] == 10


def test_cli_runs_exact_hook_and_never_prints_validation_or_credential(tmp_path, monkeypatch, capsys):
    mod = module()
    state = ProviderState()
    constructor = mod.GoDaddyDNS
    monkeypatch.setattr(mod, "GoDaddyDNS", lambda *args: constructor(*args, transport=httpx.MockTransport(state.handle)))
    monkeypatch.setattr(mod, "wait_for_txt", lambda *_args: None)
    monkeypatch.setenv("CERTBOT_DOMAIN", "dev.nebius.yylx.world")
    monkeypatch.setenv("CERTBOT_VALIDATION", "v" * 43)
    credential = tmp_path / "credential.json"
    credential.write_text(json.dumps({"token": "private-pat", "expires_on": "2099-01-01"}))
    credential.chmod(0o600)
    args = ["--zone", "yylx.world", "--certificate-domain", "dev.nebius.yylx.world",
            "--credential-file", str(credential), "--state-dir", str(tmp_path / "journal")]
    assert mod.main(["auth", *args]) == 0
    assert mod.main(["cleanup", *args]) == 0
    output = capsys.readouterr()
    assert output.out.splitlines() == ['{"status": "present"}', '{"status": "cleaned"}']
    assert output.err == ""
    monkeypatch.setenv("CERTBOT_DOMAIN", "private-malformed-domain")
    assert mod.main(["auth", *args]) == 1
    output = capsys.readouterr()
    assert "private-pat" not in output.err + output.out
    assert "private-malformed-domain" not in output.err + output.out
    assert "v" * 43 not in output.err + output.out
    credential.write_text(json.dumps({"token": "private-pat", "expires_on": "2000-01-01"}))
    assert mod.main(["auth", *args]) == 1
    output = capsys.readouterr()
    assert "credential expired" in output.err
    assert "private-pat" not in output.err + output.out


def test_encoded_inventory_is_rejected_before_processing(monkeypatch):
    import gzip

    response = httpx.Response(200, content=gzip.compress(b'{"items":[]}'), headers={"content-encoding": "gzip"})
    with provider(lambda _request: response) as dns:
        with pytest.raises(module().DNSChallengeError):
            dns.records()


def test_real_http_transport_does_not_follow_credential_redirect(monkeypatch):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    calls = []

    class Server(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append((self.path, self.headers.get("Authorization")))
            self.send_response(302)
            self.send_header("Location", "/credential-sink")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Server)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setattr(module(), "_API", f"http://127.0.0.1:{server.server_port}/v3/domains/zones/")
        with module().GoDaddyDNS("yylx.world", "dev.nebius.yylx.world", "private-pat") as dns:
            with pytest.raises(module().DNSChallengeError):
                dns.records()
        assert len(calls) == 1
        assert calls[0][0].startswith("/v3/domains/zones/yylx.world/dns-records?")
        assert calls[0][1] == "Bearer private-pat"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_same_challenge_lock_refuses_second_process_attempt(tmp_path):
    state = ProviderState()
    root = tmp_path / "journal"

    def contender(*_args):
        with provider(state.handle) as other:
            with pytest.raises(module().DNSChallengeError, match="already in use"):
                hook(other, root, "auth")

    with provider(state.handle) as dns:
        assert hook(dns, root, "auth", contender) == "present"
    assert state.calls.count("POST") == 1


def test_authority_discovery_rejects_private_addresses(monkeypatch):
    from types import SimpleNamespace

    import dns.name
    import dns.rdata

    mod = module()

    class Answer(list):
        canonical_name = dns.name.from_text("yylx.world")

    def resolve(name, kind, **kwargs):
        if kind == "NS":
            return Answer([dns.rdata.from_text("IN", "NS", "ns1.example.")])
        return Answer([dns.rdata.from_text("IN", "A", "127.0.0.1")])

    monkeypatch.setattr(mod.dns.resolver, "Resolver", lambda: SimpleNamespace(resolve=resolve))
    with pytest.raises(mod.DNSChallengeError, match="not public"):
        mod._authorities("yylx.world", mod.time.monotonic() + 5)


def test_first_use_journal_directory_is_durable_before_provider_write(tmp_path, monkeypatch):
    import os

    state = ProviderState()
    root = tmp_path / "journal"
    synced = []
    actual_fsync = os.fsync

    def fsync(descriptor):
        synced.append(os.readlink(f"/proc/self/fd/{descriptor}"))
        actual_fsync(descriptor)

    def provider_write(request):
        if request.method == "POST":
            assert str(root.parent) in synced, "journal directory entry was not made durable before POST"
            assert str(root) in synced, "pending intent was not made durable before POST"
        return state.handle(request)

    monkeypatch.setattr(module().os, "fsync", fsync)
    with provider(provider_write) as dns:
        assert hook(dns, root, "auth") == "present"
