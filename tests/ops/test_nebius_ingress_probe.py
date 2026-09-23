"""Exact-address HTTPS probes authenticate SNI, certificate and legacy route."""
from __future__ import annotations

import hashlib
import importlib
import json
import ssl
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from tests.ops import test_nebius_certificates as certificates


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_probe")


@pytest.fixture
def endpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(certificates, "NOW", datetime.now(UTC))
    chain, key, roots = certificates.material(names=("legacy.example.test", "management.example.test"))
    cert, private = tmp_path / "server.crt", tmp_path / "server.key"
    cert.write_bytes(chain)
    private.write_bytes(key)
    private.chmod(0o600)
    client_context = ssl.create_default_context(cadata=roots[0].public_bytes(serialization.Encoding.PEM).decode())
    monkeypatch.setattr(ssl, "create_default_context", lambda: client_context)
    state = {"status": 200, "environment": "development", "apiRouteBase": "https://legacy.example.test/api"}
    paths = []

    class Handler(BaseHTTPRequestHandler):
        def handle(self):
            try:
                super().handle()
            except (ConnectionResetError, BrokenPipeError, ssl.SSLEOFError):
                # Certificate-only clients intentionally send no HTTP request.
                pass

        def do_GET(self):
            paths.append((self.path, self.headers["Host"]))
            payload = json.dumps({"status": "ok"} if self.path == "/api/v1/health" else state).encode()
            self.send_response(state["status"])
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, private)
    context.set_alpn_protocols(["http/1.1"])
    server = HTTPServer(("127.0.0.1", 0), Handler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield {"address": "127.0.0.1", "port": server.server_port}, state, paths, hashlib.sha256(
            x509.load_pem_x509_certificate(chain).public_bytes(serialization.Encoding.DER)).hexdigest()
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)


def test_public_management_tls_requires_delivered_fingerprint(endpoint):
    address, _state, _paths, fingerprint = endpoint
    assert module().probe_management(**address, hostname="management.example.test", fingerprint=fingerprint) is None
    with pytest.raises(module().ProbeError):
        module().probe_management(**address, hostname="management.example.test", fingerprint="0" * 64)


def test_legacy_probe_uses_literal_address_and_original_host_for_both_routes(endpoint):
    address, _state, paths, _fingerprint = endpoint
    module().probe_legacy(**address, hostname="legacy.example.test", environment="development")
    assert paths == [("/api/v1/health", "legacy.example.test"), ("/loom-frontend-config.json", "legacy.example.test")]


@pytest.mark.parametrize("hostname", ["foreign.example.test", "legacy.example.test\r\nInjected: true"])
def test_wrong_or_unsafe_server_name_never_qualifies(endpoint, hostname):
    address, _state, _paths, fingerprint = endpoint
    with pytest.raises(module().ProbeError):
        module().probe_management(**address, hostname=hostname, fingerprint=fingerprint)


@pytest.mark.parametrize("key,value", [("status", 302), ("status", 500), ("environment", "production"),
                                       ("apiRouteBase", "https://foreign.example.test/api")])
def test_legacy_probe_rejects_redirect_error_or_wrong_environment(endpoint, key, value):
    address, state, _paths, _fingerprint = endpoint
    state[key] = value
    with pytest.raises(module().ProbeError):
        module().probe_legacy(**address, hostname="legacy.example.test", environment="development")


def test_public_probe_cannot_replace_the_pinned_address_with_dns(endpoint):
    address, _state, _paths, fingerprint = endpoint
    with pytest.raises(module().ProbeError):
        module().probe_management(**{**address, "address": "management.example.test"},
                                  hostname="management.example.test", fingerprint=fingerprint)


def test_certificate_fingerprint_does_not_replace_system_trust(endpoint, monkeypatch):
    address, _state, _paths, fingerprint = endpoint
    untrusted = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    monkeypatch.setattr(ssl, "create_default_context", lambda: untrusted)
    with pytest.raises(module().ProbeError):
        module().probe_management(**address, hostname="management.example.test", fingerprint=fingerprint)
