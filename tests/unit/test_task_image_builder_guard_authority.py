from __future__ import annotations

import ipaddress
import json
import os
import ssl
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from uuid import UUID

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from loom_task_image_builder_guard import authority as authority_module
from loom_task_image_builder_guard.authority import AuthorityClient
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import AuthorityConfig
from loom_task_image_builder_guard.protocol import read_sealed_memfd

NOW = datetime(2026, 9, 2, 16, 0, tzinfo=UTC)
GRANT = UUID("11111111-1111-1111-1111-111111111111")
REQUEST = UUID("22222222-2222-2222-2222-222222222222")
CHALLENGE = UUID("33333333-3333-3333-3333-333333333333")
PROOF = UUID("44444444-4444-4444-4444-444444444444")
EXCHANGE = UUID("55555555-5555-5555-5555-555555555555")
SESSION = UUID("66666666-6666-6666-6666-666666666666")
NEXT_SESSION = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
ATTESTATION = UUID("77777777-7777-7777-7777-777777777777")
MATERIALIZATION = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
ATTEMPT = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
OPERATION = UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
DIGEST_A = "1" * 64
DIGEST_B = "2" * 64
DIGEST_C = "3" * 64
DIGEST_D = "4" * 64
BOOTSTRAP_TOKEN = "loom_tibp_" + "A" * 64
SESSION_TOKEN = "loom_tibs_" + "B" * 64
BEARER = "node-bearer-private-value"


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _request() -> dict[str, object]:
    return {
        "schema_version": 1,
        "request_id": str(REQUEST),
        "grant_id": str(GRANT),
        "observed_at": _timestamp(NOW),
        "node_name": "trt-gb10-1",
        "node_boot_id": "88888888-8888-8888-8888-888888888888",
        "slurm_cluster_id": "gb10",
        "slurm_job_id": "12345",
        "supervisor_pid": 42100,
        "supervisor_uid": 993,
        "supervisor_gid": 980,
        "supervisor_executable_sha256": DIGEST_A,
        "cgroup_path": "/sys/fs/cgroup/slurm/job_12345/step_batch/user/task_0",
        "cgroup_inode": 987654,
        "submitting_identity": "loom-builder",
        "slurm_account": "loom-task-builder",
        "slurm_partition": "loom-task-builder",
        "slurm_qos": "loom-task-image-builder-rootless-gb10",
        "cpu_arch": "arm64",
        "slurm_request_sha256": DIGEST_B,
    }


def _attachment() -> dict[str, object]:
    root = "/sys/fs/cgroup/slurm/job_12345/step_batch/user/task_0/loom-builder"
    return {
        "schema_version": 1,
        "cgroup_inode": 987654,
        "containment_root": root,
        "trusted_service_cgroup": f"{root}/trusted-service",
        "build_egress_cgroup": f"{root}/build-egress",
        "bpf_program_sha256": DIGEST_A,
        "bpf_map_schema_sha256": DIGEST_B,
        "containment_policy_sha256": DIGEST_C,
        "resource_limits_sha256": DIGEST_D,
        "probe_sha256": "5" * 64,
        "link_ids": list(range(101, 125)),
        "program_ids": list(range(201, 225)),
        "map_ids": list(range(301, 319)),
    }


def _proof(client: AuthorityClient) -> dict[str, object]:
    request_sha256 = client.request_sha256(_request())
    return {
        "schema_version": 1,
        "proof_id": str(PROOF),
        "grant_id": str(GRANT),
        "request_id": str(REQUEST),
        "request_sha256": request_sha256,
        "challenge_nonce": str(CHALLENGE),
        "observed_at": _timestamp(NOW + timedelta(seconds=2)),
        "node_name": "trt-gb10-1",
        "node_boot_id": "88888888-8888-8888-8888-888888888888",
        "slurm_cluster_id": "gb10",
        "slurm_job_id": "12345",
        "cgroup_path": "/sys/fs/cgroup/slurm/job_12345/step_batch/user/task_0",
        "cgroup_inode": 987654,
        "attachment": _attachment(),
        "attestation_generation": 1,
        "attestation_expires_at": _timestamp(NOW + timedelta(seconds=50)),
    }


def _attestation() -> dict[str, object]:
    return {
        "schema_version": 1,
        "attestation_id": str(ATTESTATION),
        "grant_id": str(GRANT),
        "generation": 2,
        "node_name": "trt-gb10-1",
        "node_boot_id": "88888888-8888-8888-8888-888888888888",
        "slurm_cluster_id": "gb10",
        "slurm_job_id": "12345",
        "cgroup_path": "/sys/fs/cgroup/slurm/job_12345/step_batch/user/task_0",
        "cgroup_inode": 987654,
        "attachment": _attachment(),
        "issued_at": _timestamp(NOW + timedelta(seconds=4)),
        "expires_at": _timestamp(NOW + timedelta(seconds=54)),
    }


def _owner_file(path: Path, payload: bytes, mode: int) -> Path:
    path.write_bytes(payload)
    path.chmod(mode)
    return path


def _new_ca() -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "guard-test-ca")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOW - timedelta(days=1))
        .not_valid_after(NOW + timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(key, hashes.SHA256())
    )
    return key, certificate


def _certificate(
    common_name: str,
    ca_key: rsa.RSAPrivateKey,
    ca: x509.Certificate,
    *,
    server: bool,
) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOW - timedelta(days=1))
        .not_valid_after(NOW + timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=True,
        )
    )
    if server:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
    return key, builder.sign(ca_key, hashes.SHA256())


def _key_bytes(key: rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], context: ssl.SSLContext) -> None:
        super().__init__(address, _Handler)
        self.socket = context.wrap_socket(self.socket, server_side=True)
        self.responses: dict[str, tuple[int, bytes, dict[str, str]]] = {}
        self.requests: list[tuple[str, dict[str, str], bytes, str | None]] = []
        self.methods: list[str] = []
        self.drip_paths: set[str] = set()


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_PUT(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def _handle(self) -> None:
        self.server.methods.append(self.command)  # type: ignore[attr-defined]
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length)
        self.server.requests.append(  # type: ignore[attr-defined]
            (
                self.path,
                {key.lower(): value for key, value in self.headers.items()},
                body,
                self.connection.version(),  # type: ignore[union-attr]
            )
        )
        if self.path in self.server.drip_paths:  # type: ignore[attr-defined]
            try:
                self.connection.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    b"Content-Length: 2\r\nX-Drip: "
                )
                for character in b"0123456789abcdef":
                    self.connection.sendall(bytes((character,)))
                    time.sleep(0.1)
                self.connection.sendall(b"\r\n\r\n{}")
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            return
        status, payload, headers = self.server.responses[self.path]  # type: ignore[attr-defined]
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        response_header_names = {key.lower() for key in headers}
        if not {"transfer-encoding", "content-length"} & response_header_names:
            self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if headers.get("Transfer-Encoding", "").lower() == "chunked":
            self.wfile.write(f"{len(payload):X}\r\n".encode("ascii") + payload + b"\r\n0\r\n\r\n")
        else:
            self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        del format, args


@contextmanager
def _authority(tmp_path: Path) -> Iterator[tuple[_Server, AuthorityConfig]]:
    ca_key, ca = _new_ca()
    server_key, server_cert = _certificate("localhost", ca_key, ca, server=True)
    client_key, client_cert = _certificate("guard", ca_key, ca, server=False)
    ca_path = _owner_file(
        tmp_path / "ca.pem", ca.public_bytes(serialization.Encoding.PEM), 0o444
    )
    client_path = _owner_file(
        tmp_path / "client.pem",
        client_cert.public_bytes(serialization.Encoding.PEM),
        0o444,
    )
    client_key_path = _owner_file(tmp_path / "client-key.pem", _key_bytes(client_key), 0o600)
    bearer_path = _owner_file(tmp_path / "bearer", BEARER.encode("ascii"), 0o600)
    server_path = _owner_file(
        tmp_path / "server.pem",
        server_cert.public_bytes(serialization.Encoding.PEM),
        0o600,
    )
    server_key_path = _owner_file(tmp_path / "server-key.pem", _key_bytes(server_key), 0o600)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.maximum_version = ssl.TLSVersion.TLSv1_3
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cafile=str(ca_path))
    context.load_cert_chain(server_path, server_key_path)
    server = _Server(("127.0.0.1", 0), context)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = AuthorityConfig(
        base_url=f"https://localhost:{server.server_port}",
        connect_ip="127.0.0.1",
        ca_path=ca_path,
        cert_path=client_path,
        key_path=client_key_path,
        bearer_path=bearer_path,
        timeout_seconds=2,
        max_response_bytes=4096,
    )
    try:
        yield server, config
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def _json_response(value: object) -> tuple[int, bytes, dict[str, str]]:
    return 200, json.dumps(value, separators=(",", ":")).encode("ascii"), {
        "Content-Type": "application/json"
    }


def test_real_tls13_client_uses_exact_routes_bearer_and_contract_bindings(
    tmp_path: Path,
) -> None:
    with _authority(tmp_path) as (server, config):
        client = AuthorityClient(
            config,
            trusted_uid=os.geteuid(),
            trusted_gid=os.getegid(),
            now_factory=lambda: NOW + timedelta(seconds=5),
        )
        request = _request()
        request_sha = client.request_sha256(request)
        proof = _proof(client)
        proof_sha = client.request_sha256(proof)
        attestation = _attestation()
        exchange = {
            "schema_version": 1,
            "exchange_id": str(EXCHANGE),
            "grant_id": str(GRANT),
            "proof_sha256": proof_sha,
            "bootstrap_token": BOOTSTRAP_TOKEN,
            "observed_at": _timestamp(NOW + timedelta(seconds=4)),
        }
        revocation = {
            "schema_version": 1,
            "grant_id": str(GRANT),
            "reason": "supervisor_exit",
            "observed_at": _timestamp(NOW + timedelta(seconds=5)),
        }
        base = f"/v1/projections/{GRANT}"
        server.responses = {
            f"{base}/challenge": _json_response(
                {
                        "schema_version": 1,
                    "request_id": str(REQUEST),
                    "grant_id": str(GRANT),
                    "request_sha256": request_sha,
                    "challenge_nonce": str(CHALLENGE),
                    "containment_policy_sha256": DIGEST_C,
                    "resource_profile_sha256": DIGEST_D,
                    "issued_at": _timestamp(NOW + timedelta(seconds=1)),
                    "expires_at": _timestamp(NOW + timedelta(seconds=50)),
                }
            ),
            f"{base}/attachment": _json_response(
                {
                    "schema_version": 1,
                    "grant_id": str(GRANT),
                    "proof_id": str(PROOF),
                    "proof_sha256": proof_sha,
                    "bootstrap_token": BOOTSTRAP_TOKEN,
                    "issued_at": _timestamp(NOW + timedelta(seconds=3)),
                    "expires_at": _timestamp(NOW + timedelta(seconds=55)),
                }
            ),
            f"{base}/exchange": _json_response(
                {
                    "schema_version": 2,
                    "grant_id": str(GRANT),
                    "session_id": str(SESSION),
                    "purpose": "production",
                    "shadow_campaign_id": None,
                    "pool_id": "staging-gb10-task-image",
                    "cpu_arch": "arm64",
                    "session_token": SESSION_TOKEN,
                    "generation": 1,
                    "attestation_generation": 1,
                    "attestation_sha256": DIGEST_A,
                    "issued_at": _timestamp(NOW + timedelta(seconds=3)),
                    "expires_at": _timestamp(NOW + timedelta(minutes=10)),
                }
            ),
            f"{base}/attestations/2": _json_response(attestation),
            f"{base}/revocation": (204, b"", {}),
        }

        challenge = client.challenge(
            GRANT,
            request,
            containment_policy_sha256=DIGEST_C,
            resource_profile_sha256=DIGEST_D,
        )
        receipt = client.attach(GRANT, proof)
        session = client.exchange(GRANT, exchange)
        accepted = client.attest(GRANT, 2, attestation)
        client.revoke(GRANT, revocation)

        assert challenge.challenge_nonce == CHALLENGE
        assert receipt.bootstrap_token == BOOTSTRAP_TOKEN
        assert BOOTSTRAP_TOKEN not in repr(receipt)
        assert session.session_token == SESSION_TOKEN
        assert session.generation == 1
        assert SESSION_TOKEN not in repr(session)
        assert accepted.generation == 2
        assert [item[0] for item in server.requests] == [
            f"{base}/challenge",
            f"{base}/attachment",
            f"{base}/exchange",
            f"{base}/attestations/2",
            f"{base}/revocation",
        ]
        for _path, headers, _body, tls_version in server.requests:
            assert headers["authorization"] == f"Bearer {BEARER}"
            assert headers["content-type"] == "application/json"
            assert tls_version == "TLSv1.3"


def test_fixed_session_and_materialization_routes_keep_capabilities_sealed(
    tmp_path: Path,
) -> None:
    with _authority(tmp_path) as (server, config):
        client = AuthorityClient(
            config,
            trusted_uid=os.geteuid(),
            trusted_gid=os.getegid(),
            now_factory=lambda: NOW + timedelta(seconds=6),
        )
        attestation = _attestation()
        renewal = {
            "schema_version": 1,
            "renewal_id": str(OPERATION),
            "grant_id": str(GRANT),
            "session_id": str(SESSION),
            "session_generation": 1,
            "session_token": SESSION_TOKEN,
            "attestation": attestation,
            "observed_at": _timestamp(NOW + timedelta(seconds=4)),
        }
        current = {
            "schema_version": 1,
            "grant_id": str(GRANT),
            "session_id": str(NEXT_SESSION),
            "session_generation": 2,
            "session_token": "loom_tibs_" + "C" * 64,
        }
        claim = current | {"claim_id": str(OPERATION)}
        operation = current | {
            "operation_id": str(OPERATION),
            "materialization_id": str(MATERIALIZATION),
            "attempt_id": str(ATTEMPT),
            "lease_epoch": 3,
        }
        claim_payload = json.dumps(
            {
                "schema_version": "loom.task-image-materialization-claim.v1",
                "claim_id": str(OPERATION),
                "materialization_id": str(MATERIALIZATION),
                "attempt_id": str(ATTEMPT),
                "lease_epoch": 3,
                "state": "claimed",
                "deterministic_failure_count": 0,
                "lease_expires_at": _timestamp(NOW + timedelta(seconds=40)),
                "plan": {"dockerfile_path": "bundle/private/Dockerfile"},
            },
            separators=(",", ":"),
        ).encode("ascii")
        bundle_payload = b'{"presigned_url":"https://object.invalid/private?signature=SECRET"}'
        operation_response = {
            "schema_version": "loom.task-image-materialization-operation.v1",
            "operation": "start",
            "operation_id": str(OPERATION),
            "materialization_id": str(MATERIALIZATION),
            "attempt_id": str(ATTEMPT),
            "lease_epoch": 3,
            "state": "running",
            "deterministic_failure_count": 0,
            "lease_expires_at": _timestamp(NOW + timedelta(seconds=40)),
        }
        base = f"/v1/projections/{GRANT}"
        materialization_base = f"{base}/materializations/{MATERIALIZATION}"
        server.responses = {
            f"{base}/sessions/1/renew": _json_response(
                {
                    "schema_version": 2,
                    "grant_id": str(GRANT),
                    "session_id": str(NEXT_SESSION),
                    "purpose": "production",
                    "shadow_campaign_id": None,
                    "pool_id": "staging-gb10-task-image",
                    "cpu_arch": "arm64",
                    "session_token": current["session_token"],
                    "generation": 2,
                    "attestation_generation": 2,
                    "attestation_sha256": client.request_sha256(attestation),
                    "issued_at": _timestamp(NOW + timedelta(seconds=4)),
                    "expires_at": _timestamp(NOW + timedelta(minutes=10)),
                }
            ),
            f"{base}/materializations/claim": (
                200,
                claim_payload,
                {"Content-Type": "application/json"},
            ),
            f"{materialization_base}/start": _json_response(operation_response),
            f"{materialization_base}/heartbeat": _json_response(
                operation_response | {"operation": "heartbeat"}
            ),
            f"{materialization_base}/release": _json_response(
                operation_response
                | {"operation": "release", "state": "queued", "lease_expires_at": None}
            ),
            f"{materialization_base}/fail": _json_response(
                operation_response
                | {
                    "operation": "containment_release",
                    "state": "queued",
                    "lease_expires_at": None,
                }
            ),
            f"{materialization_base}/bundle": (
                200,
                bundle_payload,
                {"Content-Type": "application/json"},
            ),
        }

        session = client.renew(GRANT, 1, renewal)
        claimed = client.claim(GRANT, claim)
        assert claimed is not None
        started = client.start(GRANT, MATERIALIZATION, operation)
        heartbeat = client.heartbeat(GRANT, MATERIALIZATION, operation)
        released = client.release(GRANT, MATERIALIZATION, operation)
        failed = client.fail(
            GRANT,
            MATERIALIZATION,
            operation | {"failure_kind": "containment"},
        )
        bundled = client.bundle(GRANT, MATERIALIZATION, operation)
        try:
            assert session.generation == 2
            assert read_sealed_memfd(claimed.descriptor, maximum=4096) == claim_payload
            assert read_sealed_memfd(bundled.descriptor, maximum=4096) == bundle_payload
            assert "Dockerfile" not in repr(claimed)
            assert "signature" not in repr(bundled)
            assert (started.operation, heartbeat.operation) == ("start", "heartbeat")
            assert (released.state, failed.operation) == ("queued", "containment_release")
        finally:
            claimed.close()
            bundled.close()

        assert [item[0] for item in server.requests] == [
            f"{base}/sessions/1/renew",
            f"{base}/materializations/claim",
            f"{materialization_base}/start",
            f"{materialization_base}/heartbeat",
            f"{materialization_base}/release",
            f"{materialization_base}/fail",
            f"{materialization_base}/bundle",
        ]
        assert server.methods == ["PUT", "POST", "PUT", "PUT", "PUT", "PUT", "PUT"]


@pytest.mark.parametrize(
    ("response", "expected_code"),
    [
        ((307, b"", {"Location": "https://example.invalid/stolen"}), "authority_http_failed"),
        ((200, b"{}", {"Transfer-Encoding": "chunked", "Content-Type": "application/json"}), "authority_response_invalid"),
        ((200, b'{"schema_version":1,"schema_version":1}', {"Content-Type": "application/json"}), "authority_response_invalid"),
        ((200, b"x" * 4097, {"Content-Type": "application/json"}), "authority_response_too_large"),
        (
            (
                200,
                b"{}",
                {"Content-Type": "application/json", "Content-Length": "9" * 5000},
            ),
            "authority_response_invalid",
        ),
    ],
)
def test_rejects_redirect_chunking_duplicate_json_and_oversized_response(
    tmp_path: Path,
    response: tuple[int, bytes, dict[str, str]],
    expected_code: str,
) -> None:
    with _authority(tmp_path) as (server, config):
        path = f"/v1/projections/{GRANT}/challenge"
        server.responses[path] = response
        client = AuthorityClient(
            config,
            trusted_uid=os.geteuid(),
            trusted_gid=os.getegid(),
            now_factory=lambda: NOW + timedelta(seconds=5),
        )

        with pytest.raises(GuardError) as caught:
            client.challenge(
                GRANT,
                _request(),
                containment_policy_sha256=DIGEST_C,
                resource_profile_sha256=DIGEST_D,
            )

        assert caught.value.code == expected_code
        assert "example.invalid" not in repr(caught.value)


def test_authority_request_has_one_total_deadline_across_drip_fed_headers(
    tmp_path: Path,
) -> None:
    with _authority(tmp_path) as (server, config):
        path = f"/v1/projections/{GRANT}/challenge"
        server.drip_paths.add(path)
        client = AuthorityClient(
            replace(config, timeout_seconds=1),
            trusted_uid=os.geteuid(),
            trusted_gid=os.getegid(),
            now_factory=lambda: NOW + timedelta(seconds=5),
        )

        started = time.monotonic()
        with pytest.raises(GuardError) as caught:
            client.challenge(
                GRANT,
                _request(),
                containment_policy_sha256=DIGEST_C,
                resource_profile_sha256=DIGEST_D,
            )
        elapsed = time.monotonic() - started

        assert caught.value.code == "authority_deadline_exceeded"
        assert elapsed < 1.5


def test_authority_connects_to_pinned_numeric_address_without_name_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _authority(tmp_path) as (server, config):
        client = AuthorityClient(
            config,
            trusted_uid=os.geteuid(),
            trusted_gid=os.getegid(),
            now_factory=lambda: NOW + timedelta(seconds=5),
        )
        request = _request()
        path = f"/v1/projections/{GRANT}/challenge"
        server.responses[path] = _json_response(
            {
                "schema_version": 1,
                "request_id": str(REQUEST),
                "grant_id": str(GRANT),
                "request_sha256": client.request_sha256(request),
                "challenge_nonce": str(CHALLENGE),
                "containment_policy_sha256": DIGEST_C,
                "resource_profile_sha256": DIGEST_D,
                "issued_at": _timestamp(NOW + timedelta(seconds=1)),
                "expires_at": _timestamp(NOW + timedelta(seconds=50)),
            }
        )

        def forbid_resolution(*args: object, **kwargs: object) -> object:
            del args, kwargs
            raise AssertionError("numeric authority connection performed DNS resolution")

        monkeypatch.setattr(authority_module.socket, "getaddrinfo", forbid_resolution)

        challenge = client.challenge(
            GRANT,
            request,
            containment_policy_sha256=DIGEST_C,
            resource_profile_sha256=DIGEST_D,
        )

        assert challenge.grant_id == GRANT


def test_rejects_stale_or_mismatched_response_without_reflecting_tokens(
    tmp_path: Path,
) -> None:
    with _authority(tmp_path) as (server, config):
        client = AuthorityClient(
            config,
            trusted_uid=os.geteuid(),
            trusted_gid=os.getegid(),
            now_factory=lambda: NOW + timedelta(minutes=2),
        )
        path = f"/v1/projections/{GRANT}/attachment"
        proof = _proof(client)
        server.responses[path] = _json_response(
            {
                "schema_version": 1,
                "grant_id": str(GRANT),
                "proof_id": str(PROOF),
                "proof_sha256": client.request_sha256(proof),
                "bootstrap_token": BOOTSTRAP_TOKEN,
                "issued_at": _timestamp(NOW),
                "expires_at": _timestamp(NOW + timedelta(seconds=30)),
            }
        )

        with pytest.raises(GuardError) as caught:
            client.attach(GRANT, proof)

        assert caught.value.code == "authority_receipt_invalid"
        assert BOOTSTRAP_TOKEN not in str(caught.value)
        assert BOOTSTRAP_TOKEN not in repr(caught.value)


def test_credential_files_must_be_stable_owned_and_canonical(tmp_path: Path) -> None:
    with _authority(tmp_path) as (_server, config):
        config.bearer_path.chmod(0o644)

        with pytest.raises(GuardError) as caught:
            AuthorityClient(
                config,
                trusted_uid=os.geteuid(),
                trusted_gid=os.getegid(),
            )

        assert caught.value.code == "authority_credentials_invalid"


def test_certificate_memfd_is_closed_when_private_key_memfd_creation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _authority(tmp_path) as (_server, config):
        certificate_fd = os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC)
        calls = 0

        def fail_second_memfd(name: str, payload: bytes, *, maximum: int) -> int:
            nonlocal calls
            del name, payload, maximum
            calls += 1
            if calls == 1:
                return certificate_fd
            raise GuardError("memfd_write_failed")

        monkeypatch.setattr(
            authority_module,
            "create_sealed_memfd",
            fail_second_memfd,
        )

        with pytest.raises(GuardError) as caught:
            AuthorityClient(
                config,
                trusted_uid=os.geteuid(),
                trusted_gid=os.getegid(),
            )

        assert caught.value.code == "authority_credentials_invalid"
        with pytest.raises(OSError):
            os.fstat(certificate_fd)
