from __future__ import annotations

import hashlib
import json
import os
import socket
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from threading import Thread
from uuid import UUID

import pytest

from loom_task_image_builder_guard.__main__ import main
from loom_task_image_builder_guard.authority import (
    AcceptedAttestation,
    BuildSession,
    ProjectionChallenge,
    ProjectionReceipt,
)
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.ledger import GuardLedger
from loom_task_image_builder_guard.models import (
    AuthorityConfig,
    CommandConfig,
    CommandIdentity,
    ContainmentConfig,
    GuardConfigValue,
    IdentityConfig,
    IoLimit,
    ProtocolConfig,
    ServiceConfig,
    SlurmConfig,
)
from loom_task_image_builder_guard.protocol import (
    LOCAL_SCHEMA,
    create_sealed_memfd,
    read_sealed_memfd,
    receive_request,
    send_packet,
)
from loom_task_image_builder_guard.service import GuardService, NodeReconciler
from loom_task_image_builder_guard.slurm import SlurmFacts

NOW = datetime(2026, 9, 2, 16, 0, tzinfo=UTC)
GRANT = UUID("11111111-1111-1111-1111-111111111111")
REQUEST = UUID("22222222-2222-2222-2222-222222222222")
CHALLENGE = UUID("33333333-3333-3333-3333-333333333333")
PROOF = UUID("44444444-4444-4444-4444-444444444444")
RESPONSE = UUID("55555555-5555-5555-5555-555555555555")
EXCHANGE = UUID("66666666-6666-6666-6666-666666666666")
SESSION = UUID("77777777-7777-7777-7777-777777777777")
BOOT = UUID("88888888-8888-8888-8888-888888888888")
DIGEST_A = "1" * 64
DIGEST_B = "2" * 64
DIGEST_C = "3" * 64
DIGEST_D = "4" * 64
BOOTSTRAP = "loom_tibp_" + "A" * 64
SESSION_TOKEN = "loom_tibs_" + "B" * 64


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def _config(tmp_path: Path) -> GuardConfigValue:
    dummy = CommandIdentity(Path("/usr/bin/true"), DIGEST_A)
    return GuardConfigValue(
        cluster_id="gb10",
        cpu_arch="arm64",
        node_name="trt-gb10-1",
        identity=IdentityConfig(
            993,
            980,
            (0,),
            Path("/usr/local/libexec/loom-task-builder-supervisor"),
            DIGEST_A,
        ),
        protocol=ProtocolConfig(
            tmp_path / "run" / "guard.sock",
            0o660,
            os.getegid(),
            4096,
            8,
            16,
            2,
        ),
        authority=AuthorityConfig(
            "https://authority.invalid:8445",
            Path("/ca"),
            Path("/cert"),
            Path("/key"),
            Path("/bearer"),
            2,
            65536,
        ),
        commands=CommandConfig(dummy, dummy, dummy),
        slurm=SlurmConfig(
            "trt-gb10",
            DIGEST_B,
            "loom-task-builder",
            "loom-task-builder",
            "loom-task-image-builder-rootless-gb10",
            "loom_rootless_buildkit",
            8,
            32768,
            "02:00:00",
        ),
        containment=ContainmentConfig(
            Path("/sys/fs/cgroup"),
            tmp_path / "bpffs",
            tmp_path / "ledger",
            Path("/object"),
            Path("/policy"),
            4096,
            (IoLimit("8:1", 1, 1, 1, 1),),
            DIGEST_C,
            DIGEST_D,
            DIGEST_A,
            DIGEST_B,
        ),
        service=ServiceConfig(15, 60, 8),
    )


class _Peer:
    pid = 42100
    uid = 993
    gid = 980
    job_id = "12345"
    executable_sha256 = DIGEST_A
    batch_cgroup_relative = PurePosixPath("/slurm/job_12345/step_batch/user/task_0")
    cgroup_relative = batch_cgroup_relative

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.adopted = False
        self.closed = False

    def assert_unchanged(self) -> None:
        self.events.append("peer_assert")

    def adopt_trusted_service_cgroup(self) -> None:
        self.events.append("peer_move")
        self.adopted = True
        self.cgroup_relative = self.batch_cgroup_relative / "loom-builder" / "trusted-service"

    def close(self) -> None:
        self.events.append("peer_close")
        self.closed = True


class _Peers:
    def __init__(self, peer: _Peer, events: list[str]) -> None:
        self.peer = peer
        self.events = events

    def capture(self, connection: socket.socket) -> _Peer:
        assert connection.family == socket.AF_UNIX
        self.events.append("peer_capture")
        return self.peer


class _Slurm:
    def __init__(self, ledger: GuardLedger, events: list[str]) -> None:
        self.ledger = ledger
        self.events = events
        self.quarantines = 0

    def observe(self, *, job_id: str, grant_id: UUID) -> SlurmFacts:
        entry = self.ledger.get(grant_id)
        assert entry is not None
        self.events.append("slurm_observe")
        return SlurmFacts(
            job_id,
            "trt-gb10-1",
            f"loom-task-builder-v1:grant={grant_id}",
            "loom-task-builder",
            "loom-task-builder",
            "loom-task-image-builder-rootless-gb10",
            8,
            32768,
            "02:00:00",
        )

    def quarantine_capability(self) -> None:
        self.events.append("slurm_quarantine")
        self.quarantines += 1


@dataclass(frozen=True)
class _Batch:
    authority_path: str = "/sys/fs/cgroup/slurm/job_12345/step_batch/user/task_0"
    inode: int = 987654


class _Attachment:
    cgroup_inode = 987654
    containment_root = (
        "/sys/fs/cgroup/slurm/job_12345/step_batch/user/task_0/loom-builder"
    )
    trusted_service_cgroup = f"{containment_root}/trusted-service"
    build_egress_cgroup = f"{containment_root}/build-egress"
    bpf_program_sha256 = DIGEST_A
    bpf_map_schema_sha256 = DIGEST_B
    containment_policy_sha256 = DIGEST_C
    resource_limits_sha256 = DIGEST_D
    probe_sha256 = "5" * 64
    link_ids = tuple(range(101, 125))
    program_ids = tuple(range(201, 225))
    map_ids = tuple(range(301, 319))

    def __init__(self, events: list[str]) -> None:
        self.events = events

    def close(self) -> None:
        self.events.append("attachment_close")


class _Containment:
    def __init__(self, ledger: GuardLedger, peer: _Peer, events: list[str]) -> None:
        self.ledger = ledger
        self.peer = peer
        self.events = events

    def prepare(
        self, batch: _Batch, peer: _Peer, policy: object, grant_id: UUID
    ) -> _Attachment:
        del batch, policy
        assert peer is self.peer
        assert self.ledger.get(grant_id).state == "challenged"  # type: ignore[union-attr]
        self.events.append("containment_attach")
        peer.adopt_trusted_service_cgroup()
        return _Attachment(self.events)


class _Authority:
    def __init__(self, ledger: GuardLedger, peer: _Peer, events: list[str]) -> None:
        self.ledger = ledger
        self.peer = peer
        self.events = events

    @staticmethod
    def request_sha256(value: object) -> str:
        return hashlib.sha256(_json(value)).hexdigest()

    def challenge(self, grant_id: UUID, request: dict[str, object], **kwargs: object):
        del kwargs
        assert self.ledger.get(grant_id).state == "intent"  # type: ignore[union-attr]
        self.events.append("authority_challenge")
        return ProjectionChallenge(
            REQUEST,
            GRANT,
            self.request_sha256(request),
            CHALLENGE,
            DIGEST_C,
            DIGEST_D,
            NOW,
            NOW + timedelta(seconds=50),
        )

    def attach(self, grant_id: UUID, proof: dict[str, object]) -> ProjectionReceipt:
        assert grant_id == GRANT
        assert self.peer.adopted is True
        assert self.ledger.get(grant_id).state in {"attached", "projected"}  # type: ignore[union-attr]
        self.events.append("authority_attach")
        proof_sha = self.request_sha256(proof)
        wire = _json(
            {
                "schema_version": 1,
                "grant_id": str(GRANT),
                "proof_id": str(PROOF),
                "proof_sha256": proof_sha,
                "bootstrap_token": BOOTSTRAP,
                "issued_at": "2026-09-02T16:00:03Z",
                "expires_at": "2026-09-02T16:00:50Z",
            }
        )
        return ProjectionReceipt(
            GRANT,
            PROOF,
            proof_sha,
            BOOTSTRAP,
            NOW + timedelta(seconds=3),
            NOW + timedelta(seconds=50),
            wire,
        )

    def exchange(self, grant_id: UUID, request: dict[str, object]) -> BuildSession:
        assert grant_id == GRANT
        assert request["bootstrap_token"] == BOOTSTRAP
        self.events.append("authority_exchange")
        wire = _json(
            {
                "schema_version": 1,
                "grant_id": str(GRANT),
                "session_id": str(SESSION),
                "purpose": "production",
                "shadow_campaign_id": None,
                "pool_id": "staging-gb10-task-image",
                "cpu_arch": "arm64",
                "session_token": SESSION_TOKEN,
                "attestation_generation": 1,
                "attestation_sha256": self.ledger.get(GRANT).document()[  # type: ignore[union-attr]
                    "attestation_sha256"
                ],
                "issued_at": "2026-09-02T16:00:05Z",
                "expires_at": "2026-09-02T16:10:00Z",
            }
        )
        return BuildSession(
            GRANT,
            SESSION,
            "production",
            None,
            "staging-gb10-task-image",
            "arm64",
            SESSION_TOKEN,
            1,
            self.ledger.get(GRANT).document()["attestation_sha256"],  # type: ignore[arg-type,union-attr]
            NOW + timedelta(seconds=5),
            NOW + timedelta(minutes=10),
            wire,
        )

    def attest(
        self, grant_id: UUID, generation: int, attestation: dict[str, object]
    ) -> AcceptedAttestation:
        self.events.append("authority_attest")
        return AcceptedAttestation(
            UUID(str(attestation["attestation_id"])),
            grant_id,
            generation,
            NOW + timedelta(seconds=15),
            NOW + timedelta(seconds=60),
            self.request_sha256(attestation),
        )

    def revoke(self, grant_id: UUID, request: dict[str, object]) -> None:
        del grant_id, request
        self.events.append("authority_revoke")


class _Reconciler:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def reconcile(self, ledger: GuardLedger) -> None:
        del ledger
        self.events.append("reconcile")

    def assert_live(self, entry: object) -> None:
        del entry

    def recover_live(self) -> tuple[tuple[object, object], ...]:
        return ()


class _Probe:
    def __init__(self, pin_root: Path, classification: str) -> None:
        self.pin_root = pin_root
        self.classification = classification
        self.cleaned: list[UUID] = []

    def classify(self, entry: object) -> str:
        del entry
        return self.classification

    def assert_live(self, entry: object) -> None:
        if self.classification != "live_exact":
            raise GuardError("runtime_identity_ambiguous")
        del entry

    def open_live(self, entry: object) -> _Peer:
        del entry
        peer = _Peer([])
        peer.adopted = True
        peer.cgroup_relative = (
            peer.batch_cgroup_relative / "loom-builder" / "trusted-service"
        )
        return peer

    def cleanup_terminal(self, entry: object) -> None:
        grant_id = entry.grant_id  # type: ignore[attr-defined]
        path = self.pin_root / str(grant_id)
        if path.exists():
            path.rmdir()
        self.cleaned.append(grant_id)


def _service(tmp_path: Path, *, monotonic: object | None = None):
    config = _config(tmp_path)
    config.containment.ledger_root.mkdir(mode=0o700)
    config.containment.bpffs_root.mkdir(mode=0o700)
    config.protocol.socket_path.parent.mkdir(mode=0o700)
    ledger = GuardLedger(
        config.containment.ledger_root,
        config.service.max_ledger_entries,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )
    events: list[str] = []
    peer = _Peer(events)
    slurm = _Slurm(ledger, events)
    monotonic_factory = (lambda: 0.0) if monotonic is None else monotonic
    service = GuardService(
        config,
        ledger=ledger,
        peers=_Peers(peer, events),
        slurm=slurm,
        derive_batch=lambda value, job_id: (
            events.append("derive_batch") or _Batch()
        ),
        containment=_Containment(ledger, peer, events),
        policy=object(),
        authority=_Authority(ledger, peer, events),
        reconciler=_Reconciler(events),
        node_boot_id=BOOT,
        uuid_factory=iter((REQUEST, PROOF, RESPONSE, RESPONSE)).__next__,
        now_factory=lambda: NOW + timedelta(seconds=3),
        monotonic=monotonic_factory,  # type: ignore[arg-type]
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )
    return service, ledger, peer, slurm, events


def _run_connection(service: GuardService, server: socket.socket) -> Thread:
    def run() -> None:
        try:
            service.serve_connection(server)
        finally:
            server.close()

    thread = Thread(target=run)
    thread.start()
    return thread


def test_projection_orders_intent_observation_containment_proof_and_sealed_ack(
    tmp_path: Path,
) -> None:
    service, ledger, peer, _slurm, events = _service(tmp_path)
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(service, server)
    try:
        send_packet(
            client,
            _json({"schema": LOCAL_SCHEMA, "operation": "project", "grant_id": str(GRANT)}),
        )
        response_payload, descriptor = receive_request(client, maximum=4096)
        response = json.loads(response_payload)
        assert descriptor is not None
        receipt = read_sealed_memfd(descriptor, maximum=65536)
        os.close(descriptor)
        assert json.loads(receipt)["bootstrap_token"] == BOOTSTRAP
        assert ledger.get(GRANT).state == "projected"  # type: ignore[union-attr]
        send_packet(
            client,
            _json(
                {
                    "schema": LOCAL_SCHEMA,
                    "operation": "ack",
                    "response_id": response["response_id"],
                }
            ),
        )
        thread.join(timeout=3)
        assert not thread.is_alive()
    finally:
        client.close()

    assert events.index("peer_capture") < events.index("slurm_observe")
    assert events.index("authority_challenge") < events.index("containment_attach")
    assert events.index("peer_move") < events.index("authority_attach")
    service.close()
    assert events[-2:] == ["attachment_close", "peer_close"]
    assert peer.closed is True
    ledger.close()


def test_exchange_requires_outer_inner_binding_and_returns_only_sealed_session(
    tmp_path: Path,
) -> None:
    service, ledger, _peer, _slurm, _events = _service(tmp_path)
    first_server, first_client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    first_thread = _run_connection(service, first_server)
    send_packet(
        first_client,
        _json({"schema": LOCAL_SCHEMA, "operation": "project", "grant_id": str(GRANT)}),
    )
    projected_payload, projected_fd = receive_request(first_client, maximum=4096)
    assert projected_fd is not None
    projected = json.loads(projected_payload)
    os.close(projected_fd)
    send_packet(
        first_client,
        _json(
            {
                "schema": LOCAL_SCHEMA,
                "operation": "ack",
                "response_id": projected["response_id"],
            }
        ),
    )
    first_thread.join(timeout=3)
    first_client.close()

    proof_sha = ledger.get(GRANT).document()["proof_sha256"]  # type: ignore[union-attr]
    exchange_body = {
        "schema_version": 1,
        "exchange_id": str(EXCHANGE),
        "grant_id": str(GRANT),
        "proof_sha256": proof_sha,
        "bootstrap_token": BOOTSTRAP,
        "observed_at": "2026-09-02T16:00:03Z",
    }
    exchange_fd = create_sealed_memfd("bootstrap-exchange", _json(exchange_body), maximum=65536)
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(service, server)
    try:
        send_packet(
            client,
            _json(
                {
                    "schema": LOCAL_SCHEMA,
                    "operation": "exchange",
                    "grant_id": str(GRANT),
                    "exchange_id": str(EXCHANGE),
                    "proof_sha256": proof_sha,
                }
            ),
            descriptor=exchange_fd,
        )
        os.close(exchange_fd)
        response_payload, descriptor = receive_request(client, maximum=4096)
        response = json.loads(response_payload)
        assert descriptor is not None
        session = json.loads(read_sealed_memfd(descriptor, maximum=65536))
        os.close(descriptor)
        assert session["session_token"] == SESSION_TOKEN
        send_packet(
            client,
            _json(
                {
                    "schema": LOCAL_SCHEMA,
                    "operation": "ack",
                    "response_id": response["response_id"],
                }
            ),
        )
        thread.join(timeout=3)
        assert not thread.is_alive()
    finally:
        client.close()

    entry_payload = ledger.get(GRANT).raw  # type: ignore[union-attr]
    assert b"loom_tibp_" not in entry_payload
    assert b"loom_tibs_" not in entry_payload
    assert ledger.get(GRANT).state == "exchanged"  # type: ignore[union-attr]
    ledger.close()


def test_reconciliation_runs_before_socket_binding_and_only_narrow_quarantine(
    tmp_path: Path,
) -> None:
    service, ledger, _peer, _slurm, events = _service(tmp_path)
    service.stop()
    service.start()

    assert events[0] == "reconcile"
    assert not service.config.protocol.socket_path.exists()
    ledger.close()


def test_start_replaces_only_an_exact_stale_socket_under_singleton_lock(
    tmp_path: Path,
) -> None:
    service, ledger, _peer, _slurm, events = _service(tmp_path)
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    stale.bind(str(service.config.protocol.socket_path))
    os.chmod(service.config.protocol.socket_path, 0o660)
    os.chown(
        service.config.protocol.socket_path,
        os.geteuid(),
        service.config.protocol.socket_gid,
    )
    stale.close()

    service.stop()
    service.start()

    assert events[0] == "reconcile"
    assert not service.config.protocol.socket_path.exists()
    ledger.close()


def test_invalid_exchange_descriptor_never_reaches_authority_or_logs_secret(
    tmp_path: Path,
) -> None:
    service, ledger, _peer, _slurm, events = _service(tmp_path)
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(service, server)
    send_packet(
        client,
        _json(
            {
                "schema": LOCAL_SCHEMA,
                "operation": "exchange",
                "grant_id": str(GRANT),
                "exchange_id": str(EXCHANGE),
                "proof_sha256": DIGEST_A,
            }
        ),
    )
    error_payload, descriptor = receive_request(client, maximum=4096)
    thread.join(timeout=3)
    client.close()

    assert descriptor is None
    assert json.loads(error_payload) == {
        "schema": LOCAL_SCHEMA,
        "operation": "error",
        "code": "local_exchange_descriptor_required",
    }
    assert "authority_exchange" not in events
    ledger.close()


def test_attestation_advances_exactly_once_per_monotonic_interval(tmp_path: Path) -> None:
    monotonic = [0.0]
    service, ledger, _peer, _slurm, events = _service(
        tmp_path, monotonic=lambda: monotonic[0]
    )
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(service, server)
    send_packet(
        client,
        _json({"schema": LOCAL_SCHEMA, "operation": "project", "grant_id": str(GRANT)}),
    )
    response_payload, descriptor = receive_request(client, maximum=4096)
    assert descriptor is not None
    os.close(descriptor)
    response = json.loads(response_payload)
    send_packet(
        client,
        _json(
            {
                "schema": LOCAL_SCHEMA,
                "operation": "ack",
                "response_id": response["response_id"],
            }
        ),
    )
    thread.join(timeout=3)
    client.close()

    service.run_attestations_once()
    assert events.count("authority_attest") == 0
    monotonic[0] = 15.0
    service.run_attestations_once()
    service.run_attestations_once()

    assert events.count("authority_attest") == 1
    assert ledger.get(GRANT).document()["attestation_generation"] == 2  # type: ignore[union-attr]
    service.close()
    ledger.close()


def test_reconciler_retains_exact_live_pins_and_quarantines_ambiguity(
    tmp_path: Path,
) -> None:
    service, ledger, _peer, slurm, _events = _service(tmp_path)
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(service, server)
    send_packet(
        client,
        _json({"schema": LOCAL_SCHEMA, "operation": "project", "grant_id": str(GRANT)}),
    )
    response_payload, descriptor = receive_request(client, maximum=4096)
    assert descriptor is not None
    os.close(descriptor)
    response = json.loads(response_payload)
    send_packet(
        client,
        _json(
            {
                "schema": LOCAL_SCHEMA,
                "operation": "ack",
                "response_id": response["response_id"],
            }
        ),
    )
    thread.join(timeout=3)
    client.close()
    service.close()
    pin_path = service.config.containment.bpffs_root / str(GRANT)
    pin_path.mkdir(mode=0o700)

    live_probe = _Probe(service.config.containment.bpffs_root, "live_exact")
    reconciler = NodeReconciler(
        service.config.containment.bpffs_root,
        probe=live_probe,
        slurm=slurm,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )
    reconciler.reconcile(ledger)
    assert ledger.get(GRANT).state == "projected"  # type: ignore[union-attr]
    assert pin_path.exists()
    recovered = reconciler.recover_live()
    assert [entry.grant_id for entry, _peer in recovered] == [GRANT]
    for _entry, recovered_peer in recovered:
        recovered_peer.close()

    live_probe.classification = "ambiguous"
    reconciler.reconcile(ledger)
    assert ledger.get(GRANT).state == "quarantined"  # type: ignore[union-attr]
    assert pin_path.exists()
    assert slurm.quarantines == 1
    ledger.close()


def test_reconciler_removes_only_terminal_empty_exact_entry(tmp_path: Path) -> None:
    service, ledger, _peer, slurm, _events = _service(tmp_path)
    ledger.create_intent(
        grant_id=GRANT,
        request_id=REQUEST,
        peer_pid=42100,
        job_id="12345",
        peer_executable_sha256=DIGEST_A,
        batch_cgroup_relative="/slurm/job_12345/step_batch/user/task_0",
    )
    ledger.mark_terminal(GRANT, reason="slurm_completed")
    probe = _Probe(service.config.containment.bpffs_root, "terminal_empty")
    reconciler = NodeReconciler(
        service.config.containment.bpffs_root,
        probe=probe,
        slurm=slurm,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )

    reconciler.reconcile(ledger)

    assert ledger.get(GRANT) is None
    assert probe.cleaned == [GRANT]
    assert slurm.quarantines == 0
    ledger.close()


def test_project_retry_after_guard_restart_reuses_durable_proof_and_pins(
    tmp_path: Path,
) -> None:
    first, ledger, _peer, _slurm, _events = _service(tmp_path)
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(first, server)
    send_packet(
        client,
        _json({"schema": LOCAL_SCHEMA, "operation": "project", "grant_id": str(GRANT)}),
    )
    response_payload, descriptor = receive_request(client, maximum=4096)
    assert descriptor is not None
    os.close(descriptor)
    response = json.loads(response_payload)
    send_packet(
        client,
        _json(
            {
                "schema": LOCAL_SCHEMA,
                "operation": "ack",
                "response_id": response["response_id"],
            }
        ),
    )
    thread.join(timeout=3)
    client.close()
    first.close()
    persisted = ledger.get(GRANT)
    assert persisted is not None

    events: list[str] = []
    peer = _Peer(events)
    peer.adopted = True
    peer.cgroup_relative = (
        peer.batch_cgroup_relative / "loom-builder" / "trusted-service"
    )
    slurm = _Slurm(ledger, events)
    restarted = GuardService(
        first.config,
        ledger=ledger,
        peers=_Peers(peer, events),
        slurm=slurm,
        derive_batch=lambda value, job_id: events.append("derive_batch") or _Batch(),
        containment=_Containment(ledger, peer, events),
        policy=object(),
        authority=_Authority(ledger, peer, events),
        reconciler=_Reconciler(events),
        node_boot_id=BOOT,
        uuid_factory=iter((RESPONSE,)).__next__,
        now_factory=lambda: NOW + timedelta(seconds=4),
        monotonic=lambda: 0.0,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(restarted, server)
    send_packet(
        client,
        _json({"schema": LOCAL_SCHEMA, "operation": "project", "grant_id": str(GRANT)}),
    )
    replay_payload, replay_fd = receive_request(client, maximum=4096)
    assert replay_fd is not None
    os.close(replay_fd)
    replay = json.loads(replay_payload)
    send_packet(
        client,
        _json(
            {
                "schema": LOCAL_SCHEMA,
                "operation": "ack",
                "response_id": replay["response_id"],
            }
        ),
    )
    thread.join(timeout=3)
    client.close()

    assert "containment_attach" not in events
    assert ledger.get(GRANT).request_id == persisted.request_id  # type: ignore[union-attr]
    assert ledger.get(GRANT).document()["proof_sha256"] == persisted.document()[  # type: ignore[union-attr]
        "proof_sha256"
    ]
    restarted.close()
    ledger.close()


def test_cli_is_self_check_or_one_absolute_config_without_ambient_injection(
    capfd: pytest.CaptureFixture[str],
) -> None:
    assert main(["--self-check"], environ={}) == 0
    output = capfd.readouterr()
    assert json.loads(output.out) == {
        "schema": "loom.task-image-builder-node-guard-self-check/v1",
        "status": "ok",
    }
    assert output.err == ""

    assert main(["--config", "relative/private-token"], environ={}) == 2
    output = capfd.readouterr()
    assert output.out == ""
    assert output.err == "loom_task_image_builder_guard error=cli_arguments_invalid\n"
    assert "private-token" not in output.err

    assert main(
        ["--config", "/etc/loom/task-image-builder-guard/config.json"],
        environ={"PYTHONPATH": "/tmp/injected"},
    ) == 1
    output = capfd.readouterr()
    assert output.err == "loom_task_image_builder_guard error=unsafe_environment\n"
    assert "/tmp/injected" not in output.err
