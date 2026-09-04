from __future__ import annotations

import fcntl
import hashlib
import json
import os
import socket
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from threading import Event, Thread
from typing import ClassVar
from uuid import UUID

import pytest

import loom_task_image_builder_guard.__main__ as guard_main
from loom_task_image_builder_guard import protocol as protocol_module
from loom_task_image_builder_guard import service as service_module
from loom_task_image_builder_guard.__main__ import SystemdNotifier, main
from loom_task_image_builder_guard.authority import (
    AcceptedAttestation,
    BuildSession,
    ProjectionChallenge,
    ProjectionReceipt,
)
from loom_task_image_builder_guard.bpf import (
    ATTACHMENTS,
    BpfObjectInfo,
    Endpoint,
    NetworkPolicy,
    ScopeNetworkPolicy,
    TrafficLimits,
    static_map_items,
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
    PeerCredentials,
    create_sealed_memfd,
    read_sealed_memfd,
    receive_request,
    send_packet,
)
from loom_task_image_builder_guard.service import (
    GuardService,
    NodeReconciler,
    SystemReconciliationProbe,
)
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
            "192.0.2.10",
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
            ("0123456789abcdef",),
            4096,
            (IoLimit("8:1", 1, 1, 1, 1),),
            DIGEST_C,
            DIGEST_D,
            DIGEST_A,
            DIGEST_B,
        ),
        service=ServiceConfig(15, 60, 8),
    )


def _limits(value: int) -> TrafficLimits:
    return TrafficLimits(value, value, value, value, value, value, value)


def _network_policy() -> NetworkPolicy:
    trusted = Endpoint("203.0.113.10", 443, "tcp")
    build = Endpoint("198.51.100.20", 443, "tcp")
    return NetworkPolicy(
        containment_policy_sha256=DIGEST_C,
        resource_profile_sha256=DIGEST_D,
        bpf_program_sha256=DIGEST_A,
        bpf_map_schema_sha256=DIGEST_B,
        scopes=(
            ScopeNetworkPolicy("root", (build, trusted), (), _limits(300)),
            ScopeNetworkPolicy("trusted-service", (trusted,), (), _limits(100)),
            ScopeNetworkPolicy("build-egress", (build,), (), _limits(200)),
        ),
    )


def test_build_service_shares_one_progress_tracker_across_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class _Progress:
        def __init__(self) -> None:
            self.marks = 0

        def mark(self) -> None:
            self.marks += 1

    progress = _Progress()
    captured: dict[str, object] = {}
    runner = object()
    peers = object()
    slurm = object()
    kernel = object()
    network = object()
    loader = object()
    device_probe = object()
    containment = object()
    policy = object()
    ledger = object()
    probe = object()
    reconciler = object()
    authority = object()
    service = object()

    monkeypatch.setattr(guard_main, "MainLoopProgress", lambda: progress)

    def command_runner(*, progress: object) -> object:
        captured["runner_progress"] = progress
        return runner

    def peer_inspector(identity: object, *, progress: object) -> object:
        del identity
        captured["peers_progress"] = progress
        return peers

    monkeypatch.setattr(guard_main, "PinnedCommandRunner", command_runner)
    monkeypatch.setattr(guard_main, "PeerInspector", peer_inspector)
    monkeypatch.setattr(guard_main, "SlurmInspector", lambda **kwargs: slurm)
    monkeypatch.setattr(guard_main, "BpfSyscall", lambda: kernel)
    monkeypatch.setattr(
        guard_main,
        "NetworkPolicy",
        type("NetworkPolicyFactory", (), {"from_file": staticmethod(lambda *args, **kwargs: network)}),
    )
    monkeypatch.setattr(guard_main, "BpfLoader", lambda **kwargs: loader)
    monkeypatch.setattr(guard_main, "BpftoolDeviceProbe", lambda *args: device_probe)
    monkeypatch.setattr(guard_main, "ContainmentManager", lambda **kwargs: containment)
    monkeypatch.setattr(guard_main, "GuardPolicy", lambda **kwargs: policy)
    monkeypatch.setattr(guard_main, "GuardLedger", lambda *args: ledger)
    monkeypatch.setattr(guard_main, "SystemReconciliationProbe", lambda *args, **kwargs: probe)

    def node_reconciler(*args: object, progress: object, **kwargs: object) -> object:
        del args, kwargs
        captured["reconciler_progress"] = progress
        return reconciler

    def authority_client(config: object, *, progress: object) -> object:
        del config
        captured["authority_progress"] = progress
        return authority

    def guard_service(*args: object, progress: object, **kwargs: object) -> object:
        del args, kwargs
        captured["service_progress"] = progress
        return service

    monkeypatch.setattr(guard_main, "NodeReconciler", node_reconciler)
    monkeypatch.setattr(guard_main, "AuthorityClient", authority_client)
    monkeypatch.setattr(guard_main, "GuardService", guard_service)
    monkeypatch.setattr(guard_main, "_boot_id", lambda: BOOT)

    assert guard_main.build_service(_config(tmp_path)) is service
    assert captured["service_progress"] is progress
    callbacks = (
        captured["runner_progress"],
        captured["peers_progress"],
        captured["reconciler_progress"],
        captured["authority_progress"],
    )
    for callback in callbacks:
        assert callable(callback)
        callback()
    assert progress.marks == len(callbacks)


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

    @contextmanager
    def containment_hold(self) -> Iterator[None]:
        self.events.append("transfer_hold_begin")
        try:
            yield
        finally:
            self.events.append("transfer_hold_end")

    def close(self) -> None:
        self.events.append("peer_close")
        self.closed = True


class _Peers:
    def __init__(self, peer: _Peer, events: list[str]) -> None:
        self.peer = peer
        self.events = events

    def capture(
        self,
        connection: socket.socket,
        message_credentials: PeerCredentials,
    ) -> _Peer:
        assert connection.family == socket.AF_UNIX
        assert message_credentials.pid > 0
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
        assert self.ledger.get(grant_id).state == "containment_pending"  # type: ignore[union-attr]
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
        assert self.ledger.get(grant_id).state in {  # type: ignore[union-attr]
            "intent",
            "challenge_pending",
        }
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
            datetime.fromisoformat(str(attestation["issued_at"]).replace("Z", "+00:00")),
            datetime.fromisoformat(str(attestation["expires_at"]).replace("Z", "+00:00")),
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


class _PinnedKernel:
    _MAPS: ClassVar[dict[str, tuple[int, int, int, int]]] = {
        "scope_subject": (2, 4, 4, 1),
        "allow_v4": (1, 12, 1, 4096),
        "allow_v6": (1, 24, 1, 4096),
        "subject_limits": (1, 4, 120, 16),
        "flow_sockets": (1, 8, 8, 4096),
        "drop_counters": (6, 4, 8, 16),
    }

    def __init__(
        self,
        cgroups: dict[str, int],
        policy: NetworkPolicy,
        *,
        corrupt_link: bool = False,
    ) -> None:
        self.cgroups = cgroups
        self.corrupt_link = corrupt_link
        self.infos: dict[int, BpfObjectInfo] = {}
        self.opened: dict[int, tuple[str, str, str]] = {}
        self.map_contents = {
            (scope.name, name): dict(entries)
            for scope in policy.scopes
            for name, entries in static_map_items(scope).items()
        }
        self.ids: dict[tuple[str, str, str], int] = {}
        next_id = 100
        for scope in ("root", "trusted-service", "build-egress"):
            for kind, names in (
                ("program", tuple(item[0] for item in ATTACHMENTS)),
                ("map", tuple(self._MAPS)),
                ("link", tuple(item[0] for item in ATTACHMENTS)),
            ):
                for name in names:
                    next_id += 1
                    self.ids[(scope, kind, name)] = next_id

    def obj_get(self, path: Path) -> int:
        scope = path.parents[1].name
        kind = {"progs": "program", "maps": "map", "links": "link"}[
            path.parent.name
        ]
        name = path.name
        object_id = self.ids[(scope, kind, name)]
        if kind == "program":
            expected_type = {item[0]: item[2] for item in ATTACHMENTS}[name]
            info = BpfObjectInfo(
                "program",
                object_id,
                expected_type,
                name[:15],
            )
        elif kind == "map":
            map_type, key_size, value_size, maximum = self._MAPS[name]
            info = BpfObjectInfo(
                "map",
                object_id,
                map_type,
                name[:15],
                key_size=key_size,
                value_size=value_size,
                max_entries=maximum,
            )
        else:
            attach_type = {item[0]: item[1] for item in ATTACHMENTS}[name]
            program_id = self.ids[(scope, "program", name)]
            if self.corrupt_link and (scope, name) == ("root", "guard_egress"):
                program_id = self.ids[(scope, "program", "guard_ingress")]
            info = BpfObjectInfo(
                "link",
                object_id,
                3,
                "",
                program_id=program_id,
                cgroup_id=self.cgroups[scope],
                attach_type=attach_type,
            )
        descriptor = os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC)
        self.infos[descriptor] = info
        self.opened[descriptor] = (scope, kind, name)
        return descriptor

    def map_items(
        self,
        descriptor: int,
        *,
        key_size: int,
        value_size: int,
        max_entries: int,
    ) -> tuple[tuple[bytes, bytes], ...]:
        del key_size, value_size, max_entries
        scope, kind, name = self.opened[descriptor]
        assert kind == "map"
        return tuple(sorted(self.map_contents.get((scope, name), {}).items()))

    def object_info(self, descriptor: int, kind: str) -> BpfObjectInfo:
        info = self.infos[descriptor]
        assert info.kind == kind
        return info


@dataclass(frozen=True)
class _PinnedEntry:
    grant_id: UUID
    payload: dict[str, object]

    def document(self) -> dict[str, object]:
        return self.payload


@dataclass(frozen=True)
class _PinnedDeviceProgram:
    program_id: int
    tag: str = "0123456789abcdef"
    attach_type: str = "cgroup_device"
    attach_flags: str = ""
    name: str = "slurm_device"


class _PinnedDeviceProbe:
    def __init__(self) -> None:
        self.program_id = 77
        self.tag = "0123456789abcdef"

    def inspect_path(self, path: Path) -> tuple[_PinnedDeviceProgram, ...]:
        assert path.name == "task_0"
        return (_PinnedDeviceProgram(self.program_id, tag=self.tag),)


def _pinned_probe_fixture(
    tmp_path: Path,
    *,
    corrupt_link: bool,
) -> tuple[SystemReconciliationProbe, _PinnedEntry, _PinnedDeviceProbe]:
    config = _config(tmp_path)
    cgroup_root = tmp_path / "cgroup"
    batch = cgroup_root / "slurm/job_12345/step_batch/user/task_0"
    containment = batch / "loom-builder"
    cgroup_paths = {
        "root": containment,
        "trusted-service": containment / "trusted-service",
        "build-egress": containment / "build-egress",
    }
    for path in cgroup_paths.values():
        path.mkdir(parents=True, exist_ok=True)
    controls = {
        batch: {
            "cgroup.type": "domain\n",
            "cgroup.procs": "",
            "cgroup.stat": "nr_descendants 3\nnr_dying_descendants 0\n",
            "cgroup.controllers": "io pids\n",
            "cgroup.subtree_control": "io pids\n",
            "cpuset.cpus.effective": "0-7\n",
            "cpu.max": "800000 100000\n",
            "memory.max": "34359738368\n",
            "memory.swap.max": "0\n",
        },
        cgroup_paths["root"]: {
            "cgroup.type": "domain\n",
            "cgroup.procs": "",
            "cgroup.stat": "nr_descendants 2\nnr_dying_descendants 0\n",
            "cgroup.subtree_control": "io pids\n",
            "pids.max": "4096\n",
            "io.max": "8:1 rbps=1 wbps=1 riops=1 wiops=1\n",
        },
        cgroup_paths["trusted-service"]: {
            "cgroup.type": "domain\n",
            "cgroup.procs": "42100\n",
            "cgroup.stat": "nr_descendants 0\nnr_dying_descendants 0\n",
            "cgroup.subtree_control": "\n",
        },
        cgroup_paths["build-egress"]: {
            "cgroup.type": "domain\n",
            "cgroup.procs": "",
            "cgroup.stat": "nr_descendants 0\nnr_dying_descendants 0\n",
            "cgroup.subtree_control": "\n",
        },
    }
    for root, files in controls.items():
        for name, value in files.items():
            control = root / name
            control.write_text(value, encoding="ascii")
            control.chmod(0o644)
    pin_root = tmp_path / "pins"
    grant_root = pin_root / str(GRANT)
    for scope in cgroup_paths:
        for kind in ("progs", "maps", "links"):
            root = grant_root / scope / kind
            root.mkdir(parents=True)
            root.chmod(0o700)
            root.parent.chmod(0o700)
            names = (
                tuple(item[0] for item in ATTACHMENTS)
                if kind != "maps"
                else tuple(_PinnedKernel._MAPS)
            )
            for name in names:
                (root / name).touch()
    grant_root.chmod(0o700)
    config = replace(
        config,
        identity=replace(
            config.identity,
            uid=os.geteuid(),
            gid=os.getegid(),
        ),
        containment=replace(
            config.containment,
            cgroup_root=cgroup_root,
            bpffs_root=pin_root,
        ),
    )
    cgroup_ids = {name: path.stat().st_ino for name, path in cgroup_paths.items()}
    network_policy = _network_policy()
    kernel = _PinnedKernel(
        cgroup_ids,
        network_policy,
        corrupt_link=corrupt_link,
    )
    device_probe = _PinnedDeviceProbe()
    payload: dict[str, object] = {
        "pin_path": str(grant_root),
        "link_ids": sorted(
            value
            for (scope, kind, name), value in kernel.ids.items()
            if kind == "link"
        ),
        "program_ids": sorted(
            value
            for (scope, kind, name), value in kernel.ids.items()
            if kind == "program"
        ),
        "map_ids": sorted(
            value
            for (scope, kind, name), value in kernel.ids.items()
            if kind == "map"
        ),
        "proof": {
            "cgroup_inode": batch.stat().st_ino,
            "attachment": {
                "schema_version": 1,
                "cgroup_inode": batch.stat().st_ino,
                "containment_root": str(cgroup_paths["root"]),
                "trusted_service_cgroup": str(cgroup_paths["trusted-service"]),
                "build_egress_cgroup": str(cgroup_paths["build-egress"]),
                "bpf_program_sha256": config.containment.bpf_program_sha256,
                "bpf_map_schema_sha256": config.containment.bpf_map_schema_sha256,
                "containment_policy_sha256": (
                    config.containment.containment_policy_sha256
                ),
                "resource_limits_sha256": (
                    config.containment.resource_profile_sha256
                ),
            }
        },
        "projection_request": {
            "cgroup_path": str(batch),
            "cgroup_inode": batch.stat().st_ino,
        },
    }
    attachment = payload["proof"]["attachment"]  # type: ignore[index]
    attachment["link_ids"] = list(payload["link_ids"])  # type: ignore[arg-type,index]
    attachment["program_ids"] = list(  # type: ignore[arg-type,index]
        payload["program_ids"]
    )
    attachment["map_ids"] = list(payload["map_ids"])  # type: ignore[arg-type,index]
    attachment["probe_sha256"] = hashlib.sha256(  # type: ignore[index]
        _json(
            {
                "schema": "loom.task-image-builder-guard-containment-probe/v1",
                "cgroups": {
                    "batch": {"path": str(batch), "inode": batch.stat().st_ino},
                    "root": {
                        "path": str(cgroup_paths["root"]),
                        "inode": cgroup_ids["root"],
                    },
                    "trusted_service": {
                        "path": str(cgroup_paths["trusted-service"]),
                        "inode": cgroup_ids["trusted-service"],
                    },
                    "build_egress": {
                        "path": str(cgroup_paths["build-egress"]),
                        "inode": cgroup_ids["build-egress"],
                    },
                },
                "descendants": {
                    "batch": 3,
                    "root": 2,
                    "trusted_service": 0,
                    "build_egress": 0,
                },
                "inherited": {
                    "controllers": ["io", "pids"],
                    "delegated": [],
                    "cpu_count": 8,
                    "cpu_max": [800000, 100000],
                    "memory_max": 34359738368,
                    "memory_swap_max": 0,
                    "device_programs": [
                        {
                            "id": 77,
                            "tag": "0123456789abcdef",
                            "attach_type": "cgroup_device",
                            "attach_flags": "",
                            "name": "slurm_device",
                        }
                    ],
                },
                "applied": {
                    "pids_max": 4096,
                    "io_max": ["8:1 rbps=1 wbps=1 riops=1 wiops=1"],
                },
                "process_migration": {
                    "common_ancestor": {
                        "path": str(cgroup_paths["root"] / "cgroup.procs"),
                        "uid": os.geteuid(),
                        "gid": os.getegid(),
                        "mode": 0o644,
                    },
                    "destination": {
                        "path": str(
                            cgroup_paths["build-egress"] / "cgroup.procs"
                        ),
                        "uid": os.geteuid(),
                        "gid": os.getegid(),
                        "mode": 0o644,
                    },
                },
                "bpf": {
                    "pin_path": str(grant_root),
                    "link_ids": payload["link_ids"],
                    "program_ids": payload["program_ids"],
                    "map_ids": payload["map_ids"],
                },
            }
        )
    ).hexdigest()
    probe = SystemReconciliationProbe(
        config,
        peers=object(),  # type: ignore[arg-type]
        slurm=object(),  # type: ignore[arg-type]
        kernel=kernel,  # type: ignore[arg-type]
        device_probe=device_probe,  # type: ignore[arg-type]
        network_policy=network_policy,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )
    return probe, _PinnedEntry(GRANT, payload), device_probe


def _service(
    tmp_path: Path,
    *,
    monotonic: object | None = None,
    startup: object | None = None,
    ready: object | None = None,
    watchdog: object | None = None,
    keepalive_interval_seconds: float = 10.0,
    progress_timeout_seconds: float = 75.0,
    startup_extension_limit_seconds: float = 900.0,
):
    config = _config(tmp_path)
    config.containment.ledger_root.mkdir(mode=0o700)
    config.containment.bpffs_root.mkdir(mode=0o700)
    config.protocol.socket_path.parent.mkdir(mode=0o711)
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
        startup=(lambda: None) if startup is None else startup,  # type: ignore[arg-type]
        ready=(lambda: None) if ready is None else ready,  # type: ignore[arg-type]
        watchdog=(lambda: None) if watchdog is None else watchdog,  # type: ignore[arg-type]
        keepalive_interval_seconds=keepalive_interval_seconds,
        progress_timeout_seconds=progress_timeout_seconds,
        startup_extension_limit_seconds=startup_extension_limit_seconds,
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


def test_peer_pidfd_is_captured_before_local_request_parsing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, ledger, peer, _slurm, events = _service(tmp_path)
    parse = service_module.parse_local_request

    def record_parse(payload: bytes) -> object:
        events.append("request_parse")
        return parse(payload)

    monkeypatch.setattr(service_module, "parse_local_request", record_parse)
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(service, server)
    try:
        send_packet(client, b"not-json")
        response_payload, descriptor = receive_request(client, maximum=4096)
        thread.join(timeout=1)
    finally:
        client.close()

    assert descriptor is None
    assert json.loads(response_payload)["code"] == "local_request_invalid"
    assert events.index("peer_capture") < events.index("request_parse")
    assert events[-1] == "peer_close"
    assert peer.closed is True
    ledger.close()


def test_peer_pidfd_is_captured_before_received_descriptor_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, ledger, peer, _slurm, events = _service(tmp_path)
    descriptor = create_sealed_memfd("capture-order", b"sealed", maximum=4096)
    original_fcntl = protocol_module.fcntl.fcntl

    def record_descriptor_lifecycle(
        received_fd: int,
        operation: int,
        argument: int = 0,
    ) -> int:
        if operation in {fcntl.F_GETFD, fcntl.F_SETFD}:
            events.append("descriptor_validation")
        return int(original_fcntl(received_fd, operation, argument))

    monkeypatch.setattr(protocol_module.fcntl, "fcntl", record_descriptor_lifecycle)
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(service, server)
    try:
        send_packet(
            client,
            _json(
                {
                    "schema": LOCAL_SCHEMA,
                    "operation": "project",
                    "grant_id": str(GRANT),
                }
            ),
            descriptor=descriptor,
        )
        response_payload, response_descriptor = receive_request(client, maximum=4096)
        thread.join(timeout=1)
    finally:
        client.close()
        os.close(descriptor)

    assert response_descriptor is None
    assert json.loads(response_payload)["code"] == "local_project_descriptor_forbidden"
    assert events.index("peer_capture") < events.index("descriptor_validation")
    assert events[-1] == "peer_close"
    assert peer.closed is True
    ledger.close()


def test_projection_orders_intent_observation_containment_proof_and_sealed_ack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, ledger, peer, _slurm, events = _service(tmp_path)
    real_send = service_module.send_packet

    def observed_send(
        connection: socket.socket,
        payload: bytes,
        *,
        descriptor: int | None = None,
    ) -> None:
        if descriptor is not None:
            events.append("secret_send")
        real_send(connection, payload, descriptor=descriptor)

    monkeypatch.setattr(service_module, "send_packet", observed_send)
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
    assert events.index("transfer_hold_begin") < events.index("secret_send")
    assert events.index("secret_send") < events.index("transfer_hold_end")
    service.close()
    assert events[-2:] == ["attachment_close", "peer_close"]
    assert peer.closed is True
    ledger.close()


def test_unrecorded_containment_mutation_revokes_quarantines_and_withdraws_feature(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, ledger, peer, slurm, events = _service(tmp_path)

    def fail_attachment_record(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise GuardError("ledger_write_failed")

    monkeypatch.setattr(ledger, "record_attachment", fail_attachment_record)
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(service, server)
    try:
        send_packet(
            client,
            _json(
                {"schema": LOCAL_SCHEMA, "operation": "project", "grant_id": str(GRANT)}
            ),
        )
        response_payload, descriptor = receive_request(client, maximum=4096)
        thread.join(timeout=3)
        assert not thread.is_alive()
    finally:
        client.close()

    assert descriptor is None
    assert json.loads(response_payload) == {
        "schema": LOCAL_SCHEMA,
        "operation": "error",
        "code": "ledger_write_failed",
    }
    assert peer.adopted is True
    assert ledger.get(GRANT).state == "quarantined"  # type: ignore[union-attr]
    assert events.index("containment_attach") < events.index("authority_revoke")
    assert events.index("authority_revoke") < events.index("slurm_quarantine")
    assert events[-2:] == ["attachment_close", "peer_close"]
    assert slurm.quarantines == 1
    ledger.close()


def test_containment_mutation_has_a_durable_pending_state_before_prepare(
    tmp_path: Path,
) -> None:
    service, ledger, peer, _slurm, _events = _service(tmp_path)
    observed_states: list[str] = []

    class SimulatedCrash(BaseException):
        pass

    class CrashingContainment:
        def prepare(self, *args: object, **kwargs: object) -> object:
            del args, kwargs
            entry = ledger.get(GRANT)
            assert entry is not None
            observed_states.append(entry.state)
            raise SimulatedCrash

    service.containment = CrashingContainment()  # type: ignore[assignment]
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        with pytest.raises(SimulatedCrash):
            service._project(
                server,
                GRANT,
                peer,
                PeerCredentials(pid=42100, uid=993, gid=980),
            )
    finally:
        server.close()
        client.close()

    assert observed_states == ["containment_pending"]
    assert ledger.get(GRANT).state == "containment_pending"  # type: ignore[union-attr]
    ledger.close()


def test_reconciliation_quarantines_pending_containment_even_if_peer_looks_live(
    tmp_path: Path,
) -> None:
    service, ledger, peer, slurm, _events = _service(tmp_path)

    class SimulatedCrash(BaseException):
        pass

    class CrashingContainment:
        def prepare(self, *args: object, **kwargs: object) -> object:
            del args, kwargs
            raise SimulatedCrash

    service.containment = CrashingContainment()  # type: ignore[assignment]
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        with pytest.raises(SimulatedCrash):
            service._project(
                server,
                GRANT,
                peer,
                PeerCredentials(pid=42100, uid=993, gid=980),
            )
    finally:
        server.close()
        client.close()
    reconciler = NodeReconciler(
        service.config.containment.bpffs_root,
        probe=_Probe(service.config.containment.bpffs_root, "live_exact"),
        slurm=slurm,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )

    reconciler.reconcile(ledger)

    assert ledger.get(GRANT).state == "quarantined"  # type: ignore[union-attr]
    assert slurm.quarantines == 1
    ledger.close()


def test_recorded_attachment_survives_authority_transport_failure_for_exact_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, ledger, peer, slurm, events = _service(tmp_path)
    original_attach = service.authority.attach

    def fail_attach(grant_id: UUID, proof: dict[str, object]) -> ProjectionReceipt:
        del grant_id, proof
        raise GuardError("authority_transport_failed")

    monkeypatch.setattr(service.authority, "attach", fail_attach)
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(service, server)
    try:
        send_packet(
            client,
            _json(
                {"schema": LOCAL_SCHEMA, "operation": "project", "grant_id": str(GRANT)}
            ),
        )
        failure_payload, failure_fd = receive_request(client, maximum=4096)
        thread.join(timeout=3)
        assert not thread.is_alive()
    finally:
        client.close()

    assert failure_fd is None
    assert json.loads(failure_payload)["code"] == "authority_transport_failed"
    assert ledger.get(GRANT).state == "attached"  # type: ignore[union-attr]
    assert slurm.quarantines == 0

    monkeypatch.setattr(service.authority, "attach", original_attach)
    peer.closed = False
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(service, server)
    try:
        send_packet(
            client,
            _json(
                {"schema": LOCAL_SCHEMA, "operation": "project", "grant_id": str(GRANT)}
            ),
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
        assert not thread.is_alive()
    finally:
        client.close()

    assert events.count("containment_attach") == 1
    assert ledger.get(GRANT).state == "projected"  # type: ignore[union-attr]
    service.close()
    ledger.close()


def test_projection_replays_exact_persisted_request_after_challenge_commit_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, ledger, peer, _slurm, _events = _service(tmp_path)
    original_challenge = service.authority.challenge
    original_record = ledger.record_challenge
    requests: list[dict[str, object]] = []

    def record_request(
        grant_id: UUID, request: dict[str, object], **kwargs: object
    ) -> ProjectionChallenge:
        requests.append(json.loads(_json(request)))
        return original_challenge(grant_id, request, **kwargs)

    def crash_after_authority(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise GuardError("ledger_write_failed")

    monkeypatch.setattr(service.authority, "challenge", record_request)
    monkeypatch.setattr(ledger, "record_challenge", crash_after_authority)
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    thread = _run_connection(service, server)
    send_packet(
        client,
        _json({"schema": LOCAL_SCHEMA, "operation": "project", "grant_id": str(GRANT)}),
    )
    failure_payload, failure_fd = receive_request(client, maximum=4096)
    thread.join(timeout=3)
    client.close()

    assert failure_fd is None
    assert json.loads(failure_payload)["code"] == "ledger_write_failed"
    assert ledger.get(GRANT).state in {"intent", "challenge_pending"}  # type: ignore[union-attr]

    monkeypatch.setattr(ledger, "record_challenge", original_record)
    service._now = lambda: NOW + timedelta(seconds=4)
    peer.closed = False
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

    assert requests[0] == requests[1]
    service.close()
    ledger.close()


def test_exchange_requires_outer_inner_binding_and_returns_only_sealed_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, ledger, _peer, _slurm, events = _service(tmp_path)
    real_send = service_module.send_packet

    def observed_send(
        connection: socket.socket,
        payload: bytes,
        *,
        descriptor: int | None = None,
    ) -> None:
        if descriptor is not None:
            events.append("secret_send")
        real_send(connection, payload, descriptor=descriptor)

    monkeypatch.setattr(service_module, "send_packet", observed_send)
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
    events.clear()

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
    assert events.index("transfer_hold_begin") < events.index("secret_send")
    assert events.index("secret_send") < events.index("transfer_hold_end")
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


def test_singleton_lock_is_held_before_reconciliation_can_mutate_state(
    tmp_path: Path,
) -> None:
    service, ledger, _peer, _slurm, events = _service(tmp_path)
    runtime_fd = os.open(
        service.config.protocol.socket_path.parent,
        os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY,
    )
    fcntl.flock(runtime_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(GuardError) as caught:
            service.start()
    finally:
        os.close(runtime_fd)

    assert caught.value.code == "service_already_running"
    assert "reconcile" not in events
    ledger.close()


def test_recovered_allocation_is_freshly_attested_before_service_readiness(
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
    entry = ledger.get(GRANT)
    assert entry is not None

    events: list[str] = []
    peer = _Peer(events)
    peer.adopted = True
    peer.cgroup_relative = (
        peer.batch_cgroup_relative / "loom-builder" / "trusted-service"
    )

    class RecoveringReconciler(_Reconciler):
        def recover_live(self) -> tuple[tuple[object, object], ...]:
            return ((entry, peer),)

    slurm = _Slurm(ledger, events)
    restarted: GuardService

    def ready() -> None:
        events.append("ready")
        restarted.stop()

    restarted = GuardService(
        first.config,
        ledger=ledger,
        peers=_Peers(peer, events),
        slurm=slurm,
        derive_batch=lambda value, job_id: _Batch(),
        containment=_Containment(ledger, peer, events),
        policy=object(),
        authority=_Authority(ledger, peer, events),
        reconciler=RecoveringReconciler(events),
        node_boot_id=BOOT,
        uuid_factory=iter((UUID("99999999-9999-9999-9999-999999999999"),)).__next__,
        now_factory=lambda: NOW + timedelta(seconds=4),
        monotonic=lambda: 0.0,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
        ready=ready,
    )

    restarted.start()

    assert events.index("authority_attest") < events.index("ready")
    assert ledger.get(GRANT).document()["attestation_generation"] == 2  # type: ignore[union-attr]
    restarted.close()
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


def test_start_accepts_a_root_owned_search_only_runtime_directory(tmp_path: Path) -> None:
    service, ledger, _peer, _slurm, _events = _service(tmp_path)
    service.config.protocol.socket_path.parent.chmod(0o711)

    service.stop()
    service.start()

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


def test_deeply_nested_exchange_document_returns_a_typed_error(tmp_path: Path) -> None:
    service, ledger, _peer, _slurm, _events = _service(tmp_path)
    payload = b"[" * 2000 + b"0" + b"]" * 2000

    with pytest.raises(GuardError) as caught:
        service._exchange_document(payload)

    assert caught.value.code == "local_exchange_invalid"
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


def test_attestation_replays_exact_pending_document_after_remote_commit_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic = [0.0]
    service, ledger, _peer, _slurm, _events = _service(
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

    attestations: list[dict[str, object]] = []
    original_attest = service.authority.attest
    original_record = ledger.record_attestation
    attestation_ids = iter(
        (
            UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        )
    )
    service._new_uuid = attestation_ids.__next__

    def record_attestation(
        grant_id: UUID, generation: int, attestation: dict[str, object]
    ) -> AcceptedAttestation:
        attestations.append(json.loads(_json(attestation)))
        return original_attest(grant_id, generation, attestation)

    class SimulatedCrash(BaseException):
        pass

    def crash_after_authority(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise SimulatedCrash

    monkeypatch.setattr(service.authority, "attest", record_attestation)
    monkeypatch.setattr(ledger, "record_attestation", crash_after_authority)
    monotonic[0] = 15.0
    service._now = lambda: NOW + timedelta(seconds=15)

    with pytest.raises(SimulatedCrash):
        service.run_attestations_once()

    monkeypatch.setattr(ledger, "record_attestation", original_record)
    service._now = lambda: NOW + timedelta(seconds=16)
    service.run_attestations_once()

    assert attestations[0] == attestations[1]
    assert ledger.get(GRANT).document()["attestation_generation"] == 2  # type: ignore[union-attr]
    service.close()
    ledger.close()


def test_attestation_clock_failure_still_quarantines_and_closes_live_state(
    tmp_path: Path,
) -> None:
    monotonic = [0.0]
    service, ledger, peer, slurm, events = _service(
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
    service._now = lambda: datetime(2026, 9, 2, 16, 0, 15)
    monotonic[0] = 15.0

    service.run_attestations_once()

    assert ledger.get(GRANT).state == "quarantined"  # type: ignore[union-attr]
    assert slurm.quarantines == 1
    assert peer.closed is True
    assert events[-3:] == ["slurm_quarantine", "attachment_close", "peer_close"]
    ledger.close()


def test_attestation_withdrawal_failure_aborts_service_after_closing_live_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic = [0.0]
    service, ledger, peer, slurm, _events = _service(
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
    service._now = lambda: datetime(2026, 9, 2, 16, 0, 15)
    monotonic[0] = 15.0
    attempts = 0

    def fail_withdrawal() -> None:
        nonlocal attempts
        attempts += 1
        raise GuardError("slurm_quarantine_failed")

    monkeypatch.setattr(slurm, "quarantine_capability", fail_withdrawal)

    with pytest.raises(GuardError) as caught:
        service.run_attestations_once()

    assert caught.value.code == "slurm_quarantine_failed"
    assert attempts == 1
    assert peer.closed is True
    assert GRANT not in service._live
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


def test_reconciler_preserves_only_verified_clean_pre_containment_intent(
    tmp_path: Path,
) -> None:
    service, ledger, _peer, slurm, _events = _service(tmp_path)
    ledger.create_intent(
        grant_id=GRANT,
        request_id=REQUEST,
        peer_pid=42100,
        job_id="12345",
        peer_executable_sha256=DIGEST_A,
        batch_cgroup_relative="/slurm/job_12345/step_batch/user/task_0",
    )
    probe = _Probe(service.config.containment.bpffs_root, "pre_containment_clean")
    reconciler = NodeReconciler(
        service.config.containment.bpffs_root,
        probe=probe,
        slurm=slurm,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )

    reconciler.reconcile(ledger)

    assert ledger.get(GRANT).state == "intent"  # type: ignore[union-attr]
    assert reconciler.recover_live() == ()
    assert slurm.quarantines == 0
    ledger.close()


def test_reconciler_quarantines_pre_containment_intent_with_unjournaled_pins(
    tmp_path: Path,
) -> None:
    service, ledger, _peer, slurm, _events = _service(tmp_path)
    ledger.create_intent(
        grant_id=GRANT,
        request_id=REQUEST,
        peer_pid=42100,
        job_id="12345",
        peer_executable_sha256=DIGEST_A,
        batch_cgroup_relative="/slurm/job_12345/step_batch/user/task_0",
    )
    (service.config.containment.bpffs_root / str(GRANT)).mkdir(mode=0o700)
    reconciler = NodeReconciler(
        service.config.containment.bpffs_root,
        probe=_Probe(
            service.config.containment.bpffs_root,
            "pre_containment_clean",
        ),
        slurm=slurm,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )

    reconciler.reconcile(ledger)

    assert ledger.get(GRANT).state == "quarantined"  # type: ignore[union-attr]
    assert slurm.quarantines == 1
    ledger.close()


def test_reconciler_quarantines_containment_pending_without_runtime_probe(
    tmp_path: Path,
) -> None:
    service, ledger, peer, slurm, _events = _service(tmp_path)

    class SimulatedCrash(BaseException):
        pass

    class CrashingContainment:
        def prepare(self, *args: object, **kwargs: object) -> object:
            del args, kwargs
            raise SimulatedCrash

    service.containment = CrashingContainment()  # type: ignore[assignment]
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        with pytest.raises(SimulatedCrash):
            service._project(
                server,
                GRANT,
                peer,
                PeerCredentials(pid=42100, uid=993, gid=980),
            )
    finally:
        server.close()
        client.close()

    class ForbiddenProbe(_Probe):
        def classify(self, entry: object) -> str:
            del entry
            raise AssertionError("containment-pending state must not be probed as safe")

    reconciler = NodeReconciler(
        service.config.containment.bpffs_root,
        probe=ForbiddenProbe(service.config.containment.bpffs_root, "live_exact"),
        slurm=slurm,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )

    reconciler.reconcile(ledger)

    assert ledger.get(GRANT).state == "quarantined"  # type: ignore[union-attr]
    assert slurm.quarantines == 1
    ledger.close()


def test_pre_containment_probe_requires_stable_empty_batch_subtree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe, _entry, _device_probe = _pinned_probe_fixture(
        tmp_path / "fixture", corrupt_link=False
    )
    batch = tmp_path / "batch"
    batch.mkdir()
    stat_path = batch / "cgroup.stat"
    stat_path.write_text(
        "nr_descendants 0\nnr_dying_descendants 0\n",
        encoding="ascii",
    )

    probe._verify_pre_containment_batch(batch)

    observations = iter(
        (
            "nr_descendants 0\nnr_dying_descendants 0\n",
            "nr_descendants 1\nnr_dying_descendants 0\n",
        )
    )
    monkeypatch.setattr(probe, "_read_control", lambda *args, **kwargs: next(observations))
    with pytest.raises(GuardError) as caught:
        probe._verify_pre_containment_batch(batch)
    assert caught.value.code == "reconciliation_cgroup_identity_invalid"


def test_reconciler_withdraws_feature_when_durable_quarantine_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, ledger, _peer, slurm, _events = _service(tmp_path)
    ledger.create_intent(
        grant_id=GRANT,
        request_id=REQUEST,
        peer_pid=42100,
        job_id="12345",
        peer_executable_sha256=DIGEST_A,
        batch_cgroup_relative="/slurm/job_12345/step_batch/user/task_0",
    )
    probe = _Probe(service.config.containment.bpffs_root, "ambiguous")
    reconciler = NodeReconciler(
        service.config.containment.bpffs_root,
        probe=probe,
        slurm=slurm,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )

    def fail_quarantine(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise GuardError("ledger_write_failed")

    monkeypatch.setattr(ledger, "quarantine", fail_quarantine)

    with pytest.raises(GuardError) as caught:
        reconciler.reconcile(ledger)

    assert caught.value.code == "ledger_write_failed"
    assert slurm.quarantines == 1
    ledger.close()


def test_reconciler_attempts_failed_feature_withdrawal_only_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, ledger, _peer, slurm, _events = _service(tmp_path)
    ledger.create_intent(
        grant_id=GRANT,
        request_id=REQUEST,
        peer_pid=42100,
        job_id="12345",
        peer_executable_sha256=DIGEST_A,
        batch_cgroup_relative="/slurm/job_12345/step_batch/user/task_0",
    )
    probe = _Probe(service.config.containment.bpffs_root, "ambiguous")
    reconciler = NodeReconciler(
        service.config.containment.bpffs_root,
        probe=probe,
        slurm=slurm,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )
    attempts = 0

    def fail_withdrawal() -> None:
        nonlocal attempts
        attempts += 1
        raise GuardError("slurm_quarantine_failed")

    monkeypatch.setattr(slurm, "quarantine_capability", fail_withdrawal)

    with pytest.raises(GuardError) as caught:
        reconciler.reconcile(ledger)

    assert caught.value.code == "slurm_quarantine_failed"
    assert attempts == 1
    ledger.close()


def test_pin_reconciliation_rejects_a_link_to_the_wrong_pinned_program(
    tmp_path: Path,
) -> None:
    exact_probe, exact_entry, _exact_device = _pinned_probe_fixture(
        tmp_path / "exact",
        corrupt_link=False,
    )
    exact_probe._verify_pin_tree(exact_entry)  # type: ignore[arg-type]
    probe, entry, _device = _pinned_probe_fixture(
        tmp_path / "corrupt",
        corrupt_link=True,
    )

    with pytest.raises(GuardError) as caught:
        probe._verify_pin_tree(entry)  # type: ignore[arg-type]

    assert caught.value.code == "reconciliation_pin_identity_invalid"


def test_runtime_reconciliation_rejects_resource_limit_drift(tmp_path: Path) -> None:
    probe, entry, _device = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    probe._verify_pin_tree(entry)  # type: ignore[arg-type]
    attachment = entry.payload["proof"]  # type: ignore[index]
    root = Path(attachment["attachment"]["containment_root"])  # type: ignore[index]
    (root / "pids.max").write_text("max\n", encoding="ascii")

    with pytest.raises(GuardError) as caught:
        probe._verify_runtime_controls(entry.payload)

    assert caught.value.code == "reconciliation_resource_identity_invalid"


def test_runtime_reconciliation_accepts_containment_descendant_binding(
    tmp_path: Path,
) -> None:
    probe, entry, _device = _pinned_probe_fixture(tmp_path, corrupt_link=False)

    probe._verify_runtime_controls(entry.payload)


@pytest.mark.parametrize(
    ("scope", "expected_descendants", "dying_descendants"),
    (
        ("batch", 4, 0),
        ("root", 3, 0),
        ("trusted_service", 1, 0),
        ("build_egress", 1, 0),
        ("build_egress", 0, 1),
    ),
)
def test_runtime_reconciliation_rejects_descendant_drift(
    tmp_path: Path,
    scope: str,
    expected_descendants: int,
    dying_descendants: int,
) -> None:
    probe, entry, _device = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    probe._verify_runtime_controls(entry.payload)
    proof = entry.payload["proof"]
    assert isinstance(proof, dict)
    attachment = proof["attachment"]
    assert isinstance(attachment, dict)
    root = Path(str(attachment["containment_root"]))
    paths = {
        "batch": root.parent,
        "root": root,
        "trusted_service": Path(str(attachment["trusted_service_cgroup"])),
        "build_egress": Path(str(attachment["build_egress_cgroup"])),
    }
    (paths[scope] / "cgroup.stat").write_text(
        f"nr_descendants {expected_descendants}\n"
        f"nr_dying_descendants {dying_descendants}\n",
        encoding="ascii",
    )

    with pytest.raises(GuardError) as caught:
        probe._verify_runtime_controls(entry.payload)

    assert caught.value.code == "reconciliation_resource_identity_invalid"


@pytest.mark.parametrize("mutation", ("domain-controller", "writable-by-group"))
def test_runtime_reconciliation_rejects_an_unlaunchable_build_egress(
    tmp_path: Path,
    mutation: str,
) -> None:
    probe, entry, _device = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    probe._verify_runtime_controls(entry.payload)
    proof = entry.payload["proof"]
    assert isinstance(proof, dict)
    attachment = proof["attachment"]
    assert isinstance(attachment, dict)
    build = Path(str(attachment["build_egress_cgroup"]))
    if mutation == "domain-controller":
        (build / "cgroup.subtree_control").write_text("io pids\n", encoding="ascii")
    else:
        (build / "cgroup.procs").chmod(0o664)

    with pytest.raises(GuardError) as caught:
        probe._verify_runtime_controls(entry.payload)

    assert caught.value.code == "reconciliation_resource_identity_invalid"


def test_runtime_reconciliation_rejects_device_authority_drift(tmp_path: Path) -> None:
    probe, entry, device = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    probe._verify_runtime_controls(entry.payload)
    device.program_id = 78

    with pytest.raises(GuardError) as caught:
        probe._verify_runtime_controls(entry.payload)

    assert caught.value.code == "reconciliation_resource_identity_invalid"


def test_runtime_reconciliation_rejects_device_program_tag_drift(tmp_path: Path) -> None:
    probe, entry, device = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    probe._verify_runtime_controls(entry.payload)
    device.tag = "fedcba9876543210"

    with pytest.raises(GuardError) as caught:
        probe._verify_runtime_controls(entry.payload)

    assert caught.value.code == "reconciliation_resource_identity_invalid"


def test_pin_reconciliation_rejects_proof_attachment_binding_drift(
    tmp_path: Path,
) -> None:
    probe, entry, _device_probe = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    probe._verify_runtime_controls(entry.payload)
    proof = entry.payload["proof"]
    assert isinstance(proof, dict)
    attachment = proof["attachment"]
    assert isinstance(attachment, dict)
    link_ids = attachment["link_ids"]
    assert isinstance(link_ids, list)
    link_ids[0] += 1

    with pytest.raises(GuardError) as caught:
        probe._verify_pin_tree(entry)  # type: ignore[arg-type]

    assert caught.value.code == "reconciliation_pin_identity_invalid"


def test_pin_reconciliation_rejects_broadened_static_map_contents(
    tmp_path: Path,
) -> None:
    probe, entry, _device_probe = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    kernel = probe.kernel
    assert isinstance(kernel, _PinnedKernel)
    kernel.map_contents[("build-egress", "allow_v4")][b"\xff" * 12] = b"\x01"

    with pytest.raises(GuardError) as caught:
        probe._verify_pin_tree(entry)  # type: ignore[arg-type]

    assert caught.value.code == "reconciliation_pin_identity_invalid"


def test_pin_reconciliation_accepts_mutable_subject_limit_runtime_state(
    tmp_path: Path,
) -> None:
    probe, entry, _device_probe = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    kernel = probe.kernel
    assert isinstance(kernel, _PinnedKernel)
    limits = kernel.map_contents[("build-egress", "subject_limits")]
    key, original = next(iter(limits.items()))
    limits[key] = bytes(range(64)) + original[64:]

    probe._verify_pin_tree(entry)  # type: ignore[arg-type]


def test_pin_reconciliation_rejects_an_extra_subject_limit_key(tmp_path: Path) -> None:
    probe, entry, _device_probe = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    kernel = probe.kernel
    assert isinstance(kernel, _PinnedKernel)
    limits = kernel.map_contents[("build-egress", "subject_limits")]
    _key, original = next(iter(limits.items()))
    limits[b"\x02\x00\x00\x00"] = original

    with pytest.raises(GuardError) as caught:
        probe._verify_pin_tree(entry)  # type: ignore[arg-type]

    assert caught.value.code == "reconciliation_pin_identity_invalid"


def test_pin_reconciliation_rejects_a_short_subject_limit_value(tmp_path: Path) -> None:
    probe, entry, _device_probe = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    kernel = probe.kernel
    assert isinstance(kernel, _PinnedKernel)
    limits = kernel.map_contents[("build-egress", "subject_limits")]
    key, original = next(iter(limits.items()))
    limits[key] = original[:-1]

    with pytest.raises(GuardError) as caught:
        probe._verify_pin_tree(entry)  # type: ignore[arg-type]

    assert caught.value.code == "reconciliation_pin_identity_invalid"


def test_pin_reconciliation_rejects_subject_limit_ceiling_drift(tmp_path: Path) -> None:
    probe, entry, _device_probe = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    kernel = probe.kernel
    assert isinstance(kernel, _PinnedKernel)
    limits = kernel.map_contents[("build-egress", "subject_limits")]
    key, original = next(iter(limits.items()))
    limits[key] = original[:64] + bytes((original[64] ^ 1,)) + original[65:]

    with pytest.raises(GuardError) as caught:
        probe._verify_pin_tree(entry)  # type: ignore[arg-type]

    assert caught.value.code == "reconciliation_pin_identity_invalid"


def test_terminal_reconciliation_detects_processes_in_descendant_cgroups(
    tmp_path: Path,
) -> None:
    probe, entry, _device_probe = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    request = entry.payload["projection_request"]
    assert isinstance(request, dict)
    batch = Path(str(request["cgroup_path"]))
    root = batch / "loom-builder"
    trusted = root / "trusted-service"
    build = root / "build-egress"
    for directory in (batch, root, trusted, build):
        (directory / "cgroup.procs").write_text("", encoding="ascii")
    descendant = build / "buildkit-worker"
    descendant.mkdir()
    (descendant / "cgroup.procs").write_text("4242\n", encoding="ascii")

    with pytest.raises(GuardError) as caught:
        probe._terminal_empty(entry)  # type: ignore[arg-type]

    assert caught.value.code == "reconciliation_allocation_not_empty"


def test_terminal_cleanup_preserves_pins_until_cgroup_removal_succeeds(
    tmp_path: Path,
) -> None:
    probe, entry, _device_probe = _pinned_probe_fixture(tmp_path, corrupt_link=False)
    request = entry.payload["projection_request"]
    assert isinstance(request, dict)
    batch = Path(str(request["cgroup_path"]))
    root = batch / "loom-builder"
    trusted = root / "trusted-service"
    build = root / "build-egress"
    for directory in (batch, root, trusted, build):
        (directory / "cgroup.procs").write_text("", encoding="ascii")
    descendant = build / "buildkit-worker"
    descendant.mkdir()
    (descendant / "cgroup.procs").write_text("", encoding="ascii")
    pin_path = Path(str(entry.payload["pin_path"]))

    with pytest.raises(GuardError) as caught:
        probe.cleanup_terminal(entry)  # type: ignore[arg-type]

    assert caught.value.code == "reconciliation_terminal_identity_invalid"
    assert pin_path.exists()


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


def test_service_stops_startup_extensions_when_the_main_thread_stalls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notifications: list[str] = []
    entered = Event()
    release = Event()
    failures: list[GuardError] = []

    def startup() -> None:
        notifications.append("startup")

    service, ledger, _peer, _slurm, events = _service(
        tmp_path,
        monotonic=time.monotonic,
        startup=startup,
        ready=lambda: notifications.append("ready"),
        watchdog=lambda: notifications.append("watchdog"),
        keepalive_interval_seconds=0.01,
        progress_timeout_seconds=0.03,
        startup_extension_limit_seconds=0.2,
    )
    original_reconcile = service.reconciler.reconcile

    def slow_reconcile(value: GuardLedger) -> None:
        entered.set()
        assert release.wait(timeout=1)
        original_reconcile(value)

    monkeypatch.setattr(service.reconciler, "reconcile", slow_reconcile)

    def run() -> None:
        try:
            service.start()
        except GuardError as exc:
            failures.append(exc)

    thread = Thread(target=run)
    thread.start()
    assert entered.wait(timeout=1)
    time.sleep(0.08)
    stopped_count = notifications.count("startup")
    time.sleep(0.04)
    assert notifications.count("startup") == stopped_count
    release.set()
    thread.join(timeout=1)

    assert events[0] == "reconcile"
    assert 1 <= stopped_count < 8
    assert "ready" not in notifications
    assert "watchdog" not in notifications
    assert [error.code for error in failures] == ["service_main_loop_stalled"]
    assert not service.config.protocol.socket_path.exists()
    ledger.close()


def test_service_stops_watchdog_when_the_ready_main_thread_stalls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notifications: list[str] = []
    entered = Event()
    release = Event()
    failures: list[GuardError] = []
    calls = 0

    service, ledger, _peer, _slurm, _events = _service(
        tmp_path,
        monotonic=time.monotonic,
        startup=lambda: notifications.append("startup"),
        ready=lambda: notifications.append("ready"),
        watchdog=lambda: notifications.append("watchdog"),
        keepalive_interval_seconds=0.01,
        progress_timeout_seconds=0.03,
        startup_extension_limit_seconds=0.2,
    )

    def blocking_attestation() -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            assert release.wait(timeout=1)

    monkeypatch.setattr(service, "run_attestations_once", blocking_attestation)

    def run() -> None:
        try:
            service.start()
        except GuardError as exc:
            failures.append(exc)

    thread = Thread(target=run)
    thread.start()
    assert entered.wait(timeout=1)
    time.sleep(0.08)
    stopped_count = notifications.count("watchdog")
    time.sleep(0.04)
    assert notifications.count("watchdog") == stopped_count
    release.set()
    thread.join(timeout=1)

    assert "ready" in notifications
    assert 1 <= stopped_count < 8
    assert [error.code for error in failures] == ["service_main_loop_stalled"]
    assert not service.config.protocol.socket_path.exists()
    ledger.close()


def test_startup_extension_is_capped_even_while_main_progress_continues() -> None:
    notifications: list[str] = []
    keepalive = service_module._ServiceKeepalive(
        startup=lambda: notifications.append("startup"),
        watchdog=lambda: notifications.append("watchdog"),
        interval_seconds=0.01,
        monotonic=time.monotonic,
        progress_timeout_seconds=0.05,
        startup_extension_limit_seconds=0.08,
    )
    try:
        keepalive.start()
        deadline = time.monotonic() + 0.14
        while time.monotonic() < deadline:
            keepalive.progress()
            time.sleep(0.005)

        stopped_count = notifications.count("startup")
        time.sleep(0.03)

        with pytest.raises(GuardError) as caught:
            keepalive.check()
        assert caught.value.code == "service_startup_deadline_exceeded"
        assert notifications.count("startup") == stopped_count
        assert "watchdog" not in notifications
    finally:
        keepalive.close()


def test_watchdog_continues_during_slow_multi_step_main_thread_progress() -> None:
    notifications: list[str] = []
    keepalive = service_module._ServiceKeepalive(
        startup=lambda: notifications.append("startup"),
        watchdog=lambda: notifications.append("watchdog"),
        interval_seconds=0.01,
        monotonic=time.monotonic,
        progress_timeout_seconds=0.04,
        startup_extension_limit_seconds=0.3,
    )
    try:
        keepalive.start()
        keepalive.mark_ready()
        for _entry in range(6):
            time.sleep(0.02)
            keepalive.progress()
            keepalive.check()

        assert notifications.count("watchdog") >= 6
        keepalive.check()
    finally:
        keepalive.close()


def test_service_propagates_keepalive_failure_after_blocking_startup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed = Event()
    calls = 0

    def startup() -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            failed.set()
            raise GuardError("service_notify_failed")

    service, ledger, _peer, _slurm, _events = _service(
        tmp_path,
        startup=startup,
        keepalive_interval_seconds=0.01,
    )
    original_reconcile = service.reconciler.reconcile

    def slow_reconcile(value: GuardLedger) -> None:
        assert failed.wait(timeout=1)
        original_reconcile(value)

    monkeypatch.setattr(service.reconciler, "reconcile", slow_reconcile)

    with pytest.raises(GuardError) as caught:
        service.start()

    assert caught.value.code == "service_notify_failed"
    assert not service.config.protocol.socket_path.exists()
    ledger.close()


def test_service_normalizes_initial_keepalive_failure_and_closes_feeder(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed = Event()
    close = service_module._ServiceKeepalive.close

    def record_close(keepalive: service_module._ServiceKeepalive) -> None:
        closed.set()
        close(keepalive)

    def fail_startup() -> None:
        raise RuntimeError("injected notifier failure")

    monkeypatch.setattr(service_module._ServiceKeepalive, "close", record_close)
    service, ledger, _peer, _slurm, _events = _service(
        tmp_path,
        startup=fail_startup,
    )

    with pytest.raises(GuardError) as caught:
        service.start()

    assert caught.value.code == "service_keepalive_failed"
    assert closed.is_set()
    assert not service.config.protocol.socket_path.exists()
    ledger.close()


def test_systemd_notifier_uses_only_the_validated_notify_socket(tmp_path: Path) -> None:
    path = tmp_path / "notify.sock"
    receiver = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    receiver.bind(str(path))
    receiver.settimeout(2)
    notifier = SystemdNotifier.from_environment({"NOTIFY_SOCKET": str(path)})
    try:
        notifier.extend_startup()
        notifier.ready()
        notifier.watchdog()
        assert notifier._socket is not None
        assert notifier._socket.gettimeout() == 1.0
        assert receiver.recv(64) == b"EXTEND_TIMEOUT_USEC=60000000"
        assert receiver.recv(64) == b"READY=1"
        assert receiver.recv(64) == b"WATCHDOG=1"
    finally:
        notifier.close()
        receiver.close()

    with pytest.raises(GuardError) as caught:
        SystemdNotifier.from_environment({"NOTIFY_SOCKET": "relative.sock"})
    assert caught.value.code == "service_notify_socket_invalid"
