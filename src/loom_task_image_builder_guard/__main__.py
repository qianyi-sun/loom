"""Closed command-line entry point for the node guard zipapp."""

from __future__ import annotations

import os
import socket
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import cast
from uuid import UUID

from loom_task_image_builder_guard.authority import AuthorityClient
from loom_task_image_builder_guard.bpf import BpfLoader, BpfSyscall, NetworkPolicy
from loom_task_image_builder_guard.config import GuardConfig
from loom_task_image_builder_guard.containment import (
    BpftoolDeviceProbe,
    CgroupFilesystem,
    ContainmentManager,
    GuardPolicy,
)
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.identity import (
    PeerHandle,
    PeerInspector,
    derive_batch_cgroup,
)
from loom_task_image_builder_guard.ledger import GuardLedger
from loom_task_image_builder_guard.service import (
    Batch,
    Containment,
    GuardService,
    NodeReconciler,
    PeerSource,
    ReconciliationProbe,
    SystemReconciliationProbe,
)
from loom_task_image_builder_guard.slurm import PinnedCommandRunner, SlurmInspector

_UNSAFE_ENVIRONMENT = frozenset(
    {
        "ALL_PROXY",
        "CURL_CA_BUNDLE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "LD_LIBRARY_PATH",
        "LD_PRELOAD",
        "NO_PROXY",
        "PYTHONHOME",
        "PYTHONINSPECT",
        "PYTHONPATH",
        "PYTHONSTARTUP",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_DIR",
        "SSL_CERT_FILE",
        "all_proxy",
        "http_proxy",
        "https_proxy",
        "no_proxy",
    }
)


class SystemdNotifier:
    """Send only fixed readiness/watchdog messages to systemd's notify socket."""

    def __init__(self, address: str | bytes | None) -> None:
        self._address = address
        self._socket: socket.socket | None = None

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> SystemdNotifier:
        value = environment.get("NOTIFY_SOCKET")
        if value is None:
            return cls(None)
        if (
            not isinstance(value, str)
            or not value
            or "\x00" in value
            or value[0] not in {"/", "@"}
        ):
            raise GuardError("service_notify_socket_invalid")
        encoded = os.fsencode(value)
        address: str | bytes = b"\0" + encoded[1:] if value.startswith("@") else value
        if len(os.fsencode(address)) > 107:
            raise GuardError("service_notify_socket_invalid")
        return cls(address)

    def _send(self, payload: bytes) -> None:
        if self._address is None:
            return
        try:
            if self._socket is None:
                self._socket = socket.socket(
                    socket.AF_UNIX,
                    socket.SOCK_DGRAM | socket.SOCK_CLOEXEC,
                )
                self._socket.connect(self._address)
            if self._socket.send(payload) != len(payload):
                raise OSError("short systemd notification")
        except OSError as exc:
            self.close()
            raise GuardError("service_notify_failed") from exc

    def ready(self) -> None:
        self._send(b"READY=1")

    def watchdog(self) -> None:
        self._send(b"WATCHDOG=1")

    def close(self) -> None:
        if self._socket is not None:
            self._socket.close()
            self._socket = None


def _error(code: str) -> None:
    sys.stderr.write(f"loom_task_image_builder_guard error={code}\n")


def _boot_id() -> UUID:
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii")
        result = UUID(value.strip())
    except (OSError, UnicodeError, ValueError):
        raise GuardError("service_boot_identity_invalid") from None
    if result.int == 0:
        raise GuardError("service_boot_identity_invalid")
    return result


def build_service(
    config: GuardConfig,
    *,
    ready: Callable[[], None] = lambda: None,
    watchdog: Callable[[], None] = lambda: None,
) -> GuardService:
    """Assemble only digest-pinned, locally configured guard dependencies."""

    runner = PinnedCommandRunner()
    peers = PeerInspector(config.identity)
    slurm = SlurmInspector(
        cluster_id=config.cluster_id,
        node_name=config.node_name,
        identity=config.identity,
        policy=config.slurm,
        commands=config.commands,
        runner=runner,
    )
    kernel = BpfSyscall()
    network = NetworkPolicy.from_file(
        config.containment.network_policy_path,
        uid=0,
        gid=0,
        containment_policy_sha256=config.containment.containment_policy_sha256,
        resource_profile_sha256=config.containment.resource_profile_sha256,
        bpf_program_sha256=config.containment.bpf_program_sha256,
        bpf_map_schema_sha256=config.containment.bpf_map_schema_sha256,
    )
    loader = BpfLoader(
        kernel=kernel,
        runner=runner,
        bpftool=config.commands.bpftool,
        bpf_object_path=config.containment.bpf_object_path,
        bpffs_root=config.containment.bpffs_root,
        containment_policy_sha256=config.containment.containment_policy_sha256,
        resource_profile_sha256=config.containment.resource_profile_sha256,
        bpf_map_schema_sha256=config.containment.bpf_map_schema_sha256,
    )
    containment = ContainmentManager(
        filesystem=CgroupFilesystem(),
        bpf_loader=loader,
        device_probe=BpftoolDeviceProbe(runner, config.commands.bpftool),
    )
    policy = GuardPolicy(
        cpus=config.slurm.cpus,
        memory_mib=config.slurm.memory_mib,
        pids_max=config.containment.pids_max,
        io_limits=config.containment.io_limits,
        network=network,
    )
    ledger = GuardLedger(
        config.containment.ledger_root,
        config.service.max_ledger_entries,
    )
    probe = SystemReconciliationProbe(
        config,
        peers=peers,
        slurm=slurm,
        kernel=kernel,
    )
    reconciler = NodeReconciler(
        config.containment.bpffs_root,
        probe=cast(ReconciliationProbe, probe),
        slurm=slurm,
    )
    authority = AuthorityClient(config.authority)
    return GuardService(
        config,
        ledger=ledger,
        peers=cast(PeerSource, peers),
        slurm=slurm,
        derive_batch=lambda peer, job_id: cast(
            Batch,
            derive_batch_cgroup(
                cast(PeerHandle, peer),
                job_id=job_id,
                cgroup_root=config.containment.cgroup_root,
            ),
        ),
        containment=cast(Containment, containment),
        policy=policy,
        authority=authority,
        reconciler=reconciler,
        node_boot_id=_boot_id(),
        ready=ready,
        watchdog=watchdog,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    environment = os.environ if environ is None else environ
    if _UNSAFE_ENVIRONMENT.intersection(environment):
        _error("unsafe_environment")
        return 1
    if arguments == ("--self-check",):
        sys.stdout.write(
            '{"schema":"loom.task-image-builder-node-guard-self-check/v1",'
            '"status":"ok"}\n'
        )
        return 0
    if len(arguments) != 2 or arguments[0] != "--config":
        _error("cli_arguments_invalid")
        return 2
    config_path = Path(arguments[1])
    if not config_path.is_absolute():
        _error("cli_arguments_invalid")
        return 2
    service: GuardService | None = None
    notifier: SystemdNotifier | None = None
    try:
        config = GuardConfig.from_file(config_path)
        notifier = SystemdNotifier.from_environment(environment)
        service = build_service(
            config,
            ready=notifier.ready,
            watchdog=notifier.watchdog,
        )
        service.start()
        return 0
    except GuardError as exc:
        _error(exc.code)
        return 1
    except Exception:
        _error("guard_failed")
        return 1
    finally:
        if service is not None:
            service.close()
            service.ledger.close()
        if notifier is not None:
            notifier.close()


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["SystemdNotifier", "build_service", "main"]
