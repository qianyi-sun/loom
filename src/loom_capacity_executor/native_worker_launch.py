"""One-use trusted handoff to fixed native worker stdin, with owned cleanup."""

from __future__ import annotations

import asyncio
import os
import signal
import stat
import subprocess
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path

from loom_capacity_agent.admission import PhysicalJobBindingV2
from loom_capacity_executor.bootstrap_handoff import (
    claim_bootstrap_handoff_launch,
    consume_bootstrap_handoff,
)
from loom_capacity_executor.native_worker_bootstrap import (
    NativeWorkerBootstrap,
    _disable_bootstrap_dumps,
    encode_native_bootstrap,
    native_bootstrap_pipe,
)
from loom_capacity_executor.native_worker_container import (
    _CONTAINER_ID,
    FixedDockerCLI,
    NativeContainerError,
    NativeWorkerAllocation,
    NativeWorkerContainerPolicyV2,
    PreparedNativeImage,
    bind_native_settings,
    native_create_argv,
    prefetch_native_image,
    remove_native_container,
)


async def run_attached_native_worker(cli: FixedDockerCLI, container_id: str, bootstrap: NativeWorkerBootstrap) -> int:
    """Stream worker output directly, never into an unbounded supervisor buffer.

    The private pipe already contains the complete bounded frame and EOF.
    Cancellation reaps the CLI, not the container. The caller MUST perform
    exact-ID container cleanup in its own finally path.
    """

    descriptor = native_bootstrap_pipe(bootstrap)
    process: subprocess.Popen[bytes] | None = None
    try:
        if cli.stop_requested is not None and cli.stop_requested():
            raise NativeContainerError("native worker startup interrupted")
        process = subprocess.Popen(cli.argv("container", "start", "--attach", "--interactive", container_id),
            executable=cli.executable, env={}, pass_fds=(cli.descriptor,), stdin=descriptor)
        os.close(descriptor)
        descriptor = -1
        while process.poll() is None:
            if cli.stop_requested is not None and cli.stop_requested():
                raise NativeContainerError("native worker attachment interrupted; cleanup required")
            await asyncio.sleep(0.1)
        return process.returncode
    except (OSError, subprocess.SubprocessError):
        raise NativeContainerError("native worker attachment failed; container outcome uncertain") from None
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()


@contextmanager
def native_termination_boundary() -> Iterator[Callable[[], bool]]:
    """Dedicated-process signal latch visible even during synchronous CLI reads.

    Repeated termination requests never raise into cleanup. Control operations
    and attachment poll the latch; exact-ID cleanup intentionally ignores it.
    SIGKILL/host loss still require administrator-owned allocation cleanup.
    """

    stopping = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    previous = {number: signal.getsignal(number) for number in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)}
    try:
        for number in previous:
            signal.signal(number, request_stop)
        yield lambda: stopping
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


async def launch_native_worker_once(
    *, directory: Path, reference: str, physical: PhysicalJobBindingV2, admission: object,
    policy: NativeWorkerContainerPolicyV2, cli: FixedDockerCLI, image: PreparedNativeImage,
    allocation: NativeWorkerAllocation, now: Callable[[], datetime],
    scratch: NativeWorkerScratch | None = None,
) -> None:
    """Consume one launch marker, start one worker, and positively remove it.

    Immutable image prefetch and live allocation readback are prerequisites.
    No retry exists after consumption, including a lost create response. That
    ambiguous outcome requires the administrator's allocation cleanup/inventory;
    absence at one instant cannot prove that delayed creation will not finish.
    """

    root = policy.native_execution.trust_root()
    binding = physical.binding
    if (
        binding.resources.gpu_count != 0 or binding.resources.generic
        or binding.node_ids != (allocation.hostname,)
        or binding.pool_id != allocation.pool_id
        or binding.candidate.identity != allocation.candidate_sha
        or str(binding.intent_id) != allocation.intent_id or physical.slurm_job_id != allocation.job_id
        or binding.resources.cpu_millicores != allocation.cpu_millicores
        or binding.resources.memory_bytes != allocation.memory_bytes
        or binding.concurrency_slots != allocation.concurrency_slots or policy.pids_max != allocation.pids_max
        or image.platform != policy.native_execution.platform
    ):
        raise NativeContainerError("native worker allocation differs from protected physical binding")
    if not root.activated_at <= now() < root.expires_at:
        raise NativeContainerError("native execution root is not valid at launch")
    provisional = NativeWorkerBootstrap(native_execution=policy.native_execution,
        worker_credential="x" * 512, canonical_worker_settings=policy.canonical_worker_settings)
    # Reject incompatible settings and oversize final frames before exchanging
    # any protected capability. The scoped credential is added only afterward.
    encode_native_bootstrap(bind_native_settings(provisional, allocation))
    if scratch is not None and (str(scratch.directory) != allocation.scratch_directory or scratch.creation_possible):
        raise NativeContainerError("native scratch does not bind this launch")
    create_argv = native_create_argv(image, allocation, name="loom-native-" + allocation.intent_id,
        ownership=physical.ownership_evidence_sha256)
    if cli.stop_requested is not None and cli.stop_requested():
        raise NativeContainerError("native worker launch interrupted before handoff")
    _disable_bootstrap_dumps()
    await consume_bootstrap_handoff(directory, reference, physical, admission, now=now)
    if cli.stop_requested is not None and cli.stop_requested():
        raise NativeContainerError("native worker launch interrupted before consumption")
    if not root.activated_at <= now() < root.expires_at:
        raise NativeContainerError("native execution root expired during handoff")
    credential = claim_bootstrap_handoff_launch(directory, reference, physical, admission, now=now)
    bootstrap = bind_native_settings(NativeWorkerBootstrap(native_execution=policy.native_execution,
        worker_credential=credential, canonical_worker_settings=policy.canonical_worker_settings), allocation)
    if scratch is not None:
        scratch.creation_possible = True
    raw_id = cli.call(*create_argv)
    try:
        container_id = raw_id.decode("ascii").strip()
    except UnicodeError:
        raise NativeContainerError("native worker create identity is uncertain") from None
    if _CONTAINER_ID.fullmatch(container_id) is None:
        raise NativeContainerError("native worker create identity is uncertain")
    try:
        created = cli.json("container", "inspect", container_id)
        if (not isinstance(created, list) or len(created) != 1
            or created[0].get("Id") != container_id or created[0].get("Image") != image.image_id):
            raise NativeContainerError("native worker actual image readback changed")
        if not root.activated_at <= now() < root.expires_at:
            raise NativeContainerError("native execution root expired before worker startup")
        status = await run_attached_native_worker(cli, container_id, bootstrap)
        if status != 0:
            raise NativeContainerError("native worker exited unsuccessfully")
    finally:
        remove_native_container(cli, container_id)


def _verify_native_docker_config(directory: Path) -> None:
    """Require an empty, root-owned, non-writable-by-worker Docker config tree."""

    try:
        for path in (directory, *directory.parents):
            info = path.lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                raise NativeContainerError("native Docker config is not operator-owned")
        if any(directory.iterdir()):
            raise NativeContainerError("native Docker config must be empty")
    except OSError:
        raise NativeContainerError("native Docker config is unavailable") from None


@dataclass(slots=True)
class NativeWorkerScratch:
    """Exact per-invocation workspace; possible runtimes retain their files."""

    directory: Path
    identity: tuple[int, int]
    children: dict[str, tuple[int, int]] = field(default_factory=dict)
    creation_possible: bool = False

    def discard_if_unused(self) -> None:
        if self.creation_possible:
            return
        try:
            self._assert_identity(self.directory, self.identity)
            # Validate the complete owned set before deleting any member.
            for name, identity in self.children.items():
                self._assert_identity(self.directory / name, identity)
            for name in self.children:
                (self.directory / name).rmdir()
            self._assert_identity(self.directory, self.identity)
            self.directory.rmdir()
        except OSError:
            raise NativeContainerError("unused native scratch cleanup is unconfirmed") from None

    @staticmethod
    def _assert_identity(path: Path, identity: tuple[int, int]) -> None:
        info = path.lstat()
        if (not stat.S_ISDIR(info.st_mode) or (info.st_dev, info.st_ino) != identity
            or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700):
            raise NativeContainerError("native scratch identity changed before cleanup")


def create_native_scratch(root: Path, intent_id: str) -> NativeWorkerScratch:
    """Create fresh scratch beneath an already verified/provisioned parent."""

    from uuid import UUID

    if str(UUID(intent_id)) != intent_id:
        raise NativeContainerError("native scratch intent identity is invalid")
    destination = Path(tempfile.mkdtemp(prefix=f"{intent_id}-", dir=root))
    info = destination.lstat()
    scratch = NativeWorkerScratch(destination, (info.st_dev, info.st_ino))
    try:
        for name in ("tmp", "trajectories", "benchmarks"):
            child = destination / name
            child.mkdir(mode=0o700)
            info = child.lstat()
            scratch.children[name] = (info.st_dev, info.st_ino)
    except OSError:
        scratch.discard_if_unused()
        raise NativeContainerError("native scratch initialization failed") from None
    return scratch


def _native_scratch_directory(intent_id: str) -> NativeWorkerScratch:
    """Create one fresh private runtime tree; never reuse a previous worker's files."""

    root = Path("/var/lib/loom/native-workers")
    try:
        for path in (root, *root.parents):
            info = path.lstat()
            if (not stat.S_ISDIR(info.st_mode) or info.st_uid not in {0, os.geteuid()}
                or info.st_mode & 0o022):
                raise NativeContainerError("native scratch parent is not protected")
        return create_native_scratch(root, intent_id)
    except OSError:
        raise NativeContainerError("native scratch tree is unavailable") from None


async def run_native_worker_on_host(
    *, directory: Path, reference: str, physical: PhysicalJobBindingV2, admission: object,
    policy: NativeWorkerContainerPolicyV2, cli: FixedDockerCLI, image_digest: str,
    now: Callable[[], datetime],
) -> None:
    """Resolve immutable image and live Slurm containment before one-use launch."""

    with native_termination_boundary() as stopping:
        await _run_native_worker_on_host(directory=directory, reference=reference, physical=physical,
            admission=admission, policy=policy, cli=replace(cli, stop_requested=stopping),
            image_digest=image_digest, now=now)


async def _run_native_worker_on_host(
    *, directory: Path, reference: str, physical: PhysicalJobBindingV2, admission: object,
    policy: NativeWorkerContainerPolicyV2, cli: FixedDockerCLI, image_digest: str,
    now: Callable[[], datetime],
) -> None:

    from loom_control_plane.slurm_job_cgroup import discover_docker_cgroup_parent

    if os.geteuid() == 0 or os.getegid() == 0:
        raise NativeContainerError("native worker launcher requires an unprivileged runtime identity")
    _verify_native_docker_config(Path(cli.config_directory))
    binding = physical.binding
    if binding.resources.gpu_count or binding.resources.generic or len(binding.node_ids) != 1:
        raise NativeContainerError("native worker allocation requires the CPU-only trial runtime")
    try:
        socket_info = Path("/var/run/docker.sock").lstat()
        if not stat.S_ISSOCK(socket_info.st_mode) or socket_info.st_uid != 0 or socket_info.st_mode & 0o002:
            raise NativeContainerError("native worker Docker endpoint is not protected")
    except OSError:
        raise NativeContainerError("native worker Docker endpoint is unavailable") from None
    image = prefetch_native_image(cli, image_digest=image_digest, platform=policy.native_execution.platform)
    parent = discover_docker_cgroup_parent(docker_driver=native_daemon_cgroup_driver(cli),
        job_id=physical.slurm_job_id, pids_max=policy.pids_max, wait_seconds=60)
    scratch = _native_scratch_directory(str(binding.intent_id))
    try:
        allocation = NativeWorkerAllocation(intent_id=str(binding.intent_id), job_id=physical.slurm_job_id,
            cgroup_parent=parent, cpu_millicores=binding.resources.cpu_millicores,
            memory_bytes=binding.resources.memory_bytes, pids_max=policy.pids_max,
            concurrency_slots=binding.concurrency_slots, scratch_directory=str(scratch.directory),
            docker_socket_gid=socket_info.st_gid, runtime_uid=os.geteuid(), runtime_gid=os.getegid(),
            pool_id=binding.pool_id, hostname=binding.node_ids[0], candidate_sha=binding.candidate.identity)
        await launch_native_worker_once(directory=directory, reference=reference, physical=physical,
            admission=admission, policy=policy, cli=cli, image=image, allocation=allocation, now=now,
            scratch=scratch)
    finally:
        scratch.discard_if_unused()


def native_daemon_cgroup_driver(cli: FixedDockerCLI) -> str:
    """Ask Docker explicitly for machine-readable daemon containment facts."""

    daemon = cli.json("info", "--format={{json .}}")
    if (not isinstance(daemon, dict) or daemon.get("CgroupVersion") != "2"
        or daemon.get("CgroupDriver") not in {"systemd", "cgroupfs"}):
        raise NativeContainerError("native worker Docker cgroup v2 is unavailable")
    driver: str = daemon["CgroupDriver"]
    return driver
