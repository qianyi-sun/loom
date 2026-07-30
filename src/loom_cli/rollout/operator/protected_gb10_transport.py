"""Fixed SSH transport for exact GB10 candidate observation and convergence.

The transport deliberately accepts neither arbitrary remote commands nor paths.
Its inventory is constructed from the installed staging configuration, while
the candidate identity comes only from the immutable final-gate plan.
"""

from __future__ import annotations

import hashlib
import json
import re
import shlex
import time
import tomllib
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol

from loom_cli.rollout.gb10_convergence import (
    GB10ConvergencePlan,
    GB10FleetCandidateObservation,
    GB10HostCandidateObservation,
    GB10MutationKind,
)

from .final_gate_plan import FinalGatePlan

_HOST_RE = re.compile(r"trt-gb10-(?:[1-9]|1[0-5])\Z")
_SERVICE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.@-]*[.]service\Z")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STAGING_IMAGE_TAG_RE = re.compile(r"^staging-[0-9a-f]{7}$")
_KNOWN_HOSTS = Path("/etc/loom/staging-rollout-gb10-known-hosts")
_LEGACY_SHARED_ROOT = PurePosixPath("/shared_work2/qianyi/.loom-staging-rollout/worker-repos")
_LEGACY_UNIT_ROOT = PurePosixPath("deploy/worker-pools/gb10")
_LEGACY_SERVICE = "loom-gb10-worker.service"
_FIXED_REPO = PurePosixPath("/home/qianyi/loom-worker-build-staging")
_FIXED_ENV_FILE = _FIXED_REPO / ".env"
_STAGING_SHARED_ENV_ROOT = PurePosixPath("/srv/loom/staging-shared/generated")
_FIXED_NODE_AGENT_SERVICE = "loom-gb10-node-agent.service"
_RETIREMENT_UNITS = (
    _LEGACY_SERVICE,
    _FIXED_NODE_AGENT_SERVICE,
    "loom-gb10-node-agent.timer",
)
# Installed transports retry transient single-bastion SSH failures. Six attempts
# with a 2s pause tolerate the connection storms the fleet observe competes with
# (the 30s autoscaler cadence) while keeping the total per-host budget bounded.
_INSTALLED_OBSERVE_ATTEMPTS = 6
_INSTALLED_OBSERVE_INTERVAL_SECONDS = 2.0
_FIXED_IDENTITY = Path("/var/lib/loom-staging-rollout/gb10-deploy-ed25519")
_MAX_OUTPUT_BYTES = 64 * 1024
_RETIREMENT_MUTATION_ORDER = (
    GB10MutationKind.LEGACY_RETIRE,
    GB10MutationKind.SERVICE_TIMER,
)
_LEGACY_MUTATION_ORDER = (
    GB10MutationKind.CHECKOUT,
    GB10MutationKind.ENVIRONMENT,
    GB10MutationKind.UNITS,
    GB10MutationKind.LEGACY_RETIRE,
    GB10MutationKind.SERVICE_TIMER,
)


class GB10FleetApplyError(RuntimeError):
    """Structured, secret-free failure for the fixed GB10 host authority."""

    def __init__(self, failed_hosts: Sequence[str]) -> None:
        hosts = tuple(sorted(failed_hosts))
        if (
            not hosts
            or len(set(hosts)) != len(hosts)
            or any(_HOST_RE.fullmatch(host) is None for host in hosts)
        ):
            raise ValueError("GB10 failed-host metadata is invalid")
        self.failed_hosts = hosts
        super().__init__("GB10 convergence failed safely on fixed hosts")


class CommandResult(Protocol):
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str]], CommandResult]


@dataclass(frozen=True, slots=True)
class GB10TransportTarget:
    """One fixed release-managed host from the installed staging inventory."""

    ssh_target: str
    repo_path: PurePosixPath | None
    env_file_path: PurePosixPath | None
    node_agent_service: str
    retirement_only: bool = False

    def __post_init__(self) -> None:
        repo = None if self.repo_path is None else PurePosixPath(self.repo_path)
        env_file = None if self.env_file_path is None else PurePosixPath(self.env_file_path)
        if (
            _HOST_RE.fullmatch(self.ssh_target) is None
            or _SERVICE_RE.fullmatch(self.node_agent_service) is None
            or self.node_agent_service != _FIXED_NODE_AGENT_SERVICE
            or (self.retirement_only and (repo is not None or env_file is not None))
            or (
                not self.retirement_only
                and (
                    repo is None
                    or env_file is None
                    or not repo.is_absolute()
                    or not env_file.is_absolute()
                    or ".." in repo.parts
                    or ".." in env_file.parts
                    or env_file.parent != repo
                    or env_file.name != ".env"
                    or repo != _FIXED_REPO
                    or env_file != _FIXED_ENV_FILE
                )
            )
        ):
            raise ValueError("GB10 transport target is outside fixed authority")
        object.__setattr__(self, "repo_path", repo)
        object.__setattr__(self, "env_file_path", env_file)

    @property
    def timer(self) -> str:
        return f"{self.node_agent_service.removesuffix('.service')}.timer"


@dataclass(frozen=True, slots=True)
class FixedGB10SSHTransport:
    """Observe/apply only the typed convergence operations over fixed SSH."""

    targets: tuple[GB10TransportTarget, ...]
    ssh_config: Path
    identity: Path
    run: CommandRunner
    certificate: Path | None = None
    max_concurrency: int = 8
    # A transient failure against the single SSH bastion (all fleet hosts
    # ProxyJump through trt-gb10-1) is retried within this budget rather than
    # instantly reporting the host as drifted. Only unreachable/error results
    # are retried; a well-formed observation -- exact or drifted -- is
    # authoritative and returned immediately, so genuine drift still fails
    # closed. Defaults to a single attempt so directly-constructed transports
    # (unit tests) keep their exact-once semantics; the installed builder opts
    # into retries.
    settle_attempts: int = 1
    settle_interval_seconds: float = 0.0
    sleep: Callable[[float], None] = time.sleep

    def __post_init__(self) -> None:
        paths = (self.ssh_config, self.identity)
        if (
            not self.targets
            or len({target.ssh_target for target in self.targets}) != len(self.targets)
            or not 1 <= self.max_concurrency <= 16
            or not 1 <= self.settle_attempts <= 32
            or not 0 <= self.settle_interval_seconds <= 10
            or not callable(self.sleep)
            or any(not path.is_absolute() or ".." in path.parts for path in paths)
            or (
                self.certificate is not None
                and (not self.certificate.is_absolute() or ".." in self.certificate.parts)
            )
        ):
            raise ValueError("GB10 fixed transport authority is invalid")

    def observe(self, plan: FinalGatePlan) -> GB10FleetCandidateObservation:
        self._validate_plan_inventory(plan)
        outcomes: dict[str, GB10HostCandidateObservation] = {}
        with ThreadPoolExecutor(
            max_workers=min(self.max_concurrency, len(self.targets)),
            thread_name_prefix="loom-gb10-convergence-observe",
        ) as executor:
            futures = {
                executor.submit(self._observe_one, target, plan): target for target in self.targets
            }
            for future in as_completed(futures):
                target = futures[future]
                try:
                    outcomes[target.ssh_target] = future.result()
                except Exception:
                    outcomes[target.ssh_target] = self._failed_observation(target)
        return GB10FleetCandidateObservation(
            hosts=outcomes,
            candidate_source_digest=plan.gb10_unit_digest,
        )

    def apply(self, plan: FinalGatePlan, convergence: GB10ConvergencePlan) -> None:
        self._validate_plan_inventory(plan)
        targets = {target.ssh_target: target for target in self.targets}
        if (
            convergence.blockers
            or not convergence.mutations
            or set(mutation.host for mutation in convergence.mutations) - set(targets)
        ):
            raise ValueError("GB10 convergence plan exceeds fixed transport authority")
        failures: list[str] = []
        with ThreadPoolExecutor(
            max_workers=min(self.max_concurrency, len(convergence.mutations)),
            thread_name_prefix="loom-gb10-convergence-apply",
        ) as executor:
            futures = {
                executor.submit(
                    self._apply_one,
                    targets[mutation.host],
                    plan,
                    mutation.operations,
                ): mutation.host
                for mutation in convergence.mutations
            }
            for future in as_completed(futures):
                host = futures[future]
                try:
                    if not future.result():
                        failures.append(host)
                except Exception:
                    failures.append(host)
        if failures:
            raise GB10FleetApplyError(failures)

    def _validate_plan_inventory(self, plan: FinalGatePlan) -> None:
        if _SHA_RE.fullmatch(plan.candidate_sha) is None or set(plan.gb10_boot_ids) != {
            target.ssh_target for target in self.targets
        }:
            raise ValueError("GB10 final plan inventory drifted from fixed transport")

    def _observe_one(
        self,
        target: GB10TransportTarget,
        plan: FinalGatePlan,
    ) -> GB10HostCandidateObservation:
        # Retry transient transport failures (unreachable bastion, non-zero exit,
        # stderr noise, oversize/unparseable output) within the settle budget; a
        # host that never yields a well-formed observation still fails closed.
        # A well-formed observation whose only non-exact reason is a healthy
        # node-agent oneshot momentarily firing (``service_timer_transient``) is
        # also settled within the budget: the durable checkout/env/units are
        # converged and the timer will return to "waiting" once the oneshot
        # completes, so the fleet is not held drifted by ordinary reconcile
        # activity. A durably non-exact host (a real mutation is required) or an
        # exact host is returned immediately.
        last: GB10HostCandidateObservation | None = None
        for attempt in range(self.settle_attempts):
            outcome = self._observe_once(target, plan)
            if outcome is not None:
                observation, service_timer_transient = outcome
                last = observation
                if not self._settling_node_agent(observation, service_timer_transient):
                    return observation
            if attempt + 1 < self.settle_attempts:
                self.sleep(self.settle_interval_seconds)
        return last if last is not None else self._failed_observation(target)

    @staticmethod
    def _settling_node_agent(
        observation: GB10HostCandidateObservation,
        service_timer_transient: bool,
    ) -> bool:
        """Whether the host is non-exact ONLY because its node-agent is firing."""
        return (
            service_timer_transient
            and not observation.exact
            and not observation.service_timer_exact
            and observation.baseline_ready
            and observation.candidate_source_exact
            and observation.checkout_exact
            and observation.environment_exact
            and observation.units_exact
            and observation.legacy_absent
        )

    def _observe_once(
        self,
        target: GB10TransportTarget,
        plan: FinalGatePlan,
    ) -> tuple[GB10HostCandidateObservation, bool] | None:
        command = _remote_observation_command(target, plan)
        try:
            result = self.run(self._ssh_argv(target, command))
        except Exception:
            return None
        if (
            result.returncode != 0
            or result.stderr
            or len(result.stdout.encode()) > _MAX_OUTPUT_BYTES
        ):
            return None
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError:
            return None
        # ``service_timer_transient`` reports that the node-agent oneshot is
        # momentarily firing (timer SubState "running" rather than "waiting").
        # It is settle-only signalling and is deliberately excluded from the
        # exactness evidence digest so the durable convergence digest never folds
        # in that volatile runtime substate.
        expected = {
            "baseline_ready",
            "boot_id",
            "candidate_source_exact",
            "checkout_exact",
            "environment_exact",
            "legacy_absent",
            "service_timer_exact",
            "service_timer_transient",
            "units_exact",
        }
        if (
            not isinstance(payload, dict)
            or set(payload) != expected
            or not isinstance(payload["boot_id"], str)
            or any(type(payload[key]) is not bool for key in expected - {"boot_id"})
        ):
            return None
        evidence_digest = _hash_json(
            {key: payload[key] for key in payload if key != "service_timer_transient"}
        )
        observation = GB10HostCandidateObservation(
            host=target.ssh_target,
            boot_id=payload["boot_id"],
            baseline_ready=payload["baseline_ready"],
            candidate_source_exact=payload["candidate_source_exact"],
            checkout_exact=payload["checkout_exact"],
            environment_exact=payload["environment_exact"],
            units_exact=payload["units_exact"],
            legacy_absent=payload["legacy_absent"],
            service_timer_exact=payload["service_timer_exact"],
            evidence_digest=evidence_digest,
        )
        return observation, payload["service_timer_transient"]

    def _apply_one(
        self,
        target: GB10TransportTarget,
        plan: FinalGatePlan,
        operations: tuple[GB10MutationKind, ...],
    ) -> bool:
        mutation_order = (
            _RETIREMENT_MUTATION_ORDER if target.retirement_only else _LEGACY_MUTATION_ORDER
        )
        expected_order = tuple(operation for operation in mutation_order if operation in operations)
        if (
            not operations
            or len(set(operations)) != len(operations)
            or operations != expected_order
        ):
            raise ValueError("GB10 mutation operations are invalid")
        command = _remote_apply_command(target, plan, operations)
        # The typed convergence operations are idempotent, so a transient bastion
        # failure is retried within the settle budget before the host is reported
        # as failed.
        for attempt in range(self.settle_attempts):
            try:
                result = self.run(self._ssh_argv(target, command))
            except Exception:
                result = None
            if (
                result is not None
                and result.returncode == 0
                and result.stdout == ""
                and result.stderr == ""
            ):
                return True
            if attempt + 1 < self.settle_attempts:
                self.sleep(self.settle_interval_seconds)
        return False

    def _ssh_argv(
        self,
        target: GB10TransportTarget,
        command: str,
    ) -> tuple[str, ...]:
        argv: list[str] = [
            "ssh",
            "-F",
            str(self.ssh_config),
            "-i",
            str(self.identity),
            "-o",
            "IdentitiesOnly=yes",
        ]
        if self.certificate is not None:
            argv.extend(("-o", f"CertificateFile={self.certificate}"))
        argv.extend(
            (
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "StrictHostKeyChecking=yes",
                "-o",
                f"UserKnownHostsFile={_KNOWN_HOSTS}",
                "-o",
                "GlobalKnownHostsFile=/dev/null",
                "-o",
                "UpdateHostKeys=no",
                target.ssh_target,
                command,
            )
        )
        return tuple(argv)

    @staticmethod
    def _failed_observation(target: GB10TransportTarget) -> GB10HostCandidateObservation:
        return GB10HostCandidateObservation(
            host=target.ssh_target,
            boot_id="unavailable",
            baseline_ready=False,
            candidate_source_exact=False,
            checkout_exact=False,
            environment_exact=False,
            units_exact=False,
            legacy_absent=False,
            service_timer_exact=False,
            evidence_digest=hashlib.sha256(
                f"gb10-observation-unavailable:{target.ssh_target}".encode()
            ).hexdigest(),
        )


def _remote_observation_command(target: GB10TransportTarget, plan: FinalGatePlan) -> str:
    return "python3 -c " + shlex.quote(_remote_observation_source(target, plan))


def _retirement_identity(
    plan: FinalGatePlan,
    units: tuple[str, ...],
) -> str:
    return _hash_json(
        {
            "schema_version": 1,
            "candidate_sha": plan.candidate_sha,
            "candidate_tree": plan.candidate_tree,
            "units": list(units),
            "durable_absence": {
                "active_authorities": 0,
                "installed_unit_files": 0,
                "timer_dropins": 0,
            },
        }
    )


def retirement_worker_image_observation_source(plan: FinalGatePlan) -> str:
    """Render the fixed read-only staging GB10 worker image verifier."""

    image_tag = f"staging-{plan.candidate_sha[:7]}"
    env_file = _STAGING_SHARED_ENV_ROOT / f"staging-gb10-worker-{image_tag}.env"
    return f"""import json
import pathlib
import re
import subprocess

candidate_sha = {plan.candidate_sha!r}
env_file = pathlib.Path({str(env_file)!r})

def emit(exact, image_id=""):
    print(json.dumps({{"candidate_sha": candidate_sha, "exact": exact,
                      "image_id": image_id}}, sort_keys=True,
                     separators=(",", ":")))

try:
    values = {{}}
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key in values:
            raise ValueError("duplicate env key")
        values[key] = value
    image_id = values.get("LOOM_WORKER_IMAGE_ID", "")
    if (values.get("LOOM_WORKER_CANDIDATE_SHA") != candidate_sha
            or re.fullmatch(r"sha256:[0-9a-f]{{64}}", image_id) is None):
        emit(False, image_id)
        raise SystemExit(0)
    driver = subprocess.run(
        ["docker", "info", "--format", "{{{{.Driver}}}}"],
        check=False, capture_output=True, text=True, timeout=30,
        env={{"HOME": str(pathlib.Path.home()), "LANG": "C.UTF-8",
             "LC_ALL": "C.UTF-8", "PATH": "/usr/local/bin:/usr/bin:/bin"}},
    )
    if driver.returncode != 0 or driver.stderr or driver.stdout.strip() != "overlay2":
        emit(False, image_id)
        raise SystemExit(0)
    inspected = subprocess.run(
        ["docker", "image", "inspect", image_id],
        check=False, capture_output=True, text=True, timeout=30,
        env={{"HOME": str(pathlib.Path.home()), "LANG": "C.UTF-8",
             "LC_ALL": "C.UTF-8", "PATH": "/usr/local/bin:/usr/bin:/bin"}},
    )
    if inspected.returncode != 0 or inspected.stderr or len(inspected.stdout) > 1024 * 1024:
        emit(False, image_id)
        raise SystemExit(0)
    payload = json.loads(inspected.stdout)
    row = payload[0] if isinstance(payload, list) and len(payload) == 1 else {{}}
    config = row.get("Config") if isinstance(row, dict) else None
    labels = config.get("Labels") if isinstance(config, dict) else None
    exact = bool(
        row.get("Id") == image_id
        and row.get("Os") == "linux"
        and row.get("Architecture") == "arm64"
        and isinstance(config, dict)
        and isinstance(labels, dict)
        and labels.get("org.opencontainers.image.revision") == candidate_sha
        and config.get("Cmd") == ["python", "-m", "loom_worker"]
        and config.get("Entrypoint") in (None, [])
    )
    emit(exact, image_id)
except (OSError, UnicodeError, ValueError, json.JSONDecodeError,
        subprocess.SubprocessError):
    emit(False)
"""


def _native_worker_build_source(
    *,
    candidate_sha: str,
    image_tag: str,
    source_sha256: str,
) -> str:
    """Render the fixed native-arm64 build receiver for trt-gb10-1."""

    if (
        _SHA_RE.fullmatch(candidate_sha) is None
        or _STAGING_IMAGE_TAG_RE.fullmatch(image_tag) is None
        or _SHA256_RE.fullmatch(source_sha256) is None
    ):
        raise ValueError("GB10 native worker build identity is invalid")
    return f"""import fcntl
import hashlib
import os
import pathlib
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time

expected_source_sha256 = {source_sha256!r}
candidate_sha = {candidate_sha!r}
image_tag = {image_tag!r}
max_source_bytes = 1024 * 1024 * 1024
max_expanded_bytes = 4 * 1024 * 1024 * 1024
max_image_bytes = 16 * 1024 * 1024 * 1024
state_root = pathlib.Path("/tmp/loom-staging-native-worker-build")
state_root.mkdir(mode=0o700, exist_ok=True)
state_metadata = state_root.lstat()
if (not stat.S_ISDIR(state_metadata.st_mode)
        or stat.S_ISLNK(state_metadata.st_mode)
        or state_metadata.st_uid != os.geteuid()):
    raise SystemExit(1)
state_root.chmod(0o700)
lock_fd = os.open(
    state_root / "build.lock",
    os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
    0o600,
)
lock_metadata = os.fstat(lock_fd)
if (not stat.S_ISREG(lock_metadata.st_mode)
        or lock_metadata.st_uid != os.geteuid()
        or stat.S_IMODE(lock_metadata.st_mode) != 0o600):
    os.close(lock_fd)
    raise SystemExit(1)
try:
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    os.close(lock_fd)
    raise SystemExit(1)
for stale in state_root.glob("work-*"):
    metadata = stale.lstat()
    if (not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != os.geteuid()):
        raise SystemExit(1)
    shutil.rmtree(stale)
work = pathlib.Path(tempfile.mkdtemp(prefix="work-", dir=state_root))
build_process = None

def interrupted(_signum, _frame):
    global build_process
    if build_process is not None and build_process.poll() is None:
        build_process.terminate()
    raise SystemExit(1)

signal.signal(signal.SIGHUP, interrupted)
signal.signal(signal.SIGTERM, interrupted)

def safe_symlink(name, linkname):
    target = pathlib.PurePosixPath(linkname)
    if target.is_absolute() or not linkname:
        return False
    resolved = []
    for part in (*name.parent.parts, *target.parts):
        if part in ("", "."):
            continue
        if part == "..":
            if not resolved:
                return False
            resolved.pop()
        else:
            resolved.append(part)
    return bool(resolved)

try:
    source = work / "source.tar"
    digest = hashlib.sha256()
    size = 0
    with source.open("xb") as output:
        while True:
            chunk = sys.stdin.buffer.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > max_source_bytes:
                raise ValueError("source archive oversized")
            digest.update(chunk)
            output.write(chunk)
    source.chmod(0o600)
    if size == 0 or digest.hexdigest() != expected_source_sha256:
        raise ValueError("source archive identity mismatch")

    context = work / "context"
    context.mkdir(mode=0o700)
    with tarfile.open(source, "r:") as archive:
        members = archive.getmembers()
        names = [pathlib.PurePosixPath(member.name) for member in members]
        if (len(members) > 100000 or len(set(names)) != len(names)
                or sum(member.size for member in members if member.isfile())
                   > max_expanded_bytes):
            raise ValueError("source archive boundary invalid")
        for member, name in zip(members, names):
            if (not member.name or name.is_absolute() or ".." in name.parts
                    or member.islnk()
                    or not (member.isfile() or member.isdir() or member.issym())):
                raise ValueError("source archive member invalid")
            destination = context.joinpath(*name.parts)
            if member.issym():
                if not safe_symlink(name, member.linkname):
                    raise ValueError("source archive symlink invalid")
                continue
            if member.isdir():
                destination.mkdir(mode=member.mode & 0o755, parents=True, exist_ok=True)
                continue
            destination.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError("source archive file unavailable")
            with destination.open("xb") as output:
                shutil.copyfileobj(stream, output, length=1024 * 1024)
            destination.chmod(member.mode & 0o755)
        for member, name in zip(members, names):
            if member.issym():
                destination = context.joinpath(*name.parts)
                destination.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
                destination.symlink_to(member.linkname)

    image = work / "worker-image.tar"
    preflight = subprocess.run(
        ["/usr/bin/docker", "info", "--format", "{{{{.Driver}}}} {{{{.Architecture}}}}"],
        check=False, capture_output=True, text=True, timeout=30,
    )
    if (preflight.returncode != 0 or preflight.stderr
            or preflight.stdout.strip() not in ("overlay2 arm64", "overlay2 aarch64")):
        raise ValueError("native Docker preflight failed")
    command = [
        "/usr/bin/docker", "buildx", "build",
        "--platform", "linux/arm64",
        "--file", "deploy/Dockerfile.worker",
        "--label", "org.opencontainers.image.revision=" + candidate_sha,
        "--label", "loom.source-archive.sha256=" + expected_source_sha256,
        "--build-arg", "LOOM_BUILD_SHA=" + candidate_sha,
        "--tag", "loom-worker:" + image_tag + "-arm64",
        "--provenance=false", "--progress=quiet",
        "--output", "type=docker,dest=" + str(image), ".",
    ]
    build_process = subprocess.Popen(
        command, cwd=context, stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    diagnostic_size = 0
    deadline = time.monotonic() + 3300
    if build_process.stderr is None:
        raise ValueError("native image build diagnostic is unavailable")
    with selectors.DefaultSelector() as selector:
        selector.register(build_process.stderr, selectors.EVENT_READ)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, 3300)
            events = selector.select(timeout=min(1.0, remaining))
            if not events and build_process.poll() is None:
                continue
            for key, _mask in events:
                chunk = os.read(key.fileobj.fileno(), 65536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                diagnostic_size += len(chunk)
                if diagnostic_size > 65536:
                    raise ValueError("native image build diagnostic is too large")
    build_process.wait(timeout=max(0.1, deadline - time.monotonic()))
    if (build_process.returncode != 0
            or not image.is_file()
            or not 0 < image.stat().st_size <= max_image_bytes):
        raise ValueError("native image build failed")
    with image.open("rb") as stream:
        shutil.copyfileobj(stream, sys.stdout.buffer, length=1024 * 1024)
    sys.stdout.buffer.flush()
except Exception:
    sys.stderr.write("GB10 native worker build failed safely\\n")
    raise SystemExit(1)
finally:
    if build_process is not None and build_process.poll() is None:
        build_process.kill()
        build_process.wait()
    shutil.rmtree(work, ignore_errors=True)
    os.close(lock_fd)
"""


def native_worker_build_ssh_argv(
    cluster_config_path: Path,
    *,
    candidate_sha: str,
    image_tag: str,
    source_sha256: str,
) -> tuple[str, ...]:
    """Bind one native build to qianyi@trt-gb10-1 and installed SSH authority."""

    from loom_cli.cluster_config import load_cluster_config

    if not cluster_config_path.is_absolute() or ".." in cluster_config_path.parts:
        raise ValueError("GB10 native build cluster config is invalid")
    try:
        cluster = load_cluster_config(cluster_config_path)
    except Exception as exc:
        raise ValueError("GB10 native build cluster config is unavailable") from exc
    pool = getattr(cluster, "gb10_pool", None)
    raw_hosts = getattr(pool, "hosts", None) if pool is not None else None
    ssh_config_value = getattr(pool, "ssh_config", None) if pool is not None else None
    identity_value = getattr(pool, "ssh_identity_file", None) if pool is not None else None
    certificate_value = getattr(pool, "ssh_certificate_file", None) if pool is not None else None
    expected_hosts = tuple(f"trt-gb10-{number}" for number in range(1, 16))
    if (
        not isinstance(raw_hosts, Sequence)
        or isinstance(raw_hosts, (str, bytes))
        or tuple(raw.get("ssh_target") if isinstance(raw, dict) else None for raw in raw_hosts)
        != expected_hosts
        or not isinstance(ssh_config_value, str)
        or not ssh_config_value
        or identity_value != str(_FIXED_IDENTITY)
    ):
        raise ValueError("GB10 native build authority is incomplete")
    ssh_config = Path(ssh_config_value).expanduser()
    if not ssh_config.is_absolute():
        ssh_config = cluster_config_path.parent / ssh_config
    ssh_config = ssh_config.resolve(strict=False)
    command = "python3 -c " + shlex.quote(
        _native_worker_build_source(
            candidate_sha=candidate_sha,
            image_tag=image_tag,
            source_sha256=source_sha256,
        )
    )
    argv = [
        "ssh",
        "-F",
        str(ssh_config),
        "-i",
        str(_FIXED_IDENTITY),
        "-o",
        "IdentitiesOnly=yes",
    ]
    if certificate_value:
        certificate = Path(str(certificate_value)).expanduser()
        if not certificate.is_absolute():
            certificate = cluster_config_path.parent / certificate
        argv.extend(("-o", f"CertificateFile={certificate.resolve(strict=False)}"))
    argv.extend(
        (
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={_KNOWN_HOSTS}",
            "-o",
            "GlobalKnownHostsFile=/dev/null",
            "-o",
            "UpdateHostKeys=no",
            "trt-gb10-1",
            command,
        )
    )
    return tuple(argv)


def _retirement_observation_source(
    target: GB10TransportTarget,
    plan: FinalGatePlan,
) -> str:
    """Read back only the durable absence of every legacy user authority."""
    units = (_LEGACY_SERVICE, target.node_agent_service, target.timer)
    retirement_identity = _retirement_identity(plan, units)
    return f"""import json
import os
import pathlib
import stat
import subprocess

units = {units!r}
candidate_sha = {plan.candidate_sha!r}
candidate_tree = {plan.candidate_tree!r}
plan_digest = {plan.plan_digest!r}
retirement_identity = {retirement_identity!r}
state = pathlib.Path.home() / ".local/state/loom-staging-rollout/gb10-authority-retirement.json"
unit_root = pathlib.Path.home() / ".config/systemd/user"

def run(argv):
    return subprocess.run(argv, check=False, capture_output=True, text=True, timeout=10,
                          env={{"HOME": str(pathlib.Path.home()), "LANG": "C.UTF-8",
                          "LC_ALL": "C.UTF-8", "PATH": "/usr/local/bin:/usr/bin:/bin",
                          "XDG_RUNTIME_DIR": "/run/user/" + str(os.getuid())}})

def absent(path):
    try:
        path.lstat()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return False

def retired(unit):
    active = run(["systemctl", "--user", "is-active", unit])
    enabled = run(["systemctl", "--user", "is-enabled", unit])
    return (active.stdout.strip() not in {{"active", "activating", "reloading"}}
            and enabled.stdout.strip() not in {{"enabled", "enabled-runtime", "linked",
                                               "linked-runtime", "alias"}})

def committed():
    try:
        item = state.lstat()
        raw = state.read_bytes()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return False
    canonical = (json.dumps(payload, sort_keys=True, separators=(",", ":"),
                            ensure_ascii=True) + "\\n").encode("ascii")
    return bool(stat.S_ISREG(item.st_mode) and not stat.S_ISLNK(item.st_mode)
                and item.st_uid == os.getuid() and item.st_nlink == 1
                and stat.S_IMODE(item.st_mode) == 0o600 and raw == canonical
                and payload.get("schema_version") == 1
                and payload.get("kind") == "loom.gb10-user-authority-retirement"
                and payload.get("phase") == "committed"
                and set(payload) == {{
                    "schema_version", "kind", "phase", "candidate_sha",
                    "candidate_tree", "plan_digest", "retirement_identity", "units"
                }}
                and isinstance(payload.get("plan_digest"), str)
                and len(payload["plan_digest"]) == 64
                and all(character in "0123456789abcdef"
                        for character in payload["plan_digest"])
                and payload.get("candidate_sha") == candidate_sha
                and payload.get("candidate_tree") == candidate_tree
                and payload.get("retirement_identity") == retirement_identity
                and payload.get("units") == list(units))

boot_id = "unavailable"
try:
    boot_id = pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(
        encoding="ascii").strip()
except OSError:
    pass
manager = run(["systemctl", "--user", "show", "--property=Version", "--value"])
linger = run(["loginctl", "show-user", str(os.getuid()), "--property=Linger", "--value"])
baseline_ready = bool(boot_id != "unavailable" and manager.returncode == 0
                      and manager.stdout.strip() and linger.returncode == 0
                      and linger.stdout.strip() == "yes")
legacy_absent = retired(units[0]) and absent(unit_root / units[0])
all_absent = all(absent(unit_root / unit) for unit in units)
dropins_absent = absent(unit_root / (units[2] + ".d"))
retirement_exact = bool(all(retired(unit) for unit in units)
                        and all_absent and dropins_absent and committed())

print(json.dumps({{
    "baseline_ready": baseline_ready,
    "boot_id": boot_id,
    "candidate_source_exact": True,
    "checkout_exact": True,
    "environment_exact": True,
    "legacy_absent": legacy_absent,
    "service_timer_exact": retirement_exact,
    "service_timer_transient": False,
    "units_exact": True,
}}, sort_keys=True, separators=(",", ":")))"""


def _legacy_observation_source(target: GB10TransportTarget, plan: FinalGatePlan) -> str:
    """Preserve the bounded legacy user-checkout observation contract."""
    if target.repo_path is None or target.env_file_path is None:
        raise ValueError("legacy GB10 target paths are unavailable")
    image_tag = f"staging-{plan.candidate_sha[:7]}"
    shared = _LEGACY_SHARED_ROOT / f"loom-remote-worker-{image_tag}"
    units = tuple(
        str(_LEGACY_UNIT_ROOT / unit) for unit in (target.node_agent_service, target.timer)
    )
    return f"""import json
import os
import pathlib
import stat
import subprocess

candidate_sha = {plan.candidate_sha!r}
candidate_tree = {plan.candidate_tree!r}
image_tag = {image_tag!r}
shared = pathlib.Path({str(shared)!r})
repo = pathlib.Path({str(target.repo_path)!r})
env_file = pathlib.Path({str(target.env_file_path)!r})
service = {target.node_agent_service!r}
timer = {target.timer!r}
legacy = {_LEGACY_SERVICE!r}
unit_paths = {units!r}

def run(argv, *, cwd=None):
    return subprocess.run(argv, cwd=cwd, check=False, capture_output=True, text=True,
                          timeout=30, env={{"HOME": str(pathlib.Path.home()),
                          "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
                          "PATH": "/usr/local/bin:/usr/bin:/bin",
                          "XDG_RUNTIME_DIR": "/run/user/" + str(os.getuid())}})

def git_exact(root, *, whole_tree):
    try:
        item = root.lstat()
    except OSError:
        return False
    if not stat.S_ISDIR(item.st_mode) or stat.S_ISLNK(item.st_mode):
        return False
    prefix = ["git", "-c", f"safe.directory={{root}}", "-C", str(root)]
    head = run([*prefix, "rev-parse", "HEAD"])
    tree = run([*prefix, "rev-parse", "HEAD^{{tree}}"])
    status = run([*prefix, "status", "--porcelain=v1",
                  "--untracked-files=all" if whole_tree else "--untracked-files=no"])
    return (head.returncode == tree.returncode == status.returncode == 0
            and not head.stderr and not tree.stderr and not status.stderr
            and head.stdout.strip() == candidate_sha
            and tree.stdout.strip() == candidate_tree and not status.stdout.strip())

def environment_exact():
    try:
        values = {{}}
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if "=" not in line or line.lstrip().startswith("#"):
                continue
            key, value = line.split("=", 1)
            if key.strip() in values:
                return False
            values[key.strip()] = value
    except (OSError, UnicodeError):
        return False
    return all(values.get(key) == image_tag for key in (
        "IMAGE_TAG", "ENV_CONFIG_VERSION", "LOOM_IMAGE_TAG",
        "LOOM_WORKER_ENV_CONFIG_VERSION"))

def units_exact():
    try:
        return all(
            (repo / relative).read_bytes()
            == (pathlib.Path.home() / ".config/systemd/user"
                / pathlib.Path(relative).name).read_bytes()
            for relative in unit_paths)
    except OSError:
        return False

def enabled(unit):
    result = run(["systemctl", "--user", "is-enabled", unit])
    return result.returncode == 0 and result.stdout.strip() == "enabled"

def properties(unit, names):
    result = run(["systemctl", "--user", "show", unit,
                  *[f"--property={{name}}" for name in names]])
    if result.returncode != 0:
        return None
    parsed = {{}}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            parsed[key] = value
    return parsed if set(parsed) == set(names) else None

try:
    boot_id = pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(
        encoding="ascii").strip()
except OSError:
    boot_id = "unavailable"
manager = run(["systemctl", "--user", "show", "--property=Version", "--value"])
linger = run(["loginctl", "show-user", str(os.getuid()), "--property=Linger", "--value"])
baseline_ready = bool(boot_id != "unavailable" and manager.returncode == 0
                      and manager.stdout.strip() and linger.returncode == 0
                      and linger.stdout.strip() == "yes")
service_props = properties(service, [
    "LoadState", "Type", "Result", "ExecMainStatus", "ActiveState", "SubState",
    "NeedDaemonReload",
])
timer_props = properties(timer, [
    "LoadState", "ActiveState", "SubState", "Unit", "NeedDaemonReload",
])
timer_enabled = enabled(timer)
service_prepared = bool(service_props is not None
    and service_props.get("LoadState") == "loaded"
    and service_props.get("Type") == "oneshot"
    and service_props.get("Result") == "success"
    and service_props.get("ExecMainStatus") == "0"
    and service_props.get("ActiveState") == "inactive"
    and service_props.get("SubState") == "dead"
    and service_props.get("NeedDaemonReload") == "no")
service_inflight_safe = bool(service_props is not None
    and service_props.get("LoadState") == "loaded"
    and service_props.get("Type") == "oneshot"
    and service_props.get("Result") in {{"", "success"}}
    and service_props.get("ExecMainStatus") in {{"", "0"}}
    and (service_props.get("ActiveState"), service_props.get("SubState"))
        in {{("activating", "start"), ("active", "running")}}
    and service_props.get("NeedDaemonReload") == "no")
timer_common = bool(timer_props is not None
    and timer_props.get("LoadState") == "loaded"
    and timer_props.get("ActiveState") == "active"
    and timer_props.get("Unit") == service
    and timer_props.get("NeedDaemonReload") == "no")
timer_waiting = bool(timer_common and timer_props.get("SubState") == "waiting")
timer_firing = bool(timer_common and timer_props.get("SubState") == "running")
service_timer_exact = bool(service_prepared and timer_waiting and timer_enabled)
service_timer_transient = bool(service_inflight_safe and timer_firing and timer_enabled)
legacy_enabled = run(["systemctl", "--user", "is-enabled", legacy])
legacy_props = properties(legacy, ["LoadState", "ActiveState", "SubState"])
legacy_absent = bool(legacy_enabled.returncode != 0 and (
    legacy_props is None or (legacy_props.get("ActiveState") not in {{"active", "activating"}}
    and legacy_props.get("SubState") != "running")))
print(json.dumps({{
    "baseline_ready": baseline_ready,
    "boot_id": boot_id,
    "candidate_source_exact": git_exact(shared, whole_tree=True),
    "checkout_exact": git_exact(repo, whole_tree=False),
    "environment_exact": environment_exact(),
    "legacy_absent": legacy_absent,
    "service_timer_exact": service_timer_exact,
    "service_timer_transient": service_timer_transient,
    "units_exact": units_exact(),
}}, sort_keys=True, separators=(",", ":")))"""


def _remote_observation_source(target: GB10TransportTarget, plan: FinalGatePlan) -> str:
    if target.retirement_only:
        return _retirement_observation_source(target, plan)
    return _legacy_observation_source(target, plan)


def _remote_apply_command(
    target: GB10TransportTarget,
    plan: FinalGatePlan,
    operations: tuple[GB10MutationKind, ...],
) -> str:
    return "python3 -c " + shlex.quote(_remote_apply_source(target, plan, operations))


def _retirement_apply_source(
    target: GB10TransportTarget,
    plan: FinalGatePlan,
    operations: tuple[GB10MutationKind, ...],
) -> str:
    """Render one lock+journal roll-forward retirement transaction."""
    units = (_LEGACY_SERVICE, target.node_agent_service, target.timer)
    retirement_identity = _retirement_identity(plan, units)
    return f"""import fcntl
import json
import os
import pathlib
import stat
import subprocess
import tempfile

candidate_sha = {plan.candidate_sha!r}
candidate_tree = {plan.candidate_tree!r}
plan_digest = {plan.plan_digest!r}
retirement_identity = {retirement_identity!r}
expected_boot_id = {plan.gb10_boot_ids[target.ssh_target]!r}
operations = {tuple(operation.value for operation in operations)!r}
units = {units!r}
state_root = pathlib.Path.home() / ".local/state/loom-staging-rollout"
receipt = state_root / "gb10-authority-retirement.json"
lock_path = state_root / "gb10-authority-retirement.lock"
unit_root = pathlib.Path.home() / ".config/systemd/user"

def run(argv):
    return subprocess.run(argv, check=False, capture_output=True, text=True, timeout=30,
                          env={{"HOME": str(pathlib.Path.home()), "LANG": "C.UTF-8",
                          "LC_ALL": "C.UTF-8", "PATH": "/usr/local/bin:/usr/bin:/bin",
                          "XDG_RUNTIME_DIR": "/run/user/" + str(os.getuid())}})

def canonical(payload):
    return (json.dumps(payload, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True) + "\\n").encode("ascii")

def ensure_state_root():
    cursor = pathlib.Path.home()
    for part in (".local", "state", "loom-staging-rollout"):
        cursor = cursor / part
        if os.path.lexists(cursor):
            item = cursor.lstat()
            if (not stat.S_ISDIR(item.st_mode) or stat.S_ISLNK(item.st_mode)
                    or item.st_uid != os.getuid() or stat.S_IMODE(item.st_mode) & 0o022):
                raise SystemExit(1)
        else:
            os.mkdir(cursor, 0o700)
        os.chmod(cursor, 0o700)

def atomic_write(path, payload):
    parent = path.parent.lstat()
    if (not stat.S_ISDIR(parent.st_mode) or stat.S_ISLNK(parent.st_mode)
            or parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) != 0o700):
        raise SystemExit(1)
    if os.path.lexists(path) and stat.S_ISLNK(path.lstat().st_mode):
        raise SystemExit(1)
    descriptor, temporary = tempfile.mkstemp(prefix=".gb10-retirement.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        parent_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass

def retired(unit):
    active = run(["systemctl", "--user", "is-active", unit])
    enabled = run(["systemctl", "--user", "is-enabled", unit])
    return (active.stdout.strip() not in {{"active", "activating", "reloading"}}
            and enabled.stdout.strip() not in {{"enabled", "enabled-runtime", "linked",
                                               "linked-runtime", "alias"}})

def absent(path):
    try:
        path.lstat()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return False

def already_committed():
    try:
        item = receipt.lstat()
        raw = receipt.read_bytes()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return False
    return bool(
        stat.S_ISREG(item.st_mode) and not stat.S_ISLNK(item.st_mode)
        and item.st_uid == os.getuid() and item.st_nlink == 1
        and stat.S_IMODE(item.st_mode) == 0o600
        and raw == canonical(payload)
        and set(payload) == {{
            "schema_version", "kind", "phase", "candidate_sha",
            "candidate_tree", "plan_digest", "retirement_identity", "units"
        }}
        and payload.get("schema_version") == 1
        and payload.get("kind") == "loom.gb10-user-authority-retirement"
        and payload.get("phase") == "committed"
        and payload.get("candidate_sha") == candidate_sha
        and payload.get("candidate_tree") == candidate_tree
        and payload.get("retirement_identity") == retirement_identity
        and payload.get("units") == list(units)
    )

if (pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(
        encoding="ascii").strip() != expected_boot_id
        or not operations
        or any(operation not in ("legacy-retire", "service-timer")
               for operation in operations)
        or run(["loginctl", "show-user", str(os.getuid()),
                "--property=Linger", "--value"]).stdout.strip() != "yes"):
    raise SystemExit(1)

ensure_state_root()
lock_descriptor = os.open(
    lock_path, os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0), 0o600)
try:
    os.fchmod(lock_descriptor, 0o600)
    fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
    dropin = unit_root / (units[2] + ".d")
    if (already_committed() and all(retired(unit) for unit in units)
            and all(absent(unit_root / unit) for unit in units)
            and absent(dropin)):
        raise SystemExit(0)
    planned = {{
        "schema_version": 1,
        "kind": "loom.gb10-user-authority-retirement",
        "phase": "planned",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "plan_digest": plan_digest,
        "retirement_identity": retirement_identity,
        "units": list(units),
    }}
    atomic_write(receipt, canonical(planned))
    for unit in units:
        run(["systemctl", "--user", "disable", "--now", unit])
        run(["systemctl", "--user", "stop", unit])
        run(["systemctl", "--user", "reset-failed", unit])
    if os.path.lexists(dropin):
        item = dropin.lstat()
        if not stat.S_ISDIR(item.st_mode) or stat.S_ISLNK(item.st_mode):
            raise SystemExit(1)
        entries = {{entry.name for entry in dropin.iterdir()}}
        if entries not in (set(), {{"deploy-window.conf"}}):
            raise SystemExit(1)
        if entries:
            leaf = dropin / "deploy-window.conf"
            if leaf.is_symlink() or not leaf.is_file():
                raise SystemExit(1)
            leaf.unlink()
        dropin.rmdir()
    for unit in units:
        path = unit_root / unit
        if os.path.lexists(path):
            item = path.lstat()
            if not (stat.S_ISREG(item.st_mode) or stat.S_ISLNK(item.st_mode)):
                raise SystemExit(1)
            path.unlink()
    if run(["systemctl", "--user", "daemon-reload"]).returncode != 0:
        raise SystemExit(1)
    for unit in units:
        if not retired(unit) or os.path.lexists(unit_root / unit):
            raise SystemExit(1)
    if os.path.lexists(dropin):
        raise SystemExit(1)
    committed = dict(planned)
    committed["phase"] = "committed"
    atomic_write(receipt, canonical(committed))
finally:
    os.close(lock_descriptor)
"""


def _legacy_apply_source(
    target: GB10TransportTarget,
    plan: FinalGatePlan,
    operations: tuple[GB10MutationKind, ...],
) -> str:
    """Preserve the fixed legacy checkout/env/unit convergence boundary."""
    if target.repo_path is None or target.env_file_path is None:
        raise ValueError("legacy GB10 target paths are unavailable")
    image_tag = f"staging-{plan.candidate_sha[:7]}"
    shared = _LEGACY_SHARED_ROOT / f"loom-remote-worker-{image_tag}"
    unit_paths = tuple(
        str(_LEGACY_UNIT_ROOT / unit) for unit in (target.node_agent_service, target.timer)
    )
    return f"""import os
import pathlib
import stat
import subprocess
import tempfile

candidate_sha = {plan.candidate_sha!r}
candidate_tree = {plan.candidate_tree!r}
image_tag = {image_tag!r}
shared = pathlib.Path({str(shared)!r})
repo = pathlib.Path({str(target.repo_path)!r})
env_file = pathlib.Path({str(target.env_file_path)!r})
service = {target.node_agent_service!r}
timer = {target.timer!r}
legacy = {_LEGACY_SERVICE!r}
unit_paths = {unit_paths!r}
operations = {tuple(operation.value for operation in operations)!r}
expected_boot_id = {plan.gb10_boot_ids[target.ssh_target]!r}

def run(argv, *, cwd=None):
    result = subprocess.run(argv, cwd=cwd, check=False, capture_output=True, text=True,
                            timeout=120, env={{"HOME": str(pathlib.Path.home()),
                            "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
                            "PATH": "/usr/local/bin:/usr/bin:/bin",
                            "XDG_RUNTIME_DIR": "/run/user/" + str(os.getuid())}})
    if result.returncode != 0:
        raise SystemExit(1)

def output(argv):
    result = subprocess.run(argv, check=False, capture_output=True, text=True,
                            timeout=30, env={{"HOME": str(pathlib.Path.home()),
                            "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
                            "PATH": "/usr/local/bin:/usr/bin:/bin",
                            "XDG_RUNTIME_DIR": "/run/user/" + str(os.getuid())}})
    if result.returncode != 0 or result.stderr:
        raise SystemExit(1)
    return result.stdout.strip()

def exact_shared():
    item = shared.lstat()
    prefix = ["git", "-c", f"safe.directory={{shared}}", "-C", str(shared)]
    return (stat.S_ISDIR(item.st_mode) and not stat.S_ISLNK(item.st_mode)
            and output([*prefix, "rev-parse", "HEAD"]) == candidate_sha
            and output([*prefix, "rev-parse", "HEAD^{{tree}}"]) == candidate_tree
            and output([*prefix, "status", "--porcelain=v1",
                        "--untracked-files=all"]) == "")

def regular(path):
    item = path.lstat()
    if (not stat.S_ISREG(item.st_mode) or stat.S_ISLNK(item.st_mode)
            or item.st_nlink != 1 or item.st_size > 1024 * 1024):
        raise SystemExit(1)
    return path.read_bytes()

def atomic_write(path, payload, mode):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    parent = path.parent.lstat()
    if not stat.S_ISDIR(parent.st_mode) or stat.S_ISLNK(parent.st_mode):
        raise SystemExit(1)
    descriptor, temporary = tempfile.mkstemp(prefix=".loom-rollout-", dir=path.parent)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(payload); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try: os.unlink(temporary)
        except FileNotFoundError: pass

if (pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(
        encoding="ascii").strip() != expected_boot_id or not exact_shared()
        or output(["loginctl", "show-user", str(os.getuid()),
                   "--property=Linger", "--value"]) != "yes"):
    raise SystemExit(1)

for operation in operations:
    if operation == "checkout":
        if not (repo / ".git").is_dir():
            raise SystemExit(1)
        run(["git", "-c", "protocol.file.allow=always", "-c", "fetch.fsckObjects=true",
             "-C", str(repo), "fetch", "--quiet", "--no-tags", "--no-recurse-submodules",
             str(shared), candidate_sha])
        run(["git", "-C", str(repo), "checkout", "--detach", candidate_sha])
    elif operation == "environment":
        existing = (regular(env_file).decode("utf-8").splitlines()
                    if os.path.lexists(env_file) else [])
        updates = {{key: image_tag for key in ("IMAGE_TAG", "ENV_CONFIG_VERSION",
            "LOOM_IMAGE_TAG", "LOOM_WORKER_ENV_CONFIG_VERSION")}}
        rendered = []; seen = set()
        for line in existing:
            if "=" not in line or line.lstrip().startswith("#"):
                rendered.append(line); continue
            key = line.split("=", 1)[0].strip()
            if key in updates:
                if key in seen: raise SystemExit(1)
                rendered.append(f"{{key}}={{updates[key]}}"); seen.add(key)
            else: rendered.append(line)
        rendered.extend(f"{{key}}={{value}}" for key, value in updates.items()
                        if key not in seen)
        atomic_write(env_file, ("\\n".join(rendered) + "\\n").encode(), 0o600)
    elif operation == "units":
        for relative in unit_paths:
            source = repo / relative
            destination = (pathlib.Path.home() / ".config/systemd/user"
                           / pathlib.Path(relative).name)
            atomic_write(destination, regular(source), 0o644)
    elif operation == "legacy-retire":
        subprocess.run(["systemctl", "--user", "disable", "--now", legacy],
            check=False, capture_output=True, text=True, timeout=30)
        subprocess.run(["systemctl", "--user", "reset-failed", legacy],
            check=False, capture_output=True, text=True, timeout=30)
    elif operation == "service-timer":
        run(["systemctl", "--user", "daemon-reload"])
        run(["systemctl", "--user", "start", service])
        run(["systemctl", "--user", "enable", "--now", timer])
        run(["systemctl", "--user", "restart", timer])
    else:
        raise SystemExit(1)
"""


def _remote_apply_source(
    target: GB10TransportTarget,
    plan: FinalGatePlan,
    operations: tuple[GB10MutationKind, ...],
) -> str:
    if target.retirement_only:
        return _retirement_apply_source(target, plan, operations)
    return _legacy_apply_source(target, plan, operations)


def _hash_json(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _external_retirement_profile(cluster_config_path: Path, profile_value: object) -> bool:
    if not isinstance(profile_value, str) or not profile_value:
        return False
    profile_path = Path(profile_value)
    if not profile_path.is_absolute():
        profile_path = cluster_config_path.parent / profile_path
    profile_path = profile_path.resolve(strict=False)
    try:
        payload = profile_path.read_bytes()
        raw = tomllib.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ValueError("GB10 installed environment profile is unavailable") from exc
    prerequisites = raw.get("external_slurm_runner_prerequisites")
    desired = raw.get("gb10_worker_pool_desired_states")
    return bool(
        isinstance(prerequisites, dict)
        and prerequisites.get("pools") == ["gb10"]
        and prerequisites.get("require_external_allocation_authority") is True
        and isinstance(desired, list)
        and len(desired) == 1
        and isinstance(desired[0], dict)
        and desired[0].get("pool_name") == "gb10"
        and desired[0].get("target_slots") == 0
        and isinstance(desired[0].get("host_intents"), dict)
        and set(desired[0]["host_intents"]) == {f"trt-gb10-{number}" for number in range(1, 16)}
        and set(desired[0]["host_intents"].values()) == {"stopped"}
    )


def build_fixed_gb10_ssh_transport(
    cluster_config_path: Path,
    *,
    expected_hosts: Sequence[str],
    run: CommandRunner,
    max_concurrency: int,
) -> FixedGB10SSHTransport:
    """Bind the fixed transport to the exact installed staging inventory."""
    from loom_cli.cluster_config import load_cluster_config

    if (
        not cluster_config_path.is_absolute()
        or ".." in cluster_config_path.parts
        or not expected_hosts
        or len(set(expected_hosts)) != len(expected_hosts)
        or any(_HOST_RE.fullmatch(host) is None for host in expected_hosts)
    ):
        raise ValueError("GB10 installed transport input is invalid")
    try:
        cluster = load_cluster_config(cluster_config_path)
    except Exception as exc:
        raise ValueError("GB10 installed cluster config is unavailable") from exc
    pool = getattr(cluster, "gb10_pool", None)
    raw_hosts = getattr(pool, "hosts", None) if pool is not None else None
    ssh_config_value = getattr(pool, "ssh_config", None) if pool is not None else None
    identity_value = getattr(pool, "ssh_identity_file", None) if pool is not None else None
    certificate_value = getattr(pool, "ssh_certificate_file", None) if pool is not None else None
    retirement_only = _external_retirement_profile(
        cluster_config_path,
        getattr(cluster, "env_state_profile", None),
    )
    if (
        not isinstance(raw_hosts, Sequence)
        or isinstance(raw_hosts, (str, bytes))
        or not isinstance(ssh_config_value, str)
        or not ssh_config_value
        or identity_value != str(_FIXED_IDENTITY)
    ):
        raise ValueError("GB10 installed cluster authority is incomplete")
    targets: list[GB10TransportTarget] = []
    for raw in raw_hosts:
        allowed_fields = (
            {"ssh_target", "node_agent_service"}
            if retirement_only
            else {
                "ssh_target",
                "repo_path",
                "env_file_path",
                "repo_url",
                "node_agent_service",
            }
        )
        if not isinstance(raw, dict) or set(raw) - allowed_fields:
            raise ValueError("GB10 installed host authority is invalid")
        ssh_target = raw.get("ssh_target")
        service = raw.get("node_agent_service")
        if not all(isinstance(value, str) and value for value in (ssh_target, service)):
            raise ValueError("GB10 installed host fields are invalid")
        repo_path = raw.get("repo_path")
        env_file_path = raw.get("env_file_path")
        if not retirement_only and not all(
            isinstance(value, str) and value for value in (repo_path, env_file_path)
        ):
            raise ValueError("GB10 installed legacy host fields are invalid")
        targets.append(
            GB10TransportTarget(
                ssh_target=str(ssh_target),
                repo_path=(None if repo_path is None else PurePosixPath(str(repo_path))),
                env_file_path=(
                    None if env_file_path is None else PurePosixPath(str(env_file_path))
                ),
                node_agent_service=str(service),
                retirement_only=retirement_only,
            )
        )
    if {target.ssh_target for target in targets} != set(expected_hosts):
        raise ValueError("GB10 installed host inventory drifted")
    ssh_config = Path(ssh_config_value).expanduser()
    if not ssh_config.is_absolute():
        ssh_config = cluster_config_path.parent / ssh_config
    ssh_config = ssh_config.resolve(strict=False)
    certificate: Path | None = None
    if certificate_value:
        if not isinstance(certificate_value, str):
            raise ValueError("GB10 installed certificate authority is invalid")
        certificate = Path(certificate_value).expanduser()
        if not certificate.is_absolute():
            certificate = cluster_config_path.parent / certificate
        certificate = certificate.resolve(strict=False)
    return FixedGB10SSHTransport(
        targets=tuple(targets),
        ssh_config=ssh_config,
        identity=_FIXED_IDENTITY,
        certificate=certificate,
        run=run,
        max_concurrency=max_concurrency,
        settle_attempts=_INSTALLED_OBSERVE_ATTEMPTS,
        settle_interval_seconds=_INSTALLED_OBSERVE_INTERVAL_SECONDS,
    )


__all__ = [
    "FixedGB10SSHTransport",
    "GB10FleetApplyError",
    "GB10TransportTarget",
    "build_fixed_gb10_ssh_transport",
    "native_worker_build_ssh_argv",
    "retirement_worker_image_observation_source",
]
