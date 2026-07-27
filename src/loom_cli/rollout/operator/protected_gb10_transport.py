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
_KNOWN_HOSTS = Path("/etc/loom/staging-rollout-gb10-known-hosts")
_SHARED_ROOT = PurePosixPath("/shared_work2/qianyi/.loom-staging-rollout/worker-repos")
_LEGACY_SERVICE = "loom-gb10-worker.service"
_UNIT_ROOT = PurePosixPath("deploy/worker-pools/gb10")
_FIXED_REPO = PurePosixPath("/home/qianyi/loom-worker-build-staging")
_FIXED_ENV_FILE = _FIXED_REPO / ".env"
_FIXED_NODE_AGENT_SERVICE = "loom-gb10-node-agent.service"
_FIXED_IDENTITY = Path("/var/lib/loom-staging-rollout/gb10-deploy-ed25519")
_MAX_OUTPUT_BYTES = 64 * 1024
_MUTATION_ORDER = (
    GB10MutationKind.CHECKOUT,
    GB10MutationKind.ENVIRONMENT,
    GB10MutationKind.UNITS,
    GB10MutationKind.LEGACY_RETIRE,
    GB10MutationKind.SERVICE_TIMER,
)


class CommandResult(Protocol):
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str]], CommandResult]


@dataclass(frozen=True, slots=True)
class GB10TransportTarget:
    """One fixed release-managed host from the installed staging inventory."""

    ssh_target: str
    repo_path: PurePosixPath
    env_file_path: PurePosixPath
    node_agent_service: str

    def __post_init__(self) -> None:
        repo = PurePosixPath(self.repo_path)
        env_file = PurePosixPath(self.env_file_path)
        if (
            _HOST_RE.fullmatch(self.ssh_target) is None
            or _SERVICE_RE.fullmatch(self.node_agent_service) is None
            or not repo.is_absolute()
            or not env_file.is_absolute()
            or ".." in repo.parts
            or ".." in env_file.parts
            or env_file.parent != repo
            or env_file.name != ".env"
            or repo != _FIXED_REPO
            or env_file != _FIXED_ENV_FILE
            or self.node_agent_service != _FIXED_NODE_AGENT_SERVICE
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

    def __post_init__(self) -> None:
        paths = (self.ssh_config, self.identity)
        if (
            not self.targets
            or len({target.ssh_target for target in self.targets}) != len(self.targets)
            or not 1 <= self.max_concurrency <= 16
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
            raise RuntimeError(
                "GB10 convergence failed safely on fixed host(s): " + ",".join(sorted(failures))
            )

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
        command = _remote_observation_command(target, plan)
        result = self.run(self._ssh_argv(target, command))
        if (
            result.returncode != 0
            or result.stderr
            or len(result.stdout.encode()) > _MAX_OUTPUT_BYTES
        ):
            return self._failed_observation(target)
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError:
            return self._failed_observation(target)
        expected = {
            "baseline_ready",
            "boot_id",
            "candidate_source_exact",
            "checkout_exact",
            "environment_exact",
            "legacy_absent",
            "service_timer_exact",
            "units_exact",
        }
        if (
            not isinstance(payload, dict)
            or set(payload) != expected
            or not isinstance(payload["boot_id"], str)
            or any(type(payload[key]) is not bool for key in expected - {"boot_id"})
        ):
            return self._failed_observation(target)
        evidence_digest = _hash_json(payload)
        return GB10HostCandidateObservation(
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

    def _apply_one(
        self,
        target: GB10TransportTarget,
        plan: FinalGatePlan,
        operations: tuple[GB10MutationKind, ...],
    ) -> bool:
        expected_order = tuple(
            operation for operation in _MUTATION_ORDER if operation in operations
        )
        if (
            not operations
            or len(set(operations)) != len(operations)
            or operations != expected_order
        ):
            raise ValueError("GB10 mutation operations are invalid")
        command = _remote_apply_command(target, plan, operations)
        result = self.run(self._ssh_argv(target, command))
        return result.returncode == 0 and result.stdout == "" and result.stderr == ""

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


def _remote_observation_source(target: GB10TransportTarget, plan: FinalGatePlan) -> str:
    image_tag = f"staging-{plan.candidate_sha[:7]}"
    shared = _SHARED_ROOT / f"loom-remote-worker-{image_tag}"
    service = target.node_agent_service
    timer = target.timer
    unit_paths = tuple(str(_UNIT_ROOT / unit) for unit in (service, timer))
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
service = {service!r}
timer = {timer!r}
legacy = {_LEGACY_SERVICE!r}
unit_paths = {unit_paths!r}

def run(argv, *, cwd=None):
    return subprocess.run(argv, cwd=cwd, check=False, capture_output=True, text=True,
                          timeout=10, env={{"HOME": str(pathlib.Path.home()), "LANG": "C.UTF-8",
                          "LC_ALL": "C.UTF-8", "PATH": "/usr/local/bin:/usr/bin:/bin",
                          "XDG_RUNTIME_DIR": "/run/user/" + str(os.getuid())}})

def git(root, *args):
    result = run(["git", "-c", f"safe.directory={{root}}", "-C", str(root), *args])
    return result.stdout.strip() if result.returncode == 0 and not result.stderr else None

def plain_directory(path):
    try:
        item = path.lstat()
    except OSError:
        return False
    return stat.S_ISDIR(item.st_mode) and not stat.S_ISLNK(item.st_mode)

def read_regular(path):
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or before.st_size < 0 or before.st_size > 1024 * 1024):
            raise OSError("unsafe unit source")
        chunks = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                raise OSError("short unit source")
            chunks.append(chunk); remaining -= len(chunk)
        after = os.fstat(descriptor)
        if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
            raise OSError("unstable unit source")
        return b"".join(chunks)
    finally:
        os.close(descriptor)

def exact_repo(root, *, whole_tree):
    if not plain_directory(root):
        return False
    if git(root, "rev-parse", "HEAD") != candidate_sha:
        return False
    if git(root, "rev-parse", "HEAD^{{tree}}") != candidate_tree:
        return False
    status_args = ("status", "--porcelain=v1", "--untracked-files=all") if whole_tree else (
        "status", "--porcelain=v1", "--untracked-files=no", "--", *unit_paths)
    return git(root, *status_args) == ""

def exact_environment():
    try:
        item = env_file.lstat()
        if not stat.S_ISREG(item.st_mode) or stat.S_ISLNK(item.st_mode):
            return False
        values = {{}}
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if "=" not in line or line.lstrip().startswith("#"):
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key in values:
                return False
            values[key] = value
    except (OSError, UnicodeError):
        return False
    return all(values.get(key) == image_tag for key in (
        "IMAGE_TAG", "ENV_CONFIG_VERSION", "LOOM_IMAGE_TAG",
        "LOOM_WORKER_ENV_CONFIG_VERSION"))

def exact_units():
    for relative in unit_paths:
        source = repo / relative
        destination = pathlib.Path.home() / ".config/systemd/user" / pathlib.Path(relative).name
        try:
            src = source.lstat(); dst = destination.lstat()
            if (not stat.S_ISREG(src.st_mode) or stat.S_ISLNK(src.st_mode)
                    or not stat.S_ISREG(dst.st_mode) or stat.S_ISLNK(dst.st_mode)
                    or stat.S_IMODE(dst.st_mode) != 0o644
                    or read_regular(source) != read_regular(destination)):
                return False
        except OSError:
            return False
    return True

def properties(unit, names):
    result = run(["systemctl", "--user", "show", unit, *[f"--property={{name}}" for name in names]])
    if result.returncode != 0:
        return None
    parsed = {{}}
    for line in result.stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            parsed[key] = value
    return parsed if set(parsed) == set(names) else None

boot_id = "unavailable"
try:
    boot_id = pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
except OSError:
    pass
manager = run(["systemctl", "--user", "show", "--property=Version", "--value"])
linger = run(["loginctl", "show-user", str(os.getuid()), "--property=Linger", "--value"])
baseline_ready = bool(boot_id != "unavailable" and manager.returncode == 0
                      and manager.stdout.strip() and linger.returncode == 0
                      and linger.stdout.strip() == "yes")
service_props = properties(service, ["LoadState", "Type", "Result", "ExecMainStatus", "NeedDaemonReload"])
timer_props = properties(timer, ["LoadState", "ActiveState", "SubState", "Unit", "NeedDaemonReload"])
timer_enabled = run(["systemctl", "--user", "is-enabled", timer])
service_timer_exact = bool(service_props == {{"LoadState": "loaded", "Type": "oneshot",
    "Result": "success", "ExecMainStatus": "0", "NeedDaemonReload": "no"}}
    and timer_props == {{"LoadState": "loaded", "ActiveState": "active",
    "SubState": "waiting", "Unit": service, "NeedDaemonReload": "no"}}
    and timer_enabled.returncode == 0 and timer_enabled.stdout.strip() == "enabled")
legacy_enabled = run(["systemctl", "--user", "is-enabled", legacy])
legacy_props = properties(legacy, ["LoadState", "ActiveState", "SubState"])
legacy_absent = bool(legacy_enabled.returncode != 0 and (
    legacy_props is None or (legacy_props.get("ActiveState") not in {{"active", "activating"}}
    and legacy_props.get("SubState") != "running")))

print(json.dumps({{
    "baseline_ready": baseline_ready,
    "boot_id": boot_id,
    "candidate_source_exact": exact_repo(shared, whole_tree=True),
    "checkout_exact": exact_repo(repo, whole_tree=False),
    "environment_exact": exact_environment(),
    "legacy_absent": legacy_absent,
    "service_timer_exact": service_timer_exact,
    "units_exact": exact_units(),
}}, sort_keys=True, separators=(",", ":")))"""


def _remote_apply_command(
    target: GB10TransportTarget,
    plan: FinalGatePlan,
    operations: tuple[GB10MutationKind, ...],
) -> str:
    return "python3 -c " + shlex.quote(_remote_apply_source(target, plan, operations))


def _remote_apply_source(
    target: GB10TransportTarget,
    plan: FinalGatePlan,
    operations: tuple[GB10MutationKind, ...],
) -> str:
    image_tag = f"staging-{plan.candidate_sha[:7]}"
    shared = _SHARED_ROOT / f"loom-remote-worker-{image_tag}"
    service = target.node_agent_service
    timer = target.timer
    unit_paths = tuple(str(_UNIT_ROOT / unit) for unit in (service, timer))
    operation_values = tuple(operation.value for operation in operations)
    expected_boot_id = plan.gb10_boot_ids[target.ssh_target]
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
service = {service!r}
timer = {timer!r}
legacy = {_LEGACY_SERVICE!r}
unit_paths = {unit_paths!r}
operations = {operation_values!r}
expected_boot_id = {expected_boot_id!r}

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
                            timeout=20, env={{"HOME": str(pathlib.Path.home()),
                            "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
                            "PATH": "/usr/local/bin:/usr/bin:/bin",
                            "XDG_RUNTIME_DIR": "/run/user/" + str(os.getuid())}})
    if result.returncode != 0 or result.stderr:
        raise SystemExit(1)
    return result.stdout.strip()

def exact_shared_source():
    item = shared.lstat()
    if not stat.S_ISDIR(item.st_mode) or stat.S_ISLNK(item.st_mode):
        return False
    prefix = ["git", "-c", f"safe.directory={{shared}}", "-C", str(shared)]
    return (output([*prefix, "rev-parse", "HEAD"]) == candidate_sha
            and output([*prefix, "rev-parse", "HEAD^{{tree}}"]) == candidate_tree
            and output([*prefix, "status", "--porcelain=v1", "--untracked-files=all"]) == "")

def ensure_user_directory(path):
    home = pathlib.Path.home()
    relative = path.relative_to(home)
    cursor = home
    for part in relative.parts:
        cursor = cursor / part
        if os.path.lexists(cursor):
            item = cursor.lstat()
            if (not stat.S_ISDIR(item.st_mode) or stat.S_ISLNK(item.st_mode)
                    or item.st_uid != os.getuid()):
                raise SystemExit(1)
        else:
            os.mkdir(cursor, 0o700)

def read_regular(path):
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or before.st_size < 0 or before.st_size > 1024 * 1024):
            raise SystemExit(1)
        chunks = []; remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                raise SystemExit(1)
            chunks.append(chunk); remaining -= len(chunk)
        after = os.fstat(descriptor)
        if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
            raise SystemExit(1)
        return b"".join(chunks)
    finally:
        os.close(descriptor)

def atomic_write(path, payload, mode):
    if not path.parent.exists():
        ensure_user_directory(path.parent)
    parent = path.parent.lstat()
    if not stat.S_ISDIR(parent.st_mode) or stat.S_ISLNK(parent.st_mode):
        raise SystemExit(1)
    if os.path.lexists(path) and stat.S_ISLNK(path.lstat().st_mode):
        raise SystemExit(1)
    fd, temporary = tempfile.mkstemp(prefix=".loom-rollout-", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb", closefd=True) as stream:
            stream.write(payload); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        try: os.unlink(temporary)
        except FileNotFoundError: pass

if (pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
        != expected_boot_id or not exact_shared_source()
        or output(["loginctl", "show-user", str(os.getuid()),
                   "--property=Linger", "--value"]) != "yes"):
    raise SystemExit(1)

for operation in operations:
    if operation == "checkout":
        if not (repo / ".git").is_dir() or not shared.is_dir():
            raise SystemExit(1)
        run(["git", "-c", "protocol.file.allow=always", "-c", "fetch.fsckObjects=true",
             "-C", str(repo), "fetch", "--quiet", "--no-tags", "--no-recurse-submodules",
             "--no-write-fetch-head", str(shared), candidate_sha])
        run(["git", "-C", str(repo), "checkout", "--detach", candidate_sha])
    elif operation == "environment":
        existing = (read_regular(env_file).decode("utf-8").splitlines()
                    if os.path.lexists(env_file) else [])
        updates = {{key: image_tag for key in ("IMAGE_TAG", "ENV_CONFIG_VERSION",
            "LOOM_IMAGE_TAG", "LOOM_WORKER_ENV_CONFIG_VERSION")}}
        output = []; seen = set()
        for line in existing:
            if "=" not in line or line.lstrip().startswith("#"):
                output.append(line); continue
            key = line.split("=", 1)[0].strip()
            if key in updates:
                if key in seen: raise SystemExit(1)
                output.append(f"{{key}}={{updates[key]}}"); seen.add(key)
            else: output.append(line)
        output.extend(f"{{key}}={{value}}" for key, value in updates.items() if key not in seen)
        atomic_write(env_file, ("\\n".join(output) + "\\n").encode(), 0o600)
    elif operation == "units":
        for relative in unit_paths:
            source = repo / relative
            destination = pathlib.Path.home() / ".config/systemd/user" / pathlib.Path(relative).name
            item = source.lstat()
            if not stat.S_ISREG(item.st_mode) or stat.S_ISLNK(item.st_mode):
                raise SystemExit(1)
            atomic_write(destination, read_regular(source), 0o644)
    elif operation == "legacy-retire":
        enabled = subprocess.run(["systemctl", "--user", "is-enabled", legacy],
            check=False, capture_output=True, text=True, timeout=10)
        if enabled.returncode == 0:
            run(["systemctl", "--user", "disable", "--now", legacy])
        subprocess.run(["systemctl", "--user", "reset-failed", legacy],
            check=False, capture_output=True, text=True, timeout=10)
    elif operation == "service-timer":
        run(["systemctl", "--user", "daemon-reload"])
        run(["systemctl", "--user", "start", service])
        run(["systemctl", "--user", "enable", "--now", timer])
        run(["systemctl", "--user", "restart", timer])
    else:
        raise SystemExit(1)
"""


def _hash_json(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


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
        if not isinstance(raw, dict) or set(raw) - {
            "ssh_target",
            "repo_path",
            "env_file_path",
            "repo_url",
            "node_agent_service",
        }:
            raise ValueError("GB10 installed host authority is invalid")
        ssh_target = raw.get("ssh_target")
        repo_path = raw.get("repo_path")
        env_file_path = raw.get("env_file_path")
        service = raw.get("node_agent_service")
        if not all(
            isinstance(value, str) and value
            for value in (ssh_target, repo_path, env_file_path, service)
        ):
            raise ValueError("GB10 installed host fields are invalid")
        targets.append(
            GB10TransportTarget(
                ssh_target=str(ssh_target),
                repo_path=PurePosixPath(str(repo_path)),
                env_file_path=PurePosixPath(str(env_file_path)),
                node_agent_service=str(service),
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
    )


__all__ = [
    "FixedGB10SSHTransport",
    "GB10TransportTarget",
    "build_fixed_gb10_ssh_transport",
]
