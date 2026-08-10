"""Fixed SSH transport for GB10 shared-candidate and legacy-agent retirement.

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
_SHARED_ROOT = PurePosixPath("/shared_work2/loom-staging-rollout/worker-repos")
_LEGACY_SERVICE = "loom-gb10-worker.service"
_FIXED_NODE_AGENT_SERVICE = "loom-gb10-node-agent.service"
# Shared candidate Git reads traverse live NFS. Keep ordinary remote probes at
# ten seconds, but give exact shared-source reads a bounded tail-latency budget.
_REMOTE_SHARED_GIT_TIMEOUT_SECONDS = 30
# Installed transports retry transient single-bastion SSH failures. Six attempts
# with a 2s pause tolerate the connection storms the fleet observe competes with
# (the 30s autoscaler cadence) while keeping the total per-host budget bounded.
_INSTALLED_OBSERVE_ATTEMPTS = 6
_INSTALLED_OBSERVE_INTERVAL_SECONDS = 2.0
_FIXED_IDENTITY = Path("/var/lib/loom-staging-rollout/gb10-deploy-ed25519")
_MAX_OUTPUT_BYTES = 64 * 1024
_MUTATION_ORDER = (
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
    """One fixed host whose predecessor direct agents must remain retired."""

    ssh_target: str
    node_agent_service: str

    def __post_init__(self) -> None:
        if (
            _HOST_RE.fullmatch(self.ssh_target) is None
            or _SERVICE_RE.fullmatch(self.node_agent_service) is None
            or self.node_agent_service != _FIXED_NODE_AGENT_SERVICE
        ):
            raise ValueError("GB10 transport target is outside fixed authority")

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


def _remote_observation_source(target: GB10TransportTarget, plan: FinalGatePlan) -> str:
    image_tag = f"staging-{plan.candidate_sha[:7]}"
    shared = _SHARED_ROOT / f"loom-remote-worker-{image_tag}"
    service = target.node_agent_service
    timer = target.timer
    return f"""import json
import os
import pathlib
import stat
import subprocess

candidate_sha = {plan.candidate_sha!r}
candidate_tree = {plan.candidate_tree!r}
shared = pathlib.Path({str(shared)!r})
service = {service!r}
timer = {timer!r}
legacy = {_LEGACY_SERVICE!r}

def run(argv, *, cwd=None, timeout_seconds=10):
    return subprocess.run(argv, cwd=cwd, check=False, capture_output=True, text=True,
                          timeout=timeout_seconds,
                          env={{"HOME": str(pathlib.Path.home()), "LANG": "C.UTF-8",
                          "LC_ALL": "C.UTF-8", "PATH": "/usr/local/bin:/usr/bin:/bin",
                          "XDG_RUNTIME_DIR": "/run/user/" + str(os.getuid())}})

def git(root, *args):
    result = run(["git", "-c", f"safe.directory={{root}}", "-C", str(root), *args],
                 timeout_seconds={_REMOTE_SHARED_GIT_TIMEOUT_SECONDS})
    return result.stdout.strip() if result.returncode == 0 and not result.stderr else None

def plain_directory(path):
    try:
        item = path.lstat()
    except OSError:
        return False
    return stat.S_ISDIR(item.st_mode) and not stat.S_ISLNK(item.st_mode)

def exact_repo(root):
    if not plain_directory(root):
        return False
    if git(root, "rev-parse", "HEAD") != candidate_sha:
        return False
    if git(root, "rev-parse", "HEAD^{{tree}}") != candidate_tree:
        return False
    return git(root, "status", "--porcelain=v1", "--untracked-files=all") == ""

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
service_props = properties(service, ["LoadState", "ActiveState", "SubState"])
timer_props = properties(timer, ["LoadState", "ActiveState", "SubState"])
timer_enabled = run(["systemctl", "--user", "is-enabled", timer])

def retired(unit, props, enabled):
    return bool(enabled.returncode != 0 and (props is None or (
        props.get("ActiveState") not in {{"active", "activating", "reloading"}}
        and props.get("SubState") not in {{"running", "start", "start-pre", "start-post"}})))

service_enabled = run(["systemctl", "--user", "is-enabled", service])
service_retired = retired(service, service_props, service_enabled)
timer_retired = retired(timer, timer_props, timer_enabled)
service_timer_exact = bool(service_retired and timer_retired)
service_timer_transient = False
legacy_enabled = run(["systemctl", "--user", "is-enabled", legacy])
legacy_props = properties(legacy, ["LoadState", "ActiveState", "SubState"])
legacy_absent = retired(legacy, legacy_props, legacy_enabled)

print(json.dumps({{
    "baseline_ready": baseline_ready,
    "boot_id": boot_id,
    "candidate_source_exact": exact_repo(shared),
    "checkout_exact": True,
    "environment_exact": True,
    "legacy_absent": legacy_absent,
    "service_timer_exact": service_timer_exact,
    "service_timer_transient": service_timer_transient,
    "units_exact": True,
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
    operation_values = tuple(operation.value for operation in operations)
    expected_boot_id = plan.gb10_boot_ids[target.ssh_target]
    return f"""import os
import pathlib
import stat
import subprocess

candidate_sha = {plan.candidate_sha!r}
candidate_tree = {plan.candidate_tree!r}
shared = pathlib.Path({str(shared)!r})
service = {service!r}
timer = {timer!r}
legacy = {_LEGACY_SERVICE!r}
operations = {operation_values!r}
expected_boot_id = {expected_boot_id!r}

def run(argv, *, cwd=None, accept_missing=False):
    result = subprocess.run(argv, cwd=cwd, check=False, capture_output=True, text=True,
                            timeout=120, env={{"HOME": str(pathlib.Path.home()),
                            "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
                            "PATH": "/usr/local/bin:/usr/bin:/bin",
                            "XDG_RUNTIME_DIR": "/run/user/" + str(os.getuid())}})
    if result.returncode != 0 and not (accept_missing and result.returncode in {{1, 5}}):
        raise SystemExit(1)

def output(argv, *, timeout_seconds=20):
    result = subprocess.run(argv, check=False, capture_output=True, text=True,
                            timeout=timeout_seconds,
                            env={{"HOME": str(pathlib.Path.home()),
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
    return (output([*prefix, "rev-parse", "HEAD"],
                   timeout_seconds={_REMOTE_SHARED_GIT_TIMEOUT_SECONDS}) == candidate_sha
            and output([*prefix, "rev-parse", "HEAD^{{tree}}"],
                       timeout_seconds={_REMOTE_SHARED_GIT_TIMEOUT_SECONDS}) == candidate_tree
            and output([*prefix, "status", "--porcelain=v1", "--untracked-files=all"],
                       timeout_seconds={_REMOTE_SHARED_GIT_TIMEOUT_SECONDS}) == "")

if (pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    != expected_boot_id or not exact_shared_source()
    or output(["loginctl", "show-user", str(os.getuid()),
                   "--property=Linger", "--value"]) != "yes"):
    raise SystemExit(1)
if any(operation not in {{"legacy-retire", "service-timer"}} for operation in operations):
    raise SystemExit(1)

def retire(unit):
    run(["systemctl", "--user", "disable", "--now", unit], accept_missing=True)
    run(["systemctl", "--user", "reset-failed", unit], accept_missing=True)

for operation in operations:
    if operation == "legacy-retire":
        retire(legacy)
    elif operation == "service-timer":
        retire(timer)
        retire(service)
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
        if not isinstance(raw, dict) or set(raw) != {
            "ssh_target",
            "node_agent_service",
        }:
            raise ValueError("GB10 installed host authority is invalid")
        ssh_target = raw.get("ssh_target")
        service = raw.get("node_agent_service")
        if not all(
            isinstance(value, str) and value
            for value in (ssh_target, service)
        ):
            raise ValueError("GB10 installed host fields are invalid")
        targets.append(
            GB10TransportTarget(
                ssh_target=str(ssh_target),
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
        settle_attempts=_INSTALLED_OBSERVE_ATTEMPTS,
        settle_interval_seconds=_INSTALLED_OBSERVE_INTERVAL_SECONDS,
    )


__all__ = [
    "FixedGB10SSHTransport",
    "GB10FleetApplyError",
    "GB10TransportTarget",
    "build_fixed_gb10_ssh_transport",
]
