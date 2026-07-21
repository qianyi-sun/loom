"""Concrete fixed-command executor for isolated exact-candidate rehearsal."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.gb10_rehearsal import (
    FixedGB10RehearsalTransport,
    GB10RehearsalAuthority,
    GB10RehearsalEvidence,
)
from loom_cli.rollout.image_readiness import REHEARSAL_POSTGRES_IMAGE
from loom_cli.rollout.preflight_credential_paths import REHEARSAL_KUBECONFIG_PATH
from loom_cli.rollout.production_defaults_readiness import ProductionDefaultsArtifact
from loom_cli.rollout.rehearsal_action_source import RehearsalPlan
from loom_cli.rollout.rehearsal_browser import (
    BROWSER_INGRESS_NAME,
    BROWSER_JOB_NAME,
    BROWSER_NETWORK_POLICY_NAME,
    INGRESS_CONTROLLER_NAMESPACE,
    INGRESS_CONTROLLER_SERVICE,
    RehearsalBrowserArtifact,
    build_rehearsal_browser_artifact,
    ingress_controller_ip,
    rehearsal_browser_job_complete,
    rehearsal_browser_pod_complete,
    rehearsal_browser_report_ready,
    rehearsal_browser_resource_ready,
)
from loom_cli.rollout.rehearsal_journal_backend import RehearsalStepOutcome
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS
from loom_cli.rollout.rehearsal_release import (
    RehearsalReleaseArtifact,
    build_rehearsal_release_artifact,
    rehearsal_deployment_ready,
    rehearsal_network_policy_ready,
    rehearsal_pods_ready,
    rehearsal_selector_argument,
    rehearsal_service_ready,
)
from loom_cli.rollout.rehearsal_secret_restore import (
    RehearsalSecretArtifact,
    build_rehearsal_secret_artifact,
)
from loom_cli.rollout.systemd_readiness import (
    RehearsalSystemdActivation,
    parse_systemctl_properties,
)

REHEARSAL_KUBECONFIG = REHEARSAL_KUBECONFIG_PATH
_MAX_OUTPUT_BYTES = 1024 * 1024
_MAX_PRODUCTION_DEFAULTS_BYTES = 1024 * 1024
_KUBERNETES_UID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z"
)
_RESOURCE_VERSION_RE = re.compile(r"[1-9][0-9]{0,31}\Z")
_REHEARSAL_DUMP_PATH = "/var/lib/postgresql/data/loom-rehearsal.dump"
_REHEARSAL_DUMP_TRANSFER_TIMEOUT = 180
_REHEARSAL_DUMP_DIGEST_TIMEOUT = 120
_REHEARSAL_DUMP_RESTORE_TIMEOUT = 1470
_API_SMOKE_REQUEST_IDS = frozenset(
    {
        "batch-readback",
        "batch-submit",
        "batches-list",
        "benchmarks",
        "health",
        "probe",
        "task",
        "whoami",
    }
)
_API_SMOKE_REASON_CODES = frozenset(
    {
        "agent-task-incompatible",
        "empty-filter",
        "generic-http-response",
        "invalid-family-run",
        "invalid-task-config",
        "no-active-worker",
        "probe-failed",
    }
)


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...


CommandRunner = Callable[[Sequence[str], bytes | None, int], CommandResult]
StreamCommandRunner = Callable[[Sequence[str], Path, int], CommandResult]
ReleaseArtifactBuilder = Callable[[RehearsalPlan], RehearsalReleaseArtifact]
SecretArtifactBuilder = Callable[[RehearsalPlan], RehearsalSecretArtifact]
BrowserArtifactBuilder = Callable[[RehearsalPlan, str], RehearsalBrowserArtifact]
RuntimeImageResolver = Callable[[RehearsalPlan, Sequence[str]], Mapping[str, Sequence[str]] | None]


class GB10RehearsalTransport(Protocol):
    def execute(self, contract: RehearsalSystemdActivation) -> GB10RehearsalEvidence: ...

    def cleanup(self, contract: RehearsalSystemdActivation) -> GB10RehearsalEvidence: ...


GB10TransportFactory = Callable[[GB10RehearsalAuthority], GB10RehearsalTransport]


def _command_environment() -> dict[str, str]:
    service_uid = os.geteuid()
    return {
        "DBUS_SESSION_BUS_ADDRESS": f"unix:path=/run/user/{service_uid}/bus",
        "HOME": "/var/lib/loom-staging-rollout",
        "KUBECONFIG": str(REHEARSAL_KUBECONFIG),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "LOGNAME": "loom-rollout",
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "USER": "loom-rollout",
        "XDG_RUNTIME_DIR": f"/run/user/{service_uid}",
    }


def _default_run(
    argv: Sequence[str],
    payload: bytes | None,
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        input=None if payload is None else payload.decode("utf-8"),
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
        env=_command_environment(),
    )


def _default_stream_run(
    argv: Sequence[str],
    source: Path,
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    fd = _open_absolute_regular_no_follow(source)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_size <= 0
        ):
            raise RuntimeError("rehearsal stream source authority is invalid")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            result = subprocess.run(
                list(argv),
                stdin=stream,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=timeout,
                env=_command_environment(),
            )
        after = os.fstat(fd)
        stable_fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_gid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
            raise RuntimeError("rehearsal stream source changed while it was read")
        return subprocess.CompletedProcess(argv, result.returncode, "", "")
    finally:
        os.close(fd)


def _default_release_artifact(plan: RehearsalPlan) -> RehearsalReleaseArtifact:
    return build_rehearsal_release_artifact(plan)


def _default_secret_artifact(plan: RehearsalPlan) -> RehearsalSecretArtifact:
    return build_rehearsal_secret_artifact(
        plan.checkpoint_manifest_path,
        manifest_sha256=plan.checkpoint_manifest_sha256,
        namespace=plan.resources.namespace,
        database=plan.resources.database,
        plan_digest=plan.plan_digest,
    )


def _default_browser_artifact(plan: RehearsalPlan, ingress_ip: str) -> RehearsalBrowserArtifact:
    return build_rehearsal_browser_artifact(plan, ingress_ip=ingress_ip)


def _open_absolute_regular_no_follow(path: Path) -> int:
    if not path.is_absolute() or ".." in path.parts or path == Path("/"):
        raise OSError("rehearsal stream source path is invalid")
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    parent_fd = os.open("/", directory_flags)
    try:
        parts = path.parts[1:]
        for component in parts[:-1]:
            next_fd = os.open(component, directory_flags, dir_fd=parent_fd)
            os.close(parent_fd)
            parent_fd = next_fd
        return os.open(parts[-1], file_flags, dir_fd=parent_fd)
    finally:
        os.close(parent_fd)


@dataclass(frozen=True, slots=True)
class IsolatedRehearsalExecutor:
    """Run only allowlisted operations against rehearsal-scoped authority."""

    run: CommandRunner = _default_run
    stream_run: StreamCommandRunner = _default_stream_run
    release_artifacts: ReleaseArtifactBuilder = _default_release_artifact
    secret_artifacts: SecretArtifactBuilder = _default_secret_artifact
    browser_artifacts: BrowserArtifactBuilder = _default_browser_artifact
    kubeconfig: Path = REHEARSAL_KUBECONFIG
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    gb10_transport_factory: GB10TransportFactory | None = None
    runtime_image_resolver: RuntimeImageResolver | None = None

    def __post_init__(self) -> None:
        if not self.kubeconfig.is_absolute() or ".." in self.kubeconfig.parts:
            raise ValueError("rehearsal executor kubeconfig authority is invalid")

    def execute(self, check_id: str, plan: RehearsalPlan) -> RehearsalStepOutcome:
        if check_id not in REHEARSAL_CHECK_IDS:
            raise ValueError("rehearsal executor check identity is invalid")
        plan.resources.require_isolated()
        if check_id == "rehearsal.namespace":
            return self._namespace(plan)
        if check_id == "rehearsal.db-clone":
            return self._database(plan)
        if check_id == "rehearsal.systemd-launch":
            return self._systemd_launch(plan)
        if check_id == "rehearsal.migration":
            return self._migration(plan)
        if check_id == "rehearsal.release":
            return self._release(plan)
        if check_id == "rehearsal.production-defaults":
            return self._production_defaults(plan)
        if check_id == "rehearsal.api-smoke":
            return self._api_smoke(plan)
        if check_id == "rehearsal.browser":
            return self._browser(plan)
        if check_id == "rehearsal.cleanup":
            return self._cleanup(plan)
        return RehearsalStepOutcome(
            passed=False,
            details={"status": "blocked"},
            blockers={"executor": "isolated-action-not-implemented"},
        )

    def _browser(self, plan: RehearsalPlan) -> RehearsalStepOutcome:
        controller = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                INGRESS_CONTROLLER_NAMESPACE,
                "get",
                "service",
                INGRESS_CONTROLLER_SERVICE,
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        ingress_ip = ingress_controller_ip(controller) if controller is not None else None
        if ingress_ip is None:
            return _blocked("browser", "ingress-controller-readback-failed")
        try:
            artifact = self.browser_artifacts(plan, ingress_ip)
        except (OSError, RuntimeError, ValueError):
            return _blocked("browser", "artifact-validation-failed")
        image_names = ("loom-staging-admin-browser-smoke",)
        if not self._load_images(plan, image_names):
            return _blocked("browser", "image-load-failed")
        runtime_images = self._runtime_image_ids(plan, image_names)
        if runtime_images is None:
            return _blocked("browser", "runtime-image-binding-failed")
        if not self._status_with_payload(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "apply",
                "--server-side=true",
                "--field-manager=loom-staging-preflight",
                "--request-timeout=30s",
                "-f",
                "-",
            ),
            artifact.payload,
            timeout=60,
        ):
            return _blocked("browser", "manifest-apply-failed")
        for resource, name, kind in (
            ("ingress", BROWSER_INGRESS_NAME, "Ingress"),
            ("networkpolicy", BROWSER_NETWORK_POLICY_NAME, "NetworkPolicy"),
            ("job", BROWSER_JOB_NAME, "Job"),
        ):
            observed = self._command(
                (
                    "kubectl",
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "--namespace",
                    plan.resources.namespace,
                    "get",
                    resource,
                    name,
                    "--request-timeout=15s",
                    "-o",
                    "json",
                ),
                None,
                timeout=20,
            )
            if observed is None or not rehearsal_browser_resource_ready(
                observed,
                artifact=artifact,
                plan=plan,
                kind=kind,
                name=name,
            ):
                return _blocked("browser", f"{resource}-readback-drift")
        if not self._status(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "wait",
                "--for=condition=Complete",
                f"job/{BROWSER_JOB_NAME}",
                "--timeout=900s",
            ),
            timeout=915,
        ):
            return _blocked("browser", "job-not-complete")
        job = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "get",
                "job",
                BROWSER_JOB_NAME,
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        if job is None or not rehearsal_browser_job_complete(
            job,
            artifact=artifact,
            plan=plan,
        ):
            return _blocked("browser", "job-readback-drift")
        pods = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "get",
                "pods",
                f"--selector=job-name={BROWSER_JOB_NAME}",
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        if pods is None or not rehearsal_browser_pod_complete(
            pods,
            artifact=artifact,
            plan=plan,
            runtime_image_digests=runtime_images["loom-staging-admin-browser-smoke"],
        ):
            return _blocked("browser", "pod-image-readback-drift")
        report = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "logs",
                f"job/{BROWSER_JOB_NAME}",
                "--container=browser",
                "--request-timeout=30s",
            ),
            None,
            timeout=45,
        )
        if report is None or not rehearsal_browser_report_ready(report, plan=plan):
            return _blocked("browser", "report-validation-failed")
        return RehearsalStepOutcome(
            passed=True,
            details={
                "browser-report-sha256": hashlib.sha256(
                    json.dumps(report, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
                "manifest-sha256": artifact.artifact_sha256,
                "status": "ready",
            },
            blockers={},
        )

    def _production_defaults(self, plan: RehearsalPlan) -> RehearsalStepOutcome:
        try:
            release = self.release_artifacts(plan)
            trusted = read_trusted_file(
                plan.production_defaults_path,
                service_uid=os.geteuid(),
                private=True,
                max_bytes=_MAX_PRODUCTION_DEFAULTS_BYTES,
                require_nonempty=True,
            )
            artifact = ProductionDefaultsArtifact.from_bytes(trusted.payload)
        except (OSError, RuntimeError, ValueError):
            return _blocked("production-defaults", "artifact-validation-failed")
        if (
            artifact.artifact_digest != plan.production_defaults_sha256
            or artifact.candidate_sha != plan.candidate_sha
            or artifact.candidate_tree != plan.candidate_tree
            or artifact.environment != "staging"
        ):
            return _blocked("production-defaults", "artifact-binding-drift")
        pod_name = self._service_pod_name(plan, release)
        if pod_name is None:
            return _blocked("production-defaults", "service-pod-readback-drift")
        result = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "exec",
                "-i",
                f"pod/{pod_name}",
                "--container",
                "loom-service",
                "--",
                "python",
                "-m",
                "loom_cli.rollout.rehearsal_production_defaults_probe",
                "--plan-sha256",
                plan.plan_digest,
                "--artifact-sha256",
                plan.production_defaults_sha256,
                "--candidate-sha",
                plan.candidate_sha,
                "--candidate-tree",
                plan.candidate_tree,
            ),
            trusted.payload,
            timeout=300,
        )
        if result is None or not _production_defaults_result_ready(result, plan=plan):
            return _blocked("production-defaults", "probe-failed")
        return RehearsalStepOutcome(
            passed=True,
            details={
                "artifact-sha256": plan.production_defaults_sha256,
                "evidence-sha256": str(result["evidence_sha256"]),
                "mutation-count": str(result["mutation_count"]),
                "status": "ready",
            },
            blockers={},
        )

    def _api_smoke(self, plan: RehearsalPlan) -> RehearsalStepOutcome:
        try:
            release = self.release_artifacts(plan)
        except (OSError, RuntimeError, ValueError):
            return _blocked("api-smoke", "artifact-validation-failed")
        pod_name = self._service_pod_name(plan, release)
        if pod_name is None:
            return _blocked("api-smoke", "service-pod-readback-drift")
        authority = plan.smoke_authority
        required_pool = authority.required_worker_pool
        if required_pool is None:
            return _blocked("api-smoke", "worker-pool-authority-missing")
        worker_authority = self._seed_api_smoke_worker(plan, required_pool=required_pool)
        if worker_authority is None:
            return _blocked("api-smoke", "worker-authority-failed")
        suffix = plan.resources.namespace.removeprefix("loom-rehearsal-")
        batch_name = f"rehearsal-{suffix}"
        probe = self._json_command_result(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "exec",
                f"pod/{pod_name}",
                "--container",
                "loom-service",
                "--",
                "python",
                "-m",
                "loom_cli.rollout.rehearsal_smoke_probe",
                "--plan-sha256",
                plan.plan_digest,
                "--batch-name",
                batch_name,
                "--represented-username",
                authority.represented_username,
                "--team-id",
                authority.team_id,
                "--admin-actor",
                authority.admin_actor,
                "--task-id",
                authority.task_id,
                "--required-worker-pool",
                required_pool,
                "--agent",
                authority.agent,
            ),
            None,
            timeout=120,
        )
        if probe is None:
            return _blocked("api-smoke", "probe-failed")
        returncode, result = probe
        if returncode != 0:
            failure = _api_smoke_failure(result)
            if failure is None:
                return _blocked("api-smoke", "probe-failed")
            failure_code, request_id, reason_code, response_sha256 = failure
            blockers = {
                "api-smoke": (
                    failure_code.removeprefix("rehearsal-api-smoke-")
                    + f".{request_id}.{reason_code}"
                )
            }
            details = {
                "failure-code": failure_code,
                "reason-code": reason_code,
                "request-id": request_id,
                "status": "blocked",
            }
            if response_sha256 is not None:
                details["response-sha256"] = response_sha256
                blockers["api-smoke-response-sha256"] = response_sha256
            return RehearsalStepOutcome(
                passed=False,
                details=details,
                blockers=blockers,
            )
        if not _api_smoke_result_ready(
            result,
            plan=plan,
            batch_name=batch_name,
        ):
            return _blocked("api-smoke", "probe-failed")
        batch_id = result["batch_id"]
        assert isinstance(batch_id, str)
        evidence = result["evidence"]
        assert isinstance(evidence, dict)
        return RehearsalStepOutcome(
            passed=True,
            details={
                "batch-id": batch_id,
                "evidence-sha256": hashlib.sha256(
                    json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
                "status": "ready",
                "worker-authority-sha256": hashlib.sha256(
                    json.dumps(
                        worker_authority,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                ).hexdigest(),
            },
            blockers={},
        )

    def _seed_api_smoke_worker(
        self,
        plan: RehearsalPlan,
        *,
        required_pool: str,
    ) -> dict[str, object] | None:
        """Publish one deterministic worker only inside the restored clone.

        A PostgreSQL dump freezes worker heartbeat timestamps. Without this
        isolated authority, the candidate service correctly rejects every
        rehearsal submission as having no fresh worker even though GB10 host
        readiness was independently proven by the preceding systemd check.
        The exact synthetic row enables only the Docker admission predicate;
        it never registers with or reaches protected staging.
        """
        worker_id = str(uuid.UUID(hex=plan.plan_digest[:32], version=4))
        hostname = "rehearsal-" + plan.resources.namespace.removeprefix("loom-rehearsal-")
        version = "candidate-" + plan.candidate_sha[:12]
        capabilities = '[{"backend":"docker","rehearsal":true}]'
        sql = (
            "WITH upserted AS ("
            "INSERT INTO workers "
            "(id,hostname,version,capabilities,max_concurrent,pool_name,"
            "drain_state,registered_at,last_seen_at,status) VALUES ("
            f"'{worker_id}'::uuid,'{hostname}','{version}',"
            f"'{capabilities}'::jsonb,1,'{required_pool}','active',now(),now(),'active') "
            "ON CONFLICT (id) DO UPDATE SET last_seen_at=EXCLUDED.last_seen_at "
            "WHERE workers.hostname=EXCLUDED.hostname "
            "AND workers.version=EXCLUDED.version "
            "AND workers.capabilities=EXCLUDED.capabilities "
            "AND workers.max_concurrent=EXCLUDED.max_concurrent "
            "AND workers.pool_name=EXCLUDED.pool_name "
            "AND workers.drain_state='active' AND workers.status='active' "
            "RETURNING id,hostname,version,capabilities,pool_name,"
            "drain_state,last_seen_at,status) "
            "SELECT json_build_object("
            "'backend','docker','fresh',last_seen_at >= now()-interval '30 seconds',"
            "'hostname',hostname,'pool_name',pool_name,'status','ready',"
            "'worker_id',id::text)::text FROM upserted;"
        )
        observed = self._psql_json(plan, sql)
        expected: dict[str, object] = {
            "backend": "docker",
            "fresh": True,
            "hostname": hostname,
            "pool_name": required_pool,
            "status": "ready",
            "worker_id": worker_id,
        }
        return expected if observed == expected else None

    def _service_pod_name(
        self,
        plan: RehearsalPlan,
        release: RehearsalReleaseArtifact,
    ) -> str | None:
        pods = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "get",
                "pods",
                "--selector=" + rehearsal_selector_argument(release, "loom-service"),
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        runtime_images = self._runtime_image_ids(plan, ("loom-service",))
        if runtime_images is None:
            return None
        return _exact_service_pod_name(
            pods,
            release=release,
            plan=plan,
            runtime_image_digests=runtime_images["loom-service"],
        )

    def _namespace(self, plan: RehearsalPlan) -> RehearsalStepOutcome:
        manifest = _namespace_manifest(plan)
        apply = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "apply",
                "--server-side=true",
                "--field-manager=loom-staging-preflight",
                "--request-timeout=30s",
                "-f",
                "-",
                "-o",
                "json",
            ),
            _json_bytes(manifest),
            timeout=45,
        )
        if apply is None or not _namespace_matches(apply, plan):
            return _blocked("namespace", "apply-failed")
        network_policy_manifest = _default_deny_network_policy_manifest(plan)
        network_policy = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "apply",
                "--server-side=true",
                "--field-manager=loom-staging-preflight",
                "--request-timeout=30s",
                "-f",
                "-",
                "-o",
                "json",
            ),
            _json_bytes(network_policy_manifest),
            timeout=45,
        )
        if network_policy is None or not _default_deny_network_policy_matches(network_policy, plan):
            return _blocked("namespace", "network-policy-failed")
        binding_manifest = _observer_binding_manifest(plan)
        binding = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "apply",
                "--server-side=true",
                "--field-manager=loom-staging-preflight",
                "--request-timeout=30s",
                "-f",
                "-",
                "-o",
                "json",
            ),
            _json_bytes(binding_manifest),
            timeout=45,
        )
        if binding is None or not _observer_binding_matches(binding, plan):
            return _blocked("namespace", "observer-binding-failed")
        observed = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "get",
                "namespace",
                plan.resources.namespace,
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        if observed is None or not _namespace_matches(observed, plan):
            return _blocked("namespace", "readback-drift")
        observed_network_policy = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "get",
                "networkpolicy",
                "loom-rehearsal-default-deny",
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        if observed_network_policy is None or not _default_deny_network_policy_matches(
            observed_network_policy, plan
        ):
            return _blocked("namespace", "network-policy-readback-drift")
        observed_binding = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "get",
                "rolebinding",
                "loom-rollout-rehearsal-observer",
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        if observed_binding is None or not _observer_binding_matches(observed_binding, plan):
            return _blocked("namespace", "observer-readback-drift")
        return RehearsalStepOutcome(
            passed=True,
            details={"namespace": plan.resources.namespace, "status": "ready"},
            blockers={},
        )

    def _database(self, plan: RehearsalPlan) -> RehearsalStepOutcome:
        if any(
            plan.image_digests.get(name) is None
            for name in (REHEARSAL_POSTGRES_IMAGE, "loom-control-plane")
        ):
            return _blocked("database", "image-authority-missing")
        image_names = (REHEARSAL_POSTGRES_IMAGE, "loom-control-plane")
        if not self._load_images(plan, image_names):
            return _blocked("database", "image-load-failed")
        runtime_images = self._runtime_image_ids(plan, image_names)
        if runtime_images is None:
            return _blocked("database", "runtime-image-binding-failed")
        manifest = _database_pod_manifest(plan)
        applied = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "apply",
                "--server-side=true",
                "--field-manager=loom-staging-preflight",
                "--request-timeout=30s",
                "-f",
                "-",
                "-o",
                "json",
            ),
            _json_bytes(manifest),
            timeout=45,
        )
        if applied is None or not _database_pod_matches(
            applied,
            plan,
            require_ready=False,
            runtime_image_digests=runtime_images,
        ):
            return _blocked("database", "pod-apply-failed")
        if not self._status(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "wait",
                "--for=condition=Ready",
                "pod/loom-rehearsal-db",
                "--timeout=180s",
            ),
            timeout=195,
        ):
            return _blocked("database", "pod-not-ready")
        observed = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "get",
                "pod",
                "loom-rehearsal-db",
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        if observed is None or not _database_pod_matches(
            observed,
            plan,
            require_ready=True,
            runtime_image_digests=runtime_images,
        ):
            return _blocked("database", "pod-readback-drift")
        existing = self._database_identity(plan)
        if existing is not None and existing.get("restored") is True:
            if existing.get("schema_revision") == plan.schema_revision:
                return _database_ready(plan)
            return _blocked("database", "existing-restore-drift")
        dump_path = plan.checkpoint_manifest_path.parent / "postgres" / "loom.dump"
        transfer_argv = (
            "kubectl",
            "--kubeconfig",
            str(self.kubeconfig),
            "--namespace",
            plan.resources.namespace,
            "exec",
            "-i",
            "pod/loom-rehearsal-db",
            "--container",
            "postgres",
            "--",
            "tee",
            "--",
            _REHEARSAL_DUMP_PATH,
        )
        try:
            transferred = self.stream_run(
                transfer_argv,
                dump_path,
                _REHEARSAL_DUMP_TRANSFER_TIMEOUT,
            )
        except (OSError, RuntimeError, subprocess.SubprocessError):
            if not self._remove_staged_dump(plan):
                return _blocked("database", "restore-staging-cleanup-failed")
            return _blocked("database", "restore-failed")
        if transferred.returncode != 0:
            if not self._remove_staged_dump(plan):
                return _blocked("database", "restore-staging-cleanup-failed")
            return _blocked("database", "restore-failed")
        staged_digest = self._text_command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "exec",
                "pod/loom-rehearsal-db",
                "--container",
                "postgres",
                "--",
                "sha256sum",
                "--",
                _REHEARSAL_DUMP_PATH,
            ),
            timeout=_REHEARSAL_DUMP_DIGEST_TIMEOUT,
            max_bytes=256,
        )
        expected_dump_digest = plan.db_snapshot_identity.removeprefix("pgdump-sha256:")
        if staged_digest is None or staged_digest.split() != [
            expected_dump_digest,
            _REHEARSAL_DUMP_PATH,
        ]:
            if not self._remove_staged_dump(plan):
                return _blocked("database", "restore-staging-cleanup-failed")
            return _blocked("database", "restore-staging-verification-failed")
        restored = self._status(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "exec",
                "pod/loom-rehearsal-db",
                "--container",
                "postgres",
                "--",
                "pg_restore",
                "--exit-on-error",
                "--jobs=4",
                "--no-owner",
                "--no-privileges",
                "--username=loom_rehearsal",
                f"--dbname={plan.resources.database}",
                _REHEARSAL_DUMP_PATH,
            ),
            timeout=_REHEARSAL_DUMP_RESTORE_TIMEOUT,
        )
        removed = self._remove_staged_dump(plan)
        if not removed:
            return _blocked("database", "restore-staging-cleanup-failed")
        if not restored:
            return _blocked("database", "restore-failed")
        identity = self._database_identity(plan)
        if (
            identity is None
            or identity.get("restored") is not True
            or identity.get("database") != plan.resources.database
            or identity.get("schema_revision") != plan.schema_revision
        ):
            return _blocked("database", "restore-verification-failed")
        return _database_ready(plan)

    def _remove_staged_dump(self, plan: RehearsalPlan) -> bool:
        return self._status(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "exec",
                "pod/loom-rehearsal-db",
                "--container",
                "postgres",
                "--",
                "rm",
                "-f",
                "--",
                _REHEARSAL_DUMP_PATH,
            ),
            timeout=30,
        )

    def _database_identity(self, plan: RehearsalPlan) -> dict[str, object] | None:
        identity = self._psql_json(
            plan,
            "SELECT json_build_object('database',current_database(),"
            "'restored',to_regclass('public.alembic_version') IS NOT NULL)::text;",
        )
        if identity is None or identity.get("restored") is not True:
            return identity
        revision = self._psql_json(
            plan,
            "SELECT json_build_object('schema_revision',version_num)::text "
            "FROM alembic_version LIMIT 1;",
        )
        if revision is None or not isinstance(revision.get("schema_revision"), str):
            return None
        return {**identity, "schema_revision": revision["schema_revision"]}

    def _migration(self, plan: RehearsalPlan) -> RehearsalStepOutcome:
        identity = self._database_identity(plan)
        if identity is None or identity.get("database") != plan.resources.database:
            return _blocked("migration", "database-unavailable")
        observed_revision = identity.get("schema_revision")
        if identity.get("restored") is not True or not isinstance(observed_revision, str):
            return _blocked("migration", "database-baseline-missing")
        if observed_revision == plan.migration_target_revision:
            return _migration_ready(plan)
        if observed_revision != plan.schema_revision:
            return _blocked("migration", "database-baseline-drift")
        db_url = "postgresql+psycopg://loom_rehearsal@127.0.0.1:5432/" + plan.resources.database
        migrated = self._status(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "exec",
                "pod/loom-rehearsal-db",
                "--container",
                "migration",
                "--",
                "env",
                f"LOOM_DB_URL={db_url}",
                "PYTHONDONTWRITEBYTECODE=1",
                "alembic",
                "-c",
                "migrations/alembic.ini",
                "upgrade",
                "head",
            ),
            timeout=900,
        )
        if not migrated:
            return _blocked("migration", "upgrade-failed")
        verified = self._database_identity(plan)
        if (
            verified is None
            or verified.get("database") != plan.resources.database
            or verified.get("restored") is not True
            or verified.get("schema_revision") != plan.migration_target_revision
        ):
            return _blocked("migration", "upgrade-verification-failed")
        return _migration_ready(plan)

    def _release(self, plan: RehearsalPlan) -> RehearsalStepOutcome:
        try:
            release = self.release_artifacts(plan)
            secrets = self.secret_artifacts(plan)
        except (OSError, RuntimeError, ValueError):
            return _blocked("release", "artifact-validation-failed")
        image_names = tuple(sorted(release.deployment_images))
        if not self._local_images_match(plan, image_names) or not self._load_images(
            plan,
            ("loom-service", "loom-web"),
        ):
            return _blocked("release", "image-load-failed")
        runtime_images = self._runtime_image_ids(plan, image_names)
        if runtime_images is None:
            return _blocked("release", "runtime-image-binding-failed")
        if not self._status_with_payload(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "apply",
                "--server-side=true",
                "--field-manager=loom-staging-preflight",
                "--request-timeout=30s",
                "-f",
                "-",
            ),
            secrets.payload,
            timeout=45,
        ):
            return _blocked("release", "secret-apply-failed")
        for name in secrets.secret_names:
            if self._secret_plan_digest(plan, name) != plan.plan_digest:
                return _blocked("release", "secret-readback-drift")
        if not self._status_with_payload(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "apply",
                "--server-side=true",
                "--field-manager=loom-staging-preflight",
                "--request-timeout=30s",
                "-f",
                "-",
            ),
            release.payload,
            timeout=60,
        ):
            return _blocked("release", "manifest-apply-failed")
        for name in sorted(release.deployment_images):
            if not self._status(
                (
                    "kubectl",
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "--namespace",
                    plan.resources.namespace,
                    "rollout",
                    "status",
                    f"deployment/{name}",
                    "--timeout=300s",
                ),
                timeout=315,
            ):
                return _blocked("release", "deployment-not-ready")
            deployment = self._command(
                (
                    "kubectl",
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "--namespace",
                    plan.resources.namespace,
                    "get",
                    "deployment",
                    name,
                    "--request-timeout=15s",
                    "-o",
                    "json",
                ),
                None,
                timeout=20,
            )
            if deployment is None or not rehearsal_deployment_ready(
                deployment,
                artifact=release,
                plan=plan,
                deployment_name=name,
            ):
                return _blocked("release", "deployment-readback-drift")
            pods = self._command(
                (
                    "kubectl",
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "--namespace",
                    plan.resources.namespace,
                    "get",
                    "pods",
                    "--selector=" + rehearsal_selector_argument(release, name),
                    "--request-timeout=15s",
                    "-o",
                    "json",
                ),
                None,
                timeout=20,
            )
            if pods is None or not rehearsal_pods_ready(
                pods,
                artifact=release,
                deployment_name=name,
                runtime_image_digests=runtime_images[name],
            ):
                return _blocked("release", "pod-image-readback-drift")
        for name in ("loom-control-plane", "loom-postgres", "loom-service", "loom-web"):
            service = self._command(
                (
                    "kubectl",
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "--namespace",
                    plan.resources.namespace,
                    "get",
                    "service",
                    name,
                    "--request-timeout=15s",
                    "-o",
                    "json",
                ),
                None,
                timeout=20,
            )
            if service is None or not rehearsal_service_ready(
                service,
                artifact=release,
                plan=plan,
                service_name=name,
            ):
                return _blocked("release", "service-readback-drift")
        policy = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "get",
                "networkpolicy",
                "loom-rehearsal-release",
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        if policy is None or not rehearsal_network_policy_ready(
            policy,
            artifact=release,
            plan=plan,
        ):
            return _blocked("release", "network-policy-readback-drift")
        return RehearsalStepOutcome(
            passed=True,
            details={
                "manifest-sha256": release.artifact_sha256,
                "secret-artifact-sha256": secrets.artifact_sha256,
                "status": "ready",
            },
            blockers={},
        )

    def _load_images(self, plan: RehearsalPlan, names: Sequence[str]) -> bool:
        tags = tuple(f"{name}:{plan.image_tag}" for name in names)
        if not tags or not self._local_images_match(plan, names):
            return False
        if not self._status(
            ("kind", "load", "docker-image", *tags, "--name", plan.cluster_name),
            timeout=900,
        ):
            return False
        return self._local_images_match(plan, names)

    def _local_images_match(self, plan: RehearsalPlan, names: Sequence[str]) -> bool:
        expected = tuple(plan.image_digests.get(name) for name in names)
        return bool(
            names
            and all(value is not None for value in expected)
            and tuple(self._local_image_id(f"{name}:{plan.image_tag}") for name in names)
            == expected
        )

    def _local_image_id(self, tag: str) -> str | None:
        value = self._text_command(
            ("docker", "image", "inspect", "--format={{.Id}}", tag),
            timeout=30,
            max_bytes=128,
        )
        if value is None:
            return None
        image_id = value.strip()
        return image_id if re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) else None

    def _runtime_image_ids(
        self,
        plan: RehearsalPlan,
        names: Sequence[str],
    ) -> dict[str, tuple[str, ...]] | None:
        if self.runtime_image_resolver is not None:
            resolved = self.runtime_image_resolver(plan, names)
            values = {} if resolved is None else dict(resolved)
            if (
                len(set(names)) != len(names)
                or set(values) != set(names)
                or any(
                    isinstance(value, str)
                    or not isinstance(value, Sequence)
                    or not 1 <= len(value) <= 3
                    or len(set(value)) != len(value)
                    or any(
                        not isinstance(digest, str)
                        or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None
                        for digest in value
                    )
                    for value in values.values()
                )
            ):
                return None
            return {name: tuple(values[name]) for name in names}
        result: dict[str, tuple[str, ...]] = {}
        for name in names:
            image_ids = self._kind_runtime_image_ids(plan, name)
            if image_ids is None:
                return None
            result[name] = image_ids
        return result

    def _kind_runtime_image_ids(
        self,
        plan: RehearsalPlan,
        name: str,
    ) -> tuple[str, ...] | None:
        expected = plan.image_digests.get(name)
        if expected is None:
            return None
        node = f"{plan.cluster_name}-control-plane"
        reference = f"docker.io/library/{name}:{plan.image_tag}"
        listing = self._text_command(
            (
                "docker",
                "exec",
                node,
                "ctr",
                "-n",
                "k8s.io",
                "images",
                "list",
                f"name=={reference}",
            ),
            timeout=30,
            max_bytes=4096,
        )
        lines = listing.splitlines() if listing is not None else []
        fields = lines[1].split(maxsplit=5) if len(lines) == 2 else []
        if len(fields) < 5 or fields[0] != reference or fields[2] != expected:
            return None
        descriptor = self._containerd_content(node, expected)
        if descriptor is None:
            return None
        media_type = descriptor.get("mediaType")
        manifest = descriptor
        manifest_digest = expected
        if media_type == "application/vnd.oci.image.index.v1+json":
            manifests = descriptor.get("manifests")
            matches = (
                [
                    item
                    for item in manifests
                    if isinstance(item, dict)
                    and item.get("mediaType") == "application/vnd.oci.image.manifest.v1+json"
                    and item.get("platform") == {"architecture": "amd64", "os": "linux"}
                ]
                if isinstance(manifests, list)
                else []
            )
            if len(matches) != 1:
                return None
            selected_digest = matches[0].get("digest")
            if (
                not isinstance(selected_digest, str)
                or re.fullmatch(r"sha256:[0-9a-f]{64}", selected_digest) is None
            ):
                return None
            manifest_digest = selected_digest
            resolved_manifest = self._containerd_content(node, manifest_digest)
            if resolved_manifest is None:
                return None
            manifest = resolved_manifest
        if manifest.get("mediaType") != "application/vnd.oci.image.manifest.v1+json":
            return None
        config = manifest.get("config")
        config_digest = config.get("digest") if isinstance(config, dict) else None
        if (
            not isinstance(config_digest, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", config_digest) is None
        ):
            return None
        inspected = self._text_command(
            ("docker", "exec", node, "crictl", "inspecti", reference),
            timeout=30,
            max_bytes=_MAX_OUTPUT_BYTES,
        )
        try:
            payload = json.loads(inspected) if inspected is not None else None
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        status = payload.get("status")
        info = payload.get("info")
        image_spec = info.get("imageSpec") if isinstance(info, dict) else None
        image_config = image_spec.get("config") if isinstance(image_spec, dict) else None
        labels = image_config.get("Labels") if isinstance(image_config, dict) else None
        repo_digests = status.get("repoDigests") if isinstance(status, dict) else None
        imported_digests = (
            self._validated_import_digests(
                node,
                repo_digests,
                expected=expected,
                expected_media_type=media_type,
                reference=reference,
                image_tag=plan.image_tag,
            )
            if isinstance(repo_digests, list)
            else None
        )
        if not (
            isinstance(status, dict)
            and status.get("id") == config_digest
            and isinstance(status.get("repoTags"), list)
            and reference in status["repoTags"]
            and imported_digests is not None
            and isinstance(image_spec, dict)
            and image_spec.get("architecture") == "amd64"
            and image_spec.get("os") == "linux"
            and isinstance(labels, dict)
            and labels.get("org.opencontainers.image.revision") == plan.candidate_sha
        ):
            return None
        return tuple(dict.fromkeys((config_digest, manifest_digest, *imported_digests)))

    def _validated_import_digests(
        self,
        node: str,
        repo_digests: Sequence[object],
        *,
        expected: str,
        expected_media_type: object,
        reference: str,
        image_tag: str,
    ) -> tuple[str, ...] | None:
        if len(repo_digests) > 1:
            return None
        validated: list[str] = []
        for repo_digest in repo_digests:
            if not isinstance(repo_digest, str) or repo_digest.count("@") != 1:
                return None
            repository, digest = repo_digest.rsplit("@", 1)
            if (
                re.fullmatch(r"docker\.io/library/import-[0-9]{4}-[0-9]{2}-[0-9]{2}", repository)
                is None
                or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None
            ):
                return None
            imported = self._containerd_content(node, digest)
            descriptors = imported.get("manifests") if imported is not None else None
            matches = (
                [
                    item
                    for item in descriptors
                    if isinstance(item, dict)
                    and item.get("digest") == expected
                    and item.get("mediaType") == expected_media_type
                    and item.get("annotations")
                    == {
                        "io.containerd.image.name": reference,
                        "org.opencontainers.image.ref.name": image_tag,
                    }
                ]
                if imported is not None
                and imported.get("mediaType") == "application/vnd.oci.image.index.v1+json"
                and isinstance(descriptors, list)
                else []
            )
            if len(matches) != 1:
                return None
            validated.append(digest)
        return tuple(validated)

    def _containerd_content(self, node: str, digest: str) -> dict[str, object] | None:
        if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
            return None
        value = self._text_command(
            ("docker", "exec", node, "ctr", "-n", "k8s.io", "content", "get", digest),
            timeout=30,
            max_bytes=_MAX_OUTPUT_BYTES,
        )
        try:
            payload = json.loads(value) if value is not None else None
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    def _secret_plan_digest(self, plan: RehearsalPlan, name: str) -> str | None:
        value = self._text_command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "get",
                "secret",
                name,
                "--request-timeout=15s",
                "-o=jsonpath={.metadata.name}{'\\t'}{.metadata.annotations.loom\\.openai\\.dev/plan-sha256}{'\\n'}",
            ),
            timeout=20,
            max_bytes=256,
        )
        if value is None:
            return None
        parts = value.rstrip("\n").split("\t")
        return parts[1] if parts == [name, plan.plan_digest] else None

    def _systemd_launch(self, plan: RehearsalPlan) -> RehearsalStepOutcome:
        contract = RehearsalSystemdActivation(
            unit=plan.resources.systemd_unit,
            plan_digest=plan.plan_digest,
        )
        existing = self._systemd_properties(contract)
        latency_ms = 0
        if existing is not None:
            if not contract.ready(existing, latency_ms=0) and not contract.absent(existing):
                return _blocked("systemd", "existing-unit-drift")
        if existing is None or contract.absent(existing):
            started_at = self.monotonic()
            if not self._status(contract.start_argv, timeout=30):
                return _blocked("systemd", "activation-failed")
            latency_ms = max(0, round((self.monotonic() - started_at) * 1000))
            observed = self._systemd_properties(contract)
            if observed is None or not contract.ready(observed, latency_ms=latency_ms):
                return _blocked("systemd", "activation-readback-drift")
        fleet = self._gb10_transport(plan).execute(contract)
        if fleet.blockers:
            return _gb10_blocked("systemd", fleet)
        return _systemd_ready(
            contract,
            latency_ms=latency_ms,
            gb10_evidence_digest=fleet.evidence_digest,
            gb10_host_count=len(fleet.host_boot_ids),
        )

    def _gb10_transport(self, plan: RehearsalPlan) -> GB10RehearsalTransport:
        if self.gb10_transport_factory is not None:
            return self.gb10_transport_factory(plan.gb10_authority)
        return FixedGB10RehearsalTransport(
            authority=plan.gb10_authority,
            service_uid=os.geteuid(),
            run=lambda argv, timeout: self.run(argv, None, timeout),
        )

    def _systemd_properties(
        self,
        contract: RehearsalSystemdActivation,
    ) -> dict[str, str] | None:
        try:
            result = self.run(contract.show_argv, None, 15)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            return None
        if (
            result.returncode != 0
            or not isinstance(result.stdout, str)
            or not isinstance(result.stderr, str)
            or len(result.stdout.encode()) > _MAX_OUTPUT_BYTES
            or len(result.stderr.encode()) > _MAX_OUTPUT_BYTES
        ):
            return None
        properties = parse_systemctl_properties(result.stdout)
        return properties or None

    def _cleanup(self, plan: RehearsalPlan) -> RehearsalStepOutcome:
        contract = RehearsalSystemdActivation(
            unit=plan.resources.systemd_unit,
            plan_digest=plan.plan_digest,
        )
        fleet = self._gb10_transport(plan).cleanup(contract)
        if fleet.blockers:
            return _gb10_blocked("cleanup", fleet)
        properties = self._systemd_properties(contract)
        if properties is not None and not contract.absent(properties):
            if not contract.ready(properties, latency_ms=0):
                return _blocked("cleanup", "systemd-identity-drift")
            if not self._status(contract.stop_argv, timeout=30):
                return _blocked("cleanup", "systemd-stop-failed")
            if not self._status(contract.reset_argv, timeout=30):
                # A successful stop may synchronously garbage-collect a
                # transient unit before reset-failed obtains its manager
                # reference. Accept that race only after the independent,
                # exact load-state probe proves the unit is already absent.
                if self._systemd_load_state(contract) != "not-found":
                    return _blocked("cleanup", "systemd-reset-failed")
        if not self._wait_systemd_absent(contract):
            return _blocked("cleanup", "systemd-remains")

        observed = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "get",
                "namespace",
                plan.resources.namespace,
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        if observed is None:
            return _cleanup_ready(plan)
        if not _namespace_matches(observed, plan):
            return _blocked("cleanup", "namespace-identity-drift")
        metadata = observed.get("metadata")
        if not isinstance(metadata, dict):  # covered by _namespace_matches
            return _blocked("cleanup", "namespace-identity-drift")
        uid = metadata.get("uid")
        resource_version = metadata.get("resourceVersion")
        if (
            not isinstance(uid, str)
            or _KUBERNETES_UID_RE.fullmatch(uid) is None
            or not isinstance(resource_version, str)
            or _RESOURCE_VERSION_RE.fullmatch(resource_version) is None
        ):
            return _blocked("cleanup", "namespace-delete-precondition-missing")
        delete_options: dict[str, object] = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "preconditions": {"resourceVersion": resource_version, "uid": uid},
            "propagationPolicy": "Foreground",
        }
        if not self._status_with_payload(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "delete",
                "--raw",
                f"/api/v1/namespaces/{plan.resources.namespace}",
                "-f",
                "-",
            ),
            _json_bytes(delete_options),
            timeout=30,
        ):
            return _blocked("cleanup", "namespace-delete-failed")
        if not self._status(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "wait",
                "--for=delete",
                f"namespace/{plan.resources.namespace}",
                "--timeout=300s",
            ),
            timeout=315,
        ):
            return _blocked("cleanup", "namespace-delete-timeout")
        final = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "get",
                "namespace",
                plan.resources.namespace,
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        if final is not None:
            return _blocked("cleanup", "namespace-remains")
        return _cleanup_ready(plan)

    def _wait_systemd_absent(self, contract: RehearsalSystemdActivation) -> bool:
        deadline = self.monotonic() + 5.0
        while True:
            load_state = self._systemd_load_state(contract)
            if load_state == "not-found":
                return True
            if load_state != "loaded" or self.monotonic() >= deadline:
                return False
            self.sleep(0.1)

    def _systemd_load_state(self, contract: RehearsalSystemdActivation) -> str:
        try:
            result = self.run(contract.load_state_argv, None, 15)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            return "unavailable"
        if (
            result.returncode not in {0, 4}
            or not isinstance(result.stdout, str)
            or not isinstance(result.stderr, str)
            or len(result.stdout.encode()) > 64
            or len(result.stderr.encode()) > _MAX_OUTPUT_BYTES
        ):
            return "unavailable"
        value = result.stdout.strip()
        return value if value in {"loaded", "not-found"} else "unavailable"

    def _psql_json(self, plan: RehearsalPlan, sql: str) -> dict[str, object] | None:
        return self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                plan.resources.namespace,
                "exec",
                "pod/loom-rehearsal-db",
                "--container",
                "postgres",
                "--",
                "psql",
                "--no-psqlrc",
                "--tuples-only",
                "--no-align",
                "--set=ON_ERROR_STOP=1",
                "--username=loom_rehearsal",
                f"--dbname={plan.resources.database}",
                f"--command={sql}",
            ),
            None,
            timeout=30,
        )

    def _status(self, argv: Sequence[str], *, timeout: int) -> bool:
        try:
            result = self.run(argv, None, timeout)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            return False
        return bool(
            result.returncode == 0
            and isinstance(result.stdout, str)
            and isinstance(result.stderr, str)
            and len(result.stdout.encode()) <= _MAX_OUTPUT_BYTES
            and len(result.stderr.encode()) <= _MAX_OUTPUT_BYTES
        )

    def _status_with_payload(
        self,
        argv: Sequence[str],
        payload: bytes,
        *,
        timeout: int,
    ) -> bool:
        try:
            result = self.run(argv, payload, timeout)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            return False
        return bool(
            result.returncode == 0
            and isinstance(result.stdout, str)
            and isinstance(result.stderr, str)
            and len(result.stdout.encode()) <= _MAX_OUTPUT_BYTES
            and len(result.stderr.encode()) <= _MAX_OUTPUT_BYTES
        )

    def _command(
        self,
        argv: Sequence[str],
        payload: bytes | None,
        *,
        timeout: int,
    ) -> dict[str, object] | None:
        try:
            result = self.run(argv, payload, timeout)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            return None
        if (
            result.returncode != 0
            or not isinstance(result.stdout, str)
            or not isinstance(result.stderr, str)
            or len(result.stdout.encode()) > _MAX_OUTPUT_BYTES
        ):
            return None
        try:
            value = json.loads(result.stdout, object_pairs_hook=_reject_duplicate_keys)
        except (json.JSONDecodeError, ValueError):
            return None
        return value if isinstance(value, dict) else None

    def _json_command_result(
        self,
        argv: Sequence[str],
        payload: bytes | None,
        *,
        timeout: int,
    ) -> tuple[int, dict[str, object]] | None:
        """Read bounded JSON from a probe on either its success or blocked exit."""
        try:
            result = self.run(argv, payload, timeout)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            return None
        if (
            result.returncode not in {0, 1}
            or not isinstance(result.stdout, str)
            or not isinstance(result.stderr, str)
            or len(result.stdout.encode()) > _MAX_OUTPUT_BYTES
            or len(result.stderr.encode()) > _MAX_OUTPUT_BYTES
        ):
            return None
        try:
            value = json.loads(result.stdout, object_pairs_hook=_reject_duplicate_keys)
        except (json.JSONDecodeError, ValueError):
            return None
        return (result.returncode, value) if isinstance(value, dict) else None

    def _text_command(
        self,
        argv: Sequence[str],
        *,
        timeout: int,
        max_bytes: int,
    ) -> str | None:
        try:
            result = self.run(argv, None, timeout)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            return None
        if (
            result.returncode != 0
            or not isinstance(result.stdout, str)
            or not isinstance(result.stderr, str)
            or len(result.stdout.encode()) > max_bytes
            or len(result.stderr.encode()) > _MAX_OUTPUT_BYTES
        ):
            return None
        return result.stdout


def _namespace_manifest(plan: RehearsalPlan) -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "annotations": {
                "loom.openai.dev/candidate-sha": plan.candidate_sha,
                "loom.openai.dev/candidate-tree": plan.candidate_tree,
                "loom.openai.dev/mutation-epoch": str(plan.mutation_epoch),
                "loom.openai.dev/plan-sha256": plan.plan_digest,
            },
            "labels": {
                "loom.openai.dev/authority": "staging-preflight",
                "loom.openai.dev/isolation": plan.resources.namespace.removeprefix(
                    "loom-rehearsal-"
                ),
                "pod-security.kubernetes.io/audit": "restricted",
                "pod-security.kubernetes.io/enforce": "restricted",
                "pod-security.kubernetes.io/enforce-version": "latest",
                "pod-security.kubernetes.io/warn": "restricted",
            },
            "name": plan.resources.namespace,
        },
    }


def _namespace_matches(value: dict[str, object], plan: RehearsalPlan) -> bool:
    metadata = value.get("metadata")
    if value.get("apiVersion") != "v1" or value.get("kind") != "Namespace":
        return False
    if not isinstance(metadata, dict):
        return False
    expected = _namespace_manifest(plan)["metadata"]
    assert isinstance(expected, dict)
    labels = metadata.get("labels")
    annotations = metadata.get("annotations")
    expected_labels = expected["labels"]
    expected_annotations = expected["annotations"]
    return bool(
        metadata.get("name") == plan.resources.namespace
        and isinstance(labels, dict)
        and isinstance(annotations, dict)
        and isinstance(expected_labels, dict)
        and isinstance(expected_annotations, dict)
        and all(labels.get(key) == item for key, item in expected_labels.items())
        and all(annotations.get(key) == item for key, item in expected_annotations.items())
    )


def _observer_binding_manifest(plan: RehearsalPlan) -> dict[str, object]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {
            "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
            "name": "loom-rollout-rehearsal-observer",
            "namespace": plan.resources.namespace,
        },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "loom-rollout-rehearsal-observer",
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "loom-rollout-rehearsal",
                "namespace": "loom-rollout-system",
            }
        ],
    }


def _default_deny_network_policy_manifest(plan: RehearsalPlan) -> dict[str, object]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
            "name": "loom-rehearsal-default-deny",
            "namespace": plan.resources.namespace,
        },
        "spec": {
            "podSelector": {},
            "policyTypes": ["Ingress", "Egress"],
        },
    }


def _default_deny_network_policy_matches(value: dict[str, object], plan: RehearsalPlan) -> bool:
    expected = _default_deny_network_policy_manifest(plan)
    metadata = value.get("metadata")
    expected_metadata = expected["metadata"]
    if not isinstance(metadata, dict) or not isinstance(expected_metadata, dict):
        return False
    annotations = metadata.get("annotations")
    return bool(
        value.get("apiVersion") == expected["apiVersion"]
        and value.get("kind") == expected["kind"]
        and metadata.get("name") == expected_metadata["name"]
        and metadata.get("namespace") == expected_metadata["namespace"]
        and isinstance(annotations, dict)
        and annotations.get("loom.openai.dev/plan-sha256") == plan.plan_digest
        and value.get("spec") == expected["spec"]
    )


def _database_pod_manifest(plan: RehearsalPlan) -> dict[str, object]:
    image = f"{REHEARSAL_POSTGRES_IMAGE}:{plan.image_tag}"
    migration_image = f"loom-control-plane:{plan.image_tag}"
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "annotations": {
                "loom.openai.dev/image-id": plan.image_digests[REHEARSAL_POSTGRES_IMAGE],
                "loom.openai.dev/migration-image-id": plan.image_digests["loom-control-plane"],
                "loom.openai.dev/plan-sha256": plan.plan_digest,
            },
            "labels": {"loom.openai.dev/component": "rehearsal-database"},
            "name": "loom-rehearsal-db",
            "namespace": plan.resources.namespace,
        },
        "spec": {
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "env": [
                        {"name": "PGDATA", "value": "/var/lib/postgresql/data/pgdata"},
                        {"name": "POSTGRES_DB", "value": plan.resources.database},
                        {"name": "POSTGRES_HOST_AUTH_METHOD", "value": "trust"},
                        {"name": "POSTGRES_USER", "value": "loom_rehearsal"},
                    ],
                    "image": image,
                    "imagePullPolicy": "Never",
                    "name": "postgres",
                    "readinessProbe": {
                        "exec": {
                            "command": [
                                "pg_isready",
                                "--dbname",
                                plan.resources.database,
                                "--username",
                                "loom_rehearsal",
                            ]
                        },
                        "failureThreshold": 30,
                        "periodSeconds": 2,
                        "timeoutSeconds": 1,
                    },
                    "resources": {
                        "limits": {
                            "cpu": "2",
                            "ephemeral-storage": "55Gi",
                            "memory": "2Gi",
                        },
                        "requests": {
                            "cpu": "250m",
                            "ephemeral-storage": "1Gi",
                            "memory": "512Mi",
                        },
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "readOnlyRootFilesystem": True,
                    },
                    "volumeMounts": [
                        {"mountPath": "/var/lib/postgresql/data", "name": "data"},
                        {"mountPath": "/var/run/postgresql", "name": "socket"},
                        {"mountPath": "/tmp", "name": "tmp"},
                    ],
                },
                {
                    "command": ["/bin/sleep", "infinity"],
                    "env": [
                        {"name": "HOME", "value": "/tmp"},
                        {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
                    ],
                    "image": migration_image,
                    "imagePullPolicy": "Never",
                    "name": "migration",
                    "resources": {
                        "limits": {"cpu": "1", "ephemeral-storage": "1Gi", "memory": "1Gi"},
                        "requests": {
                            "cpu": "100m",
                            "ephemeral-storage": "128Mi",
                            "memory": "256Mi",
                        },
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "readOnlyRootFilesystem": True,
                    },
                    "volumeMounts": [{"mountPath": "/tmp", "name": "tmp"}],
                },
            ],
            "enableServiceLinks": False,
            "restartPolicy": "Never",
            "securityContext": {
                "fsGroup": 999,
                "runAsGroup": 999,
                "runAsNonRoot": True,
                "runAsUser": 999,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "serviceAccountName": "default",
            "terminationGracePeriodSeconds": 30,
            "volumes": [
                {"emptyDir": {"sizeLimit": "50Gi"}, "name": "data"},
                {"emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"}, "name": "socket"},
                {"emptyDir": {"sizeLimit": "1Gi"}, "name": "tmp"},
            ],
        },
    }


def _database_pod_matches(
    value: dict[str, object],
    plan: RehearsalPlan,
    *,
    require_ready: bool,
    runtime_image_digests: Mapping[str, Sequence[str]],
) -> bool:
    expected = _database_pod_manifest(plan)
    metadata = value.get("metadata")
    expected_metadata = expected["metadata"]
    spec = value.get("spec")
    expected_spec = expected["spec"]
    if (
        value.get("apiVersion") != "v1"
        or value.get("kind") != "Pod"
        or not isinstance(metadata, dict)
        or not isinstance(expected_metadata, dict)
        or not isinstance(spec, dict)
        or not isinstance(expected_spec, dict)
    ):
        return False
    annotations = metadata.get("annotations")
    labels = metadata.get("labels")
    expected_annotations = expected_metadata["annotations"]
    expected_labels = expected_metadata["labels"]
    exact_spec_fields = (
        "automountServiceAccountToken",
        "enableServiceLinks",
        "restartPolicy",
        "securityContext",
        "serviceAccountName",
        "terminationGracePeriodSeconds",
        "volumes",
    )
    expected_containers = expected_spec["containers"]
    if not isinstance(expected_containers, list):  # pragma: no cover - local manifest contract
        return False
    observed_containers = []
    for container in expected_containers:
        if not isinstance(container, dict):
            continue
        observed = {
            **container,
            "terminationMessagePath": "/dev/termination-log",
            "terminationMessagePolicy": "File",
        }
        readiness_probe = container.get("readinessProbe")
        if isinstance(readiness_probe, dict):
            observed["readinessProbe"] = {
                **readiness_probe,
                "successThreshold": 1,
            }
        observed_containers.append(observed)
    if not (
        metadata.get("name") == expected_metadata["name"]
        and metadata.get("namespace") == expected_metadata["namespace"]
        and isinstance(annotations, dict)
        and isinstance(labels, dict)
        and isinstance(expected_annotations, dict)
        and isinstance(expected_labels, dict)
        and all(annotations.get(key) == item for key, item in expected_annotations.items())
        and all(labels.get(key) == item for key, item in expected_labels.items())
        and len(observed_containers) == len(expected_containers)
        and spec.get("containers") == observed_containers
        and all(spec.get(key) == expected_spec[key] for key in exact_spec_fields)
    ):
        return False
    if not require_ready:
        return True
    status = value.get("status")
    if not isinstance(status, dict) or status.get("phase") != "Running":
        return False
    conditions = status.get("conditions")
    statuses = status.get("containerStatuses")
    expected_digests = {
        "migration": runtime_image_digests.get("loom-control-plane"),
        "postgres": runtime_image_digests.get(REHEARSAL_POSTGRES_IMAGE),
    }
    return bool(
        isinstance(conditions, list)
        and any(
            isinstance(condition, dict)
            and condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in conditions
        )
        and isinstance(statuses, list)
        and len(statuses) == 2
        and all(isinstance(status, dict) for status in statuses)
        and {str(status["name"]) for status in statuses} == set(expected_digests)
        and all(_runtime_container_ready(status, expected_digests) for status in statuses)
    )


def _runtime_container_ready(
    status: dict[str, object],
    expected_digests: Mapping[str, Sequence[str] | None],
) -> bool:
    name = status.get("name")
    image_id = status.get("imageID")
    expected = expected_digests.get(name) if isinstance(name, str) else None
    return bool(
        isinstance(image_id, str)
        and isinstance(expected, Sequence)
        and not isinstance(expected, str)
        and any(image_id.endswith(digest) for digest in expected)
        and status.get("ready") is True
    )


def _database_ready(plan: RehearsalPlan) -> RehearsalStepOutcome:
    return RehearsalStepOutcome(
        passed=True,
        details={
            "database": plan.resources.database,
            "schema-revision": plan.schema_revision,
            "status": "restored",
        },
        blockers={},
    )


def _migration_ready(plan: RehearsalPlan) -> RehearsalStepOutcome:
    return RehearsalStepOutcome(
        passed=True,
        details={
            "plan-sha256": plan.migration_plan_sha256,
            "schema-revision": plan.migration_target_revision,
            "status": "migrated",
        },
        blockers={},
    )


def _systemd_ready(
    contract: RehearsalSystemdActivation,
    *,
    latency_ms: int,
    gb10_evidence_digest: str,
    gb10_host_count: int,
) -> RehearsalStepOutcome:
    return RehearsalStepOutcome(
        passed=True,
        details={
            "latency-ms": str(latency_ms),
            "gb10-evidence-sha256": gb10_evidence_digest,
            "gb10-host-count": str(gb10_host_count),
            "status": "active",
            "unit": contract.unit,
        },
        blockers={},
    )


def _cleanup_ready(plan: RehearsalPlan) -> RehearsalStepOutcome:
    return RehearsalStepOutcome(
        passed=True,
        details={
            "namespace": plan.resources.namespace,
            "status": "absent",
            "unit": plan.resources.systemd_unit,
        },
        blockers={},
        cleanup_verified=True,
    )


def _gb10_blocked(component: str, evidence: GB10RehearsalEvidence) -> RehearsalStepOutcome:
    return RehearsalStepOutcome(
        passed=False,
        details={
            "gb10-evidence-sha256": evidence.evidence_digest,
            "component": component,
            "status": "blocked",
        },
        blockers={f"gb10-{host}": reason for host, reason in evidence.blockers.items()},
    )


def _observer_binding_matches(value: dict[str, object], plan: RehearsalPlan) -> bool:
    expected = _observer_binding_manifest(plan)
    metadata = value.get("metadata")
    expected_metadata = expected["metadata"]
    if not isinstance(metadata, dict) or not isinstance(expected_metadata, dict):
        return False
    annotations = metadata.get("annotations")
    return bool(
        value.get("apiVersion") == expected["apiVersion"]
        and value.get("kind") == expected["kind"]
        and metadata.get("name") == expected_metadata["name"]
        and metadata.get("namespace") == expected_metadata["namespace"]
        and isinstance(annotations, dict)
        and annotations.get("loom.openai.dev/plan-sha256") == plan.plan_digest
        and value.get("roleRef") == expected["roleRef"]
        and value.get("subjects") == expected["subjects"]
    )


def _blocked(component: str, reason: str) -> RehearsalStepOutcome:
    return RehearsalStepOutcome(
        passed=False,
        details={"status": "blocked"},
        blockers={component: reason},
    )


def _exact_service_pod_name(
    value: dict[str, object] | None,
    *,
    release: RehearsalReleaseArtifact,
    plan: RehearsalPlan,
    runtime_image_digests: Sequence[str],
) -> str | None:
    if value is None or not rehearsal_pods_ready(
        value,
        artifact=release,
        deployment_name="loom-service",
        runtime_image_digests=runtime_image_digests,
    ):
        return None
    items = value.get("items")
    if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
        return None
    metadata = items[0].get("metadata")
    if not isinstance(metadata, dict):
        return None
    annotations = metadata.get("annotations")
    name = metadata.get("name")
    if (
        not isinstance(annotations, dict)
        or annotations.get("loom.openai.dev/plan-sha256") != plan.plan_digest
        or not isinstance(name, str)
        or re.fullmatch(r"loom-service-[a-z0-9](?:[-a-z0-9.]{0,61}[a-z0-9])?", name) is None
    ):
        return None
    return name


def _api_smoke_result_ready(
    value: dict[str, object],
    *,
    plan: RehearsalPlan,
    batch_name: str,
) -> bool:
    expected = {
        "batch_id",
        "batch_name",
        "evidence",
        "persisted",
        "plan_sha256",
        "recovered",
        "schema_version",
        "status",
    }
    evidence = value.get("evidence")
    batch_id = value.get("batch_id")
    try:
        parsed_batch = uuid.UUID(batch_id) if isinstance(batch_id, str) else None
    except ValueError:
        return False
    return bool(
        set(value) == expected
        and parsed_batch is not None
        and parsed_batch.version == 4
        and str(parsed_batch) == batch_id
        and value.get("batch_name") == batch_name
        and value.get("persisted") is True
        and value.get("plan_sha256") == plan.plan_digest
        and type(value.get("recovered")) is bool
        and value.get("schema_version") == 1
        and value.get("status") == "ready"
        and isinstance(evidence, dict)
        and 6 <= len(evidence) <= 7
        and all(
            isinstance(key, str)
            and key.startswith(("get:/api/v1/", "post:/api/v1/"))
            and isinstance(item, str)
            and re.fullmatch(r"[0-9a-f]{64}", item) is not None
            for key, item in evidence.items()
        )
    )


def _api_smoke_failure(
    value: dict[str, object],
) -> tuple[str, str, str, str | None] | None:
    expected = {
        "failure_code",
        "reason_code",
        "request_id",
        "response_sha256",
        "schema_version",
        "status",
    }
    failure_code = value.get("failure_code")
    reason_code = value.get("reason_code")
    request_id = value.get("request_id")
    response_sha256 = value.get("response_sha256")
    if (
        set(value) != expected
        or value.get("schema_version") != 1
        or value.get("status") != "blocked"
        or not isinstance(failure_code, str)
        or re.fullmatch(r"rehearsal-api-smoke-(?:failed|http-[1-5][0-9]{2})", failure_code) is None
        or not isinstance(reason_code, str)
        or reason_code not in _API_SMOKE_REASON_CODES
        or not isinstance(request_id, str)
        or request_id not in _API_SMOKE_REQUEST_IDS
    ):
        return None
    is_http = failure_code.startswith("rehearsal-api-smoke-http-")
    if is_http:
        if (
            not isinstance(response_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", response_sha256) is None
            or reason_code == "probe-failed"
            or request_id == "probe"
        ):
            return None
    elif response_sha256 is not None or reason_code != "probe-failed" or request_id != "probe":
        return None
    return failure_code, request_id, reason_code, response_sha256


def _production_defaults_result_ready(
    value: dict[str, object],
    *,
    plan: RehearsalPlan,
) -> bool:
    mutation_count = value.get("mutation_count")
    return bool(
        set(value)
        == {
            "artifact_sha256",
            "candidate_sha",
            "candidate_tree",
            "evidence_sha256",
            "mutation_count",
            "plan_sha256",
            "schema_version",
            "status",
        }
        and value.get("artifact_sha256") == plan.production_defaults_sha256
        and value.get("candidate_sha") == plan.candidate_sha
        and value.get("candidate_tree") == plan.candidate_tree
        and isinstance(value.get("evidence_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", str(value["evidence_sha256"])) is not None
        and type(mutation_count) is int
        and 0 <= mutation_count <= 64
        and value.get("plan_sha256") == plan.plan_digest
        and value.get("schema_version") == 1
        and value.get("status") == "ready"
    )


def _json_bytes(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


__all__ = [
    "REHEARSAL_KUBECONFIG",
    "CommandResult",
    "CommandRunner",
    "IsolatedRehearsalExecutor",
]
