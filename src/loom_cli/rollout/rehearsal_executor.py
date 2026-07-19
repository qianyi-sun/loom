"""Concrete fixed-command executor for isolated exact-candidate rehearsal."""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.image_readiness import REHEARSAL_POSTGRES_IMAGE
from loom_cli.rollout.rehearsal_action_source import RehearsalPlan
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

REHEARSAL_KUBECONFIG = Path("/var/lib/loom-staging-rollout/credentials/rehearsal-kubeconfig")
_MAX_OUTPUT_BYTES = 1024 * 1024
_KUBERNETES_UID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z"
)
_RESOURCE_VERSION_RE = re.compile(r"[1-9][0-9]{0,31}\Z")


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
    kubeconfig: Path = REHEARSAL_KUBECONFIG
    monotonic: Callable[[], float] = time.monotonic

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
        if check_id == "rehearsal.cleanup":
            return self._cleanup(plan)
        return RehearsalStepOutcome(
            passed=False,
            details={"status": "blocked"},
            blockers={"executor": "isolated-action-not-implemented"},
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
        if not self._load_images(plan, (REHEARSAL_POSTGRES_IMAGE, "loom-control-plane")):
            return _blocked("database", "image-load-failed")
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
        if applied is None or not _database_pod_matches(applied, plan, require_ready=False):
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
        if observed is None or not _database_pod_matches(observed, plan, require_ready=True):
            return _blocked("database", "pod-readback-drift")
        existing = self._database_identity(plan)
        if existing is not None and existing.get("restored") is True:
            if existing.get("schema_revision") == plan.schema_revision:
                return _database_ready(plan)
            return _blocked("database", "existing-restore-drift")
        dump_path = plan.checkpoint_manifest_path.parent / "postgres" / "loom.dump"
        restore_argv = (
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
            "pg_restore",
            "--exit-on-error",
            "--no-owner",
            "--no-privileges",
            f"--dbname={plan.resources.database}",
        )
        try:
            restored = self.stream_run(restore_argv, dump_path, 1800)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            return _blocked("database", "restore-failed")
        if restored.returncode != 0:
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
        if existing is not None:
            if contract.ready(existing, latency_ms=0):
                return _systemd_ready(contract, latency_ms=0)
            if not contract.absent(existing):
                return _blocked("systemd", "existing-unit-drift")
        started_at = self.monotonic()
        if not self._status(contract.start_argv, timeout=30):
            return _blocked("systemd", "activation-failed")
        latency_ms = max(0, round((self.monotonic() - started_at) * 1000))
        observed = self._systemd_properties(contract)
        if observed is None or not contract.ready(observed, latency_ms=latency_ms):
            return _blocked("systemd", "activation-readback-drift")
        return _systemd_ready(contract, latency_ms=latency_ms)

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
        properties = self._systemd_properties(contract)
        if properties is not None and not contract.absent(properties):
            if not contract.ready(properties, latency_ms=0):
                return _blocked("cleanup", "systemd-identity-drift")
            if not self._status(contract.stop_argv, timeout=30):
                return _blocked("cleanup", "systemd-stop-failed")
            if not self._status(contract.reset_argv, timeout=30):
                return _blocked("cleanup", "systemd-reset-failed")
        if self._systemd_load_state(contract) != "not-found":
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
    value: dict[str, object], plan: RehearsalPlan, *, require_ready: bool
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
        "containers",
        "enableServiceLinks",
        "restartPolicy",
        "securityContext",
        "serviceAccountName",
        "terminationGracePeriodSeconds",
        "volumes",
    )
    if not (
        metadata.get("name") == expected_metadata["name"]
        and metadata.get("namespace") == expected_metadata["namespace"]
        and isinstance(annotations, dict)
        and isinstance(labels, dict)
        and isinstance(expected_annotations, dict)
        and isinstance(expected_labels, dict)
        and all(annotations.get(key) == item for key, item in expected_annotations.items())
        and all(labels.get(key) == item for key, item in expected_labels.items())
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
        "migration": plan.image_digests["loom-control-plane"],
        "postgres": plan.image_digests[REHEARSAL_POSTGRES_IMAGE],
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
        and all(
            isinstance(status.get("imageID"), str)
            and str(status["imageID"]).endswith(expected_digests[str(status["name"])])
            and status.get("ready") is True
            for status in statuses
        )
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
) -> RehearsalStepOutcome:
    return RehearsalStepOutcome(
        passed=True,
        details={
            "latency-ms": str(latency_ms),
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
