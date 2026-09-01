"""Request-bound capacity publication immediately before protected smoke."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast

import yaml  # type: ignore[import-untyped]

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.cluster_config import lifecycle_inventory_buckets, load_cluster_config
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactStore
from loom_cli.rollout.readonly_database_authority import ReadonlyDatabaseEvidence

from .config import OperatorConfig
from .final_gate_plan import FinalGatePlan
from .installed_preflight_commands import InstalledPreflightCommands
from .lifecycle_capacity_job import (
    LifecycleCapacityJobPlan,
    build_rollout_capacity_job_plan,
)
from .policy import sanitized_child_environment
from .readonly_database_client import probe_installed_readonly_database_baseline
from .staging_mutation_guard import (
    MutationGuardEvidence,
    guard_evidence_path,
    read_mutation_guard_evidence,
)

_JOB_UID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_MAX_COMMAND_OUTPUT = 1024 * 1024
_CAPACITY_FRESHNESS = timedelta(minutes=5)


class CommandResult(Protocol):
    returncode: int
    stdout: str
    stderr: str


class CapacityCommands(Protocol):
    def simple(self, argv: Sequence[str]) -> CommandResult: ...

    def manifest_server_apply(self, rendered: str) -> CommandResult: ...

    def lifecycle_capacity_wait(self, job_name: str) -> CommandResult: ...


GuardReader = Callable[[FinalGatePlan], MutationGuardEvidence]
JobPlanBuilder = Callable[[FinalGatePlan, MutationGuardEvidence], LifecycleCapacityJobPlan]
DatabaseReader = Callable[[], ReadonlyDatabaseEvidence]
Clock = Callable[[], datetime]


class InstalledRolloutCapacityRefreshError(RuntimeError):
    """One fail-closed protected rollout capacity refresh failure."""


def _safe_command(result: CommandResult, label: str) -> str:
    if (
        type(result.returncode) is not int
        or not isinstance(result.stdout, str)
        or not isinstance(result.stderr, str)
        or len(result.stdout.encode()) > _MAX_COMMAND_OUTPUT
        or len(result.stderr.encode()) > _MAX_COMMAND_OUTPUT
    ):
        raise InstalledRolloutCapacityRefreshError(f"{label} result is invalid")
    if result.returncode != 0:
        raise InstalledRolloutCapacityRefreshError(f"{label} failed")
    return result.stdout


def _object(payload: str, label: str) -> dict[str, object]:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise InstalledRolloutCapacityRefreshError(f"{label} output is invalid") from exc
    if not isinstance(value, dict):
        raise InstalledRolloutCapacityRefreshError(f"{label} output is invalid")
    return cast(dict[str, object], value)


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise InstalledRolloutCapacityRefreshError(f"{label} is invalid")
    return cast(dict[str, object], value)


def _sequence(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise InstalledRolloutCapacityRefreshError(f"{label} is invalid")
    return cast(list[object], value)


def _exact_value(expected: object, observed: object) -> bool:
    if isinstance(expected, dict):
        return (
            isinstance(observed, dict)
            and set(expected) == set(observed)
            and all(_exact_value(value, observed[key]) for key, value in expected.items())
        )
    if isinstance(expected, list):
        return (
            isinstance(observed, list)
            and len(expected) == len(observed)
            and all(
                _exact_value(left, right)
                for left, right in zip(expected, observed, strict=True)
            )
        )
    return type(expected) is type(observed) and expected == observed


def _expected_job(plan: LifecycleCapacityJobPlan) -> dict[str, object]:
    try:
        value = yaml.safe_load(plan.job_manifest)
    except yaml.YAMLError as exc:
        raise InstalledRolloutCapacityRefreshError("capacity Job plan is invalid") from exc
    return _mapping(value, "capacity Job plan")


def _controller_labels(*, job_name: str, job_uid: str) -> dict[str, object]:
    return {
        "batch.kubernetes.io/controller-uid": job_uid,
        "batch.kubernetes.io/job-name": job_name,
        "controller-uid": job_uid,
        "job-name": job_name,
    }


def _normalize_expected_resources(container: dict[str, object]) -> None:
    resources = _mapping(container.get("resources"), "expected capacity resources")
    for category in ("limits", "requests"):
        quantities = _mapping(
            resources.get(category),
            f"expected capacity resource {category}",
        )
        for name, value in tuple(quantities.items()):
            if isinstance(value, bool) or not isinstance(value, (int, str)):
                raise InstalledRolloutCapacityRefreshError(
                    "expected capacity resources are invalid"
                )
            quantities[name] = str(value)


def _expected_defaulted_job_spec(
    expected_job: Mapping[str, object],
    *,
    job_name: str,
    job_uid: str,
) -> dict[str, object]:
    expected_spec = copy.deepcopy(
        _mapping(expected_job.get("spec"), "expected capacity Job spec")
    )
    expected_spec.update(
        {
            "completionMode": "NonIndexed",
            "completions": 1,
            "manualSelector": False,
            "parallelism": 1,
            "podReplacementPolicy": "TerminatingOrFailed",
            "selector": {
                "matchLabels": {"batch.kubernetes.io/controller-uid": job_uid}
            },
            "suspend": False,
        }
    )
    template = _mapping(
        expected_spec.get("template"),
        "expected capacity Pod template",
    )
    template_metadata = _mapping(
        template.get("metadata"),
        "expected capacity Pod metadata",
    )
    template_labels = _mapping(
        template_metadata.get("labels"),
        "expected capacity Pod labels",
    )
    template_metadata["labels"] = {
        **template_labels,
        **_controller_labels(job_name=job_name, job_uid=job_uid),
    }
    pod_spec = _mapping(template.get("spec"), "expected capacity Pod spec")
    pod_spec.update(
        {
            "dnsPolicy": "ClusterFirst",
            "schedulerName": "default-scheduler",
            "terminationGracePeriodSeconds": 30,
        }
    )
    containers = _sequence(pod_spec.get("containers"), "expected capacity containers")
    if len(containers) != 1:
        raise InstalledRolloutCapacityRefreshError(
            "expected capacity containers are invalid"
        )
    container = _mapping(containers[0], "expected capacity container")
    container.update(
        {
            "imagePullPolicy": "IfNotPresent",
            "terminationMessagePath": "/dev/termination-log",
            "terminationMessagePolicy": "File",
        }
    )
    _normalize_expected_resources(container)
    return expected_spec


def _job_spec_matches(
    expected_job: Mapping[str, object],
    observed_job: Mapping[str, object],
    *,
    job_name: str,
    job_uid: str,
) -> bool:
    try:
        expected_spec = _expected_defaulted_job_spec(
            expected_job,
            job_name=job_name,
            job_uid=job_uid,
        )
        observed_spec = _mapping(observed_job.get("spec"), "capacity Job spec")
    except InstalledRolloutCapacityRefreshError:
        return False
    return _exact_value(expected_spec, observed_spec)


def _require_metadata(
    value: object,
    expected: Mapping[str, object],
    *,
    label: str,
) -> str:
    metadata = _mapping(value, f"{label} metadata")
    uid = metadata.get("uid")
    name = expected.get("name")
    expected_labels = _mapping(expected.get("labels"), f"expected {label} labels")
    creation_timestamp = metadata.get("creationTimestamp")
    resource_version = metadata.get("resourceVersion")
    try:
        created_at = datetime.fromisoformat(str(creation_timestamp))
    except ValueError as exc:
        raise InstalledRolloutCapacityRefreshError(f"{label} identity drifted") from exc
    if (
        not isinstance(name, str)
        or not isinstance(uid, str)
        or _JOB_UID_RE.fullmatch(uid) is None
        or not isinstance(creation_timestamp, str)
        or created_at.tzinfo is None
        or not isinstance(resource_version, str)
        or not resource_version.isdigit()
        or resource_version.startswith("0")
    ):
        raise InstalledRolloutCapacityRefreshError(f"{label} identity drifted")
    expected_metadata = {
        **expected,
        "creationTimestamp": creation_timestamp,
        "generation": 1,
        "labels": {
            **expected_labels,
            **_controller_labels(job_name=name, job_uid=uid),
        },
        "resourceVersion": resource_version,
        "uid": uid,
    }
    if not _exact_value(expected_metadata, metadata):
        raise InstalledRolloutCapacityRefreshError(f"{label} identity drifted")
    return uid


def _require_job_identity(
    job: Mapping[str, object],
    expected: Mapping[str, object],
    *,
    expected_uid: str | None = None,
    require_complete: bool,
) -> str:
    expected_metadata = _mapping(
        expected.get("metadata"),
        "expected capacity Job metadata",
    )
    job_name = expected_metadata.get("name")
    if not isinstance(job_name, str):
        raise InstalledRolloutCapacityRefreshError("capacity Job identity drifted")
    uid = _require_metadata(
        job.get("metadata"),
        expected_metadata,
        label="capacity Job",
    )
    if expected_uid is not None and uid != expected_uid:
        raise InstalledRolloutCapacityRefreshError("capacity Job identity drifted")
    if (
        job.get("apiVersion") != "batch/v1"
        or job.get("kind") != "Job"
        or not _job_spec_matches(
            expected,
            job,
            job_name=job_name,
            job_uid=uid,
        )
    ):
        raise InstalledRolloutCapacityRefreshError("capacity Job identity drifted")
    if not require_complete:
        return uid
    status = _mapping(job.get("status"), "capacity Job status")
    conditions = _sequence(status.get("conditions"), "capacity Job conditions")
    if (
        status.get("succeeded") != 1
        or status.get("failed", 0) not in (0, None)
        or not any(
            isinstance(item, dict)
            and item.get("type") == "Complete"
            and item.get("status") == "True"
            for item in conditions
        )
    ):
        raise InstalledRolloutCapacityRefreshError("capacity Job completion is invalid")
    return uid


def _capacity_output(payload: str) -> dict[str, object]:
    value = _object(payload, "capacity Job")
    capacity = value.get("capacity")
    expected_capacity = {
        "admission_allowed",
        "bytes_used",
        "disk_free_percent",
        "evidence_sha256",
        "gc_required",
        "inode_free_percent",
        "object_count",
        "observed_at",
        "policy_sha256",
    }
    if (
        set(value) != {"action", "capacity", "gc", "schema_version"}
        or value.get("schema_version") != 1
        or value.get("action") != "rollout-capacity"
        or value.get("gc") is not None
        or not isinstance(capacity, dict)
        or set(capacity) != expected_capacity
        or capacity.get("admission_allowed") is not True
        or capacity.get("gc_required") is not False
        or capacity.get("policy_sha256") != staging_capacity_policy_digest()
    ):
        raise InstalledRolloutCapacityRefreshError("capacity Job evidence is incomplete")
    try:
        observed_at = datetime.fromisoformat(str(capacity["observed_at"]))
        model = StagingCapacity(
            object_count=capacity["object_count"],
            bytes_used=capacity["bytes_used"],
            disk_free_percent=capacity["disk_free_percent"],
            inode_free_percent=capacity["inode_free_percent"],
        )
    except (TypeError, ValueError) as exc:
        raise InstalledRolloutCapacityRefreshError("capacity Job evidence is invalid") from exc
    if observed_at.tzinfo is None or capacity.get("evidence_sha256") != model.evidence_digest:
        raise InstalledRolloutCapacityRefreshError("capacity Job evidence is invalid")
    return cast(dict[str, object], capacity)


def _verify_database_capacity(
    database: ReadonlyDatabaseEvidence,
    capacity: Mapping[str, object],
    *,
    mutation_epoch: int,
) -> None:
    stored = database.capacity
    keys = ("object_count", "bytes_used", "disk_free_percent", "inode_free_percent")
    try:
        observed_at = datetime.fromisoformat(str(capacity["observed_at"]))
    except (KeyError, ValueError) as exc:
        raise InstalledRolloutCapacityRefreshError(
            "published capacity timestamp is invalid"
        ) from exc
    if (
        database.mutation_epoch != mutation_epoch
        or stored is None
        or any(stored.get(key) != capacity.get(key) for key in keys)
        or stored.get("policy_sha256") != capacity.get("policy_sha256")
        or stored.get("evidence_sha256") != capacity.get("evidence_sha256")
        or stored.get("observed_at_epoch") != int(observed_at.timestamp())
    ):
        raise InstalledRolloutCapacityRefreshError(
            "published capacity database evidence drifted"
        )


def _require_guard(
    plan: FinalGatePlan,
    guard: MutationGuardEvidence,
) -> None:
    if (
        plan.environment != "staging"
        or plan.namespace != "loom-staging"
        or guard.request_id != plan.request_id
        or guard.candidate_sha != plan.candidate_sha
        or guard.candidate_tree != plan.candidate_tree
        or guard.mutation_epoch != plan.starting_mutation_epoch
        or guard.state != "ready"
    ):
        raise InstalledRolloutCapacityRefreshError("rollout guard identity drifted")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate key")
        value[key] = item
    return value


def build_installed_rollout_capacity_job_plan(
    *,
    config: OperatorConfig,
    service_uid: int,
    final_plan: FinalGatePlan,
    guard: MutationGuardEvidence,
    expected_buckets: tuple[str, ...],
    expected_filesystem_paths: tuple[str, ...],
    capacity_source: str,
    expected_drive_count: int | None,
    container_registry: str,
) -> LifecycleCapacityJobPlan:
    """Reconstruct one Job only from the final plan's immutable artifacts."""

    _require_guard(final_plan, guard)
    try:
        publication = PreflightArtifactStore(
            config.state_root,
            service_uid=service_uid,
        ).read(final_plan.artifact_bundle_digest)
        descriptor_read = read_trusted_file(
            publication.descriptor_path,
            service_uid=service_uid,
            private=True,
            max_bytes=1024 * 1024,
            require_nonempty=True,
        )
        rendered_read = read_trusted_file(
            publication.rendered_manifest_path,
            service_uid=service_uid,
            private=True,
            max_bytes=16 * 1024 * 1024,
            require_nonempty=True,
        )
        descriptor = json.loads(
            descriptor_read.payload,
            object_pairs_hook=_reject_duplicate_keys,
        )
        rendered = rendered_read.payload.decode("utf-8")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, RuntimeError, ValueError) as exc:
        raise InstalledRolloutCapacityRefreshError(
            "rollout capacity artifact identity drifted"
        ) from exc
    image_digests = descriptor.get("image_digests") if isinstance(descriptor, dict) else None
    registry_digests = (
        descriptor.get("registry_digests") if isinstance(descriptor, dict) else None
    )
    registry_digest = (
        registry_digests.get("loom-control-plane")
        if isinstance(registry_digests, dict)
        else None
    )
    if (
        publication.bundle_digest != final_plan.artifact_bundle_digest
        or publication.candidate_sha != final_plan.candidate_sha
        or publication.candidate_tree != final_plan.candidate_tree
        or publication.mutation_epoch != final_plan.starting_mutation_epoch
        or str(publication.descriptor_path) != final_plan.artifact_descriptor_path
        or str(publication.rendered_manifest_path) != final_plan.rendered_manifest_path
        or publication.rendered_manifest_sha256 != final_plan.rendered_manifest_sha256
        or hashlib.sha256(rendered_read.payload).hexdigest()
        != final_plan.rendered_manifest_sha256
        or not isinstance(descriptor, dict)
        or descriptor.get("candidate_sha") != final_plan.candidate_sha
        or descriptor.get("candidate_tree") != final_plan.candidate_tree
        or descriptor.get("mutation_epoch") != final_plan.starting_mutation_epoch
        or descriptor.get("container_registry") != container_registry
        or publication.container_registry != container_registry
        or image_digests != dict(final_plan.image_digests)
        or not isinstance(registry_digests, dict)
        or bool(registry_digests) != bool(container_registry)
        or (
            container_registry
            and (
                not isinstance(registry_digest, str)
                or re.fullmatch(r"sha256:[0-9a-f]{64}", registry_digest) is None
            )
        )
    ):
        raise InstalledRolloutCapacityRefreshError(
            "rollout capacity artifact identity drifted"
        )
    return build_rollout_capacity_job_plan(
        candidate_sha=final_plan.candidate_sha,
        candidate_tree=final_plan.candidate_tree,
        mutation_epoch=final_plan.starting_mutation_epoch + 1,
        artifact_bundle_sha256=final_plan.artifact_bundle_digest,
        rendered_manifest_sha256=final_plan.rendered_manifest_sha256,
        control_plane_image_id=final_plan.image_digests["loom-control-plane"],
        image_tag=f"staging-{final_plan.candidate_sha[:7]}",
        rendered_yaml=rendered,
        expected_buckets=expected_buckets,
        expected_filesystem_paths=expected_filesystem_paths,
        request_id=final_plan.request_id,
        attempt_number=final_plan.attempt_number,
        rollout_plan_digest=final_plan.plan_digest,
        guard_generation=guard.generation,
        guard_backend_pid=guard.database_backend_pid,
        capacity_source=capacity_source,
        expected_drive_count=expected_drive_count,
        container_registry=container_registry,
        registry_digest=cast(str, registry_digest or ""),
    )


@dataclass(frozen=True, slots=True)
class InstalledRolloutCapacityRefresh:
    """Execute or recover one exact attempt-bound capacity-only Job."""

    config: OperatorConfig
    service_uid: int
    commands: CapacityCommands
    read_guard: GuardReader
    build_job_plan: JobPlanBuilder
    read_database: DatabaseReader
    now: Clock

    def __post_init__(self) -> None:
        if (
            self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or self.service_uid < 1
            or not all(
                callable(value)
                for value in (
                    self.commands.simple,
                    self.commands.manifest_server_apply,
                    self.commands.lifecycle_capacity_wait,
                    self.read_guard,
                    self.build_job_plan,
                    self.read_database,
                    self.now,
                )
            )
        ):
            raise ValueError("installed rollout capacity authority is invalid")

    def __call__(self, final_plan: FinalGatePlan) -> str:
        guard = self.read_guard(final_plan)
        _require_guard(final_plan, guard)
        plan = self.build_job_plan(final_plan, guard)
        if (
            plan.candidate_sha != final_plan.candidate_sha
            or plan.candidate_tree != final_plan.candidate_tree
            or plan.mutation_epoch != final_plan.starting_mutation_epoch + 1
        ):
            raise InstalledRolloutCapacityRefreshError("capacity Job plan identity drifted")
        expected = _expected_job(plan)
        existing_payload = _safe_command(
            self.commands.simple(
                (
                    "kubectl",
                    "--kubeconfig",
                    str(self.config.kubeconfig_path),
                    "--namespace",
                    self.config.namespace,
                    "get",
                    "job",
                    plan.job_name,
                    "--ignore-not-found",
                    "--output=json",
                )
            ),
            "capacity Job lookup",
        ).strip()
        if existing_payload:
            applied_uid = _require_job_identity(
                _object(existing_payload, "capacity Job lookup"),
                expected,
                require_complete=False,
            )
        else:
            apply = _object(
                _safe_command(
                    self.commands.manifest_server_apply(plan.job_manifest),
                    "capacity Job apply",
                ),
                "capacity Job apply",
            )
            applied_uid = _require_metadata(
                apply.get("metadata"),
                _mapping(expected.get("metadata"), "expected capacity Job metadata"),
                label="capacity Job apply",
            )
        _safe_command(
            self.commands.lifecycle_capacity_wait(plan.job_name),
            "capacity Job wait",
        )
        capacity = _capacity_output(
            _safe_command(
                self.commands.simple(
                    (
                        "kubectl",
                        "--kubeconfig",
                        str(self.config.kubeconfig_path),
                        "--namespace",
                        self.config.namespace,
                        "logs",
                        f"job/{plan.job_name}",
                        "--container=lifecycle",
                        "--tail=20",
                    )
                ),
                "capacity Job logs",
            ).strip()
        )
        completed_at = self.now()
        observed_at = datetime.fromisoformat(str(capacity["observed_at"]))
        if (
            completed_at.tzinfo is None
            or observed_at.tzinfo is None
            or completed_at - observed_at < timedelta(0)
            or completed_at - observed_at > _CAPACITY_FRESHNESS
        ):
            raise InstalledRolloutCapacityRefreshError("capacity Job evidence is stale")
        job = _object(
            _safe_command(
                self.commands.simple(
                    (
                        "kubectl",
                        "--kubeconfig",
                        str(self.config.kubeconfig_path),
                        "--namespace",
                        self.config.namespace,
                        "get",
                        "job",
                        plan.job_name,
                        "--output=json",
                    )
                ),
                "capacity Job readback",
            ),
            "capacity Job readback",
        )
        job_uid = _require_job_identity(
            job,
            expected,
            expected_uid=applied_uid,
            require_complete=True,
        )
        database = self.read_database()
        _verify_database_capacity(database, capacity, mutation_epoch=plan.mutation_epoch)
        final_guard = self.read_guard(final_plan)
        _require_guard(final_plan, final_guard)
        if final_guard != guard:
            raise InstalledRolloutCapacityRefreshError("rollout guard identity drifted")
        evidence = {
            "capacity": capacity,
            "database_evidence_sha256": database.evidence_sha256,
            "guard_evidence_sha256": guard.evidence_digest,
            "job_plan_sha256": plan.plan_digest,
            "job_uid": job_uid,
            "mutation_epoch": plan.mutation_epoch,
            "schema_version": 1,
        }
        return hashlib.sha256(
            json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


def build_installed_rollout_capacity_refresh(
    config: OperatorConfig,
    *,
    service_uid: int,
) -> InstalledRolloutCapacityRefresh:
    """Compose the fixed installed refresh authority for one final helper."""

    cluster = load_cluster_config(config.cluster_config_path)
    replicas = cluster.topology.minio_replicas
    if isinstance(replicas, bool) or not isinstance(replicas, int) or replicas < 1:
        raise ValueError("installed rollout capacity topology is invalid")
    multi_node = bool(cluster.topology.multi_node)
    capacity_source = "minio-admin" if multi_node else "filesystem"
    expected_drive_count = replicas if multi_node else None
    expected_filesystem_paths = (
        ()
        if multi_node
        else tuple(f"/var/lib/loom-minio-capacity/{index}" for index in range(replicas))
    )
    expected_buckets = lifecycle_inventory_buckets(cluster)
    container_registry = str(cluster.container_registry)
    commands = InstalledPreflightCommands(
        config,
        sanitized_child_environment(config, service_uid=service_uid),
    )

    def read_guard(plan: FinalGatePlan) -> MutationGuardEvidence:
        return read_mutation_guard_evidence(
            guard_evidence_path(config, plan.request_id),
            service_uid=service_uid,
        )

    def build_job_plan(
        plan: FinalGatePlan,
        guard: MutationGuardEvidence,
    ) -> LifecycleCapacityJobPlan:
        return build_installed_rollout_capacity_job_plan(
            config=config,
            service_uid=service_uid,
            final_plan=plan,
            guard=guard,
            expected_buckets=expected_buckets,
            expected_filesystem_paths=expected_filesystem_paths,
            capacity_source=capacity_source,
            expected_drive_count=expected_drive_count,
            container_registry=container_registry,
        )

    return InstalledRolloutCapacityRefresh(
        config=config,
        service_uid=service_uid,
        commands=commands,
        read_guard=read_guard,
        build_job_plan=build_job_plan,
        read_database=lambda: probe_installed_readonly_database_baseline(
            service_uid=service_uid,
        ),
        now=lambda: datetime.now(UTC),
    )


__all__ = [
    "InstalledRolloutCapacityRefresh",
    "InstalledRolloutCapacityRefreshError",
    "build_installed_rollout_capacity_job_plan",
    "build_installed_rollout_capacity_refresh",
]
