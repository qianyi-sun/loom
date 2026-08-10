"""Installed, digest-approved one-shot staging capacity publication."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.cluster_config import validate_container_registry_prefixes
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.preflight_artifact_store import LoadedPreflightArtifacts
from loom_cli.rollout.readonly_database_authority import ReadonlyDatabaseEvidence

from .config import OperatorConfig
from .lifecycle_capacity_job import (
    LifecycleCapacityJobPlan,
    build_lifecycle_capacity_job_plan,
)
from .model import CandidateBinding

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_JOB_UID_RE = re.compile(r"^[0-9a-f-]{36}$")
_MAX_COMMAND_OUTPUT = 1024 * 1024
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_CAPACITY_FRESHNESS = timedelta(minutes=5)


class CommandResult(Protocol):
    returncode: int
    stdout: str
    stderr: str


class CapacityCommands(Protocol):
    def simple(self, argv: Sequence[str]) -> CommandResult: ...

    def manifest_server_apply(self, rendered: str) -> CommandResult: ...

    def lifecycle_capacity_wait(self, job_name: str) -> CommandResult: ...


class ActiveStore(Protocol):
    def read_active(self) -> object | None: ...


ArtifactLoader = Callable[[CandidateBinding, int], LoadedPreflightArtifacts]
EpochReader = Callable[[], int]
DatabaseReader = Callable[[], ReadonlyDatabaseEvidence]
Clock = Callable[[], datetime]


class InstalledLifecycleCapacityError(RuntimeError):
    """Raised when one-shot capacity publication cannot proceed safely."""


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def _safe_command(result: CommandResult, label: str) -> str:
    if (
        type(result.returncode) is not int
        or not isinstance(result.stdout, str)
        or not isinstance(result.stderr, str)
        or len(result.stdout.encode()) > _MAX_COMMAND_OUTPUT
        or len(result.stderr.encode()) > _MAX_COMMAND_OUTPUT
    ):
        raise InstalledLifecycleCapacityError(f"{label} result is invalid")
    if result.returncode != 0:
        raise InstalledLifecycleCapacityError(f"{label} failed")
    return result.stdout


def _object(payload: str, label: str) -> dict[str, object]:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise InstalledLifecycleCapacityError(f"{label} output is invalid") from exc
    if not isinstance(value, dict):
        raise InstalledLifecycleCapacityError(f"{label} output is invalid")
    return cast(dict[str, object], value)


def _ensure_private_directory(path: Path, *, service_uid: int) -> None:
    try:
        path.mkdir(mode=_PRIVATE_DIRECTORY_MODE, parents=True, exist_ok=True)
    except OSError as exc:
        raise InstalledLifecycleCapacityError("capacity evidence directory is unavailable") from exc
    for current in (path.parent, path):
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise InstalledLifecycleCapacityError(
                "capacity evidence directory is unavailable"
            ) from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != service_uid
            or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
        ):
            raise InstalledLifecycleCapacityError("capacity evidence directory is unsafe")


def _publish_private(path: Path, payload: Mapping[str, object], *, service_uid: int) -> None:
    _ensure_private_directory(path.parent, service_uid=service_uid)
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            _PRIVATE_FILE_MODE,
        )
    except OSError as exc:
        raise InstalledLifecycleCapacityError(
            "capacity evidence already exists or is unsafe"
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != service_uid
            or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
            or metadata.st_nlink != 1
        ):
            raise InstalledLifecycleCapacityError("capacity evidence publication is unsafe")
        payload_bytes = _json_bytes(dict(payload))
        written = 0
        while written < len(payload_bytes):
            written += os.write(descriptor, payload_bytes[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _require_exact_claim(
    path: Path,
    plan: LifecycleCapacityJobPlan,
    *,
    service_uid: int,
) -> None:
    try:
        payload = read_trusted_file(
            path,
            service_uid=service_uid,
            private=True,
            max_bytes=2 * 1024 * 1024,
            require_nonempty=True,
        ).payload
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise InstalledLifecycleCapacityError("capacity plan claim is unavailable") from exc
    if (
        not isinstance(value, dict)
        or set(value) != {"approved_plan_digest", "claimed_at", "plan", "schema_version"}
        or value.get("schema_version") != 1
        or value.get("approved_plan_digest") != plan.plan_digest
        or not isinstance(value.get("claimed_at"), str)
        or not isinstance(value.get("plan"), dict)
    ):
        raise InstalledLifecycleCapacityError("capacity plan claim is invalid")
    try:
        claimed_at = datetime.fromisoformat(cast(str, value["claimed_at"]))
        claimed_plan = LifecycleCapacityJobPlan.from_dict(cast(dict[str, object], value["plan"]))
    except (ValueError, TypeError) as exc:
        raise InstalledLifecycleCapacityError("capacity plan claim is invalid") from exc
    if claimed_at.tzinfo is None or claimed_plan != plan:
        raise InstalledLifecycleCapacityError("capacity plan claim drifted")


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
        or value.get("action") != "capacity"
        or value.get("gc") is not None
        or not isinstance(capacity, dict)
        or set(capacity) != expected_capacity
        or capacity.get("admission_allowed") is not True
        or capacity.get("gc_required") is not False
        or capacity.get("policy_sha256") != staging_capacity_policy_digest()
    ):
        raise InstalledLifecycleCapacityError("capacity Job evidence is incomplete")
    try:
        observed_at = datetime.fromisoformat(str(capacity["observed_at"]))
        model = StagingCapacity(
            object_count=capacity["object_count"],
            bytes_used=capacity["bytes_used"],
            disk_free_percent=capacity["disk_free_percent"],
            inode_free_percent=capacity["inode_free_percent"],
        )
    except (TypeError, ValueError) as exc:
        raise InstalledLifecycleCapacityError("capacity Job evidence is invalid") from exc
    if observed_at.tzinfo is None or capacity.get("evidence_sha256") != model.evidence_digest:
        raise InstalledLifecycleCapacityError("capacity Job evidence is invalid")
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
        raise InstalledLifecycleCapacityError("published capacity timestamp is invalid") from exc
    if (
        database.mutation_epoch != mutation_epoch
        or stored is None
        or any(stored.get(key) != capacity.get(key) for key in keys)
        or stored.get("policy_sha256") != capacity.get("policy_sha256")
        or stored.get("evidence_sha256") != capacity.get("evidence_sha256")
        or stored.get("observed_at_epoch") != int(observed_at.timestamp())
    ):
        raise InstalledLifecycleCapacityError("published capacity database evidence drifted")


class InstalledLifecycleCapacityService:
    """Inventory once, then apply only the exact operator-approved digest."""

    def __init__(
        self,
        *,
        config: OperatorConfig,
        service_uid: int,
        store: ActiveStore,
        bind_candidate: Callable[[], CandidateBinding],
        read_mutation_epoch: EpochReader,
        load_artifacts: ArtifactLoader,
        commands: CapacityCommands,
        read_database: DatabaseReader,
        now: Clock,
        expected_buckets: tuple[str, ...],
        expected_filesystem_paths: tuple[str, ...],
        capacity_source: str = "filesystem",
        container_registry: str = "",
        container_registry_push: str = "",
    ) -> None:
        if (
            service_uid < 1
            or config.source_mode != "sealed-cumulative"
            or config.source_commit_sha is None
            or config.source_tree_sha is None
            or config.source_base_sha is None
            or len(expected_buckets) != 2
            or len(set(expected_buckets)) != 2
            or capacity_source not in {"filesystem", "minio-admin"}
            or (capacity_source == "filesystem" and not expected_filesystem_paths)
            or (capacity_source == "minio-admin" and bool(expected_filesystem_paths))
            or len(set(expected_filesystem_paths)) != len(expected_filesystem_paths)
        ):
            raise InstalledLifecycleCapacityError("sealed lifecycle capacity authority is absent")
        self.config = config
        self.service_uid = service_uid
        self.store = store
        self.bind_candidate = bind_candidate
        self.read_mutation_epoch = read_mutation_epoch
        self.load_artifacts = load_artifacts
        self.commands = commands
        self.read_database = read_database
        self.now = now
        self.expected_buckets = expected_buckets
        self.capacity_source = capacity_source
        self.expected_filesystem_paths = expected_filesystem_paths
        try:
            self.registry_publication = validate_container_registry_prefixes(
                container_registry,
                container_registry_push,
            )
        except ValueError as exc:
            raise InstalledLifecycleCapacityError(
                "lifecycle capacity registry authority is invalid"
            ) from exc
        self.evidence_root = config.state_root / "lifecycle-capacity-jobs"

    def inventory(self) -> LifecycleCapacityJobPlan:
        candidate = self.bind_candidate()
        epoch = self.read_mutation_epoch()
        loaded = self.load_artifacts(candidate, epoch)
        if (
            candidate.resolved_sha != self.config.source_commit_sha
            or candidate.resolved_tree != self.config.source_tree_sha
            or loaded.publication.candidate_sha != candidate.resolved_sha
            or loaded.publication.candidate_tree != candidate.resolved_tree
            or loaded.publication.mutation_epoch != epoch
        ):
            raise InstalledLifecycleCapacityError("capacity artifact identity drifted")
        return build_lifecycle_capacity_job_plan(
            candidate_sha=candidate.resolved_sha,
            candidate_tree=candidate.resolved_tree,
            mutation_epoch=epoch,
            artifact_bundle_sha256=loaded.publication.bundle_digest,
            rendered_manifest_sha256=loaded.manifests.rendered_sha256,
            control_plane_image_id=loaded.images.image_digests["loom-control-plane"],
            image_tag=candidate.image_tag,
            rendered_yaml=loaded.manifests.rendered_yaml,
            expected_buckets=self.expected_buckets,
            expected_filesystem_paths=self.expected_filesystem_paths,
            capacity_source=self.capacity_source,
            container_registry=(
                self.registry_publication[0] if self.registry_publication is not None else ""
            ),
            registry_digest=(
                loaded.images.registry_digests["loom-control-plane"]
                if self.registry_publication is not None
                else ""
            ),
        )

    def prepare_apply(self, *, approved_plan_digest: str) -> LifecycleCapacityJobPlan:
        """Claim one exact plan while the broker holds the short admission lock."""
        if _SHA256_RE.fullmatch(approved_plan_digest) is None:
            raise InstalledLifecycleCapacityError("approved capacity plan digest is invalid")
        plan = self.inventory()
        if plan.plan_digest != approved_plan_digest:
            raise InstalledLifecycleCapacityError("approved capacity plan digest drifted")
        if self.store.read_active() is not None:
            raise InstalledLifecycleCapacityError("an active rollout blocks lifecycle capacity")
        claim = self.evidence_root / f"{plan.plan_digest}.claim.json"
        claimed_at = self.now()
        if claimed_at.tzinfo is None:
            raise InstalledLifecycleCapacityError("capacity clock is invalid")
        _publish_private(
            claim,
            {
                "approved_plan_digest": approved_plan_digest,
                "claimed_at": claimed_at.astimezone(UTC).isoformat(),
                "plan": plan.to_dict(),
                "schema_version": 1,
            },
            service_uid=self.service_uid,
        )
        return plan

    def execute_claimed(self, plan: LifecycleCapacityJobPlan) -> dict[str, object]:
        """Execute a claimed plan outside the short launch lock."""
        claim = self.evidence_root / f"{plan.plan_digest}.claim.json"
        result_path = self.evidence_root / f"{plan.plan_digest}.result.json"
        _require_exact_claim(claim, plan, service_uid=self.service_uid)
        if self.inventory() != plan:
            raise InstalledLifecycleCapacityError("capacity plan drifted before image load")
        image = f"loom-control-plane:{plan.image_tag}"
        if self.registry_publication is None:
            _safe_command(
                self.commands.simple(
                    ("kind", "load", "docker-image", "--name", self.config.cluster_name, image)
                ),
                "exact capacity image load",
            )
        else:
            pull, push = self.registry_publication
            target = f"{push}/{image}"
            manifest = _object(
                _safe_command(
                    self.commands.simple(
                        ("docker", "manifest", "inspect", "--insecure", "--verbose", target)
                    ),
                    "exact capacity image publication readback",
                ),
                "exact capacity image publication readback",
            )
            schema = manifest.get("SchemaV2Manifest", manifest)
            descriptor = manifest.get("Descriptor")
            config = schema.get("config") if isinstance(schema, dict) else None
            if (
                not isinstance(config, dict)
                or config.get("digest") != plan.control_plane_image_id
                or not isinstance(descriptor, dict)
                or descriptor.get("digest") != plan.control_plane_registry_digest
                or f"{pull}/loom-control-plane@{plan.control_plane_registry_digest}"
                not in plan.job_manifest
            ):
                raise InstalledLifecycleCapacityError(
                    "exact capacity image publication identity drifted"
                )
        if self.inventory() != plan:
            raise InstalledLifecycleCapacityError("capacity plan drifted during image load")
        apply_record = _object(
            _safe_command(
                self.commands.manifest_server_apply(plan.job_manifest),
                "capacity Job apply",
            ),
            "capacity Job apply",
        )
        metadata = apply_record.get("metadata")
        applied_uid = metadata.get("uid") if isinstance(metadata, dict) else None
        expected_annotations = {
            "loom.carin.dev/candidate-sha": plan.candidate_sha,
            "loom.carin.dev/candidate-tree": plan.candidate_tree,
            "loom.carin.dev/control-plane-image-id": plan.control_plane_image_id,
            "loom.carin.dev/preflight-artifact": plan.artifact_bundle_sha256,
            "loom.carin.dev/rendered-manifest": plan.rendered_manifest_sha256,
        }
        if (
            not isinstance(metadata, dict)
            or metadata.get("name") != plan.job_name
            or metadata.get("namespace") != plan.namespace
            or metadata.get("labels") != {"app": "loom-staging-data-lifecycle"}
            or metadata.get("annotations") != expected_annotations
            or not isinstance(applied_uid, str)
            or _JOB_UID_RE.fullmatch(applied_uid) is None
        ):
            raise InstalledLifecycleCapacityError("capacity Job apply identity drifted")
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
        observed_at = datetime.fromisoformat(str(capacity["observed_at"]))
        completed_at = self.now()
        if (
            completed_at.tzinfo is None
            or observed_at.tzinfo is None
            or completed_at - observed_at < timedelta(0)
            or completed_at - observed_at > _CAPACITY_FRESHNESS
        ):
            raise InstalledLifecycleCapacityError("capacity Job evidence is stale")
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
        job_metadata = job.get("metadata")
        status = job.get("status")
        uid = job_metadata.get("uid") if isinstance(job_metadata, dict) else None
        conditions = status.get("conditions") if isinstance(status, dict) else None
        if (
            not isinstance(uid, str)
            or _JOB_UID_RE.fullmatch(uid) is None
            or uid != applied_uid
            or not isinstance(job_metadata, dict)
            or job_metadata.get("name") != plan.job_name
            or job_metadata.get("namespace") != plan.namespace
            or job_metadata.get("labels") != {"app": "loom-staging-data-lifecycle"}
            or job_metadata.get("annotations") != expected_annotations
            or not isinstance(status, dict)
            or status.get("succeeded") != 1
            or status.get("failed", 0) not in (0, None)
            or not isinstance(conditions, list)
            or not any(
                isinstance(item, dict)
                and item.get("type") == "Complete"
                and item.get("status") == "True"
                for item in conditions
            )
        ):
            raise InstalledLifecycleCapacityError("capacity Job completion is invalid")
        if self.read_mutation_epoch() != plan.mutation_epoch:
            raise InstalledLifecycleCapacityError(
                "capacity Job changed the protected mutation epoch"
            )
        if self.store.read_active() is not None:
            raise InstalledLifecycleCapacityError(
                "a rollout became active during lifecycle capacity"
            )
        database = self.read_database()
        _verify_database_capacity(database, capacity, mutation_epoch=plan.mutation_epoch)
        evidence: dict[str, object] = {
            "capacity": dict(capacity),
            "database_evidence_sha256": database.evidence_sha256,
            "job_uid": uid,
            "mutation_epoch": plan.mutation_epoch,
            "plan_digest": plan.plan_digest,
            "schema_version": 1,
        }
        evidence["evidence_sha256"] = hashlib.sha256(_json_bytes(evidence).rstrip()).hexdigest()
        _publish_private(result_path, evidence, service_uid=self.service_uid)
        return evidence


__all__ = [
    "InstalledLifecycleCapacityError",
    "InstalledLifecycleCapacityService",
]
