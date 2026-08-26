"""Evidence-first protected references for installed preflight artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.final_attestation_admission import FinalAttestationAdmission
from loom_cli.rollout.lifecycle_protocol import LifecyclePhase
from loom_cli.rollout.preflight_artifact_reference import (
    PreflightArtifactReference,
    PreflightArtifactReferenceError,
)
from loom_cli.rollout.preflight_artifact_retention import PreflightArtifactProtection
from loom_cli.rollout.preflight_contract import (
    CheckOperation,
    PreflightAttestation,
    StageCapability,
)
from loom_cli.rollout.preflight_pipeline import PreflightAssessment

from .backup_job import BackupJobState
from .backup_lease import BackupLease, component_set_digest, evaluate_backup_lease
from .backup_rotation import BackupRotationState
from .config import OperatorConfig
from .final_admission_store import FinalAdmissionStore
from .final_gate_store import FinalGateExecutionStore
from .lifecycle_capacity_job import LifecycleCapacityJobPlan
from .model import (
    ActivePointer,
    DriverEnvelope,
    PreflightRequest,
    RequestEvent,
    RolloutRequest,
)
from .protected_apply_recovery import find_advanced_epoch_attempt
from .store import RequestStoreError

ResumeEligibility = Callable[[str, datetime], bool]
MaintenanceReferenceInventory = Callable[[], tuple[PreflightArtifactProtection, ...]]

_NONTERMINAL_BACKUP_PHASES = frozenset(
    {
        LifecyclePhase.BACKUP_PENDING,
        LifecyclePhase.BACKUP_RUNNING,
        LifecyclePhase.BACKUP_CANCEL_REQUESTED,
        LifecyclePhase.BACKUP_VERIFIED,
        LifecyclePhase.LAUNCH_PENDING,
    }
)
_RESUMABLE_TERMINAL_EVENTS = frozenset({"attempt_failed", "cancelled", "launch_failed"})
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MANIFEST_REQUEST_RE = re.compile(r"^req-manifest-ownership-[a-z0-9]{8,32}$")
_MANIFEST_RESOURCE_NAME_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_CAPACITY_ENTRY_RE = re.compile(r"^(?P<digest>[0-9a-f]{64})\.(?P<kind>claim|result)\.json$")
_PRIVATE_DIRECTORY_MODE = 0o700
_MAX_MAINTENANCE_RECORD_BYTES = 2 * 1024 * 1024
_MANIFEST_EVENT_SEQUENCE = (
    "inventory-approved",
    "epoch-claimed",
    "ownership-adopted",
    "managed-fields-cleaned",
    "network-policies-converged",
    "live-state-verified",
    "completed",
)
_MANIFEST_FAILURE_STAGES = (
    "epoch-claim",
    "ownership-adoption",
    "managed-field-cleanup",
    "network-policy-convergence",
    "live-state-verification",
    "final-no-force-dry-run",
)


class PreflightArtifactReferenceInventoryError(RuntimeError):
    """Raised when installed evidence cannot safely classify artifact references."""


class _ReferenceStore(Protocol):
    def request_ids(self) -> tuple[str, ...]: ...

    def read_preflight_request(self, request_id: str) -> PreflightRequest: ...

    def read_preflight_assessment(self, request_id: str) -> PreflightAssessment: ...

    def read_preflight_backup_job_state(self, request_id: str) -> BackupJobState: ...

    def read_active(self) -> ActivePointer | None: ...

    def read_backup_rotation(self) -> BackupRotationState: ...

    def read_backup_retention_claim(self) -> tuple[str, tuple[str, ...]] | None: ...

    def read_request(self, request_id: str) -> RolloutRequest: ...

    def attempt_numbers(self, request_id: str) -> tuple[int, ...]: ...

    def read_events(self, request_id: str) -> list[RequestEvent]: ...


class _ResumeStore(Protocol):
    def read_request(self, request_id: str) -> RolloutRequest: ...

    def attempt_numbers(self, request_id: str) -> tuple[int, ...]: ...

    def read_attempt_envelope(
        self,
        request_id: str,
        attempt_number: int,
    ) -> DriverEnvelope: ...

    def read_backup_lease(self, digest: str) -> BackupLease: ...


class _AttestationStore(Protocol):
    def read(self, digest: str) -> PreflightAttestation: ...


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _private_directory_metadata(path: Path, *, service_uid: int) -> os.stat_result:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise PreflightArtifactReferenceInventoryError(
            "preflight artifact maintenance directory is unavailable"
        ) from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise PreflightArtifactReferenceInventoryError(
            "preflight artifact maintenance directory is unsafe"
        )
    return metadata


def _optional_private_directory_metadata(
    path: Path,
    *,
    service_uid: int,
) -> os.stat_result | None:
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise PreflightArtifactReferenceInventoryError(
            "preflight artifact maintenance directory is unavailable"
        ) from exc
    return _private_directory_metadata(path, service_uid=service_uid)


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("maintenance evidence contains duplicate fields")
        result[key] = value
    return result


def _read_private_bytes(path: Path, *, service_uid: int) -> bytes:
    try:
        first = read_trusted_file(
            path,
            service_uid=service_uid,
            private=True,
            max_bytes=_MAX_MAINTENANCE_RECORD_BYTES,
            require_nonempty=True,
        )
        second = read_trusted_file(
            path,
            service_uid=service_uid,
            private=True,
            max_bytes=_MAX_MAINTENANCE_RECORD_BYTES,
            require_nonempty=True,
        )
    except (OSError, ValueError) as exc:
        raise PreflightArtifactReferenceInventoryError(
            "preflight artifact maintenance evidence is unreadable"
        ) from exc
    if any(
        observation.metadata.st_uid != service_uid
        or not stat.S_ISREG(observation.metadata.st_mode)
        or stat.S_IMODE(observation.metadata.st_mode) != 0o600
        or observation.metadata.st_nlink != 1
        for observation in (first, second)
    ):
        raise PreflightArtifactReferenceInventoryError(
            "preflight artifact maintenance evidence owner or mode is unsafe"
        )
    if (
        first.payload != second.payload
        or first.metadata_fingerprint != second.metadata_fingerprint
        or first.acl_fingerprint != second.acl_fingerprint
    ):
        raise PreflightArtifactReferenceInventoryError(
            "preflight artifact maintenance evidence changed during inventory"
        )
    return first.payload


def _read_private_json(path: Path, *, service_uid: int) -> dict[str, object]:
    try:
        value = json.loads(
            _read_private_bytes(path, service_uid=service_uid),
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise PreflightArtifactReferenceInventoryError(
            "preflight artifact maintenance evidence is invalid"
        ) from exc
    if not isinstance(value, dict):
        raise PreflightArtifactReferenceInventoryError(
            "preflight artifact maintenance evidence is invalid"
        )
    return value


def _utc_value(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("maintenance timestamp is invalid")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("maintenance timestamp is invalid")
    return parsed.astimezone(UTC)


def _sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _manifest_resource_identity(value: object) -> bool:
    if not isinstance(value, str):
        return False
    parts = value.split("|")
    return (
        len(parts) == 4
        and bool(parts[0])
        and bool(parts[1])
        and parts[2] in {"", "loom-staging"}
        and _MANIFEST_RESOURCE_NAME_RE.fullmatch(parts[3]) is not None
    )


@dataclass(frozen=True, slots=True)
class InstalledMaintenanceReferenceInventory:
    """Read exact in-flight maintenance evidence without trusting filenames alone."""

    config: OperatorConfig
    service_uid: int

    def __post_init__(self) -> None:
        if (
            self.service_uid < 1
            or self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or not self.config.state_root.is_absolute()
            or ".." in self.config.state_root.parts
        ):
            raise ValueError("installed maintenance reference authority is invalid")

    def __call__(self) -> tuple[PreflightArtifactProtection, ...]:
        reasons: dict[str, set[str]] = {}
        try:
            self._manifest_ownership(reasons)
            self._lifecycle_capacity(reasons)
        except PreflightArtifactReferenceInventoryError:
            raise
        except Exception as exc:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact maintenance reference authority is unreadable"
            ) from exc
        return tuple(
            PreflightArtifactProtection(digest, tuple(sorted(found)))
            for digest, found in sorted(reasons.items())
        )

    def _manifest_ownership(self, reasons: dict[str, set[str]]) -> None:
        root = self.config.state_root / "maintenance" / "manifest-ownership"
        before = _optional_private_directory_metadata(root, service_uid=self.service_uid)
        if before is None:
            return
        try:
            names = sorted(os.listdir(root))
        except OSError as exc:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact manifest ownership journal is unreadable"
            ) from exc
        for name in names:
            if _MANIFEST_REQUEST_RE.fullmatch(name) is None:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact manifest ownership journal contains unknown entries"
                )
            request_root = root / name
            request_before = _private_directory_metadata(
                request_root,
                service_uid=self.service_uid,
            )
            try:
                entries = set(os.listdir(request_root))
            except OSError as exc:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact manifest ownership journal is unreadable"
                ) from exc
            if not entries <= {"inventory.json", "events.jsonl"} or "inventory.json" not in entries:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact manifest ownership journal contains unknown entries"
                )
            inventory = _read_private_json(
                request_root / "inventory.json",
                service_uid=self.service_uid,
            )
            bundle_digest = self._validate_manifest_inventory(name, inventory)
            terminal = False
            if "events.jsonl" in entries:
                terminal = self._validate_manifest_events(
                    name,
                    inventory,
                    _read_private_bytes(
                        request_root / "events.jsonl",
                        service_uid=self.service_uid,
                    ),
                )
            request_after = _private_directory_metadata(
                request_root,
                service_uid=self.service_uid,
            )
            if _metadata_identity(request_before) != _metadata_identity(request_after):
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact manifest ownership journal changed during inventory"
                )
            if not terminal:
                reasons.setdefault(bundle_digest, set()).add("manifest-ownership-claim")
        after = _private_directory_metadata(root, service_uid=self.service_uid)
        if _metadata_identity(before) != _metadata_identity(after):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact manifest ownership journal changed during inventory"
            )

    @staticmethod
    def _validate_manifest_inventory(
        request_id: str,
        inventory: Mapping[str, object],
    ) -> str:
        expected = {
            "action",
            "artifact_bundle_sha256",
            "candidate_sha",
            "candidate_tree",
            "dry_run_sha256",
            "inventory_sha256",
            "mutation_epoch",
            "plan_sha256",
            "rendered_manifest_sha256",
            "request_id",
            "resources",
            "schema_version",
        }
        resources = inventory.get("resources")
        digest = inventory.get("artifact_bundle_sha256")
        mutation_epoch = inventory.get("mutation_epoch")
        if (
            set(inventory) != expected
            or inventory.get("schema_version") != 2
            or inventory.get("action") != "inventory"
            or inventory.get("request_id") != request_id
            or not _sha256(digest)
            or not all(
                _sha256(inventory.get(field))
                for field in (
                    "dry_run_sha256",
                    "inventory_sha256",
                    "plan_sha256",
                    "rendered_manifest_sha256",
                )
            )
            or not isinstance(inventory.get("candidate_sha"), str)
            or _SHA_RE.fullmatch(str(inventory["candidate_sha"])) is None
            or not isinstance(inventory.get("candidate_tree"), str)
            or _SHA_RE.fullmatch(str(inventory["candidate_tree"])) is None
            or type(mutation_epoch) is not int
            or mutation_epoch < 0
            or not isinstance(resources, list)
            or not resources
        ):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact manifest ownership inventory is invalid"
            )
        resource_keys = {
            "desired_sha256",
            "generation",
            "identity",
            "live_sha256",
            "managed_fields_sha256",
            "overlay_sha256",
            "resource_version",
            "uid",
        }
        identities: set[str] = set()
        for resource_value in resources:
            if not isinstance(resource_value, Mapping):
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact manifest ownership inventory is invalid"
                )
            resource = resource_value
            generation = resource.get("generation")
            identity = resource.get("identity")
            uid = resource.get("uid")
            resource_version = resource.get("resource_version")
            if (
                set(resource) != resource_keys
                or not _manifest_resource_identity(identity)
                or identity in identities
                or not isinstance(uid, str)
                or not uid
                or not isinstance(resource_version, str)
                or not resource_version.isdigit()
                or (generation is not None and (type(generation) is not int or generation < 1))
                or not all(
                    _sha256(resource.get(field))
                    for field in (
                        "desired_sha256",
                        "live_sha256",
                        "managed_fields_sha256",
                        "overlay_sha256",
                    )
                )
            ):
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact manifest ownership inventory is invalid"
                )
            identities.add(str(identity))
        expected_digest = hashlib.sha256(
            json.dumps(
                {
                    "artifact_bundle_sha256": digest,
                    "dry_run_sha256": inventory["dry_run_sha256"],
                    "plan_sha256": inventory["plan_sha256"],
                    "version": "v2",
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        if inventory["inventory_sha256"] != expected_digest:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact manifest ownership inventory drifted"
            )
        return str(digest)

    @staticmethod
    def _validate_manifest_events(
        request_id: str,
        inventory: Mapping[str, object],
        payload: bytes,
    ) -> bool:
        try:
            lines = payload.splitlines()
            events = [json.loads(line, object_pairs_hook=_reject_duplicate_keys) for line in lines]
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact manifest ownership events are invalid"
            ) from exc
        if not events or any(not isinstance(event, dict) for event in events):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact manifest ownership events are invalid"
            )
        mutation_epoch = inventory.get("mutation_epoch")
        resources = inventory.get("resources")
        if type(mutation_epoch) is not int or not isinstance(resources, list):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact manifest ownership events are invalid"
            )
        claimed_epoch = mutation_epoch + 1
        names: list[str] = []
        observed: list[datetime] = []
        for raw in events:
            event = raw
            evidence = event.get("evidence")
            name = event.get("event")
            try:
                occurred = _utc_value(event.get("observed_at"))
            except ValueError as exc:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact manifest ownership events are invalid"
                ) from exc
            if (
                set(event) != {"event", "evidence", "observed_at", "request_id"}
                or event.get("request_id") != request_id
                or not isinstance(name, str)
                or not isinstance(evidence, Mapping)
            ):
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact manifest ownership events are invalid"
                )
            names.append(name)
            observed.append(occurred)
            if name == "inventory-approved":
                valid = (
                    set(evidence) == {"inventory_sha256", "plan_sha256", "starting_epoch"}
                    and evidence.get("inventory_sha256") == inventory["inventory_sha256"]
                    and evidence.get("plan_sha256") == inventory["plan_sha256"]
                    and evidence.get("starting_epoch") == inventory["mutation_epoch"]
                )
            elif name == "epoch-claimed":
                valid = (
                    set(evidence) == {"observed_epoch"}
                    and evidence.get("observed_epoch") == claimed_epoch
                )
            elif name == "ownership-adopted":
                valid = set(evidence) == {"adoption_sha256"} and _sha256(
                    evidence.get("adoption_sha256")
                )
            elif name == "managed-fields-cleaned":
                valid = (
                    set(evidence) == {"cleanup_count", "cleanup_sha256"}
                    and evidence.get("cleanup_count") == len(resources)
                    and _sha256(evidence.get("cleanup_sha256"))
                )
            elif name == "network-policies-converged":
                valid = set(evidence) == {"network_sha256"} and _sha256(
                    evidence.get("network_sha256")
                )
            elif name == "live-state-verified":
                valid = (
                    set(evidence) == {"attempts", "post_apply_sha256"}
                    and type(evidence.get("attempts")) is int
                    and 1 <= int(evidence["attempts"]) <= 3
                    and _sha256(evidence.get("post_apply_sha256"))
                )
            elif name == "completed":
                valid = (
                    set(evidence) == {"final_dry_run_sha256", "observed_epoch"}
                    and _sha256(evidence.get("final_dry_run_sha256"))
                    and evidence.get("observed_epoch") == claimed_epoch
                )
            elif name == "failed":
                valid = (
                    set(evidence) == {"failure_class", "failure_code"}
                    and isinstance(evidence.get("failure_class"), str)
                    and isinstance(evidence.get("failure_code"), str)
                    and bool(evidence["failure_class"])
                    and bool(evidence["failure_code"])
                )
            else:
                valid = False
            if not valid:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact manifest ownership events are invalid"
                )
        if observed != sorted(observed):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact manifest ownership events are invalid"
            )
        terminal = names[-1] in {"completed", "failed"}
        successful = names if names[-1] != "failed" else names[:-1]
        if (
            tuple(successful) != _MANIFEST_EVENT_SEQUENCE[: len(successful)]
            or ("completed" in names and names[-1] != "completed")
            or ("failed" in names and names[-1] != "failed")
        ):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact manifest ownership events are inconsistent"
            )
        if names[-1] == "failed":
            failure_evidence = events[-1]["evidence"]
            if (
                not isinstance(failure_evidence, Mapping)
                or not 1 <= len(successful) <= len(_MANIFEST_FAILURE_STAGES)
                or failure_evidence.get("failure_code")
                != (f"manifest_ownership.{_MANIFEST_FAILURE_STAGES[len(successful) - 1]}.failed")
            ):
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact manifest ownership events are inconsistent"
                )
        return terminal

    def _lifecycle_capacity(self, reasons: dict[str, set[str]]) -> None:
        root = self.config.state_root / "lifecycle-capacity-jobs"
        before = _optional_private_directory_metadata(root, service_uid=self.service_uid)
        if before is None:
            return
        try:
            names = sorted(os.listdir(root))
        except OSError as exc:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact lifecycle capacity journal is unreadable"
            ) from exc
        grouped: dict[str, set[str]] = {}
        for name in names:
            match = _CAPACITY_ENTRY_RE.fullmatch(name)
            if match is None:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact lifecycle capacity journal contains unknown entries"
                )
            grouped.setdefault(match.group("digest"), set()).add(match.group("kind"))
        for digest, kinds in sorted(grouped.items()):
            if "claim" not in kinds:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact lifecycle capacity result lacks its claim"
                )
            claim = _read_private_json(
                root / f"{digest}.claim.json",
                service_uid=self.service_uid,
            )
            plan = self._validate_capacity_claim(digest, claim)
            if "result" in kinds:
                result = _read_private_json(
                    root / f"{digest}.result.json",
                    service_uid=self.service_uid,
                )
                self._validate_capacity_result(plan, result)
            else:
                reasons.setdefault(plan.artifact_bundle_sha256, set()).add(
                    "lifecycle-capacity-claim"
                )
        after = _private_directory_metadata(root, service_uid=self.service_uid)
        if _metadata_identity(before) != _metadata_identity(after):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact lifecycle capacity journal changed during inventory"
            )

    @staticmethod
    def _validate_capacity_claim(
        digest: str,
        claim: Mapping[str, object],
    ) -> LifecycleCapacityJobPlan:
        plan_value = claim.get("plan")
        if (
            set(claim) != {"approved_plan_digest", "claimed_at", "plan", "schema_version"}
            or claim.get("schema_version") != 1
            or claim.get("approved_plan_digest") != digest
            or not isinstance(plan_value, dict)
        ):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact lifecycle capacity claim is invalid"
            )
        try:
            _utc_value(claim.get("claimed_at"))
            plan = LifecycleCapacityJobPlan.from_dict(plan_value)
        except (TypeError, ValueError) as exc:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact lifecycle capacity claim is invalid"
            ) from exc
        if plan.plan_digest != digest:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact lifecycle capacity claim drifted"
            )
        return plan

    @staticmethod
    def _validate_capacity_result(
        plan: LifecycleCapacityJobPlan,
        result: Mapping[str, object],
    ) -> None:
        expected = {
            "capacity",
            "database_evidence_sha256",
            "evidence_sha256",
            "job_uid",
            "mutation_epoch",
            "plan_digest",
            "schema_version",
        }
        capacity = result.get("capacity")
        capacity_keys = {
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
            set(result) != expected
            or result.get("schema_version") != 1
            or result.get("plan_digest") != plan.plan_digest
            or result.get("mutation_epoch") != plan.mutation_epoch
            or not _sha256(result.get("database_evidence_sha256"))
            or not _sha256(result.get("evidence_sha256"))
            or not isinstance(result.get("job_uid"), str)
            or not result["job_uid"]
            or not isinstance(capacity, Mapping)
            or set(capacity) != capacity_keys
            or capacity.get("admission_allowed") is not True
            or capacity.get("gc_required") is not False
            or not _sha256(capacity.get("evidence_sha256"))
            or not _sha256(capacity.get("policy_sha256"))
            or type(capacity.get("object_count")) is not int
            or type(capacity.get("bytes_used")) is not int
            or isinstance(capacity.get("disk_free_percent"), bool)
            or not isinstance(capacity.get("disk_free_percent"), (int, float))
            or isinstance(capacity.get("inode_free_percent"), bool)
            or not isinstance(capacity.get("inode_free_percent"), (int, float))
        ):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact lifecycle capacity result is invalid"
            )
        try:
            _utc_value(capacity.get("observed_at"))
            capacity_model = StagingCapacity(
                object_count=capacity["object_count"],
                bytes_used=capacity["bytes_used"],
                disk_free_percent=capacity["disk_free_percent"],
                inode_free_percent=capacity["inode_free_percent"],
            )
        except ValueError as exc:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact lifecycle capacity result is invalid"
            ) from exc
        if (
            capacity.get("evidence_sha256") != capacity_model.evidence_digest
            or capacity.get("policy_sha256") != staging_capacity_policy_digest()
        ):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact lifecycle capacity result is invalid"
            )
        without_digest = dict(result)
        evidence_digest = without_digest.pop("evidence_sha256")
        computed = hashlib.sha256(
            json.dumps(without_digest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if evidence_digest != computed:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact lifecycle capacity result drifted"
            )


def resume_binding_matches(
    config: OperatorConfig,
    request: RolloutRequest,
    envelope: DriverEnvelope,
) -> bool:
    """Bind resume to the original request and every protected config input."""
    return (
        request.runner_config_sha256 == config.config_sha256
        and envelope.runner_config_sha256 == config.config_sha256
        and request.preflight_attestation_sha256 == envelope.preflight_attestation_sha256
        and request.preflight_registry_sha256 == envelope.preflight_registry_sha256
        and request.preflight_coverage_sha256 == envelope.preflight_coverage_sha256
        and envelope.request_id == request.request_id
        and envelope.rollout_id == request.rollout_id
        and envelope.initiating_operator == request.caller.username
        and envelope.initiating_uid == request.caller.uid
        and envelope.remote_url == request.candidate.remote_url == config.remote_url
        and envelope.target_ref == request.candidate.target_ref
        and config.target_ref == "refs/heads/dev"
        and envelope.resolved_sha == request.candidate.resolved_sha
        and envelope.image_tag == request.candidate.image_tag
        and envelope.fetched_at == request.candidate.fetched_at
        and envelope.source_mode == request.candidate.source_mode == config.source_mode
        and envelope.resolved_tree == request.candidate.resolved_tree
        and envelope.approved_base_sha == request.candidate.approved_base_sha
        and (
            config.source_mode == "merged-dev"
            or (
                envelope.resolved_sha == config.source_commit_sha
                and envelope.resolved_tree == config.source_tree_sha
                and envelope.approved_base_sha == config.source_base_sha
            )
        )
        and envelope.cluster_name == config.cluster_name
        and envelope.namespace == config.namespace
        and envelope.environment == config.environment
        and envelope.cp_url == config.cp_url
        and envelope.cluster_config_path == str(config.cluster_config_path)
        and envelope.rollout_root == str(config.rollout_root)
        and envelope.admin_token_source == config.admin_token_source
        and envelope.worker_token_source == config.worker_token_source
        and envelope.service_token_source == config.service_token_source
        and envelope.expect_admin_token_fingerprint == config.expect_admin_token_fingerprint
        and envelope.smoke_on_behalf_username == config.smoke_on_behalf_username
        and envelope.smoke_on_behalf_team_id == config.smoke_on_behalf_team_id
        and envelope.scope == config.scope
        and envelope.gb10_prep_concurrency == config.gb10_prep_concurrency
    )


@dataclass(frozen=True, slots=True)
class InstalledResumeEligibility:
    """Evaluate whether an installed failed chain can still resume exactly."""

    config: OperatorConfig
    service_uid: int
    store: _ResumeStore
    attestation_store: _AttestationStore
    read_mutation_epoch: Callable[[], int]

    def __post_init__(self) -> None:
        if (
            self.service_uid < 1
            or self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or not self.config.state_root.is_absolute()
            or ".." in self.config.state_root.parts
        ):
            raise ValueError("installed resume eligibility authority is invalid")

    def __call__(self, request_id: str, now: datetime) -> bool:
        if now.tzinfo is None or now.utcoffset() != timedelta(0):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact resume clock must be UTC"
            )
        request = self.store.read_request(request_id)
        attempts = self.store.attempt_numbers(request_id)
        if not attempts:
            return False
        first = self.store.read_attempt_envelope(request_id, attempts[0])
        latest = self.store.read_attempt_envelope(request_id, attempts[-1])
        if (
            attempts[0] != 1
            or not resume_binding_matches(self.config, request, first)
            or not resume_binding_matches(self.config, request, latest)
            or latest.backup_manifest_path != first.backup_manifest_path
            or latest.backup_manifest_sha256 != first.backup_manifest_sha256
        ):
            return False
        attestation = self.attestation_store.read(request.preflight_attestation_sha256)
        bindings = attestation.bindings
        if (
            attestation.attestation_digest != request.preflight_attestation_sha256
            or attestation.registry_digest != request.preflight_registry_sha256
            or attestation.coverage_digest != request.preflight_coverage_sha256
            or bindings.candidate_sha != request.candidate.resolved_sha
            or bindings.candidate_tree != request.candidate.resolved_tree
            or bindings.runner_config_hash != request.runner_config_sha256
            or bindings.backup_manifest_sha256 != first.backup_manifest_sha256
            or bindings.environment != self.config.environment
            or bindings.namespace != self.config.namespace
        ):
            return False
        lease = self.store.read_backup_lease(bindings.backup_lease_digest)
        if (
            lease.lease_id != bindings.backup_lease_id
            or lease.evidence_digest != bindings.backup_lease_digest
            or lease.manifest_sha256 != bindings.backup_manifest_sha256
            or component_set_digest(lease.component_sha256) != bindings.backup_component_set_digest
        ):
            return False
        post_apply = self._has_post_apply_resume(
            request,
            attempts=attempts,
            attestation=attestation,
        )
        current_epoch = self.read_mutation_epoch()
        if type(current_epoch) is not int or current_epoch < 0:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact resume mutation epoch is invalid"
            )
        lease_eligibility = evaluate_backup_lease(
            lease,
            now=now,
            source_request_id=request.request_id,
            environment=bindings.environment,
            namespace=bindings.namespace,
            mutation_epoch=bindings.staging_mutation_epoch,
            db_snapshot_identity=bindings.db_snapshot_identity,
            schema_revision=bindings.schema_revision,
            object_inventory_root=bindings.object_inventory_root,
            manifest_sha256=bindings.backup_manifest_sha256,
            component_sha256=lease.component_sha256,
        )
        if post_apply:
            return current_epoch == bindings.staging_mutation_epoch + 1
        return (
            current_epoch == bindings.staging_mutation_epoch
            and attestation.issued_at <= now < attestation.expires_at
            and lease_eligibility.eligible
        )

    def _has_post_apply_resume(
        self,
        request: RolloutRequest,
        *,
        attempts: tuple[int, ...],
        attestation: PreflightAttestation,
    ) -> bool:
        for attempt_number in reversed(attempts):
            executions = FinalGateExecutionStore(
                self.config.state_root,
                request_id=request.request_id,
                attempt_number=attempt_number,
                service_uid=self.service_uid,
            ).read_all()
            protected_apply = executions.get("final.protected-apply")
            if protected_apply is None:
                continue
            evidence = protected_apply.evidence
            if (
                not protected_apply.passed
                or protected_apply.tier != 4
                or protected_apply.stage is not StageCapability.FINAL_ONLY
                or protected_apply.operation is not CheckOperation.APPLY
                or evidence.get("ready") is not True
                or evidence.get("candidate-sha") != request.candidate.resolved_sha
                or evidence.get("attestation-digest") != attestation.attestation_digest
                or evidence.get("observed-epoch") != attestation.bindings.staging_mutation_epoch + 1
                or evidence.get("protected-mutation") is not True
                or evidence.get("blockers") != {}
            ):
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact protected apply authority drifted"
                )
            admission: FinalAttestationAdmission = FinalAdmissionStore(
                self.config.state_root,
                request_id=request.request_id,
                attempt_number=attempt_number,
                service_uid=self.service_uid,
            ).read(attestation)
            if admission.attestation != attestation:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact final admission authority drifted"
                )
            return True
        recovery_attempt = find_advanced_epoch_attempt(
            self.config.state_root,
            request_id=request.request_id,
            through_attempt=attempts[-1],
            candidate_sha=request.candidate.resolved_sha,
            attestation_digest=attestation.attestation_digest,
            starting_mutation_epoch=attestation.bindings.staging_mutation_epoch,
            service_uid=self.service_uid,
        )
        if recovery_attempt is None:
            return False
        admission = FinalAdmissionStore(
            self.config.state_root,
            request_id=request.request_id,
            attempt_number=recovery_attempt,
            service_uid=self.service_uid,
        ).read(attestation)
        if admission.attestation != attestation:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact final admission authority drifted"
            )
        return True


def _known_absence(error: RequestStoreError) -> bool:
    return str(error) in {
        "preflight backup job directory does not exist",
        "preflight request does not exist",
        "rollout request is not promoted",
    }


def _event_time(event: RequestEvent) -> datetime:
    try:
        value = datetime.fromisoformat(event.occurred_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PreflightArtifactReferenceInventoryError(
            "preflight artifact event timestamp is invalid"
        ) from exc
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise PreflightArtifactReferenceInventoryError(
            "preflight artifact event timestamp is invalid"
        )
    return value.astimezone(UTC)


def _latest_terminal(events: list[RequestEvent]) -> RequestEvent | None:
    for event in reversed(events):
        if event.event in {
            "attempt_done",
            "attempt_failed",
            "cancelled",
            "launch_failed",
        }:
            return event
    return None


@dataclass(slots=True)
class InstalledPreflightArtifactReferenceInventory:
    """Collect exact publication digests from durable installed evidence."""

    config: OperatorConfig
    service_uid: int
    store: _ReferenceStore
    resume_eligible: ResumeEligibility
    maintenance_references: MaintenanceReferenceInventory

    def __post_init__(self) -> None:
        if (
            self.service_uid < 1
            or self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or self.config.state_root != self.config.state_root.absolute()
            or ".." in self.config.state_root.parts
        ):
            raise ValueError("installed preflight artifact reference authority is invalid")

    def collect(self, *, now: datetime) -> tuple[PreflightArtifactProtection, ...]:
        if now.tzinfo is None or now.utcoffset() != timedelta(0):
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact reference clock must be UTC"
            )
        reasons: dict[str, set[str]] = {}
        request_digests: dict[str, str] = {}
        requests: dict[str, PreflightRequest] = {}
        try:
            request_ids = self.store.request_ids()
        except Exception as exc:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact request inventory is unavailable"
            ) from exc
        for request_id in request_ids:
            try:
                request = self.store.read_preflight_request(request_id)
            except RequestStoreError as exc:
                if _known_absence(exc):
                    continue
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact request authority is unreadable"
                ) from exc
            except Exception as exc:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact request authority is unreadable"
                ) from exc
            try:
                assessment = self.store.read_preflight_assessment(request_id)
            except Exception as exc:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact assessment authority is unreadable"
                ) from exc
            try:
                reference = PreflightArtifactReference.from_assessment(assessment)
            except PreflightArtifactReferenceError as exc:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact publication authority is invalid"
                ) from exc
            requests[request_id] = request
            request_digests[request_id] = reference.bundle_digest
            if request.status == "preview":
                continue
            self._collect_backup_phase(request_id, reference.bundle_digest, reasons)

        active = self.store.read_active()
        if active is not None:
            self._protect_request(
                active.request_id,
                "active-rollout",
                request_digests=request_digests,
                reasons=reasons,
            )
        rotation = self.store.read_backup_rotation()
        if rotation.active is not None:
            self._protect_request(
                rotation.active.request_id,
                "backup-rotation-active",
                request_digests=request_digests,
                reasons=reasons,
            )
        if rotation.candidate is not None:
            self._protect_request(
                rotation.candidate.request_id,
                "backup-rotation-candidate",
                request_digests=request_digests,
                reasons=reasons,
            )
        self._collect_backup_maintenance_claim(
            rotation,
            request_digests=request_digests,
            reasons=reasons,
        )

        completed: list[tuple[datetime, str, str]] = []
        for request_id, request in requests.items():
            if request.status == "preview":
                continue
            try:
                self.store.read_request(request_id)
            except RequestStoreError as exc:
                if _known_absence(exc):
                    continue
                raise PreflightArtifactReferenceInventoryError(
                    "promoted preflight artifact request is unreadable"
                ) from exc
            except Exception as exc:
                raise PreflightArtifactReferenceInventoryError(
                    "promoted preflight artifact request is unreadable"
                ) from exc
            try:
                attempts = self.store.attempt_numbers(request_id)
                events = self.store.read_events(request_id)
            except Exception as exc:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight artifact attempt authority is unreadable"
                ) from exc
            for event in events:
                if event.event == "attempt_done":
                    completed.append((_event_time(event), request_id, request_digests[request_id]))
            terminal = _latest_terminal(events)
            if attempts and terminal is not None and terminal.event in _RESUMABLE_TERMINAL_EVENTS:
                try:
                    eligible = self.resume_eligible(request_id, now)
                except Exception as exc:
                    raise PreflightArtifactReferenceInventoryError(
                        "preflight artifact resume authority is unreadable"
                    ) from exc
                if eligible:
                    reasons.setdefault(request_digests[request_id], set()).add("resume-eligible")
        if completed:
            _time, _request_id, digest = max(completed, key=lambda item: (item[0], item[1]))
            reasons.setdefault(digest, set()).add("current-release")

        try:
            maintenance = self.maintenance_references()
        except Exception as exc:
            raise PreflightArtifactReferenceInventoryError(
                "preflight artifact maintenance reference authority is unreadable"
            ) from exc
        for protection in maintenance:
            reasons.setdefault(protection.bundle_digest, set()).update(protection.reasons)
        return tuple(
            PreflightArtifactProtection(digest, tuple(sorted(found)))
            for digest, found in sorted(reasons.items())
        )

    def _collect_backup_phase(
        self,
        request_id: str,
        bundle_digest: str,
        reasons: dict[str, set[str]],
    ) -> None:
        try:
            state = self.store.read_preflight_backup_job_state(request_id)
        except RequestStoreError as exc:
            if _known_absence(exc):
                return
            raise PreflightArtifactReferenceInventoryError(
                "preflight backup artifact authority is unreadable"
            ) from exc
        if state.phase in _NONTERMINAL_BACKUP_PHASES:
            reasons.setdefault(bundle_digest, set()).add("nonterminal-preflight-backup")
            return
        if state.phase is LifecyclePhase.BACKUP_FAILED:
            try:
                events = self.store.read_events(request_id)
            except Exception as exc:
                raise PreflightArtifactReferenceInventoryError(
                    "preflight backup cleanup authority is unreadable"
                ) from exc
            if not any(event.event == "backup_cleanup_done" for event in events):
                reasons.setdefault(bundle_digest, set()).add("backup-cleanup-pending")

    def _protect_request(
        self,
        request_id: str,
        reason: str,
        *,
        request_digests: dict[str, str],
        reasons: dict[str, set[str]],
    ) -> None:
        try:
            digest = request_digests[request_id]
        except KeyError as exc:
            raise PreflightArtifactReferenceInventoryError(
                "protected rollout lacks preflight artifact evidence"
            ) from exc
        reasons.setdefault(digest, set()).add(reason)

    def _collect_backup_maintenance_claim(
        self,
        rotation: BackupRotationState,
        *,
        request_digests: dict[str, str],
        reasons: dict[str, set[str]],
    ) -> None:
        claim = self.store.read_backup_retention_claim()
        if claim is None:
            return
        _plan_digest, payload_ids = claim
        if not payload_ids:
            claimed = rotation.candidate or rotation.active
            if claimed is None:
                raise PreflightArtifactReferenceInventoryError(
                    "backup maintenance artifact claim is inconsistent"
                )
            self._protect_request(
                claimed.request_id,
                (
                    "backup-recovery-claim"
                    if rotation.candidate is not None
                    else "backup-retention-claim"
                ),
                request_digests=request_digests,
                reasons=reasons,
            )
            return
        records = tuple(
            item
            for item in (rotation.active, rotation.candidate, *rotation.retirements)
            if item is not None
        )
        by_payload = {item.payload_id: item for item in records}
        if len(by_payload) != len(records) or not set(payload_ids).issubset(by_payload):
            raise PreflightArtifactReferenceInventoryError(
                "backup maintenance artifact claim is inconsistent"
            )
        for payload_id in payload_ids:
            self._protect_request(
                by_payload[payload_id].request_id,
                "backup-retention-claim",
                request_digests=request_digests,
                reasons=reasons,
            )


__all__ = [
    "InstalledMaintenanceReferenceInventory",
    "InstalledPreflightArtifactReferenceInventory",
    "InstalledResumeEligibility",
    "MaintenanceReferenceInventory",
    "PreflightArtifactReferenceInventoryError",
    "ResumeEligibility",
    "resume_binding_matches",
]
