"""Immutable handoff from deep preflight to installed final-gate actions."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TypedDict, cast
from uuid import uuid4

from loom_cli.rollout.external_supervisor_readiness import SCRIPT_PATH
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactPublication
from loom_cli.rollout.preflight_contract import (
    EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
    PreflightAttestation,
    external_supervisor_transition_digest,
    external_supervisor_unit_set_digest,
    external_supervisor_unit_set_digest_or_empty,
)
from loom_cli.rollout.systemd_unit_readiness import UNIT_PATHS

from .backup_lease import BackupLease, component_set_digest
from .model import DriverEnvelope, driver_envelope_sha256, validate_safe_identifier
from .protected_apply_baseline import ProtectedApplyBaseline

_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_PLAN_BYTES = 2 * 1024 * 1024
_PROTECTED_BASELINE_IDS = frozenset(
    {
        "staging.health",
        "staging.auth",
        "staging.catalog-task",
        "staging.storage-db",
        "staging.network",
        "staging.release-baseline",
    }
)


class _SystemdEvidence(TypedDict):
    supervisor_artifact_digest: str
    supervisor_profile_sha256: str
    supervisor_script_digests: dict[str, str]
    supervisor_unit_digests: dict[str, str]
    supervisor_unit_set_digest: str
    systemd_unit_digests: dict[str, str]
    systemd_unit_set_digest: str


class _PredecessorEvidence(TypedDict):
    kind: str
    authority_digest: str
    pointer_digest: str
    unit_sha256: dict[str, str]
    unit_set_digest: str
    live_evidence_digest: str
    pending_transition_digest: str


class FinalGatePlanError(RuntimeError):
    """Raised when an immutable final plan is unsafe or inconsistent."""


@dataclass(frozen=True, slots=True)
class FinalGatePlan:
    """All non-secret authority consumed after protected mutation begins."""

    schema_version: int
    request_id: str
    rollout_id: str
    attempt_number: int
    request_envelope_sha256: str
    source_mode: str
    candidate_sha: str
    candidate_tree: str
    approved_base_sha: str | None
    attestation_digest: str
    registry_digest: str
    coverage_digest: str
    starting_mutation_epoch: int
    artifact_bundle_digest: str
    artifact_descriptor_path: str
    rendered_manifest_path: str
    rendered_manifest_sha256: str
    migration_manifest_path: str
    migration_manifest_sha256: str
    migration_manifest_artifact_sha256: str
    production_defaults_path: str
    production_defaults_sha256: str
    migration_job_name: str
    migration_image_id: str
    image_digests: Mapping[str, str]
    migration_plan_digest: str
    migration_target_revision: str
    browser_image_digest: str
    browser_report_schema: str
    backup_manifest_path: str
    backup_manifest_sha256: str
    backup_lease_id: str
    backup_source_request_id: str
    backup_lease_digest: str
    backup_component_set_digest: str
    db_snapshot_identity: str
    object_inventory_root: str
    schema_revision: str
    environment: str
    namespace: str
    route: str
    service_token_source: str
    runner_source_sha: str
    runner_source_tree: str
    runner_install_hash: str
    runner_config_hash: str
    secret_metadata_fingerprints: Mapping[str, str]
    gb10_inventory_digest: str
    gb10_boot_ids: Mapping[str, str]
    gb10_mount_digest: str
    gb10_unit_digest: str
    supervisor_artifact_digest: str
    supervisor_profile_sha256: str
    supervisor_script_digests: Mapping[str, str]
    systemd_unit_digests: Mapping[str, str]
    systemd_unit_set_digest: str
    supervisor_predecessor_kind: str
    supervisor_predecessor_digest: str
    supervisor_predecessor_pointer_digest: str
    supervisor_predecessor_unit_sha256: Mapping[str, str]
    supervisor_predecessor_unit_set_digest: str
    supervisor_predecessor_live_evidence_digest: str
    supervisor_predecessor_pending_transition_digest: str
    supervisor_transition_digest: str
    check_implementation_digests: Mapping[str, str]
    evidence_hashes: Mapping[str, str]
    protected_baseline_digest: str
    protected_baseline_resource_digests: Mapping[str, str]
    plan_digest: str

    def __post_init__(self) -> None:
        validate_safe_identifier(self.request_id, "request_id")
        validate_safe_identifier(self.rollout_id, "rollout_id")
        if (
            self.schema_version != 3
            or type(self.attempt_number) is not int
            or self.attempt_number < 1
            or self.source_mode not in {"merged-dev", "sealed-cumulative"}
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or type(self.starting_mutation_epoch) is not int
            or self.starting_mutation_epoch < 0
            or self.environment != "staging"
            or not self.namespace
            or not self.route.startswith("https://")
        ):
            raise ValueError("final gate plan identity is invalid")
        if self.source_mode == "sealed-cumulative":
            if self.approved_base_sha is None or _SHA_RE.fullmatch(self.approved_base_sha) is None:
                raise ValueError("sealed final gate plan base is invalid")
        elif self.approved_base_sha is not None:
            raise ValueError("merged final gate plan cannot bind an approved base")
        digest_fields = (
            self.request_envelope_sha256,
            self.attestation_digest,
            self.registry_digest,
            self.coverage_digest,
            self.artifact_bundle_digest,
            self.rendered_manifest_sha256,
            self.migration_manifest_sha256,
            self.migration_manifest_artifact_sha256,
            self.production_defaults_sha256,
            self.migration_plan_digest,
            self.browser_report_schema,
            self.backup_manifest_sha256,
            self.backup_lease_digest,
            self.backup_component_set_digest,
            self.object_inventory_root,
            self.runner_install_hash,
            self.runner_config_hash,
            self.gb10_inventory_digest,
            self.gb10_mount_digest,
            self.gb10_unit_digest,
            self.supervisor_artifact_digest,
            self.supervisor_profile_sha256,
            self.systemd_unit_set_digest,
            self.supervisor_predecessor_digest,
            self.supervisor_predecessor_pointer_digest,
            self.supervisor_predecessor_unit_set_digest,
            self.supervisor_predecessor_live_evidence_digest,
            self.supervisor_predecessor_pending_transition_digest,
            self.supervisor_transition_digest,
            self.protected_baseline_digest,
            self.plan_digest,
        )
        if any(_SHA256_RE.fullmatch(value) is None for value in digest_fields):
            raise ValueError("final gate plan digest is invalid")
        for value in (
            self.artifact_descriptor_path,
            self.rendered_manifest_path,
            self.migration_manifest_path,
            self.production_defaults_path,
            self.backup_manifest_path,
        ):
            path = Path(value)
            if not path.is_absolute() or ".." in path.parts:
                raise ValueError("final gate plan path is invalid")
        if not self.service_token_source.startswith("file:"):
            raise ValueError("final gate service token source is invalid")
        service_token_path = Path(self.service_token_source.removeprefix("file:"))
        if (
            not service_token_path.is_absolute()
            or ".." in service_token_path.parts
            or any(character in str(service_token_path) for character in ("\n", "\r", "\x00"))
        ):
            raise ValueError("final gate service token source is invalid")
        if Path(self.artifact_descriptor_path).parent != Path(self.rendered_manifest_path).parent:
            raise ValueError("final gate preflight artifact roots differ")
        if Path(self.artifact_descriptor_path).parent != Path(self.migration_manifest_path).parent:
            raise ValueError("final gate migration artifact root differs")
        if Path(self.artifact_descriptor_path).parent != Path(self.production_defaults_path).parent:
            raise ValueError("final gate production defaults artifact root differs")
        if not self.migration_job_name or self.migration_image_id != self.image_digests.get(
            "loom-control-plane"
        ):
            raise ValueError("final gate migration artifact identity is invalid")
        if not self.migration_target_revision or not self.db_snapshot_identity:
            raise ValueError("final gate migration or snapshot identity is missing")
        if not self.backup_lease_id or not self.schema_revision:
            raise ValueError("final gate checkpoint identity is missing")
        validate_safe_identifier(self.backup_source_request_id, "backup_source_request_id")
        if (
            _SHA_RE.fullmatch(self.runner_source_sha) is None
            or _SHA_RE.fullmatch(self.runner_source_tree) is None
        ):
            raise ValueError("final gate runner source identity is invalid")
        maps = {
            "image": self.image_digests,
            "secret metadata": self.secret_metadata_fingerprints,
            "GB10 boot": self.gb10_boot_ids,
            "supervisor script": self.supervisor_script_digests,
            "systemd unit": self.systemd_unit_digests,
            "supervisor predecessor unit": self.supervisor_predecessor_unit_sha256,
            "check implementation": self.check_implementation_digests,
            "evidence": self.evidence_hashes,
            "protected baseline": self.protected_baseline_resource_digests,
        }
        for label, values in maps.items():
            if (
                not values
                or len(values) > 256
                or not all(
                    isinstance(key, str)
                    and key
                    and isinstance(value, str)
                    and value
                    and len(key) <= 128
                    and len(value) <= 256
                    for key, value in values.items()
                )
            ):
                raise ValueError(f"final gate {label} map is invalid")
        if any(
            not value.startswith("sha256:")
            or _SHA256_RE.fullmatch(value.removeprefix("sha256:")) is None
            for value in self.image_digests.values()
        ):
            raise ValueError("final gate image digest map is invalid")
        if (
            not self.browser_image_digest.startswith("sha256:")
            or _SHA256_RE.fullmatch(self.browser_image_digest.removeprefix("sha256:")) is None
            or self.browser_image_digest not in self.image_digests.values()
        ):
            raise ValueError("final gate browser image digest is invalid")
        if any(
            _SHA256_RE.fullmatch(value) is None
            for values in (
                self.supervisor_script_digests,
                self.systemd_unit_digests,
                self.supervisor_predecessor_unit_sha256,
                self.check_implementation_digests,
                self.evidence_hashes,
                self.protected_baseline_resource_digests,
            )
            for value in values.values()
        ):
            raise ValueError("final gate check digest map is invalid")
        unit_names = set(self.systemd_unit_digests)
        dynamic_services = {
            name for name in unit_names - set(UNIT_PATHS) if name.endswith(".service")
        }
        dynamic_timers = {name for name in unit_names - set(UNIT_PATHS) if name.endswith(".timer")}
        target_unit_sha256 = {
            name: digest
            for name, digest in self.systemd_unit_digests.items()
            if name not in UNIT_PATHS
        }
        if (
            not set(UNIT_PATHS).issubset(unit_names)
            or not dynamic_services
            or len(dynamic_services) != len(dynamic_timers)
            or {name.removesuffix(".service") for name in dynamic_services}
            != {name.removesuffix(".timer") for name in dynamic_timers}
            or any("/" in name or "\\" in name for name in dynamic_services | dynamic_timers)
            or set(self.supervisor_script_digests) != {SCRIPT_PATH}
            or self.systemd_unit_set_digest
            != _hash_json({"failed": {}, "units": dict(self.systemd_unit_digests)})
        ):
            raise ValueError("final gate systemd supervisor coverage is invalid")
        # An absent predecessor (first introduction of the supervisor) carries no
        # units and the absent authority/pointer digests; a present predecessor
        # (legacy-manifest or canonical) carries a complete paired unit set.
        supervisor_predecessor_absent = self.supervisor_predecessor_kind == "absent"
        if (
            self.supervisor_predecessor_kind not in {"legacy-manifest", "canonical", "absent"}
            or bool(self.supervisor_predecessor_unit_sha256) == supervisor_predecessor_absent
            or (self.supervisor_predecessor_digest == EXTERNAL_SUPERVISOR_ABSENT_DIGEST)
            != supervisor_predecessor_absent
            or external_supervisor_unit_set_digest_or_empty(self.supervisor_predecessor_unit_sha256)
            != self.supervisor_predecessor_unit_set_digest
            or (
                self.supervisor_predecessor_kind == "legacy-manifest"
                and self.supervisor_predecessor_pointer_digest != EXTERNAL_SUPERVISOR_ABSENT_DIGEST
            )
            or (
                self.supervisor_predecessor_kind == "canonical"
                and self.supervisor_predecessor_pointer_digest == EXTERNAL_SUPERVISOR_ABSENT_DIGEST
            )
            or (
                supervisor_predecessor_absent
                and self.supervisor_predecessor_pointer_digest != EXTERNAL_SUPERVISOR_ABSENT_DIGEST
            )
        ):
            raise ValueError("final gate supervisor predecessor authority is invalid")
        expected_transition = external_supervisor_transition_digest(
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            environment=self.environment,
            predecessor_kind=self.supervisor_predecessor_kind,
            predecessor_digest=self.supervisor_predecessor_digest,
            predecessor_pointer_digest=self.supervisor_predecessor_pointer_digest,
            predecessor_unit_sha256=self.supervisor_predecessor_unit_sha256,
            predecessor_unit_set_digest=self.supervisor_predecessor_unit_set_digest,
            predecessor_live_evidence_digest=(self.supervisor_predecessor_live_evidence_digest),
            predecessor_pending_transition_digest=(
                self.supervisor_predecessor_pending_transition_digest
            ),
            target_artifact_digest=self.supervisor_artifact_digest,
            target_profile_sha256=self.supervisor_profile_sha256,
            target_script_sha256=self.supervisor_script_digests,
            target_unit_sha256=target_unit_sha256,
            target_unit_set_digest=external_supervisor_unit_set_digest(target_unit_sha256),
        )
        if expected_transition != self.supervisor_transition_digest:
            raise ValueError("final gate supervisor transition identity drifted")
        if self.check_implementation_digests.keys() != self.evidence_hashes.keys():
            raise ValueError("final gate check evidence maps differ")
        if self.protected_baseline_resource_digests.keys() != _PROTECTED_BASELINE_IDS:
            raise ValueError("final gate protected baseline coverage differs")
        if any(
            not value.startswith("sha256:") or len(value) > 96
            for value in self.secret_metadata_fingerprints.values()
        ):
            raise ValueError("final gate secret metadata is invalid")
        object.__setattr__(self, "image_digests", MappingProxyType(dict(self.image_digests)))
        object.__setattr__(
            self,
            "secret_metadata_fingerprints",
            MappingProxyType(dict(self.secret_metadata_fingerprints)),
        )
        object.__setattr__(self, "gb10_boot_ids", MappingProxyType(dict(self.gb10_boot_ids)))
        object.__setattr__(
            self,
            "supervisor_script_digests",
            MappingProxyType(dict(self.supervisor_script_digests)),
        )
        object.__setattr__(
            self,
            "systemd_unit_digests",
            MappingProxyType(dict(self.systemd_unit_digests)),
        )
        object.__setattr__(
            self,
            "supervisor_predecessor_unit_sha256",
            MappingProxyType(dict(self.supervisor_predecessor_unit_sha256)),
        )
        object.__setattr__(
            self,
            "check_implementation_digests",
            MappingProxyType(dict(self.check_implementation_digests)),
        )
        object.__setattr__(self, "evidence_hashes", MappingProxyType(dict(self.evidence_hashes)))
        object.__setattr__(
            self,
            "protected_baseline_resource_digests",
            MappingProxyType(dict(self.protected_baseline_resource_digests)),
        )

    @classmethod
    def build(
        cls,
        envelope: DriverEnvelope,
        attestation: PreflightAttestation,
        artifacts: PreflightArtifactPublication,
        lease: BackupLease,
        baseline: ProtectedApplyBaseline,
        systemd_evidence: Mapping[str, object],
        predecessor_evidence: Mapping[str, object],
    ) -> FinalGatePlan:
        bindings = attestation.bindings
        systemd = _parse_systemd_evidence(systemd_evidence)
        predecessor = _parse_external_supervisor_predecessor_evidence(predecessor_evidence)
        supervisor_transition = external_supervisor_transition_digest(
            candidate_sha=bindings.candidate_sha,
            candidate_tree=bindings.candidate_tree,
            environment=bindings.environment,
            predecessor_kind=predecessor["kind"],
            predecessor_digest=predecessor["authority_digest"],
            predecessor_pointer_digest=predecessor["pointer_digest"],
            predecessor_unit_sha256=predecessor["unit_sha256"],
            predecessor_unit_set_digest=predecessor["unit_set_digest"],
            predecessor_live_evidence_digest=predecessor["live_evidence_digest"],
            predecessor_pending_transition_digest=predecessor["pending_transition_digest"],
            target_artifact_digest=systemd["supervisor_artifact_digest"],
            target_profile_sha256=systemd["supervisor_profile_sha256"],
            target_script_sha256=systemd["supervisor_script_digests"],
            target_unit_sha256=systemd["supervisor_unit_digests"],
            target_unit_set_digest=systemd["supervisor_unit_set_digest"],
        )
        if (
            envelope.preflight_attestation_sha256 != attestation.attestation_digest
            or envelope.preflight_registry_sha256 != attestation.registry_digest
            or envelope.preflight_coverage_sha256 != attestation.coverage_digest
            or envelope.resolved_sha != bindings.candidate_sha
            or (
                envelope.resolved_tree is not None
                and envelope.resolved_tree != bindings.candidate_tree
            )
            or envelope.backup_manifest_sha256 != bindings.backup_manifest_sha256
            or envelope.runner_config_sha256 != bindings.runner_config_hash
            or envelope.environment != bindings.environment
            or envelope.namespace != bindings.namespace
            or artifacts.candidate_sha != bindings.candidate_sha
            or artifacts.candidate_tree != bindings.candidate_tree
            or artifacts.mutation_epoch != bindings.staging_mutation_epoch
            or artifacts.migration_plan_sha256 != bindings.migration_plan_digest
            or artifacts.migration_image_id != bindings.image_digests.get("loom-control-plane")
            or artifacts.browser_report_schema_sha256 != bindings.browser_report_schema
            or lease.lease_id != bindings.backup_lease_id
            or lease.evidence_digest != bindings.backup_lease_digest
            or lease.manifest_sha256 != bindings.backup_manifest_sha256
            or component_set_digest(lease.component_sha256) != bindings.backup_component_set_digest
            or lease.environment != bindings.environment
            or lease.namespace != bindings.namespace
            or lease.mutation_epoch != bindings.staging_mutation_epoch
            or lease.db_snapshot_identity != bindings.db_snapshot_identity
            or lease.schema_revision != bindings.schema_revision
            or lease.object_inventory_root != bindings.object_inventory_root
            or baseline.environment != bindings.environment
            or baseline.namespace != bindings.namespace
            or baseline.mutation_epoch != bindings.staging_mutation_epoch
            or predecessor["kind"] != bindings.supervisor_predecessor_kind
            or predecessor["authority_digest"] != bindings.supervisor_predecessor_digest
            or predecessor["pointer_digest"] != bindings.supervisor_predecessor_pointer_digest
            or predecessor["unit_sha256"] != dict(bindings.supervisor_predecessor_unit_sha256)
            or predecessor["unit_set_digest"] != bindings.supervisor_predecessor_unit_set_digest
            or predecessor["live_evidence_digest"]
            != bindings.supervisor_predecessor_live_evidence_digest
            or predecessor["pending_transition_digest"]
            != bindings.supervisor_predecessor_pending_transition_digest
            or supervisor_transition != bindings.supervisor_transition_digest
        ):
            raise ValueError("final gate plan inputs drifted")
        payload = {
            "schema_version": 3,
            "request_id": envelope.request_id,
            "rollout_id": envelope.rollout_id,
            "attempt_number": envelope.attempt_number,
            "request_envelope_sha256": driver_envelope_sha256(envelope),
            "source_mode": envelope.source_mode,
            "candidate_sha": bindings.candidate_sha,
            "candidate_tree": bindings.candidate_tree,
            "approved_base_sha": envelope.approved_base_sha,
            "attestation_digest": attestation.attestation_digest,
            "registry_digest": attestation.registry_digest,
            "coverage_digest": attestation.coverage_digest,
            "starting_mutation_epoch": bindings.staging_mutation_epoch,
            "artifact_bundle_digest": artifacts.bundle_digest,
            "artifact_descriptor_path": str(artifacts.descriptor_path),
            "rendered_manifest_path": str(artifacts.rendered_manifest_path),
            "rendered_manifest_sha256": artifacts.rendered_manifest_sha256,
            "migration_manifest_path": str(artifacts.migration_manifest_path),
            "migration_manifest_sha256": artifacts.migration_manifest_sha256,
            "migration_manifest_artifact_sha256": artifacts.migration_manifest_artifact_sha256,
            "production_defaults_path": str(artifacts.production_defaults_path),
            "production_defaults_sha256": artifacts.production_defaults_sha256,
            "migration_job_name": artifacts.migration_job_name,
            "migration_image_id": artifacts.migration_image_id,
            "image_digests": dict(bindings.image_digests),
            "migration_plan_digest": bindings.migration_plan_digest,
            "migration_target_revision": artifacts.migration_target_revision,
            "browser_image_digest": bindings.browser_image_digest,
            "browser_report_schema": bindings.browser_report_schema,
            "backup_manifest_path": envelope.backup_manifest_path,
            "backup_manifest_sha256": bindings.backup_manifest_sha256,
            "backup_lease_id": bindings.backup_lease_id,
            "backup_source_request_id": lease.source_request_id,
            "backup_lease_digest": bindings.backup_lease_digest,
            "backup_component_set_digest": bindings.backup_component_set_digest,
            "db_snapshot_identity": bindings.db_snapshot_identity,
            "object_inventory_root": bindings.object_inventory_root,
            "schema_revision": bindings.schema_revision,
            "environment": bindings.environment,
            "namespace": bindings.namespace,
            "route": bindings.route,
            "service_token_source": envelope.service_token_source,
            "runner_source_sha": bindings.runner_source_sha,
            "runner_source_tree": bindings.runner_source_tree,
            "runner_install_hash": bindings.runner_install_hash,
            "runner_config_hash": bindings.runner_config_hash,
            "secret_metadata_fingerprints": dict(bindings.secret_metadata_fingerprints),
            "gb10_inventory_digest": bindings.gb10_inventory_digest,
            "gb10_boot_ids": dict(bindings.gb10_boot_ids),
            "gb10_mount_digest": bindings.gb10_mount_digest,
            "gb10_unit_digest": bindings.gb10_unit_digest,
            "supervisor_artifact_digest": systemd["supervisor_artifact_digest"],
            "supervisor_profile_sha256": systemd["supervisor_profile_sha256"],
            "supervisor_script_digests": systemd["supervisor_script_digests"],
            "systemd_unit_digests": systemd["systemd_unit_digests"],
            "systemd_unit_set_digest": systemd["systemd_unit_set_digest"],
            "supervisor_predecessor_kind": predecessor["kind"],
            "supervisor_predecessor_digest": predecessor["authority_digest"],
            "supervisor_predecessor_pointer_digest": predecessor["pointer_digest"],
            "supervisor_predecessor_unit_sha256": predecessor["unit_sha256"],
            "supervisor_predecessor_unit_set_digest": predecessor["unit_set_digest"],
            "supervisor_predecessor_live_evidence_digest": predecessor["live_evidence_digest"],
            "supervisor_predecessor_pending_transition_digest": predecessor[
                "pending_transition_digest"
            ],
            "supervisor_transition_digest": supervisor_transition,
            "check_implementation_digests": dict(attestation.check_implementation_digests),
            "evidence_hashes": dict(attestation.evidence_hashes),
            "protected_baseline_digest": baseline.baseline_digest,
            "protected_baseline_resource_digests": dict(baseline.resource_digests),
        }
        return cls.from_dict({**payload, "plan_digest": _hash_json(payload)})

    def to_dict(self) -> dict[str, object]:
        return _plan_payload(self, include_digest=True)

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> FinalGatePlan:
        expected = set(cls.__dataclass_fields__)
        if set(value) != expected:
            raise ValueError("final gate plan fields are invalid")
        plan = cls(
            schema_version=_integer(value, "schema_version"),
            request_id=_string(value, "request_id"),
            rollout_id=_string(value, "rollout_id"),
            attempt_number=_integer(value, "attempt_number"),
            request_envelope_sha256=_string(value, "request_envelope_sha256"),
            source_mode=_string(value, "source_mode"),
            candidate_sha=_string(value, "candidate_sha"),
            candidate_tree=_string(value, "candidate_tree"),
            approved_base_sha=_optional_string(value, "approved_base_sha"),
            attestation_digest=_string(value, "attestation_digest"),
            registry_digest=_string(value, "registry_digest"),
            coverage_digest=_string(value, "coverage_digest"),
            starting_mutation_epoch=_integer(value, "starting_mutation_epoch"),
            artifact_bundle_digest=_string(value, "artifact_bundle_digest"),
            artifact_descriptor_path=_string(value, "artifact_descriptor_path"),
            rendered_manifest_path=_string(value, "rendered_manifest_path"),
            rendered_manifest_sha256=_string(value, "rendered_manifest_sha256"),
            migration_manifest_path=_string(value, "migration_manifest_path"),
            migration_manifest_sha256=_string(value, "migration_manifest_sha256"),
            migration_manifest_artifact_sha256=_string(value, "migration_manifest_artifact_sha256"),
            production_defaults_path=_string(value, "production_defaults_path"),
            production_defaults_sha256=_string(value, "production_defaults_sha256"),
            migration_job_name=_string(value, "migration_job_name"),
            migration_image_id=_string(value, "migration_image_id"),
            image_digests=_string_map(value, "image_digests"),
            migration_plan_digest=_string(value, "migration_plan_digest"),
            migration_target_revision=_string(value, "migration_target_revision"),
            browser_image_digest=_string(value, "browser_image_digest"),
            browser_report_schema=_string(value, "browser_report_schema"),
            backup_manifest_path=_string(value, "backup_manifest_path"),
            backup_manifest_sha256=_string(value, "backup_manifest_sha256"),
            backup_lease_id=_string(value, "backup_lease_id"),
            backup_source_request_id=_string(value, "backup_source_request_id"),
            backup_lease_digest=_string(value, "backup_lease_digest"),
            backup_component_set_digest=_string(value, "backup_component_set_digest"),
            db_snapshot_identity=_string(value, "db_snapshot_identity"),
            object_inventory_root=_string(value, "object_inventory_root"),
            schema_revision=_string(value, "schema_revision"),
            environment=_string(value, "environment"),
            namespace=_string(value, "namespace"),
            route=_string(value, "route"),
            service_token_source=_string(value, "service_token_source"),
            runner_source_sha=_string(value, "runner_source_sha"),
            runner_source_tree=_string(value, "runner_source_tree"),
            runner_install_hash=_string(value, "runner_install_hash"),
            runner_config_hash=_string(value, "runner_config_hash"),
            secret_metadata_fingerprints=_string_map(value, "secret_metadata_fingerprints"),
            gb10_inventory_digest=_string(value, "gb10_inventory_digest"),
            gb10_boot_ids=_string_map(value, "gb10_boot_ids"),
            gb10_mount_digest=_string(value, "gb10_mount_digest"),
            gb10_unit_digest=_string(value, "gb10_unit_digest"),
            supervisor_artifact_digest=_string(value, "supervisor_artifact_digest"),
            supervisor_profile_sha256=_string(value, "supervisor_profile_sha256"),
            supervisor_script_digests=_string_map(value, "supervisor_script_digests"),
            systemd_unit_digests=_string_map(value, "systemd_unit_digests"),
            systemd_unit_set_digest=_string(value, "systemd_unit_set_digest"),
            supervisor_predecessor_kind=_string(value, "supervisor_predecessor_kind"),
            supervisor_predecessor_digest=_string(value, "supervisor_predecessor_digest"),
            supervisor_predecessor_pointer_digest=_string(
                value, "supervisor_predecessor_pointer_digest"
            ),
            supervisor_predecessor_unit_sha256=_string_map(
                value, "supervisor_predecessor_unit_sha256"
            ),
            supervisor_predecessor_unit_set_digest=_string(
                value, "supervisor_predecessor_unit_set_digest"
            ),
            supervisor_predecessor_live_evidence_digest=_string(
                value, "supervisor_predecessor_live_evidence_digest"
            ),
            supervisor_predecessor_pending_transition_digest=_string(
                value, "supervisor_predecessor_pending_transition_digest"
            ),
            supervisor_transition_digest=_string(value, "supervisor_transition_digest"),
            check_implementation_digests=_string_map(value, "check_implementation_digests"),
            evidence_hashes=_string_map(value, "evidence_hashes"),
            protected_baseline_digest=_string(value, "protected_baseline_digest"),
            protected_baseline_resource_digests=_string_map(
                value, "protected_baseline_resource_digests"
            ),
            plan_digest=_string(value, "plan_digest"),
        )
        if _hash_json(_plan_payload(plan, include_digest=False)) != plan.plan_digest:
            raise ValueError("final gate plan content digest drifted")
        return plan


class FinalGatePlanStore:
    """Publish one exact final plan under an existing request attempt."""

    def __init__(
        self,
        state_root: Path,
        *,
        request_id: str,
        attempt_number: int,
        service_uid: int | None = None,
    ) -> None:
        self.service_uid = os.geteuid() if service_uid is None else service_uid
        self.request_id = validate_safe_identifier(request_id, "request_id")
        self.attempt_number = attempt_number
        if (
            not state_root.is_absolute()
            or ".." in state_root.parts
            or type(attempt_number) is not int
            or attempt_number < 1
            or self.service_uid < 0
        ):
            raise FinalGatePlanError("final gate plan store authority is invalid")
        self.attempt_root = (
            state_root / "requests" / self.request_id / "attempts" / str(attempt_number)
        )
        self.path = self.attempt_root / "final-gate-plan.json"

    def publish(self, plan: FinalGatePlan) -> Path:
        if plan.request_id != self.request_id or plan.attempt_number != self.attempt_number:
            raise FinalGatePlanError("final gate plan request identity drifted")
        _require_directory(self.attempt_root, uid=self.service_uid)
        payload = _json_bytes(plan.to_dict())
        if len(payload) > _MAX_PLAN_BYTES:
            raise FinalGatePlanError("final gate plan is too large")
        try:
            existing = self.read()
        except FileNotFoundError:
            pass
        else:
            if existing != plan:
                raise FinalGatePlanError("final gate plan cannot be replaced")
            return self.path
        directory_fd = _open_directory(self.attempt_root)
        temporary = f".{self.path.name}.{uuid4().hex}.tmp"
        created = False
        try:
            fd = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                _PRIVATE_FILE_MODE,
                dir_fd=directory_fd,
            )
            created = True
            try:
                os.fchmod(fd, _PRIVATE_FILE_MODE)
                _write_all(fd, payload)
                os.fsync(fd)
            finally:
                os.close(fd)
            os.link(
                temporary,
                self.path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            os.unlink(temporary, dir_fd=directory_fd)
            created = False
            os.fsync(directory_fd)
        except FileExistsError:
            if self.read() != plan:
                raise FinalGatePlanError("final gate plan cannot be replaced") from None
        except OSError as exc:
            raise FinalGatePlanError("could not publish final gate plan") from exc
        finally:
            if created:
                try:
                    os.unlink(temporary, dir_fd=directory_fd)
                except OSError:
                    pass
            os.close(directory_fd)
        return self.path

    def read(self) -> FinalGatePlan:
        fd = os.open(
            self.path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.service_uid
                or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
                or metadata.st_nlink != 1
                or metadata.st_size > _MAX_PLAN_BYTES
            ):
                raise FinalGatePlanError("final gate plan authority is unsafe")
            payload = os.read(fd, _MAX_PLAN_BYTES + 1)
        finally:
            os.close(fd)
        if len(payload) > _MAX_PLAN_BYTES:
            raise FinalGatePlanError("final gate plan is too large")
        try:
            value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
            if not isinstance(value, dict):
                raise ValueError("plan must be an object")
            return FinalGatePlan.from_dict(value)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise FinalGatePlanError("final gate plan is invalid") from exc


def _plan_payload(plan: FinalGatePlan, *, include_digest: bool) -> dict[str, object]:
    payload = {
        name: (dict(value) if isinstance(value, Mapping) else value)
        for name, value in ((field, getattr(plan, field)) for field in plan.__dataclass_fields__)
        if include_digest or name != "plan_digest"
    }
    return payload


def _string(value: Mapping[str, object], key: str) -> str:
    found = value[key]
    if not isinstance(found, str):
        raise ValueError(f"final gate plan {key} must be a string")
    return found


def _optional_string(value: Mapping[str, object], key: str) -> str | None:
    found = value[key]
    if found is not None and not isinstance(found, str):
        raise ValueError(f"final gate plan {key} must be an optional string")
    return found


def _integer(value: Mapping[str, object], key: str) -> int:
    found = value[key]
    if type(found) is not int:
        raise ValueError(f"final gate plan {key} must be an integer")
    return found


def _string_map(value: Mapping[str, object], key: str) -> dict[str, str]:
    found = value[key]
    if not isinstance(found, Mapping) or not all(
        isinstance(name, str) and isinstance(item, str) for name, item in found.items()
    ):
        raise ValueError(f"final gate plan {key} must be a string map")
    return dict(found)


def _parse_systemd_evidence(value: Mapping[str, object]) -> _SystemdEvidence:
    unit_digests = value.get("unit-digests")
    supervisor_unit_digests = value.get("supervisor-unit-digests")
    failed_units = value.get("failed-units")
    script_digests = value.get("supervisor-script-digests")
    unit_count = value.get("unit-count")
    strings = {
        "supervisor_artifact_digest": value.get("supervisor-artifact-digest"),
        "supervisor_profile_sha256": value.get("supervisor-profile-sha256"),
        "supervisor_unit_set_digest": value.get("supervisor-unit-set-digest"),
        "systemd_unit_set_digest": value.get("unit-set-digest"),
    }
    calculated_unit_set_digest = (
        hashlib.sha256(
            json.dumps(
                {"failed": {}, "units": dict(unit_digests)},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        if isinstance(unit_digests, Mapping)
        else None
    )
    if (
        not isinstance(unit_digests, Mapping)
        or not isinstance(supervisor_unit_digests, Mapping)
        or not isinstance(script_digests, Mapping)
        or not isinstance(failed_units, Mapping)
        or failed_units
        or type(unit_count) is not int
        or unit_count != len(unit_digests)
        or not unit_digests
        or not script_digests
        or any(
            not isinstance(key, str)
            or not key
            or not isinstance(item, str)
            or _SHA256_RE.fullmatch(item) is None
            for values in (unit_digests, supervisor_unit_digests, script_digests)
            for key, item in values.items()
        )
        or any(
            not isinstance(item, str) or _SHA256_RE.fullmatch(item) is None
            for item in strings.values()
        )
        or strings["systemd_unit_set_digest"] != calculated_unit_set_digest
        or dict(supervisor_unit_digests)
        != {name: digest for name, digest in unit_digests.items() if name not in UNIT_PATHS}
        or external_supervisor_unit_set_digest(supervisor_unit_digests)
        != strings["supervisor_unit_set_digest"]
    ):
        raise ValueError("final gate systemd evidence is invalid")
    return {
        "supervisor_artifact_digest": cast(str, strings["supervisor_artifact_digest"]),
        "supervisor_profile_sha256": cast(str, strings["supervisor_profile_sha256"]),
        "supervisor_script_digests": dict(script_digests),
        "supervisor_unit_digests": dict(supervisor_unit_digests),
        "supervisor_unit_set_digest": strings["supervisor_unit_set_digest"],
        "systemd_unit_digests": dict(unit_digests),
        "systemd_unit_set_digest": cast(str, strings["systemd_unit_set_digest"]),
    }


def _parse_external_supervisor_predecessor_evidence(
    value: Mapping[str, object],
) -> _PredecessorEvidence:
    expected_fields = {
        "authority-kind",
        "authority-digest",
        "pointer-digest",
        "unit-digests",
        "unit-set-digest",
        "live-evidence-digest",
        "pending-transition-digest",
        "transition-clear",
        "runtime-ready",
        # ``pool-identity-digest`` is part of the external-supervisor.predecessor
        # evidence schema. It is a live count of external-supervisor worker rows
        # per pool (legacy ``gb10-arm64`` vs target ``gb10``) that ordinary worker
        # registration shifts, so it is deliberately NOT an authority/transition
        # field and is NOT folded into the supervisor transition digest below --
        # exactly as the final-admission drift check excludes it (see
        # final_attestation_admission.py). We still require it to be present and a
        # well-formed sha256 so the evidence stays schema-conformant.
        "pool-identity-digest",
    }
    units = value.get("unit-digests")
    strings = {
        "kind": value.get("authority-kind"),
        "authority_digest": value.get("authority-digest"),
        "pointer_digest": value.get("pointer-digest"),
        "unit_set_digest": value.get("unit-set-digest"),
        "live_evidence_digest": value.get("live-evidence-digest"),
        "pending_transition_digest": value.get("pending-transition-digest"),
        "pool_identity_digest": value.get("pool-identity-digest"),
    }
    # An absent predecessor (first introduction of the supervisor) carries no
    # units and the absent authority/pointer digests; a present predecessor
    # (legacy-manifest or canonical) carries a complete paired unit set.
    supervisor_predecessor_absent = strings["kind"] == "absent"
    if (
        set(value) != expected_fields
        or strings["kind"] not in {"legacy-manifest", "canonical", "absent"}
        or not isinstance(units, Mapping)
        or bool(units) == supervisor_predecessor_absent
        or (strings["authority_digest"] == EXTERNAL_SUPERVISOR_ABSENT_DIGEST)
        != supervisor_predecessor_absent
        or (
            supervisor_predecessor_absent
            and strings["pointer_digest"] != EXTERNAL_SUPERVISOR_ABSENT_DIGEST
        )
        or value.get("transition-clear") is not True
        or value.get("runtime-ready") is not True
        or any(
            not isinstance(name, str)
            or not name
            or not isinstance(digest, str)
            or _SHA256_RE.fullmatch(digest) is None
            for name, digest in units.items()
        )
        or any(
            not isinstance(item, str) or _SHA256_RE.fullmatch(item) is None
            for name, item in strings.items()
            if name != "kind"
        )
        or external_supervisor_unit_set_digest_or_empty(units) != strings["unit_set_digest"]
    ):
        raise ValueError("final gate external supervisor predecessor evidence is invalid")
    return {
        "kind": strings["kind"],
        "authority_digest": cast(str, strings["authority_digest"]),
        "pointer_digest": cast(str, strings["pointer_digest"]),
        "unit_sha256": dict(units),
        "unit_set_digest": strings["unit_set_digest"],
        "live_evidence_digest": cast(str, strings["live_evidence_digest"]),
        "pending_transition_digest": cast(str, strings["pending_transition_digest"]),
    }


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _json_bytes(value: Mapping[str, object]) -> bytes:
    return (json.dumps(dict(value), sort_keys=True, separators=(",", ":")) + "\n").encode()


def _require_directory(path: Path, *, uid: int) -> None:
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != uid:
        raise FinalGatePlanError("final gate plan directory authority is unsafe")


def _open_directory(path: Path) -> int:
    return os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )


def _write_all(fd: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(fd, payload[offset:])
        if written <= 0:  # pragma: no cover - os.write contract
            raise OSError("final gate plan write made no progress")
        offset += written


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("final gate plan contains duplicate fields")
        value[key] = item
    return value


__all__ = ["FinalGatePlan", "FinalGatePlanError", "FinalGatePlanStore"]
