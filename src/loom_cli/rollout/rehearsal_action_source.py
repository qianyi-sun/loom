"""Single-source exact authority for every isolated rehearsal action."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Protocol

from loom_cli.rollout.admin_smoke_contract import (
    AdminSmokeAuthority as RehearsalSmokeAuthority,
)
from loom_cli.rollout.external_supervisor_readiness import (
    SCRIPT_PATH as EXTERNAL_SUPERVISOR_SCRIPT_PATH,
)
from loom_cli.rollout.external_supervisor_readiness import (
    ExternalSupervisorArtifact,
)
from loom_cli.rollout.gb10_rehearsal import GB10RehearsalAuthority
from loom_cli.rollout.image_readiness import ALL_BUILD_IMAGES, ImageArtifactSet
from loom_cli.rollout.manifest_readiness import ManifestArtifact
from loom_cli.rollout.migration_manifest_readiness import MigrationManifestArtifact
from loom_cli.rollout.operator.checkpoint_lease import CriticalCheckpointEvidence
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactStore
from loom_cli.rollout.production_defaults_readiness import ProductionDefaultsArtifact
from loom_cli.rollout.rehearsal_readiness import (
    REHEARSAL_CHECK_IDS,
    RehearsalAction,
    RehearsalResult,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ROUTE_RE = re.compile(r"^https://[a-z0-9.-]+/[a-z0-9/-]+$")
_IMAGE_TAG_RE = re.compile(r"^staging-[a-z0-9][a-z0-9-]{5,63}$")
_CLUSTER_NAME_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_REVISION_RE = re.compile(r"^[0-9]{4}(?:_[a-z0-9_]+)?$")


@dataclass(frozen=True, slots=True)
class RehearsalResources:
    """Names confined to one non-protected rehearsal identity."""

    namespace: str
    database: str
    object_prefix: str
    route: str
    systemd_unit: str

    @classmethod
    def derive(cls, isolation_id: str, *, route_origin: str) -> RehearsalResources:
        suffix = isolation_id.removeprefix("rehearsal-")
        if (
            not isolation_id.startswith("rehearsal-")
            or not 16 <= len(suffix) <= 32
            or any(character not in "0123456789abcdef" for character in suffix)
            or _ROUTE_RE.fullmatch(route_origin) is None
            or route_origin.endswith("/")
        ):
            raise ValueError("rehearsal resource identity is invalid")
        resources = cls(
            namespace=f"loom-rehearsal-{suffix}",
            database=f"loom_rehearsal_{suffix}",
            object_prefix=f"rehearsal/{suffix}/",
            route=f"{route_origin}/rehearsal/{suffix}",
            systemd_unit=f"loom-preflight-{suffix}.service",
        )
        resources.require_isolated()
        return resources

    def require_isolated(self) -> None:
        if (
            not self.namespace.startswith("loom-rehearsal-")
            or self.namespace == "loom-staging"
            or not self.database.startswith("loom_rehearsal_")
            or not self.object_prefix.startswith("rehearsal/")
            or not self.object_prefix.endswith("/")
            or _ROUTE_RE.fullmatch(self.route) is None
            or "/rehearsal/" not in self.route
            or not self.systemd_unit.startswith("loom-preflight-")
            or not self.systemd_unit.endswith(".service")
        ):
            raise ValueError("rehearsal resource escaped isolated authority")


@dataclass(frozen=True, slots=True)
class RehearsalPlan:
    """Immutable exact-candidate plan shared by identity and action factories."""

    candidate_sha: str
    candidate_tree: str
    cluster_name: str
    checkpoint_request_id: str
    checkpoint_evidence_sha256: str
    checkpoint_manifest_path: Path
    checkpoint_manifest_sha256: str
    mutation_epoch: int
    db_snapshot_identity: str
    object_inventory_root: str
    schema_revision: str
    image_digests: Mapping[str, str]
    image_tag: str
    image_artifact_sha256: str
    artifact_bundle_sha256: str
    artifact_descriptor_path: Path
    rendered_manifest_path: Path
    production_defaults_path: Path
    manifest_artifact_sha256: str
    rendered_manifest_sha256: str
    production_defaults_sha256: str
    external_supervisor_artifact_sha256: str
    external_supervisor_profile_sha256: str
    external_supervisor_script_sha256: Mapping[str, str]
    external_supervisor_unit_sha256: Mapping[str, str]
    migration_plan_sha256: str
    migration_target_revision: str
    browser_report_schema_sha256: str
    resources: RehearsalResources
    smoke_authority: RehearsalSmokeAuthority
    gb10_authority: GB10RehearsalAuthority

    def __post_init__(self) -> None:
        image_digests = dict(self.image_digests)
        supervisor_script_sha256 = dict(self.external_supervisor_script_sha256)
        supervisor_unit_sha256 = dict(self.external_supervisor_unit_sha256)
        if (
            len(self.candidate_sha) not in {40, 64}
            or any(character not in "0123456789abcdef" for character in self.candidate_sha)
            or len(self.candidate_tree) != 40
            or any(character not in "0123456789abcdef" for character in self.candidate_tree)
            or self.mutation_epoch < 0
            or _CLUSTER_NAME_RE.fullmatch(self.cluster_name) is None
            or not self.checkpoint_request_id.startswith("req-")
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.checkpoint_evidence_sha256,
                    self.checkpoint_manifest_sha256,
                    self.object_inventory_root,
                    self.image_artifact_sha256,
                    self.artifact_bundle_sha256,
                    self.manifest_artifact_sha256,
                    self.rendered_manifest_sha256,
                    self.production_defaults_sha256,
                    self.external_supervisor_artifact_sha256,
                    self.external_supervisor_profile_sha256,
                    self.migration_plan_sha256,
                    self.browser_report_schema_sha256,
                )
            )
            or not self.checkpoint_manifest_path.is_absolute()
            or self.checkpoint_manifest_path.name != "backup-manifest.json"
            or ".." in self.checkpoint_manifest_path.parts
            or not self.artifact_descriptor_path.is_absolute()
            or not self.rendered_manifest_path.is_absolute()
            or not self.production_defaults_path.is_absolute()
            or ".." in self.artifact_descriptor_path.parts
            or ".." in self.rendered_manifest_path.parts
            or ".." in self.production_defaults_path.parts
            or self.artifact_descriptor_path.parent != self.rendered_manifest_path.parent
            or self.artifact_descriptor_path.parent != self.production_defaults_path.parent
            or self.artifact_descriptor_path.parent.name != self.artifact_bundle_sha256
            or self.artifact_descriptor_path.parent.parent.name != "preflight-artifacts"
            or self.artifact_descriptor_path.name != "artifact.json"
            or self.rendered_manifest_path.name != "rendered.yaml"
            or self.production_defaults_path.name != "production-defaults.json"
            or not self.db_snapshot_identity.startswith("pgdump-sha256:")
            or _SHA256_RE.fullmatch(self.db_snapshot_identity.removeprefix("pgdump-sha256:"))
            is None
            or _REVISION_RE.fullmatch(self.schema_revision) is None
            or _REVISION_RE.fullmatch(self.migration_target_revision) is None
            or _IMAGE_TAG_RE.fullmatch(self.image_tag) is None
            or self.smoke_authority.required_worker_pool is None
            or not image_digests
            or set(image_digests) != {name for name, _path in ALL_BUILD_IMAGES}
            or any(
                not name or not digest.startswith("sha256:")
                for name, digest in image_digests.items()
            )
            or set(supervisor_script_sha256) != {EXTERNAL_SUPERVISOR_SCRIPT_PATH}
            or not supervisor_unit_sha256
            or any(
                _SHA256_RE.fullmatch(digest) is None for digest in supervisor_script_sha256.values()
            )
            or any(
                not name or "/" in name or "\\" in name or _SHA256_RE.fullmatch(digest) is None
                for name, digest in supervisor_unit_sha256.items()
            )
        ):
            raise ValueError("rehearsal plan identity is invalid")
        self.resources.require_isolated()
        object.__setattr__(self, "image_digests", MappingProxyType(image_digests))
        object.__setattr__(
            self,
            "external_supervisor_script_sha256",
            MappingProxyType(dict(sorted(supervisor_script_sha256.items()))),
        )
        object.__setattr__(
            self,
            "external_supervisor_unit_sha256",
            MappingProxyType(dict(sorted(supervisor_unit_sha256.items()))),
        )

    @property
    def plan_digest(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_record(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def to_record(self) -> dict[str, object]:
        """Return the strict secret-free plan consumed by the installed helper."""
        return {
            "artifact_bundle_sha256": self.artifact_bundle_sha256,
            "artifact_descriptor_path": str(self.artifact_descriptor_path),
            "browser_report_schema_sha256": self.browser_report_schema_sha256,
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "cluster_name": self.cluster_name,
            "checkpoint_request_id": self.checkpoint_request_id,
            "checkpoint_evidence_sha256": self.checkpoint_evidence_sha256,
            "checkpoint_manifest_path": str(self.checkpoint_manifest_path),
            "checkpoint_manifest_sha256": self.checkpoint_manifest_sha256,
            "db_snapshot_identity": self.db_snapshot_identity,
            "gb10_authority": self.gb10_authority.to_record(),
            "image_artifact_sha256": self.image_artifact_sha256,
            "image_digests": dict(self.image_digests),
            "image_tag": self.image_tag,
            "external_supervisor_artifact_sha256": self.external_supervisor_artifact_sha256,
            "external_supervisor_profile_sha256": self.external_supervisor_profile_sha256,
            "external_supervisor_script_sha256": dict(self.external_supervisor_script_sha256),
            "external_supervisor_unit_sha256": dict(self.external_supervisor_unit_sha256),
            "manifest_artifact_sha256": self.manifest_artifact_sha256,
            "migration_plan_sha256": self.migration_plan_sha256,
            "migration_target_revision": self.migration_target_revision,
            "mutation_epoch": self.mutation_epoch,
            "object_inventory_root": self.object_inventory_root,
            "resources": {
                "database": self.resources.database,
                "namespace": self.resources.namespace,
                "object_prefix": self.resources.object_prefix,
                "route": self.resources.route,
                "systemd_unit": self.resources.systemd_unit,
            },
            "rendered_manifest_path": str(self.rendered_manifest_path),
            "rendered_manifest_sha256": self.rendered_manifest_sha256,
            "production_defaults_path": str(self.production_defaults_path),
            "production_defaults_sha256": self.production_defaults_sha256,
            "schema_revision": self.schema_revision,
            "schema_version": 6,
            "smoke_authority": self.smoke_authority.to_record(),
        }

    @classmethod
    def from_record(cls, value: Mapping[str, object]) -> RehearsalPlan:
        """Parse the one strict plan schema accepted by the installed helper."""
        expected = {
            "artifact_bundle_sha256",
            "artifact_descriptor_path",
            "browser_report_schema_sha256",
            "candidate_sha",
            "candidate_tree",
            "cluster_name",
            "checkpoint_request_id",
            "checkpoint_evidence_sha256",
            "checkpoint_manifest_path",
            "checkpoint_manifest_sha256",
            "db_snapshot_identity",
            "external_supervisor_artifact_sha256",
            "external_supervisor_profile_sha256",
            "external_supervisor_script_sha256",
            "external_supervisor_unit_sha256",
            "gb10_authority",
            "image_artifact_sha256",
            "image_digests",
            "image_tag",
            "manifest_artifact_sha256",
            "migration_plan_sha256",
            "migration_target_revision",
            "mutation_epoch",
            "object_inventory_root",
            "resources",
            "rendered_manifest_path",
            "rendered_manifest_sha256",
            "production_defaults_path",
            "production_defaults_sha256",
            "schema_revision",
            "schema_version",
            "smoke_authority",
        }
        resources = value.get("resources")
        smoke_authority = value.get("smoke_authority")
        image_digests = value.get("image_digests")
        supervisor_script_sha256 = value.get("external_supervisor_script_sha256")
        supervisor_unit_sha256 = value.get("external_supervisor_unit_sha256")
        mutation_epoch = value.get("mutation_epoch")
        if (
            set(value) != expected
            or type(value.get("schema_version")) is not int
            or value.get("schema_version") != 6
            or type(mutation_epoch) is not int
            or not isinstance(resources, Mapping)
            or set(resources) != {"database", "namespace", "object_prefix", "route", "systemd_unit"}
            or not isinstance(image_digests, Mapping)
            or not isinstance(supervisor_script_sha256, Mapping)
            or not isinstance(supervisor_unit_sha256, Mapping)
            or not isinstance(smoke_authority, Mapping)
            or not image_digests
            or any(
                not isinstance(key, str) or not isinstance(item, str)
                for values in (
                    image_digests,
                    supervisor_script_sha256,
                    supervisor_unit_sha256,
                )
                for key, item in values.items()
            )
        ):
            raise ValueError("rehearsal plan schema is invalid")
        string_fields = (
            "artifact_bundle_sha256",
            "artifact_descriptor_path",
            "browser_report_schema_sha256",
            "candidate_sha",
            "candidate_tree",
            "cluster_name",
            "checkpoint_request_id",
            "checkpoint_evidence_sha256",
            "checkpoint_manifest_path",
            "checkpoint_manifest_sha256",
            "db_snapshot_identity",
            "external_supervisor_artifact_sha256",
            "external_supervisor_profile_sha256",
            "image_artifact_sha256",
            "image_tag",
            "manifest_artifact_sha256",
            "migration_plan_sha256",
            "migration_target_revision",
            "object_inventory_root",
            "rendered_manifest_path",
            "rendered_manifest_sha256",
            "production_defaults_path",
            "production_defaults_sha256",
            "schema_revision",
        )
        if any(not isinstance(value.get(field), str) for field in string_fields) or any(
            not isinstance(resources.get(field), str)
            for field in ("database", "namespace", "object_prefix", "route", "systemd_unit")
        ):
            raise ValueError("rehearsal plan schema is invalid")
        return cls(
            candidate_sha=str(value["candidate_sha"]),
            candidate_tree=str(value["candidate_tree"]),
            cluster_name=str(value["cluster_name"]),
            checkpoint_request_id=str(value["checkpoint_request_id"]),
            checkpoint_evidence_sha256=str(value["checkpoint_evidence_sha256"]),
            checkpoint_manifest_path=Path(str(value["checkpoint_manifest_path"])),
            checkpoint_manifest_sha256=str(value["checkpoint_manifest_sha256"]),
            mutation_epoch=mutation_epoch,
            db_snapshot_identity=str(value["db_snapshot_identity"]),
            object_inventory_root=str(value["object_inventory_root"]),
            schema_revision=str(value["schema_revision"]),
            image_digests={str(key): str(item) for key, item in image_digests.items()},
            image_tag=str(value["image_tag"]),
            image_artifact_sha256=str(value["image_artifact_sha256"]),
            artifact_bundle_sha256=str(value["artifact_bundle_sha256"]),
            artifact_descriptor_path=Path(str(value["artifact_descriptor_path"])),
            rendered_manifest_path=Path(str(value["rendered_manifest_path"])),
            production_defaults_path=Path(str(value["production_defaults_path"])),
            manifest_artifact_sha256=str(value["manifest_artifact_sha256"]),
            rendered_manifest_sha256=str(value["rendered_manifest_sha256"]),
            production_defaults_sha256=str(value["production_defaults_sha256"]),
            external_supervisor_artifact_sha256=str(value["external_supervisor_artifact_sha256"]),
            external_supervisor_profile_sha256=str(value["external_supervisor_profile_sha256"]),
            external_supervisor_script_sha256={
                str(key): str(item)
                for key, item in _mapping_field(value, "external_supervisor_script_sha256").items()
            },
            external_supervisor_unit_sha256={
                str(key): str(item)
                for key, item in _mapping_field(value, "external_supervisor_unit_sha256").items()
            },
            migration_plan_sha256=str(value["migration_plan_sha256"]),
            migration_target_revision=str(value["migration_target_revision"]),
            browser_report_schema_sha256=str(value["browser_report_schema_sha256"]),
            resources=RehearsalResources(
                database=str(resources["database"]),
                namespace=str(resources["namespace"]),
                object_prefix=str(resources["object_prefix"]),
                route=str(resources["route"]),
                systemd_unit=str(resources["systemd_unit"]),
            ),
            smoke_authority=RehearsalSmokeAuthority.from_record(smoke_authority),
            gb10_authority=GB10RehearsalAuthority.from_record(
                _mapping_field(value, "gb10_authority")
            ),
        )


@dataclass(frozen=True, slots=True)
class RehearsalObservation:
    """Secret-free terminal observation returned by the isolated backend."""

    check_id: str
    evidence_digest: str
    journal_digest: str
    protected_mutation: bool
    cleanup_verified: bool
    blockers: Mapping[str, str]


class RehearsalBackend(Protocol):
    def execute(self, check_id: str, plan: RehearsalPlan) -> RehearsalObservation: ...


@dataclass(frozen=True, slots=True)
class RehearsalActionSource:
    """Create identity and actions from the same immutable plan implementation."""

    image_artifacts: Callable[[], ImageArtifactSet]
    manifest_artifacts: Callable[[], ManifestArtifact]
    migration_artifacts: Callable[[], MigrationManifestArtifact]
    production_defaults_artifacts: Callable[[], ProductionDefaultsArtifact]
    external_supervisor_artifacts: Callable[[], ExternalSupervisorArtifact]
    artifact_store: PreflightArtifactStore
    migration_plan_sha256: str
    migration_target_revision: str
    browser_report_schema_sha256: str
    cluster_name: str
    route_origin: str
    smoke_authority: RehearsalSmokeAuthority
    gb10_authority: GB10RehearsalAuthority
    backend: RehearsalBackend

    def __post_init__(self) -> None:
        if (
            _SHA256_RE.fullmatch(self.migration_plan_sha256) is None
            or _REVISION_RE.fullmatch(self.migration_target_revision) is None
            or _SHA256_RE.fullmatch(self.browser_report_schema_sha256) is None
            or _CLUSTER_NAME_RE.fullmatch(self.cluster_name) is None
            or not self.route_origin.startswith("https://")
            or self.route_origin.endswith("/")
        ):
            raise ValueError("rehearsal action source authority is invalid")

    def identity(
        self,
        candidate: CandidateBinding,
        checkpoint: CriticalCheckpointEvidence,
    ) -> tuple[str, str]:
        artifacts = self.image_artifacts()
        manifests = self.manifest_artifacts()
        migration = self.migration_artifacts()
        production_defaults = self.production_defaults_artifacts()
        external_supervisor = self.external_supervisor_artifacts()
        isolation_id = self._isolation_id(
            candidate,
            checkpoint,
            artifacts,
            manifests,
            migration,
            production_defaults,
            external_supervisor,
        )
        plan = self._plan(
            candidate,
            checkpoint,
            isolation_id=isolation_id,
            artifacts=artifacts,
            manifests=manifests,
            migration=migration,
            production_defaults=production_defaults,
            external_supervisor=external_supervisor,
        )
        return isolation_id, plan.plan_digest

    def actions(
        self,
        candidate: CandidateBinding,
        checkpoint: CriticalCheckpointEvidence,
        isolation_id: str,
    ) -> Mapping[str, RehearsalAction]:
        expected_isolation_id, expected_digest = self.identity(candidate, checkpoint)
        if isolation_id != expected_isolation_id:
            raise ValueError("rehearsal isolation identity drifted")
        plan = self._plan(
            candidate,
            checkpoint,
            isolation_id=isolation_id,
            artifacts=self.image_artifacts(),
            manifests=self.manifest_artifacts(),
            migration=self.migration_artifacts(),
            production_defaults=self.production_defaults_artifacts(),
            external_supervisor=self.external_supervisor_artifacts(),
        )
        if plan.plan_digest != expected_digest:
            raise ValueError("rehearsal plan identity drifted")

        def action(check_id: str) -> RehearsalAction:
            def execute() -> RehearsalResult:
                observation = self.backend.execute(check_id, plan)
                if observation.check_id != check_id:
                    raise ValueError("rehearsal backend check identity drifted")
                return RehearsalResult(
                    check_id=check_id,
                    isolation_id=isolation_id,
                    candidate_sha=candidate.resolved_sha,
                    mutation_epoch=checkpoint.mutation_epoch,
                    evidence_digest=observation.evidence_digest,
                    journal_digest=observation.journal_digest,
                    protected_mutation=observation.protected_mutation,
                    cleanup_verified=observation.cleanup_verified,
                    blockers=observation.blockers,
                )

            return execute

        return MappingProxyType({check_id: action(check_id) for check_id in REHEARSAL_CHECK_IDS})

    def _plan(
        self,
        candidate: CandidateBinding,
        checkpoint: CriticalCheckpointEvidence,
        *,
        isolation_id: str,
        artifacts: ImageArtifactSet,
        manifests: ManifestArtifact,
        migration: MigrationManifestArtifact,
        production_defaults: ProductionDefaultsArtifact,
        external_supervisor: ExternalSupervisorArtifact,
    ) -> RehearsalPlan:
        if (
            checkpoint.environment != "staging"
            or checkpoint.namespace != "loom-staging"
            or candidate.resolved_tree is None
            or external_supervisor.candidate_sha != candidate.resolved_sha
            or external_supervisor.candidate_tree != candidate.resolved_tree
            or external_supervisor.image_tag != candidate.image_tag
            or external_supervisor.environment != checkpoint.environment
        ):
            raise ValueError("rehearsal candidate or checkpoint authority is invalid")
        resources = RehearsalResources.derive(isolation_id, route_origin=self.route_origin)
        publication = self.artifact_store.publish(
            candidate_sha=candidate.resolved_sha,
            candidate_tree=candidate.resolved_tree,
            mutation_epoch=checkpoint.mutation_epoch,
            images=artifacts,
            manifests=manifests,
            migration=migration,
            production_defaults=production_defaults,
            migration_plan_sha256=self.migration_plan_sha256,
            migration_target_revision=self.migration_target_revision,
            browser_report_schema_sha256=self.browser_report_schema_sha256,
        )
        return RehearsalPlan(
            candidate_sha=candidate.resolved_sha,
            candidate_tree=candidate.resolved_tree,
            cluster_name=self.cluster_name,
            checkpoint_request_id=checkpoint.request_id,
            checkpoint_evidence_sha256=checkpoint.evidence_digest,
            checkpoint_manifest_path=checkpoint.manifest_path,
            checkpoint_manifest_sha256=checkpoint.manifest_sha256,
            mutation_epoch=checkpoint.mutation_epoch,
            db_snapshot_identity=checkpoint.db_snapshot_identity,
            object_inventory_root=checkpoint.object_inventory_root,
            schema_revision=checkpoint.schema_revision,
            image_digests=artifacts.image_digests,
            image_tag=candidate.image_tag,
            image_artifact_sha256=artifacts.artifact_digest,
            artifact_bundle_sha256=publication.bundle_digest,
            artifact_descriptor_path=publication.descriptor_path,
            rendered_manifest_path=publication.rendered_manifest_path,
            production_defaults_path=publication.production_defaults_path,
            manifest_artifact_sha256=publication.manifest_artifact_sha256,
            rendered_manifest_sha256=publication.rendered_manifest_sha256,
            production_defaults_sha256=publication.production_defaults_sha256,
            external_supervisor_artifact_sha256=external_supervisor.artifact_digest,
            external_supervisor_profile_sha256=external_supervisor.profile_sha256,
            external_supervisor_script_sha256=external_supervisor.script_sha256,
            external_supervisor_unit_sha256=external_supervisor.unit_sha256,
            migration_plan_sha256=self.migration_plan_sha256,
            migration_target_revision=self.migration_target_revision,
            browser_report_schema_sha256=self.browser_report_schema_sha256,
            resources=resources,
            smoke_authority=self.smoke_authority,
            gb10_authority=self.gb10_authority,
        )

    def _isolation_id(
        self,
        candidate: CandidateBinding,
        checkpoint: CriticalCheckpointEvidence,
        artifacts: ImageArtifactSet,
        manifests: ManifestArtifact,
        migration: MigrationManifestArtifact,
        production_defaults: ProductionDefaultsArtifact,
        external_supervisor: ExternalSupervisorArtifact,
    ) -> str:
        payload = {
            "browser_report_schema_sha256": self.browser_report_schema_sha256,
            "candidate_sha": candidate.resolved_sha,
            "candidate_tree": candidate.resolved_tree,
            "cluster_name": self.cluster_name,
            "checkpoint_evidence_sha256": checkpoint.evidence_digest,
            "checkpoint_manifest_sha256": checkpoint.manifest_sha256,
            "image_artifact_sha256": artifacts.artifact_digest,
            "image_tag": candidate.image_tag,
            "manifest_artifact_sha256": manifests.artifact_digest,
            "rendered_manifest_sha256": manifests.rendered_sha256,
            "production_defaults_sha256": production_defaults.artifact_digest,
            "external_supervisor_artifact_sha256": external_supervisor.artifact_digest,
            "external_supervisor_profile_sha256": external_supervisor.profile_sha256,
            "external_supervisor_script_sha256": dict(external_supervisor.script_sha256),
            "external_supervisor_unit_sha256": dict(external_supervisor.unit_sha256),
            "migration_plan_sha256": self.migration_plan_sha256,
            "migration_manifest_artifact_sha256": migration.artifact_digest,
            "migration_manifest_sha256": migration.rendered_sha256,
            "migration_target_revision": self.migration_target_revision,
            "route_origin": self.route_origin,
            "gb10_authority": self.gb10_authority.to_record(),
            "smoke_authority": self.smoke_authority.to_record(),
            "schema_version": 6,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return f"rehearsal-{digest[:24]}"


__all__ = [
    "RehearsalActionSource",
    "RehearsalBackend",
    "RehearsalObservation",
    "RehearsalPlan",
    "RehearsalResources",
    "RehearsalSmokeAuthority",
]


def _mapping_field(value: Mapping[str, object], field: str) -> Mapping[str, object]:
    item = value.get(field)
    if not isinstance(item, Mapping):
        raise ValueError("rehearsal plan schema is invalid")
    return item
