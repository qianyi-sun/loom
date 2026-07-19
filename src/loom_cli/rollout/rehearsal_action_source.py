"""Single-source exact authority for every isolated rehearsal action."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

from loom_cli.rollout.image_readiness import ImageArtifactSet
from loom_cli.rollout.operator.checkpoint_lease import CriticalCheckpointEvidence
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.rehearsal_readiness import (
    REHEARSAL_CHECK_IDS,
    RehearsalAction,
    RehearsalResult,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ROUTE_RE = re.compile(r"^https://[a-z0-9.-]+/[a-z0-9/-]+$")


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
            or not route_origin.startswith("https://")
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
    checkpoint_evidence_sha256: str
    checkpoint_manifest_sha256: str
    mutation_epoch: int
    db_snapshot_identity: str
    object_inventory_root: str
    schema_revision: str
    image_digests: Mapping[str, str]
    image_artifact_sha256: str
    migration_plan_sha256: str
    browser_report_schema_sha256: str
    resources: RehearsalResources

    def __post_init__(self) -> None:
        image_digests = dict(self.image_digests)
        if (
            len(self.candidate_sha) not in {40, 64}
            or any(character not in "0123456789abcdef" for character in self.candidate_sha)
            or len(self.candidate_tree) != 40
            or any(character not in "0123456789abcdef" for character in self.candidate_tree)
            or self.mutation_epoch < 0
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.checkpoint_evidence_sha256,
                    self.checkpoint_manifest_sha256,
                    self.object_inventory_root,
                    self.image_artifact_sha256,
                    self.migration_plan_sha256,
                    self.browser_report_schema_sha256,
                )
            )
            or not self.db_snapshot_identity.startswith("pgdump-sha256:")
            or not self.schema_revision
            or not image_digests
            or any(not name or not digest.startswith("sha256:") for name, digest in image_digests.items())
        ):
            raise ValueError("rehearsal plan identity is invalid")
        self.resources.require_isolated()
        object.__setattr__(self, "image_digests", MappingProxyType(image_digests))

    @property
    def plan_digest(self) -> str:
        payload = {
            "browser_report_schema_sha256": self.browser_report_schema_sha256,
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "checkpoint_evidence_sha256": self.checkpoint_evidence_sha256,
            "checkpoint_manifest_sha256": self.checkpoint_manifest_sha256,
            "db_snapshot_identity": self.db_snapshot_identity,
            "image_artifact_sha256": self.image_artifact_sha256,
            "image_digests": dict(self.image_digests),
            "migration_plan_sha256": self.migration_plan_sha256,
            "mutation_epoch": self.mutation_epoch,
            "object_inventory_root": self.object_inventory_root,
            "resources": {
                "database": self.resources.database,
                "namespace": self.resources.namespace,
                "object_prefix": self.resources.object_prefix,
                "route": self.resources.route,
                "systemd_unit": self.resources.systemd_unit,
            },
            "schema_revision": self.schema_revision,
            "schema_version": 1,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


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
    migration_plan_sha256: str
    browser_report_schema_sha256: str
    route_origin: str
    backend: RehearsalBackend

    def __post_init__(self) -> None:
        if (
            _SHA256_RE.fullmatch(self.migration_plan_sha256) is None
            or _SHA256_RE.fullmatch(self.browser_report_schema_sha256) is None
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
        isolation_id = self._isolation_id(candidate, checkpoint, artifacts)
        plan = self._plan(candidate, checkpoint, isolation_id=isolation_id, artifacts=artifacts)
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
    ) -> RehearsalPlan:
        if (
            checkpoint.environment != "staging"
            or checkpoint.namespace != "loom-staging"
            or candidate.resolved_tree is None
        ):
            raise ValueError("rehearsal candidate or checkpoint authority is invalid")
        resources = RehearsalResources.derive(isolation_id, route_origin=self.route_origin)
        return RehearsalPlan(
            candidate_sha=candidate.resolved_sha,
            candidate_tree=candidate.resolved_tree,
            checkpoint_evidence_sha256=checkpoint.evidence_digest,
            checkpoint_manifest_sha256=checkpoint.manifest_sha256,
            mutation_epoch=checkpoint.mutation_epoch,
            db_snapshot_identity=checkpoint.db_snapshot_identity,
            object_inventory_root=checkpoint.object_inventory_root,
            schema_revision=checkpoint.schema_revision,
            image_digests=artifacts.image_digests,
            image_artifact_sha256=artifacts.artifact_digest,
            migration_plan_sha256=self.migration_plan_sha256,
            browser_report_schema_sha256=self.browser_report_schema_sha256,
            resources=resources,
        )

    def _isolation_id(
        self,
        candidate: CandidateBinding,
        checkpoint: CriticalCheckpointEvidence,
        artifacts: ImageArtifactSet,
    ) -> str:
        payload = {
            "browser_report_schema_sha256": self.browser_report_schema_sha256,
            "candidate_sha": candidate.resolved_sha,
            "candidate_tree": candidate.resolved_tree,
            "checkpoint_evidence_sha256": checkpoint.evidence_digest,
            "checkpoint_manifest_sha256": checkpoint.manifest_sha256,
            "image_artifact_sha256": artifacts.artifact_digest,
            "migration_plan_sha256": self.migration_plan_sha256,
            "route_origin": self.route_origin,
            "schema_version": 1,
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
]
