"""Repeated live dependency fence for zero-ceiling execution preparation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass

from loom_capacity_manager.contracts import canonical_digest

from .final_gate_plan import FinalGatePlan
from .protected_execution_prerequisite_source import (
    ProtectedExecutionPrerequisiteAuthority,
)
from .protected_execution_prerequisites import ProtectedExecutionPrerequisiteArtifact
from .protected_staging_capacity_manager_configuration_component import (
    ProtectedStagingDesiredConfiguration,
)

DesiredConfigurationSource = Callable[[FinalGatePlan], ProtectedStagingDesiredConfiguration]
ExecutionAuthoritySource = Callable[
    [ProtectedStagingDesiredConfiguration], ProtectedExecutionPrerequisiteAuthority
]


@dataclass(frozen=True, slots=True)
class ProtectedExecutionPreparationDependencyGuard:
    """Match freshly captured execution authority to the immutable rollout artifact."""

    desired_configuration_source: DesiredConfigurationSource
    authority_source: ExecutionAuthoritySource

    def __post_init__(self) -> None:
        if not callable(self.desired_configuration_source) or not callable(self.authority_source):
            raise ValueError("execution preparation dependency source is invalid")

    def __call__(
        self,
        plan: FinalGatePlan,
        artifact: ProtectedExecutionPrerequisiteArtifact,
    ) -> str:
        if (
            not isinstance(plan, FinalGatePlan)
            or not isinstance(artifact, ProtectedExecutionPrerequisiteArtifact)
            or artifact.candidate_sha != plan.candidate_sha
            or artifact.candidate_tree != plan.candidate_tree
            or artifact.core_artifact_bundle_sha256 != plan.artifact_bundle_digest
        ):
            raise ValueError("execution preparation dependency binding is invalid")
        desired = self.desired_configuration_source(plan)
        if not isinstance(desired, ProtectedStagingDesiredConfiguration) or not desired.exact:
            raise ValueError("execution preparation configuration is not exact")
        desired_subjects = {
            str(subject.subject_id): canonical_digest(subject) for subject in desired.subjects
        }
        if (
            desired.fleet.fleet_generation != artifact.desired_fleet_generation
            or canonical_digest(desired.fleet) != artifact.desired_fleet_sha256
            or desired_subjects != dict(artifact.desired_subject_sha256)
            or desired.staging_subject.subject_id != artifact.staging_subject_id
        ):
            raise ValueError("execution preparation configuration drifted")
        authority = self.authority_source(desired)
        if not isinstance(authority, ProtectedExecutionPrerequisiteAuthority):
            raise ValueError("execution preparation authority is invalid")
        if (
            authority.executor_profile_seed != artifact.executor_profile_seed
            or authority.subject_acknowledgements
            != artifact.execution_policy.subject_acknowledgements
            or dict(authority.manager_client_cidrs) != dict(artifact.manager_client_cidrs)
            or dict(authority.credential_metadata_sha256)
            != dict(artifact.credential_metadata_sha256)
            or dict(authority.coexistence_witness_sha256)
            != dict(artifact.coexistence_witness_sha256)
            or authority.legacy_writer_fences != artifact.execution_policy.legacy_writer_fences
        ):
            raise ValueError("execution preparation authority drifted")
        return hashlib.sha256(
            json.dumps(
                {
                    "artifact_sha256": artifact.artifact_sha256,
                    "configuration_evidence_sha256": desired.original.evidence_digest,
                    "credential_metadata_sha256": artifact.credential_metadata_manifest_sha256,
                    "legacy_writer_sha256": artifact.legacy_writer_manifest_sha256,
                    "manager_route_sha256": artifact.manager_route_sha256,
                    "witness_sha256": artifact.witness_manifest_sha256,
                },
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
        ).hexdigest()


__all__ = ["ProtectedExecutionPreparationDependencyGuard"]
