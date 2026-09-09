"""Exact retained dynamic-application evidence for typed membership consumers.

The caller authenticates the projection/member history and immutable base origin.
This reader verifies persisted installation facts, not current launch permission.
Operator-static candidate formats are not inferred or translated here.
"""

from __future__ import annotations

from typing import Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.build_generation_store import _require_values
from loom_capacity_manager.contracts import DynamicDevelopmentSubjectProjectionV1, canonical_bytes
from loom_capacity_manager.membership_contracts import PersonalApplicationMemberV1
from loom_capacity_manager.models import (
    CapacityCandidate,
    CapacityDemandReporter,
    CapacityDeploymentGeneration,
    CapacityWorkerProfile,
)


async def require_application_generation_evidence(
    session: AsyncSession,
    member: PersonalApplicationMemberV1,
    projection: DynamicDevelopmentSubjectProjectionV1,
    origin: DynamicDevelopmentSubjectProjectionV1,
    *, reporter_state: Literal["current", "fenced"] = "current",
) -> None:
    """Join the original create/update and the reporter's last membership event.

    Capacity/teardown advance membership but must not rewrite the installation's
    originating operation attestation. A rotated reporter is checked against its
    final configuration, not every earlier event that shared its credentials.
    """
    member = PersonalApplicationMemberV1.model_validate_json(canonical_bytes(member))
    projection = DynamicDevelopmentSubjectProjectionV1.model_validate_json(canonical_bytes(projection))
    origin = DynamicDevelopmentSubjectProjectionV1.model_validate_json(canonical_bytes(origin))
    subject, ack = member.configuration, member.acknowledgement
    mutable = {"operation_kind", "operation_id", "operation_epoch", "configuration_generation", "min_slots", "max_slots"}
    if (
        reporter_state not in {"current", "fenced"} or origin.operation_kind not in {"create", "update"}
        or origin.configuration_generation > projection.configuration_generation
        or projection.model_dump(exclude=mutable) != origin.model_dump(exclude=mutable)
        or (projection.operation_kind in {"create", "update"} and canonical_bytes(projection) != canonical_bytes(origin))
        or projection.subject_id != subject.subject_id or projection.subject_incarnation != subject.subject_incarnation
        or projection.owner_id != member.owner_id or subject.display_name != f"dev-{projection.environment_name}"
        or projection.configuration_generation != subject.configuration_generation
        or projection.deployment_generation != subject.deployment_generation
        or projection.candidate_generation != subject.candidate_generation
        or projection.demand_reporter_incarnation != subject.demand_reporter_incarnation
        or subject.lifecycle_state != ("disabled" if projection.operation_kind == "destroy" else "active")
        or subject.min_slots != (0 if projection.operation_kind == "destroy" else projection.min_slots)
        or subject.max_slots != (0 if projection.operation_kind == "destroy" else projection.max_slots)
        or ack.candidate.algorithm != "source-sha256" or ack.candidate.identity != projection.candidate_sha256
        or ack.candidate.publication_sha256 != projection.candidate_publication_sha256
        or ack.protected_admission_sha256 != projection.protected_admission_sha256
    ):
        raise ValueError("application generation origin or latest membership changed")
    candidate = (await session.scalars(select(CapacityCandidate).where(
        CapacityCandidate.subject_id == subject.subject_id,
        CapacityCandidate.subject_incarnation == subject.subject_incarnation,
        CapacityCandidate.candidate_generation == subject.candidate_generation,
    ).execution_options(populate_existing=True))).one_or_none()
    _require_values(candidate, {
        "candidate_digest": origin.candidate_sha256,
        "candidate_identity_algorithm": "source-sha256", "candidate_identity": origin.candidate_sha256,
        "source_payload": {"publication_sha256": origin.candidate_publication_sha256},
        "artifact_payload": {"candidate_sha256": origin.candidate_sha256},
        "architecture_payload": {"supported_architectures": list(origin.supported_architectures), "supported_pool_ids": list(origin.supported_pool_ids)},
        "launcher_payload": {"local_activation_sha256": origin.local_activation_sha256},
        "attestation_payload": {"operation_id": str(origin.operation_id), "operation_epoch": origin.operation_epoch,
            "protected_admission_sha256": origin.protected_admission_sha256,
            "capacity_agent_installation_sha256": origin.capacity_agent_installation_sha256},
        "protocol_payload": origin.protocol_versions,
    }, label="application candidate")
    deployment = (await session.scalars(select(CapacityDeploymentGeneration).where(
        CapacityDeploymentGeneration.subject_id == subject.subject_id,
        CapacityDeploymentGeneration.subject_incarnation == subject.subject_incarnation,
        CapacityDeploymentGeneration.deployment_generation == subject.deployment_generation,
    ).execution_options(populate_existing=True))).one_or_none()
    _require_values(deployment, {
        "candidate_digest": origin.candidate_sha256,
        "required_profiles": [profile.model_dump(mode="json") for profile in subject.profiles],
        "readiness_state": "ready", "lifecycle_state": "active",
        "cutover_payload": {"local_activation_sha256": origin.local_activation_sha256,
            "candidate_publication_sha256": origin.candidate_publication_sha256,
            "protected_admission_sha256": origin.protected_admission_sha256,
            "capacity_agent_installation_sha256": origin.capacity_agent_installation_sha256},
    }, label="application deployment")
    reporter = (await session.scalars(select(CapacityDemandReporter).where(
        CapacityDemandReporter.subject_id == subject.subject_id,
        CapacityDemandReporter.subject_incarnation == subject.subject_incarnation,
        CapacityDemandReporter.reporter_incarnation == subject.demand_reporter_incarnation,
    ).execution_options(populate_existing=True))).one_or_none()
    _require_values(reporter, {"state": reporter_state,
        "configuration_generation": subject.configuration_generation,
        "deployment_generation": subject.deployment_generation,
        "token_sha256": projection.demand_reporter_token_sha256,
    }, label="application reporter")
    profiles = tuple((await session.scalars(select(CapacityWorkerProfile).where(
        CapacityWorkerProfile.subject_id == subject.subject_id,
        CapacityWorkerProfile.subject_incarnation == subject.subject_incarnation,
        CapacityWorkerProfile.deployment_generation == subject.deployment_generation,
    ).execution_options(populate_existing=True))).all())
    if len(profiles) != len(subject.profiles):
        raise ValueError("application worker profile set changed")
    for profile in subject.profiles:
        retained = next((item for item in profiles if item.pool_id == profile.pool_id), None)
        _require_values(retained, {
            "pool_generation": profile.pool_generation, "profile_generation": profile.profile_generation,
            "profile_digest": profile.profile_digest,
            "shape_catalog": [shape.model_dump(mode="json") for shape in profile.worker_shapes],
            "narrowing_constraints": {"eligible_resource_domains": list(profile.eligible_resource_domains)},
        }, label="application profile")
