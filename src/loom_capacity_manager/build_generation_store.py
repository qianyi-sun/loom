"""Stage native build-service facts without claiming executable readiness.

This is an internal persistence component, not an admission endpoint. The owning
typed membership transaction must authenticate its delegate, lock authority first,
verify complete history/release and perform replay/CAS before calling it. Staged
deployments are pending: runtime installation and executable readiness require
separate authenticated convergence. Application installation fields are not used.
"""

from __future__ import annotations

import json
from typing import Any, Literal

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.build_membership_contracts import (
    ExecutionPreparationV4,
    PersonalBuildMemberV1,
)
from loom_capacity_manager.contracts import FleetManifestV1, canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.models import (
    CapacityCandidate,
    CapacityDemandReporter,
    CapacityDeploymentGeneration,
    CapacityWorkerProfile,
)
from loom_capacity_manager.store import CapacityManagementStore, _write_transaction
from loom_capacity_manager.typed_membership_commands import (
    PersonalBuildCommandV2,
    PersonalMembershipMutationV2,
    derive_build_member,
    parse_typed_membership_mutation,
)


def _candidate_values(member: PersonalBuildMemberV1, preparation: ExecutionPreparationV4) -> dict[str, Any]:
    subject, candidate = member.configuration, member.acknowledgement.candidate
    return {
        "subject_id": subject.subject_id, "subject_incarnation": subject.subject_incarnation,
        "candidate_generation": subject.candidate_generation,
        "candidate_digest": canonical_executable_digest(candidate),
        "candidate_identity_algorithm": candidate.algorithm, "candidate_identity": candidate.identity,
        "source_payload": {"publication_sha256": candidate.publication_sha256},
        "artifact_payload": {"runtime_candidate": candidate.model_dump(mode="json")},
        "architecture_payload": {"platform_pools": {"linux/amd64": "oldlab", "linux/arm64": "gb10"}},
        "launcher_payload": {"purpose": member.purpose, "trusted_fleet_release_sha256": preparation.trusted_fleet_release_sha256},
        "attestation_payload": {"build_template_sha256": canonical_digest(preparation.personal_builds)},
        "protocol_payload": {profile.pool_id: {"generation": profile.protocol_generation, "digest": profile.protocol_digest} for profile in subject.profiles},
    }


def _deployment_values(member: PersonalBuildMemberV1, preparation: ExecutionPreparationV4) -> dict[str, Any]:
    subject, ack = member.configuration, member.acknowledgement
    return {
        "subject_id": subject.subject_id, "subject_incarnation": subject.subject_incarnation,
        "deployment_generation": subject.deployment_generation,
        "candidate_digest": canonical_executable_digest(ack.candidate),
        "required_profiles": [profile.model_dump(mode="json") for profile in subject.profiles],
        "readiness_state": "pending", "lifecycle_state": "active",
        "cutover_payload": {
            "purpose": member.purpose, "runtime_candidate": ack.candidate.model_dump(mode="json"),
            "build_template_sha256": canonical_digest(preparation.personal_builds),
            "protected_admission_sha256": ack.protected_admission_sha256,
        },
    }


def _require_values(row: object | None, expected: dict[str, Any], *, label: str) -> None:
    if row is None:
        raise ValueError(f"retained build {label} evidence changed")
    for field, value in expected.items():
        actual = getattr(row, field)
        if isinstance(value, (dict, list)):
            matches = json.dumps(actual, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) == json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        else:
            matches = actual == value and (not isinstance(value, (int, bool)) or type(actual) is type(value))
        if not matches:
            raise ValueError(f"retained build {label} evidence changed")


async def _require_build_installation_facts(
    session: AsyncSession, member: PersonalBuildMemberV1, preparation: ExecutionPreparationV4,
) -> None:
    """Verify retained pending installation, independently of mutable reporting."""
    subject = member.configuration
    candidate = (await session.scalars(select(CapacityCandidate).where(
        CapacityCandidate.subject_id == subject.subject_id,
        CapacityCandidate.subject_incarnation == subject.subject_incarnation,
        CapacityCandidate.candidate_generation == subject.candidate_generation,
    ).execution_options(populate_existing=True))).one_or_none()
    _require_values(candidate, _candidate_values(member, preparation), label="candidate")
    deployment = (await session.scalars(select(CapacityDeploymentGeneration).where(
        CapacityDeploymentGeneration.subject_id == subject.subject_id,
        CapacityDeploymentGeneration.subject_incarnation == subject.subject_incarnation,
        CapacityDeploymentGeneration.deployment_generation == subject.deployment_generation,
    ).execution_options(populate_existing=True))).one_or_none()
    _require_values(deployment, _deployment_values(member, preparation), label="deployment")
    profiles = (await session.scalars(select(CapacityWorkerProfile).where(
        CapacityWorkerProfile.subject_id == subject.subject_id,
        CapacityWorkerProfile.subject_incarnation == subject.subject_incarnation,
        CapacityWorkerProfile.deployment_generation == subject.deployment_generation,
    ).execution_options(populate_existing=True))).all()
    if len(profiles) != len(subject.profiles):
        raise ValueError("retained build worker profile set changed")
    for profile in subject.profiles:
        row = next((item for item in profiles if item.pool_id == profile.pool_id), None)
        _require_values(row, {
            "pool_generation": profile.pool_generation, "profile_generation": profile.profile_generation,
            "profile_digest": profile.profile_digest,
            "shape_catalog": [shape.model_dump(mode="json") for shape in profile.worker_shapes],
            "narrowing_constraints": {"eligible_resource_domains": list(profile.eligible_resource_domains)},
        }, label="worker profile")


async def _require_staged_facts(
    session: AsyncSession, request: PersonalMembershipMutationV2,
    member: PersonalBuildMemberV1, preparation: ExecutionPreparationV4, *, reporter_state: Literal["current", "fenced"] = "current",
) -> None:
    await _require_build_installation_facts(session, member, preparation)
    subject = member.configuration
    reporter = (await session.scalars(select(CapacityDemandReporter).where(
        CapacityDemandReporter.subject_id == subject.subject_id,
        CapacityDemandReporter.subject_incarnation == subject.subject_incarnation,
        CapacityDemandReporter.reporter_incarnation == subject.demand_reporter_incarnation,
    ).execution_options(populate_existing=True))).one_or_none()
    _require_values(reporter, {
        "configuration_generation": subject.configuration_generation,
        "deployment_generation": subject.deployment_generation, "state": reporter_state,
        "token_sha256": request.command.projection.demand_reporter_token_sha256,
    }, label="reporter")


async def stage_build_generation_evidence(
    session: AsyncSession, request: PersonalMembershipMutationV2, member: PersonalBuildMemberV1,
    preparation: ExecutionPreparationV4, fleet: FleetManifestV1, *,
    previous: PersonalBuildMemberV1 | None = None,
    previous_request: PersonalMembershipMutationV2 | None = None,
) -> None:
    """Stage facts in the caller's SERIALIZABLE membership transaction.

The caller must already authenticate the current operation and any predecessor
release. This component neither grants membership nor promotes pending runtime
installation to ready. It rolls back its writes on error and never commits an
enclosing transaction. History/replay identity is owned by the membership store.
"""
    request = parse_typed_membership_mutation(canonical_bytes(request))
    preparation = ExecutionPreparationV4.model_validate_json(preparation.model_dump_json())
    fleet = FleetManifestV1.model_validate_json(fleet.model_dump_json())
    if not isinstance(request.command, PersonalBuildCommandV2):
        raise ValueError("build staging requires a build command")
    checked = derive_build_member(request, preparation, fleet, reincarnation=member.reincarnation)
    if canonical_bytes(member) != canonical_bytes(checked):
        raise ValueError("build staging member differs from its command")
    member = checked
    if (previous is None) != (previous_request is None):
        raise ValueError("build staging requires the original predecessor request and member")
    projection, subject = request.command.projection, member.configuration
    if previous is not None and previous_request is not None:
        previous_request = parse_typed_membership_mutation(canonical_bytes(previous_request))
        expected_previous = derive_build_member(previous_request, preparation, fleet, reincarnation=previous.reincarnation)
        if canonical_bytes(previous) != canonical_bytes(expected_previous):
            raise ValueError("build staging predecessor differs from its original command")
        previous = expected_previous
        old = previous.configuration
        recreating = old.lifecycle_state == "disabled" and projection.operation_kind == "create"
        if (
            subject.subject_id != old.subject_id or member.owner_id != previous.owner_id
            or subject.configuration_generation <= old.configuration_generation
            or (not recreating and (subject.subject_incarnation != old.subject_incarnation or old.lifecycle_state == "disabled"))
            or (projection.operation_kind == "create" and not recreating)
        ):
            raise ValueError("build staging lifecycle identity changed")
        if recreating:
            if member.reincarnation is None or member.reincarnation.predecessor != old:
                raise ValueError("build staging requires predecessor release evidence")
        elif projection.operation_kind == "update":
            if subject.deployment_generation <= old.deployment_generation or subject.candidate_generation < old.candidate_generation:
                raise ValueError("build staging deployment generation must advance")
        elif (
            subject.deployment_generation != old.deployment_generation or subject.candidate_generation != old.candidate_generation
            or subject.demand_reporter_incarnation != old.demand_reporter_incarnation
            or projection.demand_reporter_token_sha256 != previous_request.command.projection.demand_reporter_token_sha256
            or member.acknowledgement.protected_admission_sha256 != previous.acknowledgement.protected_admission_sha256
        ):
            raise ValueError("non-deployment build staging must retain its service evidence")
    elif projection.operation_kind != "create" or subject.candidate_generation != 1 or subject.deployment_generation != 1:
        raise ValueError("initial build staging requires a fresh service generation")

    management = CapacityManagementStore()
    async with _write_transaction(session):
        if previous is not None and previous_request is not None:
            await _require_staged_facts(session, previous_request, previous, preparation)
        if projection.operation_kind in {"create", "update"}:
            reporter_conflict = (await session.scalars(select(CapacityDemandReporter.id).where(or_(
                CapacityDemandReporter.reporter_incarnation == subject.demand_reporter_incarnation,
                CapacityDemandReporter.token_sha256 == projection.demand_reporter_token_sha256,
            )).limit(1))).first()
            if reporter_conflict is not None:
                raise ValueError("build reporter identity or token was already used")
            existing = (await session.scalars(select(CapacityCandidate).where(
                CapacityCandidate.subject_id == subject.subject_id,
                CapacityCandidate.subject_incarnation == subject.subject_incarnation,
                CapacityCandidate.candidate_generation == subject.candidate_generation,
            ))).one_or_none()
            candidate_values = _candidate_values(member, preparation)
            if existing is None:
                session.add(CapacityCandidate(**candidate_values))
            else:
                _require_values(existing, candidate_values, label="candidate")
            session.add(CapacityDeploymentGeneration(**_deployment_values(member, preparation)))
            for profile in subject.profiles:
                await management._persist_worker_profile(session, subject, profile)
        await management._register_demand_reporter(session, subject, token_sha256=(
            None if projection.operation_kind == "destroy" else projection.demand_reporter_token_sha256
        ))
        await session.flush()
