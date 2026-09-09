"""Internal operator import of a retired application-only typed configuration.

This composes the existing immutable proposal/configuration activation workflow;
it is not an HTTP endpoint or execution activation. The idempotency key identifies
the derived ConfigurationActivationV1, not a separate membership import receipt.
Build rollover remains closed: no build member may be silently omitted.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import UUID, uuid5

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.application_generation_store import require_application_reporter_evidence
from loom_capacity_manager.application_origin_contracts import ManagedApplicationOriginV1
from loom_capacity_manager.build_generation_store import _require_values
from loom_capacity_manager.build_membership_contracts import PersonalMembershipSnapshotV2
from loom_capacity_manager.contracts import (
    ConfigurationActivationV1,
    ConfigurationGenerationRefV1,
    SubjectConfigurationV1,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionDrainV2,
    ExecutionRetirementV2,
    canonical_executable_digest,
)
from loom_capacity_manager.membership_contracts import PersonalApplicationMemberV1
from loom_capacity_manager.models import (
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityConfigGeneration,
    CapacityConfigurationEpoch,
    CapacityDemandReporter,
    CapacityExecutionEpoch,
    CapacityPoolReporter,
    CapacityWorkerProfile,
)
from loom_capacity_manager.store import (
    ActivatedConfiguration,
    CapacityManagementStore,
    ConfigurationConflictError,
    ExecutionConflictError,
    _lock_shadow_authority,
    _write_transaction,
)
from loom_capacity_manager.typed_membership_commands import (
    PersonalApplicationCommandV2,
    parse_typed_membership_mutation,
)
from loom_capacity_manager.typed_membership_store import (
    _load_base_configurations,
    _load_typed_immutable_history,
    _validated_materialization,
)


@dataclass(frozen=True)
class ImportedRetiredApplications:
    configuration: ActivatedConfiguration
    origins: tuple[ManagedApplicationOriginV1, ...]


def _reference(subject: SubjectConfigurationV1) -> ConfigurationGenerationRefV1:
    return ConfigurationGenerationRefV1(scope="subject", subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation, generation=subject.configuration_generation,
        digest=canonical_digest(subject))


def _require_retired(epoch: CapacityExecutionEpoch) -> None:
    if (epoch.state != "retired" or epoch.retired_at is None or epoch.activated_at is None
        or epoch.drain_only_at is None or not epoch.activation_actor
        or epoch.activation_idempotency_key is None or not epoch.activation_request_digest
        or not epoch.retirement_actor or epoch.retirement_idempotency_key is None
        or not epoch.drain_actor or epoch.drain_idempotency_key is None):
        raise ExecutionConflictError("application import requires an activated, drained and retired epoch")
    try:
        drain = ExecutionDrainV2.model_validate_json(json.dumps(epoch.drain_request_payload))
        retirement = ExecutionRetirementV2.model_validate_json(json.dumps(epoch.retirement_request_payload))
        if (canonical_executable_digest(drain) != epoch.drain_request_digest
            or canonical_executable_digest(retirement) != epoch.retirement_request_digest
            or epoch.effective_ceiling != 0 or epoch.effective_rate_per_minute != 0
            or not epoch.activated_at <= epoch.drain_only_at <= epoch.retired_at):
            raise ValueError("retirement evidence changed")
        for request in (drain, retirement):
            if (request.authority_incarnation != epoch.authority_incarnation
                or request.expected_writer_epoch != epoch.current_writer_epoch
                or request.execution_epoch != epoch.execution_epoch
                or request.execution_manifest_sha256 != epoch.execution_manifest_sha256):
                raise ValueError("retirement identity changed")
        for checkpoint in retirement.executor_checkpoints:
            if any(getattr(checkpoint, field) != getattr(epoch, f"{checkpoint.pool_id}_{field}")
                for field in ("executor_id", "executor_incarnation", "pool_generation")):
                raise ValueError("retired executor identity changed")
    except ValueError as exc:
        raise ExecutionConflictError("application import retirement evidence is invalid") from exc


async def import_retired_applications(
    session: AsyncSession, management: CapacityManagementStore, *, execution_epoch: int,
    expected_snapshot: PersonalMembershipSnapshotV2, actor: str, idempotency_key: UUID,
) -> ImportedRetiredApplications:
    """Reuse exact retained generations under caller-authenticated operator scope.

    The entire configuration is imported atomically; no caller-selected subset or
    replacement installation is accepted. Replay retains ordinary configuration
    activation semantics and remains unavailable while execution is active.
    """
    if not actor.strip() or not isinstance(idempotency_key, UUID) or idempotency_key.int == 0:
        raise ConfigurationConflictError("application import requires actor and nonzero identity")
    try:
        async with _write_transaction(session):
            # Refresh before the existing shadow helper can see a cached singleton.
            (await session.scalars(select(CapacityAuthorityState).with_for_update()
                .execution_options(populate_existing=True))).one_or_none()
            authority = await _lock_shadow_authority(session)
            history = await _load_typed_immutable_history(session, execution_epoch)
            epoch = history.epoch
            _require_retired(epoch)
            if authority.authority_incarnation != epoch.authority_incarnation:
                raise ExecutionConflictError("retired application authority incarnation changed")
            if canonical_bytes(expected_snapshot) != canonical_bytes(history.snapshot()):
                raise ConfigurationConflictError("application import requires the complete final snapshot")
            if any(not isinstance(result.member, PersonalApplicationMemberV1) for result in history.results):
                raise ConfigurationConflictError("build membership successor import is not yet supported")

            subjects = await _load_base_configurations(session, epoch)
            subjects.update({identity: result.member.configuration for identity, result in history.latest.items()})
            origins = {origin.configuration.subject_id: origin for origin in history.preparation.managed_application_origins}
            for row, result in zip(history.events, history.results, strict=True):
                request = parse_typed_membership_mutation(json.dumps(row.request_payload))
                assert isinstance(request.command, PersonalApplicationCommandV2)
                projection = request.command.projection
                previous = origins.get(row.subject_id)
                installation = projection if projection.operation_kind in {"create", "update"} else (
                    None if previous is None else previous.installation_projection)
                if installation is None:
                    raise ConfigurationConflictError("application import installation origin is missing")
                origins[row.subject_id] = ManagedApplicationOriginV1(configuration=result.member.configuration,
                    acknowledgement=result.member.acknowledgement, base_projection=projection,
                    installation_projection=installation)
            ordered_origins = tuple(origins[identity] for identity in sorted(origins, key=lambda value: value.int))
            acknowledgements = {ack.subject_id: ack for ack in history.preparation.subject_acknowledgements}
            for subject_id in subjects.keys() - origins.keys():
                subject, ack = subjects[subject_id], acknowledgements.get(subject_id)
                if (ack is None or ack.subject_incarnation != subject.subject_incarnation
                    or ack.configuration_generation != subject.configuration_generation
                    or ack.deployment_generation != subject.deployment_generation
                    or ack.reporter_incarnation != subject.demand_reporter_incarnation):
                    raise ConfigurationConflictError("static import acknowledgement changed")
                candidate = (await session.scalars(select(CapacityCandidate).where(
                    CapacityCandidate.subject_id == subject_id,
                    CapacityCandidate.subject_incarnation == subject.subject_incarnation,
                    CapacityCandidate.candidate_generation == subject.candidate_generation)
                    .execution_options(populate_existing=True))).one_or_none()
                _require_values(candidate, {"candidate_identity_algorithm": ack.candidate.algorithm,
                    "candidate_identity": ack.candidate.identity,
                    "candidate_digest": ack.candidate.identity if ack.candidate.algorithm == "source-sha256" else ack.candidate.publication_sha256}, label="static import candidate")
                if candidate is None or candidate.source_payload.get("publication_sha256") != ack.candidate.publication_sha256:
                    raise ConfigurationConflictError("static import candidate publication changed")
            activation = ConfigurationActivationV1(expected_configuration_epoch=epoch.configuration_epoch,
                fleet=ConfigurationGenerationRefV1(scope="fleet", generation=epoch.fleet_generation, digest=epoch.fleet_digest),
                subjects=tuple(_reference(subject) for subject in subjects.values()))
            replay = (await session.scalars(select(CapacityConfigurationEpoch).where(
                CapacityConfigurationEpoch.activation_idempotency_key == idempotency_key)
                .execution_options(populate_existing=True))).one_or_none()
            if replay is not None:
                configuration = management._replay_configuration_epoch(replay, actor=actor,
                    request_digest=canonical_digest(activation), conflict_message="application configuration activation key changed")
                return ImportedRetiredApplications(configuration, ordered_origins)
            latest_epoch = await session.scalar(select(CapacityConfigurationEpoch.configuration_epoch)
                .order_by(CapacityConfigurationEpoch.configuration_epoch.desc()).limit(1))
            if latest_epoch != epoch.configuration_epoch:
                raise ConfigurationConflictError("application import configuration has already advanced")
            await _validated_materialization(session, epoch, history.fleet, history.latest)
            for pool in history.fleet.pools:
                pool_reporter = (await session.scalars(select(CapacityPoolReporter).where(
                    CapacityPoolReporter.pool_id == pool.pool_id,
                    CapacityPoolReporter.reporter_incarnation == pool.pool_reporter_incarnation)
                    .execution_options(populate_existing=True))).one_or_none()
                _require_values(pool_reporter, {"state": "current", "pool_generation": pool.pool_generation}, label="import pool reporter")
            for reporter_id, (subject, token) in history.reporter_bindings.items():
                await require_application_reporter_evidence(session, subject, token,
                    reporter_state="current" if subjects[subject.subject_id].demand_reporter_incarnation == reporter_id else "fenced")
            # Refresh static rows as well: activation must not conceal a changed
            # profile or reporter by reusing stale ORM identities or repairing it.
            for subject in subjects.values():
                profiles = (await session.scalars(select(CapacityWorkerProfile).where(
                    CapacityWorkerProfile.subject_id == subject.subject_id,
                    CapacityWorkerProfile.subject_incarnation == subject.subject_incarnation,
                    CapacityWorkerProfile.deployment_generation == subject.deployment_generation)
                    .execution_options(populate_existing=True))).all()
                if len(profiles) != len(subject.profiles):
                    raise ConfigurationConflictError("application import profile set changed")
                for profile in subject.profiles:
                    profile_row = next((value for value in profiles if value.pool_id == profile.pool_id), None)
                    _require_values(profile_row, {"pool_generation": profile.pool_generation,
                        "profile_generation": profile.profile_generation, "profile_digest": profile.profile_digest,
                        "shape_catalog": [shape.model_dump(mode="json") for shape in profile.worker_shapes],
                        "narrowing_constraints": {"eligible_resource_domains": list(profile.eligible_resource_domains)}}, label="import profile")
                reporter = (await session.scalars(select(CapacityDemandReporter).where(
                    CapacityDemandReporter.subject_id == subject.subject_id,
                    CapacityDemandReporter.subject_incarnation == subject.subject_incarnation,
                    CapacityDemandReporter.reporter_incarnation == subject.demand_reporter_incarnation)
                    .execution_options(populate_existing=True))).one_or_none()
                _require_values(reporter, {"state": "current", "configuration_generation": subject.configuration_generation,
                    "deployment_generation": subject.deployment_generation}, label="import reporter")
                reference = _reference(subject)
                generation = (await session.scalars(select(CapacityConfigGeneration).where(
                    CapacityConfigGeneration.scope == "subject", CapacityConfigGeneration.subject_id == subject.subject_id,
                    CapacityConfigGeneration.subject_incarnation == subject.subject_incarnation,
                    CapacityConfigGeneration.scope_generation == subject.configuration_generation)
                    .execution_options(populate_existing=True))).one_or_none()
                if generation is None:
                    await management.propose_subject_configuration(session, subject, actor=actor,
                        idempotency_key=uuid5(idempotency_key, f"retired-application:{subject.subject_id}"))
                else:
                    _require_values(generation, {"digest": reference.digest, "payload": subject.model_dump(mode="json")}, label="import generation")
            configuration = await management.activate_configuration(session, activation, actor=actor, idempotency_key=idempotency_key)
            return ImportedRetiredApplications(configuration, ordered_origins)
    except ValueError as exc:
        raise ConfigurationConflictError("retired application import evidence changed") from exc
