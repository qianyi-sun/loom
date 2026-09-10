"""Route-local personal reporter identity; never executable admission authority."""

from __future__ import annotations

import json
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.auth import AuthorizationError, CapacityPrincipal
from loom_capacity_manager.contracts import (
    ConfigurationGenerationRefV1,
    DynamicDevelopmentSubjectProjectionV1,
    SubjectConfigurationV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionPlanProposalV2,
    canonical_executable_digest,
)
from loom_capacity_manager.execution_store import _ADMISSION_CLOSE_REASONS, _admission_closure_id
from loom_capacity_manager.membership_contracts import ExecutionPreparationV3
from loom_capacity_manager.membership_store import _validated_membership_history
from loom_capacity_manager.models import (
    CapacityAuthorityState,
    CapacityConfigGeneration,
    CapacityConfigurationEpoch,
    CapacityDemandReporter,
    CapacityDevelopmentProjection,
    CapacityExecutableAdmissionAcknowledgement,
    CapacityExecutableAdmissionClosureAcknowledgement,
    CapacityExecutableAdmissionProposal,
    CapacityExecutionEpoch,
    CapacityPersonalMembershipEvent,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    CapacityStoreError,
    _parse_contract,
    _write_transaction,
)


async def _personal_base(
    session: AsyncSession,
    epoch: CapacityExecutionEpoch,
    preparation: ExecutionPreparationV3,
    subject_id: UUID,
) -> tuple[SubjectConfigurationV1, str, str] | None:
    base = await session.get(CapacityConfigurationEpoch, epoch.configuration_epoch)
    if base is None:
        raise AuthorizationError("invalid capacity credentials")
    references = tuple(
        _parse_contract(ConfigurationGenerationRefV1, value)
        for value in base.subject_generation_manifest
    )
    reference = next((value for value in references if value.subject_id == subject_id), None)
    if reference is None:
        return None
    row = (
        await session.execute(
            select(CapacityConfigGeneration).where(
                CapacityConfigGeneration.scope == "subject",
                CapacityConfigGeneration.subject_id == subject_id,
                CapacityConfigGeneration.subject_incarnation == reference.subject_incarnation,
                CapacityConfigGeneration.scope_generation == reference.generation,
                CapacityConfigGeneration.digest == reference.digest,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise AuthorizationError("invalid capacity credentials")
    subject = _parse_contract(SubjectConfigurationV1, row.payload)
    projections = (
        (
            await session.execute(
                select(CapacityDevelopmentProjection)
                .where(
                    CapacityDevelopmentProjection.subject_id == subject_id,
                    CapacityDevelopmentProjection.subject_incarnation
                    == reference.subject_incarnation,
                    CapacityDevelopmentProjection.configuration_generation == reference.generation,
                )
                .limit(2)
            )
        )
        .scalars()
        .all()
    )
    if not projections:
        return None
    if len(projections) != 1:
        raise AuthorizationError("invalid capacity credentials")
    projection = projections[0]
    request = _parse_contract(DynamicDevelopmentSubjectProjectionV1, projection.request_payload)
    acknowledgement = next(
        (value for value in preparation.subject_acknowledgements if value.subject_id == subject_id),
        None,
    )
    if (
        canonical_digest(subject) != reference.digest
        or subject.subject_id != subject_id
        or subject.subject_incarnation != reference.subject_incarnation
        or subject.configuration_generation != reference.generation
        or canonical_digest(request) != projection.request_digest
        or request.operation_id != projection.operation_id
        or request.subject_id != subject_id
        or request.subject_incarnation != subject.subject_incarnation
        or request.configuration_generation != subject.configuration_generation
        or request.deployment_generation != subject.deployment_generation
        or request.demand_reporter_incarnation != subject.demand_reporter_incarnation
        or subject.account_id != f"dev-owner-{request.owner_id.hex}"
        or _parse_contract(SubjectConfigurationV1, projection.result_payload["subject"]) != subject
        or acknowledgement is None
        or acknowledgement.subject_incarnation != subject.subject_incarnation
        or acknowledgement.configuration_generation != subject.configuration_generation
        or acknowledgement.deployment_generation != subject.deployment_generation
        or acknowledgement.reporter_incarnation != subject.demand_reporter_incarnation
    ):
        raise AuthorizationError("invalid capacity credentials")
    return subject, request.demand_reporter_token_sha256, request.operation_kind


async def _retained_closure_epoch(
    session: AsyncSession,
    reporter: CapacityDemandReporter,
    *,
    subject_id: UUID,
    closure_id: UUID | None,
) -> tuple[CapacityExecutionEpoch, ExecutableAdmissionPlanProposalV2] | None:
    """Resolve only retired, exact reporter work; never a live admission proposal."""

    if subject_id != reporter.subject_id:
        return None
    statement = (
        select(CapacityExecutableAdmissionProposal, CapacityExecutionEpoch)
        .join(
            CapacityExecutionEpoch,
            CapacityExecutionEpoch.execution_epoch
            == CapacityExecutableAdmissionProposal.execution_epoch,
        )
        .where(
            CapacityExecutionEpoch.state == "retired",
            CapacityExecutableAdmissionProposal.subject_id == reporter.subject_id,
            CapacityExecutableAdmissionProposal.subject_incarnation == reporter.subject_incarnation,
            CapacityExecutableAdmissionProposal.reporter_incarnation
            == reporter.reporter_incarnation,
            ~select(CapacityExecutableAdmissionAcknowledgement.id)
            .where(
                CapacityExecutableAdmissionAcknowledgement.proposal_id
                == CapacityExecutableAdmissionProposal.proposal_id
            )
            .exists(),
        )
        .order_by(
            CapacityExecutableAdmissionProposal.created_at, CapacityExecutableAdmissionProposal.id
        )
    )
    if closure_id is None:
        statement = statement.where(
            ~select(CapacityExecutableAdmissionClosureAcknowledgement.id)
            .where(
                CapacityExecutableAdmissionClosureAcknowledgement.proposal_id
                == CapacityExecutableAdmissionProposal.proposal_id
            )
            .exists(),
        ).limit(1)
    else:
        # Receipted closures remain authenticatable for exact idempotent retry.
        statement = statement.where(
            ~select(CapacityExecutableAdmissionClosureAcknowledgement.id)
            .where(
                CapacityExecutableAdmissionClosureAcknowledgement.proposal_id
                == CapacityExecutableAdmissionProposal.proposal_id
            )
            .exists()
            | select(CapacityExecutableAdmissionClosureAcknowledgement.id)
            .where(
                CapacityExecutableAdmissionClosureAcknowledgement.proposal_id
                == CapacityExecutableAdmissionProposal.proposal_id,
                CapacityExecutableAdmissionClosureAcknowledgement.closure_id == closure_id,
            )
            .exists()
        )
    for row, epoch in (await session.execute(statement.with_for_update(read=True))).all():
        proposal = ExecutableAdmissionPlanProposalV2.model_validate_json(
            json.dumps(row.proposal_payload)
        )
        anchor = proposal.shapes[0].binding
        if (
            canonical_executable_digest(proposal) != row.proposal_digest
            or proposal.proposal_id != row.proposal_id
            or proposal.reporter_incarnation != reporter.reporter_incarnation
            or anchor.subject_id != reporter.subject_id
            or anchor.subject_incarnation != reporter.subject_incarnation
            or anchor.deployment_generation != reporter.deployment_generation
            or anchor.execution.execution_epoch != epoch.execution_epoch
            or anchor.execution.execution_manifest_sha256 != epoch.execution_manifest_sha256
            or row.execution_manifest_sha256 != epoch.execution_manifest_sha256
        ):
            raise AuthorizationError("invalid capacity credentials")
        if closure_id is None or any(
            closure_id == _admission_closure_id(proposal.proposal_id, row.proposal_digest, reason)
            for reason in _ADMISSION_CLOSE_REASONS
        ):
            return epoch, proposal
    return None


async def authenticate_personal_subject_agent(
    session: AsyncSession,
    management: CapacityManagementStore,
    *,
    token_sha256: str,
    retained_closure_subject_id: UUID | None = None,
    retained_closure_id: UUID | None = None,
) -> CapacityPrincipal:
    """Authenticate a retained personal identity, not permission for any work item.

    Only the protected subject routes install this dependency. Their stores must
    independently bind the identity to pinned work and enforce increase/cleanup
    policy under their own authority lock after this transaction finishes.
    """

    try:
        async with _write_transaction(session):
            authority = (
                await session.execute(
                    select(CapacityAuthorityState)
                    .where(
                        CapacityAuthorityState.singleton_id == 1,
                    )
                    .with_for_update(read=True)
                )
            ).scalar_one_or_none()
            if authority is None:
                raise AuthorizationError("invalid capacity credentials")
            reporters = (
                (
                    await session.execute(
                        select(CapacityDemandReporter)
                        .where(
                            CapacityDemandReporter.token_sha256 == token_sha256,
                        )
                        .limit(2)
                        .with_for_update(read=True)
                    )
                )
                .scalars()
                .all()
            )
            if len(reporters) != 1 or reporters[0].state not in {"current", "fenced"}:
                raise AuthorizationError("invalid capacity credentials")
            reporter = reporters[0]
            retained = (
                None
                if retained_closure_subject_id is None
                else await _retained_closure_epoch(
                    session,
                    reporter,
                    subject_id=retained_closure_subject_id,
                    closure_id=retained_closure_id,
                )
            )
            if retained is not None:
                epoch = retained[0]
            else:
                if authority.execution_state not in {"active", "drain-only"}:
                    raise AuthorizationError("invalid capacity credentials")
                current_epoch = await session.get(CapacityExecutionEpoch, authority.execution_epoch)
                if current_epoch is None:
                    raise AuthorizationError("invalid capacity credentials")
                epoch = current_epoch
                management._execution_context(authority, epoch)
            if epoch.manifest_payload.get("schema_version") == 4:
                return await _authenticate_typed_reporter(
                    session, epoch, reporter, token_sha256=token_sha256,
                    archive_only=retained is not None,
                )
            preparation = management._execution_preparation_from_row(epoch)
            if not isinstance(preparation, ExecutionPreparationV3):
                raise AuthorizationError("invalid capacity credentials")
            rows = (
                (
                    await session.execute(
                        select(CapacityPersonalMembershipEvent)
                        .where(
                            CapacityPersonalMembershipEvent.execution_epoch
                            == epoch.execution_epoch,
                        )
                        .order_by(CapacityPersonalMembershipEvent.revision)
                    )
                )
                .scalars()
                .all()
            )
            results = await _validated_membership_history(session, rows, epoch)
            history: list[tuple[SubjectConfigurationV1, str, str]] = []
            base = await _personal_base(session, epoch, preparation, reporter.subject_id)
            if base is not None:
                history.append(base)
            for row, result in zip(rows, results, strict=True):
                if row.actor != preparation.personal_membership.management_principal_id:
                    raise AuthorizationError("invalid capacity credentials")
                if row.subject_id == reporter.subject_id:
                    projection = row.request_payload["projection"]
                    history.append(
                        (
                            result.member.configuration,
                            projection["demand_reporter_token_sha256"],
                            projection["operation_kind"],
                        )
                    )
            matched = next(
                (
                    index
                    for index in reversed(range(len(history)))
                    if (
                        history[index][0].subject_incarnation == reporter.subject_incarnation
                        and history[index][0].demand_reporter_incarnation
                        == reporter.reporter_incarnation
                        and history[index][0].configuration_generation
                        == reporter.configuration_generation
                        and history[index][0].deployment_generation
                        == reporter.deployment_generation
                        and history[index][1] == token_sha256
                    )
                ),
                None,
            )
            if matched is None:
                raise AuthorizationError("invalid capacity credentials")
            if reporter.state == "current":
                if matched != len(history) - 1:
                    raise AuthorizationError("invalid capacity credentials")
            else:
                if matched == len(history) - 1:
                    raise AuthorizationError("invalid capacity credentials")
                old = history[matched][0]
                successor, _token, operation = history[matched + 1]
                if (
                    successor.demand_reporter_incarnation == reporter.reporter_incarnation
                    or successor.configuration_generation <= old.configuration_generation
                    or not (
                        (
                            operation == "update"
                            and successor.subject_incarnation == old.subject_incarnation
                            and successor.deployment_generation > old.deployment_generation
                        )
                        or (
                            operation == "create"
                            and successor.subject_incarnation != old.subject_incarnation
                            and old.lifecycle_state == "disabled"
                        )
                    )
                ):
                    raise AuthorizationError("invalid capacity credentials")
            return CapacityPrincipal(
                principal_id=f"personal-agent-{reporter.subject_id}-{reporter.reporter_incarnation}",
                # Archive-only identity carries no generally usable scope. The
                # two closure routes accept it only for their retained work.
                scopes=frozenset()
                if retained is not None
                else frozenset({"capacity:report:demand"}),
                subject_id=reporter.subject_id,
                subject_incarnation=reporter.subject_incarnation,
                demand_reporter_incarnation=reporter.reporter_incarnation,
                pool_id=None,
                pool_reporter_incarnation=None,
                executor_id=None,
                executor_incarnation=None,
                executor_pool_generation=None,
            )
    except (CapacityStoreError, ValueError, KeyError) as exc:
        raise AuthorizationError("invalid capacity credentials") from exc


async def _authenticate_typed_reporter(
    session: AsyncSession, epoch: CapacityExecutionEpoch, reporter: CapacityDemandReporter,
    *, token_sha256: str, archive_only: bool,
) -> CapacityPrincipal:
    """Authenticate V4 installation history without granting build readiness.

    The immutable reader authenticates lifecycle transitions and historical
    installation facts. Only this target reporter's state is checked here;
    another owner's equivocation must not disable cleanup for this identity.
    Source-bearing epochs remain closed until their graph consumer is connected.
    """
    from loom_capacity_manager.typed_membership_store import _load_typed_immutable_history

    history = await _load_typed_immutable_history(session, epoch.execution_epoch)
    binding = history.reporter_bindings.get(reporter.reporter_incarnation)
    if binding is None:
        raise AuthorizationError("invalid capacity credentials")
    subject, token = binding
    if (
        token != token_sha256
        or subject.subject_id != reporter.subject_id
        or subject.subject_incarnation != reporter.subject_incarnation
        or subject.configuration_generation != reporter.configuration_generation
        or subject.deployment_generation != reporter.deployment_generation
    ):
        raise AuthorizationError("invalid capacity credentials")
    tips = {origin.configuration.subject_id: origin.configuration
        for origin in history.preparation.managed_application_origins}
    tips.update({origin.configuration.subject_id: origin.configuration
        for origin in history.preparation.managed_build_origins})
    tips.update({identity: result.member.configuration for identity, result in history.latest.items()})
    tip = tips.get(reporter.subject_id)
    if tip is None or (
        not archive_only and reporter.state != (
            "current" if tip.demand_reporter_incarnation == reporter.reporter_incarnation else "fenced"
        )
    ):
        raise AuthorizationError("invalid capacity credentials")
    return CapacityPrincipal(
        principal_id=f"personal-agent-{reporter.subject_id}-{reporter.reporter_incarnation}",
        scopes=frozenset() if archive_only else frozenset({"capacity:report:demand"}),
        subject_id=reporter.subject_id, subject_incarnation=reporter.subject_incarnation,
        demand_reporter_incarnation=reporter.reporter_incarnation,
        pool_id=None, pool_reporter_incarnation=None, executor_id=None,
        executor_incarnation=None, executor_pool_generation=None,
    )
