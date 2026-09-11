"""Transactional typed build membership; not executable admission.

The caller supplies an already authenticated management principal. This store
checks its pinned delegation against current durable authority, never a caller
preparation/fleet. Fresh/managed application and pending-build lifecycle include
release-gated recreation; executable V4 admission remains closed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.application_generation_store import (
    require_application_installation_evidence,
)
from loom_capacity_manager.application_origin_contracts import ManagedApplicationOriginV1
from loom_capacity_manager.build_generation_store import (
    _require_build_installation_facts,
    _require_values,
    stage_build_generation_evidence,
)
from loom_capacity_manager.build_membership_contracts import (
    ExecutionPreparationV4,
    PersonalBuildMemberV1,
    PersonalMembershipSnapshotV2,
)
from loom_capacity_manager.contracts import (
    AccountPolicyV1,
    ConfigurationGenerationRefV1,
    ConfigurationSnapshotV1,
    DynamicDevelopmentSubjectProjectionV1,
    FleetManifestV1,
    SubjectConfigurationV1,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionAuthorityV2,
    canonical_executable_digest,
)
from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMemberV1,
    PersonalMembershipCheckpointV1,
    PersonalReincarnationEvidenceV1,
)
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.membership_release import predecessor_release_sha256
from loom_capacity_manager.membership_store import (
    CapacityMembershipStore,
    PersonalMembershipRevisionConflictError,
)
from loom_capacity_manager.models import (
    CapacityAccountPolicy,
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityConfigGeneration,
    CapacityConfigurationEpoch,
    CapacityDemandReporter,
    CapacityExecutionEpoch,
    CapacityPersonalMembershipEvent,
    CapacitySubject,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    ConfigurationConflictError,
    ExecutionConflictError,
    IdempotencyConflictError,
    _canonical_json_digest,
    _derive_development_subject,
    _derive_owner_account,
    _parse_contract,
    _subject_scalars_match,
    _write_transaction,
)
from loom_capacity_manager.successor_origin_contracts import (
    ManagedApplicationOriginV2,
    ManagedBuildOriginV1,
)
from loom_capacity_manager.typed_membership_commands import (
    PersonalApplicationCommandV2,
    PersonalBuildCommandV2,
    PersonalMembershipMutationV2,
    PersonalMembershipResultV2,
    derive_application_member,
    derive_build_member,
    parse_typed_membership_mutation,
)
from loom_capacity_manager.typed_membership_events import validate_typed_membership_event_prefix


@dataclass(frozen=True)
class _TypedHistory:
    epoch: CapacityExecutionEpoch
    preparation: ExecutionPreparationV4
    fleet: FleetManifestV1
    events: tuple[CapacityPersonalMembershipEvent, ...]
    results: tuple[PersonalMembershipResultV2, ...]
    latest: dict[UUID, PersonalMembershipResultV2]
    latest_requests: dict[UUID, PersonalMembershipMutationV2]
    reporter_bindings: dict[UUID, tuple[SubjectConfigurationV1, str]]

    def snapshot(self, through_revision: int | None = None) -> PersonalMembershipSnapshotV2:
        revision = len(self.events) if through_revision is None else through_revision
        if type(revision) is not int or revision < 0 or revision > len(self.events):
            raise ConfigurationConflictError("typed membership revision is invalid or unavailable")
        latest = {result.member.configuration.subject_id: result.member for result in self.results[:revision]}
        return PersonalMembershipSnapshotV2(
            namespace_id=self.preparation.personal_membership.namespace_id,
            revision=revision, head_sha256=self.events[revision - 1].head_sha256 if revision else "0" * 64,
            members=tuple(latest.values()),
        )


async def _load_typed_immutable_history(session: AsyncSession, execution_epoch: int) -> _TypedHistory:
    """Authenticate immutable history and installation, never reporter currentness.

    Read the whole log even for a prefix to authenticate installation origins and
    lifecycle structure. A later epoch may have advanced mutable reporter rows.
    Current consumers must use _load_typed_history under their authority fence.
    """
    return await _load_typed_history_node(session, execution_epoch)


async def _load_typed_history_node(
    session: AsyncSession, execution_epoch: int, *, source_history: _TypedHistory | None = None,
) -> _TypedHistory:
    """Internal graph node; inherited history must already be authenticated.

    Only read-only graph traversal supplies a source node. Ordinary runtime
    consumers remain fenced until all purpose-aware mutation paths are connected.
    """
    if type(execution_epoch) is not int or execution_epoch <= 0:
        raise ConfigurationConflictError("typed membership execution epoch is invalid")
    epoch = await session.get(CapacityExecutionEpoch, execution_epoch, populate_existing=True)
    if epoch is None:
        raise ExecutionConflictError("typed membership execution is unavailable")
    try:
        preparation = ExecutionPreparationV4.model_validate_json(json.dumps(epoch.manifest_payload))
        if preparation.retired_source is not None:
            if source_history is None:
                raise ConfigurationConflictError("typed successor source graph authentication is not yet connected")
            from loom_capacity_manager.retired_source_graph import _authenticate_source_edge
            _authenticate_source_edge(preparation, execution_epoch, source_history)
        elif source_history is not None:
            raise ConfigurationConflictError("typed history has an unexpected inherited source")
        _require_values(epoch, {
            "execution_manifest_sha256": canonical_executable_digest(preparation),
            "authority_incarnation": preparation.authority_incarnation,
            "prepared_writer_epoch": preparation.expected_writer_epoch,
            "configuration_epoch": preparation.configuration_epoch,
            "fleet_generation": preparation.fleet_generation, "fleet_digest": preparation.fleet_digest,
            "trusted_fleet_release_sha256": preparation.trusted_fleet_release_sha256,
            "requested_ceiling": preparation.requested_ceiling,
            "requested_rate_per_minute": preparation.requested_rate_per_minute,
            "rollback_evidence_sha256": preparation.rollback_evidence_sha256,
            "environment_acknowledgements_sha256": _canonical_json_digest([item.model_dump(mode="json") for item in preparation.subject_acknowledgements]),
            "legacy_writer_manifest_sha256": _canonical_json_digest([item.model_dump(mode="json") for item in preparation.legacy_writer_fences]),
        }, label="execution manifest")
        for executor in preparation.executors:
            _require_values(epoch, {f"{executor.pool_id}_{field}": getattr(executor, field) for field in (
                "executor_id", "executor_incarnation", "pool_id", "pool_generation", "signing_key_sha256",
                "local_authority_sha256", "controller_authority_sha256",
            )}, label="execution executor")
        fleet_row = (await session.scalars(select(CapacityConfigGeneration).where(
            CapacityConfigGeneration.scope == "fleet", CapacityConfigGeneration.scope_generation == epoch.fleet_generation,
            CapacityConfigGeneration.digest == epoch.fleet_digest,
        ).execution_options(populate_existing=True))).one_or_none()
        if fleet_row is None:
            raise ConfigurationConflictError("typed membership fleet is unavailable")
        fleet = _parse_contract(FleetManifestV1, fleet_row.payload)
        if fleet.fleet_generation != epoch.fleet_generation or canonical_digest(fleet) != epoch.fleet_digest:
            raise ConfigurationConflictError("typed membership fleet changed")
        bases = await _load_base_configurations(session, epoch)
        for pinned_origin in preparation.managed_application_origins:
            base = bases.get(pinned_origin.configuration.subject_id)
            if base is None or canonical_bytes(base) != canonical_bytes(pinned_origin.configuration):
                raise ConfigurationConflictError("typed managed origin differs from immutable base generation")
            derived = _derive_development_subject(fleet, pinned_origin.base_projection)
            if canonical_bytes(derived) != canonical_bytes(base):
                raise ConfigurationConflictError("typed managed base differs from pinned fleet projection")
            # Source lineage was independently matched above. Installation reader
            # retains the original V1 installation contract, not inherited fields.
            await require_application_installation_evidence(session, ManagedApplicationOriginV1(
                configuration=pinned_origin.configuration, acknowledgement=pinned_origin.acknowledgement,
                base_projection=pinned_origin.base_projection, installation_projection=pinned_origin.installation_projection))
        for build_origin in preparation.managed_build_origins:
            from loom_capacity_manager.membership import _validate_build_configuration
            base = bases.get(build_origin.configuration.subject_id)
            member = build_origin.inherited.anchor.member
            if (base is None or not isinstance(member, PersonalBuildMemberV1)
                or canonical_bytes(base) != canonical_bytes(build_origin.configuration)):
                raise ConfigurationConflictError("typed managed build differs from immutable base generation")
            _validate_build_configuration(member, preparation.personal_membership.namespace_id,
                preparation.personal_builds, _derive_owner_account(fleet, member.owner_id))
            await _require_build_installation_facts(session, member, preparation)
        if source_history is not None and await session.scalar(select(CapacityPersonalMembershipEvent.id).where(
            CapacityPersonalMembershipEvent.execution_epoch == execution_epoch).limit(1)) is not None:
            raise ConfigurationConflictError("source-bearing typed event consumers are not yet connected")
        events = tuple((await session.scalars(select(CapacityPersonalMembershipEvent).where(
            CapacityPersonalMembershipEvent.execution_epoch == execution_epoch,
        ).order_by(CapacityPersonalMembershipEvent.revision).execution_options(populate_existing=True))).all())
        results = validate_typed_membership_event_prefix(events, preparation, fleet, execution_epoch=execution_epoch)
        latest: dict[UUID, PersonalMembershipResultV2] = {}
        latest_requests: dict[UUID, PersonalMembershipMutationV2] = {}
        reporters: dict[UUID, tuple[PersonalMembershipMutationV2, PersonalBuildMemberV1 | PersonalApplicationMemberV1]] = {}
        application_origins: dict[tuple[UUID, UUID, int], DynamicDevelopmentSubjectProjectionV1] = {
            (origin.configuration.subject_id, origin.configuration.subject_incarnation, origin.configuration.deployment_generation): origin.installation_projection
            for origin in preparation.managed_application_origins
        }
        reporter_bindings = {origin.configuration.demand_reporter_incarnation:
            (origin.configuration, origin.base_projection.demand_reporter_token_sha256)
            for origin in preparation.managed_application_origins}
        reporter_bindings.update({origin.configuration.demand_reporter_incarnation:
            (origin.configuration, origin.base_projection.demand_reporter_token_sha256)
            for origin in preparation.managed_build_origins})
        for event, result in zip(events, results, strict=True):
            original = parse_typed_membership_mutation(json.dumps(event.request_payload))
            evidence = result.member.reincarnation
            previous_result = latest.get(event.subject_id)
            if evidence is not None and (previous_result is None or previous_result.member.configuration.subject_incarnation != event.subject_incarnation):
                if evidence.release_set_sha256 != await predecessor_release_sha256(session, evidence.predecessor):
                    raise ConfigurationConflictError("typed recreation predecessor release evidence changed")
            if isinstance(original.command, PersonalApplicationCommandV2) and original.command.projection.operation_kind in {"create", "update"}:
                application_origins[(event.subject_id, event.subject_incarnation, event.deployment_generation)] = original.command.projection
            latest[event.subject_id] = result
            latest_requests[event.subject_id] = original
            reporters[event.reporter_incarnation] = (original, result.member)
            reporter_bindings[event.reporter_incarnation] = (result.member.configuration, original.command.projection.demand_reporter_token_sha256)
        for original, member in reporters.values():
            if isinstance(member, PersonalBuildMemberV1):
                await _require_build_installation_facts(session, member, preparation)
            else:
                origin = application_origins.get((member.configuration.subject_id, member.configuration.subject_incarnation, member.configuration.deployment_generation))
                if origin is None or not isinstance(original.command, PersonalApplicationCommandV2):
                    raise ConfigurationConflictError("typed application installation origin is unavailable")
                await require_application_installation_evidence(session, ManagedApplicationOriginV1(
                    configuration=member.configuration, acknowledgement=member.acknowledgement,
                    base_projection=original.command.projection, installation_projection=origin))
        return _TypedHistory(epoch, preparation, fleet, events, results, latest, latest_requests, reporter_bindings)
    except ValueError as exc:
        raise ConfigurationConflictError("typed membership historical evidence is invalid") from exc


async def _load_typed_history(session: AsyncSession, execution_epoch: int) -> _TypedHistory:
    """Resolve reporting only from the explicitly current activated authority.

    Used by allocation, mutation and current materialization, never by historical
    snapshot reads. Do not select a tip by largest retained/prepared epoch or
    recursively infer reporter state from an obsolete epoch's last event.
    """
    history = await _load_typed_immutable_history(session, execution_epoch)
    authority = await session.get(CapacityAuthorityState, 1, populate_existing=True)
    if authority is None or not isinstance(CapacityManagementStore._execution_context(authority, history.epoch), ExecutionAuthorityV2):
        raise ExecutionConflictError("typed reporter evidence requires current activated authority")
    tips = {origin.configuration.subject_id: origin.configuration for origin in history.preparation.managed_application_origins}
    tips.update({origin.configuration.subject_id: origin.configuration for origin in history.preparation.managed_build_origins})
    tips.update({identity: result.member.configuration for identity, result in history.latest.items()})
    try:
        for reporter_id, (subject, token) in history.reporter_bindings.items():
            row = (await session.scalars(select(CapacityDemandReporter).where(
                CapacityDemandReporter.subject_id == subject.subject_id,
                CapacityDemandReporter.subject_incarnation == subject.subject_incarnation,
                CapacityDemandReporter.reporter_incarnation == reporter_id,
            ).execution_options(populate_existing=True))).one_or_none()
            _require_values(row, {
                "configuration_generation": subject.configuration_generation,
                "deployment_generation": subject.deployment_generation, "token_sha256": token,
            }, label="typed reporter")
            # Equivocation fences one reporter's demand, not other owners'
            # history or retained accounting. Target admission still requires
            # current state; superseded reporters must remain fenced.
            states = {"current", "equivocal"} if tips[subject.subject_id].demand_reporter_incarnation == reporter_id else {"fenced"}
            if row is None or row.state not in states:
                raise ValueError("typed reporter state changed")
    except ValueError as exc:
        raise ConfigurationConflictError("typed current reporter evidence changed") from exc
    return history


async def _require_account(session: AsyncSession, epoch: int, account: AccountPolicyV1, *, optional: bool = False) -> None:
    row = (await session.scalars(select(CapacityAccountPolicy).where(
        CapacityAccountPolicy.configuration_epoch == epoch, CapacityAccountPolicy.account_id == account.account_id,
    ).execution_options(populate_existing=True))).one_or_none()
    if row is None and optional:
        return
    values = {name: getattr(account, name) for name in AccountPolicyV1.model_fields if name != "schema_version"}
    _require_values(row, values | {"payload": account.model_dump(mode="json"), "max_builds": 0, "max_artifact_bytes": 0}, label="owner account")


def _require_subject(row: CapacitySubject | None, expected: SubjectConfigurationV1) -> None:
    if row is None or not _subject_scalars_match(row, expected) or canonical_bytes(_parse_contract(SubjectConfigurationV1, row.payload)) != canonical_bytes(expected):
        raise ConfigurationConflictError("typed membership subject materialization changed")


async def _load_base_configurations(
    session: AsyncSession, epoch: CapacityExecutionEpoch,
) -> dict[UUID, SubjectConfigurationV1]:
    """Authenticate immutable roots without consulting current materialization."""
    configuration = await session.get(CapacityConfigurationEpoch, epoch.configuration_epoch, populate_existing=True)
    if configuration is None:
        raise ConfigurationConflictError("typed membership base configuration is missing")
    references = tuple(ConfigurationGenerationRefV1.model_validate_json(json.dumps(value)) for value in configuration.subject_generation_manifest)
    snapshot = ConfigurationSnapshotV1(configuration_epoch=epoch.configuration_epoch,
        fleet=ConfigurationGenerationRefV1(scope="fleet", generation=epoch.fleet_generation, digest=epoch.fleet_digest), subjects=references)
    if configuration.fleet_generation != epoch.fleet_generation or configuration.fleet_digest != epoch.fleet_digest or canonical_digest(snapshot) != configuration.canonical_digest:
        raise ConfigurationConflictError("typed membership base configuration changed")
    expected: dict[UUID, SubjectConfigurationV1] = {}
    for reference in references:
        generation = (await session.scalars(select(CapacityConfigGeneration).where(
            CapacityConfigGeneration.scope == "subject", CapacityConfigGeneration.subject_id == reference.subject_id,
            CapacityConfigGeneration.subject_incarnation == reference.subject_incarnation,
            CapacityConfigGeneration.scope_generation == reference.generation, CapacityConfigGeneration.digest == reference.digest,
        ).execution_options(populate_existing=True))).one_or_none()
        if generation is None:
            raise ConfigurationConflictError("typed membership base generation is missing")
        subject = _parse_contract(SubjectConfigurationV1, generation.payload)
        if subject.subject_id != reference.subject_id or subject.subject_incarnation != reference.subject_incarnation or subject.configuration_generation != reference.generation or canonical_digest(subject) != reference.digest:
            raise ConfigurationConflictError("typed membership base generation changed")
        expected[subject.subject_id] = subject
    return expected


async def _validated_materialization(
    session: AsyncSession, epoch: CapacityExecutionEpoch, fleet: FleetManifestV1,
    latest: dict[UUID, PersonalMembershipResultV2],
) -> tuple[list[CapacitySubject], tuple[AccountPolicyV1, ...]]:
    """Join immutable base references and the verified event overlay, not just JSON."""
    expected = await _load_base_configurations(session, epoch)
    preparation = ExecutionPreparationV4.model_validate_json(json.dumps(epoch.manifest_payload))
    origins: dict[UUID, ManagedApplicationOriginV1 | ManagedBuildOriginV1] = {
        origin.configuration.subject_id: origin for origin in preparation.managed_application_origins}
    origins.update({origin.configuration.subject_id: origin for origin in preparation.managed_build_origins})
    for identity in set(expected) & latest.keys():
        member = latest[identity].member
        base = origins.get(identity)
        if (
            base is None
            or isinstance(member, PersonalBuildMemberV1) != isinstance(base, ManagedBuildOriginV1)
            or member.owner_id != base.base_projection.owner_id
            or member.configuration.display_name != base.configuration.display_name
            or member.configuration.configuration_generation <= base.configuration.configuration_generation
            or (member.reincarnation is None and member.configuration.subject_incarnation != base.configuration.subject_incarnation)
            or (member.reincarnation is not None and member.reincarnation.origin != (
                base.inherited.original_origin if isinstance(base, (ManagedApplicationOriginV2, ManagedBuildOriginV1)) else ConfigurationGenerationRefV1(
                scope="subject", subject_id=base.configuration.subject_id,
                subject_incarnation=base.configuration.subject_incarnation,
                generation=base.configuration.configuration_generation, digest=canonical_digest(base.configuration))))
        ):
            raise ConfigurationConflictError("typed membership cannot replace the pinned managed base identity")
    expected.update({identity: result.member.configuration for identity, result in latest.items()})
    rows = list((await session.scalars(select(CapacitySubject).where(
        CapacitySubject.configuration_epoch == epoch.configuration_epoch,
    ).with_for_update().execution_options(populate_existing=True))).all())
    if len(rows) != len(expected) or len({row.subject_id for row in rows}) != len(rows):
        raise ConfigurationConflictError("typed membership materialized subject set changed")
    for row in rows:
        if row.subject_id not in expected:
            raise ConfigurationConflictError("typed membership contains an unregistered subject")
        _require_subject(row, expected[row.subject_id])
    accounts = {account.account_id: account for account in fleet.account_policies}
    derived: dict[str, AccountPolicyV1] = {}
    for account_id in {subject.account_id for subject in expected.values()} - accounts.keys():
        account_row = (await session.scalars(select(CapacityAccountPolicy).where(
            CapacityAccountPolicy.configuration_epoch == epoch.configuration_epoch, CapacityAccountPolicy.account_id == account_id,
        ).execution_options(populate_existing=True))).one_or_none()
        if account_row is None or account_row.kind != "owner" or account_row.owner_id is None:
            raise ConfigurationConflictError("typed membership owner account is unavailable")
        derived[account_id] = _derive_owner_account(fleet, account_row.owner_id)
        if derived[account_id].account_id != account_id:
            raise ConfigurationConflictError("typed membership owner account identity changed")
    for account in (*accounts.values(), *derived.values()):
        await _require_account(session, epoch.configuration_epoch, account)
    CapacityManagementStore._validate_activation(fleet, tuple(expected.values()), tuple(derived.values()))
    return rows, tuple(derived.values())


class CapacityTypedMembershipStore:
    """Append exact typed services under one SERIALIZABLE authority lock."""

    async def checkpoint(
        self, session: AsyncSession, *, actor: str, management: CapacityManagementStore,
    ) -> PersonalMembershipCheckpointV1:
        """Authenticate current typed delegation and operator policy, not readiness."""
        async with _write_transaction(session):
            authority = (await session.scalars(select(CapacityAuthorityState).where(
                CapacityAuthorityState.singleton_id == 1,
            ).with_for_update().execution_options(populate_existing=True))).one_or_none()
            if authority is None or authority.execution_state != "active":
                raise ExecutionConflictError("typed membership authority is unavailable")
            epoch = (await session.scalars(select(CapacityExecutionEpoch).where(
                CapacityExecutionEpoch.execution_epoch == authority.execution_epoch,
            ).with_for_update().execution_options(populate_existing=True))).one_or_none()
            if epoch is None or epoch.manifest_payload.get("schema_version") != 4:
                raise ExecutionConflictError("execution does not delegate typed membership")
            history = await _load_typed_history(session, epoch.execution_epoch)
            preparation = history.preparation
            if actor != preparation.personal_membership.management_principal_id:
                raise ExecutionConflictError("execution does not delegate typed membership")
            current = await management.execution_authority(session)
            if not isinstance(current, ExecutionAuthorityV2) or current.execution_state != "active":
                raise ExecutionConflictError("typed membership execution fence changed")
            await _validated_materialization(session, epoch, history.fleet, history.latest)
            snapshot = history.snapshot()
            return PersonalMembershipCheckpointV1(execution=current, namespace_id=snapshot.namespace_id,
                revision=snapshot.revision, head_sha256=snapshot.head_sha256)

    async def apply_authenticated(
        self, session: AsyncSession, request: PersonalMembershipMutationV2, *,
        actor: str, idempotency_key: UUID, management: CapacityManagementStore,
    ) -> PersonalMembershipResultV2:
        """Keep operator validation and mutation under the same authority lock.

        A replay may name an earlier revision of this same execution. Do not
        compare its revision to the current checkpoint before exact replay lookup.
        """
        async with _write_transaction(session):
            await self.checkpoint(session, actor=actor, management=management)
            return await self.apply(session, request, actor=actor, idempotency_key=idempotency_key)

    async def snapshot(
        self, session: AsyncSession, epoch: CapacityExecutionEpoch | int, *, through_revision: int | None = None,
    ) -> PersonalMembershipSnapshotV2:
        """Return historical evidence only; this never certifies current reporting."""
        history = await _load_typed_immutable_history(session, epoch.execution_epoch if isinstance(epoch, CapacityExecutionEpoch) else epoch)
        return history.snapshot(through_revision)

    async def verify_snapshot_materialization(
        self, session: AsyncSession, epoch: CapacityExecutionEpoch, snapshot: PersonalMembershipSnapshotV2,
    ) -> None:
        history = await _load_typed_history(session, epoch.execution_epoch)
        if canonical_bytes(snapshot) != canonical_bytes(history.snapshot()):
            raise ConfigurationConflictError("typed membership snapshot is not the current history")
        await _validated_materialization(session, history.epoch, history.fleet, history.latest)

    async def apply_build(
        self, session: AsyncSession, request: PersonalMembershipMutationV2, *, actor: str, idempotency_key: UUID,
    ) -> PersonalMembershipResultV2:
        if not isinstance(request.command, PersonalBuildCommandV2):
            raise ConfigurationConflictError("build membership requires a build command")
        return await self.apply(session, request, actor=actor, idempotency_key=idempotency_key)

    async def apply(
        self, session: AsyncSession, request: PersonalMembershipMutationV2, *, actor: str, idempotency_key: UUID,
    ) -> PersonalMembershipResultV2:
        try:
            request = parse_typed_membership_mutation(canonical_bytes(request))
            if not isinstance(idempotency_key, UUID) or idempotency_key.int == 0:
                raise ValueError("typed membership idempotency identity must be nonzero")
            async with _write_transaction(session):
                return await self._apply_locked(session, request, actor=actor, idempotency_key=idempotency_key)
        except ValueError as exc:
            raise ConfigurationConflictError(str(exc)) from exc

    async def _apply_locked(
        self, session: AsyncSession, request: PersonalMembershipMutationV2, *, actor: str, idempotency_key: UUID,
    ) -> PersonalMembershipResultV2:
        authority = (await session.scalars(select(CapacityAuthorityState).where(
            CapacityAuthorityState.singleton_id == 1,
        ).with_for_update().execution_options(populate_existing=True))).one_or_none()
        if authority is None:
            raise ExecutionConflictError("typed membership authority is unavailable")
        epoch = (await session.scalars(select(CapacityExecutionEpoch).where(
            CapacityExecutionEpoch.execution_epoch == authority.execution_epoch,
        ).with_for_update().execution_options(populate_existing=True))).one_or_none()
        if epoch is None or epoch.manifest_payload.get("schema_version") != 4:
            raise ExecutionConflictError("execution does not delegate typed membership")
        preparation = ExecutionPreparationV4.model_validate_json(json.dumps(epoch.manifest_payload))
        current = CapacityManagementStore._execution_context(authority, epoch)
        if (
            not isinstance(current, ExecutionAuthorityV2) or current.execution_state != "active"
            or current != request.execution
            or actor != preparation.personal_membership.management_principal_id
            or request.namespace_id != preparation.personal_membership.namespace_id
            or canonical_executable_digest(preparation) != epoch.execution_manifest_sha256
            or preparation.configuration_epoch != epoch.configuration_epoch
        ):
            raise ExecutionConflictError("typed membership execution fence changed")
        history = await _load_typed_history(session, epoch.execution_epoch)
        fleet = history.fleet
        member = (derive_build_member(request, preparation, fleet) if isinstance(request.command, PersonalBuildCommandV2)
            else derive_application_member(request, preparation, fleet))
        account = _derive_owner_account(fleet, member.owner_id)
        projection = request.command.projection
        digest = canonical_digest(request)
        replays = list((await session.scalars(select(CapacityPersonalMembershipEvent).where(or_(
            CapacityPersonalMembershipEvent.idempotency_key == idempotency_key,
            CapacityPersonalMembershipEvent.operation_id == projection.operation_id,
        )).with_for_update().execution_options(populate_existing=True))).all())
        if len(replays) > 1 or (replays and any((
            replays[0].execution_epoch != epoch.execution_epoch, replays[0].idempotency_key != idempotency_key,
            replays[0].operation_id != projection.operation_id, replays[0].actor != actor,
            replays[0].request_digest != digest, replays[0].request_payload != request.model_dump(mode="json"),
        ))):
            raise IdempotencyConflictError("typed membership replay identity changed")
        events, results, latest, latest_requests = history.events, history.results, history.latest, history.latest_requests
        rows, derived_accounts = await _validated_materialization(session, epoch, fleet, latest)
        if replays:
            replay = next((result for event, result in zip(events, results, strict=True) if event.id == replays[0].id), None)
            if replay is None:
                raise IdempotencyConflictError("typed membership replay is outside current history")
            return replay.model_copy(update={"replayed": True})
        revision = events[-1].revision if events else 0
        if request.expected_revision != revision:
            raise PersonalMembershipRevisionConflictError("typed membership revision is stale")
        subject = member.configuration
        previous_result = latest.get(subject.subject_id)
        previous = previous_result.member if previous_result is not None else None
        if previous is not None and previous.purpose != member.purpose:
            raise ConfigurationConflictError("typed membership cannot change subject purpose")
        evidence = None if previous is None else previous.reincarnation
        if previous is not None and projection.operation_kind == "create":
            old = previous.configuration
            if old.lifecycle_state != "disabled" or old.min_slots != 0 or old.max_slots != 0:
                raise ConfigurationConflictError("typed recreation requires a disabled predecessor")
            for model in (CapacityPersonalMembershipEvent, CapacityConfigGeneration, CapacitySubject, CapacityCandidate, CapacityDemandReporter):
                if await session.scalar(select(model.id).where(
                    model.subject_incarnation == subject.subject_incarnation).limit(1)) is not None:
                    raise ConfigurationConflictError("typed recreation incarnation was already used")
            previous_event = next(event for event in reversed(events) if event.subject_id == subject.subject_id)
            base_configuration = next((origin.configuration for origin in preparation.managed_application_origins if origin.configuration.subject_id == subject.subject_id), None)
            origin_configuration = base_configuration or next(result.member.configuration for result in results if result.member.configuration.subject_id == subject.subject_id)
            evidence = PersonalReincarnationEvidenceV1(namespace_id=request.namespace_id,
                execution_manifest_sha256=epoch.execution_manifest_sha256,
                origin=previous.reincarnation.origin if previous.reincarnation is not None else ConfigurationGenerationRefV1(
                    scope="subject", subject_id=origin_configuration.subject_id,
                    subject_incarnation=origin_configuration.subject_incarnation,
                    generation=origin_configuration.configuration_generation, digest=canonical_digest(origin_configuration)),
                predecessor=old, predecessor_revision=previous_event.revision, predecessor_head_sha256=previous_event.head_sha256,
                admission_revision=revision + 1, successor_incarnation=subject.subject_incarnation,
                release_set_sha256=await predecessor_release_sha256(session, old))
        if evidence is not None:
            member = (derive_build_member(request, preparation, fleet, reincarnation=evidence)
                if isinstance(request.command, PersonalBuildCommandV2) else derive_application_member(request, preparation, fleet, reincarnation=evidence))
        if previous is None:
            base = next((origin for origin in preparation.managed_application_origins
                if origin.configuration.subject_id == subject.subject_id), None)
            conflicts = (await session.scalars(select(CapacitySubject).where(or_(
                CapacitySubject.subject_id == subject.subject_id,
                CapacitySubject.subject_incarnation == subject.subject_incarnation,
                CapacitySubject.display_name == subject.display_name,
            )).execution_options(populate_existing=True))).all()
            for conflict in conflicts:
                if (
                    base is None or not isinstance(member, PersonalApplicationMemberV1)
                    or conflict.subject_id != base.configuration.subject_id
                    or conflict.subject_incarnation != base.configuration.subject_incarnation
                    or conflict.display_name != base.configuration.display_name
                    or conflict.account_id != base.configuration.account_id
                ):
                    raise ConfigurationConflictError("typed membership identity was already used")
        if len(set(preparation.personal_membership.managed_base_subject_ids) | set(latest) | {subject.subject_id}) > preparation.personal_membership.max_subjects:
            raise ConfigurationConflictError("typed membership exceeds its subject bound")
        next_subjects = {row.subject_id: _parse_contract(SubjectConfigurationV1, row.payload) for row in rows} | {subject.subject_id: subject}
        if sum(item.account_id == account.account_id and item.lifecycle_state != "disabled" for item in next_subjects.values()) > account.max_live_subjects:
            raise ConfigurationConflictError("typed membership owner exceeds max_live_subjects")
        await _require_account(session, epoch.configuration_epoch, account, optional=True)
        derived = {item.account_id: item for item in derived_accounts} | {account.account_id: account}
        CapacityManagementStore._validate_activation(fleet, tuple(next_subjects.values()), tuple(derived.values()))
        head = canonical_membership_event_head(actor=actor, execution_epoch=epoch.execution_epoch,
            idempotency_key=idempotency_key, operation_id=projection.operation_id,
            previous_sha256=events[-1].head_sha256 if events else "0" * 64, request_digest=digest,
            request_payload=request.model_dump(mode="json"), member=member, revision=member.revision)
        result = PersonalMembershipResultV2(revision=member.revision, head_sha256=head, member=member, replayed=False)
        event = CapacityPersonalMembershipEvent(execution_epoch=epoch.execution_epoch,
            execution_manifest_sha256=current.execution_manifest_sha256, authority_incarnation=current.authority_incarnation,
            writer_epoch=current.writer_epoch, namespace_id=request.namespace_id, revision=member.revision,
            previous_sha256=events[-1].head_sha256 if events else "0" * 64, head_sha256=head, actor=actor,
            idempotency_key=idempotency_key, operation_id=projection.operation_id, request_digest=digest,
            request_payload=request.model_dump(mode="json"), result_payload=result.model_dump(mode="json"),
            subject_id=subject.subject_id, subject_incarnation=subject.subject_incarnation, owner_id=member.owner_id,
            configuration_generation=subject.configuration_generation, deployment_generation=subject.deployment_generation,
            reporter_incarnation=subject.demand_reporter_incarnation)
        validate_typed_membership_event_prefix((*events, event), preparation, fleet, execution_epoch=epoch.execution_epoch)
        if isinstance(member, PersonalBuildMemberV1):
            assert previous is None or isinstance(previous, PersonalBuildMemberV1)
            await stage_build_generation_evidence(session, request, member, preparation, fleet,
                previous=previous, previous_request=latest_requests.get(subject.subject_id))
        else:
            assert isinstance(request.command, PersonalApplicationCommandV2)
            assert previous is None or isinstance(previous, PersonalApplicationMemberV1)
            application_store = CapacityMembershipStore(CapacityManagementStore())
            if projection.operation_kind in {"create", "update"}:
                await application_store._require_unused_reporter(session, request.command.projection)
            await application_store._persist_generation_evidence(session, request.command.projection, subject, previous)
        await CapacityMembershipStore(CapacityManagementStore())._materialize_subject(session, epoch.configuration_epoch, subject, account, rows)
        await session.flush()
        session.add(event)
        await session.flush()
        return result
