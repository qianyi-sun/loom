"""Transactional typed build membership; not executable admission.

The caller supplies an already authenticated management principal. This store
checks its pinned delegation against current durable authority, never a caller
preparation/fleet. Fresh application and pending-build lifecycle are supported;
managed-base adoption, recreation and executable V4 admission remain closed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.application_generation_store import (
    require_application_generation_evidence,
)
from loom_capacity_manager.build_generation_store import (
    _require_staged_facts,
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
    FleetManifestV1,
    SubjectConfigurationV1,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionAuthorityV2,
    canonical_executable_digest,
)
from loom_capacity_manager.membership_contracts import PersonalApplicationMemberV1
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.membership_store import (
    CapacityMembershipStore,
    PersonalMembershipRevisionConflictError,
)
from loom_capacity_manager.models import (
    CapacityAccountPolicy,
    CapacityAuthorityState,
    CapacityConfigGeneration,
    CapacityConfigurationEpoch,
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
    _derive_owner_account,
    _parse_contract,
    _subject_scalars_match,
    _write_transaction,
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


async def _load_typed_history(session: AsyncSession, execution_epoch: int) -> _TypedHistory:
    """Authenticate retained evidence, not current authority or permission to launch.

    Read the whole log even for a historical prefix: reporter rotation is proved
    by its successor, and the retained reporter represents its last generation.
    Callers requiring currentness retain authority-first transactional fencing.
    """
    if type(execution_epoch) is not int or execution_epoch <= 0:
        raise ConfigurationConflictError("typed membership execution epoch is invalid")
    epoch = await session.get(CapacityExecutionEpoch, execution_epoch, populate_existing=True)
    if epoch is None:
        raise ExecutionConflictError("typed membership execution is unavailable")
    try:
        preparation = ExecutionPreparationV4.model_validate_json(json.dumps(epoch.manifest_payload))
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
        events = tuple((await session.scalars(select(CapacityPersonalMembershipEvent).where(
            CapacityPersonalMembershipEvent.execution_epoch == execution_epoch,
        ).order_by(CapacityPersonalMembershipEvent.revision).execution_options(populate_existing=True))).all())
        results = validate_typed_membership_event_prefix(events, preparation, fleet, execution_epoch=execution_epoch)
        latest: dict[UUID, PersonalMembershipResultV2] = {}
        latest_requests: dict[UUID, PersonalMembershipMutationV2] = {}
        reporters: dict[UUID, tuple[PersonalMembershipMutationV2, PersonalBuildMemberV1 | PersonalApplicationMemberV1]] = {}
        application_origins: dict[tuple[UUID, int], PersonalMembershipMutationV2] = {}
        for event, result in zip(events, results, strict=True):
            original = parse_typed_membership_mutation(json.dumps(event.request_payload))
            if result.member.reincarnation is not None:
                raise ConfigurationConflictError("typed membership history lifecycle is not yet admitted")
            if isinstance(original.command, PersonalApplicationCommandV2) and original.command.projection.operation_kind in {"create", "update"}:
                application_origins[(event.subject_id, event.deployment_generation)] = original
            latest[event.subject_id] = result
            latest_requests[event.subject_id] = original
            reporters[event.reporter_incarnation] = (original, result.member)
        for reporter_id, (original, member) in reporters.items():
            current_reporter = latest[member.configuration.subject_id].member.configuration.demand_reporter_incarnation
            if isinstance(member, PersonalBuildMemberV1):
                await _require_staged_facts(session, original, member, preparation,
                    reporter_state="current" if reporter_id == current_reporter else "fenced")
            else:
                origin = application_origins.get((member.configuration.subject_id, member.configuration.deployment_generation))
                if origin is None or not isinstance(origin.command, PersonalApplicationCommandV2) or not isinstance(original.command, PersonalApplicationCommandV2):
                    raise ConfigurationConflictError("typed application installation origin is unavailable")
                await require_application_generation_evidence(session, member, original.command.projection, origin.command.projection,
                    reporter_state="current" if reporter_id == current_reporter else "fenced")
        return _TypedHistory(epoch, preparation, fleet, events, results, latest, latest_requests)
    except ValueError as exc:
        raise ConfigurationConflictError("typed membership historical evidence is invalid") from exc


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


async def _validated_materialization(
    session: AsyncSession, epoch: CapacityExecutionEpoch, fleet: FleetManifestV1,
    latest: dict[UUID, PersonalMembershipResultV2],
) -> tuple[list[CapacitySubject], tuple[AccountPolicyV1, ...]]:
    """Join immutable base references and the verified event overlay, not just JSON."""
    configuration = await session.get(CapacityConfigurationEpoch, epoch.configuration_epoch, populate_existing=True)
    if configuration is None:
        raise ConfigurationConflictError("typed membership base configuration is missing")
    references = tuple(ConfigurationGenerationRefV1.model_validate_json(json.dumps(value)) for value in configuration.subject_generation_manifest)
    snapshot = ConfigurationSnapshotV1(configuration_epoch=epoch.configuration_epoch,
        fleet=ConfigurationGenerationRefV1(scope="fleet", generation=epoch.fleet_generation, digest=epoch.fleet_digest), subjects=references)
    if configuration.fleet_generation != epoch.fleet_generation or configuration.fleet_digest != epoch.fleet_digest or canonical_digest(snapshot) != configuration.canonical_digest:
        raise ConfigurationConflictError("typed membership base configuration changed")
    expected = {identity: result.member.configuration for identity, result in latest.items()}
    for reference in references:
        if reference.subject_id in expected:
            raise ConfigurationConflictError("build membership cannot adopt a base subject")
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

    async def snapshot(
        self, session: AsyncSession, epoch: CapacityExecutionEpoch | int, *, through_revision: int | None = None,
    ) -> PersonalMembershipSnapshotV2:
        history = await _load_typed_history(session, epoch.execution_epoch if isinstance(epoch, CapacityExecutionEpoch) else epoch)
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
        if previous is not None and projection.operation_kind == "create":
            raise ConfigurationConflictError("typed membership recreation requires authenticated release and is not yet admitted")
        if previous is None:
            conflict = (await session.scalars(select(CapacitySubject.id).where(or_(
                CapacitySubject.subject_id == subject.subject_id,
                CapacitySubject.subject_incarnation == subject.subject_incarnation,
                CapacitySubject.display_name == subject.display_name,
            )).limit(1))).first()
            if conflict is not None:
                raise ConfigurationConflictError("typed build identity was already used")
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
