"""Read-only typed event checks for the successor membership persistence path.

These verify stored values, build lifecycle structure and prefix hashing, not
durable insertion, release authenticity or current admission. They deliberately do not
widen legacy history parsers. The owning store must obtain the preparation/fleet
from authenticated durable authority and verify lifecycle, retained generations,
predecessor release and materialization before consuming the resulting members.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from uuid import UUID

from loom_capacity_manager.application_origin_contracts import ManagedApplicationOriginV1
from loom_capacity_manager.build_membership_contracts import (
    ExecutionPreparationV4,
    PersonalBuildMemberV1,
)
from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    ConfigurationGenerationRefV1,
    FleetManifestV1,
    SubjectConfigurationV1,
    canonical_digest,
)
from loom_capacity_manager.membership_contracts import PersonalApplicationMemberV1
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.models import CapacityPersonalMembershipEvent
from loom_capacity_manager.typed_membership_commands import (
    PersonalApplicationCommandV2,
    PersonalBuildCommandV2,
    PersonalMembershipMutationV2,
    PersonalMembershipResultV2,
    parse_typed_membership_mutation,
    parse_typed_membership_result,
    validate_typed_membership_result,
)


def _payload(value: dict[str, object]) -> bytes:
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    except (TypeError, RecursionError) as exc:
        raise ValueError("invalid typed event payload") from exc
    if len(encoded) > MAX_CONTRACT_BYTES:
        raise ValueError("typed event payload exceeds byte bound")
    return encoded


def validate_typed_membership_event(
    row: CapacityPersonalMembershipEvent, preparation: ExecutionPreparationV4, fleet: FleetManifestV1,
) -> tuple[PersonalMembershipMutationV2, PersonalMembershipResultV2]:
    """Check the complete original request/result against each indexed row value."""
    request = parse_typed_membership_mutation(_payload(row.request_payload))
    result = parse_typed_membership_result(_payload(row.result_payload))
    validate_typed_membership_result(request, result, preparation, fleet)
    projection, member, execution = request.command.projection, result.member, request.execution
    configuration = member.configuration
    if (
        result.replayed
        or not isinstance(row.idempotency_key, UUID) or row.idempotency_key.int == 0
        or row.actor != preparation.personal_membership.management_principal_id
        or row.execution_epoch != execution.execution_epoch
        or row.execution_manifest_sha256 != execution.execution_manifest_sha256
        or row.authority_incarnation != execution.authority_incarnation
        or row.writer_epoch != execution.writer_epoch
        or row.namespace_id != request.namespace_id
        or row.revision != result.revision
        or row.head_sha256 != result.head_sha256
        or row.operation_id != projection.operation_id
        or row.request_digest != canonical_digest(request)
        or row.subject_id != configuration.subject_id
        or row.subject_incarnation != configuration.subject_incarnation
        or row.owner_id != member.owner_id
        or row.configuration_generation != configuration.configuration_generation
        or row.deployment_generation != configuration.deployment_generation
        or row.reporter_incarnation != configuration.demand_reporter_incarnation
        or row.head_sha256 != canonical_membership_event_head(
            actor=row.actor, execution_epoch=row.execution_epoch, idempotency_key=row.idempotency_key,
            operation_id=row.operation_id, previous_sha256=row.previous_sha256, request_digest=row.request_digest,
            request_payload=row.request_payload, member=member, revision=row.revision,
        )
    ):
        raise ValueError("typed membership event binding changed")
    return request, result


def _build_transition(
    request: PersonalMembershipMutationV2, member: PersonalBuildMemberV1,
    prior: tuple[PersonalMembershipMutationV2, PersonalMembershipResultV2, CapacityPersonalMembershipEvent] | None,
    origin: SubjectConfigurationV1, used_incarnations: set[UUID], reporters: set[UUID], tokens: set[str],
) -> None:
    assert isinstance(request.command, PersonalBuildCommandV2)
    projection, subject = request.command.projection, member.configuration
    fresh_reporter = subject.demand_reporter_incarnation not in reporters and projection.demand_reporter_token_sha256 not in tokens
    if prior is None:
        if (
            projection.operation_kind != "create" or subject.subject_incarnation in used_incarnations
            or subject.candidate_generation != 1 or subject.deployment_generation != 1
            or member.reincarnation is not None or not fresh_reporter
        ):
            raise ValueError("initial build membership requires a fresh service identity")
        return
    old_request, old_result, old_row = prior
    old_member, old = old_result.member, old_result.member.configuration
    if (
        not isinstance(old_member, PersonalBuildMemberV1)
        or member.owner_id != old_member.owner_id or subject.display_name != old.display_name
        or subject.configuration_generation <= old.configuration_generation
    ):
        raise ValueError("build membership historical identity changed")
    recreating = old.lifecycle_state == "disabled" and projection.operation_kind == "create"
    if recreating:
        evidence = member.reincarnation
        origin_reference = ConfigurationGenerationRefV1(
            scope="subject", subject_id=origin.subject_id, subject_incarnation=origin.subject_incarnation,
            generation=origin.configuration_generation, digest=canonical_digest(origin),
        )
        if (
            subject.subject_incarnation in used_incarnations or not fresh_reporter
            or subject.candidate_generation != 1 or subject.deployment_generation != 1
            or evidence is None or evidence.origin != origin_reference
            or evidence.predecessor != old or evidence.predecessor_revision != old_row.revision
            or evidence.predecessor_head_sha256 != old_row.head_sha256
            or evidence.admission_revision != member.revision
        ):
            raise ValueError("build membership predecessor event changed")
        return  # The durable caller must still recompute and authenticate release_set_sha256.
    if (
        old.lifecycle_state == "disabled" or projection.operation_kind == "create"
        or subject.subject_incarnation != old.subject_incarnation
        or member.reincarnation != old_member.reincarnation
    ):
        raise ValueError("build membership requires a released fresh reincarnation")
    if projection.operation_kind == "update":
        if (
            subject.deployment_generation <= old.deployment_generation
            or subject.candidate_generation < old.candidate_generation or not fresh_reporter
        ):
            raise ValueError("build deployment must advance and rotate its reporter")
    elif (
        subject.deployment_generation != old.deployment_generation
        or subject.candidate_generation != old.candidate_generation
        or subject.demand_reporter_incarnation != old.demand_reporter_incarnation
        or projection.demand_reporter_token_sha256 != old_request.command.projection.demand_reporter_token_sha256
        or member.acknowledgement != old_member.acknowledgement.model_copy(update={
            "configuration_generation": subject.configuration_generation,
            "acknowledgement_sha256": member.acknowledgement.acknowledgement_sha256,
        })
    ):
        raise ValueError("non-deployment build membership must retain service evidence")


def _application_transition(
    request: PersonalMembershipMutationV2, member: PersonalApplicationMemberV1,
    prior: tuple[PersonalMembershipMutationV2, PersonalMembershipResultV2, CapacityPersonalMembershipEvent] | None,
    origin: SubjectConfigurationV1, used_incarnations: set[UUID], reporters: set[UUID], tokens: set[str],
    base: ManagedApplicationOriginV1 | None = None,
) -> None:
    """Authenticate lifecycle from a real prior event or pinned managed origin."""
    assert isinstance(request.command, PersonalApplicationCommandV2)
    projection, subject = request.command.projection, member.configuration
    fresh_reporter = subject.demand_reporter_incarnation not in reporters and projection.demand_reporter_token_sha256 not in tokens
    if prior is None and base is None:
        if (
            projection.operation_kind != "create" or subject.subject_incarnation in used_incarnations
            or subject.candidate_generation != 1 or subject.deployment_generation != 1 or not fresh_reporter
            or member.reincarnation is not None
        ):
            raise ValueError("initial application membership requires a fresh service identity")
        return
    if prior is None:
        assert base is not None
        old, old_projection, old_ack = base.configuration, base.base_projection, base.acknowledgement
        old_owner = base.base_projection.owner_id
    else:
        old_request, old_result, _old_row = prior
        if not isinstance(old_result.member, PersonalApplicationMemberV1) or not isinstance(old_request.command, PersonalApplicationCommandV2):
            raise ValueError("application membership historical purpose changed")
        old, old_projection, old_ack = old_result.member.configuration, old_request.command.projection, old_result.member.acknowledgement
        old_owner = old_result.member.owner_id
    if (
        member.owner_id != old_owner or subject.display_name != old.display_name
        or subject.configuration_generation <= old.configuration_generation
    ):
        raise ValueError("application membership historical identity or lifecycle changed")
    if old.lifecycle_state == "disabled" and projection.operation_kind == "create":
        evidence = member.reincarnation
        reference = ConfigurationGenerationRefV1(scope="subject", subject_id=origin.subject_id,
            subject_incarnation=origin.subject_incarnation, generation=origin.configuration_generation,
            digest=canonical_digest(origin))
        if (
            prior is None or evidence is None or evidence.origin != reference
            or subject.subject_incarnation in used_incarnations or not fresh_reporter
            or subject.candidate_generation != 1 or subject.deployment_generation != 1
            or evidence.predecessor != old or evidence.predecessor_revision != prior[2].revision
            or evidence.predecessor_head_sha256 != prior[2].head_sha256
            or evidence.admission_revision != member.revision
        ):
            raise ValueError("application membership predecessor event changed")
        return  # Durable admission must separately authenticate the release ledger.
    if (
        old.lifecycle_state == "disabled" or projection.operation_kind == "create"
        or subject.subject_incarnation != old.subject_incarnation
        or member.reincarnation != (None if prior is None else prior[1].member.reincarnation)
    ):
        raise ValueError("application membership requires a released fresh reincarnation")
    if projection.operation_kind == "update":
        if (
            subject.deployment_generation <= old.deployment_generation
            or subject.candidate_generation <= old.candidate_generation or not fresh_reporter
        ):
            raise ValueError("application deployment must advance and rotate its reporter")
        return
    # Capacity and teardown may only change lifecycle coordinates and limits.
    # In particular, retain all source, installation, protocol and token facts.
    mutable = {"expected_configuration_epoch", "operation_kind", "operation_id", "operation_epoch", "configuration_generation", "min_slots", "max_slots"}
    if (
        _payload(projection.model_dump(mode="json", exclude=mutable))
        != _payload(old_projection.model_dump(mode="json", exclude=mutable))
        or member.acknowledgement != old_ack.model_copy(update={
            "configuration_generation": subject.configuration_generation,
            "acknowledgement_sha256": member.acknowledgement.acknowledgement_sha256,
        })
    ):
        raise ValueError("non-deployment application membership must retain service evidence")


def validate_typed_membership_event_prefix(
    rows: Sequence[CapacityPersonalMembershipEvent], preparation: ExecutionPreparationV4, fleet: FleetManifestV1,
    *, execution_epoch: int,
) -> tuple[PersonalMembershipResultV2, ...]:
    """Validate mixed lifecycle against pinned origins, not durable release facts.

Operation and idempotency IDs share one domain across purposes. The durable
unique indexes also enforce their uniqueness across other execution epochs.
    The durable caller authenticates origin installations and immutable base
    generations before admitting adoption. Actual release-set verification stays
    a store responsibility. This cannot establish that a prefix is the latest.
"""
    if type(execution_epoch) is not int or execution_epoch <= 0:
        raise ValueError("typed membership execution epoch must be positive")
    preparation = ExecutionPreparationV4.model_validate_json(preparation.model_dump_json())
    bases = {origin.configuration.subject_id: origin for origin in preparation.managed_application_origins}
    previous = "0" * 64
    operations = {projection.operation_id for origin in bases.values()
        for projection in (origin.installation_projection, origin.base_projection)}
    keys: set[UUID] = set()
    prior_members: dict[UUID, tuple[PersonalMembershipMutationV2, PersonalMembershipResultV2, CapacityPersonalMembershipEvent]] = {}
    names = {origin.configuration.display_name: origin.configuration.subject_id for origin in bases.values()}
    origins: dict[UUID, SubjectConfigurationV1] = {identity: base.configuration for identity, base in bases.items()}
    used_incarnations = {item.subject_incarnation for item in preparation.subject_acknowledgements}
    base_ids = set(preparation.personal_membership.managed_base_subject_ids) | {item.subject_id for item in preparation.subject_acknowledgements}
    reporters = {item.reporter_incarnation for item in preparation.subject_acknowledgements}
    tokens = {origin.base_projection.demand_reporter_token_sha256 for origin in bases.values()}
    results: list[PersonalMembershipResultV2] = []
    for revision, row in enumerate(rows, start=1):
        request, result = validate_typed_membership_event(row, preparation, fleet)
        if (
            row.execution_epoch != execution_epoch or row.revision != revision
            or row.previous_sha256 != previous or row.operation_id in operations
            or row.idempotency_key in keys
        ):
            raise ValueError("typed membership prefix or replay identity changed")
        member, subject = result.member, result.member.configuration
        if names.setdefault(subject.display_name, subject.subject_id) != subject.subject_id:
            raise ValueError("typed membership name was already retained by another subject")
        prior = prior_members.get(subject.subject_id)
        if prior is not None and member.purpose != prior[1].member.purpose:
            raise ValueError("typed membership subject purpose changed")
        origin = origins.setdefault(subject.subject_id, subject)
        if isinstance(member, PersonalBuildMemberV1):
            if subject.subject_id in base_ids:
                raise ValueError("build membership cannot replace an immutable base subject")
            _build_transition(request, member, prior, origin, used_incarnations, reporters, tokens)
        else:
            if subject.subject_id in base_ids and subject.subject_id not in bases:
                raise ValueError("application base adoption requires authenticated original provenance")
            _application_transition(request, member, prior, origin, used_incarnations, reporters, tokens, bases.get(subject.subject_id))
        prior_members[subject.subject_id] = (request, result, row)
        used_incarnations.add(subject.subject_incarnation)
        reporters.add(subject.demand_reporter_incarnation)
        tokens.add(request.command.projection.demand_reporter_token_sha256)
        previous = row.head_sha256
        operations.add(row.operation_id)
        keys.add(row.idempotency_key)
        results.append(result)
    return tuple(results)
