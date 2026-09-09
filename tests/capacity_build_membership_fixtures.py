"""Guarded SQL fixtures for typed membership; not V4 runtime readiness evidence.

The production preparation interlock remains closed. These fixtures seed an exact
prepared epoch and exercise its installed SQL transition guards, allowing the new
membership database guard to be tested before the executable consumers are opened.
"""

from dataclasses import replace
from uuid import UUID

from sqlalchemy import func, select

from loom_capacity_manager.build_membership_contracts import (
    ExecutionPreparationV4,
    PersonalBuildTemplateV1,
    personal_build_subject_id,
)
from loom_capacity_manager.contracts import canonical_digest, canonical_digest_excluding
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    SubjectExecutionAcknowledgementV2,
    canonical_executable_digest,
)
from loom_capacity_manager.membership_contracts import PersonalMembershipPolicyV1
from loom_capacity_manager.models import CapacityAuthorityState, CapacityExecutionEpoch
from loom_capacity_manager.store import _canonical_json_digest
from loom_capacity_manager.typed_membership_commands import (
    PersonalBuildCommandV2,
    PersonalBuildProjectionV1,
    PersonalMembershipMutationV2,
)
from tests.capacity_execution_fixtures import (
    execution_policy,
    register_execution_executors,
    setup_execution,
)
from tests.capacity_fixtures import fleet_with_development_template


async def typed_sql_execution(session, *, max_subjects=8):
    fleet = fleet_with_development_template()
    fixture = await setup_execution(session, execution_policy=execution_policy(), fleet=fleet)
    profiles = []
    for profile in fleet.development_subject_template.profiles:
        architecture = "arm64" if profile.pool_id == "gb10" else "x86_64"
        shape = profile.worker_shapes[0].model_copy(update={
            "shape_id": f"personal-build-{profile.pool_id}", "warm_approved": False,
            "capabilities": (f"cpu_arch.{architecture}", "personal-build-worker"),
        })
        profile = profile.model_copy(update={"worker_shapes": (shape,)})
        profiles.append(profile.model_copy(update={"profile_digest": canonical_digest_excluding(profile, "profile_digest")}))
    template = PersonalBuildTemplateV1(
        runtime_candidate=CandidateBindingV2(algorithm="git-sha1", identity="a" * 40, publication_sha256="b" * 64),
        profiles=tuple(profiles), max_slots_per_subject=2, max_pending_slots_per_subject=2, max_pending_jobs_per_subject=2,
    )
    preparation = ExecutionPreparationV4.model_validate(fixture.request.model_dump(mode="python") | {
        "schema_version": 4, "personal_builds": template,
        "personal_membership": PersonalMembershipPolicyV1(namespace_id=UUID(int=88001),
            management_principal_id="build-management", development_template_sha256=canonical_digest(fleet.development_subject_template), max_subjects=max_subjects),
    })
    digest = canonical_executable_digest(preparation)
    values = dict(execution_epoch=42, authority_incarnation=preparation.authority_incarnation,
        prepared_writer_epoch=preparation.expected_writer_epoch, current_writer_epoch=preparation.expected_writer_epoch,
        configuration_epoch=preparation.configuration_epoch, fleet_generation=preparation.fleet_generation,
        fleet_digest=preparation.fleet_digest, execution_manifest_sha256=digest,
        manifest_payload=preparation.model_dump(mode="json"), trusted_fleet_release_sha256=preparation.trusted_fleet_release_sha256,
        environment_acknowledgements_sha256=_canonical_json_digest([ack.model_dump(mode="json") for ack in preparation.subject_acknowledgements]),
        legacy_writer_manifest_sha256=_canonical_json_digest([fence.model_dump(mode="json") for fence in preparation.legacy_writer_fences]),
        rollback_evidence_sha256=preparation.rollback_evidence_sha256, requested_ceiling=preparation.requested_ceiling,
        requested_rate_per_minute=preparation.requested_rate_per_minute, effective_ceiling=0, effective_rate_per_minute=0,
        state="prepared", actor="sql-test-preparation", idempotency_key=UUID(int=88002), request_digest=digest)
    for executor in preparation.executors:
        for field in ("executor_id", "executor_incarnation", "pool_id", "pool_generation", "signing_key_sha256", "local_authority_sha256", "controller_authority_sha256"):
            values[f"{executor.pool_id}_{field}"] = getattr(executor, field)
    row = CapacityExecutionEpoch(**values)
    session.add(row)
    await session.flush()
    authority = (await session.scalars(select(CapacityAuthorityState).with_for_update())).one()
    authority.execution_epoch = 42
    authority.execution_state = "prepared"
    authority.execution_manifest_sha256 = digest
    authority.executable_new_capacity_ceiling = 0
    await session.flush()
    prepared = fixture.store._execution_context(authority, row)
    await register_execution_executors(session, replace(fixture, request=preparation), prepared)
    row.state = "active"
    row.effective_ceiling = preparation.requested_ceiling
    row.effective_rate_per_minute = preparation.requested_rate_per_minute
    row.activation_actor = "sql-test-activation"
    row.activation_idempotency_key = UUID(int=88003)
    row.activation_request_digest = "c" * 64
    row.activated_at = await session.scalar(select(func.now()))
    await session.flush()
    authority.execution_state = "active"
    authority.executable_new_capacity_ceiling = preparation.requested_ceiling
    await session.flush()
    return fixture.store, preparation, fleet, fixture.store._execution_context(authority, row)


def build_request(preparation, execution, *, owner=88010, revision=0):
    owner_id = UUID(int=owner)
    subject_id = personal_build_subject_id(preparation.personal_membership.namespace_id, owner_id)
    projection = PersonalBuildProjectionV1(owner_id=owner_id, subject_incarnation=UUID(int=owner + 1000),
        operation_kind="create", operation_id=UUID(int=owner + 2000), operation_epoch=1,
        configuration_generation=1, candidate_generation=1, deployment_generation=1,
        demand_reporter_incarnation=UUID(int=owner + 3000), demand_reporter_token_sha256=f"{owner:064x}", max_slots=2)
    acknowledgement = SubjectExecutionAcknowledgementV2(subject_id=subject_id,
        subject_incarnation=projection.subject_incarnation, configuration_generation=1, deployment_generation=1,
        candidate=preparation.personal_builds.runtime_candidate, reporter_incarnation=projection.demand_reporter_incarnation,
        protected_admission_sha256="d" * 64, legacy_writer_high_water=0, acknowledgement_sha256="e" * 64)
    return PersonalMembershipMutationV2(execution=execution, namespace_id=preparation.personal_membership.namespace_id,
        expected_revision=revision, command=PersonalBuildCommandV2(projection=projection, acknowledgement=acknowledgement))


async def staged_build_event(session, management, preparation, fleet, request, *, previous_head="0" * 64, previous=None, previous_request=None, idempotency_key=None):
    from loom_capacity_manager.build_generation_store import stage_build_generation_evidence
    from loom_capacity_manager.membership_digest import canonical_membership_event_head
    from loom_capacity_manager.membership_store import CapacityMembershipStore
    from loom_capacity_manager.models import CapacityPersonalMembershipEvent, CapacitySubject
    from loom_capacity_manager.store import _derive_owner_account
    from loom_capacity_manager.typed_membership_commands import (
        PersonalMembershipResultV2,
        derive_build_member,
    )
    member = derive_build_member(request, preparation, fleet)
    await stage_build_generation_evidence(session, request, member, preparation, fleet, previous=previous, previous_request=previous_request)
    rows = (await session.scalars(select(CapacitySubject).where(CapacitySubject.configuration_epoch == preparation.configuration_epoch))).all()
    await CapacityMembershipStore(management)._materialize_subject(session, preparation.configuration_epoch,
        member.configuration, _derive_owner_account(fleet, member.owner_id), rows)
    await session.flush()
    key = idempotency_key or UUID(int=member.owner_id.int + 4000)
    digest = canonical_digest(request)
    actor = preparation.personal_membership.management_principal_id
    head = canonical_membership_event_head(actor=actor, execution_epoch=request.execution.execution_epoch,
        idempotency_key=key, operation_id=request.command.projection.operation_id,
        previous_sha256=previous_head, request_digest=digest, request_payload=request.model_dump(mode="json"),
        member=member, revision=member.revision)
    result = PersonalMembershipResultV2(revision=member.revision, head_sha256=head, member=member, replayed=False)
    subject = member.configuration
    return CapacityPersonalMembershipEvent(execution_epoch=request.execution.execution_epoch,
        execution_manifest_sha256=request.execution.execution_manifest_sha256,
        authority_incarnation=request.execution.authority_incarnation, writer_epoch=request.execution.writer_epoch,
        namespace_id=request.namespace_id, revision=member.revision, previous_sha256=previous_head, head_sha256=head,
        actor=actor, idempotency_key=key, operation_id=request.command.projection.operation_id,
        request_digest=digest, request_payload=request.model_dump(mode="json"), result_payload=result.model_dump(mode="json"),
        subject_id=subject.subject_id, subject_incarnation=subject.subject_incarnation, owner_id=member.owner_id,
        configuration_generation=subject.configuration_generation, deployment_generation=subject.deployment_generation,
        reporter_incarnation=subject.demand_reporter_incarnation)
