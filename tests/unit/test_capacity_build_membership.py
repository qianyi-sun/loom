"""Build membership composes with applications without creating another budget."""

from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.contracts import canonical_digest, canonical_digest_excluding
from loom_capacity_manager.executable_contracts import CandidateBindingV2
from loom_capacity_manager.membership import resolved_subject_references
from loom_capacity_manager.membership_contracts import (
    ExecutionPreparationV3,
    PersonalApplicationMemberV1,
    parse_execution_preparation,
)
from tests.capacity_fixtures import allocator_subject
from tests.unit.test_capacity_membership import (
    OWNER_A,
    OWNER_B,
    _member,
    delegated_input_with_new_owner,
)


def build_membership_input():
    from tests.capacity_build_membership_fixtures import application_origin_payload

    module = import_module("loom_capacity_manager.build_membership_contracts")
    base = delegated_input_with_new_owner()
    profiles = []
    for profile in base.fleet.development_subject_template.profiles:
        architecture = "arm64" if profile.pool_id == "gb10" else "x86_64"
        shape = profile.worker_shapes[0].model_copy(update={
            "shape_id": f"personal-build-{profile.pool_id}", "warm_approved": False,
            "capabilities": (f"cpu_arch.{architecture}", "personal-build-worker"),
        })
        profile = profile.model_copy(update={"worker_shapes": (shape,)})
        profile = profile.model_copy(update={"profile_digest": canonical_digest_excluding(profile, "profile_digest")})
        profiles.append(profile)
    template = module.PersonalBuildTemplateV1(
        runtime_candidate=CandidateBindingV2(algorithm="git-sha1", identity="a" * 40, publication_sha256="b" * 64),
        profiles=tuple(profiles), max_slots_per_subject=2,
        max_pending_slots_per_subject=2, max_pending_jobs_per_subject=2,
    )
    prep = module.ExecutionPreparationV4.model_validate(base.preparation.model_dump(mode="python") | {
        "schema_version": 4, "personal_builds": template,
        "subject_acknowledgements": (_member(base.managed_base_subjects[0], OWNER_A).acknowledgement,),
        "managed_application_origins": (application_origin_payload(base.managed_base_subjects[0],
            _member(base.managed_base_subjects[0], OWNER_A).acknowledgement,
            configuration_epoch=base.configuration.configuration_epoch),),
    })
    build_input = allocator_subject(93, account_id=f"dev-owner-{OWNER_B.hex}", max_slots=2, pending=(
        ("build-arm", ("gb10",), ("cpu_arch.arm64", "personal-build-worker")),
        ("build-amd", ("oldlab",), ("cpu_arch.x86_64", "personal-build-worker")),
    ))
    config = build_input.configuration.model_copy(update={
        "subject_id": module.personal_build_subject_id(base.membership.namespace_id, OWNER_B),
        "display_name": module.personal_build_subject_name(OWNER_B), "submission_rate_per_minute": 3,
        "profiles": template.profiles,
    })
    demand = build_input.last_demand.model_copy(update={"subject_id": config.subject_id})
    build_input = build_input.model_copy(update={
        "configuration": config, "last_demand": demand,
        "freshness": build_input.freshness.model_copy(update={"last_payload_digest": canonical_digest(demand)}),
    })
    acknowledgement = base.membership.members[0].acknowledgement.model_copy(update={
        "subject_id": config.subject_id, "subject_incarnation": config.subject_incarnation,
        "configuration_generation": config.configuration_generation,
        "deployment_generation": config.deployment_generation,
        "reporter_incarnation": config.demand_reporter_incarnation, "candidate": template.runtime_candidate,
    })
    member = module.PersonalBuildMemberV1(revision=2, owner_id=OWNER_B, configuration=config, acknowledgement=acknowledgement)
    snapshot = module.PersonalMembershipSnapshotV2(
        namespace_id=base.membership.namespace_id, revision=2, head_sha256="d" * 64,
        members=(*base.membership.members, member),
    )
    return module.DelegatedAllocationInputV3.model_validate(base.model_dump(mode="python") | {
        "schema_version": 3, "preparation": prep, "membership": snapshot, "subjects": (*base.subjects, build_input),
    })


def _replace_build(value, **changes):
    member = value.membership.members[-1]
    config = member.configuration.model_copy(update=changes)
    ack = member.acknowledgement.model_copy(update={
        "subject_id": config.subject_id, "subject_incarnation": config.subject_incarnation,
        "configuration_generation": config.configuration_generation,
        "deployment_generation": config.deployment_generation,
        "reporter_incarnation": config.demand_reporter_incarnation,
    })
    member = member.model_copy(update={"configuration": config, "acknowledgement": ack})
    return value.model_copy(update={
        "membership": value.membership.model_copy(update={"members": (*value.membership.members[:-1], member)}),
        "subjects": (*value.subjects[:-1], value.subjects[-1].model_copy(update={"configuration": config})),
    })


def test_typed_build_and_application_resolve_and_route_both_native_platforms():
    value = build_membership_input()
    refs = resolved_subject_references(value)
    assert {item.subject_id for item in refs} == {item.configuration.subject_id for item in value.subjects}
    result = allocate_shadow(value)
    build_id = value.membership.members[-1].configuration.subject_id
    actual = {allowance.attempt_id: allocation.pool_id for allocation in result.allocations
              if allocation.subject_id == build_id for allowance in allocation.placement_allowances}
    assert actual == {"build-arm": "gb10", "build-amd": "oldlab"}
    assert value.membership.members[-1].configuration.min_slots == 0
    assert value.membership.members[-1].configuration.account_id == value.membership.members[0].configuration.account_id


def test_app_and_build_use_one_owner_ceiling_in_real_allocator():
    value = build_membership_input()
    accounts = tuple(item.model_copy(update={"max_slots": 2}) for item in value.fleet.account_policies)
    fleet = value.fleet.model_copy(update={"account_policies": accounts})
    fleet = fleet.model_copy(update={"fleet_digest": canonical_digest_excluding(fleet, "fleet_digest")})
    configuration = value.configuration.model_copy(update={"fleet": value.configuration.fleet.model_copy(update={"digest": canonical_digest(fleet)})})
    value = value.model_copy(update={
        "fleet": fleet, "configuration": configuration,
        "effective_account_policies": tuple(item.model_copy(update={"max_slots": 2}) for item in value.effective_account_policies),
        "preparation": value.preparation.model_copy(update={"fleet_digest": canonical_digest(fleet)}),
    })
    result = allocate_shadow(value)
    owner_subjects = {item.configuration.subject_id for item in value.subjects if item.configuration.account_id == f"dev-owner-{OWNER_B.hex}"}
    assert sum(item.desired_slots for item in result.allocations if item.subject_id in owner_subjects) == 2


@pytest.mark.parametrize("changes", (
    {"subject_id": UUID(int=99)}, {"account_id": "build-budget"}, {"display_name": "dev-bob-build"},
    {"tier_id": "staging"}, {"min_slots": 1}, {"rollout_surge_slots": 1},
    {"max_slots": 3}, {"max_pending_slots": 3}, {"max_pending_jobs": 3},
    {"submission_rate_per_minute": 99}, {"lifecycle_state": "provisioning"},
))
def test_build_identity_and_policy_cannot_be_rebadged(changes):
    with pytest.raises(ValueError):
        resolved_subject_references(_replace_build(build_membership_input(), **changes))


def test_build_member_runtime_candidate_must_match_operator_template():
    value = build_membership_input()
    member = value.membership.members[-1]
    candidate = member.acknowledgement.candidate.model_copy(update={"publication_sha256": "f" * 64})
    member = member.model_copy(update={"acknowledgement": member.acknowledgement.model_copy(update={"candidate": candidate})})
    with pytest.raises(ValueError):
        resolved_subject_references(value.model_copy(update={"membership": value.membership.model_copy(update={"members": (*value.membership.members[:-1], member)})}))


def test_build_profiles_cannot_use_application_template():
    value = build_membership_input()
    with pytest.raises(ValueError):
        resolved_subject_references(_replace_build(value, profiles=value.fleet.development_subject_template.profiles))


@pytest.mark.parametrize("boundary", ("warm", "architecture", "purpose", "multi_node", "multiple_shapes", "digest"))
def test_build_template_requires_one_cold_native_shape_per_pool(boundary):
    value = build_membership_input()
    module = import_module("loom_capacity_manager.build_membership_contracts")
    template = value.preparation.personal_builds
    profile = template.profiles[0]
    shape = profile.worker_shapes[0]
    if boundary == "warm":
        shape = shape.model_copy(update={"warm_approved": True})
    elif boundary in {"architecture", "purpose"}:
        shape = shape.model_copy(update={"capabilities": ("cpu_arch.x86_64", "personal-build-worker") if boundary == "architecture" else ("cpu_arch.arm64", "cpu")})
    elif boundary == "multi_node":
        # Even a valid multi-node shape (zero-slot helper node) is outside the cold-build policy.
        extra = shape.total_resources.model_copy(update={"slots": 0, "cpu_millicores": 0, "memory_bytes": 0})
        shape = shape.model_copy(update={"node_resources": (*shape.node_resources, extra)})
    profiles = (shape, shape.model_copy(update={"shape_id": "second-shape"})) if boundary == "multiple_shapes" else (shape,)
    profile = profile.model_copy(update={"worker_shapes": profiles})
    profile = profile.model_copy(update={"profile_digest": "f" * 64 if boundary == "digest" else canonical_digest_excluding(profile, "profile_digest")})
    changed = template.model_copy(update={"profiles": (profile, template.profiles[1])})
    with pytest.raises(ValueError):
        module.PersonalBuildTemplateV1.model_validate_json(changed.model_dump_json())


def test_build_and_application_share_membership_and_live_subject_bounds():
    value = build_membership_input()
    limited = value.preparation.personal_membership.model_copy(update={"max_subjects": 2})
    with pytest.raises(ValueError):
        resolved_subject_references(value.model_copy(update={"preparation": value.preparation.model_copy(update={"personal_membership": limited})}))
    value = build_membership_input()
    template_account = value.fleet.account_policies[0].model_copy(update={"max_live_subjects": 1})
    fleet = value.fleet.model_copy(update={"account_policies": (template_account,)})
    fleet = fleet.model_copy(update={"fleet_digest": canonical_digest_excluding(fleet, "fleet_digest")})
    value = value.model_copy(update={
        "fleet": fleet, "configuration": value.configuration.model_copy(update={"fleet": value.configuration.fleet.model_copy(update={"digest": canonical_digest(fleet)})}),
        "effective_account_policies": tuple(item.model_copy(update={"max_live_subjects": 1}) for item in value.effective_account_policies),
        "preparation": value.preparation.model_copy(update={"fleet_digest": canonical_digest(fleet)}),
    })
    with pytest.raises(ValueError, match="max_live_subjects"):
        resolved_subject_references(value)


def test_build_identity_is_stable_per_owner_namespace_and_not_an_application():
    module = import_module("loom_capacity_manager.build_membership_contracts")
    value = build_membership_input()
    member = value.membership.members[-1]
    namespace = value.membership.namespace_id
    assert module.personal_build_subject_id(namespace, OWNER_B) == member.configuration.subject_id
    assert module.personal_build_subject_id(UUID(int=98), OWNER_B) != member.configuration.subject_id
    assert module.personal_build_subject_id(namespace, UUID(int=99)) != member.configuration.subject_id
    assert not isinstance(member, PersonalApplicationMemberV1)
    with pytest.raises(ValueError):
        PersonalApplicationMemberV1.model_validate_json(member.model_dump_json())
    assert not isinstance(value.preparation, ExecutionPreparationV3)
    with pytest.raises(ValueError):
        parse_execution_preparation(value.preparation.model_dump_json())


def test_new_snapshot_rejects_duplicate_build_service_for_owner():
    value = build_membership_input()
    module = import_module("loom_capacity_manager.build_membership_contracts")
    member = value.membership.members[-1]
    configuration = member.configuration.model_copy(update={"subject_id": UUID(int=99), "display_name": "different-build-name"})
    duplicate = member.model_copy(update={
        "revision": 3, "configuration": configuration,
        "acknowledgement": member.acknowledgement.model_copy(update={"subject_id": configuration.subject_id}),
    })
    snapshot = value.membership.model_copy(update={"revision": 3, "members": (*value.membership.members, duplicate)})
    with pytest.raises(ValueError):
        module.PersonalMembershipSnapshotV2.model_validate_json(snapshot.model_dump_json())


@pytest.mark.parametrize("field,limit", (("max_slots_per_subject", 5), ("max_pending_slots_per_subject", 5), ("max_pending_jobs_per_subject", 5)))
def test_build_template_limits_are_checked_even_without_build_members(field, limit):
    value = build_membership_input()
    value = value.model_copy(update={
        "subjects": value.subjects[:-1],
        "membership": value.membership.model_copy(update={"members": value.membership.members[:-1]}),
        "preparation": value.preparation.model_copy(update={"personal_builds": value.preparation.personal_builds.model_copy(update={field: limit})}),
    })
    with pytest.raises(ValueError, match="shared owner policy"):
        resolved_subject_references(value)


def test_build_profile_architecture_is_checked_against_fleet_domain():
    from loom_capacity_manager.membership import _validate_build_template
    value = build_membership_input()
    pool = value.fleet.pools[0]
    assert pool.pool_id == "gb10"
    pool = pool.model_copy(update={"resource_domains": tuple(domain.model_copy(update={"architecture": "x86_64"}) for domain in pool.resource_domains)})
    pool = pool.model_copy(update={"pool_digest": canonical_digest_excluding(pool, "pool_digest")})
    profile = value.preparation.personal_builds.profiles[0].model_copy(update={"pool_digest": pool.pool_digest})
    profile = profile.model_copy(update={"profile_digest": canonical_digest_excluding(profile, "profile_digest")})
    template = value.preparation.personal_builds.model_copy(update={"profiles": (profile, value.preparation.personal_builds.profiles[1])})
    fleet = value.fleet.model_copy(update={"pools": (pool, value.fleet.pools[1])})
    with pytest.raises(ValueError, match="natively placed"):
        _validate_build_template(template, fleet, value.fleet.account_policies[0])


@pytest.mark.parametrize("version", (4.0, "4", True, 3))
def test_build_preparation_version_is_exact_even_for_unchecked_models(version):
    value = build_membership_input()
    with pytest.raises(ValueError):
        resolved_subject_references(value.model_copy(update={"preparation": value.preparation.model_copy(update={"schema_version": version})}))


def test_disabled_build_retains_identity_but_requires_zero_capacity():
    value = _replace_build(build_membership_input(), lifecycle_state="disabled", max_slots=0)
    assert len(resolved_subject_references(value)) == 3
    with pytest.raises(ValueError):
        resolved_subject_references(_replace_build(value, max_slots=1))


@pytest.mark.parametrize("managed", (True, False))
def test_build_cannot_override_a_managed_or_immutable_base_identity(managed):
    from loom_capacity_manager.application_origin_contracts import ManagedApplicationOriginV1
    from tests.capacity_build_membership_fixtures import application_origin_payload

    value = build_membership_input()
    build_id = value.membership.members[-1].configuration.subject_id
    base = value.managed_base_subjects[0].model_copy(update={"subject_id": build_id})
    reference = value.configuration.subjects[0].model_copy(update={"subject_id": build_id, "digest": canonical_digest(base)})
    policy = value.preparation.personal_membership.model_copy(update={"managed_base_subject_ids": (build_id,) if managed else ()})
    ack = _member(base, OWNER_A).acknowledgement
    origins = (ManagedApplicationOriginV1.model_validate(application_origin_payload(base, ack,
        configuration_epoch=value.configuration.configuration_epoch)),) if managed else ()
    value = value.model_copy(update={
        "configuration": value.configuration.model_copy(update={"subjects": (reference,)}),
        "managed_base_subjects": (base,) if managed else (),
        "preparation": value.preparation.model_copy(update={"personal_membership": policy,
            "managed_application_origins": origins, "subject_acknowledgements": (ack,)}),
        "subjects": value.subjects[1:],
    })
    with pytest.raises(ValueError, match="cannot override an application or immutable base"):
        resolved_subject_references(value)


@pytest.mark.parametrize("managed", (True, False))
def test_reserved_build_name_cannot_collide_with_a_base_subject(managed):
    value = build_membership_input()
    base = value.managed_base_subjects[0].model_copy(update={"display_name": value.membership.members[-1].configuration.display_name})
    reference = value.configuration.subjects[0].model_copy(update={"digest": canonical_digest(base)})
    policy = value.preparation.personal_membership.model_copy(update={"managed_base_subject_ids": (base.subject_id,) if managed else ()})
    value = value.model_copy(update={
        "configuration": value.configuration.model_copy(update={"subjects": (reference,)}),
        "managed_base_subjects": (base,) if managed else (),
        "preparation": value.preparation.model_copy(update={"personal_membership": policy,
            "managed_application_origins": value.preparation.managed_application_origins if managed else ()}),
        "subjects": (value.subjects[0].model_copy(update={"configuration": base}), *value.subjects[1:]),
    })
    # A managed base's pinned origin rejects rebadging before name resolution;
    # unmanaged static bases still exercise the resolved-name collision boundary.
    with pytest.raises(ValueError, match="origin differs from immutable base" if managed else "name collides"):
        resolved_subject_references(value)


def _build_successor():
    from loom_capacity_manager.contracts import ConfigurationGenerationRefV1
    from loom_capacity_manager.executable_contracts import canonical_executable_digest
    from loom_capacity_manager.membership_contracts import PersonalReincarnationEvidenceV1
    value = build_membership_input()
    original = value.membership.members[-1].configuration
    successor = _replace_build(value, subject_incarnation=UUID(int=234), demand_reporter_incarnation=UUID(int=235), configuration_generation=3)
    evidence = PersonalReincarnationEvidenceV1(
        namespace_id=value.membership.namespace_id, execution_manifest_sha256=canonical_executable_digest(value.preparation),
        origin=ConfigurationGenerationRefV1(scope="subject", generation=1, digest=canonical_digest(original), subject_id=original.subject_id, subject_incarnation=original.subject_incarnation),
        predecessor=original.model_copy(update={"lifecycle_state": "disabled", "max_slots": 0, "configuration_generation": 2}),
        predecessor_revision=2, predecessor_head_sha256="e" * 64, admission_revision=3,
        successor_incarnation=UUID(int=234), release_set_sha256="f" * 64,
    )
    member = successor.membership.members[-1].model_copy(update={"revision": 3, "reincarnation": evidence})
    return successor.model_copy(update={"membership": successor.membership.model_copy(update={"revision": 3, "members": (*successor.membership.members[:-1], member)})})


def test_build_successor_keeps_stable_service_id_with_new_incarnation():
    value = _build_successor()
    refs = resolved_subject_references(value)
    member = value.membership.members[-1]
    selected = next(item for item in refs if item.subject_id == member.configuration.subject_id)
    assert selected.subject_incarnation == UUID(int=234)
    assert selected.generation == 3
    assert member.configuration.candidate_generation == member.configuration.deployment_generation == 1


@pytest.mark.parametrize("boundary", ("reporter", "incarnation", "account", "namespace", "manifest", "candidate_generation", "deployment_generation"))
def test_build_successor_rejects_changed_release_provenance_or_identity(boundary):
    value = _build_successor()
    member = value.membership.members[-1]
    evidence = member.reincarnation
    if boundary in {"reporter", "incarnation", "candidate_generation", "deployment_generation"}:
        field, changed = {
            "reporter": ("demand_reporter_incarnation", evidence.predecessor.demand_reporter_incarnation),
            "incarnation": ("subject_incarnation", evidence.predecessor.subject_incarnation),
            "candidate_generation": ("candidate_generation", 2), "deployment_generation": ("deployment_generation", 2),
        }[boundary]
        value = _replace_build(value, **{field: changed})
    else:
        if boundary == "account":
            evidence = evidence.model_copy(update={"predecessor": evidence.predecessor.model_copy(update={"account_id": "foreign-account"})})
        elif boundary == "namespace":
            evidence = evidence.model_copy(update={"namespace_id": UUID(int=999)})
        else:
            evidence = evidence.model_copy(update={"execution_manifest_sha256": "1" * 64})
        member = member.model_copy(update={"reincarnation": evidence})
        value = value.model_copy(update={"membership": value.membership.model_copy(update={"members": (*value.membership.members[:-1], member)})})
    with pytest.raises(ValueError):
        resolved_subject_references(value)


@pytest.mark.parametrize("policy_version", (2, 3, 4))
async def test_direct_v4_preparation_is_rejected_before_legacy_validation(policy_version):
    from loom_capacity_manager.store import CapacityManagementStore, ExecutionConflictError
    from tests.capacity_execution_fixtures import execution_policy
    module = import_module("loom_capacity_manager.build_membership_contracts")
    value = build_membership_input()
    policy = execution_policy()
    if policy_version == 3:
        from loom_capacity_manager.membership_contracts import ExecutionPreparationPolicyV3
        policy = ExecutionPreparationPolicyV3.model_validate(policy.model_dump(mode="python") | {
            "schema_version": 3, "personal_membership": value.preparation.personal_membership,
        })
    elif policy_version == 4:
        policy = module.ExecutionPreparationPolicyV4.model_validate(policy.model_dump(mode="python") | {
            "schema_version": 4, "personal_membership": value.preparation.personal_membership,
            "personal_builds": value.preparation.personal_builds,
            "managed_application_origins": value.preparation.managed_application_origins,
            "subject_acknowledgements": value.preparation.subject_acknowledgements,
        })
    store = CapacityManagementStore(execution_policy=policy)
    session = AsyncMock()
    authority = SimpleNamespace(authority_incarnation=value.preparation.authority_incarnation, writer_epoch=value.preparation.expected_writer_epoch)
    with pytest.raises(ExecutionConflictError, match="unsupported execution preparation schema"):
        await store._validate_execution_preparation(session, authority, value.preparation)
    session.execute.assert_not_awaited()


async def test_direct_v4_reconciliation_requires_active_execution_authority():
    from loom_capacity_manager.reconciler import _commit_reconciled_epoch
    from loom_capacity_manager.store import CapacityStoreError
    value = build_membership_input()
    shadow = allocate_shadow(value)
    writer = SimpleNamespace(authority_incarnation=value.fleet.authority_incarnation, writer_epoch=1)
    session = MagicMock()
    session.begin.return_value.__aenter__ = AsyncMock()
    session.begin.return_value.__aexit__ = AsyncMock(return_value=False)
    connection = SimpleNamespace(get_isolation_level=AsyncMock(return_value="SERIALIZABLE"))
    session.connection = AsyncMock(return_value=connection)
    query = MagicMock()
    query.scalar_one_or_none.return_value = SimpleNamespace(authority_incarnation=writer.authority_incarnation, writer_epoch=1)
    session.execute = AsyncMock(return_value=query)
    store = SimpleNamespace(load_allocation_input=AsyncMock(return_value=value), execution_authority=AsyncMock(return_value=None))
    with pytest.raises(CapacityStoreError, match="active execution authority changed"):
        await _commit_reconciled_epoch(session, store, writer, shadow)
    store.execution_authority.assert_awaited_once()
    session.add.assert_not_called()
