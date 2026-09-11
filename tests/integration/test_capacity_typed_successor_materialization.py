"""Materialization joins purpose-aware inherited bases to a supplied verified tip.

This tests the internal materialization consumer, not event insertion or activation.
The retired source chain is real; the local overlay is deliberately supplied at
the consumer boundary while runtime and SQL successor admission stay closed.
"""

import pytest

from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.store import (
    CapacityManagementStore,
    ConfigurationConflictError,
    _derive_owner_account,
)
from loom_capacity_manager.typed_membership_commands import PersonalMembershipResultV2
from loom_capacity_manager.typed_membership_store import _validated_materialization
from tests.integration.test_capacity_retired_source_graph import load, seed_empty_successor
from tests.integration.test_capacity_successor_source_verification import successor


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("disabled", (False, True))
async def test_inherited_materialization_accepts_exact_typed_overlay(capacity_session, build, disabled):
    candidate, _ = await successor(capacity_session, resized=True)
    candidate, source = await seed_empty_successor(capacity_session, candidate, epoch=43)
    history = await load(capacity_session, source)
    origin = candidate.managed_build_origins[0] if build else candidate.managed_application_origins[0]
    old = origin.configuration
    config = old.model_copy(update={"configuration_generation": old.configuration_generation + 1,
        "lifecycle_state": "disabled" if disabled else "active", "min_slots": 0, "max_slots": 0 if disabled else 1})
    member = origin.inherited.anchor.member.model_copy(update={"revision": 1, "reincarnation": None,
        "configuration": config, "acknowledgement": origin.acknowledgement.model_copy(update={"configuration_generation": config.configuration_generation})})
    result = PersonalMembershipResultV2(revision=1, head_sha256="f" * 64, member=member, replayed=False)
    rows, _ = await _validated_materialization(capacity_session, history.epoch, history.fleet, {})
    await CapacityMembershipStore(CapacityManagementStore())._materialize_subject(capacity_session,
        candidate.configuration_epoch, config, _derive_owner_account(history.fleet, member.owner_id), rows)
    await capacity_session.flush()
    rows, accounts = await _validated_materialization(capacity_session, history.epoch, history.fleet, {config.subject_id: result})
    assert len(rows) == len({row.subject_id for row in rows})
    assert next(row for row in rows if row.subject_id == config.subject_id).payload == config.model_dump(mode="json")
    assert member.configuration.account_id in {account.account_id for account in accounts}
    with pytest.raises(ConfigurationConflictError):
        await _validated_materialization(capacity_session, history.epoch, history.fleet, {})


async def test_inherited_build_materialization_recreation_keeps_original_root(capacity_session):
    from uuid import UUID

    from loom_capacity_manager.membership_contracts import PersonalReincarnationEvidenceV1
    from loom_capacity_manager.retired_application_import import _reference

    candidate, _ = await successor(capacity_session, resized=True)
    candidate, source = await seed_empty_successor(capacity_session, candidate, epoch=43)
    history = await load(capacity_session, source)
    origin = candidate.managed_build_origins[0]
    predecessor = origin.configuration.model_copy(update={"lifecycle_state": "disabled", "min_slots": 0, "max_slots": 0,
        "configuration_generation": origin.configuration.configuration_generation + 1})
    evidence = PersonalReincarnationEvidenceV1(namespace_id=candidate.personal_membership.namespace_id,
        execution_manifest_sha256=source.execution_manifest_sha256, origin=origin.inherited.original_origin,
        predecessor=predecessor, predecessor_revision=1, predecessor_head_sha256="e" * 64,
        admission_revision=2, successor_incarnation=UUID(int=99950), release_set_sha256="f" * 64)
    config = origin.configuration.model_copy(update={"subject_incarnation": evidence.successor_incarnation,
        "configuration_generation": predecessor.configuration_generation + 1,
        "demand_reporter_incarnation": UUID(int=99951), "candidate_generation": 1, "deployment_generation": 1})
    member = origin.inherited.anchor.member.model_copy(update={"revision": 2, "reincarnation": evidence,
        "configuration": config, "acknowledgement": origin.acknowledgement.model_copy(update={
            "subject_incarnation": config.subject_incarnation, "configuration_generation": config.configuration_generation,
            "reporter_incarnation": config.demand_reporter_incarnation})})
    result = PersonalMembershipResultV2.model_validate_json(PersonalMembershipResultV2(
        revision=2, head_sha256="a" * 64, member=member, replayed=False).model_dump_json())
    rows, _ = await _validated_materialization(capacity_session, history.epoch, history.fleet, {})
    await CapacityMembershipStore(CapacityManagementStore())._materialize_subject(capacity_session,
        candidate.configuration_epoch, config, _derive_owner_account(history.fleet, member.owner_id), rows)
    await capacity_session.flush()
    await _validated_materialization(capacity_session, history.epoch, history.fleet, {config.subject_id: result})
    wrong_root = _reference(origin.configuration)
    assert wrong_root != evidence.origin
    wrong = result.model_copy(update={"member": member.model_copy(update={"reincarnation": evidence.model_copy(update={"origin": wrong_root})})})
    with pytest.raises(ConfigurationConflictError, match="pinned managed base identity"):
        await _validated_materialization(capacity_session, history.epoch, history.fleet, {config.subject_id: wrong})


@pytest.mark.parametrize("build", (False, True))
async def test_inherited_materialization_rejects_opposite_purpose_overlay(capacity_session, build):
    from loom_capacity_manager.build_value_contracts import PersonalBuildMemberV1
    from loom_capacity_manager.executable_contracts import CandidateBindingV2
    from loom_capacity_manager.membership_contracts import PersonalApplicationMemberV1

    candidate, _ = await successor(capacity_session, resized=True)
    candidate, source = await seed_empty_successor(capacity_session, candidate, epoch=43)
    history = await load(capacity_session, source)
    origin = candidate.managed_build_origins[0] if build else candidate.managed_application_origins[0]
    config = origin.configuration.model_copy(update={"configuration_generation": origin.configuration.configuration_generation + 1})
    binding = (CandidateBindingV2(algorithm="source-sha256", identity="a" * 64, publication_sha256="b" * 64)
        if build else candidate.personal_builds.runtime_candidate)
    model = PersonalApplicationMemberV1 if build else PersonalBuildMemberV1
    opposite = model(revision=1, owner_id=origin.base_projection.owner_id, configuration=config,
        acknowledgement=origin.acknowledgement.model_copy(update={"candidate": binding, "configuration_generation": config.configuration_generation}))
    result = PersonalMembershipResultV2.model_validate_json(PersonalMembershipResultV2(
        revision=1, head_sha256="a" * 64, member=opposite, replayed=False).model_dump_json())
    assert result.member.purpose != origin.inherited.anchor.member.purpose
    with pytest.raises(ConfigurationConflictError, match="pinned managed base identity"):
        await _validated_materialization(capacity_session, history.epoch, history.fleet, {config.subject_id: result})
