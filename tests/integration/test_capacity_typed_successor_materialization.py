"""Materialization joins purpose-aware inherited bases to a supplied verified tip.

This tests the internal materialization consumer, not event insertion or activation.
The retired source chain is real; the local overlay is deliberately supplied at
the consumer boundary while runtime and SQL successor admission stay closed.
"""

import pytest

from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.store import CapacityManagementStore, ConfigurationConflictError, _derive_owner_account
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
