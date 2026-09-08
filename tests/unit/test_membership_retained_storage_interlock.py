"""No recreation may reuse a retained membership database's protected singleton."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom.personal_dev_environment import (
    PersonalDevAccessBinding,
    PersonalDevEnvironmentApplyRequest,
)
from loom.personal_dev_environment_store import (
    PersonalDevEnvironmentConflictError,
    SqlAlchemyPersonalDevEnvironmentAuthority,
)
from tests.unit.test_personal_dev_reconciler import _NOW


@pytest.mark.parametrize("mode,accepted_mode", (
    ("membership-v1", "shadow-v1"),
    ("membership-v1", "membership-v1"),
    ("shadow-v1", "membership-v1"),
))
async def test_membership_retained_data_recreation_rejects_before_identity_or_storage_changes(monkeypatch, mode, accepted_mode):
    requested = PersonalDevEnvironmentApplyRequest(
        name="alice", owner_user_id=uuid4(), owner_team_id=uuid4(), candidate_id=uuid4(),
        candidate_sha="a" * 64, min_slots=0, max_slots=2, expected_operation_epoch=4,
        idempotency_key=uuid4(),
    )
    candidate = SimpleNamespace(
        owner_user_id=requested.owner_user_id, owner_team_id=requested.owner_team_id,
        candidate_sha=requested.candidate_sha, artifact_state="retained",
    )
    environment = SimpleNamespace(
        owner_user_id=requested.owner_user_id, owner_team_id=requested.owner_team_id,
        subject_id=uuid4(), subject_incarnation=uuid4(), operation_epoch=4,
        name="alice", status="deleted", keep_data=True,
        accepted_capacity_mode=accepted_mode,
    )
    previous = vars(environment).copy()
    results = iter((None, None, None, candidate, None, None))

    class Session:
        async def execute(self, query):
            value = next(results)
            return SimpleNamespace(scalar_one_or_none=lambda: value)

    authority = SqlAlchemyPersonalDevEnvironmentAuthority(Session())

    async def locked(_name):
        return environment

    async def limits(*args, **kwargs):
        pytest.fail("unsupported retained storage reached recreation admission")

    monkeypatch.setattr(authority, "_locked_environment", locked)
    monkeypatch.setattr(authority, "_assert_limits", limits)
    with pytest.raises(PersonalDevEnvironmentConflictError, match="fresh storage"):
        await authority._claim_apply(
            requested, access_binding=PersonalDevAccessBinding("bearer", b"h" * 32),
            capacity_mode=mode, now=_NOW,
        )
    assert vars(environment) == previous
