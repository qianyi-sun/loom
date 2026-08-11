from uuid import UUID

import pytest

from loom.personal_dev_environment import (
    PersonalDevAccessBinding,
    PersonalDevEnvironmentApplyRequest,
    PersonalDevLifecycleLimits,
)

_OWNER = UUID("00000000-0000-0000-0000-000000000001")
_TEAM = UUID("00000000-0000-0000-0000-000000000002")
_CANDIDATE = UUID("00000000-0000-0000-0000-000000000003")
_KEY = UUID("00000000-0000-0000-0000-000000000004")


def _request(**overrides: object) -> PersonalDevEnvironmentApplyRequest:
    values: dict[str, object] = {
        "name": "alice",
        "owner_user_id": _OWNER,
        "owner_team_id": _TEAM,
        "candidate_id": _CANDIDATE,
        "candidate_sha": "a" * 64,
        "min_slots": 0,
        "max_slots": 2,
        "expected_operation_epoch": 0,
        "idempotency_key": _KEY,
    }
    values.update(overrides)
    return PersonalDevEnvironmentApplyRequest(**values)  # type: ignore[arg-type]


def test_apply_request_digest_is_canonical_and_complete() -> None:
    request = _request()
    assert request.request_sha256 == _request().request_sha256
    assert request.request_sha256 != _request(max_slots=3).request_sha256
    assert request.request_sha256 != _request(expected_operation_epoch=1).request_sha256
    assert len(request.request_sha256) == 64


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", "dev"),
        ("candidate_sha", "A" * 64),
        ("candidate_sha", "a" * 40),
        ("min_slots", -1),
        ("max_slots", 9),
        ("min_slots", 3),
        ("expected_operation_epoch", -1),
    ],
)
def test_apply_request_rejects_unsafe_values(field: str, value: object) -> None:
    overrides = {field: value}
    if field == "min_slots" and value == 3:
        overrides["max_slots"] = 2
    with pytest.raises(ValueError):
        _request(**overrides)


def test_lifecycle_limits_are_finite_and_ordered() -> None:
    assert PersonalDevLifecycleLimits().global_live_instances == 16
    with pytest.raises(ValueError):
        PersonalDevLifecycleLimits(global_live_instances=0)
    with pytest.raises(ValueError):
        PersonalDevLifecycleLimits(
            per_owner_aggregate_min_slots=9,
            per_owner_aggregate_max_slots=8,
        )


def test_access_binding_accepts_only_exact_verified_credential_hashes() -> None:
    bearer = PersonalDevAccessBinding(auth_kind="bearer", credential_hash=b"b" * 32)
    browser = PersonalDevAccessBinding(auth_kind="session", credential_hash=b"s" * 32)
    assert bearer.credential_hash == b"b" * 32
    assert browser.auth_kind == "session"

    with pytest.raises(ValueError):
        PersonalDevAccessBinding(auth_kind="step", credential_hash=b"x" * 32)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        PersonalDevAccessBinding(auth_kind="bearer", credential_hash=b"short")
