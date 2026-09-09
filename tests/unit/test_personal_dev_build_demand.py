"""Owner/lease-fenced native-platform demand, before executable admission."""

from dataclasses import replace
from datetime import timedelta
from importlib import import_module
from uuid import UUID

import pytest

from loom.personal_dev_candidate import CandidateRegistration
from tests.unit.test_personal_dev_builder import _NOW, _attempt, _candidate


def _request(platform="linux/arm64", *, candidate=None, attempt=None):
    module = import_module("loom.personal_dev_build_demand")
    return module.PersonalDevBuildDemandRequest(
        registration=CandidateRegistration(
            candidate=candidate or _candidate(),
            build_attempt=attempt or _attempt(state="running"), created=False,
        ),
        platform=platform,
    )


def _project(requests):
    return import_module("loom.personal_dev_build_demand").project_personal_build_demand(
        owner_user_id=_candidate().owner_user_id, requests=requests, now=_NOW,
    )


def test_both_platforms_have_distinct_stable_native_only_demand():
    arm, amd = _request(), _request("linux/amd64")
    buckets = _project((arm, amd))
    assert buckets == _project((amd, arm))
    assert len(buckets) == 2
    by_pool = {item.eligible_pool_ids: item for item in buckets}
    assert set(by_pool) == {("gb10",), ("oldlab",)}
    assert by_pool[("gb10",)].required_capabilities == ("cpu_arch.arm64", "personal-build-worker")
    assert by_pool[("oldlab",)].required_capabilities == ("cpu_arch.x86_64", "personal-build-worker")
    assert len({item.attempt_ids[0] for item in buckets}) == 2
    assert all(item.requested_slots == 1 and item.local_priority == 0 for item in buckets)
    assert all(item.oldest_submitted_at == _NOW for item in buckets)
    # Removing an already-finished platform does not change the other's identity.
    assert _project((arm,)) == (by_pool[("gb10",)],)


def test_no_platform_work_produces_no_warm_demand():
    assert _project(()) == ()


def test_new_whole_attempt_lease_cannot_reuse_platform_demand_identity():
    original = _project((_request(),))[0]
    renewed = _project((_request(attempt=_attempt(state="running", lease_epoch=10)),))[0]
    assert original.attempt_ids != renewed.attempt_ids


@pytest.mark.parametrize("changes", (
    {"owner_user_id": UUID(int=90)}, {"owner_user_id": UUID(int=0)},
    {"owner_team_id": UUID(int=0)}, {"id": UUID(int=0)},
    {"status": "ready"}, {"artifact_state": "collected"},
    {"candidate_sha": "0" * 64}, {"source_sha256": "not-a-digest"},
    {"archive_sha256": "0" * 64},
))
def test_foreign_or_unusable_candidate_cannot_emit_build_demand(changes):
    with pytest.raises(ValueError):
        _project((_request(candidate=_candidate(**changes)),))


@pytest.mark.parametrize("changes", (
    {"candidate_id": UUID(int=90)}, {"id": UUID(int=0)},
    {"state": "claimed"}, {"state": "succeeded"}, {"state": "cancelled"},
    {"lease_epoch": 0}, {"lease_epoch": True}, {"lease_expires_at": _NOW},
    {"lease_expires_at": None}, {"claimed_by": None},
    {"created_at": _NOW + timedelta(seconds=1)},
    {"created_at": _NOW.replace(tzinfo=None)}, {"finished_at": _NOW},
))
def test_stale_or_unclaimed_attempt_cannot_emit_build_demand(changes):
    with pytest.raises(ValueError):
        _project((_request(attempt=_attempt(**{"state": "running", **changes})),))


@pytest.mark.parametrize("platform", ("linux/neutral", "linux/arm/v7", "arm64"))
def test_no_architecture_neutral_or_emulated_build_demand(platform):
    with pytest.raises(ValueError):
        _project((_request(platform),))


def test_duplicate_platform_requests_are_rejected_not_double_charged():
    request = _request()
    with pytest.raises(ValueError):
        _project((request, request))


def test_conflicting_records_for_same_whole_attempt_are_rejected():
    first = _request()
    second = _request("linux/amd64", candidate=_candidate(source_sha256="e" * 64))
    with pytest.raises(ValueError):
        _project((first, second))


def test_batch_and_time_bounds_reject_before_projection():
    module = import_module("loom.personal_dev_build_demand")
    request = _request()
    with pytest.raises(ValueError):
        _project((request,) * 2049)
    with pytest.raises(ValueError):
        module.project_personal_build_demand(
            owner_user_id=_candidate().owner_user_id, requests=(request,), now=_NOW.replace(tzinfo=None),
        )
    with pytest.raises(ValueError):
        _project((replace(request, registration=CandidateRegistration.from_candidate(_candidate())),))
