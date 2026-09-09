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


def _allocator_build_subject(index=40, *, capabilities=True):
    from tests.capacity_fixtures import allocator_subject

    subject = allocator_subject(index, account_id=f"dev-owner-{_candidate().owner_user_id.hex}")
    profiles = []
    for profile in subject.configuration.profiles:
        architecture = "cpu_arch.arm64" if profile.pool_id == "gb10" else "cpu_arch.x86_64"
        shapes = tuple(shape.model_copy(update={
            "capabilities": (architecture, "personal-build-worker") if capabilities else ("cpu",),
        }) for shape in profile.worker_shapes)
        profiles.append(profile.model_copy(update={"worker_shapes": shapes}))
    demand = subject.last_demand.model_copy(update={"pending_unassigned": _project((
        _request(), _request("linux/amd64"),
    ))})
    return subject.model_copy(update={
        "configuration": subject.configuration.model_copy(update={"profiles": tuple(profiles)}),
        "last_demand": demand,
    })


@pytest.mark.parametrize("capabilities", (True, False))
def test_existing_allocator_routes_only_to_build_capable_native_shapes(capabilities):
    from loom_capacity_manager.allocator import allocate_shadow
    from tests.capacity_fixtures import allocator_input

    subject = _allocator_build_subject(capabilities=capabilities)
    result = allocate_shadow(allocator_input((subject,), gb10_slots=1, oldlab_slots=1))
    actual = {allowance.attempt_id: allocation.pool_id for allocation in result.allocations
              for allowance in allocation.placement_allowances}
    expected = {bucket.attempt_ids[0]: bucket.eligible_pool_ids[0]
                for bucket in subject.last_demand.pending_unassigned} if capabilities else {}
    assert actual == expected


def test_build_and_application_share_one_owner_ceiling():
    from loom_capacity_manager.allocator import allocate_shadow
    from loom_capacity_manager.contracts import canonical_digest, canonical_digest_excluding
    from tests.capacity_fixtures import allocator_input, allocator_subject

    build = _allocator_build_subject()
    application = allocator_subject(41, account_id=build.configuration.account_id,
                                    pending=(("application-task", ("gb10", "oldlab"), ("cpu",)),))
    value = allocator_input((build, application), gb10_slots=2, oldlab_slots=2)
    fleet = value.fleet.model_copy(update={"account_policies": tuple(
        account.model_copy(update={"max_slots": 1}) for account in value.fleet.account_policies
    )})
    fleet = fleet.model_copy(update={"fleet_digest": canonical_digest_excluding(fleet, "fleet_digest")})
    configuration = value.configuration.model_copy(update={
        "fleet": value.configuration.fleet.model_copy(update={"digest": canonical_digest(fleet)}),
    })
    result = allocate_shadow(value.model_copy(update={"fleet": fleet, "configuration": configuration}))
    assert sum(item.desired_slots for item in result.allocations) == 1
