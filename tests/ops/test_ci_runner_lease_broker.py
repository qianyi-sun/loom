from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loom_control_plane import ci_runner_lease_broker as leases

HEAD_SHA = "a" * 40
NOW = datetime(2026, 8, 7, 20, 0, tzinfo=UTC)


def _config() -> leases.LeaseBrokerConfig:
    return leases.LeaseBrokerConfig(
        repository="qianyi-sun/loom",
        oldlab_labels=(
            "self-hosted",
            "linux",
            "x64",
            "loom-ci",
            "oldlab-5",
            "ephemeral-kvm",
        ),
        capacities={"normal": 5, "image": 4, "smoke": 2},
    )


def _request(
    sequence: int,
    *,
    work_class: str = "normal",
    workflow_run_id: int = 10_000,
    run_attempt: int = 1,
    head_sha: str = HEAD_SHA,
    ttl_seconds: int = 300,
) -> leases.AssignmentRequest:
    return leases.AssignmentRequest(
        repository="qianyi-sun/loom",
        workflow_run_id=workflow_run_id,
        run_attempt=run_attempt,
        job_key=f"{work_class}-job-{sequence}",
        head_sha=head_sha,
        work_class=work_class,
        lease_ttl_seconds=ttl_seconds,
    )


def _broker(tmp_path: Path) -> leases.CiRunnerLeaseBroker:
    return leases.CiRunnerLeaseBroker(tmp_path / "leases.sqlite3", _config())


def _route_request(
    *,
    workflow_name: str = "CI",
    workflow_run_id: int = 20_000,
    run_attempt: int = 1,
    head_sha: str = HEAD_SHA,
    job_count: int = 7,
) -> leases.RouteRequest:
    workflow_id = leases.WORKFLOW_CLASS_CONTRACTS[workflow_name][0]
    return leases.RouteRequest(
        repository="qianyi-sun/loom",
        workflow_name=workflow_name,
        workflow_id=workflow_id,
        workflow_run_id=workflow_run_id,
        run_attempt=run_attempt,
        head_sha=head_sha,
        job_keys=tuple(f"{workflow_name}-job-{index}" for index in range(job_count)),
    )


def test_checked_in_profile_defines_exact_5_4_2_capacity() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    config = leases.LeaseBrokerConfig.from_profile(repo_root / "deploy/ci-runners/oldlab5.toml")

    assert config.capacities == {"normal": 5, "image": 4, "smoke": 2}
    assert config.repository == "qianyi-sun/loom"
    assert "oldlab-5" in config.oldlab_labels


@pytest.mark.parametrize(
    ("work_class", "capacity", "hosted_runs_on"),
    [
        ("normal", 5, ("ubuntu-latest",)),
        ("image", 4, ("ubuntu-24.04",)),
        ("smoke", 2, ("ubuntu-latest",)),
    ],
)
def test_oldlab_is_preferred_until_exact_class_capacity(
    tmp_path: Path,
    work_class: str,
    capacity: int,
    hosted_runs_on: tuple[str, ...],
) -> None:
    broker = _broker(tmp_path)

    assignments = [
        broker.allocate(_request(index, work_class=work_class), now=NOW)
        for index in range(capacity + 2)
    ]

    oldlab = [item for item in assignments if item.target is leases.PlacementTarget.OLDLAB]
    hosted = [item for item in assignments if item.target is leases.PlacementTarget.GITHUB_HOSTED]
    assert [item.slot for item in oldlab] == list(range(capacity))
    assert len(hosted) == 2
    assert all(item.runs_on == hosted_runs_on for item in hosted)
    assert all(leases.CLASS_LABELS[work_class] in item.runs_on for item in oldlab)


def test_concurrent_requests_never_oversubscribe_oldlab_slots(tmp_path: Path) -> None:
    broker = _broker(tmp_path)

    with ThreadPoolExecutor(max_workers=20) as executor:
        assignments = list(
            executor.map(
                lambda index: broker.allocate(_request(index), now=NOW),
                range(20),
            )
        )

    oldlab = [item for item in assignments if item.target is leases.PlacementTarget.OLDLAB]
    hosted = [item for item in assignments if item.target is leases.PlacementTarget.GITHUB_HOSTED]
    assert len(oldlab) == 5
    assert {item.slot for item in oldlab} == {0, 1, 2, 3, 4}
    assert len(hosted) == 15
    assert len({item.lease_epoch for item in assignments}) == 20


def test_exact_request_replay_returns_frozen_assignment(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    request = _request(1)

    first = broker.allocate(request, now=NOW)
    replay = broker.allocate(request, now=NOW + timedelta(minutes=1))

    assert replay == first


def test_route_request_schema_is_exact_and_binds_workflow_contract() -> None:
    value = _route_request().public_dict()

    assert leases.RouteRequest.from_mapping(value) == _route_request()

    with pytest.raises(leases.LeaseBrokerError, match="fields do not match"):
        leases.RouteRequest.from_mapping({**value, "unexpected": True})
    with pytest.raises(leases.LeaseBrokerError, match="id does not match"):
        leases.RouteRequest.from_mapping({**value, "workflow_id": 1})
    with pytest.raises(leases.LeaseBrokerError, match="must be unique"):
        leases.RouteRequest.from_mapping({**value, "job_keys": ["duplicate", "duplicate"]})


@pytest.mark.parametrize(
    ("workflow_name", "job_count", "expected_oldlab", "expected_hosted"),
    [
        ("CI", 7, 5, 2),
        ("images", 6, 4, 2),
    ],
)
def test_route_allocation_is_oldlab_first_with_class_overflow(
    tmp_path: Path,
    workflow_name: str,
    job_count: int,
    expected_oldlab: int,
    expected_hosted: int,
) -> None:
    document = _broker(tmp_path).allocate_route(
        _route_request(workflow_name=workflow_name, job_count=job_count),
        now=NOW,
    )

    assert document.workflow_name == workflow_name
    assert len(document.request_sha256) == 64
    assert (
        sum(item.target is leases.PlacementTarget.OLDLAB for item in document.assignments)
        == expected_oldlab
    )
    assert (
        sum(item.target is leases.PlacementTarget.GITHUB_HOSTED for item in document.assignments)
        == expected_hosted
    )


def test_cluster_and_staging_share_two_smoke_slots(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    cluster = broker.allocate_route(
        _route_request(workflow_name="cluster-smoke", workflow_run_id=21_000, job_count=1),
        now=NOW,
    )
    staging = broker.allocate_route(
        _route_request(workflow_name="staging-smoke", workflow_run_id=21_001, job_count=1),
        now=NOW,
    )
    overflow = broker.allocate_route(
        _route_request(workflow_name="cluster-smoke", workflow_run_id=21_002, job_count=1),
        now=NOW,
    )

    assert cluster.assignments[0].target is leases.PlacementTarget.OLDLAB
    assert staging.assignments[0].target is leases.PlacementTarget.OLDLAB
    assert overflow.assignments[0].target is leases.PlacementTarget.GITHUB_HOSTED


def test_route_replay_returns_the_same_frozen_document(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    request = _route_request()

    first = broker.allocate_route(request, now=NOW)
    replay = broker.allocate_route(request, now=NOW + timedelta(minutes=1))

    assert replay == first


def test_batch_allocation_rolls_back_every_new_assignment_on_conflict(
    tmp_path: Path,
) -> None:
    broker = _broker(tmp_path)
    broker.allocate(_request(1), now=NOW)
    new_request = _request(2, workflow_run_id=10_001)
    conflicting_replay = _request(1, head_sha="b" * 40)

    with pytest.raises(leases.LeaseBrokerError, match="different inputs"):
        broker.allocate_many((new_request, conflicting_replay), now=NOW)

    status = broker.status(now=NOW)
    assert status["classes"]["normal"]["oldlab_assigned"] == 1
    recovered = broker.allocate(new_request, now=NOW)
    assert recovered.lease_epoch == 2


def test_request_identity_replay_with_changed_head_fails_closed(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    broker.allocate(_request(1), now=NOW)

    with pytest.raises(
        leases.LeaseBrokerError,
        match="replayed with different inputs",
    ):
        broker.allocate(_request(1, head_sha="b" * 40), now=NOW)


def test_release_makes_oldlab_slot_the_next_assignment(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    assignments = [broker.allocate(_request(index), now=NOW) for index in range(6)]
    assert assignments[-1].target is leases.PlacementTarget.GITHUB_HOSTED

    released = broker.release(
        assignment_id=assignments[2].assignment_id,
        lease_epoch=assignments[2].lease_epoch,
        reason="completed",
        terminal_observed=True,
        now=NOW + timedelta(minutes=2),
    )
    next_assignment = broker.allocate(
        _request(7, workflow_run_id=10_001),
        now=NOW + timedelta(minutes=2, seconds=1),
    )

    assert released.state is leases.AssignmentState.RELEASED
    assert next_assignment.target is leases.PlacementTarget.OLDLAB
    assert next_assignment.slot == 2


def test_release_requires_exact_epoch_and_terminal_observation(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    assignment = broker.allocate(_request(1), now=NOW)

    with pytest.raises(leases.LeaseBrokerError, match="terminal observation"):
        broker.release(
            assignment_id=assignment.assignment_id,
            lease_epoch=assignment.lease_epoch,
            reason="cancelled",
            terminal_observed=False,
            now=NOW,
        )
    with pytest.raises(leases.LeaseBrokerError, match="stale lease epoch"):
        broker.release(
            assignment_id=assignment.assignment_id,
            lease_epoch=assignment.lease_epoch + 1,
            reason="cancelled",
            terminal_observed=True,
            now=NOW,
        )


def test_release_is_idempotent_but_reason_cannot_change(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    assignment = broker.allocate(_request(1), now=NOW)
    first = broker.release(
        assignment_id=assignment.assignment_id,
        lease_epoch=assignment.lease_epoch,
        reason="cancelled",
        terminal_observed=True,
        now=NOW,
    )
    replay = broker.release(
        assignment_id=assignment.assignment_id,
        lease_epoch=assignment.lease_epoch,
        reason="cancelled",
        terminal_observed=True,
        now=NOW + timedelta(minutes=1),
    )
    assert replay == first

    with pytest.raises(leases.LeaseBrokerError, match="another reason"):
        broker.release(
            assignment_id=assignment.assignment_id,
            lease_epoch=assignment.lease_epoch,
            reason="completed",
            terminal_observed=True,
            now=NOW,
        )


def test_expired_clock_does_not_reuse_slot_without_terminal_evidence(
    tmp_path: Path,
) -> None:
    broker = _broker(tmp_path)
    assignments = [broker.allocate(_request(index, ttl_seconds=60), now=NOW) for index in range(5)]

    status = broker.status(now=NOW + timedelta(minutes=2))
    overflow = broker.allocate(
        _request(6, workflow_run_id=10_001),
        now=NOW + timedelta(minutes=2),
    )

    normal = status["classes"]["normal"]
    assert normal["overdue_oldlab_assignments"] == 5
    assert normal["available"] == 0
    assert overflow.target is leases.PlacementTarget.GITHUB_HOSTED

    broker.release(
        assignment_id=assignments[0].assignment_id,
        lease_epoch=assignments[0].lease_epoch,
        reason="expired",
        terminal_observed=True,
        now=NOW + timedelta(minutes=2),
    )
    recovered = broker.allocate(
        _request(7, workflow_run_id=10_002),
        now=NOW + timedelta(minutes=2, seconds=1),
    )
    assert recovered.target is leases.PlacementTarget.OLDLAB


def test_restart_preserves_assignments_and_capacity(tmp_path: Path) -> None:
    state_db = tmp_path / "leases.sqlite3"
    first_broker = leases.CiRunnerLeaseBroker(state_db, _config())
    first = [first_broker.allocate(_request(index), now=NOW) for index in range(3)]

    restarted = leases.CiRunnerLeaseBroker(state_db, _config())
    later = [restarted.allocate(_request(index), now=NOW) for index in range(3, 7)]

    assert all(item.target is leases.PlacementTarget.OLDLAB for item in first)
    assert [item.target for item in later] == [
        leases.PlacementTarget.OLDLAB,
        leases.PlacementTarget.OLDLAB,
        leases.PlacementTarget.GITHUB_HOSTED,
        leases.PlacementTarget.GITHUB_HOSTED,
    ]
    assert state_db.stat().st_mode & 0o777 == 0o600


def test_non_5_4_2_capacity_is_rejected() -> None:
    config = leases.LeaseBrokerConfig(
        repository="qianyi-sun/loom",
        oldlab_labels=_config().oldlab_labels,
        capacities={"normal": 4, "image": 5, "smoke": 2},
    )

    with pytest.raises(leases.LeaseBrokerError, match="exactly 5/4/2"):
        config.validate()


def test_symlink_state_database_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "target.sqlite3"
    sqlite3.connect(target).close()
    link = tmp_path / "leases.sqlite3"
    link.symlink_to(target)

    with pytest.raises(leases.LeaseBrokerError, match="must not be a symlink"):
        leases.CiRunnerLeaseBroker(link, _config())


def test_cli_allocates_and_reports_status(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    state_db = tmp_path / "leases.sqlite3"
    request_file = tmp_path / "request.json"
    request_file.write_text(
        json.dumps(
            {
                "repository": "qianyi-sun/loom",
                "workflow_run_id": 123,
                "run_attempt": 1,
                "job_key": "tests-root-1-of-2",
                "head_sha": HEAD_SHA,
                "work_class": "normal",
                "lease_ttl_seconds": 300,
            }
        ),
        encoding="utf-8",
    )

    assert (
        leases.main(
            [
                "--state-db",
                str(state_db),
                "--profile",
                str(repo_root / "deploy/ci-runners/oldlab5.toml"),
                "allocate",
                "--request-file",
                str(request_file),
            ]
        )
        == 0
    )
    allocation = json.loads(capsys.readouterr().out)
    assert allocation["target"] == "oldlab"
    assert allocation["runs_on"][-1] == "loom-ci-normal"

    assert (
        leases.main(
            [
                "--state-db",
                str(state_db),
                "--profile",
                str(repo_root / "deploy/ci-runners/oldlab5.toml"),
                "status",
            ]
        )
        == 0
    )
    status = json.loads(capsys.readouterr().out)
    assert status["classes"]["normal"]["oldlab_assigned"] == 1


def test_cli_allocates_an_atomic_route_document(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    state_db = tmp_path / "leases.sqlite3"
    request_file = tmp_path / "route-request.json"
    request_file.write_text(
        json.dumps(_route_request(job_count=7).public_dict()),
        encoding="utf-8",
    )

    assert (
        leases.main(
            [
                "--state-db",
                str(state_db),
                "--profile",
                str(repo_root / "deploy/ci-runners/oldlab5.toml"),
                "allocate-route",
                "--request-file",
                str(request_file),
            ]
        )
        == 0
    )
    document = json.loads(capsys.readouterr().out)
    assert document["schema_version"] == 1
    assert document["workflow_name"] == "CI"
    assert len(document["assignments"]) == 7
    assert [item["target"] for item in document["assignments"]].count("oldlab") == 5
    assert [item["target"] for item in document["assignments"]].count("github_hosted") == 2
