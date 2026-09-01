from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pytest

from loom import data_lifecycle_maintenance
from loom.data_lifecycle_operator import OperatorAction


@pytest.fixture(autouse=True)
def _acquired_mutation_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    @contextmanager
    def acquired(_engine: object) -> Iterator[bool]:
        yield True

    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "hold_staging_mutation_guard",
        acquired,
    )


def test_resume_action_uses_exact_authority_without_capacity_inventory(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    engine = Mock()
    client = Mock()
    journal = Mock()
    deleter = Mock()
    object_inventory = Mock()
    runtime = SimpleNamespace(database=object(), object_store=object())
    run_gc = Mock(return_value={"status": "completed"})
    capacity_store = Mock()
    collect_capacity = Mock()

    monkeypatch.setattr(
        data_lifecycle_maintenance, "load_lifecycle_runtime", Mock(return_value=runtime)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance, "build_lifecycle_engine", Mock(return_value=engine)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "build_lifecycle_object_store_client",
        Mock(return_value=client),
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "S3ObservedObjectInventory",
        Mock(return_value=object_inventory),
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance, "SqlAlchemyGcJournal", Mock(return_value=journal)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance, "S3ExactObjectDeleter", Mock(return_value=deleter)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "SqlAlchemyStagingCapacityStore",
        Mock(return_value=capacity_store),
    )
    monkeypatch.setattr(data_lifecycle_maintenance, "collect_staging_capacity", collect_capacity)
    monkeypatch.setattr(data_lifecycle_maintenance, "run_lifecycle_operator", run_gc)

    assert (
        data_lifecycle_maintenance.main(
            [
                "--action",
                "resume",
                "--namespace",
                "loom-staging",
                "--bucket",
                "loom-staging-artifacts",
                "--requested-by",
                "staging-lifecycle-recovery",
                "--resume-run-id",
                "54e079e0-c321-41b8-a0c0-34601e23dc42",
                "--request-id",
                "req-gc-resume-live",
            ]
        )
        == 0
    )

    request = run_gc.call_args.kwargs["request"]
    assert request.action is OperatorAction.RESUME
    assert request.requested_by == "staging-lifecycle-recovery"
    assert request.resume_run_id == UUID("54e079e0-c321-41b8-a0c0-34601e23dc42")
    assert request.request_id == "req-gc-resume-live"
    assert run_gc.call_args.kwargs["journal"] is journal
    assert run_gc.call_args.kwargs["object_deleter"] is deleter
    assert run_gc.call_args.kwargs["batch_size"] == 1000
    object_inventory.load.assert_not_called()
    collect_capacity.assert_not_called()
    capacity_store.publish.assert_not_called()
    output = json.loads(capsys.readouterr().out)
    assert output == {
        "action": "resume",
        "capacity": None,
        "gc": {"status": "completed"},
        "schema_version": 1,
    }
    client.close.assert_called_once_with()
    engine.dispose.assert_called_once_with()


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (
            [
                "--action",
                "resume",
                "--namespace",
                "loom-staging",
                "--bucket",
                "loom-staging-artifacts",
            ],
            "resume requires",
        ),
        (
            [
                "--action",
                "auto",
                "--namespace",
                "loom-staging",
                "--bucket",
                "loom-staging-artifacts",
                "--resume-run-id",
                "54e079e0-c321-41b8-a0c0-34601e23dc42",
                "--request-id",
                "req-gc-resume-live",
            ],
            "do not accept resume authority",
        ),
    ],
)
def test_resume_authority_is_explicit_and_action_scoped(argv: list[str], message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        data_lifecycle_maintenance.main(argv)


@pytest.mark.parametrize(
    ("action", "expected_gc_calls"),
    [("capacity", 0), ("auto", 1)],
)
def test_maintenance_action_separates_capacity_from_gc(
    action: str,
    expected_gc_calls: int,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    engine = Mock()
    client = Mock()
    runtime = SimpleNamespace(database=object(), object_store=object())
    observed_at = datetime(2026, 7, 20, tzinfo=UTC)
    capacity = SimpleNamespace(
        capacity=SimpleNamespace(
            admission_allowed=True,
            bytes_used=7,
            disk_free_percent=42,
            gc_required=False,
            inode_free_percent=51,
            object_count=1,
        ),
        evidence_sha256="evidence",
        observed_at=observed_at,
        policy_sha256="policy",
    )
    observed = (object(),)
    object_inventory = Mock()
    object_inventory.load.return_value = observed
    capacity_store = Mock()
    run_gc = Mock(return_value={"status": "complete"})

    monkeypatch.setattr(
        data_lifecycle_maintenance, "load_lifecycle_runtime", Mock(return_value=runtime)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance, "build_lifecycle_engine", Mock(return_value=engine)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "build_lifecycle_object_store_client",
        Mock(return_value=client),
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "S3ObservedObjectInventory",
        Mock(return_value=object_inventory),
    )
    collect_capacity = Mock(return_value=capacity)
    monkeypatch.setattr(data_lifecycle_maintenance, "collect_staging_capacity", collect_capacity)
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "SqlAlchemyStagingCapacityStore",
        Mock(return_value=capacity_store),
    )
    monkeypatch.setattr(data_lifecycle_maintenance, "run_lifecycle_operator", run_gc)

    assert (
        data_lifecycle_maintenance.main(
            [
                "--action",
                action,
                "--namespace",
                "loom-staging",
                "--bucket",
                "loom-staging-artifacts",
                "--filesystem-path",
                "/data",
            ]
        )
        == 0
    )

    collect_capacity.assert_called_once_with(
        namespace="loom-staging",
        objects=observed,
        filesystem_paths=[data_lifecycle_maintenance.Path("/data")],
        observed_at=collect_capacity.call_args.kwargs["observed_at"],
    )
    capacity_store.publish.assert_called_once_with(capacity)
    assert run_gc.call_count == expected_gc_calls
    output = json.loads(capsys.readouterr().out)
    assert output["action"] == action
    assert output["gc"] == ({"status": "complete"} if action == "auto" else None)
    client.close.assert_called_once_with()
    engine.dispose.assert_called_once_with()


def test_minio_admin_capacity_source_probes_over_network(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    object_store = SimpleNamespace(
        endpoint_url="http://loom-minio:9000",
        access_key="ak",
        secret_key="sk",
    )
    runtime = SimpleNamespace(database=object(), object_store=object_store)
    observed_at = datetime(2026, 7, 20, tzinfo=UTC)
    capacity = SimpleNamespace(
        capacity=SimpleNamespace(
            admission_allowed=True,
            bytes_used=7,
            disk_free_percent=42,
            gc_required=False,
            inode_free_percent=51,
            object_count=1,
        ),
        evidence_sha256="evidence",
        observed_at=observed_at,
        policy_sha256="policy",
    )
    observed = (object(),)
    drives = [object(), object()]
    object_inventory = Mock()
    object_inventory.load.return_value = observed
    capacity_store = Mock()
    probe = Mock(return_value=drives)
    from_drives = Mock(return_value=capacity)
    filesystem_collector = Mock()

    monkeypatch.setattr(
        data_lifecycle_maintenance, "load_lifecycle_runtime", Mock(return_value=runtime)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance, "build_lifecycle_engine", Mock(return_value=Mock())
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "build_lifecycle_object_store_client",
        Mock(return_value=Mock()),
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "S3ObservedObjectInventory",
        Mock(return_value=object_inventory),
    )
    monkeypatch.setattr(data_lifecycle_maintenance, "probe_minio_admin_drives", probe)
    monkeypatch.setattr(
        data_lifecycle_maintenance, "collect_staging_capacity_from_drives", from_drives
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance, "collect_staging_capacity", filesystem_collector
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "SqlAlchemyStagingCapacityStore",
        Mock(return_value=capacity_store),
    )
    monkeypatch.setattr(data_lifecycle_maintenance, "run_lifecycle_operator", Mock(return_value={}))

    assert (
        data_lifecycle_maintenance.main(
            [
                "--action",
                "capacity",
                "--namespace",
                "loom-staging",
                "--bucket",
                "loom-staging-artifacts",
                "--capacity-source",
                "minio-admin",
                "--expected-drive-count",
                "2",
            ]
        )
        == 0
    )

    probe.assert_called_once_with(
        endpoint_url="http://loom-minio:9000",
        access_key="ak",
        secret_key="sk",
        expected_drive_count=2,
    )
    from_drives.assert_called_once_with(
        namespace="loom-staging",
        objects=observed,
        drives=drives,
        observed_at=from_drives.call_args.kwargs["observed_at"],
    )
    filesystem_collector.assert_not_called()
    capacity_store.publish.assert_called_once_with(capacity)


def test_filesystem_source_requires_a_path(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = SimpleNamespace(database=object(), object_store=object())
    object_inventory = Mock()
    object_inventory.load.return_value = ()
    monkeypatch.setattr(
        data_lifecycle_maintenance, "load_lifecycle_runtime", Mock(return_value=runtime)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance, "build_lifecycle_engine", Mock(return_value=Mock())
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "build_lifecycle_object_store_client",
        Mock(return_value=Mock()),
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "S3ObservedObjectInventory",
        Mock(return_value=object_inventory),
    )
    with pytest.raises(RuntimeError, match="requires at least one --filesystem-path"):
        data_lifecycle_maintenance.main(
            [
                "--namespace",
                "loom-staging",
                "--bucket",
                "loom-staging-artifacts",
            ]
        )


def test_minio_admin_source_requires_positive_expected_drive_count() -> None:
    with pytest.raises(RuntimeError, match="positive --expected-drive-count"):
        data_lifecycle_maintenance.main(
            [
                "--namespace",
                "loom-staging",
                "--bucket",
                "loom-staging-artifacts",
                "--capacity-source",
                "minio-admin",
            ]
        )


def test_filesystem_source_rejects_expected_drive_count() -> None:
    with pytest.raises(RuntimeError, match="cannot use --expected-drive-count"):
        data_lifecycle_maintenance.main(
            [
                "--namespace",
                "loom-staging",
                "--bucket",
                "loom-staging-artifacts",
                "--expected-drive-count",
                "1",
            ]
        )


@pytest.mark.parametrize("action", ["auto", "capacity", "resume"])
def test_guard_denial_is_a_successful_noop_before_object_store_or_inventory(
    action: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime = SimpleNamespace(database=object(), object_store=object())
    engine = Mock()
    client_builder = Mock(return_value=Mock())
    inventory_builder = Mock(return_value=Mock())
    capacity_store_builder = Mock(return_value=Mock())
    collect_capacity = Mock()
    run_gc = Mock()

    @contextmanager
    def denied(_engine: object) -> Iterator[bool]:
        yield False

    monkeypatch.setattr(
        data_lifecycle_maintenance, "load_lifecycle_runtime", Mock(return_value=runtime)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance, "build_lifecycle_engine", Mock(return_value=engine)
    )
    monkeypatch.setattr(data_lifecycle_maintenance, "hold_staging_mutation_guard", denied)
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "build_lifecycle_object_store_client",
        client_builder,
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "S3ObservedObjectInventory",
        inventory_builder,
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "SqlAlchemyStagingCapacityStore",
        capacity_store_builder,
    )
    monkeypatch.setattr(data_lifecycle_maintenance, "collect_staging_capacity", collect_capacity)
    monkeypatch.setattr(data_lifecycle_maintenance, "run_lifecycle_operator", run_gc)

    argv = [
        "--action",
        action,
        "--namespace",
        "loom-staging",
        "--bucket",
        "loom-staging-artifacts",
    ]
    if action == "resume":
        argv.extend(
            (
                "--resume-run-id",
                "54e079e0-c321-41b8-a0c0-34601e23dc42",
                "--request-id",
                "req-gc-resume-live",
            )
        )
    else:
        argv.extend(("--filesystem-path", "/data"))

    assert data_lifecycle_maintenance.main(argv) == 0

    assert json.loads(capsys.readouterr().out) == {
        "action": action,
        "capacity": None,
        "coordination": {"status": "rollout_guard_active"},
        "gc": None,
        "schema_version": 1,
    }
    client_builder.assert_not_called()
    inventory_builder.assert_not_called()
    capacity_store_builder.assert_not_called()
    collect_capacity.assert_not_called()
    run_gc.assert_not_called()
    engine.dispose.assert_called_once_with()


@pytest.mark.parametrize("action", ["auto", "capacity", "resume"])
def test_acquired_guard_spans_action_failure_and_client_cleanup(
    action: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = SimpleNamespace(database=object(), object_store=object())
    engine = Mock()
    client = Mock()
    object_inventory = Mock()
    operation_failure = RuntimeError(f"{action} operation failed")
    if action == "resume":
        run_gc = Mock(side_effect=operation_failure)
    else:
        object_inventory.load.side_effect = operation_failure
        run_gc = Mock()
    guard_events: list[str] = []

    @contextmanager
    def tracking_guard(observed_engine: object) -> Iterator[bool]:
        assert observed_engine is engine
        guard_events.append("entered")
        try:
            yield True
        finally:
            assert client.close.call_count == 1
            guard_events.append("released")

    monkeypatch.setattr(
        data_lifecycle_maintenance, "load_lifecycle_runtime", Mock(return_value=runtime)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance, "build_lifecycle_engine", Mock(return_value=engine)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "build_lifecycle_object_store_client",
        Mock(return_value=client),
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "S3ObservedObjectInventory",
        Mock(return_value=object_inventory),
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "hold_staging_mutation_guard",
        tracking_guard,
    )
    monkeypatch.setattr(data_lifecycle_maintenance, "run_lifecycle_operator", run_gc)

    argv = [
        "--action",
        action,
        "--namespace",
        "loom-staging",
        "--bucket",
        "loom-staging-artifacts",
    ]
    if action == "resume":
        argv.extend(
            (
                "--resume-run-id",
                "54e079e0-c321-41b8-a0c0-34601e23dc42",
                "--request-id",
                "req-gc-resume-live",
            )
        )
    else:
        argv.extend(("--filesystem-path", "/data"))

    with pytest.raises(RuntimeError, match=f"{action} operation failed"):
        data_lifecycle_maintenance.main(argv)

    assert guard_events == ["entered", "released"]
    client.close.assert_called_once_with()
    engine.dispose.assert_called_once_with()


def test_rollout_capacity_uses_exact_active_guard_without_running_gc(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    engine = Mock()
    client = Mock()
    runtime = SimpleNamespace(database=object(), object_store=object())
    capacity = SimpleNamespace(
        capacity=SimpleNamespace(
            admission_allowed=True,
            bytes_used=7,
            disk_free_percent=42,
            gc_required=False,
            inode_free_percent=51,
            object_count=1,
        ),
        evidence_sha256="evidence",
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
        policy_sha256="policy",
    )
    object_inventory = Mock()
    object_inventory.load.return_value = (object(),)
    capacity_store = Mock()
    run_gc = Mock()
    authority_events: list[str] = []

    @contextmanager
    def protected_authority(observed_engine: object, authority: object) -> Iterator[None]:
        assert observed_engine is engine
        assert {
            name: getattr(authority, name)
            for name in (
                "candidate_sha",
                "candidate_tree",
                "generation",
                "guard_backend_pid",
                "mutation_epoch",
                "plan_digest",
                "request_id",
            )
        } == {
            "candidate_sha": "a" * 40,
            "candidate_tree": "b" * 40,
            "generation": "1" * 32,
            "guard_backend_pid": 4321,
            "mutation_epoch": 9,
            "plan_digest": "f" * 64,
            "request_id": "req-1111111111111111",
        }
        authority_events.append("entered")
        try:
            yield
        finally:
            assert capacity_store.publish.call_count == 1
            authority_events.append("verified")

    monkeypatch.setattr(
        data_lifecycle_maintenance, "load_lifecycle_runtime", Mock(return_value=runtime)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance, "build_lifecycle_engine", Mock(return_value=engine)
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "build_lifecycle_object_store_client",
        Mock(return_value=client),
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "S3ObservedObjectInventory",
        Mock(return_value=object_inventory),
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "collect_staging_capacity",
        Mock(return_value=capacity),
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "SqlAlchemyStagingCapacityStore",
        Mock(return_value=capacity_store),
    )
    monkeypatch.setattr(data_lifecycle_maintenance, "run_lifecycle_operator", run_gc)
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "hold_staging_mutation_guard",
        Mock(side_effect=AssertionError("rollout refresh must not acquire the guarded lock")),
    )
    monkeypatch.setattr(
        data_lifecycle_maintenance,
        "hold_rollout_capacity_authority",
        protected_authority,
        raising=False,
    )

    try:
        result = data_lifecycle_maintenance.main(
            [
                "--action",
                "rollout-capacity",
                "--namespace",
                "loom-staging",
                "--bucket",
                "loom-staging-artifacts",
                "--filesystem-path",
                "/data",
                "--rollout-request-id",
                "req-1111111111111111",
                "--rollout-plan-digest",
                "f" * 64,
                "--rollout-candidate-sha",
                "a" * 40,
                "--rollout-candidate-tree",
                "b" * 40,
                "--rollout-guard-generation",
                "1" * 32,
                "--rollout-guard-backend-pid",
                "4321",
                "--rollout-mutation-epoch",
                "9",
            ]
        )
    except SystemExit:
        pytest.fail("rollout capacity action is unavailable")

    assert result == 0
    assert authority_events == ["entered", "verified"]
    assert json.loads(capsys.readouterr().out)["action"] == "rollout-capacity"
    run_gc.assert_not_called()
    client.close.assert_called_once_with()
    engine.dispose.assert_called_once_with()


@pytest.mark.parametrize(
    "missing_option",
    [
        "--rollout-request-id",
        "--rollout-plan-digest",
        "--rollout-candidate-sha",
        "--rollout-candidate-tree",
        "--rollout-guard-generation",
        "--rollout-guard-backend-pid",
        "--rollout-mutation-epoch",
    ],
)
def test_rollout_capacity_rejects_incomplete_guard_authority(missing_option: str) -> None:
    option_values = {
        "--rollout-request-id": "req-1111111111111111",
        "--rollout-plan-digest": "f" * 64,
        "--rollout-candidate-sha": "a" * 40,
        "--rollout-candidate-tree": "b" * 40,
        "--rollout-guard-generation": "1" * 32,
        "--rollout-guard-backend-pid": "4321",
        "--rollout-mutation-epoch": "9",
    }
    argv = [
        "--action",
        "rollout-capacity",
        "--namespace",
        "loom-staging",
        "--bucket",
        "loom-staging-artifacts",
        "--filesystem-path",
        "/data",
    ]
    for option, value in option_values.items():
        if option != missing_option:
            argv.extend((option, value))

    with pytest.raises(RuntimeError, match="requires exact rollout authority"):
        data_lifecycle_maintenance.main(argv)


def test_ordinary_capacity_rejects_rollout_guard_authority() -> None:
    with pytest.raises(RuntimeError, match="does not accept rollout authority"):
        data_lifecycle_maintenance.main(
            [
                "--action",
                "capacity",
                "--namespace",
                "loom-staging",
                "--bucket",
                "loom-staging-artifacts",
                "--filesystem-path",
                "/data",
                "--rollout-request-id",
                "req-1111111111111111",
            ]
        )


def test_rollout_capacity_rejects_non_staging_namespace() -> None:
    with pytest.raises(RuntimeError, match="requires the loom-staging namespace"):
        data_lifecycle_maintenance.main(
            [
                "--action",
                "rollout-capacity",
                "--namespace",
                "different-namespace",
                "--bucket",
                "loom-staging-artifacts",
                "--filesystem-path",
                "/data",
                "--rollout-request-id",
                "req-1111111111111111",
                "--rollout-plan-digest",
                "f" * 64,
                "--rollout-candidate-sha",
                "a" * 40,
                "--rollout-candidate-tree",
                "b" * 40,
                "--rollout-guard-generation",
                "1" * 32,
                "--rollout-guard-backend-pid",
                "4321",
                "--rollout-mutation-epoch",
                "9",
            ]
        )
