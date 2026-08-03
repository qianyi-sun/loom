from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from loom import data_lifecycle_maintenance


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
    monkeypatch.setattr(
        data_lifecycle_maintenance, "run_lifecycle_operator", Mock(return_value={})
    )

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
            ]
        )
        == 0
    )

    probe.assert_called_once_with(
        endpoint_url="http://loom-minio:9000",
        access_key="ak",
        secret_key="sk",
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
