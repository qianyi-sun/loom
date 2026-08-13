from __future__ import annotations

import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest
from scripts.ops.global_dev_fleet_autoscaler_external_once import (
    GlobalDevExternalError,
    InstanceSnapshot,
    _atomic_write,
    _main,
    _parser,
    _prune_worker_env_files,
    _read_secret_file,
    _safe_env_value,
    _validate_owner_only_directory,
    _worker_env_is_current,
)

from loom_control_plane.worker_pool_autoscaler import AutoscalerObservation


def _snapshot(*, status: str = "ready") -> InstanceSnapshot:
    return InstanceSnapshot(
        name="alice",
        environment="dev-alice",
        pool_name="dev-alice",
        database="loom_dev_alice",
        deployment_generation=4,
        candidate_sha="a" * 40,
        operation_epoch=9,
        status=status,
        min_slots=0,
        max_slots=8,
        observation=AutoscalerObservation(
            active_slots=2,
            pending_slots=1,
            draining_slots=0,
            occupied_slots=2,
            queued_slots=20,
            idle_worker_ids=(),
            drained_worker_ids=(),
        ),
        terminal_slots=7,
        actuator_config={"external_runner": True},
    )


def test_ready_demand_is_queue_driven_and_policy_bounded() -> None:
    demand = _snapshot().demand(datetime(2026, 8, 6, tzinfo=UTC))
    assert demand.environment == "dev-alice"
    assert demand.requested_slots == 8
    assert demand.deployment_generation == 4


def test_non_ready_instance_publishes_zero_for_drain() -> None:
    demand = _snapshot(status="deleting").demand(datetime(2026, 8, 6, tzinfo=UTC))
    assert demand.min_slots == 0
    assert demand.requested_slots == 0


def test_secret_inputs_and_outputs_are_owner_only(tmp_path: Path) -> None:
    secret = tmp_path / "db-url"
    secret.write_text("postgresql://admin:password@db/postgres\n", encoding="utf-8")
    secret.chmod(0o600)
    assert _read_secret_file(secret, "fixture") == ("postgresql://admin:password@db/postgres")

    secret.chmod(0o640)
    with pytest.raises(GlobalDevExternalError, match="owner-only"):
        _read_secret_file(secret, "fixture")

    output = tmp_path / "report.json"
    _atomic_write(output, "{}\n")
    assert stat.S_IMODE(output.stat().st_mode) == 0o600


def test_worker_environment_directory_must_be_owner_only(tmp_path: Path) -> None:
    directory = tmp_path / "worker-env"
    directory.mkdir(mode=0o700)
    _validate_owner_only_directory(directory, "worker environment directory")

    directory.chmod(0o750)
    with pytest.raises(GlobalDevExternalError, match="owner-only"):
        _validate_owner_only_directory(directory, "worker environment directory")


def test_worker_env_rejects_multiline_or_empty_values() -> None:
    assert _safe_env_value("https://cp.example") == "https://cp.example"
    with pytest.raises(GlobalDevExternalError):
        _safe_env_value("token\nINJECTED=value")
    with pytest.raises(GlobalDevExternalError):
        _safe_env_value("")


def test_worker_env_reuse_requires_generation_candidate_and_image(tmp_path: Path) -> None:
    path = tmp_path / "dev-alice.env"
    path.write_text(
        "LOOM_DEV_DEPLOYMENT_GENERATION=4\n"
        "LOOM_DEV_OPERATION_EPOCH=9\n"
        f"LOOM_WORKER_CANDIDATE_SHA={'b' * 40}\n"
        "LOOM_IMAGE_TAG=dev-aaaaaaa\n",
        encoding="utf-8",
    )
    path.chmod(0o600)
    assert not _worker_env_is_current(path, _snapshot(), image_tag="dev-aaaaaaa")
    path.write_text(
        "LOOM_DEV_DEPLOYMENT_GENERATION=4\n"
        "LOOM_DEV_OPERATION_EPOCH=9\n"
        f"LOOM_WORKER_CANDIDATE_SHA={'a' * 40}\n"
        "LOOM_IMAGE_TAG=dev-aaaaaaa\n",
        encoding="utf-8",
    )
    assert _worker_env_is_current(path, _snapshot(), image_tag="dev-aaaaaaa")

    path.write_text(
        "LOOM_DEV_DEPLOYMENT_GENERATION=4\n"
        "LOOM_DEV_OPERATION_EPOCH=8\n"
        f"LOOM_WORKER_CANDIDATE_SHA={'a' * 40}\n"
        "LOOM_IMAGE_TAG=dev-aaaaaaa\n",
        encoding="utf-8",
    )
    assert not _worker_env_is_current(path, _snapshot(), image_tag="dev-aaaaaaa")


def test_worker_env_cleanup_is_registry_driven_and_narrow(tmp_path: Path) -> None:
    active = tmp_path / "dev-alice.env"
    stale = tmp_path / "dev-bob.env"
    unrelated = tmp_path / "production.env"
    for path in (active, stale, unrelated):
        path.write_text("credential\n", encoding="utf-8")
        path.chmod(0o600)

    assert _prune_worker_env_files(tmp_path, {"dev-alice"}) == 1
    assert active.exists()
    assert not stale.exists()
    assert unrelated.exists()


def test_parser_accepts_the_manager_execution_witness(tmp_path: Path) -> None:
    witness = tmp_path / "manager-witness.json"
    args = _parser().parse_args(
        [
            "--management-db-url-file",
            "/run/loom/management-db-url",
            "--fixture-admin-db-url-file",
            "/run/loom/fixture-admin-db-url",
            "--state-db",
            "/var/lib/loom/global-capacity.sqlite3",
            "--output-json",
            "/var/lib/loom/global-capacity.json",
            "--global-budget",
            "0",
            "--worker-env-dir",
            "/run/loom/worker-env",
            "--worker-minio-endpoint",
            "https://minio.example",
            "--image-tag",
            "dev-aaaaaaaa",
            "--global-execution-witness-json",
            str(witness),
        ]
    )

    assert args.global_execution_witness_json == witness


@pytest.mark.asyncio
async def test_missing_witness_skips_the_legacy_broker_and_reports_a_fenced_tick(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protected = tmp_path / "protected"
    protected.mkdir(mode=0o700)
    protected.chmod(0o700)
    management = protected / "management-db-url"
    admin = protected / "admin-db-url"
    for path in (management, admin):
        path.write_text("postgresql://loom:secret@db/loom\n", encoding="utf-8")
        path.chmod(0o600)
    worker_env = protected / "worker-env"
    worker_env.mkdir(mode=0o700)
    worker_env.chmod(0o700)

    class _Broker:
        def __init__(self, _path: Path) -> None:
            pass

        def status(self) -> object:
            raise AssertionError("missing witness must fence before broker mutation")

    async def _no_rows(_url: str) -> list[object]:
        return []

    monkeypatch.setattr(
        "scripts.ops.global_dev_fleet_autoscaler_external_once._registry_rows",
        _no_rows,
    )
    monkeypatch.setattr(
        "scripts.ops.global_dev_fleet_autoscaler_external_once.SharedCapacityBroker",
        _Broker,
    )
    args = _parser().parse_args(
        [
            "--management-db-url-file",
            str(management),
            "--fixture-admin-db-url-file",
            str(admin),
            "--state-db",
            str(protected / "state.sqlite3"),
            "--output-json",
            str(protected / "report.json"),
            "--global-budget",
            "0",
            "--worker-env-dir",
            str(worker_env),
            "--worker-minio-endpoint",
            "https://minio.example",
            "--image-tag",
            "dev-aaaaaaaa",
        ]
    )

    result = await _main(args)

    assert result == {
        "authority": "global-dev-fleet-autoscaler",
        "aggregate": {"legacy_scale_up_fenced": True},
        "instances": 0,
    }
