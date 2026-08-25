from __future__ import annotations

from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from loom.errors import ConfigError
from loom.models.result import FailureReason
from loom.models.task import TaskConfig
from loom_drivers.daytona.config import DaytonaConfig
from loom_drivers.daytona.driver import DaytonaDriver
from loom_drivers.daytona.service_controller import DaytonaApiGate, provider_scope
from loom_worker.config import WorkerSettings
from loom_worker.main_loop import (
    _build_daytona_runtime,
    _classify_setup_failure,
    _daytona_sandbox_name,
    _resolve_daytona_trial_image,
    _worker_capabilities,
)
from loom_worker.task_image import TaskImageBuildError


def _settings(**overrides: object) -> WorkerSettings:
    values: dict[str, object] = {
        "token": "worker-token",
        "minio_access_key": "access",
        "minio_secret_key": "secret",
        "sandbox_backend": "daytona",
        "candidate_sha": "a" * 40,
    }
    values.update(overrides)
    return WorkerSettings(_env_file=None, **values)


def _task(image: str) -> TaskConfig:
    return TaskConfig.model_validate(
        {
            "schema_version": "1",
            "task": {"id": "daytona/test", "name": "daytona test"},
            "environment": {"os": "linux", "docker_image": image},
            "agent": {"name": "oracle"},
            "verifier": {"name": "pytest"},
        }
    )


def test_worker_backend_defaults_to_docker() -> None:
    settings = _settings(sandbox_backend="docker", candidate_sha="")
    assert settings.sandbox_backend == "docker"
    assert _worker_capabilities(settings)[0]["backend"] == "docker"


def test_daytona_registration_is_explicit_and_remote_x86() -> None:
    caps = _worker_capabilities(_settings())[0]
    assert caps["backend"] == "daytona"
    assert caps["cpu_arch"] == "x86_64"
    assert caps["resource_modes"] == ["limit"]


def test_daytona_credentials_fail_before_runtime_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DAYTONA_API_KEY", raising=False)
    monkeypatch.delenv("DAYTONA_JWT_TOKEN", raising=False)
    with pytest.raises(ConfigError, match="DAYTONA_API_KEY"):
        _build_daytona_runtime(_settings())


def test_daytona_rejects_mutable_candidate_and_docker_local_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    with pytest.raises(RuntimeError, match="exact lowercase"):
        _build_daytona_runtime(_settings(candidate_sha="dev"))
    with pytest.raises(RuntimeError, match="Docker/Slurm-local"):
        _build_daytona_runtime(_settings(sandbox_isolation=True))


def test_daytona_image_resolution_accepts_only_digest() -> None:
    immutable = "registry.example/loom/task@sha256:" + "b" * 64
    assert (
        _resolve_daytona_trial_image(
            task_config=_task(immutable),
            materialization=None,
        )
        == immutable
    )
    with pytest.raises(TaskImageBuildError, match="mutable image aliases"):
        _resolve_daytona_trial_image(
            task_config=_task("python:3.12-slim"),
            materialization=None,
        )


def test_daytona_sandbox_name_is_candidate_and_attempt_bound() -> None:
    trial_id = uuid4()
    name = _daytona_sandbox_name(
        trial_id=trial_id,
        attempt_count=2,
        candidate_sha="c" * 40,
    )
    assert name == f"loom-{trial_id.hex}-2-cccccccc"
    assert len(name) <= 63


def test_daytona_backpressure_failure_reasons_are_structured() -> None:
    assert (
        _classify_setup_failure("DAYTONA_RATE_LIMITED: provider throttled")
        == FailureReason.DAYTONA_RATE_LIMITED
    )
    assert (
        _classify_setup_failure("DAYTONA_CAPACITY_UNAVAILABLE: pool exhausted")
        == FailureReason.DAYTONA_CAPACITY_UNAVAILABLE
    )


async def test_managed_driver_disables_provider_idle_lifecycle_and_reports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    config = DaytonaConfig.from_env()
    sdk = MagicMock()
    from daytona import DaytonaError

    sdk.get = AsyncMock(side_effect=DaytonaError("missing", status_code=404))
    sandbox = MagicMock(id="sandbox-1")
    sdk.create = AsyncMock(return_value=sandbox)
    sdk.delete = AsyncMock()
    sdk.close = AsyncMock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona",
        lambda _: sdk,
    )
    ledger_id = uuid4()
    started: list[tuple[UUID, str, datetime]] = []
    deleted: list[tuple[UUID, bool, str | None]] = []

    async def reserve() -> dict[str, Any]:
        return {"id": str(ledger_id), "sandbox_id": None}

    async def mark_started(
        got_ledger_id: UUID,
        sandbox_id: str,
        started_at: datetime,
    ) -> None:
        started.append((got_ledger_id, sandbox_id, started_at))

    async def mark_deleted(
        got_ledger_id: UUID,
        succeeded: bool,
        stopped_at: datetime,
        error: str | None,
    ) -> None:
        deleted.append((got_ledger_id, succeeded, error))

    driver = DaytonaDriver(
        image="registry.example/task@sha256:" + "d" * 64,
        config=config,
        trial_id=uuid4(),
        team_id=uuid4(),
        sandbox_name="loom-managed-test",
        candidate_sha="e" * 40,
        provider_scope=provider_scope(config),
        attempt_count=1,
        api_gate=DaytonaApiGate(max_concurrent=2, min_interval_sec=0),
        reserve_callback=reserve,
        started_callback=mark_started,
        deleted_callback=mark_deleted,
    )
    await driver.start()
    params = sdk.create.await_args.args[0]
    assert params.name == "loom-managed-test"
    assert params.auto_stop_interval == 0
    assert params.auto_pause_interval == 0
    assert params.auto_delete_interval == -1
    assert params.labels["loom.candidate_sha"] == "e" * 40
    assert started[0][:2] == (ledger_id, "sandbox-1")
    await driver.stop()
    assert deleted == [(ledger_id, True, None)]


async def test_managed_driver_recovers_existing_sandbox_without_duplicate_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    config = DaytonaConfig.from_env()
    sandbox = MagicMock(id="sandbox-existing")
    sdk = MagicMock(
        get=AsyncMock(return_value=sandbox),
        create=AsyncMock(),
        delete=AsyncMock(),
        close=AsyncMock(),
    )
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona",
        lambda _: sdk,
    )
    ledger_id = uuid4()

    async def reserve() -> dict[str, Any]:
        return {"id": str(ledger_id), "sandbox_id": "sandbox-existing"}

    driver = DaytonaDriver(
        image="registry.example/task@sha256:" + "f" * 64,
        config=config,
        sandbox_name="loom-existing-test",
        api_gate=DaytonaApiGate(max_concurrent=1, min_interval_sec=0),
        reserve_callback=reserve,
    )
    await driver.start()
    sdk.get.assert_awaited_once_with("sandbox-existing")
    sdk.create.assert_not_awaited()
    await driver.stop()
