from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from loom.driver.base import StartOptions
from loom.errors import (
    DriverAlreadyStartedError,
    DriverError,
    DriverNotStartedError,
)
from loom.models.exec import ExecResult
from loom.models.networking import Allowlist, NoNetwork
from loom_drivers.daytona.config import DaytonaConfig
from loom_drivers.daytona.driver import DaytonaDriver, _raise_structured_backpressure


@pytest.fixture
def fake_cfg(monkeypatch: pytest.MonkeyPatch) -> DaytonaConfig:
    monkeypatch.setenv("DAYTONA_API_KEY", "k")
    return DaytonaConfig.from_env()


def _make_sdk_mock(exec_rv: Any = None) -> MagicMock:
    sdk = MagicMock()
    sb = MagicMock(id="sb-test")
    sb.process.exec = AsyncMock(return_value=exec_rv or MagicMock(
        exit_code=0, result="hello\n", artifacts=None,
    ))
    sb.update_network_settings = AsyncMock()
    sb.refresh_data = AsyncMock()
    sb.info = MagicMock(return_value=MagicMock(
        updated_at="2026-06-08T00:00:00Z",
    ))
    sdk.create = AsyncMock(return_value=sb)
    sdk.delete = AsyncMock()
    sdk.close = AsyncMock()
    return sdk


async def test_start_creates_sandbox_and_marks_running(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(image="python:3.12-slim", config=fake_cfg)
    await drv.start()
    assert drv.state == "running"
    sdk.create.assert_awaited_once()
    await drv.stop()


async def test_managed_start_enforces_persisted_resources(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(
        image="python:3.12-slim",
        config=fake_cfg,
        resource_cpu=4,
        resource_memory_gib=8,
        resource_disk_gib=20,
    )
    await drv.start(options=StartOptions(cpus=2, memory_mb=2048, storage_mb=10240))
    params = sdk.create.await_args.args[0]
    assert params.resources.cpu == 2
    assert params.resources.memory == 2
    assert params.resources.disk == 10
    await drv.stop()


async def test_managed_start_rejects_task_above_policy_ceiling(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(
        image="python:3.12-slim",
        config=fake_cfg,
        resource_cpu=2,
        resource_memory_gib=4,
        resource_disk_gib=10,
    )
    with pytest.raises(DriverError, match="exceeds the Daytona policy ceiling"):
        await drv.start(options=StartOptions(cpus=3))


def test_provider_backpressure_is_structured() -> None:
    from daytona import DaytonaError

    with pytest.raises(DriverError, match="DAYTONA_RATE_LIMITED"):
        _raise_structured_backpressure(
            DaytonaError("too many requests", status_code=429)
        )
    with pytest.raises(DriverError, match="DAYTONA_CAPACITY_UNAVAILABLE"):
        _raise_structured_backpressure(
            DaytonaError("organization compute pool exhausted", status_code=503)
        )


async def test_double_start_rejected(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(image="img", config=fake_cfg)
    await drv.start()
    with pytest.raises(DriverAlreadyStartedError):
        await drv.start()
    await drv.stop()


async def test_exec_before_start_raises(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(image="img", config=fake_cfg)
    with pytest.raises(DriverNotStartedError):
        await drv.exec("echo hi")


async def test_exec_maps_response_to_exec_result(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock(
        exec_rv=MagicMock(exit_code=7, result="combined out", artifacts=None),
    )
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(image="img", config=fake_cfg)
    await drv.start()
    r = await drv.exec("false")
    assert isinstance(r, ExecResult)
    assert r.return_code == 7
    assert r.stdout == b"combined out"
    assert r.stderr == b""
    assert r.truncated is False
    await drv.stop()


async def test_stop_deletes_sandbox_and_unregisters(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(image="img", config=fake_cfg)
    await drv.start()
    await drv.stop(delete=True)
    sdk.delete.assert_awaited_once()
    assert drv.state == "stopped"


async def test_stop_is_idempotent_pre_start(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(image="img", config=fake_cfg)
    await drv.stop()
    await drv.stop()
    sdk.delete.assert_not_awaited()


async def test_set_network_policy_calls_update(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(image="img", config=fake_cfg)
    await drv.start()
    sb = sdk.create.return_value
    await drv.set_network_policy(NoNetwork())
    sb.update_network_settings.assert_awaited()
    kwargs = sb.update_network_settings.await_args.kwargs
    assert kwargs == {"network_block_all": True, "network_allow_list": None}
    await drv.stop()


async def test_upload_calls_fs_upload(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    sdk = _make_sdk_mock()
    sb = sdk.create.return_value
    sb.fs.upload_file = AsyncMock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    src = tmp_path / "payload.txt"
    src.write_bytes(b"hello world")
    drv = DaytonaDriver(image="img", config=fake_cfg)
    await drv.start()
    await drv.upload(src, PurePosixPath("/workspace/payload.txt"))
    sb.fs.upload_file.assert_awaited_once()
    args, _ = sb.fs.upload_file.await_args
    assert args[0] == b"hello world"
    assert args[1] == "/workspace/payload.txt"
    await drv.stop()


async def test_download_writes_bytes_to_disk(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    sdk = _make_sdk_mock()
    sb = sdk.create.return_value
    sb.fs.download_file = AsyncMock(return_value=b"remote-data")
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(image="img", config=fake_cfg)
    await drv.start()
    dst = tmp_path / "out.txt"
    await drv.download(PurePosixPath("/workspace/out.txt"), dst)
    assert dst.read_bytes() == b"remote-data"
    await drv.stop()


async def test_allowlist_resolves_domains_via_sandbox_exec(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock()
    sb = sdk.create.return_value
    sb.process.exec = AsyncMock(side_effect=[
        MagicMock(exit_code=0, result="203.0.113.5\n203.0.113.6\n", artifacts=None),
    ])
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(image="img", config=fake_cfg)
    await drv.start()
    await drv.set_network_policy(
        Allowlist(domains=("api.example.com",), cidrs=("10.0.0.0/8",)),
    )
    kwargs = sb.update_network_settings.await_args.kwargs
    assert kwargs["network_block_all"] is False
    assert kwargs["network_allow_list"] == (
        "10.0.0.0/8,203.0.113.5/32,203.0.113.6/32"
    )
    await drv.stop()


async def test_allowlist_unresolvable_domain_raises_driver_error(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock()
    sb = sdk.create.return_value
    sb.process.exec = AsyncMock(return_value=MagicMock(
        exit_code=1, result="", artifacts=None,
    ))
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(image="img", config=fake_cfg)
    await drv.start()
    with pytest.raises(DriverError, match=r"bogus\.invalid"):
        await drv.set_network_policy(
            Allowlist(domains=("bogus.invalid",), cidrs=()),
        )
    await drv.stop()


async def test_stop_skips_usage_when_no_trial_id(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = _make_sdk_mock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )
    drv = DaytonaDriver(image="img", config=fake_cfg)
    await drv.start()
    await drv.stop()  # must not raise without trial/team/session_factory


async def test_stop_persists_usage_via_session_factory(
    fake_cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unit-only: stub the session_factory + persist_record; verify the
    driver wires compute_record output through. Real DB persistence path
    is covered by the integration suite."""
    sdk = _make_sdk_mock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )

    persisted: list[Any] = []

    class _FakeSession:
        async def __aenter__(self) -> "_FakeSession":
            return self

        async def __aexit__(self, *args: Any) -> None:
            return None

        async def commit(self) -> None:
            return None

    async def _fake_persist(session: Any, record: Any) -> None:
        persisted.append(record)

    monkeypatch.setattr(
        "loom_drivers.daytona.driver.persist_record", _fake_persist,
    )

    team_id = uuid4()
    trial_id = uuid4()
    drv = DaytonaDriver(
        image="python:3.12-slim",
        config=fake_cfg,
        trial_id=trial_id,
        team_id=team_id,
        session_factory=_FakeSession,
        per_second_usd=Decimal("0.0010"),
    )
    await drv.start()
    await drv.stop()
    assert len(persisted) == 1
    rec = persisted[0]
    assert rec.team_id == team_id
    assert rec.trial_id == trial_id
    assert rec.sandbox_id == "sb-test"
    assert rec.image == "python:3.12-slim"
    assert rec.cloud_provider == "daytona"
    assert rec.compute_seconds >= 0.0
