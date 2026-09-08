"""Exercise production loop composition through its real HTTP readiness server."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
from pathlib import Path
from uuid import UUID

import pytest

from loom_capacity_agent import runtime as runtime_module
from tests.unit.test_capacity_agent_runtime import (
    _assigned_observation,
    _configuration,
    _Factory,
    _observation,
    _Publisher,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("recover_committed", [False, True])
async def test_new_demand_wakes_readiness_recovery_before_its_next_poll(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    recover_committed: bool,
) -> None:
    """A healthy publication must not wait for an independently phased timer."""
    configuration = _configuration()
    allow_capture = asyncio.Event()
    recovery_checked = asyncio.Event()
    server_started = asyncio.Event()
    address: list[tuple[str, int]] = []
    statuses: list[bytes] = []

    class Engine:
        async def dispose(self) -> None:
            pass

    class Publisher(_Publisher):
        async def aclose(self) -> None:
            pass

    async def read_high_water(*_args: object, **_kwargs: object) -> int:
        return 1 if recover_committed else 0

    async def recover(*_args: object, **_kwargs: object):
        await allow_capture.wait()
        return _observation(configuration, 1)

    async def capture(*_args: object, **kwargs: object):
        await allow_capture.wait()
        return _observation(configuration, kwargs["expected_high_water"] + 1)

    async def read_release(*_args: object, **_kwargs: object):
        return None

    real_demand = runtime_module.CapacityAgentRuntime
    real_release = runtime_module.ExecutableProtectedReleaseReporterRuntime
    real_recovery = runtime_module.ExecutableTerminalInventoryEvidenceRecoveryRuntime
    real_start_server = asyncio.start_server

    def demand_runtime(**kwargs: object):
        return real_demand(
            **kwargs, capture=capture, recover=recover, read_high_water=read_high_water
        )

    def release_runtime(**kwargs: object):
        return real_release(**kwargs, read_next=read_release)

    class CheckedRecovery(real_recovery):
        async def run_once(self) -> None:
            await super().run_once()
            recovery_checked.set()

    async def start_server(callback, **_kwargs: object):
        server = await real_start_server(callback, host="127.0.0.1", port=0)
        address.append(server.sockets[0].getsockname()[:2])
        server_started.set()
        return server

    monkeypatch.setattr(runtime_module, "load_reporter_configuration", lambda _path: configuration)
    monkeypatch.setattr(
        runtime_module,
        "load_database_url",
        lambda _path: "postgresql+psycopg://agent:secret@postgres/loom",
    )
    monkeypatch.setattr(runtime_module, "create_capacity_agent_engine", lambda _url: Engine())
    monkeypatch.setattr(runtime_module, "async_sessionmaker", lambda *_args, **_kwargs: _Factory())
    monkeypatch.setattr(
        runtime_module.DemandReporterClient,
        "from_files",
        classmethod(lambda _cls, _configuration, _connection: Publisher()),
    )
    monkeypatch.setattr(runtime_module, "CapacityAgentRuntime", demand_runtime)
    monkeypatch.setattr(
        runtime_module, "ExecutableProtectedReleaseReporterRuntime", release_runtime
    )
    monkeypatch.setattr(
        runtime_module, "ExecutableTerminalInventoryEvidenceRecoveryRuntime", CheckedRecovery
    )
    monkeypatch.setattr(runtime_module.asyncio, "start_server", start_server)

    arguments = argparse.Namespace(
        configuration_file=tmp_path / "configuration.json",
        database_url_file=tmp_path / "database-url",
        manager_origin="https://capacity.internal",
        bearer_token_file=tmp_path / "bearer-token",
        ca_file=tmp_path / "ca.pem",
        certificate_file=tmp_path / "client.pem",
        private_key_file=tmp_path / "client.key",
        poll_interval_seconds=30.0,
        max_attempts=100,
        health_port=0,
    )

    async def ready_response() -> bytes:
        reader, writer = await asyncio.open_connection(*address[0])
        try:
            writer.write(b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()
            response = await reader.read()
            status = response.split(b"\r\n", 1)[0]
            statuses.append(status)
            return status
        finally:
            writer.close()
            await writer.wait_closed()

    async def wait_ready() -> None:
        while await ready_response() != b"HTTP/1.1 200 OK":
            await asyncio.sleep(0.01)

    task = asyncio.create_task(runtime_module._main_async(arguments))
    try:
        await asyncio.wait_for(server_started.wait(), timeout=2)
        # Force recovery to inspect the initial empty state before demand commits.
        await asyncio.wait_for(recovery_checked.wait(), timeout=2)
        assert await ready_response() == b"HTTP/1.1 503 Service Unavailable"
        allow_capture.set()
        try:
            await asyncio.wait_for(wait_ready(), timeout=2)
        except TimeoutError:
            pytest.fail(f"fresh demand did not wake readiness recovery: {set(statuses)!r}")
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_observation_update_during_recovery_is_not_lost() -> None:
    """Clearing the wakeup after an awaited check would lose a newer snapshot."""
    configuration = _configuration()
    current = [
        _assigned_observation(
            configuration,
            protected_attempt_id=UUID(int=810),
            submission_intent_id=UUID(int=841),
        )
    ]
    observation_changed = asyncio.Event()
    fetch_started = asyncio.Event()
    finish_fetch = asyncio.Event()

    class Publisher(_Publisher):
        async def get_executable_terminal_inventory_evidence(self, _intent_id: UUID):
            fetch_started.set()
            await finish_fetch.wait()
            return None

    recovery = runtime_module.ExecutableTerminalInventoryEvidenceRecoveryRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=Publisher(),
        observation_source=lambda: current[0],
        observation_changed=observation_changed,
    )

    async def wait_ready() -> None:
        while not recovery.ready:
            await asyncio.sleep(0.01)

    task = asyncio.create_task(recovery.run_forever(poll_interval_seconds=30))
    try:
        await asyncio.wait_for(fetch_started.wait(), timeout=2)
        current[0] = _observation(configuration, 2)
        observation_changed.set()
        assert recovery.ready is False
        finish_fetch.set()
        await asyncio.wait_for(wait_ready(), timeout=2)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_failed_recovery_retries_without_an_observation_update() -> None:
    """The wakeup must retain periodic retry for unchanged manager evidence."""
    configuration = _configuration()
    observation = _assigned_observation(
        configuration,
        protected_attempt_id=UUID(int=820),
        submission_intent_id=UUID(int=821),
    )
    failed = asyncio.Event()

    class Publisher(_Publisher):
        async def get_executable_terminal_inventory_evidence(self, _intent_id: UUID):
            if not failed.is_set():
                failed.set()
                raise RuntimeError("manager unavailable")
            return None

    recovery = runtime_module.ExecutableTerminalInventoryEvidenceRecoveryRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=Publisher(),
        observation_source=lambda: observation,
        observation_changed=asyncio.Event(),
    )

    async def wait_ready() -> None:
        while not recovery.ready:
            await asyncio.sleep(0.01)

    task = asyncio.create_task(recovery.run_forever(poll_interval_seconds=0.05))
    try:
        await asyncio.wait_for(failed.wait(), timeout=2)
        assert recovery.ready is False
        await asyncio.wait_for(wait_ready(), timeout=2)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
