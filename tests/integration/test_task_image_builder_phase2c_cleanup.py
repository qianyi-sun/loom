"""A timed-out external Go probe must not retain its local guard thread."""

from __future__ import annotations

import subprocess
from pathlib import Path
from threading import Thread

import pytest

from tests.integration import test_task_image_builder_phase2c_flow as flow


class ProbeInterrupted(BaseException):
    """Model the non-Exception interruption used by the pytest watchdog."""


@pytest.mark.parametrize("interruption", [subprocess.TimeoutExpired("docker", 1), ProbeInterrupted()])
@pytest.mark.asyncio
async def test_external_probe_interruption_retires_guard(
    tmp_path: Path,
    isolated_migration_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    interruption: BaseException,
) -> None:
    resources = []
    threads = []
    closed = []
    containers = []
    original_service = flow._service

    def capture_service(*args, **kwargs):
        result = original_service(*args, **kwargs)
        service, ledger, *_rest = result
        resources.append((service, ledger))
        original_close = ledger.close

        def close_ledger():
            closed.append("ledger")
            original_close()

        monkeypatch.setattr(ledger, "close", close_ledger)
        return result

    def capture_thread(*args, **kwargs):
        thread = Thread(*args, **kwargs)
        threads.append(thread)
        return thread

    def interrupted_probe(argv, **kwargs):
        containers.append(argv)
        if argv[1] == "rm":
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise interruption

    monkeypatch.setattr(flow, "_service", capture_service)
    monkeypatch.setattr(flow, "Thread", capture_thread)
    monkeypatch.setattr(flow.subprocess, "run", interrupted_probe)
    try:
        with pytest.raises(type(interruption)):
            await flow.test_real_authority_guard_socket_and_go_orchestrator_flow(
                tmp_path, isolated_migration_postgres_url, tmp_path / "supervisor.test",
            )
        assert threads and all(not thread.is_alive() for thread in threads)
        assert closed == ["ledger"]
        assert len(containers) == 2
        name = containers[0][containers[0].index("--name") + 1]
        assert containers[1] == ["docker", "rm", "--force", name]
    finally:
        # Keep the RED regression safe: the original test leaks these resources.
        for service, ledger in resources:
            service.stop()
            for thread in threads:
                thread.join(timeout=5)
            service.close()
            ledger.close()
