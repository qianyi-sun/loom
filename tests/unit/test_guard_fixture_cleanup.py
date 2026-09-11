"""A failed Docker call must not strand pytest's non-daemon guard thread."""

from importlib import import_module
from threading import Event

import pytest


@pytest.mark.parametrize("failed", [False, True])
def test_guard_scope_stops_and_joins_before_closing_even_on_base_exception(failed):
    module = import_module("tests.support.guard_fixture")
    running, stopped = Event(), Event()
    events = []

    class Guard:
        def start(self):
            events.append("start")
            running.set()
            stopped.wait(5)
            events.append("exited")

        def stop(self):
            events.append("stop")
            stopped.set()

        def close(self):
            assert "exited" in events
            events.append("close")

    class Interrupted(BaseException):
        pass

    try:
        with module.running_guard(Guard()) as failure:
            assert running.wait(1)
            if failed:
                raise Interrupted("fixture deadline")
            assert failure == []
    except Interrupted:
        assert failed
    assert events == ["start", "stop", "exited", "close"]


def test_go_guard_flow_is_owned_by_docker_lane():
    import tomllib
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    manifest = tomllib.loads((root / "config/component-ownership.toml").read_text())
    path = "tests/integration/test_task_image_builder_phase2c_flow.py"
    suites = {suite["lane"]: suite for suite in manifest["test_suites"] if suite["id"].startswith("python-integration")}
    assert path in suites["integration"]["exclude_paths"]
    assert path in suites["integration-docker"]["include_paths"]


def test_actual_listening_guard_is_stopped_when_docker_times_out(tmp_path):
    import socket
    import subprocess
    import time

    from tests.support.guard_fixture import running_guard
    from tests.unit.test_task_image_builder_guard_service import _service

    service, ledger, *_rest = _service(tmp_path)
    try:
        with pytest.raises(subprocess.TimeoutExpired), running_guard(service):
            deadline = time.monotonic() + 3
            while not service.config.protocol.socket_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert service.config.protocol.socket_path.exists()
            raise subprocess.TimeoutExpired(["docker", "run", "fixture"], 180)
        with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as channel:
            with pytest.raises((FileNotFoundError, ConnectionRefusedError)):
                channel.connect(str(service.config.protocol.socket_path))
    finally:
        ledger.close()


@pytest.mark.parametrize("result", ["removed", "absent", "daemon-error"])
def test_container_cleanup_is_bounded_and_exact(monkeypatch, result):
    import subprocess

    from tests.integration.test_task_image_builder_phase2c_flow import _remove_fixture_container

    observed = []

    def run(command, **kwargs):
        observed.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0 if result == "removed" else 1,
            stdout="", stderr="No such container: fixture" if result == "absent" else "unavailable")

    monkeypatch.setattr(subprocess, "run", run)
    if result == "daemon-error":
        with pytest.raises(AssertionError, match="cleanup failed"):
            _remove_fixture_container("loom-phase2c-test-exact")
    else:
        _remove_fixture_container("loom-phase2c-test-exact")
    assert observed[0][0] == ["docker", "rm", "-f", "loom-phase2c-test-exact"]
    assert observed[0][1]["timeout"] == 10
