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
