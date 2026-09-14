"""The disposable primitive fixture joins its attached root before child deletion."""

import subprocess
from importlib import import_module
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("boundary", ["stopping", "stopped", "timeout", "delete-failed", "residue"])
def test_fixture_cleanup_joins_root_before_deleting_child_state(monkeypatch, boundary):
    module = import_module("tests.support.native_kvm.execute")
    events = []

    class Root:
        def poll(self):
            return 0 if boundary == "stopped" else None

        def kill(self):
            events.append("kill")

        def wait(self, timeout):
            events.append("wait")
            if boundary == "timeout":
                raise subprocess.TimeoutExpired("root", timeout)
            return 0

    def run(command, **kwargs):
        assert "wait" in events, "child delete raced attached root's own cleanup"
        if "delete" in command:
            events.append(command[-1])
            return SimpleNamespace(returncode=int(boundary == "delete-failed"))
        assert command[-2:] == ["list", "--format=json"]
        events.append("list")
        return SimpleNamespace(stdout='[{"id":"child","status":"stopped"}]' if boundary == "residue" else "[]")

    monkeypatch.setattr(module.subprocess, "run", run)
    if boundary in {"timeout", "delete-failed", "residue"}:
        with pytest.raises((subprocess.TimeoutExpired, AssertionError)):
            module.cleanup_fixture_runtime(["runsc"], ["root", "child"], Root())
        if boundary == "timeout":
            assert events == ["kill", "wait"]
    else:
        module.cleanup_fixture_runtime(["runsc"], ["root", "child"], Root())
        assert events == (["kill"] if boundary == "stopping" else []) + ["wait", "child", "root", "list"]
