"""Disposable KVM fixtures retain reliable cleanup and liveness evidence."""

import subprocess
from importlib import import_module
from pathlib import Path
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
            if boundary == "timeout" and "kill" not in events:
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
            assert events == ["wait", "kill", "wait"]
    else:
        module.cleanup_fixture_runtime(["runsc"], ["root", "child"], Root())
        assert events == ["wait", "child", "root", "list"]


def test_killed_pulse_writer_preserves_last_complete_liveness_evidence(tmp_path, monkeypatch):
    module = import_module("tests.support.native_kvm.lifecycle_probe")
    pulse = tmp_path / "lifecycle-pulse"
    module.publish_pulse(pulse, 1)

    def interrupted_write(path, text, **kwargs):
        # SIGKILL can land after open(O_TRUNC), before the first write.
        with path.open("w"):
            pass
        raise InterruptedError("fixture writer killed after truncation")

    monkeypatch.setattr(Path, "write_text", interrupted_write)
    with pytest.raises(InterruptedError):
        module.publish_pulse(pulse, 2)
    assert pulse.read_bytes() == b"1"


def test_fixture_pulse_advances_only_with_complete_positive_sequence(tmp_path):
    module = import_module("tests.support.native_kvm.lifecycle_probe")
    pulse = tmp_path / "lifecycle-pulse"
    for sequence in (1, 9, 10):
        module.publish_pulse(pulse, sequence)
        assert int(pulse.read_bytes()) == sequence
