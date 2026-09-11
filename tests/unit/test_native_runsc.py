"""Fixed native commands have no feature-controlled command or clock fallback."""

from pathlib import Path

import pytest


def layout():
    from loom_capacity_executor.native_runsc import NativeRunscLayout

    return NativeRunscLayout(Path("/protected/runsc"), Path("/private/state"),
        Path("/private/bundles"), "a" * 64)


@pytest.mark.parametrize("role", ["pause", "buildkit", "client"])
def test_native_start_uses_only_fixed_attached_kvm_role(role):
    configured = layout()
    command = configured.command("start", role)
    assert command[0] == "/protected/runsc"
    assert "--platform=kvm" in command and "--network=none" in command
    assert "--ignore-cgroups=true" in command and "--host-uds=none" in command
    assert "--oci-seccomp=true" in command and "--directfs=false" in command
    assert command[-3:] == ("run", f"--bundle=/private/bundles/{role}", f"loom-native-{role}-" + "a" * 64)
    assert "--detach" not in command


@pytest.mark.parametrize("operation,role", [("shell", "client"), ("start", "unknown"), ("ready", "client")])
def test_native_commands_reject_generic_or_wrong_role_actions(operation, role):
    with pytest.raises(ValueError):
        layout().command(operation, role)


@pytest.mark.parametrize("path", ["/", "relative", "/private/../other", "/private/line\nbreak"])
def test_native_layout_rejects_ambiguous_paths(path):
    from loom_capacity_executor.native_runsc import NativeRunscLayout

    with pytest.raises(ValueError):
        NativeRunscLayout(Path(path), Path("/private/state"), Path("/private/bundles"), "a" * 64)


def test_native_start_binds_parent_checks_deadline_and_clears_environment(monkeypatch):
    from loom_capacity_executor import native_runsc as module

    observed = []
    monkeypatch.setattr(module, "bind_native_parent_death", lambda parent: observed.append(("bound", parent)))
    monkeypatch.setattr(module.time, "clock_gettime_ns", lambda _clock_id: 99)
    monkeypatch.setenv("WORKER_CREDENTIAL", "must-not-be-inherited")

    def execute(path, argv, env):
        observed.append(("exec", path, argv, env))
        raise SystemExit(0)

    monkeypatch.setattr(module.os, "execve", execute)
    with pytest.raises(SystemExit):
        module.exec_native_runsc(layout(), operation="start", role="client", expected_parent_pid=123, deadline_boottime_ns=100)
    assert observed[0] == ("bound", 123)
    assert observed[1][1:3] == ("/protected/runsc", layout().command("start", "client"))
    assert set(observed[1][3]) == {"PATH", "LANG"}


@pytest.mark.parametrize("boundary", ["expired", "missing-clock", "clock-error", "invalid-deadline", "parent-changed"])
def test_native_start_never_executes_without_current_deadline_and_parent(monkeypatch, boundary):
    from loom_capacity_executor import native_runsc as module

    def bind(_parent):
        if boundary == "parent-changed":
            raise RuntimeError("parent changed")

    def clock(_clock_id):
        if boundary == "clock-error":
            raise OSError("clock unavailable")
        return 100

    monkeypatch.setattr(module, "bind_native_parent_death", bind)
    monkeypatch.setattr(module.time, "clock_gettime_ns", clock)
    if boundary == "missing-clock":
        monkeypatch.delattr(module.time, "CLOCK_BOOTTIME")
    monkeypatch.setattr(module.os, "execve", lambda *_args: pytest.fail("unauthorized exec"))
    with pytest.raises((ValueError, RuntimeError)):
        module.exec_native_runsc(layout(), operation="start", role="client", expected_parent_pid=123,
            deadline_boottime_ns=True if boundary == "invalid-deadline" else 100)
