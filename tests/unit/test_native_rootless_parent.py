"""A reparented mapped target must not bind itself to an unrelated subreaper."""

import os
from importlib import import_module

import pytest


@pytest.mark.parametrize("boundary", ["exact", "adopted", "stale-child", "symlink", "writable", "missing",
    "malformed", "parent-changed", "invalid-rootless", "state-public"])
def test_mapped_parent_requires_original_rootless_chain(tmp_path, monkeypatch, boundary):
    module = import_module("loom_capacity_executor.native_rootless_parent")
    state = tmp_path / "rootlesskit"
    state.mkdir(mode=0o755 if boundary == "state-public" else 0o700)
    marker = state / "child_pid"
    marker.write_text("999" if boundary == "stale-child" else "321\n" if boundary == "malformed" else "321")
    marker.chmod(0o644 if boundary == "writable" else 0o444)
    if boundary == "missing":
        marker.unlink()
    elif boundary == "symlink":
        target = tmp_path / "marker"
        marker.rename(target)
        marker.symlink_to(target)
    events = []
    monkeypatch.setattr(module.os, "getppid", lambda: 321)
    monkeypatch.setattr(module, "bind_native_parent_death", lambda parent: events.append(parent))
    parents = iter([123, 456]) if boundary == "parent-changed" else None
    monkeypatch.setattr(module, "_process_parent", lambda pid: next(parents) if parents else 456 if boundary == "adopted" else 123)
    clock = iter([0, 6_000_000_000])
    monkeypatch.setattr(module.time, "clock_gettime_ns", lambda _: next(clock))
    if boundary == "exact":
        assert module.bind_native_rootless_parent(state, expected_rootless_pid=123) == 321
        assert events == [321, 321]
    else:
        with pytest.raises((ValueError, RuntimeError, OSError)):
            module.bind_native_rootless_parent(state, expected_rootless_pid=True if boundary == "invalid-rootless" else 123)


def test_native_proc_parent_reader_uses_current_process_stat():
    module = import_module("loom_capacity_executor.native_rootless_parent")
    assert module._process_parent(os.getpid()) == os.getppid()
