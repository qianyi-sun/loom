"""Fixed root entrypoint refuses request parsing before principal authentication."""

from importlib import import_module
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("fault", ["uid", "name", "argv", "root", "original-command"])
def test_helper_authenticates_fixed_management_caller_before_reading_stdin(monkeypatch, fault):
    module = import_module("loom_capacity_executor.native_node_recovery")
    calls = []
    monkeypatch.setattr(module, "read_native_node_recovery_policy", lambda *args, **kwargs: SimpleNamespace(management_uid=25000))
    monkeypatch.setattr(module, "_require_initial_root", lambda: None)
    monkeypatch.setattr(module.pwd, "getpwnam", lambda name: SimpleNamespace(pw_uid=25000))
    monkeypatch.setattr(module, "_read_request", lambda: calls.append("read"))
    monkeypatch.setattr(module.sys, "argv", ["/opt/loom/recovery"] + (["extra"] if fault == "argv" else []))
    monkeypatch.setenv("SUDO_UID", "24850" if fault == "uid" else "25000")
    monkeypatch.setenv("SUDO_USER", "worker" if fault == "name" else "loom-native-recovery")
    monkeypatch.delenv("SSH_ORIGINAL_COMMAND", raising=False)
    if fault == "original-command":
        monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "command")
    if fault == "root":
        def not_root():
            raise ValueError("initial root")
        monkeypatch.setattr(module, "_require_initial_root", not_root)
    with pytest.raises(ValueError):
        module.run_native_recovery_helper(policy_path="/etc/loom/recovery.json", policy_sha256="a" * 64)
    assert calls == []


@pytest.mark.parametrize("fault", ["exact", "populated", "unknown", "duplicate", "absent", "malformed"])
def test_quiescence_requires_whole_subtree_unpopulated(fault):
    module = import_module("loom_capacity_executor.native_node_recovery")
    wire = {"exact": b"populated 0\nfrozen 0\n", "populated": b"populated 1\nfrozen 0\n",
        "unknown": b"populated unknown\n", "duplicate": b"populated 0\npopulated 1\n",
        "absent": b"frozen 0\n", "malformed": b"populated 0 extra\n"}[fault]
    if fault == "exact":
        module._require_unpopulated(wire)
    else:
        with pytest.raises(ValueError):
            module._require_unpopulated(wire)


@pytest.mark.parametrize("fault", ["oversized", "deadline", "invalid-json"])
def test_request_reader_is_bounded_without_logging_untrusted_input(monkeypatch, fault):
    import os

    module = import_module("loom_capacity_executor.native_node_recovery")
    read_fd, write_fd = os.pipe()
    try:
        monkeypatch.setattr(module.sys, "stdin", SimpleNamespace(fileno=lambda: read_fd))
        if fault == "deadline":
            monkeypatch.setattr(module.select, "select", lambda *args: ([], [], []))
        elif fault == "oversized":
            monkeypatch.setattr(module, "_MAX_REQUEST", 8)
            os.write(write_fd, b"012345678")
        else:
            os.write(write_fd, b"{}\n")
        os.close(write_fd)
        write_fd = None
        with pytest.raises(ValueError):
            module._read_request()
    finally:
        os.close(read_fd)
        if write_fd is not None:
            os.close(write_fd)
