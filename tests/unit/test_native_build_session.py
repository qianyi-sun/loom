"""The mapped session cannot verify artifacts before confirmed runtime cleanup."""

import os
import socket
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_capacity_executor.native_runsc import NativeRunscLayout
from loom_capacity_executor.native_runtime_cleanup import NativeRuntimeCleanupResult
from loom_capacity_executor.native_supervisor import NativeSupervisionResult
from loom_capacity_manager.contracts import canonical_digest
from tests.unit.test_native_execution_permit import execution_request
from tests.unit.test_native_sandbox_consumer import bound_context
from tests.unit.test_personal_dev_builder import _registration
from tests.unit.test_personal_dev_builder_artifact import _artifact


@pytest.mark.parametrize("boundary", ["success", "expired", "failed", "unreaped", "uncertain", "wrong-artifact"])
def test_session_orders_execution_cleanup_and_private_artifact_verification(tmp_path, monkeypatch, boundary):
    from loom_capacity_executor import native_build_session as module

    claim = execution_request().claim
    context = bound_context(_registration(), "oldlab").model_copy(update={
        "claim_digest": canonical_digest(claim), "request_id": claim.request_id})
    layout = NativeRunscLayout(Path("/runtime/runsc"), tmp_path / "state", tmp_path / "bundles", context.claim_digest)
    workspace = tmp_path / "work"
    workspace.mkdir(mode=0o700)
    events = []
    broker = SimpleNamespace()

    @contextmanager
    def started(_layout):
        events.append("spawn")
        yield object(), broker
        events.append("settled")

    def supervise(*args, **kwargs):
        assert events == ["bound", "spawn"]
        events.append("supervise")
        return NativeSupervisionResult(boundary not in {"expired", "failed"},
            "expired" if boundary == "expired" else "fixture", boundary != "unreaped")

    def cleanup(*args, **kwargs):
        assert events == ["bound", "spawn", "supervise", "settled"]
        events.append("cleanup")
        if boundary in {"success", "wrong-artifact"}:
            # The path does not exist at all until cleanup is confirmed.
            output = workspace / "output/build"
            output.mkdir(parents=True)
            _artifact(output / "artifacts.tar", manifest_overrides={"candidate_sha": "f" * 64} if boundary == "wrong-artifact" else {})
        return NativeRuntimeCleanupResult(boundary != "uncertain", "fixture")

    monkeypatch.setattr(module, "bind_native_parent_death", lambda pid: events.append("bound"))
    monkeypatch.setattr(module, "_broker_session", started)
    monkeypatch.setattr(module, "supervise_native_execution", supervise)
    monkeypatch.setattr(module, "reconcile_native_runtime_cleanup", cleanup)
    arguments = dict(claim=claim, context=context, layout=layout, workspace=workspace,
        authority=object(), expected_parent_pid=123, max_artifact_bytes=1024**2,
        max_image_archive_bytes=256 * 1024)
    if boundary == "wrong-artifact":
        with pytest.raises(RuntimeError):
            module.execute_native_build_session(**arguments)
    else:
        result = module.execute_native_build_session(**arguments)
        assert (result.artifact is not None) is (boundary == "success")
        assert result.supervision.broker_reaped is (boundary != "unreaped")
    assert events == ["bound", "spawn", "supervise", "settled", "cleanup"]
    if boundary not in {"success", "wrong-artifact"}:
        assert not (workspace / "verified").exists()


@pytest.mark.parametrize("boundary", ["claim", "request", "pool", "contract", "layout", "limit", "workspace"])
def test_invalid_session_identity_never_starts_broker(tmp_path, monkeypatch, boundary):
    from loom_capacity_executor import native_build_session as module

    claim = execution_request().claim
    context = bound_context(_registration(), "oldlab").model_copy(update={
        "claim_digest": canonical_digest(claim), "request_id": claim.request_id})
    updates = {"claim": {"claim_digest": "f" * 64}, "request": {"request_id": execution_request().claim.request_id},
        "pool": {"platform": "linux/arm64"}, "contract": {"build_contract_sha256": "f" * 64}}
    context = context.model_copy(update=updates.get(boundary, {}))
    layout = NativeRunscLayout(Path("/runtime/runsc"), tmp_path / "state", tmp_path / "bundles",
        "e" * 64 if boundary == "layout" else canonical_digest(claim))
    workspace = tmp_path / "work"
    workspace.mkdir(mode=0o755 if boundary == "workspace" else 0o700)
    monkeypatch.setattr(module, "bind_native_parent_death", lambda pid: None)
    monkeypatch.setattr(module, "_broker_session", lambda *_: pytest.fail("invalid session started broker"))
    with pytest.raises((ValueError, RuntimeError)):
        module.execute_native_build_session(claim=claim, context=context, layout=layout, workspace=workspace,
            authority=object(), expected_parent_pid=123, max_artifact_bytes=True if boundary == "limit" else 1024**2,
            max_image_archive_bytes=256 * 1024)


@pytest.mark.parametrize("boundary", ["exact", "pid", "parent", "claim", "malformed", "dead", "timeout", "body-error"])
def test_broker_readiness_and_exception_paths_settle_only_owned_child(tmp_path, monkeypatch, boundary):
    from loom_capacity_executor import native_build_session as module
    from loom_capacity_executor.native_runtime_broker import NativeBrokerReady
    from loom_capacity_manager.contracts import canonical_bytes

    layout = NativeRunscLayout(Path("/runtime/runsc"), tmp_path / "state", tmp_path / "bundles", "a" * 64)
    events = []
    child = SimpleNamespace(pid=12345, returncode=0 if boundary == "dead" else None)
    child.poll = lambda: child.returncode

    def kill():
        events.append("kill")
        child.returncode = -9

    def wait(**kwargs):
        events.append("wait")
        assert child.returncode is not None
        return child.returncode

    def popen(command, **kwargs):
        assert command[1:3] == ["-m", "loom_capacity_executor.native_runtime_broker"]
        assert kwargs["close_fds"] is True and len(kwargs["pass_fds"]) == 1
        descriptor = kwargs["pass_fds"][0]
        assert command[-2:] == ["--control-fd", str(descriptor)]
        ready = NativeBrokerReady(pid=12346 if boundary == "pid" else child.pid,
            parent_pid=os.getpid() + (1 if boundary == "parent" else 0),
            claim_digest="f" * 64 if boundary == "claim" else layout.claim_digest)
        with socket.socket(fileno=os.dup(descriptor)) as channel:
            channel.send(b"not-json" if boundary == "malformed" else canonical_bytes(ready))
        return child

    child.kill, child.wait = kill, wait
    monkeypatch.setattr(module.subprocess, "Popen", popen)
    if boundary == "timeout":
        monkeypatch.setattr(module.select, "select", lambda *args: ([], [], []))
    if boundary == "exact":
        with module._broker_session(layout) as (channel, observed):
            assert observed is child and not channel.get_inheritable()
            events.append("body")
    else:
        with pytest.raises((ValueError, RuntimeError)):
            with module._broker_session(layout):
                assert boundary == "body-error"
                events.append("body")
                raise RuntimeError("injected session exception")
    assert events == (["body"] if boundary in {"exact", "body-error"} else []) + (
        [] if boundary == "dead" else ["kill"]) + ["wait"]
