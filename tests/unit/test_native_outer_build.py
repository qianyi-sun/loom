"""Outer IO settles fixed execution before exact scoped upload/outcome."""

import asyncio
import hashlib
import os
import sys
from importlib import import_module
from pathlib import Path

import pytest

from loom_capacity_agent.build_admission import BuildOutcomeReceiptV1
from loom_capacity_agent.build_artifact_stream import BuildArtifactUploadReceiptV1
from loom_capacity_executor.native_allocated_io import scoped_native_allocated_io
from loom_capacity_executor.native_build_source import NativeStagedBuildSource
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_native_rootless_runtime import spec_file


@pytest.mark.parametrize("mode", ["success", "failed", "oversize", "wrong-claim", "malformed", "artifact-mismatch",
    "uncertain", "upload-error", "early-upload-reply", "outcome-error", "cancel"])
async def test_outer_io_matches_stream_and_result_before_upload_or_outcome(tmp_path, monkeypatch, mode):
    module = import_module("loom_capacity_executor.native_outer_build")
    _runtime, spec, spec_path, _digest = spec_file(tmp_path)
    source_path = tmp_path / "source.tar"
    source_path.write_bytes(b"source bytes")
    context = spec.context.model_copy(update={"archive_size_bytes": source_path.stat().st_size,
        "archive_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest()})
    spec = spec.model_copy(update={"context": context})
    wire = canonical_bytes(spec)
    spec_path.chmod(0o600)
    spec_path.write_bytes(wire)
    spec_path.chmod(0o400)
    spool = tmp_path / "spool"
    spool.mkdir(mode=0o700)
    archive = tmp_path / "artifact.tar"
    archive.write_bytes(b"artifact bytes" * 10000)
    processes, calls = [], []
    started = asyncio.Event()
    original = asyncio.create_subprocess_exec
    child_mode = mode if mode not in {"upload-error", "outcome-error", "early-upload-reply"} else "success"

    async def spawn(*args, **kwargs):
        assert args[:4] == (sys.executable, "-I", "-m", "loom_capacity_executor.native_rootless_runtime")
        assert args[4] == "launch" and "--expected-parent" in args
        assert kwargs["env"] == {"PATH": "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C.UTF-8"}
        authority, artifact = kwargs["pass_fds"]
        # Intercept only process creation, keeping real pipes/socket backpressure,
        # subprocess cancellation and production outer IO behavior under test.
        child = await original(sys.executable, str(Path(__file__).parents[1] / "support/native_kvm/outer_session_child.py"),
            str(spec_path), hashlib.sha256(wire).hexdigest(), str(authority), str(artifact), child_mode, str(archive),
            **{**kwargs, "env": {**os.environ, "PYTHONPATH": str(Path(__file__).parents[2] / "src")}})
        processes.append(child)
        started.set()
        return child

    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", spawn)

    class Client:
        async def authorize_execution(self, *args, **kwargs):
            pytest.fail("transport fixture requested execution authority")

        async def upload_artifact(self, claim, *, worker_credential, artifact, chunks):
            assert worker_credential == "w" * 43 and claim == spec.claim
            assert processes[0].returncode == 0, "upload started before child settlement"
            calls.append("upload")
            if mode != "early-upload-reply":
                received = b"".join([chunk async for chunk in chunks])
                assert received == archive.read_bytes()
            if mode == "upload-error":
                raise OSError("upload reply lost")
            return BuildArtifactUploadReceiptV1(claim_digest=canonical_digest(claim), artifact=artifact)

        async def record_outcome(self, request, *, worker_credential):
            assert processes[0].returncode == 0
            assert worker_credential == "w" * 43 and request.claim == spec.claim
            calls.append("outcome")
            if mode == "outcome-error":
                raise OSError("outcome reply lost")
            assert request.result == ("failed" if mode == "failed" else "artifact-ready")
            return BuildOutcomeReceiptV1(request=request, request_digest=canonical_digest(request))

    task = None
    try:
        async with scoped_native_allocated_io(claim=spec.claim, source=NativeStagedBuildSource(context, source_path),
            client=Client(), worker_credential="w" * 43) as owner:
            task = asyncio.create_task(module.run_native_outer_build(owner, spec_path=spec_path,
                expected_sha256=hashlib.sha256(wire).hexdigest(), artifact_workspace=spool, timeout_seconds=5))
            if mode == "cancel":
                await asyncio.wait_for(started.wait(), 2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            elif mode in {"success", "failed"}:
                result = await task
                assert result.request.claim == spec.claim
            else:
                with pytest.raises((RuntimeError, ValueError, OSError, ExceptionGroup)):
                    await task
        assert all(process.returncode is not None for process in processes)
        assert list(spool.iterdir()) == []
        assert calls == (["upload", "outcome"] if mode in {"success", "outcome-error"} else ["outcome"] if mode == "failed"
            else ["upload"] if mode in {"upload-error", "early-upload-reply"} else [])
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        for process in processes:
            if process.returncode is None:
                process.kill()
            await process.wait()
