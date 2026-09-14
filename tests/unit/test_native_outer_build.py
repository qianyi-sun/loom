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


@pytest.mark.parametrize("mode,startup_delay", [(mode, 0) for mode in ["success", "failed", "oversize", "stdout-flood", "wrong-claim", "malformed", "artifact-mismatch",
    "uncertain", "upload-error", "early-upload-reply", "outcome-error", "cancel", "authority-cleanup-cancel",
    "upload-cleanup-cancel", "outcome-cleanup-cancel"]] + [pytest.param(mode, 4,
        id=mode + "-cold-start") for mode in ["authority-cleanup-cancel", "upload-cleanup-cancel", "outcome-cleanup-cancel"]])
async def test_outer_io_matches_stream_and_result_before_upload_or_outcome(tmp_path, monkeypatch, mode, startup_delay):
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
    authority_cleanup, allow_authority_cleanup = asyncio.Event(), asyncio.Event()
    authority_settled = []
    write_started, write_cleanup, allow_write_cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()
    write_settled = []
    original = asyncio.create_subprocess_exec
    child_mode = mode if mode not in {"upload-error", "outcome-error", "early-upload-reply",
        "upload-cleanup-cancel", "outcome-cleanup-cancel"} else "success"

    async def blocked_write():
        try:
            write_started.set()
            await asyncio.Future()
        finally:
            write_cleanup.set()
            await allow_write_cleanup.wait()
            assert list(spool.iterdir()), "spool removed before client cleanup settled"
            write_settled.append(True)

    async def spawn(*args, **kwargs):
        if startup_delay:
            # Fault injection: cold imports/runner contention may exceed three
            # seconds. This is not a delay used to guess cancellation readiness.
            await asyncio.sleep(startup_delay)
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
            if mode == "authority-cleanup-cancel":
                try:
                    await asyncio.Future()
                finally:
                    authority_cleanup.set()
                    await allow_authority_cleanup.wait()
                    authority_settled.append(True)
                return
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
            if mode == "upload-cleanup-cancel":
                await blocked_write()
            return BuildArtifactUploadReceiptV1(claim_digest=canonical_digest(claim), artifact=artifact)

        async def record_outcome(self, request, *, worker_credential):
            assert processes[0].returncode == 0
            assert worker_credential == "w" * 43 and request.claim == spec.claim
            calls.append("outcome")
            if mode == "outcome-error":
                raise OSError("outcome reply lost")
            if mode == "outcome-cleanup-cancel":
                await blocked_write()
            assert request.result == ("failed" if mode == "failed" else "artifact-ready")
            return BuildOutcomeReceiptV1(request=request, request_digest=canonical_digest(request))

    task = None
    try:
        async with scoped_native_allocated_io(claim=spec.claim, source=NativeStagedBuildSource(context, source_path),
            client=Client(), worker_credential="w" * 43) as owner:
            task = asyncio.create_task(module.run_native_outer_build(owner, spec_path=spec_path,
                expected_sha256=hashlib.sha256(wire).hexdigest(), artifact_workspace=spool, timeout_seconds=5))
            if mode in {"upload-cleanup-cancel", "outcome-cleanup-cancel"}:
                await asyncio.wait_for(write_started.wait(), 3)
                task.cancel()
                await asyncio.wait_for(write_cleanup.wait(), 3)
                task.cancel()
                await asyncio.sleep(0)
                allow_write_cleanup.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert write_settled == [True], "repeated cancellation interrupted write cleanup"
            elif mode == "authority-cleanup-cancel":
                await asyncio.wait_for(authority_cleanup.wait(), 3)
                task.cancel()
                await asyncio.sleep(0)  # Let cancellation reach the await boundary, not an elapsed-time guess.
                task.cancel()
                allow_authority_cleanup.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert authority_settled == [True], "repeated cancellation interrupted IO cleanup"
            elif mode == "cancel":
                await asyncio.wait_for(started.wait(), 2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            elif mode in {"success", "failed"}:
                result = await task
                assert result.request.claim == spec.claim
            else:
                with pytest.raises((RuntimeError, ValueError, OSError, ExceptionGroup)) as failure:
                    await task
                assert not isinstance(failure.value, TimeoutError), "launcher cleanup timed out"
        assert all(process.returncode is not None for process in processes)
        assert list(spool.iterdir()) == []
        assert calls == (["upload", "outcome"] if mode in {"success", "outcome-error", "outcome-cleanup-cancel"}
            else ["outcome"] if mode == "failed"
            else ["upload"] if mode in {"upload-error", "early-upload-reply", "upload-cleanup-cancel"} else [])
    finally:
        allow_authority_cleanup.set()
        allow_write_cleanup.set()
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        for process in processes:
            if process.returncode is None:
                process.kill()
            if process.stdout is not None:
                while await process.stdout.read(65536):
                    pass
            await process.wait()


@pytest.mark.parametrize("creation_fails", [False, True])
async def test_cancelled_spawn_settles_late_creation_with_repeated_cancellation(monkeypatch, creation_fails):
    module = import_module("loom_capacity_executor.native_outer_build")
    original = asyncio.create_subprocess_exec
    created, allow_return = asyncio.Event(), asyncio.Event()
    processes = []

    async def delayed_spawn(*args, **kwargs):
        if not creation_fails:
            process = await original(sys.executable, "-c",
                "import os,time; os.write(1, b'x'*1024**2); time.sleep(60)",
                stdout=asyncio.subprocess.PIPE, limit=4097)
            processes.append(process)
        created.set()
        await allow_return.wait()
        if creation_fails:
            raise OSError("creation failed")
        return process

    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", delayed_spawn)
    task = asyncio.create_task(module._spawn(Path("/unused"), "a" * 64, 3, 4))
    try:
        await asyncio.wait_for(created.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        allow_return.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)
        for process in processes:
            assert process.returncode is not None
            assert process.stdout.at_eof()
    finally:
        allow_return.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        for process in processes:
            if process.returncode is None:
                process.kill()
            while await process.stdout.read(65536):
                pass
            await process.wait()
