"""Runtime inputs are complete fixed copies, not portable proc-FD references."""

import asyncio
import hashlib
import os
import threading
from importlib import import_module

import pytest

from loom.personal_dev_candidate import PERSONAL_DEV_BUILD_CONTRACT_SHA256
from loom_capacity_executor.native_build_source import NativeStagedBuildSource
from loom_capacity_executor.native_sandbox_contract import render_native_sandbox_contract
from tests.unit.test_native_build_context import context_for
from tests.unit.test_native_execution_permit import execution_request


def prepared(tmp_path):
    workspace = tmp_path / "runtime"
    workspace.mkdir(mode=0o700)
    archive = tmp_path / "source.tar"
    archive.write_bytes(b"source bytes")
    context = context_for(execution_request().claim).model_copy(update={
        "archive_size_bytes": archive.stat().st_size, "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "build_contract_sha256": PERSONAL_DEV_BUILD_CONTRACT_SHA256})
    return workspace, NativeStagedBuildSource(context, archive)


@pytest.mark.parametrize("boundary", ["exact", "digest", "truncated", "excess", "symlink", "public", "reused", "limit"])
async def test_fixed_input_copy_is_verified_readonly_and_never_reuses_input(tmp_path, boundary):
    module = import_module("loom_capacity_executor.native_runtime_input")
    workspace, source = prepared(tmp_path)
    if boundary == "digest":
        source.archive.write_bytes(b"broken bytes")
    elif boundary == "truncated":
        source.archive.write_bytes(b"x")
    elif boundary == "excess":
        source.archive.write_bytes(b"source bytes!")
    elif boundary == "symlink":
        link = tmp_path / "link"
        link.symlink_to(source.archive)
        source = NativeStagedBuildSource(source.context, link)
    elif boundary == "public":
        workspace.chmod(0o755)
    elif boundary == "reused":
        (workspace / "input").mkdir()
        (workspace / "input/keep").write_text("keep")
    arguments = dict(workspace=workspace, max_artifact_bytes=True if boundary == "limit" else 1024**2,
        max_image_archive_bytes=256 * 1024)
    if boundary == "exact":
        # Use the real same-process descriptor-scoped staging shape.
        descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            scoped = source.archive.__class__(f"/proc/self/fd/{descriptor}/source.tar")
            await module.prepare_native_runtime_input(NativeStagedBuildSource(source.context, scoped), **arguments)
        finally:
            os.close(descriptor)
        assert (workspace / "input/source.tar").read_bytes() == b"source bytes"
        assert (workspace / "input/contract.json").read_bytes() == render_native_sandbox_contract(source.context,
            max_artifact_bytes=1024**2, max_image_archive_bytes=256 * 1024)
        assert (workspace / "input").stat().st_mode & 0o777 == 0o555
        for name in ("source.tar", "contract.json"):
            assert (workspace / "input" / name).stat().st_mode & 0o777 == 0o444
        assert (workspace / "input/source.tar").stat().st_ino != source.archive.stat().st_ino
    else:
        expected = {"digest": "digest changed", "truncated": "size/type changed", "excess": "size/type changed",
            "public": "private and owner-controlled", "reused": "File exists", "limit": "integer binding"}.get(boundary)
        with pytest.raises((ValueError, OSError, RuntimeError), match=expected):
            await module.prepare_native_runtime_input(source, **arguments)
        if boundary == "reused":
            assert (workspace / "input/keep").read_text() == "keep"
        else:
            assert list(workspace.iterdir()) == []


@pytest.mark.parametrize("boundary", ["cancel", "rename"])
async def test_input_copy_settles_writes_and_anchors_cleanup(tmp_path, monkeypatch, boundary):
    module = import_module("loom_capacity_executor.native_runtime_input")
    workspace, source = prepared(tmp_path)
    entered, release = threading.Event(), threading.Event()
    original = module._write_all
    moved = tmp_path / "moved"

    def write(descriptor, data):
        entered.set()
        assert release.wait(3)
        original(descriptor, data)

    monkeypatch.setattr(module, "_write_all", write)
    task = asyncio.create_task(module.prepare_native_runtime_input(source, workspace=workspace,
        max_artifact_bytes=1024**2, max_image_archive_bytes=256 * 1024))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        if boundary == "cancel":
            task.cancel()
        else:
            workspace.rename(moved)
            workspace.mkdir(mode=0o700)
            (workspace / "keep").write_text("foreign replacement")
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError if boundary == "cancel" else ValueError):
            await task
        if boundary == "cancel":
            assert list(workspace.iterdir()) == []
        else:
            assert (workspace / "keep").read_text() == "foreign replacement"
            assert list(moved.iterdir()) == []
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_input_replacement_before_open_is_never_chmodded(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_runtime_input")
    workspace, source = prepared(tmp_path)
    original = module.os.open

    def replacing(path, flags, *args, **kwargs):
        if path == "input":
            (workspace / "input").rename(workspace / "displaced-input")
            (workspace / "input").mkdir(mode=0o555)
        return original(path, flags, *args, **kwargs)

    monkeypatch.setattr(module.os, "open", replacing)
    with pytest.raises(ValueError, match="input directory changed"):
        await module.prepare_native_runtime_input(source, workspace=workspace,
            max_artifact_bytes=1024**2, max_image_archive_bytes=256 * 1024)
    assert (workspace / "input").stat().st_mode & 0o777 == 0o555
    assert list((workspace / "input").iterdir()) == []


async def test_missing_partial_file_does_not_mask_copy_cancellation(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_runtime_input")
    workspace, source = prepared(tmp_path)

    def removed_write(descriptor, data):
        (workspace / "input/source.tar").unlink()
        raise asyncio.CancelledError

    monkeypatch.setattr(module, "_write_all", removed_write)
    with pytest.raises(asyncio.CancelledError):
        await module.prepare_native_runtime_input(source, workspace=workspace,
            max_artifact_bytes=1024**2, max_image_archive_bytes=256 * 1024)
    assert list(workspace.iterdir()) == []


async def test_replaced_source_never_becomes_verified_input(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_runtime_input")
    workspace, source = prepared(tmp_path)
    original = module._write_all

    def replace(descriptor, data):
        original(descriptor, data)
        if data == b"source bytes":
            (workspace / "input/source.tar").unlink()
            (workspace / "input/source.tar").write_bytes(b"foreign replacement")

    monkeypatch.setattr(module, "_write_all", replace)
    with pytest.raises(ValueError, match="input file changed"):
        await module.prepare_native_runtime_input(source, workspace=workspace,
            max_artifact_bytes=1024**2, max_image_archive_bytes=256 * 1024)
    assert (workspace / "input/source.tar").read_bytes() == b"foreign replacement"
    assert not (workspace / "input/contract.json").exists()


async def test_missing_earlier_file_does_not_prevent_later_cleanup(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_runtime_input")
    workspace, source = prepared(tmp_path)
    original = module._write_all

    def failed_contract(descriptor, data):
        if data != b"source bytes":
            (workspace / "input/source.tar").unlink()
            raise OSError("contract write failed")
        original(descriptor, data)

    monkeypatch.setattr(module, "_write_all", failed_contract)
    with pytest.raises(OSError, match="contract write failed"):
        await module.prepare_native_runtime_input(source, workspace=workspace,
            max_artifact_bytes=1024**2, max_image_archive_bytes=256 * 1024)
    assert list(workspace.iterdir()) == []
