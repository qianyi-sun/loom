"""Copy scoped verified source into fixed readonly runtime inputs; never extract."""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path

from loom_capacity_agent.build_admission import BuildSourceContextV1
from loom_capacity_executor.native_build_source import (
    NativeStagedBuildSource,
    _settled_io,
    _write_all,
)
from loom_capacity_executor.native_sandbox_contract import render_native_sandbox_contract


def _identity(metadata: os.stat_result) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


async def prepare_native_runtime_input(source: NativeStagedBuildSource, *, workspace: Path,
    max_artifact_bytes: int, max_image_archive_bytes: int,
) -> None:
    """Caller owns a fresh private attempt workspace and its later teardown.

    Keep the source staging scope open here. Only archive bytes and the existing
    authority-free build contract are copied; no credentials or proc-FD paths
    cross into a child. Installation, one-shot fencing and startup remain separate.
    """
    context = BuildSourceContextV1.model_validate_json(source.context.model_dump_json())
    contract = render_native_sandbox_contract(context, max_artifact_bytes=max_artifact_bytes,
        max_image_archive_bytes=max_image_archive_bytes)
    if not workspace.is_absolute() or workspace == Path("/") or ".." in workspace.parts:
        raise ValueError("native runtime input workspace is invalid")
    workspace_fd = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        workspace_metadata = os.fstat(workspace_fd)
        if workspace_metadata.st_uid != os.geteuid() or stat.S_IMODE(workspace_metadata.st_mode) != 0o700:
            raise ValueError("native runtime input workspace must be private and owner-controlled")
        source_fd = os.open(source.archive, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            metadata = os.fstat(source_fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != context.archive_size_bytes:
                raise ValueError("native runtime source archive size/type changed")
            os.mkdir("input", mode=0o700, dir_fd=workspace_fd)
            input_metadata = os.stat("input", dir_fd=workspace_fd, follow_symlinks=False)
            input_fd = None
            input_matched = False
            created: dict[str, tuple[int, int]] = {}
            completed = False
            try:
                input_fd = os.open("input", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=workspace_fd)
                if _identity(os.fstat(input_fd)) != _identity(input_metadata):
                    raise ValueError("native runtime input directory changed")
                input_matched = True
                for name in ("source.tar", "contract.json"):
                    output_fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                        0o600, dir_fd=input_fd)
                    created[name] = _identity(os.fstat(output_fd))
                    try:
                        if name == "source.tar":
                            digest = hashlib.sha256()
                            copied = 0
                            while copied < context.archive_size_bytes:
                                chunk = await _settled_io(os.read, source_fd, min(1024**2, context.archive_size_bytes - copied))
                                if not chunk:
                                    raise ValueError("native runtime source archive was truncated")
                                await _settled_io(_write_all, output_fd, chunk)
                                digest.update(chunk)
                                copied += len(chunk)
                            if await _settled_io(os.read, source_fd, 1) or digest.hexdigest() != context.archive_sha256:
                                raise ValueError("native runtime source archive digest changed")
                        else:
                            await _settled_io(_write_all, output_fd, contract)
                        os.fchmod(output_fd, 0o444)
                        await _settled_io(os.fsync, output_fd)
                    finally:
                        os.close(output_fd)
                os.fchmod(input_fd, 0o555)
                await _settled_io(os.fsync, input_fd)
                await _settled_io(os.fsync, workspace_fd)
                if (_identity(workspace.lstat()) != _identity(workspace_metadata)
                    or _identity(os.stat("input", dir_fd=workspace_fd, follow_symlinks=False)) != _identity(input_metadata)):
                    raise ValueError("native runtime input path changed during preparation")
                for name, identity in created.items():
                    try:
                        metadata = os.stat(name, dir_fd=input_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        raise ValueError("native runtime input file changed during preparation") from None
                    size = context.archive_size_bytes if name == "source.tar" else len(contract)
                    if (_identity(metadata) != identity or not stat.S_ISREG(metadata.st_mode)
                        or metadata.st_uid != os.geteuid() or metadata.st_nlink != 1
                        or stat.S_IMODE(metadata.st_mode) != 0o444 or metadata.st_size != size):
                        raise ValueError("native runtime input file changed during preparation")
                completed = True
            finally:
                try:
                    if not completed:
                        if input_fd is not None and input_matched:
                            os.fchmod(input_fd, 0o700)
                            for name, identity in created.items():
                                # Never remove a replacement installed by another actor.
                                try:
                                    metadata = os.stat(name, dir_fd=input_fd, follow_symlinks=False)
                                except FileNotFoundError:
                                    continue
                                if _identity(metadata) == identity:
                                    os.unlink(name, dir_fd=input_fd)
                        try:
                            current_input = os.stat("input", dir_fd=workspace_fd, follow_symlinks=False)
                        except FileNotFoundError:
                            pass
                        else:
                            if (_identity(current_input) == _identity(input_metadata)
                                and (input_fd is None or (input_matched and not os.listdir(input_fd)))):
                                os.rmdir("input", dir_fd=workspace_fd)
                finally:
                    if input_fd is not None:
                        os.close(input_fd)
        finally:
            os.close(source_fd)
    finally:
        os.close(workspace_fd)
