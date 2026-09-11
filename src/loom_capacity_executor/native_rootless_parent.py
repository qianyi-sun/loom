"""Validate the pinned RootlessKit parent chain before any broker can start."""

from __future__ import annotations

import os
import stat
import time
from pathlib import Path

from loom_capacity_executor.native_parent_death import bind_native_parent_death


def _process_parent(pid: int) -> int:
    if type(pid) is not int or pid <= 1:
        raise ValueError("native rootless process identity is invalid")
    descriptor = os.open(f"/proc/{pid}/stat", os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        wire = os.read(descriptor, 4097)
    finally:
        os.close(descriptor)
    if len(wire) > 4096 or not wire.startswith(f"{pid} (".encode()):
        raise ValueError("native rootless parent process metadata changed")
    # comm may contain spaces and parentheses; numeric fields follow its last ).
    fields = wire.rpartition(b")")[2].split()
    if len(fields) < 2 or not fields[1].isdigit():
        raise ValueError("native rootless parent process metadata is invalid")
    return int(fields[1])


def bind_native_rootless_parent(state_directory: Path, *, expected_rootless_pid: int) -> int:
    """Reject adoption, including adoption by a Slurm subreaper whose PID > 1.

    The fixed outer launcher supplies its own pre-exec RootlessKit PID and owns
    its unreaped child handle. No PID namespace is introduced. RootlessKit 3.1
    documents child_pid as its mapped keeper PID; that keeper must still be our
    direct parent and a child of the original RootlessKit process. Fresh execution
    authority and allocation containment remain separate requirements.
    """
    if type(expected_rootless_pid) is not int or expected_rootless_pid <= 1:
        raise ValueError("native original RootlessKit identity is invalid")
    parent = os.getppid()
    bind_native_parent_death(parent)
    if _process_parent(parent) != expected_rootless_pid:
        raise RuntimeError("native mapped target was reparented")
    if not state_directory.is_absolute() or state_directory == Path("/") or ".." in state_directory.parts:
        raise ValueError("native rootless state directory is invalid")
    directory = os.open(state_directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(directory)
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
            raise ValueError("native rootless state must be private and owner-controlled")
        until = time.clock_gettime_ns(time.CLOCK_BOOTTIME) + 5_000_000_000
        while True:
            try:
                descriptor = os.open("child_pid", os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            except FileNotFoundError:
                wire = b""
            else:
                try:
                    metadata = os.fstat(descriptor)
                    if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
                        or stat.S_IMODE(metadata.st_mode) != 0o444 or metadata.st_nlink != 1):
                        raise ValueError("native rootless child identity file is invalid")
                    wire = os.read(descriptor, 32)
                finally:
                    os.close(descriptor)
            if wire:
                if wire != str(parent).encode("ascii"):
                    raise ValueError("native rootless child identity changed")
                break
            # RootlessKit's WriteFile creates an empty 0444 inode before writing.
            # Only absence or metadata-valid empty content can await publication;
            # malformed/nonempty identities are never retried.
            if _process_parent(parent) != expected_rootless_pid or time.clock_gettime_ns(time.CLOCK_BOOTTIME) >= until:
                raise RuntimeError("native rootless child identity did not become ready") from None
            time.sleep(0.01)
        if _process_parent(parent) != expected_rootless_pid:
            raise RuntimeError("native mapped parent changed during validation")
        bind_native_parent_death(parent)
        return parent
    finally:
        os.close(directory)
