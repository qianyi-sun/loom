"""Shared test fixtures for loom-launcher."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import PurePosixPath

import pytest

from loom_launcher.adapter import ExecHandle, SandboxAccess


def scripted_exec_handle(
    *,
    stdout_chunks: list[bytes],
    stderr_chunks: list[bytes] | None = None,
    return_code: int = 0,
    sandbox: SandboxAccess | None = None,
) -> ExecHandle:
    """Build an ExecHandle backed by scripted byte chunks."""
    stderr_chunks = stderr_chunks or []

    async def _stream(chunks: list[bytes]) -> AsyncIterator[bytes]:
        for c in chunks:
            yield c
            await asyncio.sleep(0)

    async def _wait() -> int:
        return return_code

    async def _kill() -> None:
        pass

    return ExecHandle(
        pid=0,
        stdout=_stream(stdout_chunks),
        stderr=_stream(stderr_chunks),
        _wait=_wait,
        _kill=_kill,
        sandbox=sandbox,
    )


class _FakeSandbox:
    """In-memory SandboxAccess. Reads return whatever the test set; exec
    returns scripted (rc, stdout) tuples by argv-join match."""

    def __init__(
        self,
        files: dict[PurePosixPath, str] | None = None,
        exec_table: dict[str, tuple[int, bytes]] | None = None,
    ) -> None:
        self.files: dict[PurePosixPath, str] = files or {}
        self.exec_table: dict[str, tuple[int, bytes]] = exec_table or {}

    async def read_text(self, path: PurePosixPath) -> str:
        if path not in self.files:
            raise FileNotFoundError(str(path))
        return self.files[path]

    async def exec_oneshot(
        self, argv: list[str], *, timeout_sec: float = 10.0,
    ) -> tuple[int, bytes]:
        key = " ".join(argv)
        return self.exec_table.get(key, (1, b""))


@pytest.fixture
def fake_sandbox() -> _FakeSandbox:
    return _FakeSandbox()


@pytest.fixture
def make_handle():
    """Factory fixture returning the scripted_exec_handle helper."""
    return scripted_exec_handle
