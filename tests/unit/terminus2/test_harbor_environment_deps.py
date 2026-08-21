"""Sandbox dependency preparation for the built-in Terminus2 runtime."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import PurePosixPath

import pytest

from loom.agent.terminus2.harbor_environment import ensure_sandbox_deps
from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult


@pytest.mark.asyncio
async def test_ensure_sandbox_deps_installs_bash_on_alpine() -> None:
    seen: list[str] = []

    def handler(
        cmd: str,
        user: str | int | None,
        cwd: PurePosixPath | None,
        env: Mapping[str, str] | None,
    ) -> ExecResult:
        seen.append(cmd)
        return ExecResult(
            return_code=0,
            stdout=b"tmux 3.5a",
            stderr=b"",
            truncated=False,
            duration_sec=0.0,
        )

    driver = FakeDriver(exec_handler=handler)
    await driver.start()

    await ensure_sandbox_deps(driver)

    assert len(seen) == 1
    assert "apk add --no-cache bash tmux asciinema" in seen[0]
