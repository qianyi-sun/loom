"""Harbor environment bridge unit tests (#744)."""

from __future__ import annotations

import tempfile
from pathlib import PurePosixPath
from uuid import uuid4

import pytest

from loom.agent.terminus2.harbor_environment import (
    LoomHarborEnvironment,
    make_trial_paths,
)
from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult

harbor = pytest.importorskip("harbor")


@pytest.mark.asyncio
async def test_loom_harbor_environment_exec_delegates_to_driver() -> None:
    seen: list[str] = []

    async def handler(cmd: str, user, cwd, env) -> ExecResult:
        seen.append(cmd)
        return ExecResult(return_code=0, stdout=b"ok", stderr=b"")

    driver = FakeDriver(exec_handler=handler)
    await driver.start()
    logs = make_trial_paths(__import__("pathlib").Path(tempfile.mkdtemp()))
    env = LoomHarborEnvironment.create(
        driver=driver,
        trial_paths=logs,
        workdir=PurePosixPath("/workspace"),
        trial_id=uuid4(),
        step_id="agent",
    )
    result = await env.exec("echo hi", cwd="/workspace")
    assert result.return_code == 0
    assert result.stdout == "ok"
    assert seen == ["echo hi"]
