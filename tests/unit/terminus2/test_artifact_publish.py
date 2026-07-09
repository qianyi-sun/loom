"""Artifact publish unit tests (#744)."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath

import pytest

from loom.agent.terminus2.runtime import _publish_harbor_artifacts_to_sandbox
from loom.driver.fake import FakeDriver


@pytest.mark.asyncio
async def test_publish_harbor_artifacts_uploads_to_loom_agent_dir(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "trajectory.json").write_text("{}", encoding="utf-8")
    (logs / "recording.cast").write_text("cast", encoding="utf-8")

    driver = FakeDriver()
    await driver.start()
    published = await _publish_harbor_artifacts_to_sandbox(
        driver,
        logs,
        PurePosixPath("/workspace"),
    )

    assert published == {
        "trajectory.json": PurePosixPath("/workspace/.loom/agent/trajectory.json"),
        "recording.cast": PurePosixPath("/workspace/.loom/agent/recording.cast"),
    }
    assert driver.filesystem[published["trajectory.json"]] == b"{}"
    assert driver.filesystem[published["recording.cast"]] == b"cast"
