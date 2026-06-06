from pathlib import Path, PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult
from loom.trajectory.storage import FakeObjectStore
from loom.trial.artifacts import ArtifactCollector


@pytest.fixture
def store() -> FakeObjectStore:
    return FakeObjectStore()


@pytest.fixture
async def fake_with_artifacts(tmp_path: Path) -> FakeDriver:
    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if "find" in cmd:
            return ExecResult(
                return_code=0,
                stdout=b"/workspace/out/a.json\x00/workspace/out/b.json\x00",
                stderr=b"", truncated=False, duration_sec=0.01,
            )
        return ExecResult(return_code=0, stdout=b"", stderr=b"",
                          truncated=False, duration_sec=0.01)

    f = FakeDriver(exec_handler=handler)
    await f.start(options=StartOptions())
    f.filesystem[PurePosixPath("/workspace/out/a.json")] = b'{"a": 1}'
    f.filesystem[PurePosixPath("/workspace/out/b.json")] = b'{"b": 2}'
    return f


async def test_collect_and_upload(
    fake_with_artifacts: FakeDriver, store: FakeObjectStore, tmp_path: Path,
):
    collector = ArtifactCollector(
        store=store, bucket="artifacts",
        team_id="t1", trial_id="r1",
        step_name="main", local_root=tmp_path / "art",
    )
    prefix = await collector.collect(
        env=fake_with_artifacts,
        patterns=["out/*.json"],
    )
    assert prefix == "s3://artifacts/t1/r1/main/"
    assert ("artifacts", "t1/r1/main/out/a.json") in store.objects
    assert ("artifacts", "t1/r1/main/out/b.json") in store.objects


async def test_empty_match_is_not_an_error(
    tmp_path: Path, store: FakeObjectStore,
):
    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        return ExecResult(return_code=0, stdout=b"", stderr=b"",
                          truncated=False, duration_sec=0.01)
    f = FakeDriver(exec_handler=handler)
    await f.start(options=StartOptions())
    collector = ArtifactCollector(
        store=store, bucket="artifacts",
        team_id="t", trial_id="r", step_name="main",
        local_root=tmp_path / "art",
    )
    prefix = await collector.collect(env=f, patterns=["nope/*"])
    assert prefix == "s3://artifacts/t/r/main/"
    assert not [k for k in store.objects if k[1].startswith("t/r/main/")]
