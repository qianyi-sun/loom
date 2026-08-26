from pathlib import Path, PurePosixPath
from types import SimpleNamespace

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
    collection = await collector.collect(
        env=fake_with_artifacts,
        patterns=["out/*.json"],
    )
    assert collection.prefix == "s3://artifacts/t1/r1/main/"
    assert [a.key for a in collection.artifacts] == [
        "t1/r1/main/out/a.json",
        "t1/r1/main/out/b.json",
    ]
    assert [a.size for a in collection.artifacts] == [8, 8]
    assert ("artifacts", "t1/r1/main/out/a.json") in store.objects
    assert ("artifacts", "t1/r1/main/out/b.json") in store.objects


async def test_collect_preserves_post_upload_object_version(
    fake_with_artifacts: FakeDriver,
    tmp_path: Path,
) -> None:
    class VersionedStore(FakeObjectStore):
        async def put_object_with_metadata(
            self,
            *,
            bucket: str,
            key: str,
            body: bytes,
        ) -> object:
            uri = await super().put_object(bucket=bucket, key=key, body=body)
            return SimpleNamespace(uri=uri, version_id=f"version:{key}")

    collector = ArtifactCollector(
        store=VersionedStore(),
        bucket="artifacts",
        team_id="t1",
        trial_id="r1",
        step_name="main",
        local_root=tmp_path / "art",
    )

    collection = await collector.collect(
        env=fake_with_artifacts,
        patterns=["out/*.json"],
    )

    assert [artifact.version_id for artifact in collection.artifacts] == [
        "version:t1/r1/main/out/a.json",
        "version:t1/r1/main/out/b.json",
    ]


async def test_collect_marks_secret_like_artifacts_blocked(
    store: FakeObjectStore, tmp_path: Path,
):
    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if "find" in cmd:
            return ExecResult(
                return_code=0,
                stdout=b"/workspace/out/safe.txt\x00/workspace/out/secret.txt\x00",
                stderr=b"", truncated=False, duration_sec=0.01,
            )
        return ExecResult(return_code=0, stdout=b"", stderr=b"",
                          truncated=False, duration_sec=0.01)

    f = FakeDriver(exec_handler=handler)
    await f.start(options=StartOptions())
    f.filesystem[PurePosixPath("/workspace/out/safe.txt")] = b"score=0.82\n"
    f.filesystem[PurePosixPath("/workspace/out/secret.txt")] = (
        b"OPENAI_API_KEY=sk-artifact-secret\n"
        b"Authorization: Bearer loom_api_artifactsecret\n"
    )
    collector = ArtifactCollector(
        store=store, bucket="artifacts",
        team_id="t1", trial_id="r1",
        step_name="main", local_root=tmp_path / "art",
    )

    collection = await collector.collect(env=f, patterns=["out/*.txt"])
    by_name = {Path(a.key).name: a for a in collection.artifacts}

    assert by_name["safe.txt"].share_status == "shared"
    assert by_name["safe.txt"].blocked_reason is None
    assert by_name["secret.txt"].share_status == "blocked"
    assert by_name["secret.txt"].blocked_reason == "secret-like content detected"
    assert "sk-artifact-secret" not in by_name["secret.txt"].blocked_reason


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
    collection = await collector.collect(env=f, patterns=["nope/*"])
    assert collection.prefix == "s3://artifacts/t/r/main/"
    assert collection.artifacts == []
    assert not [k for k in store.objects if k[1].startswith("t/r/main/")]


async def test_task_globs_cannot_collect_reserved_verifier_namespace(
    tmp_path: Path,
    store: FakeObjectStore,
) -> None:
    normal = PurePosixPath("/workspace/result.txt")
    trusted = PurePosixPath("/workspace/.loom/verifier/script.log")
    planted = PurePosixPath("/workspace/.loom/verifier/agent-planted.txt")

    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if "/workspace/.loom/verifier/script.log" in cmd:
            stdout = f"{trusted}\0{planted}\0".encode()
        else:
            stdout = f"{normal}\0{trusted}\0{planted}\0".encode()
        return ExecResult(
            return_code=0,
            stdout=stdout,
            stderr=b"",
            duration_sec=0.01,
        )

    driver = FakeDriver(exec_handler=handler)
    await driver.start(options=StartOptions())
    driver.filesystem[normal] = b"normal"
    driver.filesystem[trusted] = b"trusted verifier log"
    driver.filesystem[planted] = b"agent planted"
    collector = ArtifactCollector(
        store=store,
        bucket="artifacts",
        team_id="t",
        trial_id="r",
        step_name="main",
        local_root=tmp_path / "art",
    )

    collection = await collector.collect(
        env=driver,
        patterns=["*", ".loom/*"],
        platform_patterns=[
            ".loom/verifier/*",
            ".loom/verifier/script.log",
        ],
    )

    assert {artifact.key for artifact in collection.artifacts} == {
        "t/r/main/result.txt",
        "t/r/main/.loom/verifier/script.log",
    }
    assert ("artifacts", "t/r/main/.loom/verifier/agent-planted.txt") not in (
        store.objects
    )
