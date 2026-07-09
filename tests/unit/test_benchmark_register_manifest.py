from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from pydantic import ValidationError

from loom_benchmark_tool.register_cmd import (
    mirror_manifest_task_bundle,
    run_register,
    task_config_from_manifest_entry,
)


def _valid_task_config(task_id: str = "fake-bench/task-001") -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": "Fake task"},
        "environment": {"os": "linux", "docker_image": "python:3.12-slim"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


def _bundle_checksum(bundle_dir: Path) -> str:
    hasher = hashlib.sha256()
    for path in sorted(bundle_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(bundle_dir).as_posix().encode("utf-8")
        hasher.update(b"\x00" + rel + b"\x00")
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def test_manifest_entry_with_valid_task_config_returns_config() -> None:
    cfg = _valid_task_config()
    assert task_config_from_manifest_entry({"task_config": cfg}) == cfg


def test_manifest_entry_without_task_config_remains_legacy_placeholder() -> None:
    assert task_config_from_manifest_entry({"task_id": "fake-bench/task-001"}) == {}


def test_manifest_entry_with_invalid_task_config_raises_validation_error() -> None:
    with pytest.raises(ValidationError):
        task_config_from_manifest_entry({"task_config": {"task": {"id": "broken"}}})


def test_manifest_entry_rejects_mismatched_task_config_id() -> None:
    cfg = _valid_task_config("fake-bench/different")
    with pytest.raises(ValueError, match=r"task_config\.task\.id"):
        task_config_from_manifest_entry(
            {
                "task_id": "fake-bench/task-001",
                "task_config": cfg,
            }
        )


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    async def ensure_bucket(self, bucket: str) -> None:
        self.objects.setdefault((bucket, ".bucket"), b"")

    async def head_object(self, *, bucket: str, key: str):
        body = self.objects.get((bucket, key))
        if body is None:
            return None
        return type("ObjectInfo", (), {"size": len(body)})()

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)]

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> None:
        self.objects[(bucket, key)] = body


@pytest.mark.asyncio
async def test_mirror_manifest_task_bundle_uploads_hf_files_to_internal_store(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "repo" / "task-001"
    (bundle / "solution").mkdir(parents=True)
    (bundle / "task.toml").write_text("[task]\nid='fake-bench/task-001'\n")
    (bundle / ".gitignore").write_text("*.pt\n")
    (bundle / "solution" / "solve.sh").write_text("echo ok\n")
    checksum = _bundle_checksum(bundle)

    store = FakeObjectStore()

    result = await mirror_manifest_task_bundle(
        repo_id="PRHW/loom-benchmark-fake",
        revision="7908700",
        task_id="fake-bench/task-001",
        checksum=checksum,
        hf_path="task-001/",
        snapshot_root=tmp_path / "repo",
        object_store=store,
        bucket="loom-benchmarks",
    )

    assert result.source == (
        "s3://loom-benchmarks/fake-bench/"
        f"PRHW__loom-benchmark-fake/7908700/task-001/{checksum}/"
    )
    assert result.uploaded == 3
    assert result.skipped == 0
    assert result.bytes_uploaded == (
        len("[task]\nid='fake-bench/task-001'\n") + len("*.pt\n") + len("echo ok\n")
    )
    assert store.objects[
        (
            "loom-benchmarks",
            "fake-bench/PRHW__loom-benchmark-fake/7908700/"
            f"task-001/{checksum}/.gitignore",
        )
    ] == b"*.pt\n"
    assert store.objects[
        (
            "loom-benchmarks",
            "fake-bench/PRHW__loom-benchmark-fake/7908700/"
            f"task-001/{checksum}/task.toml",
        )
    ] == b"[task]\nid='fake-bench/task-001'\n"
    assert store.objects[
        (
            "loom-benchmarks",
            "fake-bench/PRHW__loom-benchmark-fake/7908700/"
            f"task-001/{checksum}/solution/solve.sh",
        )
    ] == b"echo ok\n"


@pytest.mark.asyncio
async def test_mirror_manifest_task_bundle_skips_matching_internal_objects(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "repo" / "task-001"
    bundle.mkdir(parents=True)
    (bundle / "task.toml").write_text("[task]\nid='fake-bench/task-001'\n")
    checksum = _bundle_checksum(bundle)
    store = FakeObjectStore()

    first = await mirror_manifest_task_bundle(
        repo_id="PRHW/loom-benchmark-fake",
        revision="7908700",
        task_id="fake-bench/task-001",
        checksum=checksum,
        hf_path="task-001/",
        snapshot_root=tmp_path / "repo",
        object_store=store,
        bucket="loom-benchmarks",
    )
    second = await mirror_manifest_task_bundle(
        repo_id="PRHW/loom-benchmark-fake",
        revision="7908700",
        task_id="fake-bench/task-001",
        checksum=checksum,
        hf_path="task-001/",
        snapshot_root=tmp_path / "repo",
        object_store=store,
        bucket="loom-benchmarks",
    )

    assert first.uploaded == 1
    assert second.uploaded == 0
    assert second.skipped == 1
    assert second.bytes_skipped == len("[task]\nid='fake-bench/task-001'\n")


@pytest.mark.asyncio
async def test_mirror_manifest_task_bundle_rejects_checksum_drift(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "repo" / "task-001"
    bundle.mkdir(parents=True)
    (bundle / "task.toml").write_text("[task]\nid='fake-bench/task-001'\n")

    with pytest.raises(ValueError, match="checksum mismatch"):
        await mirror_manifest_task_bundle(
            repo_id="PRHW/loom-benchmark-fake",
            revision="7908700",
            task_id="fake-bench/task-001",
            checksum="0" * 64,
            hf_path="task-001/",
            snapshot_root=tmp_path / "repo",
            object_store=FakeObjectStore(),
            bucket="loom-benchmarks",
        )


@pytest.mark.asyncio
async def test_mirror_manifest_task_bundle_rejects_unsafe_dockerfile(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "repo" / "task-001"
    (bundle / "environment").mkdir(parents=True)
    (bundle / "task.toml").write_text("[task]\nid='fake-bench/task-001'\n")
    (bundle / "environment" / "Dockerfile").write_text(
        "FROM node:18-bookworm\n"
        "RUN npm install -g npm@latest\n",
    )
    checksum = _bundle_checksum(bundle)

    with pytest.raises(ValueError, match="npm@latest"):
        await mirror_manifest_task_bundle(
            repo_id="PRHW/loom-benchmark-fake",
            revision="7908700",
            task_id="fake-bench/task-001",
            checksum=checksum,
            hf_path="task-001/",
            snapshot_root=tmp_path / "repo",
            object_store=FakeObjectStore(),
            bucket="loom-benchmarks",
        )


@pytest.mark.asyncio
async def test_run_register_mirror_writes_internal_source_and_hf_provenance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "repo" / "task-001"
    bundle.mkdir(parents=True)
    (bundle / "task.toml").write_text("[task]\nid='fake-bench/task-001'\n")
    checksum = _bundle_checksum(bundle)
    manifest = {
        "benchmark_id": "fake-bench",
        "display_name": "Fake Bench",
        "upstream_kind": "huggingface",
        "upstream_locator": "example/fake",
        "upstream_revision": "upstream-main",
        "license_spdx": "MIT",
        "license_url": "https://example/license",
        "splits": ["test"],
        "tasks": [
            {
                "task_id": "fake-bench/task-001",
                "checksum": checksum,
                "hf_path": "task-001/",
                "license_spdx": "MIT",
                "tags": {"split": "test"},
                "task_config": _valid_task_config("fake-bench/task-001"),
            }
        ],
    }
    class FakeExcluded:
        def __getattr__(self, name: str) -> str:
            return f"excluded.{name}"

    class FakeInsert:
        def __init__(self, model: object) -> None:
            self.model = model
            self.payload: dict[str, object] = {}
            self.excluded = FakeExcluded()

        def values(self, **kwargs: object) -> FakeInsert:
            self.payload = kwargs
            return self

        def on_conflict_do_update(self, **_kwargs: object) -> FakeInsert:
            return self

    class FakeEngine:
        async def dispose(self) -> None:
            return None

    class FakeSession:
        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def execute(self, statement: FakeInsert) -> None:
            executed.append(statement)

        async def commit(self) -> None:
            return None

    executed: list[FakeInsert] = []

    async def fake_download_hf_bundle_snapshot(**_kwargs: object) -> Path:
        return tmp_path / "repo"

    monkeypatch.setattr(
        "loom_benchmark_tool.register_cmd.read_manifest_from_hf",
        lambda **_kwargs: manifest,
    )
    monkeypatch.setattr(
        "loom_benchmark_tool.register_cmd._download_hf_bundle_snapshot",
        fake_download_hf_bundle_snapshot,
    )
    monkeypatch.setattr(
        "loom_benchmark_tool.register_cmd.create_async_engine",
        lambda _db_url: FakeEngine(),
    )
    monkeypatch.setattr(
        "loom_benchmark_tool.register_cmd.async_sessionmaker",
        lambda *_args, **_kwargs: lambda: FakeSession(),
    )
    monkeypatch.setattr(
        "loom_benchmark_tool.register_cmd.pg_insert",
        lambda model: FakeInsert(model),
    )

    result = await run_register(
        benchmark="fake",
        hf_org="PRHW",
        hf_token="hf_secret",
        db_url="postgresql://target/db",
        revision="7908700",
        registered_by="operator",
        mirror_to_object_store=True,
        object_store=FakeObjectStore(),
        bucket="loom-benchmarks",
    )

    task_insert = executed[-1]
    tags = task_insert.payload["tags"]
    assert isinstance(tags, dict)
    assert task_insert.payload["source"] == (
        "s3://loom-benchmarks/fake-bench/"
        f"PRHW__loom-benchmark-fake/7908700/task-001/{checksum}/"
    )
    assert tags == {
        "split": "test",
        "hf_repo_id": "PRHW/loom-benchmark-fake",
        "hf_revision": "7908700",
        "hf_path": "task-001/",
        "hf_checksum": checksum,
        "runtime_source_kind": "internal_object_store",
        "runtime_source_mirrored_at": tags["runtime_source_mirrored_at"],
    }
    assert "hf_secret" not in repr(task_insert.payload)
    assert result["registered"] == 1
    assert result["mirrored"] == 1
    assert result["mirror_uploaded"] == 1
