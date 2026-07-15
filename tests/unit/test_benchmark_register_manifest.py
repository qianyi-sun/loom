from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from pydantic import ValidationError

from loom.trajectory.storage import bundle_file_metadata_sha256
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
    metadata_sha256 = bundle_file_metadata_sha256(bundle)
    object_prefix = (
        f"fake-bench/PRHW__loom-benchmark-fake/7908700/task-001/{checksum}/{metadata_sha256}/"
    )

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

    assert result.source == (f"s3://loom-benchmarks/{object_prefix}")
    assert result.uploaded == 3
    assert result.skipped == 0
    assert result.bytes_uploaded == (
        len("[task]\nid='fake-bench/task-001'\n") + len("*.pt\n") + len("echo ok\n")
    )
    assert (
        store.objects[
            (
                "loom-benchmarks",
                f"{object_prefix}.gitignore",
            )
        ]
        == b"*.pt\n"
    )
    assert (
        store.objects[
            (
                "loom-benchmarks",
                f"{object_prefix}task.toml",
            )
        ]
        == b"[task]\nid='fake-bench/task-001'\n"
    )
    assert (
        store.objects[
            (
                "loom-benchmarks",
                f"{object_prefix}solution/solve.sh",
            )
        ]
        == b"echo ok\n"
    )


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
async def test_same_bytes_with_different_modes_use_disjoint_mirror_prefixes(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "repo" / "task-001"
    (bundle / "verifier").mkdir(parents=True)
    (bundle / "task.toml").write_text("[task]\nid='fake-bench/task-001'\n")
    script = bundle / "verifier" / "run.sh"
    script.write_text("#!/bin/sh\n")
    script.chmod(0o644)
    checksum = _bundle_checksum(bundle)
    store = FakeObjectStore()

    ordinary = await mirror_manifest_task_bundle(
        repo_id="PRHW/loom-benchmark-fake",
        revision="7908700",
        task_id="fake-bench/task-001",
        checksum=checksum,
        hf_path="task-001/",
        snapshot_root=tmp_path / "repo",
        object_store=store,
        bucket="loom-benchmarks",
    )
    script.chmod(0o755)
    executable = await mirror_manifest_task_bundle(
        repo_id="PRHW/loom-benchmark-fake",
        revision="7908700",
        task_id="fake-bench/task-001",
        checksum=checksum,
        hf_path="task-001/",
        snapshot_root=tmp_path / "repo",
        object_store=store,
        bucket="loom-benchmarks",
    )

    assert ordinary.source != executable.source
    assert any(
        key.startswith(ordinary.source.removeprefix("s3://loom-benchmarks/"))
        for _, key in store.objects
    )
    assert any(
        key.startswith(executable.source.removeprefix("s3://loom-benchmarks/"))
        for _, key in store.objects
    )


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
        "FROM node:18-bookworm\nRUN npm install -g npm@latest\n",
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
    metadata_sha256 = bundle_file_metadata_sha256(bundle)
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

        def begin(self):  # type: ignore[no-untyped-def]
            class _Transaction:
                async def __aenter__(self) -> None:
                    return None

                async def __aexit__(self, *_args: object) -> None:
                    return None

            return _Transaction()

        async def scalar(self, _statement: object) -> None:
            return None

        async def execute(self, statement: FakeInsert, *_args: object) -> None:
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
        f"PRHW__loom-benchmark-fake/7908700/task-001/{checksum}/{metadata_sha256}/"
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


@pytest.mark.asyncio
async def test_download_hf_bundle_snapshot_batches_tasks_and_sleeps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from loom_benchmark_tool.register_cmd import _download_hf_bundle_snapshot

    calls: list[list[str]] = []

    def fake_snapshot_download(
        *,
        repo_id: str,
        revision: str,
        repo_type: str,
        allow_patterns: list[str],
        token: str | None,
    ) -> str:
        calls.append(list(allow_patterns))
        return str(tmp_path / "snapshot")

    monkeypatch.setattr(
        "huggingface_hub.snapshot_download",
        fake_snapshot_download,
    )

    sleeps: list[float] = []

    async def fake_sleep(secs: float) -> None:
        sleeps.append(secs)

    monkeypatch.setattr("asyncio.sleep", fake_sleep)

    tasks = [{"hf_path": f"task-{i:03d}/"} for i in range(5)]
    result = await _download_hf_bundle_snapshot(
        repo_id="PRHW/loom-benchmark-fake",
        revision="main",
        hf_token=None,
        tasks=tasks,
        chunk_size=2,
        chunk_sleep_secs=7.5,
    )

    assert result == tmp_path / "snapshot"
    # 5 tasks split into batches of 2 → three batches (2, 2, 1).
    assert [len(batch) for batch in calls] == [2, 2, 1]
    assert calls[0] == ["task-000/*", "task-001/*"]
    assert calls[1] == ["task-002/*", "task-003/*"]
    assert calls[2] == ["task-004/*"]
    # Two sleeps between three batches; no trailing sleep after the last batch.
    assert sleeps == [7.5, 7.5]


@pytest.mark.asyncio
async def test_download_hf_bundle_snapshot_single_shot_when_chunk_size_none(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from loom_benchmark_tool.register_cmd import _download_hf_bundle_snapshot

    calls: list[list[str]] = []

    def fake_snapshot_download(
        *,
        repo_id: str,
        revision: str,
        repo_type: str,
        allow_patterns: list[str],
        token: str | None,
    ) -> str:
        calls.append(list(allow_patterns))
        return str(tmp_path / "snapshot")

    monkeypatch.setattr(
        "huggingface_hub.snapshot_download",
        fake_snapshot_download,
    )

    sleeps: list[float] = []

    async def fake_sleep(secs: float) -> None:
        sleeps.append(secs)

    monkeypatch.setattr("asyncio.sleep", fake_sleep)

    tasks = [{"hf_path": f"task-{i:03d}/"} for i in range(3)]
    await _download_hf_bundle_snapshot(
        repo_id="PRHW/loom-benchmark-fake",
        revision="main",
        hf_token=None,
        tasks=tasks,
        chunk_size=None,
    )

    # Single-shot: one call with all patterns, no sleeps.
    assert len(calls) == 1
    assert calls[0] == ["task-000/*", "task-001/*", "task-002/*"]
    assert sleeps == []


class ManifestFakeObjectStore:
    """Object-store stub pre-seeded with manifest bytes; used to
    exercise source='object-store' without pulling in the real S3 client."""

    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None) -> None:
        self.objects: dict[tuple[str, str], bytes] = objects or {}

    async def ensure_bucket(self, bucket: str) -> None:
        return None

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        try:
            return self.objects[(bucket, key)]
        except KeyError as exc:
            raise FileNotFoundError(
                f"missing object s3://{bucket}/{key}"
            ) from exc

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> str:
        self.objects[(bucket, key)] = body
        return f"s3://{bucket}/{key}"


@pytest.mark.asyncio
async def test_read_manifest_from_object_store_returns_parsed_dict() -> None:
    """Happy path: manifest at the expected key returns a dict."""
    import json as _json

    from loom_benchmark_tool.register_cmd import _read_manifest_from_object_store

    manifest = {
        "benchmark_id": "fake-bench",
        "task_count": 0,
        "tasks": [],
    }
    store = ManifestFakeObjectStore(
        objects={
            ("loom-benchmarks", "fake-bench/rev-abc/manifest.json"): _json.dumps(
                manifest
            ).encode(),
        },
    )
    got = await _read_manifest_from_object_store(
        object_store=store,
        bucket="loom-benchmarks",
        benchmark_id="fake-bench",
        revision="rev-abc",
    )
    assert got == manifest


@pytest.mark.asyncio
async def test_read_manifest_from_object_store_rejects_benchmark_mismatch() -> None:
    """Manifest whose embedded benchmark_id disagrees with what we
    asked for is a red flag — someone published under the wrong slug."""
    import json as _json

    from loom_benchmark_tool.register_cmd import _read_manifest_from_object_store

    store = ManifestFakeObjectStore(
        objects={
            ("loom-benchmarks", "fake-bench/rev-abc/manifest.json"): _json.dumps(
                {"benchmark_id": "OTHER-BENCH", "tasks": []}
            ).encode(),
        },
    )
    with pytest.raises(ValueError, match="benchmark_id"):
        await _read_manifest_from_object_store(
            object_store=store,
            bucket="loom-benchmarks",
            benchmark_id="fake-bench",
            revision="rev-abc",
        )


@pytest.mark.asyncio
async def test_read_manifest_from_object_store_rejects_non_json() -> None:
    from loom_benchmark_tool.register_cmd import _read_manifest_from_object_store

    store = ManifestFakeObjectStore(
        objects={
            ("loom-benchmarks", "fake-bench/rev-abc/manifest.json"): b"not json",
        },
    )
    with pytest.raises(ValueError, match="not valid JSON"):
        await _read_manifest_from_object_store(
            object_store=store,
            bucket="loom-benchmarks",
            benchmark_id="fake-bench",
            revision="rev-abc",
        )


@pytest.mark.asyncio
async def test_run_register_source_object_store_writes_s3_task_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--source=object-store reads manifest from the bucket, task rows
    point at s3://<bucket>/<bench>/<rev>/<hf_path>, no HF fetch, no
    mirror step."""
    import json as _json

    manifest = {
        "schema_version": 3,
        "benchmark_id": "fake-bench",
        "display_name": "Fake Bench",
        "series": "fake",
        "license_spdx": "MIT",
        "license_url": "https://example/license",
        "upstream_kind": "huggingface",
        "upstream_locator": "example/fake",
        "upstream_revision": "upstream-abc",
        "splits": ["test"],
        "task_count": 1,
        "tasks": [
            {
                "task_id": "fake-bench/task-001",
                "instance_id": "task-001",
                "hf_path": "task-001/",
                "checksum": "aa" * 32,
                "license_spdx": "MIT",
                "split": "test",
                "tags": {"split": "test"},
                "task_config": _valid_task_config("fake-bench/task-001"),
            }
        ],
    }
    store = ManifestFakeObjectStore(
        objects={
            ("loom-benchmarks", "fake-bench/rev-content/manifest.json"): _json.dumps(
                manifest
            ).encode(),
        },
    )

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

    def _fake_read_manifest_from_hf(**_kwargs: object) -> dict[str, object]:
        raise AssertionError("source=object-store must not touch HF")

    monkeypatch.setattr(
        "loom_benchmark_tool.register_cmd.read_manifest_from_hf",
        _fake_read_manifest_from_hf,
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
        benchmark="fake-bench",
        source="object-store",
        db_url="postgresql://target/db",
        revision="rev-content",
        registered_by="operator",
        object_store=store,
        bucket="loom-benchmarks",
    )

    assert result["source"] == "object-store"
    assert result["registered"] == 1
    assert result["mirrored"] == 0
    assert result["repo_id"] == "s3://loom-benchmarks/fake-bench"
    assert result["revision"] == "rev-content"

    task_insert = executed[-1]
    assert task_insert.payload["source"] == (
        "s3://loom-benchmarks/fake-bench/rev-content/task-001/"
    )
    tags = task_insert.payload["tags"]
    assert isinstance(tags, dict)
    assert tags["split"] == "test"
    assert tags["runtime_source_kind"] == "internal_object_store"


@pytest.mark.asyncio
async def test_run_register_source_object_store_requires_object_store() -> None:
    with pytest.raises(ValueError, match="object_store"):
        await run_register(
            benchmark="fake-bench",
            source="object-store",
            db_url="postgresql://x/y",
            revision="rev-content",
        )


@pytest.mark.asyncio
async def test_run_register_source_object_store_requires_explicit_revision() -> None:
    """The HF default of 'main' isn't meaningful for content-addressed
    revisions; force operators to pass the one publish emitted."""
    with pytest.raises(ValueError, match="explicit --revision"):
        await run_register(
            benchmark="fake-bench",
            source="object-store",
            db_url="postgresql://x/y",
            object_store=ManifestFakeObjectStore(),
        )


@pytest.mark.asyncio
async def test_run_register_source_object_store_rejects_mirror_flag() -> None:
    """--mirror-to-object-store on top of --source=object-store is
    redundant and a sign of operator confusion; refuse loudly."""
    with pytest.raises(ValueError, match="redundant"):
        await run_register(
            benchmark="fake-bench",
            source="object-store",
            db_url="postgresql://x/y",
            revision="rev-content",
            object_store=ManifestFakeObjectStore(),
            mirror_to_object_store=True,
        )
