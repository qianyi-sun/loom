"""Unit tests for the publish_cmd manifest schema + helpers.

The full publish round-trip (convert + push to HF) is exercised by an
operator running `loom_benchmark_tool publish` against a live HF token
— covering it in CI would require a fake HF server and is out of scope
here. These tests pin the pure-Python pieces: checksum stability,
safe-dirname, repo-id derivation."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from loom_benchmarks.base import BenchmarkInstance, ConvertedTask, UpstreamSource
from loom_benchmarks.util import sha256_of_dir

from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME
from loom_benchmark_tool.publish_cmd import (
    MANIFEST_SCHEMA_VERSION,
    _bundle_checksum,
    _safe_dirname,
    repo_id_for,
)

_TASK_TOML = """\
schema_version = "1"

[task]
id = "fake-bench/task-001"
name = "Fake task"

[environment]
os = "linux"
docker_image = "python:3.12-slim"

[agent]
name = "oracle"

[verifier]
name = "pytest"

[[steps]]
name = "main"
"""


def _task_toml(task_id: str) -> str:
    return _TASK_TOML.replace("fake-bench/task-001", task_id)


def test_repo_id_for_uses_loom_benchmark_prefix() -> None:
    assert repo_id_for("PRHW", "humaneval") == "PRHW/loom-benchmark-humaneval"


def test_safe_dirname_collapses_slashes() -> None:
    assert _safe_dirname("HumanEval/0") == "HumanEval_0"
    assert _safe_dirname("plain") == "plain"


def test_bundle_checksum_is_stable_across_invocations(tmp_path: Path) -> None:
    """Same bytes in different file orders → same digest. The hash
    iterates files in sorted-relpath order so adding/removing files
    in iteration order can't affect the digest."""
    a = tmp_path / "a"
    a.mkdir()
    (a / "task.toml").write_text("[task]\nid='t'\n")
    (a / "solution.py").write_text("print(1)\n")

    digest1 = _bundle_checksum(a)
    digest2 = _bundle_checksum(a)
    assert digest1 == digest2
    assert len(digest1) == 64


def test_bundle_checksum_differs_on_content_change(tmp_path: Path) -> None:
    """Editing any file should perturb the digest — sanity-check the
    hash includes the file bytes, not just the relpaths."""
    a = tmp_path / "a"
    a.mkdir()
    (a / "task.toml").write_text("[task]\nid='t'\n")
    digest1 = _bundle_checksum(a)
    (a / "task.toml").write_text("[task]\nid='different'\n")
    digest2 = _bundle_checksum(a)
    assert digest1 != digest2


def test_bundle_checksum_includes_task_dotfiles(tmp_path: Path) -> None:
    bundle = tmp_path / "a"
    bundle.mkdir()
    (bundle / "task.toml").write_text("[task]\nid='t'\n")
    (bundle / ".gitignore").write_text("*.pt\n")

    digest1 = _bundle_checksum(bundle)
    (bundle / ".gitignore").write_text("*.pt\n*.bin\n")
    digest2 = _bundle_checksum(bundle)

    assert digest1 != digest2


def test_manifest_schema_version_is_int() -> None:
    """Operators (and the worker) fork on this; an accidental change
    to a string would silently break the manifest reader."""
    assert isinstance(MANIFEST_SCHEMA_VERSION, int)
    assert MANIFEST_SCHEMA_VERSION >= 3


@pytest.mark.asyncio
async def test_run_publish_includes_valid_task_config_in_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from loom_benchmark_tool import publish_cmd

    class FakeAdapter:
        name = "fake-bench"
        display_name = "Fake Bench"
        upstream_source = UpstreamSource(kind="huggingface", locator="fake/source")
        license_spdx = "MIT"
        license_url = ""
        license_execution_policy = "notice"
        splits = ("test",)
        series = "fake"

        def list_instances(
            self,
            *,
            source_dir: Path,
            split: str,
        ) -> list[BenchmarkInstance]:
            return [
                BenchmarkInstance(
                    instance_id="task-001",
                    split=split,
                    raw={},
                    tags={"difficulty": "smoke"},
                ),
            ]

        def convert_instance(
            self,
            instance: BenchmarkInstance,
            *,
            out_dir: Path,
        ) -> ConvertedTask:
            (out_dir / "task.toml").write_text(_TASK_TOML)
            (out_dir / "instruction.md").write_text("solve it\n")
            return ConvertedTask(
                task_id="fake-bench/task-001",
                checksum=sha256_of_dir(out_dir),
                license_spdx="MIT",
                warnings=(),
            )

    captured_manifest: dict[str, Any] = {}
    captured_bundle_checksums: dict[str, str] = {}
    captured_sidecars: dict[str, bytes] = {}

    class FakeHfApi:
        def __init__(self, *, token: str) -> None:
            self.token = token

        def create_repo(self, **_kwargs: object) -> None:
            return None

        def upload_large_folder(self, **kwargs: object) -> None:
            folder = Path(str(kwargs["folder_path"]))
            captured_manifest.update(
                json.loads((folder / "manifest.json").read_text()),
            )
            bundle = folder / "task-001"
            sidecar = bundle / BUNDLE_FILE_METADATA_NAME
            captured_sidecars["task-001"] = sidecar.read_bytes()
            sidecar.unlink()
            captured_bundle_checksums["task-001"] = sha256_of_dir(bundle)

        def list_repo_refs(self, **_kwargs: object) -> object:
            return SimpleNamespace(
                branches=[SimpleNamespace(name="main", target_commit="fake-revision")]
            )

    monkeypatch.setitem(publish_cmd.REGISTRY, "fake-bench", FakeAdapter())
    monkeypatch.setattr(
        publish_cmd,
        "fetch_upstream",
        lambda *_args, **_kwargs: tmp_path,
    )
    monkeypatch.setattr(publish_cmd, "HfApi", FakeHfApi)

    stats = await publish_cmd.run_publish(
        benchmark="fake-bench",
        hf_org="fake-org",
        hf_token="fake-token",
        cache_dir=tmp_path / "cache",
    )

    assert stats["published"] == 1
    assert stats["warnings"] == 0
    assert captured_manifest["schema_version"] == MANIFEST_SCHEMA_VERSION
    task = cast(dict[str, Any], captured_manifest["tasks"][0])
    assert task["checksum"] == captured_bundle_checksums["task-001"]
    assert captured_sidecars["task-001"]
    assert task["task_config"]["task"]["id"] == "fake-bench/task-001"
    assert task["task_config"]["environment"]["docker_image"] == "python:3.12-slim"
    assert task["tags"] == {
        "difficulty": "smoke",
        "license_execution_policy": "notice",
    }


def test_run_publish_rejects_unsafe_converted_dockerfile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from loom_benchmark_tool import publish_cmd

    class FakeAdapter:
        name = "fake-bench"
        display_name = "Fake Bench"
        upstream_source = UpstreamSource(kind="huggingface", locator="fake/source")
        license_spdx = "MIT"
        license_url = ""
        splits = ("test",)
        series = "fake"

        def list_instances(
            self,
            *,
            source_dir: Path,
            split: str,
        ) -> list[BenchmarkInstance]:
            return [BenchmarkInstance(instance_id="task-001", split=split, raw={})]

        def convert_instance(
            self,
            instance: BenchmarkInstance,
            *,
            out_dir: Path,
        ) -> ConvertedTask:
            (out_dir / "task.toml").write_text(_task_toml("fake-bench/task-001"))
            dockerfile = out_dir / "environment" / "Dockerfile"
            dockerfile.parent.mkdir()
            dockerfile.write_text(
                "FROM python:3.13-bookworm\n"
                "RUN pip install torch pyyaml "
                "--index-url https://download.pytorch.org/whl/cpu\n",
            )
            return ConvertedTask(
                task_id="fake-bench/task-001",
                checksum=sha256_of_dir(out_dir),
                license_spdx="MIT",
                warnings=(),
            )

    class FakeHfApi:
        def __init__(self, *, token: str) -> None:
            self.token = token

        def create_repo(self, **_kwargs: object) -> None:
            return None

        def upload_large_folder(self, **_kwargs: object) -> None:
            raise AssertionError("unsafe bundle should not upload")

        def list_repo_refs(self, **_kwargs: object) -> object:
            raise AssertionError("unsafe bundle should not resolve revision")

    monkeypatch.setitem(publish_cmd.REGISTRY, "fake-bench", FakeAdapter())
    monkeypatch.setattr(
        publish_cmd,
        "fetch_upstream",
        lambda *_args, **_kwargs: tmp_path,
    )
    monkeypatch.setattr(publish_cmd, "HfApi", FakeHfApi)

    with pytest.raises(ValueError, match="package-specific pip index"):
        import asyncio
        asyncio.run(
            publish_cmd.run_publish(
                benchmark="fake-bench",
                hf_org="fake-org",
                hf_token="fake-token",
                cache_dir=tmp_path / "cache",
            )
        )


@pytest.mark.asyncio
async def test_run_publish_filters_specific_instance_ids(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from loom_benchmark_tool import publish_cmd

    class FakeAdapter:
        name = "fake-bench"
        display_name = "Fake Bench"
        upstream_source = UpstreamSource(kind="huggingface", locator="fake/source")
        license_spdx = "MIT"
        license_url = ""
        splits = ("test",)
        series = "fake"

        def list_instances(
            self,
            *,
            source_dir: Path,
            split: str,
        ) -> list[BenchmarkInstance]:
            return [
                BenchmarkInstance(instance_id="task-001", split=split, raw={}),
                BenchmarkInstance(instance_id="task-002", split=split, raw={}),
            ]

        def convert_instance(
            self,
            instance: BenchmarkInstance,
            *,
            out_dir: Path,
        ) -> ConvertedTask:
            task_id = f"fake-bench/{instance.instance_id}"
            (out_dir / "task.toml").write_text(_task_toml(task_id))
            (out_dir / "instruction.md").write_text("solve it\n")
            return ConvertedTask(
                task_id=task_id,
                checksum=_bundle_checksum(out_dir),
                license_spdx="MIT",
                warnings=(),
            )

    captured_manifest: dict[str, Any] = {}

    class FakeHfApi:
        def __init__(self, *, token: str) -> None:
            self.token = token

        def create_repo(self, **_kwargs: object) -> None:
            return None

        def upload_large_folder(self, **kwargs: object) -> None:
            folder = Path(str(kwargs["folder_path"]))
            captured_manifest.update(
                json.loads((folder / "manifest.json").read_text()),
            )

        def list_repo_refs(self, **_kwargs: object) -> object:
            return SimpleNamespace(
                branches=[SimpleNamespace(name="main", target_commit="fake-revision")]
            )

    monkeypatch.setitem(publish_cmd.REGISTRY, "fake-bench", FakeAdapter())
    monkeypatch.setattr(
        publish_cmd,
        "fetch_upstream",
        lambda *_args, **_kwargs: tmp_path,
    )
    monkeypatch.setattr(publish_cmd, "HfApi", FakeHfApi)

    stats = await publish_cmd.run_publish(
        benchmark="fake-bench",
        hf_org="fake-org",
        hf_token="fake-token",
        cache_dir=tmp_path / "cache",
        instance_ids={"task-002"},
    )

    assert stats["published"] == 1
    assert stats["target"] == "hf"
    assert captured_manifest["task_count"] == 1
    task = cast(dict[str, Any], captured_manifest["tasks"][0])
    assert task["instance_id"] == "task-002"
    assert task["task_id"] == "fake-bench/task-002"
    assert task["task_config"]["task"]["id"] == "fake-bench/task-002"


class FakeObjectStore:
    """Minimal ObjectStore stub for publish tests."""

    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[tuple[str, str], bytes] = {}

    async def ensure_bucket(self, bucket: str) -> None:
        self.buckets.add(bucket)

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> str:
        self.objects[(bucket, key)] = body
        return f"s3://{bucket}/{key}"


def test_object_store_revision_is_stable_across_reorderings() -> None:
    """Sorted-checksum digest — reordering the task list doesn't change
    the revision. This is what makes publish idempotent across re-runs
    that iterate instances in a different order."""
    from loom_benchmark_tool.publish_cmd import _object_store_revision

    entries = [
        {"task_id": "fake/a", "checksum": "aa" * 32},
        {"task_id": "fake/b", "checksum": "bb" * 32},
        {"task_id": "fake/c", "checksum": "cc" * 32},
    ]
    rev1 = _object_store_revision(entries)
    rev2 = _object_store_revision(list(reversed(entries)))
    assert rev1 == rev2
    assert len(rev1) == 16


def test_object_store_revision_differs_on_content_change() -> None:
    """Perturbing any per-task checksum must change the revision."""
    from loom_benchmark_tool.publish_cmd import _object_store_revision

    base = [
        {"task_id": "fake/a", "checksum": "aa" * 32},
        {"task_id": "fake/b", "checksum": "bb" * 32},
    ]
    perturbed = [
        {"task_id": "fake/a", "checksum": "aa" * 32},
        {"task_id": "fake/b", "checksum": "cc" * 32},
    ]
    assert _object_store_revision(base) != _object_store_revision(perturbed)


@pytest.mark.asyncio
async def test_run_publish_target_object_store_uploads_manifest_and_bundles(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """--target=object-store writes manifest.json + every bundle file
    under {bucket}/{benchmark_id}/{revision}/... and never touches HF."""
    from loom_benchmark_tool import publish_cmd

    class FakeAdapter:
        name = "fake-bench"
        display_name = "Fake Bench"
        upstream_source = UpstreamSource(kind="huggingface", locator="fake/source")
        license_spdx = "MIT"
        license_url = ""
        splits = ("test",)
        series = "fake"

        def list_instances(
            self,
            *,
            source_dir: Path,
            split: str,
        ) -> list[BenchmarkInstance]:
            return [
                BenchmarkInstance(instance_id="task-001", split=split, raw={}),
                BenchmarkInstance(instance_id="task-002", split=split, raw={}),
            ]

        def convert_instance(
            self,
            instance: BenchmarkInstance,
            *,
            out_dir: Path,
        ) -> ConvertedTask:
            task_id = f"fake-bench/{instance.instance_id}"
            (out_dir / "task.toml").write_text(_task_toml(task_id))
            (out_dir / "solution.py").write_text("print(1)\n")
            return ConvertedTask(
                task_id=task_id,
                checksum=_bundle_checksum(out_dir),
                license_spdx="MIT",
                warnings=(),
            )

    class ExplodingHfApi:
        def __init__(self, *args: object, **kwargs: object) -> None:
            raise AssertionError("HF should not be touched with target=object-store")

    monkeypatch.setitem(publish_cmd.REGISTRY, "fake-bench", FakeAdapter())
    monkeypatch.setattr(
        publish_cmd,
        "fetch_upstream",
        lambda *_args, **_kwargs: tmp_path,
    )
    monkeypatch.setattr(publish_cmd, "HfApi", ExplodingHfApi)

    store = FakeObjectStore()
    result = await publish_cmd.run_publish(
        benchmark="fake-bench",
        target="object-store",
        cache_dir=tmp_path / "cache",
        object_store=store,
        bucket="loom-benchmarks",
    )

    assert result["target"] == "object-store"
    assert result["published"] == 2
    assert result["repo_id"] == "s3://loom-benchmarks/fake-bench"
    revision = result["revision"]
    assert len(revision) == 16
    assert "loom-benchmarks" in store.buckets

    # Manifest lands at the revision root.
    manifest_key = f"fake-bench/{revision}/manifest.json"
    assert (
        ("loom-benchmarks", manifest_key) in store.objects
    ), f"expected manifest at {manifest_key}, got {sorted(store.objects)}"
    manifest = json.loads(store.objects[("loom-benchmarks", manifest_key)])
    assert manifest["task_count"] == 2
    assert manifest["schema_version"] == MANIFEST_SCHEMA_VERSION

    # Each task's bundle files land under {benchmark_id}/{revision}/{safe_id}/.
    for task_entry in manifest["tasks"]:
        safe = task_entry["hf_path"].rstrip("/")
        for filename in ("task.toml", "solution.py"):
            key = f"fake-bench/{revision}/{safe}/{filename}"
            assert (
                ("loom-benchmarks", key) in store.objects
            ), f"expected {key} in uploaded objects"


@pytest.mark.asyncio
async def test_run_publish_target_object_store_requires_object_store() -> None:
    """Missing object_store surfaces a clear ValueError before staging."""
    from loom_benchmark_tool import publish_cmd

    with pytest.raises(ValueError, match="object_store"):
        await publish_cmd.run_publish(
            benchmark="fake-bench",
            target="object-store",
            cache_dir=Path("/tmp/nowhere"),
        )


@pytest.mark.asyncio
async def test_run_publish_target_hf_still_requires_token() -> None:
    """Legacy default target keeps its token requirement — backwards compat."""
    from loom_benchmark_tool import publish_cmd

    with pytest.raises(ValueError, match="hf_token"):
        await publish_cmd.run_publish(
            benchmark="fake-bench",
            target="hf",
            hf_org="fake-org",
            hf_token=None,
            cache_dir=Path("/tmp/nowhere"),
        )
