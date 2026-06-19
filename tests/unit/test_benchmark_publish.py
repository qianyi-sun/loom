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
    assert digest1.startswith("sha256:")


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


def test_manifest_schema_version_is_int() -> None:
    """Operators (and the worker) fork on this; an accidental change
    to a string would silently break the manifest reader."""
    assert isinstance(MANIFEST_SCHEMA_VERSION, int)
    assert MANIFEST_SCHEMA_VERSION >= 3


def test_run_publish_includes_valid_task_config_in_manifest(
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

        def upload_folder(self, **kwargs: object) -> object:
            folder = Path(str(kwargs["folder_path"]))
            captured_manifest.update(
                json.loads((folder / "manifest.json").read_text()),
            )
            return SimpleNamespace(oid="fake-revision")

    monkeypatch.setitem(publish_cmd.REGISTRY, "fake-bench", FakeAdapter())
    monkeypatch.setattr(
        publish_cmd,
        "fetch_upstream",
        lambda *_args, **_kwargs: tmp_path,
    )
    monkeypatch.setattr(publish_cmd, "HfApi", FakeHfApi)

    stats = publish_cmd.run_publish(
        benchmark="fake-bench",
        hf_org="fake-org",
        hf_token="fake-token",
        cache_dir=tmp_path / "cache",
    )

    assert stats["published"] == 1
    assert captured_manifest["schema_version"] == MANIFEST_SCHEMA_VERSION
    task = cast(dict[str, Any], captured_manifest["tasks"][0])
    assert task["task_config"]["task"]["id"] == "fake-bench/task-001"
    assert task["task_config"]["environment"]["docker_image"] == "python:3.12-slim"
    assert task["tags"] == {
        "difficulty": "smoke",
        "license_execution_policy": "notice",
    }


def test_run_publish_filters_specific_instance_ids(
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

        def upload_folder(self, **kwargs: object) -> object:
            folder = Path(str(kwargs["folder_path"]))
            captured_manifest.update(
                json.loads((folder / "manifest.json").read_text()),
            )
            return SimpleNamespace(oid="fake-revision")

    monkeypatch.setitem(publish_cmd.REGISTRY, "fake-bench", FakeAdapter())
    monkeypatch.setattr(
        publish_cmd,
        "fetch_upstream",
        lambda *_args, **_kwargs: tmp_path,
    )
    monkeypatch.setattr(publish_cmd, "HfApi", FakeHfApi)

    stats = publish_cmd.run_publish(
        benchmark="fake-bench",
        hf_org="fake-org",
        hf_token="fake-token",
        cache_dir=tmp_path / "cache",
        instance_ids={"task-002"},
    )

    assert stats["published"] == 1
    assert captured_manifest["task_count"] == 1
    task = cast(dict[str, Any], captured_manifest["tasks"][0])
    assert task["instance_id"] == "task-002"
    assert task["task_id"] == "fake-bench/task-002"
    assert task["task_config"]["task"]["id"] == "fake-bench/task-002"
