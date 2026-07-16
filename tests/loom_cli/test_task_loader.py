"""task_loader fetches a benchmark adapter from loom_benchmarks.REGISTRY,
walks its instances, converts each into a task bundle, and yields
LoadedTask records that Trial.run() can consume."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from loom_benchmarks.base import (
    BenchmarkInstance,
    ConvertedTask,
    UpstreamSource,
)

from loom_cli.task_loader import LoadedTask, load_tasks


@dataclass(frozen=True)
class _StubAdapter:
    name: str = "stub"
    display_name: str = "Stub"
    upstream_source: UpstreamSource = field(
        default_factory=lambda: UpstreamSource(
            kind="huggingface", locator="stub/dataset",
        ),
    )
    license_spdx: str = "MIT"
    license_url: str = "https://example.com"
    splits: tuple[str, ...] = ("test",)

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        yield BenchmarkInstance(instance_id="t1", split=split, raw={"prompt": "p1"})
        yield BenchmarkInstance(instance_id="t2", split=split, raw={"prompt": "p2"})

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        (out_dir / "instruction.md").write_text(instance.raw["prompt"])
        (out_dir / "task.toml").write_text(
            'schema_version = "1"\n'
            f'[task]\nid = "{self.name}/{instance.instance_id}"\n'
            f'name = "{self.name}-{instance.instance_id}"\n'
            '[environment]\nos = "linux"\ndocker_image = "alpine"\n'
            '[agent]\nname = "oracle"\n'
            '[verifier]\nname = "pytest"\n'
            '[[steps]]\nname = "solve"\n',
        )
        return ConvertedTask(
            task_id=f"{self.name}/{instance.instance_id}",
            checksum="x" * 64, license_spdx="MIT", warnings=(),
        )


def test_load_tasks_yields_one_per_instance(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    from loom_benchmarks import registry

    def _fail_if_hugging_face_is_called(
        *args: object, **kwargs: object,
    ) -> None:
        raise AssertionError("synthetic CLI benchmark source must stay offline")

    monkeypatch.setattr(
        "loom_benchmarks.fetch.datasets.load_dataset",
        _fail_if_hugging_face_is_called,
    )
    monkeypatch.setitem(registry.REGISTRY, "stub", _StubAdapter())
    out = list(load_tasks(
        dataset="stub", split="test",
        task_filter=None, workdir=tmp_path,
    ))
    assert len(out) == 2
    assert all(isinstance(t, LoadedTask) for t in out)
    assert {t.task_config.task.id for t in out} == {"stub/t1", "stub/t2"}
    assert all(t.task_dir.is_dir() for t in out)
    assert all((t.task_dir / "task.toml").exists() for t in out)


def test_load_tasks_filter_by_task_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    from loom_benchmarks import registry

    monkeypatch.setitem(registry.REGISTRY, "stub", _StubAdapter())
    out = list(load_tasks(
        dataset="stub", split="test",
        task_filter="stub/t2", workdir=tmp_path,
    ))
    assert len(out) == 1
    assert out[0].task_config.task.id == "stub/t2"


def test_load_tasks_unknown_dataset_raises(tmp_path: Path) -> None:
    with pytest.raises(KeyError, match="no benchmark adapter"):
        list(load_tasks(
            dataset="nonexistent", split="test",
            task_filter=None, workdir=tmp_path,
        ))
