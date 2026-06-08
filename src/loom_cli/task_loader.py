"""Dataset slug -> list of loaded tasks ready to feed into Trial.run().

The loader looks up the BenchmarkAdapter in `loom_benchmarks.REGISTRY`,
fetches the upstream source (caching to `~/.cache/loom-cli/datasets/`),
walks instances, converts each into a task bundle written under
`workdir/<dataset>/<instance_id_safe>/`, and yields LoadedTask tuples.
"""

from __future__ import annotations

import tomllib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom_benchmarks.fetch import fetch_upstream
from loom_benchmarks.registry import REGISTRY

from loom.models.task import TaskConfig


@dataclass(frozen=True)
class LoadedTask:
    task_dir: Path
    task_config: TaskConfig
    checksum: str
    license_spdx: str


def _safe_segment(task_id: str) -> str:
    return task_id.replace("/", "__")


def load_tasks(
    *,
    dataset: str,
    split: str,
    task_filter: str | None,
    workdir: Path,
) -> Iterator[LoadedTask]:
    """Yield LoadedTask records for `dataset`, restricted to `task_filter`
    if non-None. `workdir` is the per-run scratch dir."""
    adapter = REGISTRY.get(dataset)
    if adapter is None:
        raise KeyError(
            f"no benchmark adapter registered for dataset {dataset!r}; "
            f"available: {sorted(REGISTRY)}",
        )
    source_dir = _resolve_source_dir(adapter, workdir=workdir)
    for instance in adapter.list_instances(source_dir=source_dir, split=split):
        candidate_id = f"{dataset}/{instance.instance_id}"
        if task_filter is not None and candidate_id != task_filter:
            continue
        out_dir = workdir / dataset / _safe_segment(instance.instance_id)
        out_dir.mkdir(parents=True, exist_ok=True)
        converted = adapter.convert_instance(instance, out_dir=out_dir)
        cfg = TaskConfig.model_validate(
            tomllib.loads((out_dir / "task.toml").read_text()),
        )
        yield LoadedTask(
            task_dir=out_dir,
            task_config=cfg,
            checksum=converted.checksum,
            license_spdx=converted.license_spdx,
        )


def _resolve_source_dir(adapter: Any, *, workdir: Path) -> Path:
    """Fetch (or reuse cached) upstream source for an adapter.

    Cache root: `~/.cache/loom-cli/datasets/`. Delegates to
    `loom_benchmarks.fetch.fetch_upstream`. If the fetch fails (e.g.
    test stubs with synthetic UpstreamSource), fall back to a workdir
    placeholder — adapters that don't actually read source_dir won't
    care."""
    source = adapter.upstream_source
    cache_root = Path.home() / ".cache" / "loom-cli" / "datasets"
    cache_root.mkdir(parents=True, exist_ok=True)
    try:
        fetched: Path = fetch_upstream(source, cache_root=cache_root)
        return fetched
    except Exception:
        fallback = workdir / "_no_source"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback
