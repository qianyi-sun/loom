"""Entry-points discovery for benchmark adapters declared in
`[project.entry-points."loom.benchmarks"]` of any installed package."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from importlib.metadata import EntryPoint, entry_points

from loom_cli.discovery import DatasetEntry

logger = logging.getLogger(__name__)

_GROUP = "loom.benchmarks"


def _entry_points(group: str) -> Iterable[EntryPoint]:
    return entry_points(group=group)


def load_builtin_entries() -> list[DatasetEntry]:
    out: list[DatasetEntry] = []
    for ep in _entry_points(_GROUP):
        try:
            adapter_cls = ep.load()
            adapter = adapter_cls()
        except Exception as exc:
            logger.warning(
                "loom.benchmarks entry-point %r failed to load: %s",
                ep.name, exc,
            )
            continue
        upstream = getattr(adapter, "upstream_source", None)
        # Adapters whose pinned upstream has a fixed task count can declare
        # `task_count = <int>` as a class attribute; consumed by
        # `loom datasets list` so users see real metadata instead of `-`.
        # Dynamic-count adapters (HF subset, dataset revision, etc.) leave
        # it unset and the column stays `-`.
        task_count_attr = getattr(adapter, "task_count", None)
        task_count = (
            task_count_attr if isinstance(task_count_attr, int) else None
        )
        out.append(DatasetEntry(
            slug=ep.name,
            source="builtin",
            display_name=getattr(adapter, "display_name", ep.name),
            license_spdx=getattr(adapter, "license_spdx", "UNKNOWN"),
            license_url=getattr(adapter, "license_url", ""),
            task_count=task_count,
            status="installed",
            available_pip_spec=None,
            entry_point=f"{ep.module}:{ep.attr}" if ep.attr else ep.value,
            upstream_kind=getattr(upstream, "kind", None),
        ))
    out.sort(key=lambda e: e.slug)
    return out
