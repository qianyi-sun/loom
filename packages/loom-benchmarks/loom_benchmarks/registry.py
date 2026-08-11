"""Compatibility registry view backed by entry points.

`loom_benchmarks.registry.REGISTRY` is a MutableMapping that lazily loads each
adapter from its entry point. This keeps `loom_benchmark_tool
list/import/verify` and third-party registry importers working.

Thread safety: single-process CLI use only. `loom run` is one process per
invocation; concurrent agentic workloads inside that process share the
same _EntryPointRegistry but mutation only happens via test
`monkeypatch.setitem`. If multi-process / multi-thread mutation becomes a
real workload, wrap state with an RLock.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, MutableMapping
from importlib.metadata import entry_points

from loom_benchmarks.base import BenchmarkAdapter

logger = logging.getLogger(__name__)

_GROUP = "loom.benchmarks"


class _EntryPointRegistry(MutableMapping[str, BenchmarkAdapter]):
    """MutableMapping backed by loom.benchmarks entry-points.

    setitem/delitem support lets tests `monkeypatch.setitem(REGISTRY, ...)`
    to inject stub adapters without re-installing a sibling package."""

    def __init__(self) -> None:
        self._cache: dict[str, BenchmarkAdapter] = {}
        self._overrides: dict[str, BenchmarkAdapter] = {}
        self._deleted: set[str] = set()
        self._ep_names: list[str] | None = None
        self._live_names_cache: list[str] | None = None

    def _ep_names_list(self) -> list[str]:
        if self._ep_names is None:
            self._ep_names = sorted(ep.name for ep in entry_points(group=_GROUP))
        return self._ep_names

    def _live_names(self) -> list[str]:
        if self._live_names_cache is None:
            live = (set(self._ep_names_list()) | set(self._overrides)) - self._deleted
            self._live_names_cache = sorted(live)
        return self._live_names_cache

    def _invalidate(self) -> None:
        self._live_names_cache = None

    def __getitem__(self, key: str) -> BenchmarkAdapter:
        if key in self._deleted:
            raise KeyError(key)
        if key in self._overrides:
            return self._overrides[key]
        if key in self._cache:
            return self._cache[key]
        for ep in entry_points(group=_GROUP):
            if ep.name == key:
                adapter = ep.load()()
                self._cache[key] = adapter
                return adapter
        raise KeyError(key)

    def __setitem__(self, key: str, value: BenchmarkAdapter) -> None:
        self._overrides[key] = value
        self._deleted.discard(key)
        self._invalidate()

    def __delitem__(self, key: str) -> None:
        if key not in self:
            raise KeyError(key)
        self._overrides.pop(key, None)
        self._deleted.add(key)
        self._invalidate()

    def __iter__(self) -> Iterator[str]:
        return iter(self._live_names())

    def __len__(self) -> int:
        return len(self._live_names())

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and key in self._live_names()

    def copy(self) -> dict[str, BenchmarkAdapter]:
        """Materialize every entry into a plain dict.

        Supports callers that require `REGISTRY.copy()` as a plain
        `dict[str, BenchmarkAdapter]`.
        Materializing forces every entry-point to load, which can be
        expensive — prefer iteration unless you actually need a snapshot.
        """
        return {key: self[key] for key in self._live_names()}


REGISTRY: MutableMapping[str, BenchmarkAdapter] = _EntryPointRegistry()
