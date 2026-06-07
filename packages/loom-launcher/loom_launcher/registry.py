"""Adapter registry — name → instance lookup.

Adapter modules call `@register_adapter` at import time. The worker (and
test code) calls `get_adapter(name)` to retrieve an instance.
"""

from __future__ import annotations

from typing import TypeVar

from loom_launcher.adapter import AgentAdapter

_REGISTRY: dict[str, AgentAdapter] = {}

T = TypeVar("T", bound=AgentAdapter)


def register_adapter(adapter: T) -> T:
    """Decorator/function that registers an adapter instance.

    Usage (module-level, after instantiating the dataclass):

        ALL_ADAPTERS = [register_adapter(MyAdapter())]

    or as a decorator on a singleton-returning function:

        @register_adapter
        class MyAdapter: ...   # if MyAdapter() can be passed directly

    Raises `ValueError` on name collision so silent overwrites can't
    happen if two modules accidentally claim the same name.
    """
    existing = _REGISTRY.get(adapter.name)
    if existing is not None and existing is not adapter:
        raise ValueError(
            f"adapter name {adapter.name!r} already registered "
            f"by {type(existing).__name__!r}; refusing to overwrite "
            f"with {type(adapter).__name__!r}",
        )
    _REGISTRY[adapter.name] = adapter
    return adapter


def get_adapter(name: str) -> AgentAdapter | None:
    """Look up a registered adapter by name. Returns None if missing —
    callers (e.g. worker factory) raise their own ConfigError with
    context."""
    return _REGISTRY.get(name)


def all_adapters() -> list[AgentAdapter]:
    """Snapshot of every registered adapter — used by conformance tests
    and the `loom-launcher list` CLI (future)."""
    return list(_REGISTRY.values())


def _clear_for_tests() -> None:
    """Test-only: reset the registry. Pytest fixtures use this; never
    call from production code."""
    _REGISTRY.clear()
