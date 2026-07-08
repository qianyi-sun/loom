"""Entry-point discovery for family-run plugins (#672)."""

from __future__ import annotations

from importlib.metadata import entry_points
from typing import Any

from loom.family_run.spec import PluginRef


class RegistryError(RuntimeError):
    """Raised when a plugin cannot be resolved."""


def _entry_points(group: str) -> dict[str, type]:
    """Return a name -> class map for all entries registered under ``group``."""
    return {ep.name: ep.load() for ep in entry_points(group=group)}


def resolve_plugin(group: str, ref: PluginRef) -> Any:
    """Look up ``ref.name`` in ``group``'s entry points and instantiate.

    Plugin constructors take no positional args. Per-instance configuration
    is passed at method-call time via the ``params`` dict on ``ref``.
    """
    registered = _entry_points(group)
    cls = registered.get(ref.name)
    if cls is None:
        available = ", ".join(sorted(registered)) or "(none)"
        raise RegistryError(
            f"family-run plugin {ref.name!r} not registered under {group!r}; "
            f"available: {available}",
        )
    return cls()
