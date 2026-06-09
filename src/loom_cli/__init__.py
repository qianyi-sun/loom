"""Ad-hoc `loom` CLI — stateless one-shot mode.

See `docs/architecture/cli-mode.md` for the wiring.
"""

from __future__ import annotations

__version__ = "0.1.0"


def _eager_import_launcher_adapters() -> None:
    """Import every loom_launcher.adapters.* module so each adapter
    self-registers and is then findable by `loom_launcher.get_adapter`.
    Best-effort: individual import failures are swallowed so a missing
    optional adapter SDK doesn't break the whole CLI."""
    import importlib
    import pkgutil

    try:
        import loom_launcher.adapters as pkg
    except ImportError:
        return
    for mod in pkgutil.iter_modules(pkg.__path__):
        try:
            importlib.import_module(f"{pkg.__name__}.{mod.name}")
        except Exception:
            continue


_eager_import_launcher_adapters()
