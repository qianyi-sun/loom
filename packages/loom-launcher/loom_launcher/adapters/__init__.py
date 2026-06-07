"""Adapter registry self-registration entry point.

Importing this package self-registers every adapter module that lives
under `loom_launcher/adapters/`. Plan 12 adds the eleven real adapters;
Plan 10 ships only `HelloAdapter` as a reference for tests.
"""

from loom_launcher.adapters import hello  # noqa: F401 — self-registers

__all__: list[str] = []
