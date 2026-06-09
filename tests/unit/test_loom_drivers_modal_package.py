"""Confirms src/loom_drivers/modal is importable as a subpackage.

We do NOT import modal SDK at module load — that lives behind a try/except
in client.py so users without `loom[modal]` installed don't get an
ImportError just from `import loom_drivers.modal`.
"""

from __future__ import annotations

import importlib
import sys


def test_loom_drivers_modal_subpackage_importable() -> None:
    mod = importlib.import_module("loom_drivers.modal")
    assert mod.__name__ == "loom_drivers.modal"


def test_loom_drivers_modal_does_not_import_modal_sdk_at_load() -> None:
    sys.modules.pop("modal", None)
    sys.modules.pop("loom_drivers.modal", None)
    importlib.import_module("loom_drivers.modal")
    assert "modal" not in sys.modules, (
        "loom_drivers.modal must not eagerly import the modal SDK; "
        "doing so breaks installs without the [modal] extra."
    )
