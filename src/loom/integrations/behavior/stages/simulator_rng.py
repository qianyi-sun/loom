"""Narrow Loom-owned hook for the OmniGibson Pipeline RNG.

The GPU image installs a patched OmniGibson exposing ``set_pipeline_seed``.
Keeping the import behind this function prevents ambient simulator imports in
control-plane and contract-only processes.
"""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from typing import cast

from loom.integrations.behavior.errors import BehaviorContractError


def set_pipeline_seed(seed: int) -> None:
    try:
        module = import_module("omnigibson.utils.pipeline_rng")
        _set_seed = cast(Callable[[int], None], module.set_pipeline_seed)
    except (ImportError, AttributeError) as exc:  # pragma: no cover - GPU image boundary
        raise BehaviorContractError("OmniGibson Pipeline RNG hook is unavailable") from exc
    _set_seed(seed)
