"""Loom-owned deterministic seed latch installed into the image's OmniGibson tree."""

from __future__ import annotations

import operator

_PIPELINE_SEED: int | None = None


def set_pipeline_seed(seed: int) -> None:
    """Latch the signed uint32 seed and reject in-process seed drift.

    Loom applies Python, NumPy, PyTorch, and CUDA seeds immediately before this
    hook.  The hook gives the simulator namespace a replay-checkable authority
    without accepting an ambient environment or wall-clock fallback.
    """

    global _PIPELINE_SEED
    try:
        value = operator.index(seed)
    except TypeError as exc:
        raise ValueError("Pipeline simulator seed must be uint32") from exc
    if isinstance(seed, bool) or not 0 <= value <= 4_294_967_295:
        raise ValueError("Pipeline simulator seed must be uint32")
    if _PIPELINE_SEED is not None and _PIPELINE_SEED != value:
        raise RuntimeError("Pipeline simulator seed drift is forbidden")
    _PIPELINE_SEED = value


def pipeline_seed() -> int:
    """Return the latched seed; fail before signed initialization."""

    if _PIPELINE_SEED is None:
        raise RuntimeError("Pipeline simulator seed is not initialized")
    return _PIPELINE_SEED
