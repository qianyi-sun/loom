"""Deterministic RNG authority for Loom-supervised BEHAVIOR execution.

The calls and their order are part of the Stage 1 ABI.  Imports stay lazy so a
clean control-plane/test installation does not need the GPU image's scientific
Python stack.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Protocol, cast

from loom.integrations.behavior.errors import BehaviorContractError


class _NumpyRandom(Protocol):
    def seed(self, seed: int) -> None: ...


class _Numpy(Protocol):
    random: _NumpyRandom


class _Cuda(Protocol):
    def manual_seed_all(self, seed: int) -> None: ...


class _Torch(Protocol):
    cuda: _Cuda

    def manual_seed(self, seed: int) -> Any: ...


@dataclass(frozen=True)
class SeedReceipt:
    """Auditable receipt of the fixed seed-call sequence."""

    seed: int
    calls: tuple[str, ...]


SEED_CALL_ORDER = (
    "random.seed",
    "numpy.random.seed",
    "torch.manual_seed",
    "torch.cuda.manual_seed_all",
    "omnigibson.set_pipeline_seed",
)


def apply_seed(
    seed: int,
    *,
    random_seed: Callable[[int], None] = random.seed,
    numpy_module: _Numpy | None = None,
    torch_module: _Torch | None = None,
    simulator_seed: Callable[[int], None] | None = None,
) -> SeedReceipt:
    """Apply one uint32 seed in the immutable Pipeline order."""

    if isinstance(seed, bool) or not 0 <= seed <= 4_294_967_295:
        raise BehaviorContractError("seed must be uint32")
    selected_numpy = numpy_module
    if selected_numpy is None:
        try:
            selected_numpy = cast(_Numpy, import_module("numpy"))
        except ModuleNotFoundError as exc:  # pragma: no cover - GPU image boundary
            raise BehaviorContractError("Pipeline image is missing numpy") from exc
    selected_torch = torch_module
    if selected_torch is None:
        try:
            selected_torch = cast(_Torch, import_module("torch"))
        except ModuleNotFoundError as exc:  # pragma: no cover - GPU image boundary
            raise BehaviorContractError("Pipeline image is missing torch") from exc
    if simulator_seed is None:
        try:
            from loom.integrations.behavior.stages.simulator_rng import set_pipeline_seed
        except ImportError as exc:  # pragma: no cover - GPU image boundary
            raise BehaviorContractError("Pipeline image is missing the OmniGibson RNG hook") from exc
        simulator_seed = set_pipeline_seed

    random_seed(seed)
    selected_numpy.random.seed(seed)
    selected_torch.manual_seed(seed)
    selected_torch.cuda.manual_seed_all(seed)
    simulator_seed(seed)
    return SeedReceipt(seed=seed, calls=SEED_CALL_ORDER)


class SeedAuthority:
    """Reject drift while allowing the two prescribed simulator seed points."""

    def __init__(self, *, allow_same_seed_replay: bool) -> None:
        self._allow_same_seed_replay = allow_same_seed_replay
        self._receipt: SeedReceipt | None = None

    @property
    def receipt(self) -> SeedReceipt | None:
        return self._receipt

    def apply(self, seed: int, **kwargs: Any) -> SeedReceipt:
        if self._receipt is not None:
            if seed != self._receipt.seed:
                raise BehaviorContractError("Pipeline RNG reseed drift is forbidden")
            if not self._allow_same_seed_replay:
                raise BehaviorContractError("VLA policy may be seeded only once")
        receipt = apply_seed(seed, **kwargs)
        if self._receipt is not None and receipt != self._receipt:
            raise BehaviorContractError("same-seed initialization receipt drift")
        self._receipt = receipt
        return receipt
