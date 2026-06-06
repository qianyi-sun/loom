"""Verifier Protocol + factory (spec §2.4)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from loom.driver.base import Driver
from loom.errors import VerifierError
from loom.models.verifier import VerifierResult

if TYPE_CHECKING:
    from loom.models.task import TaskConfig
    from loom.trajectory.reader import TrajectoryReader


@runtime_checkable
class Verifier(Protocol):
    name: str

    async def verify(
        self,
        *,
        task: TaskConfig,
        env: Driver,
        artifacts_dir: PurePosixPath,
        trajectory: TrajectoryReader,
    ) -> VerifierResult: ...


class VerifierFactory:
    """Registry of verifier name → constructor.

    Constructors are called with `**args` from the task/trial config.
    """

    def __init__(self) -> None:
        self._registry: dict[str, Callable[..., Verifier]] = {}

    def register(self, name: str, ctor: Callable[..., Verifier]) -> None:
        if name in self._registry:
            raise ValueError(f"verifier {name!r} already registered")
        self._registry[name] = ctor

    def create(self, name: str, *, args: dict[str, Any]) -> Verifier:
        if name not in self._registry:
            raise VerifierError(f"unknown verifier: {name!r}")
        return self._registry[name](**args)
