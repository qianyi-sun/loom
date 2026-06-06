"""CompositeVerifier — runs multiple verifiers and aggregates rewards.

Aggregator is one of an enum (built-in strategies) or a callable AggregatorFn
(custom). Error contribution: per spec §5.4, results with `error != None`
contribute 0 to mean/weighted aggregation; min strategy pulls reward to 0
if any verifier errored.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Protocol

from loom.driver.base import Driver
from loom.models.verifier import CheckResult, VerifierResult
from loom.verifier.base import Verifier

if TYPE_CHECKING:
    from loom.models.task import TaskConfig
    from loom.trajectory.reader import TrajectoryReader


class Aggregator(Enum):
    MEAN = "mean"
    MIN = "min"
    MAX = "max"
    WEIGHTED = "weighted"


class AggregatorFn(Protocol):
    def __call__(
        self, results: Sequence[VerifierResult],
    ) -> VerifierResult: ...


@dataclass
class CompositeVerifier:
    verifiers: Sequence[Verifier]
    aggregator: Aggregator | AggregatorFn
    weights: dict[str, float] | None = None
    name: str = "composite"

    def __post_init__(self) -> None:
        if self.aggregator is Aggregator.WEIGHTED and not self.weights:
            raise ValueError("Aggregator.WEIGHTED requires `weights` map")

    async def verify(
        self,
        *,
        task: TaskConfig,
        env: Driver,
        artifacts_dir: PurePosixPath,
        trajectory: TrajectoryReader,
    ) -> VerifierResult:
        results: list[tuple[str, VerifierResult]] = []
        for v in self.verifiers:
            r = await v.verify(
                task=task, env=env,
                artifacts_dir=artifacts_dir, trajectory=trajectory,
            )
            results.append((v.name, r))

        if not isinstance(self.aggregator, Aggregator):
            # Custom AggregatorFn
            return self.aggregator([r for _, r in results])
        return self._builtin_aggregate(results, self.aggregator)

    def _builtin_aggregate(
        self,
        results: list[tuple[str, VerifierResult]],
        aggregator: Aggregator,
    ) -> VerifierResult:
        all_keys: set[str] = set()
        for _, r in results:
            all_keys.update(r.rewards.keys())

        per_key_samples: dict[str, list[tuple[str, float]]] = {k: [] for k in all_keys}
        for name, r in results:
            for k in all_keys:
                if r.error is not None:
                    per_key_samples[k].append((name, 0.0))
                else:
                    per_key_samples[k].append((name, r.rewards.get(k, 0.0)))

        aggregated: dict[str, float] = {}
        for k, samples in per_key_samples.items():
            if aggregator is Aggregator.MEAN:
                aggregated[k] = sum(v for _, v in samples) / len(samples)
            elif aggregator is Aggregator.MIN:
                aggregated[k] = min(v for _, v in samples)
            elif aggregator is Aggregator.MAX:
                aggregated[k] = max(v for _, v in samples)
            elif aggregator is Aggregator.WEIGHTED:
                assert self.weights is not None
                num = sum(self.weights.get(name, 0.0) * v for name, v in samples)
                den = sum(self.weights.get(name, 0.0) for name, _ in samples)
                aggregated[k] = num / den if den else 0.0

        all_checks: list[CheckResult] = []
        for _, r in results:
            all_checks.extend(r.checks)

        return VerifierResult(rewards=aggregated, checks=all_checks)
