"""Bounded deterministic packing over exact per-node resource vectors."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from fractions import Fraction
from math import isfinite

from loom_capacity_manager.contracts import (
    NodeResidualV1,
    PackingPlacementV1,
    PackingRequestV1,
    PackingShapeRequestV1,
    PackingWitnessV1,
    ResourceVectorV1,
    WorkerShapeV1,
    vector_fits,
)


class TopologyInfeasible(RuntimeError):  # noqa: N818 - public plan interface
    """Raised when a complete exact placement does not exist."""


class TopologySearchLimit(RuntimeError):  # noqa: N818 - public plan interface
    """Raised when the bounded search cannot conclusively finish."""


@dataclass(frozen=True, slots=True)
class SearchBudget:
    max_states: int = 250_000
    deadline_seconds: float = 0.5

    def __post_init__(self) -> None:
        if type(self.max_states) is not int or self.max_states < 0:
            raise ValueError("max_states must be a nonnegative integer")
        if (
            isinstance(self.deadline_seconds, bool)
            or not isinstance(self.deadline_seconds, (int, float))
            or not isfinite(self.deadline_seconds)
            or self.deadline_seconds < 0
        ):
            raise ValueError("deadline_seconds must be finite and nonnegative")


@dataclass(frozen=True, slots=True)
class _NodeKey:
    domain_id: str
    node_id: str


@dataclass(frozen=True, slots=True)
class _Placed:
    instance_id: str
    domain_id: str
    node_ids: tuple[str, ...]


def _subtract(
    available: ResourceVectorV1,
    required: ResourceVectorV1,
) -> ResourceVectorV1:
    if not vector_fits(required, available):
        raise TopologyInfeasible("resource vector does not fit node residual")
    keys = tuple(sorted(set(available.generic) | set(required.generic)))
    return ResourceVectorV1(
        slots=available.slots - required.slots,
        cpu_millicores=available.cpu_millicores - required.cpu_millicores,
        memory_bytes=available.memory_bytes - required.memory_bytes,
        gpu_count=available.gpu_count - required.gpu_count,
        generic={key: available.generic.get(key, 0) - required.generic.get(key, 0) for key in keys},
    )


def _zero_like(value: ResourceVectorV1) -> ResourceVectorV1:
    return ResourceVectorV1(
        slots=0,
        cpu_millicores=0,
        memory_bytes=0,
        gpu_count=0,
        generic={key: 0 for key in value.generic},
    )


def _vector_signature(
    value: ResourceVectorV1,
) -> tuple[int, int, int, int, tuple[tuple[str, int], ...]]:
    return (
        value.slots,
        value.cpu_millicores,
        value.memory_bytes,
        value.gpu_count,
        tuple(sorted(value.generic.items())),
    )


class _PackingSearch:
    def __init__(
        self,
        request: PackingRequestV1,
        *,
        budget: SearchBudget,
        monotonic: Callable[[], float],
    ) -> None:
        self._request = request
        self._budget = budget
        self._monotonic = monotonic
        self._deadline = monotonic() + budget.deadline_seconds
        self._states = 0
        self._memo: set[
            tuple[
                int,
                tuple[
                    tuple[
                        str,
                        tuple[str, ...],
                        tuple[int, int, int, int, tuple[tuple[str, int], ...]],
                    ],
                    ...,
                ],
            ]
        ] = set()
        self._nodes: dict[_NodeKey, ResourceVectorV1] = {
            _NodeKey(domain.domain_id, node.node_id): node.allocatable
            for domain in request.domains
            for node in domain.nodes
        }
        self._features = {
            _NodeKey(domain.domain_id, node.node_id): frozenset(node.features)
            for domain in request.domains
            for node in domain.nodes
        }
        self._domain_constraints = {
            domain.domain_id: domain.topology_constraints for domain in request.domains
        }
        self._charged: list[str] = []
        self._over_limit_slots = 0
        self._new_placement_allowed = True
        self._blockers: set[str] = set()

    def solve(self) -> PackingWitnessV1:
        self._charge_fixed_commitments()
        desired = tuple(sorted(self._request.desired_shapes, key=self._shape_sort_key))
        if desired and not self._new_placement_allowed:
            raise TopologyInfeasible("fixed over-limit scope prohibits new placement")
        placed = self._search(0, desired, self._nodes, ())
        if placed is None:
            raise TopologyInfeasible("no complete per-node topology placement exists")
        placements, residual = placed
        return PackingWitnessV1(
            pool_id=self._request.pool_id,
            placements=tuple(
                PackingPlacementV1(
                    instance_id=item.instance_id,
                    domain_id=item.domain_id,
                    node_ids=item.node_ids,
                )
                for item in sorted(placements, key=lambda value: value.instance_id)
            ),
            residuals=tuple(
                NodeResidualV1(node_id=key.node_id, residual=value)
                for key, value in sorted(
                    residual.items(),
                    key=lambda item: (item[0].domain_id, item[0].node_id),
                )
            ),
            charged_commitment_ids=tuple(sorted(self._charged)),
            over_limit_slots=self._over_limit_slots,
            new_placement_allowed=self._new_placement_allowed,
            blockers=tuple(sorted(self._blockers)),
        )

    def _charge_fixed_commitments(self) -> None:
        for commitment in self._request.fixed_commitments:
            if commitment.pool_id != self._request.pool_id:
                raise TopologyInfeasible("fixed commitment is bound to a different pool")
            self._charged.append(commitment.commitment_id)
            candidates = tuple(
                key
                for key in sorted(self._nodes, key=lambda item: (item.domain_id, item.node_id))
                if len(commitment.node_ids) == 1 and key.node_id == commitment.node_ids[0]
            )
            if len(commitment.node_ids) != 1 or len(candidates) != 1:
                self._charge_ambiguous_pool(commitment.resources.slots)
                continue
            exact_node = candidates[0]
            if vector_fits(commitment.resources, self._nodes[exact_node]):
                self._nodes[exact_node] = _subtract(self._nodes[exact_node], commitment.resources)
                continue
            self._over_limit_slots += commitment.resources.slots
            self._new_placement_allowed = False
            self._blockers.add("fixed_commitment_over_limit")
            self._nodes[exact_node] = _zero_like(self._nodes[exact_node])

    def _charge_ambiguous_pool(self, slots: int) -> None:
        self._over_limit_slots += slots
        self._new_placement_allowed = False
        self._blockers.add("fixed_commitment_mapping_ambiguous")
        for key, value in tuple(self._nodes.items()):
            self._nodes[key] = _zero_like(value)

    def _shape_sort_key(
        self, request: PackingShapeRequestV1
    ) -> tuple[int, Fraction, int, str, str]:
        shape = request.shape
        dominant = self._dominant_fraction(shape)
        return (
            len(shape.compatible_domain_ids),
            -dominant,
            -len(shape.node_resources),
            shape.shape_id,
            request.instance_id,
        )

    def _dominant_fraction(self, shape: WorkerShapeV1) -> Fraction:
        candidates = [
            value
            for key, value in self._nodes.items()
            if key.domain_id in shape.compatible_domain_ids
        ]
        if not candidates:
            return Fraction(1, 1)
        maxima = ResourceVectorV1(
            slots=max(item.slots for item in candidates),
            cpu_millicores=max(item.cpu_millicores for item in candidates),
            memory_bytes=max(item.memory_bytes for item in candidates),
            gpu_count=max(item.gpu_count for item in candidates),
            generic={
                key: max(item.generic.get(key, 0) for item in candidates)
                for key in sorted({key for item in candidates for key in item.generic})
            },
        )
        fractions = [
            Fraction(required, available)
            for required, available in (
                (shape.total_resources.slots, maxima.slots),
                (shape.total_resources.cpu_millicores, maxima.cpu_millicores),
                (shape.total_resources.memory_bytes, maxima.memory_bytes),
                (shape.total_resources.gpu_count, maxima.gpu_count),
            )
            if available > 0
        ]
        fractions.extend(
            Fraction(required, maxima.generic.get(key, 0))
            for key, required in shape.total_resources.generic.items()
            if maxima.generic.get(key, 0) > 0
        )
        return max(fractions, default=Fraction(0, 1))

    def _check_budget(self) -> None:
        if self._states >= self._budget.max_states:
            raise TopologySearchLimit("topology search state limit exceeded")
        self._states += 1
        if self._monotonic() > self._deadline:
            raise TopologySearchLimit("topology search deadline exceeded")

    def _search(
        self,
        index: int,
        desired: tuple[PackingShapeRequestV1, ...],
        residual: dict[_NodeKey, ResourceVectorV1],
        placements: tuple[_Placed, ...],
    ) -> tuple[tuple[_Placed, ...], dict[_NodeKey, ResourceVectorV1]] | None:
        self._check_budget()
        if index == len(desired):
            return placements, residual
        signature = (
            index,
            tuple(
                sorted(
                    (
                        key.domain_id,
                        tuple(sorted(self._features[key])),
                        _vector_signature(value),
                    )
                    for key, value in residual.items()
                )
            ),
        )
        if signature in self._memo:
            return None
        self._memo.add(signature)
        request = desired[index]
        for domain_id in sorted(request.shape.compatible_domain_ids):
            options = self._shape_node_options(
                request.shape,
                domain_id=domain_id,
                residual=residual,
            )
            for node_ids, updated in options:
                result = self._search(
                    index + 1,
                    desired,
                    updated,
                    (
                        *placements,
                        _Placed(
                            instance_id=request.instance_id,
                            domain_id=domain_id,
                            node_ids=node_ids,
                        ),
                    ),
                )
                if result is not None:
                    return result
        return None

    def _shape_node_options(
        self,
        shape: WorkerShapeV1,
        *,
        domain_id: str,
        residual: dict[_NodeKey, ResourceVectorV1],
    ) -> Iterator[tuple[tuple[str, ...], dict[_NodeKey, ResourceVectorV1]]]:
        if domain_id not in {domain.domain_id for domain in self._request.domains}:
            return
        if any(
            key != "required_feature" and self._domain_constraints[domain_id].get(key) != value
            for key, value in shape.placement_constraints.items()
        ):
            return

        def place_part(
            part_index: int,
            current: dict[_NodeKey, ResourceVectorV1],
            selected: tuple[_NodeKey, ...],
        ) -> Iterator[tuple[tuple[str, ...], dict[_NodeKey, ResourceVectorV1]]]:
            if part_index == len(shape.node_resources):
                yield (
                    tuple(key.node_id for key in selected),
                    current,
                )
                return
            required = shape.node_resources[part_index]
            required_feature = shape.placement_constraints.get("required_feature")
            seen_node_states: set[
                tuple[
                    tuple[str, ...],
                    tuple[int, int, int, int, tuple[tuple[str, int], ...]],
                ]
            ] = set()
            for key in sorted(
                current,
                key=lambda item: (
                    item.domain_id,
                    _vector_signature(current[item]),
                    item.node_id,
                ),
            ):
                self._check_budget()
                if key.domain_id != domain_id or key in selected:
                    continue
                node_state = (
                    tuple(sorted(self._features[key])),
                    _vector_signature(current[key]),
                )
                if node_state in seen_node_states:
                    continue
                seen_node_states.add(node_state)
                if required_feature is not None and required_feature not in self._features[key]:
                    continue
                if not vector_fits(required, current[key]):
                    continue
                next_residual = dict(current)
                next_residual[key] = _subtract(current[key], required)
                yield from place_part(part_index + 1, next_residual, (*selected, key))

        yield from place_part(0, residual, ())


def pack_topology(
    request: PackingRequestV1,
    *,
    budget: SearchBudget = SearchBudget(),  # noqa: B008 - immutable value object
    monotonic: Callable[[], float] = time.monotonic,
) -> PackingWitnessV1:
    """Return one complete deterministic witness or fail closed."""

    return _PackingSearch(request, budget=budget, monotonic=monotonic).solve()


__all__ = [
    "SearchBudget",
    "TopologyInfeasible",
    "TopologySearchLimit",
    "pack_topology",
]
