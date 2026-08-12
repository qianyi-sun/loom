"""Pure deterministic hierarchical allocation for Package 1 shadow evidence."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from fractions import Fraction
from hashlib import sha256
from itertools import combinations_with_replacement
from math import isfinite
from typing import Literal
from uuid import UUID

from loom_capacity_manager.contracts import (
    AllocationInputV1,
    ClaimSlotMatchV1,
    ConfigurationSnapshotV1,
    CurrentAssignmentV1,
    DesiredShapeCountV1,
    Digest,
    FairnessCursorV1,
    FixedClaimV1,
    Identifier,
    JointMatchingWitnessV1,
    ObservedCommitmentV1,
    PackingRequestV1,
    PackingShapeRequestV1,
    PackingWitnessV1,
    PlacementAllowanceV1,
    PoolManifestV1,
    ProfileReferenceV1,
    Quantity,
    RankedHypotheticalLaunchV1,
    ResourceDomainV1,
    ResourceVectorV1,
    RolloutSurgePairingV1,
    ShadowAllocationV1,
    ShadowEpochV1,
    SubjectAllocationInputV1,
    SubjectConfigurationV1,
    TierPolicyV1,
    WorkerShapeV1,
    canonical_bytes,
    canonical_digest,
    checked_add,
    checked_sum,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionAuthorityV2,
    ExecutionFenceV2,
    StrictV2Model,
)
from loom_capacity_manager.fleet_state import (
    FleetStateError,
    validate_fleet_manifest_digests,
    validate_profile_narrowing,
)
from loom_capacity_manager.topology import (
    SearchBudget,
    TopologyInfeasible,
    TopologySearchLimit,
    pack_topology,
)

MAX_ALLOCATION_DECISIONS = 250_000
_PENDING_COMMITMENT_STATES = frozenset({"proposed", "accepted", "pending", "submitting-unknown"})


class ShadowAllocatorError(RuntimeError):
    """Raised when no complete bounded shadow allocation can be proven."""


class ExecutableAllocationError(RuntimeError):
    """Raised when a shadow plan cannot be promoted under the current fence."""


class ExecutableEpochV2(StrictV2Model):
    """Exact shadow placement promoted under one durable execution fence."""

    execution: ExecutionFenceV2
    configuration: ConfigurationSnapshotV1
    input_digest: Digest
    allocations: tuple[ShadowAllocationV1, ...]
    next_fairness_cursors: tuple[FairnessCursorV1, ...]
    hypothetical_launch_rank: tuple[RankedHypotheticalLaunchV1, ...]
    pool_witnesses: tuple[PackingWitnessV1, ...]
    blockers: tuple[Identifier, ...]
    executable_new_capacity_ceiling: Quantity
    executable_new_capacity_rate_per_minute: Quantity
    executable: Literal[True] = True

    @classmethod
    def from_shadow(
        cls,
        shadow: ShadowEpochV1,
        authority: ExecutionAuthorityV2,
        allocation_epoch: int,
    ) -> ExecutableEpochV2:
        """Bind an unchanged placement to an exact active execution epoch."""

        execution = ExecutionFenceV2.model_validate(
            authority.model_dump(mode="python") | {"allocation_epoch": allocation_epoch}
        )
        return cls(
            execution=execution,
            configuration=shadow.configuration,
            input_digest=shadow.input_digest,
            allocations=shadow.allocations,
            next_fairness_cursors=shadow.next_fairness_cursors,
            hypothetical_launch_rank=shadow.hypothetical_launch_rank,
            pool_witnesses=shadow.pool_witnesses,
            blockers=shadow.blockers,
            executable_new_capacity_ceiling=authority.executable_new_capacity_ceiling,
            executable_new_capacity_rate_per_minute=(
                authority.executable_new_capacity_rate_per_minute
            ),
        )


def promote_shadow_epoch(
    shadow: ShadowEpochV1,
    authority: ExecutionAuthorityV2 | None,
    *,
    allocation_epoch: int,
) -> ExecutableEpochV2:
    """Promote one freshly computed shadow plan without changing its placement."""

    if authority is None or authority.execution_state != "active":
        raise ExecutableAllocationError("active execution authority is required")
    if shadow.configuration.configuration_epoch != authority.configuration_epoch:
        raise ExecutableAllocationError("configuration epoch changed")
    return ExecutableEpochV2.from_shadow(shadow, authority, allocation_epoch)


@dataclass(frozen=True, slots=True)
class AllocatorSearchBounds:
    """Explicit deterministic work ceilings for one shadow calculation."""

    max_allocation_decisions: int = MAX_ALLOCATION_DECISIONS
    topology_max_states: int = 250_000
    topology_deadline_seconds: float = 0.5

    def __post_init__(self) -> None:
        if type(self.max_allocation_decisions) is not int or self.max_allocation_decisions < 0:
            raise ValueError("max_allocation_decisions must be a nonnegative integer")
        if type(self.topology_max_states) is not int or self.topology_max_states < 0:
            raise ValueError("topology_max_states must be a nonnegative integer")
        if (
            isinstance(self.topology_deadline_seconds, bool)
            or not isinstance(self.topology_deadline_seconds, (int, float))
            or not isfinite(self.topology_deadline_seconds)
            or self.topology_deadline_seconds < 0
        ):
            raise ValueError("topology_deadline_seconds must be finite and nonnegative")

    def topology_budget(self) -> SearchBudget:
        return SearchBudget(
            max_states=self.topology_max_states,
            deadline_seconds=self.topology_deadline_seconds,
        )


@dataclass(slots=True)
class _DecisionBudget:
    remaining: int = MAX_ALLOCATION_DECISIONS

    def consume(self) -> None:
        if self.remaining <= 0:
            raise ShadowAllocatorError("shadow allocation decision limit exceeded")
        self.remaining -= 1

    def consume_many(self, count: int) -> None:
        if count < 0 or count > self.remaining:
            raise ShadowAllocatorError("shadow allocation decision limit exceeded")
        self.remaining -= count


@dataclass(frozen=True, slots=True)
class _Attempt:
    attempt_id: str
    eligible_pool_ids: tuple[str, ...]
    required_capabilities: tuple[str, ...]
    local_priority: int
    oldest_key: str
    assigned: bool = False
    required_shape_id: str | None = None


@dataclass(frozen=True, slots=True)
class _SlotRequirement:
    identity: str
    kind: Literal["assigned", "pending", "warm"]
    required_capabilities: tuple[str, ...]
    required_shape_id: str | None
    order: int


@dataclass(frozen=True, slots=True)
class _ShapeInstance:
    instance_id: str
    subject_id: UUID
    shape: WorkerShapeV1


@dataclass(frozen=True, slots=True)
class _SlotToken:
    instance_id: str
    slot_key: str
    shape_id: str
    capabilities: frozenset[str]
    warm_approved: bool
    physical_identity: str | None = None


@dataclass(frozen=True, slots=True)
class _PoolPlanCandidate:
    pool_id: str
    requirements: tuple[_SlotRequirement, ...]
    instances: tuple[_ShapeInstance, ...]
    matching: tuple[tuple[str, _SlotToken], ...]
    witness: PackingWitnessV1
    headroom: Fraction


@dataclass(slots=True)
class _PoolState:
    pool_id: str
    configuration: PoolManifestV1
    domains: tuple[ResourceDomainV1, ...]
    fixed_commitments: tuple[ObservedCommitmentV1, ...]
    increase_eligible: bool
    blockers: set[str]
    topology_budget: SearchBudget
    plans_by_subject: dict[UUID, tuple[_ShapeInstance, ...]] = field(default_factory=dict)
    witness: PackingWitnessV1 | None = None

    def all_instances(
        self,
        *,
        replacement_subject: UUID | None = None,
        replacement: tuple[_ShapeInstance, ...] = (),
    ) -> tuple[_ShapeInstance, ...]:
        values: list[_ShapeInstance] = []
        for subject_id, instances in self.plans_by_subject.items():
            if subject_id != replacement_subject:
                values.extend(instances)
        values.extend(replacement)
        return tuple(sorted(values, key=lambda item: item.instance_id))

    def try_witness(
        self,
        *,
        subject_id: UUID,
        replacement: tuple[_ShapeInstance, ...],
    ) -> PackingWitnessV1 | None:
        if replacement and not self.increase_eligible:
            return None
        request = PackingRequestV1(
            pool_id=self.pool_id,
            domains=self.domains,
            fixed_commitments=self.fixed_commitments,
            desired_shapes=tuple(
                PackingShapeRequestV1(instance_id=item.instance_id, shape=item.shape)
                for item in self.all_instances(
                    replacement_subject=subject_id,
                    replacement=replacement,
                )
            ),
        )
        try:
            return pack_topology(request, budget=self.topology_budget)
        except TopologyInfeasible:
            return None
        except TopologySearchLimit as exc:
            raise ShadowAllocatorError(str(exc)) from exc

    def commit(self, candidate: _PoolPlanCandidate, subject_id: UUID) -> None:
        self.plans_by_subject[subject_id] = candidate.instances
        self.witness = candidate.witness


@dataclass(slots=True)
class _SubjectState:
    value: SubjectAllocationInputV1
    profiles: dict[str, ProfileReferenceV1]
    valid_for_increase: bool
    blockers: set[str]
    requested_slots: int
    pending: list[_Attempt]
    assignments: list[_Attempt]
    claims: tuple[FixedClaimV1, ...]
    service_by_pool: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    requirements_by_pool: dict[str, list[_SlotRequirement]] = field(
        default_factory=lambda: defaultdict(list)
    )
    matching_by_pool: dict[str, dict[str, _SlotToken]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    claim_matches_by_pool: dict[str, list[ClaimSlotMatchV1]] = field(
        default_factory=lambda: defaultdict(list)
    )
    surge_pairings_by_pool: dict[str, list[RolloutSurgePairingV1]] = field(
        default_factory=lambda: defaultdict(list)
    )
    pool_blockers: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    allocated_pending: set[str] = field(default_factory=set)
    blocked_pending: set[str] = field(default_factory=set)

    @property
    def configuration(self) -> SubjectConfigurationV1:
        return self.value.configuration

    @property
    def subject_id(self) -> UUID:
        return self.configuration.subject_id

    @property
    def service_slots(self) -> int:
        return sum(self.service_by_pool.values())

    @property
    def task_service_slots(self) -> int:
        return len(self.claims) + sum(
            1
            for requirements in self.requirements_by_pool.values()
            for requirement in requirements
            if requirement.kind in {"assigned", "pending"}
        )

    def phase_service(self, phase: Literal["minimum", "demand"]) -> int:
        if phase == "minimum":
            return min(
                self.service_slots,
                self.requested_slots,
                self.configuration.min_slots,
            )
        return self.task_service_slots

    def minimum_needed(self) -> bool:
        return self.service_slots < min(self.requested_slots, self.configuration.min_slots)

    def demand_needed(self) -> bool:
        return self.service_slots < self.requested_slots and any(
            item.attempt_id not in self.allocated_pending | self.blocked_pending
            for item in self.pending
        )

    def next_requirement(
        self, phase: Literal["minimum", "demand"], order: int
    ) -> tuple[_Attempt | None, _SlotRequirement] | None:
        if phase == "minimum" and not self.minimum_needed():
            return None
        if phase == "demand" and not self.demand_needed():
            return None
        attempt = next(
            (
                item
                for item in self.pending
                if item.attempt_id not in self.allocated_pending | self.blocked_pending
            ),
            None,
        )
        if attempt is not None:
            return attempt, _SlotRequirement(
                identity=attempt.attempt_id,
                kind="pending",
                required_capabilities=attempt.required_capabilities,
                required_shape_id=attempt.required_shape_id,
                order=order,
            )
        if phase == "minimum":
            return None, _SlotRequirement(
                identity=f"warm-{self.subject_id.hex}-{self.service_slots:08d}",
                kind="warm",
                required_capabilities=(),
                required_shape_id=None,
                order=order,
            )
        return None


def _attempts(value: SubjectAllocationInputV1) -> tuple[list[_Attempt], list[_Attempt]]:
    if value.last_demand is None:
        return [], []
    pending = [
        _Attempt(
            attempt_id=attempt_id,
            eligible_pool_ids=bucket.eligible_pool_ids,
            required_capabilities=bucket.required_capabilities,
            local_priority=bucket.local_priority,
            oldest_key=bucket.oldest_submitted_at.isoformat(),
        )
        for bucket in value.last_demand.pending_unassigned
        for attempt_id in bucket.attempt_ids
    ]
    pending.sort(
        key=lambda item: (
            item.local_priority,
            len(item.eligible_pool_ids),
            item.oldest_key,
            item.attempt_id,
        )
    )
    assignments = [
        _Attempt(
            attempt_id=item.attempt_id,
            eligible_pool_ids=(item.pool_id,),
            required_capabilities=(),
            local_priority=item.local_priority,
            oldest_key=item.submitted_at.isoformat(),
            assigned=True,
            required_shape_id=item.shape_id,
        )
        for item in value.last_demand.current_assignments
    ]
    assignments.sort(key=lambda item: (item.local_priority, item.oldest_key, item.attempt_id))
    return pending, assignments


def _shape_lookup(subject: _SubjectState, pool_id: str) -> dict[str, WorkerShapeV1]:
    profile = subject.profiles.get(pool_id)
    if profile is None:
        return {}
    return {shape.shape_id: shape for shape in profile.worker_shapes}


def _requirement_fits(requirement: _SlotRequirement, token: _SlotToken) -> bool:
    if requirement.kind == "warm" and not token.warm_approved:
        return False
    return (
        requirement.required_shape_id is None or requirement.required_shape_id == token.shape_id
    ) and set(requirement.required_capabilities) <= token.capabilities


def _match_slots(
    requirements: tuple[_SlotRequirement, ...],
    tokens: tuple[_SlotToken, ...],
    budget: _DecisionBudget,
) -> tuple[tuple[str, _SlotToken], ...] | None:
    compatible = {
        requirement.identity: tuple(
            index for index, token in enumerate(tokens) if _requirement_fits(requirement, token)
        )
        for requirement in requirements
    }
    ordered = tuple(
        sorted(
            requirements,
            key=lambda item: (
                len(compatible[item.identity]),
                item.kind == "warm",
                item.order,
                item.identity,
            ),
        )
    )
    token_owner: dict[int, str] = {}
    requirement_by_id = {item.identity: item for item in requirements}

    def augment(identity: str, visited: set[int]) -> bool:
        for token_index in compatible[identity]:
            budget.consume()
            if token_index in visited:
                continue
            visited.add(token_index)
            owner = token_owner.get(token_index)
            if owner is None or augment(owner, visited):
                token_owner[token_index] = identity
                return True
        return False

    for requirement in ordered:
        if not augment(requirement.identity, set()):
            return None
    matched = {identity: tokens[index] for index, identity in token_owner.items()}
    if set(matched) != set(requirement_by_id):  # pragma: no cover - augment invariant
        raise ShadowAllocatorError("internal matching witness is incomplete")
    return tuple(sorted(matched.items()))


def _shape_combinations(
    shapes: tuple[WorkerShapeV1, ...],
    target_slots: int,
    budget: _DecisionBudget,
) -> Iterator[tuple[WorkerShapeV1, ...]]:
    if target_slots == 0:
        yield ()
        return
    if not shapes:
        return
    maximum = max(shape.concurrency_slots for shape in shapes)
    minimum_workers = (target_slots + maximum - 1) // maximum
    maximum_workers = target_slots
    for worker_count in range(minimum_workers, maximum_workers + 1):
        for indexes in combinations_with_replacement(range(len(shapes)), worker_count):
            budget.consume()
            selected = tuple(shapes[index] for index in indexes)
            if sum(shape.concurrency_slots for shape in selected) == target_slots:
                yield selected


def _instances(
    subject_id: UUID,
    pool_id: str,
    selected: tuple[WorkerShapeV1, ...],
) -> tuple[_ShapeInstance, ...]:
    counters: Counter[str] = Counter()
    values: list[_ShapeInstance] = []
    for shape in sorted(selected, key=lambda item: item.shape_id):
        index = counters[shape.shape_id]
        counters[shape.shape_id] += 1
        binding_digest = sha256(f"{subject_id}:{pool_id}:{shape.shape_id}".encode()).hexdigest()[
            :24
        ]
        values.append(
            _ShapeInstance(
                instance_id=f"shape-{binding_digest}-{index:08d}",
                subject_id=subject_id,
                shape=shape,
            )
        )
    return tuple(values)


def _instance_tokens(
    instances: tuple[_ShapeInstance, ...],
    budget: _DecisionBudget,
) -> tuple[_SlotToken, ...]:
    budget.consume_many(checked_sum(tuple(item.shape.concurrency_slots for item in instances)))
    return tuple(
        _SlotToken(
            instance_id=instance.instance_id,
            slot_key=f"{instance.instance_id}-slot-{slot_index:08d}",
            shape_id=instance.shape.shape_id,
            capabilities=frozenset(instance.shape.capabilities),
            warm_approved=instance.shape.warm_approved,
        )
        for instance in instances
        for slot_index in range(instance.shape.concurrency_slots)
    )


def _headroom_fraction(
    witness: PackingWitnessV1,
    domains: tuple[ResourceDomainV1, ...],
) -> Fraction:
    totals = tuple(node.allocatable for domain in domains for node in domain.nodes)
    residuals = tuple(item.residual for item in witness.residuals)
    dimensions = (
        (
            checked_sum(tuple(item.slots for item in residuals)),
            checked_sum(tuple(item.slots for item in totals)),
        ),
        (
            checked_sum(tuple(item.cpu_millicores for item in residuals)),
            checked_sum(tuple(item.cpu_millicores for item in totals)),
        ),
        (
            checked_sum(tuple(item.memory_bytes for item in residuals)),
            checked_sum(tuple(item.memory_bytes for item in totals)),
        ),
        (
            checked_sum(tuple(item.gpu_count for item in residuals)),
            checked_sum(tuple(item.gpu_count for item in totals)),
        ),
    )
    fractions = [Fraction(residual, total) for residual, total in dimensions if total > 0]
    generic_keys = sorted({key for item in totals for key in item.generic})
    fractions.extend(
        Fraction(
            checked_sum(tuple(item.generic.get(key, 0) for item in residuals)),
            total,
        )
        for key in generic_keys
        if (total := checked_sum(tuple(item.generic.get(key, 0) for item in totals))) > 0
    )
    return min(fractions, default=Fraction(0, 1))


def _capacity_binding_is_exact(
    configuration: SubjectConfigurationV1,
    binding: CurrentAssignmentV1 | FixedClaimV1,
) -> bool:
    if isinstance(binding, FixedClaimV1) and (
        binding.deployment_generation < configuration.deployment_generation
    ):
        return any(profile.pool_id == binding.pool_id for profile in configuration.profiles)
    if isinstance(binding, FixedClaimV1) and (
        binding.deployment_generation > configuration.deployment_generation
    ):
        return False
    profile = next(
        (value for value in configuration.profiles if value.pool_id == binding.pool_id),
        None,
    )
    return (
        profile is not None
        and binding.pool_generation == profile.pool_generation
        and binding.profile_generation == profile.profile_generation
        and binding.profile_digest == profile.profile_digest
        and binding.shape_id in {shape.shape_id for shape in profile.worker_shapes}
    )


class _AllocationState:
    def __init__(self, value: AllocationInputV1, bounds: AllocatorSearchBounds) -> None:
        self.value = value
        self.bounds = bounds
        self.budget = _DecisionBudget(bounds.max_allocation_decisions)
        self.global_blockers: set[str] = set()
        self.order = 0
        self._validate_input()
        self.subjects = self._build_subjects()
        self.subject_by_id = {item.subject_id: item for item in self.subjects}
        accounts = value.effective_account_policies or value.fleet.account_policies
        self.account_policies = {item.account_id: item for item in accounts}
        self.tier_policies = {item.tier_id: item for item in value.fleet.tiers}
        self.fixed_commitments = self._fixed_commitments()
        self.pools = self._build_pools()
        self.claimed_slot_indexes: dict[str, set[int]] = defaultdict(set)
        self.retained_tokens: dict[tuple[UUID, str], tuple[_SlotToken, ...]] = {}
        self._charge_claims()
        self._build_retained_tokens()

    def _validate_input(self) -> None:
        fleet = self.value.fleet
        try:
            validate_fleet_manifest_digests(fleet)
        except FleetStateError as exc:
            raise ShadowAllocatorError(str(exc)) from exc
        reference = self.value.configuration.fleet
        if reference.generation != fleet.fleet_generation:
            raise ShadowAllocatorError("active fleet generation binding is inconsistent")
        if reference.digest != canonical_digest(fleet):
            raise ShadowAllocatorError("active fleet digest binding is inconsistent")
        manifest_refs = {item.subject_id: item for item in self.value.configuration.subjects}
        input_subjects = {item.configuration.subject_id: item for item in self.value.subjects}
        if set(manifest_refs) != set(input_subjects):
            raise ShadowAllocatorError("active subject manifest is incomplete")
        accounts = self.value.effective_account_policies or fleet.account_policies
        if self.value.effective_account_policies:
            effective = {account.account_id: account for account in accounts}
            for policy in fleet.account_policies:
                if effective.get(policy.account_id) != policy:
                    raise ShadowAllocatorError(
                        "effective capacity accounts do not preserve fleet policy"
                    )
            template = fleet.development_subject_template
            template_policy = (
                None
                if template is None
                else next(
                    (
                        policy
                        for policy in fleet.account_policies
                        if policy.account_id == template.owner_account_template_id
                    ),
                    None,
                )
            )
            static_ids = {policy.account_id for policy in fleet.account_policies}
            for policy in accounts:
                if policy.account_id in static_ids:
                    continue
                if (
                    template_policy is None
                    or policy.kind != "owner"
                    or policy.owner_id is None
                    or policy.account_id != f"dev-owner-{policy.owner_id.hex}"
                    or policy
                    != template_policy.model_copy(
                        update={
                            "account_id": f"dev-owner-{policy.owner_id.hex}",
                            "kind": "owner",
                            "owner_id": policy.owner_id,
                        }
                    )
                ):
                    raise ShadowAllocatorError(
                        "effective capacity account is not derived from fleet policy"
                    )
        account_ids = {account.account_id for account in accounts}
        for subject_id, item in input_subjects.items():
            config = item.configuration
            binding = manifest_refs[subject_id]
            if (
                binding.subject_incarnation != config.subject_incarnation
                or binding.generation != config.configuration_generation
                or binding.digest != canonical_digest(config)
            ):
                raise ShadowAllocatorError("active subject generation binding is inconsistent")
            for profile in config.profiles:
                try:
                    validate_profile_narrowing(fleet, profile)
                except FleetStateError as exc:
                    raise ShadowAllocatorError(str(exc)) from exc
            if config.account_id not in account_ids:
                raise ShadowAllocatorError("subject references an unknown capacity account")

        fleet_pools = {pool.pool_id: pool for pool in fleet.pools}
        input_pools = {pool.configuration.pool_id: pool for pool in self.value.pools}
        if set(fleet_pools) != set(input_pools):
            raise ShadowAllocatorError("allocation pool manifest is incomplete")
        for pool_id, manifest in fleet_pools.items():
            if canonical_bytes(input_pools[pool_id].configuration) != canonical_bytes(manifest):
                raise ShadowAllocatorError("allocation pool generation binding is inconsistent")
        subject_incarnations = {
            subject_id: item.configuration.subject_incarnation
            for subject_id, item in input_subjects.items()
        }
        for commitment in self.value.observed_commitments:
            if commitment.pool_id not in fleet_pools:
                raise ShadowAllocatorError(
                    f"observed commitment references unknown pool {commitment.pool_id!r}"
                )
            if commitment.subject_id is None:
                if commitment.subject_incarnation is not None:
                    raise ShadowAllocatorError(
                        "unattributed commitment has a partial subject binding"
                    )
                continue
            if subject_incarnations.get(commitment.subject_id) != commitment.subject_incarnation:
                raise ShadowAllocatorError(
                    "observed commitment references an inactive subject incarnation"
                )
        commitment_keys = [
            (item.kind, item.commitment_id) for item in self.value.observed_commitments
        ]
        if len(commitment_keys) != len(set(commitment_keys)):
            raise ShadowAllocatorError("duplicate observed commitment identity")

    def _claims_for_subject(
        self,
        item: SubjectAllocationInputV1,
    ) -> tuple[FixedClaimV1, ...]:
        claims = {
            claim.claim_id: claim
            for claim in (() if item.last_demand is None else item.last_demand.fixed_claims)
        }
        allowed_states = {
            "pending",
            "live",
            "cancel-pending",
            "unknown",
            "quarantined",
        }
        for evidence in self.value.observed_commitments:
            if (
                evidence.kind != "claim"
                or evidence.subject_id != item.configuration.subject_id
                or evidence.subject_incarnation != item.configuration.subject_incarnation
                or evidence.attempt_id is None
                or evidence.concurrency_slots is None
                or evidence.profile_id is None
                or evidence.profile_generation is None
                or evidence.profile_digest is None
                or evidence.deployment_generation is None
            ):
                continue
            state = evidence.state if evidence.state in allowed_states else "unknown"
            claims.setdefault(
                evidence.commitment_id,
                FixedClaimV1(
                    claim_id=evidence.commitment_id,
                    attempt_id=evidence.attempt_id,
                    worker_identity=evidence.physical_identity,
                    pool_id=evidence.pool_id,
                    pool_generation=evidence.pool_generation,
                    profile_id=evidence.profile_id,
                    profile_generation=evidence.profile_generation,
                    profile_digest=evidence.profile_digest,
                    shape_id=evidence.shape_id or "unknown-shape",
                    deployment_generation=evidence.deployment_generation,
                    concurrency_slots=evidence.concurrency_slots,
                    resources=evidence.resources,
                    state=state,  # type: ignore[arg-type]
                ),
            )
        return tuple(sorted(claims.values(), key=lambda claim: claim.claim_id))

    def _build_subjects(self) -> tuple[_SubjectState, ...]:
        values: list[_SubjectState] = []
        for item in self.value.subjects:
            pending, assignments = _attempts(item)
            claims = self._claims_for_subject(item)
            claimed_attempts = {claim.attempt_id for claim in claims}
            pending = [attempt for attempt in pending if attempt.attempt_id not in claimed_attempts]
            assignments = [
                attempt for attempt in assignments if attempt.attempt_id not in claimed_attempts
            ]
            freshness = item.freshness.state
            exact_report = (
                item.last_demand is not None
                and item.last_demand.subject_id == item.configuration.subject_id
                and item.last_demand.subject_incarnation == item.configuration.subject_incarnation
                and item.last_demand.configuration_generation
                == item.configuration.configuration_generation
                and item.last_demand.deployment_generation
                == item.configuration.deployment_generation
                and item.last_demand.reporter_incarnation
                == item.configuration.demand_reporter_incarnation
            )
            exact_capacity_bindings = (
                item.last_demand is not None
                and all(
                    _capacity_binding_is_exact(item.configuration, binding)
                    for binding in item.last_demand.current_assignments
                )
                and all(
                    _capacity_binding_is_exact(item.configuration, binding)
                    for binding in item.last_demand.fixed_claims
                )
            )
            valid = (
                freshness == "valid"
                and exact_report
                and exact_capacity_bindings
                and item.configuration.lifecycle_state in {"active", "provisioning"}
            )
            blockers: set[str] = set()
            if not valid:
                if freshness != "valid":
                    blockers.add(f"subject_input_{freshness}")
                elif not exact_report:
                    blockers.add("subject_report_binding_invalid")
                elif item.configuration.lifecycle_state not in {
                    "active",
                    "provisioning",
                }:
                    blockers.add("subject_lifecycle_ineligible")
                if exact_report and not exact_capacity_bindings:
                    blockers.add("subject_capacity_binding_invalid")
            runnable = len(pending) + len(assignments)
            requested = min(
                item.configuration.max_slots,
                max(item.configuration.min_slots, len(claims) + runnable),
            )
            values.append(
                _SubjectState(
                    value=item,
                    profiles={profile.pool_id: profile for profile in item.configuration.profiles},
                    valid_for_increase=valid,
                    blockers=blockers,
                    requested_slots=requested,
                    pending=pending if valid else [],
                    assignments=(assignments if exact_report and exact_capacity_bindings else []),
                    claims=claims,
                )
            )
        return tuple(sorted(values, key=lambda item: item.subject_id.hex))

    def _fixed_commitments(self) -> tuple[ObservedCommitmentV1, ...]:
        # Persistent observations are already monotonic and conservative.  Package 1
        # never derives a release from omission in a demand or pool report.
        observed = [
            item for item in self.value.observed_commitments if item.kind in {"physical", "reserve"}
        ]
        authenticated_by_reservation: defaultdict[str, list[ObservedCommitmentV1]] = defaultdict(
            list
        )
        for item in observed:
            if (
                item.kind == "physical"
                and item.ownership_state == "authenticated"
                and item.reservation_identity is not None
            ):
                authenticated_by_reservation[item.reservation_identity].append(item)
        deduplicated: list[ObservedCommitmentV1] = []
        for item in observed:
            if item.kind != "reserve" or item.reservation_identity is None:
                deduplicated.append(item)
                continue
            matches = tuple(
                candidate
                for candidate in authenticated_by_reservation.get(item.reservation_identity, ())
                if candidate.subject_id == item.subject_id
                and candidate.subject_incarnation == item.subject_incarnation
                and candidate.deployment_generation == item.deployment_generation
                and candidate.pool_id == item.pool_id
                and candidate.pool_generation == item.pool_generation
                and candidate.profile_id == item.profile_id
                and candidate.profile_generation == item.profile_generation
                and candidate.profile_digest == item.profile_digest
                and candidate.shape_id == item.shape_id
                and candidate.resources == item.resources
            )
            if len(matches) != 1:
                deduplicated.append(item)
        observed = deduplicated
        by_physical: defaultdict[str, list[ObservedCommitmentV1]] = defaultdict(list)
        for commitment in observed:
            by_physical[commitment.physical_identity].append(commitment)
        unmatched: defaultdict[tuple[UUID, str, str], list[FixedClaimV1]] = defaultdict(list)
        for subject in self.subjects:
            for claim in subject.claims:
                candidates = tuple(
                    item
                    for item in by_physical.get(claim.worker_identity, ())
                    if item.pool_id == claim.pool_id
                    and item.subject_id == subject.subject_id
                    and item.subject_incarnation == subject.configuration.subject_incarnation
                    and item.deployment_generation == claim.deployment_generation
                    and item.pool_generation == claim.pool_generation
                    and item.profile_id == claim.profile_id
                    and item.profile_generation == claim.profile_generation
                    and item.profile_digest == claim.profile_digest
                    and item.shape_id == claim.shape_id
                    and item.resources == claim.resources
                    and item.kind == "physical"
                    and item.state in {"observed", "live"}
                )
                if len(candidates) != 1:
                    unmatched[(subject.subject_id, claim.pool_id, claim.worker_identity)].append(
                        claim
                    )
        pool_generations = {pool.pool_id: pool.pool_generation for pool in self.value.fleet.pools}
        for (subject_id, pool_id, worker_identity), claims in sorted(
            unmatched.items(),
            key=lambda item: (item[0][0].hex, item[0][1], item[0][2]),
        ):
            subject = self.subject_by_id[subject_id]
            generic_keys = sorted({key for claim in claims for key in claim.resources.generic})
            resources = ResourceVectorV1(
                slots=max(max(claim.resources.slots, claim.concurrency_slots) for claim in claims),
                cpu_millicores=max(claim.resources.cpu_millicores for claim in claims),
                memory_bytes=max(claim.resources.memory_bytes for claim in claims),
                gpu_count=max(claim.resources.gpu_count for claim in claims),
                generic={
                    key: max(claim.resources.generic.get(key, 0) for claim in claims)
                    for key in generic_keys
                },
            )
            representative = max(
                claims,
                key=lambda claim: (
                    claim.resources.slots,
                    claim.resources.cpu_millicores,
                    claim.resources.memory_bytes,
                    claim.resources.gpu_count,
                    claim.claim_id,
                ),
            )
            identity_digest = sha256(
                f"{subject_id}:{pool_id}:{worker_identity}".encode()
            ).hexdigest()[:24]
            observed.append(
                ObservedCommitmentV1(
                    kind="reserve",
                    commitment_id=f"claim-reserve-{identity_digest}",
                    physical_identity=f"claim-reserve-worker-{identity_digest}",
                    subject_id=subject_id,
                    subject_incarnation=subject.configuration.subject_incarnation,
                    deployment_generation=representative.deployment_generation,
                    pool_id=pool_id,
                    pool_generation=pool_generations[pool_id],
                    profile_id=representative.profile_id,
                    profile_generation=representative.profile_generation,
                    profile_digest=representative.profile_digest,
                    shape_id=representative.shape_id,
                    resources=resources,
                    state="quarantined",
                    node_ids=(),
                )
            )
        return tuple(sorted(observed, key=lambda item: item.commitment_id))

    def _build_pools(self) -> dict[str, _PoolState]:
        inputs = {item.configuration.pool_id: item for item in self.value.pools}
        values: dict[str, _PoolState] = {}
        for manifest in self.value.fleet.pools:
            item = inputs[manifest.pool_id]
            observation = item.last_observation
            exact_report = (
                observation is not None
                and observation.pool_id == manifest.pool_id
                and observation.pool_generation == manifest.pool_generation
                and observation.reporter_incarnation == manifest.pool_reporter_incarnation
            )
            eligible = (
                item.freshness.state == "valid"
                and exact_report
                and manifest.health == "eligible"
                and observation is not None
                and observation.health == "eligible"
            )
            blockers: set[str] = set()
            if not eligible:
                if item.freshness.state != "valid":
                    blockers.add(f"pool_input_{item.freshness.state}")
                elif not exact_report:
                    blockers.add("pool_report_binding_invalid")
                elif manifest.health != "eligible" or (
                    observation is not None and observation.health != "eligible"
                ):
                    blockers.add("pool_health_ineligible")
            state = _PoolState(
                pool_id=manifest.pool_id,
                configuration=manifest,
                domains=manifest.resource_domains,
                fixed_commitments=tuple(
                    item for item in self.fixed_commitments if item.pool_id == manifest.pool_id
                ),
                increase_eligible=eligible,
                blockers=blockers,
                topology_budget=self.bounds.topology_budget(),
            )
            witness = state.try_witness(subject_id=UUID(int=0), replacement=())
            if witness is None:  # fixed-only packing must return a conservative witness
                raise ShadowAllocatorError("fixed commitments could not be charged")
            state.witness = witness
            if not witness.new_placement_allowed:
                state.increase_eligible = False
                state.blockers.update(witness.blockers)
            uncertain_states = {
                commitment.state
                for commitment in state.fixed_commitments
                if commitment.state in {"unknown", "quarantined", "submitting-unknown"}
            }
            if uncertain_states:
                state.increase_eligible = False
                state.blockers.update(
                    f"commitment_state_{commitment_state}" for commitment_state in uncertain_states
                )
            values[manifest.pool_id] = state
        return values

    def _charge_claims(self) -> None:
        observations_by_physical: defaultdict[str, list[ObservedCommitmentV1]] = defaultdict(list)
        for commitment in self.fixed_commitments:
            observations_by_physical[commitment.physical_identity].append(commitment)
        for subject in self.subjects:
            for claim in subject.claims:
                subject.service_by_pool[claim.pool_id] += 1
                candidates = tuple(
                    item
                    for item in observations_by_physical.get(claim.worker_identity, ())
                    if item.pool_id == claim.pool_id
                    and item.subject_id == subject.subject_id
                    and item.subject_incarnation == subject.configuration.subject_incarnation
                    and item.deployment_generation == claim.deployment_generation
                    and item.pool_generation == claim.pool_generation
                    and item.profile_id == claim.profile_id
                    and item.profile_generation == claim.profile_generation
                    and item.profile_digest == claim.profile_digest
                    and item.shape_id == claim.shape_id
                    and item.resources == claim.resources
                    and item.kind == "physical"
                    and item.state in {"observed", "live"}
                )
                if len(candidates) != 1:
                    subject.pool_blockers[claim.pool_id].add("claim_binding_quarantined")
                    continue
                commitment = candidates[0]
                used = self.claimed_slot_indexes[commitment.commitment_id]
                slot_index = next(
                    (index for index in range(commitment.resources.slots) if index not in used),
                    None,
                )
                if slot_index is None:
                    subject.pool_blockers[claim.pool_id].add("claim_slot_overcommitted")
                    continue
                used.add(slot_index)
                subject.claim_matches_by_pool[claim.pool_id].append(
                    ClaimSlotMatchV1(
                        claim_id=claim.claim_id,
                        physical_identity=commitment.physical_identity,
                        slot_index=slot_index,
                    )
                )

    def _build_retained_tokens(self) -> None:
        tokens: defaultdict[tuple[UUID, str], list[_SlotToken]] = defaultdict(list)
        for commitment in self.fixed_commitments:
            if commitment.subject_id is None:
                continue
            subject = self.subject_by_id.get(commitment.subject_id)
            if (
                subject is None
                or (
                    commitment.kind == "physical"
                    and commitment.state not in {"accepted", "pending", "live", "observed"}
                )
                or (
                    commitment.kind == "reserve"
                    and commitment.state not in {"proposed", "accepted"}
                )
                or commitment.kind not in {"physical", "reserve"}
            ):
                continue
            shape = _shape_lookup(subject, commitment.pool_id).get(commitment.shape_id or "")
            profile = subject.profiles.get(commitment.pool_id)
            if (
                profile is None
                or shape is None
                or commitment.deployment_generation != subject.configuration.deployment_generation
                or commitment.pool_generation != profile.pool_generation
                or commitment.profile_generation != profile.profile_generation
                or commitment.profile_digest != profile.profile_digest
                or commitment.resources != shape.total_resources
            ):
                subject.pool_blockers[commitment.pool_id].add(
                    "commitment_shape_binding_quarantined"
                )
                continue
            claimed = self.claimed_slot_indexes.get(commitment.commitment_id, set())
            self.budget.consume_many(shape.concurrency_slots)
            for slot_index in range(shape.concurrency_slots):
                if slot_index in claimed:
                    continue
                tokens[(subject.subject_id, commitment.pool_id)].append(
                    _SlotToken(
                        instance_id=commitment.commitment_id,
                        slot_key=f"{commitment.commitment_id}-slot-{slot_index:08d}",
                        shape_id=shape.shape_id,
                        capabilities=frozenset(shape.capabilities),
                        warm_approved=shape.warm_approved,
                        physical_identity=(
                            commitment.physical_identity if commitment.kind == "physical" else None
                        ),
                    )
                )
        self.retained_tokens = {
            key: tuple(sorted(value, key=lambda item: item.slot_key))
            for key, value in tokens.items()
        }

    def _fixed_slots(self, *, pool_id: str | None = None, subject_id: UUID | None = None) -> int:
        return checked_sum(
            tuple(
                item.resources.slots
                for item in self.fixed_commitments
                if (pool_id is None or item.pool_id == pool_id)
                and (subject_id is None or item.subject_id == subject_id)
            )
        )

    def _planned_slots(
        self,
        *,
        pool_id: str | None = None,
        subject_id: UUID | None = None,
        replacement_pool: str | None = None,
        replacement_subject: UUID | None = None,
        replacement: tuple[_ShapeInstance, ...] = (),
    ) -> int:
        values: list[int] = []
        for current_pool_id, pool in self.pools.items():
            if pool_id is not None and current_pool_id != pool_id:
                continue
            for current_subject_id, instances in pool.plans_by_subject.items():
                if (
                    current_pool_id == replacement_pool
                    and current_subject_id == replacement_subject
                ):
                    continue
                if subject_id is None or current_subject_id == subject_id:
                    values.extend(item.shape.concurrency_slots for item in instances)
        if (
            replacement_pool is not None
            and (pool_id is None or pool_id == replacement_pool)
            and (subject_id is None or subject_id == replacement_subject)
        ):
            values.extend(item.shape.concurrency_slots for item in replacement)
        return checked_sum(tuple(values))

    def _scope_allows(
        self,
        subject: _SubjectState,
        pool_id: str,
        replacement: tuple[_ShapeInstance, ...],
        *,
        allow_surge: bool = False,
        surge_backing_slots: int = 0,
    ) -> bool:
        pool = self.pools[pool_id]
        pool_slots = checked_add(
            self._fixed_slots(pool_id=pool_id),
            self._planned_slots(
                pool_id=pool_id,
                replacement_pool=pool_id,
                replacement_subject=subject.subject_id,
                replacement=replacement,
            ),
        )
        if pool_slots > pool.configuration.max_slots:
            return False
        subject_slots = checked_add(
            self._fixed_slots(subject_id=subject.subject_id),
            self._planned_slots(
                subject_id=subject.subject_id,
                replacement_pool=pool_id,
                replacement_subject=subject.subject_id,
                replacement=replacement,
            ),
        )
        subject_excess = max(0, subject_slots - subject.configuration.max_slots)
        if (
            subject_excess > (subject.configuration.rollout_surge_slots if allow_surge else 0)
            or subject_excess > surge_backing_slots
        ):
            return False
        account_id = subject.configuration.account_id
        account_subject_ids = {
            item.subject_id for item in self.subjects if item.configuration.account_id == account_id
        }
        account_fixed = checked_sum(
            tuple(
                item.resources.slots
                for item in self.fixed_commitments
                if item.subject_id in account_subject_ids
            )
        )
        account_planned = checked_sum(
            tuple(
                self._planned_slots(
                    subject_id=item,
                    replacement_pool=pool_id,
                    replacement_subject=subject.subject_id,
                    replacement=replacement,
                )
                for item in account_subject_ids
            )
        )
        account_policy = self.account_policies[account_id]
        account_ceiling = checked_add(
            account_policy.max_slots,
            account_policy.max_surge_slots if allow_surge else 0,
        )
        if checked_add(account_fixed, account_planned) > account_ceiling:
            return False
        tier_id = subject.configuration.tier_id
        tier_subject_ids = {
            item.subject_id for item in self.subjects if item.configuration.tier_id == tier_id
        }
        tier_fixed = checked_sum(
            tuple(
                item.resources.slots
                for item in self.fixed_commitments
                if item.subject_id in tier_subject_ids
            )
        )
        tier_planned = checked_sum(
            tuple(
                self._planned_slots(
                    subject_id=item,
                    replacement_pool=pool_id,
                    replacement_subject=subject.subject_id,
                    replacement=replacement,
                )
                for item in tier_subject_ids
            )
        )
        return checked_add(tier_fixed, tier_planned) <= self.tier_policies[tier_id].max_slots

    def _candidate_for_pool(
        self,
        subject: _SubjectState,
        pool_id: str,
        attempt: _Attempt | None,
        requirement: _SlotRequirement,
        *,
        allow_new_shapes: bool,
    ) -> _PoolPlanCandidate | None:
        pool = self.pools.get(pool_id)
        profile = subject.profiles.get(pool_id)
        if pool is None or profile is None or (allow_new_shapes and not pool.increase_eligible):
            return None
        requirements = tuple((*subject.requirements_by_pool[pool_id], requirement))
        retained = self.retained_tokens.get((subject.subject_id, pool_id), ())
        shapes = tuple(
            sorted(
                (
                    shape
                    for shape in profile.worker_shapes
                    if (
                        set(requirement.required_capabilities) <= set(shape.capabilities)
                        or requirement.kind == "warm"
                    )
                    and (
                        attempt is None
                        or attempt.required_shape_id is None
                        or shape.shape_id == attempt.required_shape_id
                    )
                ),
                key=lambda item: (
                    len(item.compatible_domain_ids),
                    -item.concurrency_slots,
                    item.shape_id,
                ),
            )
        )
        if requirement.kind == "warm":
            shapes = tuple(shape for shape in shapes if shape.warm_approved)
        if not shapes and len(requirements) > len(retained):
            return None

        lower_bound = max(0, len(requirements) - len(retained))
        maximum_new = min(
            len(requirements),
            subject.configuration.max_slots - self._fixed_slots(subject_id=subject.subject_id),
        )
        if not allow_new_shapes:
            maximum_new = 0
        best: _PoolPlanCandidate | None = None
        for new_slots in range(lower_bound, maximum_new + 1):
            for selected in _shape_combinations(shapes, new_slots, self.budget):
                instances = _instances(subject.subject_id, pool_id, selected)
                if not self._scope_allows(subject, pool_id, instances):
                    continue
                tokens = tuple((*retained, *_instance_tokens(instances, self.budget)))
                matching = _match_slots(requirements, tokens, self.budget)
                if matching is None:
                    continue
                witness = pool.try_witness(
                    subject_id=subject.subject_id,
                    replacement=instances,
                )
                if witness is None:
                    continue
                candidate = _PoolPlanCandidate(
                    pool_id=pool_id,
                    requirements=requirements,
                    instances=instances,
                    matching=matching,
                    witness=witness,
                    headroom=_headroom_fraction(witness, pool.domains),
                )
                if best is None or (
                    len(candidate.instances),
                    -candidate.headroom,
                    tuple(item.instance_id for item in candidate.instances),
                ) < (
                    len(best.instances),
                    -best.headroom,
                    tuple(item.instance_id for item in best.instances),
                ):
                    best = candidate
            if best is not None:
                break
        return best

    def _place_requirement(
        self,
        subject: _SubjectState,
        attempt: _Attempt | None,
        requirement: _SlotRequirement,
        *,
        allow_new_shapes: bool = True,
        preserve_existing: bool = False,
    ) -> bool:
        if not subject.valid_for_increase and not preserve_existing:
            return False
        eligible_pool_ids = (
            tuple(subject.profiles)
            if attempt is None
            else tuple(
                pool_id for pool_id in attempt.eligible_pool_ids if pool_id in subject.profiles
            )
        )
        candidates = tuple(
            candidate
            for pool_id in sorted(eligible_pool_ids)
            if (
                candidate := self._candidate_for_pool(
                    subject,
                    pool_id,
                    attempt,
                    requirement,
                    allow_new_shapes=allow_new_shapes,
                )
            )
            is not None
        )
        if not candidates:
            for pool_id in eligible_pool_ids:
                subject.pool_blockers[pool_id].add("no_feasible_topology_placement")
            if attempt is not None and not attempt.assigned:
                subject.blocked_pending.add(attempt.attempt_id)
            return False
        selected = min(
            candidates,
            key=lambda item: (
                -item.headroom,
                len(item.instances),
                item.pool_id,
                tuple(instance.instance_id for instance in item.instances),
            ),
        )
        pool = self.pools[selected.pool_id]
        pool.commit(selected, subject.subject_id)
        subject.requirements_by_pool[selected.pool_id] = list(selected.requirements)
        subject.matching_by_pool[selected.pool_id] = dict(selected.matching)
        subject.service_by_pool[selected.pool_id] += 1
        if requirement.kind == "pending":
            subject.allocated_pending.add(requirement.identity)
        return True

    def preserve_assignments(self) -> None:
        for subject in sorted(
            self.subjects,
            key=lambda item: (
                self.tier_policies[item.configuration.tier_id].priority,
                item.configuration.account_id,
                item.subject_id.hex,
            ),
        ):
            for attempt in subject.assignments:
                if subject.service_slots >= subject.requested_slots:
                    break
                self.order += 1
                requirement = _SlotRequirement(
                    identity=attempt.attempt_id,
                    kind="assigned",
                    required_capabilities=attempt.required_capabilities,
                    required_shape_id=attempt.required_shape_id,
                    order=self.order,
                )
                if not self._place_requirement(
                    subject,
                    attempt,
                    requirement,
                    allow_new_shapes=False,
                    preserve_existing=True,
                ):
                    subject.pool_blockers[attempt.eligible_pool_ids[0]].add(
                        "current_assignment_not_preserved"
                    )

    def _cursor_start(
        self,
        tier_id: str,
        phase: Literal["minimum", "demand"],
        account_ids: tuple[str, ...],
    ) -> int:
        cursor = next(
            (
                item
                for item in self.value.fairness_cursors
                if item.tier_id == tier_id
                and item.phase == phase
                and item.subject_id is None
                and item.account_id in account_ids
            ),
            None,
        )
        if cursor is None or cursor.account_id is None:
            return 0
        return account_ids.index(cursor.account_id)

    def _subject_cursor_start(
        self,
        tier_id: str,
        phase: Literal["minimum", "demand"],
        account_id: str,
        subjects: tuple[_SubjectState, ...],
    ) -> int:
        subject_ids = tuple(subject.subject_id for subject in subjects)
        cursor = next(
            (
                item
                for item in self.value.fairness_cursors
                if item.tier_id == tier_id
                and item.phase == phase
                and item.account_id == account_id
                and item.subject_id in subject_ids
            ),
            None,
        )
        if cursor is None or cursor.subject_id is None:
            return 0
        return subject_ids.index(cursor.subject_id)

    def progressive_fill(
        self,
        tier: TierPolicyV1,
        phase: Literal["minimum", "demand"],
    ) -> None:
        eligible = tuple(
            item
            for item in self.subjects
            if item.configuration.tier_id == tier.tier_id and item.valid_for_increase
        )
        by_account: defaultdict[str, list[_SubjectState]] = defaultdict(list)
        for subject in eligible:
            by_account[subject.configuration.account_id].append(subject)
        account_ids = tuple(sorted(by_account))
        if not account_ids:
            return
        start = self._cursor_start(tier.tier_id, phase, account_ids)
        account_order = account_ids[start:] + account_ids[:start]
        account_rank = {account_id: index for index, account_id in enumerate(account_order)}
        stable_subjects_by_account = {
            account_id: tuple(sorted(by_account[account_id], key=lambda item: item.subject_id.hex))
            for account_id in account_ids
        }
        subject_offsets = {
            account_id: self._subject_cursor_start(
                tier.tier_id,
                phase,
                account_id,
                stable_subjects_by_account[account_id],
            )
            for account_id in account_ids
        }
        last_success: str | None = None
        while True:
            accounts_with_need = tuple(
                account_id
                for account_id in account_ids
                if any(
                    subject.minimum_needed() if phase == "minimum" else subject.demand_needed()
                    for subject in by_account[account_id]
                )
            )
            if not accounts_with_need:
                break
            ordered_accounts = tuple(
                sorted(
                    accounts_with_need,
                    key=lambda account_id: (
                        sum(subject.phase_service(phase) for subject in by_account[account_id]),
                        account_rank[account_id],
                    ),
                )
            )
            made_progress = False
            for account_id in ordered_accounts:
                stable_subjects = stable_subjects_by_account[account_id]
                offset = subject_offsets[account_id] % len(stable_subjects)
                rotated = stable_subjects[offset:] + stable_subjects[:offset]
                subjects = tuple(
                    sorted(
                        rotated,
                        key=lambda item: (
                            item.phase_service(phase),
                            rotated.index(item),
                        ),
                    )
                )
                for selected in subjects:
                    while True:
                        self.order += 1
                        request = selected.next_requirement(phase, self.order)
                        if request is None:
                            break
                        if self._place_requirement(selected, *request):
                            subject_offsets[account_id] = (
                                stable_subjects.index(selected) + 1
                            ) % len(stable_subjects)
                            next_subject = stable_subjects[subject_offsets[account_id]]
                            self.next_cursors[(tier.tier_id, phase, account_id)] = FairnessCursorV1(
                                tier_id=tier.tier_id,
                                phase=phase,
                                account_id=account_id,
                                subject_id=next_subject.subject_id,
                            )
                            made_progress = True
                            last_success = account_id
                            break
                        attempt = request[0]
                        if attempt is None or attempt.attempt_id not in selected.blocked_pending:
                            break
                    if made_progress:
                        break
                if made_progress:
                    break
            if not made_progress:
                break
        if last_success is not None:
            next_index = (account_ids.index(last_success) + 1) % len(account_ids)
            self.next_cursors[(tier.tier_id, phase, None)] = FairnessCursorV1(
                tier_id=tier.tier_id,
                phase=phase,
                account_id=account_ids[next_index],
            )

    def place_rollout_surge_from_headroom(self) -> None:
        for subject in sorted(
            self.subjects,
            key=lambda item: (
                self.tier_policies[item.configuration.tier_id].priority,
                item.configuration.account_id,
                item.subject_id.hex,
            ),
        ):
            remaining_surge = subject.configuration.rollout_surge_slots
            if remaining_surge == 0:
                continue
            for pool_id in sorted(subject.profiles):
                pool = self.pools[pool_id]
                if not pool.increase_eligible:
                    continue
                current_physical_slots = checked_add(
                    checked_sum(
                        tuple(
                            item.resources.slots
                            for item in self.fixed_commitments
                            if item.kind == "physical"
                            and item.subject_id == subject.subject_id
                            and item.pool_id == pool_id
                            and item.deployment_generation is not None
                            and item.deployment_generation
                            == subject.configuration.deployment_generation
                        )
                    ),
                    self._planned_slots(
                        pool_id=pool_id,
                        subject_id=subject.subject_id,
                    ),
                )
                replacement_needed = max(
                    0,
                    subject.service_by_pool[pool_id] - current_physical_slots,
                )
                if replacement_needed == 0:
                    continue
                old_commitments = tuple(
                    sorted(
                        (
                            item
                            for item in self.fixed_commitments
                            if item.kind == "physical"
                            and item.subject_id == subject.subject_id
                            and item.pool_id == pool_id
                            and item.deployment_generation is not None
                            and item.deployment_generation
                            < subject.configuration.deployment_generation
                            and item.state in {"accepted", "pending", "live", "observed"}
                        ),
                        key=lambda item: item.commitment_id,
                    )
                )
                for old in old_commitments:
                    if replacement_needed == 0 or remaining_surge == 0:
                        break
                    old_backed_slots = 0
                    while (
                        old_backed_slots < old.resources.slots
                        and replacement_needed > 0
                        and remaining_surge > 0
                    ):
                        backed_limit = min(
                            old.resources.slots - old_backed_slots,
                            replacement_needed,
                            remaining_surge,
                        )
                        profile = subject.profiles[pool_id]
                        shapes = tuple(
                            sorted(
                                (
                                    shape
                                    for shape in profile.worker_shapes
                                    if shape.concurrency_slots <= backed_limit
                                ),
                                key=lambda shape: (
                                    shape.shape_id != old.shape_id,
                                    -shape.concurrency_slots,
                                    shape.shape_id,
                                ),
                            )
                        )
                        placed = False
                        for shape in shapes:
                            self.budget.consume()
                            identity = sha256(
                                (
                                    f"{subject.subject_id}:{old.commitment_id}:"
                                    f"{shape.shape_id}:"
                                    f"{len(subject.surge_pairings_by_pool[pool_id])}"
                                ).encode()
                            ).hexdigest()[:24]
                            instance = _ShapeInstance(
                                instance_id=(f"surge-{subject.subject_id.hex[:16]}-{identity}"),
                                subject_id=subject.subject_id,
                                shape=shape,
                            )
                            existing = pool.plans_by_subject.get(subject.subject_id, ())
                            replacement = tuple((*existing, instance))
                            backing_slots = checked_add(
                                checked_sum(
                                    tuple(
                                        pairing.backed_slots
                                        for pairings in subject.surge_pairings_by_pool.values()
                                        for pairing in pairings
                                    )
                                ),
                                shape.concurrency_slots,
                            )
                            if not self._scope_allows(
                                subject,
                                pool_id,
                                replacement,
                                allow_surge=True,
                                surge_backing_slots=backing_slots,
                            ):
                                continue
                            witness = pool.try_witness(
                                subject_id=subject.subject_id,
                                replacement=replacement,
                            )
                            if witness is None:
                                continue
                            pool.commit(
                                _PoolPlanCandidate(
                                    pool_id=pool_id,
                                    requirements=tuple(subject.requirements_by_pool[pool_id]),
                                    instances=replacement,
                                    matching=tuple(subject.matching_by_pool[pool_id].items()),
                                    witness=witness,
                                    headroom=_headroom_fraction(witness, pool.domains),
                                ),
                                subject.subject_id,
                            )
                            subject.surge_pairings_by_pool[pool_id].append(
                                RolloutSurgePairingV1(
                                    old_commitment_id=old.commitment_id,
                                    new_shape_instance_id=instance.instance_id,
                                    backed_slots=shape.concurrency_slots,
                                )
                            )
                            replacement_needed -= shape.concurrency_slots
                            remaining_surge -= shape.concurrency_slots
                            old_backed_slots += shape.concurrency_slots
                            placed = True
                            break
                        if not placed:
                            subject.pool_blockers[pool_id].add("rollout_surge_no_feasible_headroom")
                            break

    def allocate(self) -> ShadowEpochV1:
        self.next_cursors: dict[
            tuple[str, Literal["minimum", "demand"], str | None],
            FairnessCursorV1,
        ] = {}
        self.preserve_assignments()
        for tier in sorted(self.value.fleet.tiers, key=lambda item: item.priority):
            self.progressive_fill(tier, "minimum")
            self.progressive_fill(tier, "demand")
        self.place_rollout_surge_from_headroom()
        return self._build_epoch()

    def _pending_scope_counts(
        self,
    ) -> tuple[
        defaultdict[str, int],
        defaultdict[str, int],
        defaultdict[UUID, int],
        defaultdict[UUID, int],
    ]:
        pool_slots: defaultdict[str, int] = defaultdict(int)
        pool_jobs: defaultdict[str, int] = defaultdict(int)
        subject_slots: defaultdict[UUID, int] = defaultdict(int)
        subject_jobs: defaultdict[UUID, int] = defaultdict(int)
        for commitment in self.fixed_commitments:
            if commitment.state not in _PENDING_COMMITMENT_STATES:
                continue
            pool_slots[commitment.pool_id] += commitment.resources.slots
            pool_jobs[commitment.pool_id] += 1
            if commitment.subject_id is not None:
                subject_slots[commitment.subject_id] += commitment.resources.slots
                subject_jobs[commitment.subject_id] += 1
        return pool_slots, pool_jobs, subject_slots, subject_jobs

    def _launch_order(self) -> tuple[tuple[_SubjectState, str, _ShapeInstance, int], ...]:
        values: list[tuple[_SubjectState, str, _ShapeInstance, int]] = []
        for pool_id, pool in self.pools.items():
            for subject_id, instances in pool.plans_by_subject.items():
                subject = self.subject_by_id[subject_id]
                matched = subject.matching_by_pool[pool_id]
                for instance in instances:
                    orders = [
                        requirement.order
                        for requirement in subject.requirements_by_pool[pool_id]
                        if matched[requirement.identity].instance_id == instance.instance_id
                    ]
                    order = min(orders, default=MAX_ALLOCATION_DECISIONS)
                    values.append((subject, pool_id, instance, order))
        return tuple(
            sorted(
                values,
                key=lambda item: (
                    self.tier_policies[item[0].configuration.tier_id].priority,
                    item[3],
                    item[0].configuration.account_id,
                    item[0].subject_id.hex,
                    item[1],
                    item[2].instance_id,
                ),
            )
        )

    def _rank_launches(self) -> tuple[RankedHypotheticalLaunchV1, ...]:
        pool_slots, pool_jobs, subject_slots, subject_jobs = self._pending_scope_counts()
        tier_slots: defaultdict[str, int] = defaultdict(int)
        tier_jobs: defaultdict[str, int] = defaultdict(int)
        account_slots: defaultdict[str, int] = defaultdict(int)
        account_jobs: defaultdict[str, int] = defaultdict(int)
        represented_pending = tuple(
            item for item in self.fixed_commitments if item.state in _PENDING_COMMITMENT_STATES
        )
        global_slots = checked_add(
            self.value.existing_pending_slots,
            checked_sum(tuple(item.resources.slots for item in represented_pending)),
        )
        global_jobs = checked_add(
            self.value.existing_pending_jobs,
            len(represented_pending),
        )
        for commitment in represented_pending:
            if commitment.subject_id is None:
                continue
            subject = self.subject_by_id.get(commitment.subject_id)
            if subject is None:
                continue
            tier_id = subject.configuration.tier_id
            account_id = subject.configuration.account_id
            tier_slots[tier_id] += commitment.resources.slots
            tier_jobs[tier_id] += 1
            account_slots[account_id] += commitment.resources.slots
            account_jobs[account_id] += 1
        ranked: list[RankedHypotheticalLaunchV1] = []
        for subject, pool_id, instance, _order in self._launch_order():
            slots = instance.shape.concurrency_slots
            tier_id = subject.configuration.tier_id
            account_id = subject.configuration.account_id
            pool_policy = self.pools[pool_id].configuration
            tier_policy = self.tier_policies[tier_id]
            account_policy = self.account_policies[account_id]
            blocked: str | None = None
            if checked_add(global_jobs, 1) > self.value.fleet.global_max_pending_jobs:
                blocked = "global_pending_job_ceiling"
            elif checked_add(global_slots, slots) > self.value.fleet.global_max_pending_slots:
                blocked = "global_pending_slot_ceiling"
            elif checked_add(pool_jobs[pool_id], 1) > pool_policy.max_pending_jobs:
                blocked = "pool_pending_job_ceiling"
            elif checked_add(pool_slots[pool_id], slots) > pool_policy.max_pending_slots:
                blocked = "pool_pending_slot_ceiling"
            elif checked_add(tier_jobs[tier_id], 1) > tier_policy.max_pending_jobs:
                blocked = "tier_pending_job_ceiling"
            elif checked_add(tier_slots[tier_id], slots) > tier_policy.max_pending_slots:
                blocked = "tier_pending_slot_ceiling"
            elif checked_add(account_jobs[account_id], 1) > account_policy.max_pending_jobs:
                blocked = "account_pending_job_ceiling"
            elif checked_add(account_slots[account_id], slots) > account_policy.max_pending_slots:
                blocked = "account_pending_slot_ceiling"
            elif (
                checked_add(subject_jobs[subject.subject_id], 1)
                > subject.configuration.max_pending_jobs
            ):
                blocked = "subject_pending_job_ceiling"
            elif (
                checked_add(subject_slots[subject.subject_id], slots)
                > subject.configuration.max_pending_slots
            ):
                blocked = "subject_pending_slot_ceiling"
            if blocked is not None:
                self.global_blockers.add(blocked)
                subject.pool_blockers[pool_id].add(blocked)
                continue
            global_jobs += 1
            global_slots += slots
            pool_jobs[pool_id] += 1
            pool_slots[pool_id] += slots
            tier_jobs[tier_id] += 1
            tier_slots[tier_id] += slots
            account_jobs[account_id] += 1
            account_slots[account_id] += slots
            subject_jobs[subject.subject_id] += 1
            subject_slots[subject.subject_id] += slots
            ranked.append(
                RankedHypotheticalLaunchV1(
                    rank=len(ranked) + 1,
                    subject_id=subject.subject_id,
                    pool_id=pool_id,
                    shape_instance_id=instance.instance_id,
                )
            )
        return tuple(ranked)

    def _allocation_for_pool(
        self,
        subject: _SubjectState,
        pool_id: str,
        requested_slots: int,
    ) -> ShadowAllocationV1:
        requirements = tuple(subject.requirements_by_pool[pool_id])
        matching = subject.matching_by_pool[pool_id]
        allowances = tuple(
            PlacementAllowanceV1(
                attempt_id=requirement.identity,
                pool_id=pool_id,
                shape_instance_id=matching[requirement.identity].instance_id,
            )
            for requirement in requirements
            if requirement.kind == "pending"
        )
        witness_requirements = tuple(
            requirement
            for requirement in requirements
            if requirement.kind in {"assigned", "pending"}
        )
        witness_pairs = tuple(
            sorted(
                (
                    requirement.identity,
                    matching[requirement.identity].slot_key,
                )
                for requirement in witness_requirements
            )
        )
        matching_witness = (
            JointMatchingWitnessV1(
                matched_slots=len(witness_pairs),
                attempt_ids=tuple(pair[0] for pair in witness_pairs),
                shape_instance_ids=tuple(pair[1] for pair in witness_pairs),
            )
            if witness_requirements
            else None
        )
        commitments = tuple(
            item
            for item in self.fixed_commitments
            if item.subject_id == subject.subject_id and item.pool_id == pool_id
        )
        commitment_ids = {item.commitment_id for item in commitments}
        retained_instance_ids = {
            token.instance_id for token in matching.values() if token.instance_id in commitment_ids
        }
        claimed_physical = {
            item.physical_identity for item in subject.claim_matches_by_pool[pool_id]
        }
        retained_instance_ids.update(
            item.commitment_id for item in commitments if item.physical_identity in claimed_physical
        )
        surge_pairings = tuple(subject.surge_pairings_by_pool[pool_id])
        surge_backing_ids = {pairing.old_commitment_id for pairing in surge_pairings}
        planned = self.pools[pool_id].plans_by_subject.get(subject.subject_id, ())
        shape_counts: Counter[str] = Counter(item.shape.shape_id for item in planned)
        shape_counts.update(
            item.shape_id
            for item in commitments
            if item.commitment_id in retained_instance_ids and item.shape_id is not None
        )
        draining = tuple(
            sorted(
                item.commitment_id
                for item in commitments
                if item.commitment_id in surge_backing_ids
                or (
                    item.kind == "physical"
                    and item.state
                    in {
                        "proposed",
                        "accepted",
                        "pending",
                        "live",
                        "draining",
                        "observed",
                    }
                    and item.commitment_id not in retained_instance_ids
                )
            )
        )
        blockers = set(subject.blockers)
        blockers.update(subject.pool_blockers[pool_id])
        blockers.update(self.pools[pool_id].blockers)
        return ShadowAllocationV1(
            subject_id=subject.subject_id,
            subject_incarnation=subject.configuration.subject_incarnation,
            deployment_generation=subject.configuration.deployment_generation,
            pool_id=pool_id,
            desired_slots=subject.service_by_pool[pool_id],
            requested_slots=requested_slots,
            new_allowance_slots=len(allowances),
            retained_commitment_slots=checked_sum(
                tuple(item.resources.slots for item in commitments)
            ),
            desired_shapes=tuple(
                DesiredShapeCountV1(shape_id=shape_id, count=count)
                for shape_id, count in sorted(shape_counts.items())
            ),
            protected_claim_slots=sum(1 for item in subject.claims if item.pool_id == pool_id),
            physical_committed_shape_slots=checked_sum(
                tuple(item.resources.slots for item in commitments if item.kind == "physical")
            ),
            surge_pairings=surge_pairings,
            draining_shape_ids=draining,
            placement_allowances=tuple(sorted(allowances, key=lambda item: item.attempt_id)),
            claim_slot_matches=tuple(
                sorted(
                    subject.claim_matches_by_pool[pool_id],
                    key=lambda item: item.claim_id,
                )
            ),
            matching_witness=matching_witness,
            blockers=tuple(sorted(blockers)),
        )

    def _requested_slots_by_pool(
        self,
        subject: _SubjectState,
    ) -> dict[str, int]:
        values = {pool_id: subject.service_by_pool[pool_id] for pool_id in subject.profiles}
        remaining = max(0, subject.requested_slots - sum(values.values()))
        if remaining == 0:
            return values
        allocated_attempts = {
            requirement.identity
            for requirements in subject.requirements_by_pool.values()
            for requirement in requirements
            if requirement.kind in {"assigned", "pending"}
        }
        if subject.value.last_demand is not None:
            unplaced_assignments = tuple(
                (item.attempt_id, (item.pool_id,))
                for item in subject.value.last_demand.current_assignments
                if item.attempt_id not in allocated_attempts
            )
            unplaced_pending = tuple(
                (attempt_id, bucket.eligible_pool_ids)
                for bucket in subject.value.last_demand.pending_unassigned
                for attempt_id in bucket.attempt_ids
                if attempt_id not in allocated_attempts
            )
            for _attempt_id, pool_ids in (*unplaced_assignments, *unplaced_pending):
                eligible = tuple(sorted(pool_id for pool_id in pool_ids if pool_id in values))
                if not eligible or remaining == 0:
                    continue
                values[eligible[0]] += 1
                remaining -= 1
        if remaining:
            values[min(values)] += remaining
        return values

    def _build_epoch(self) -> ShadowEpochV1:
        launches = self._rank_launches()
        allocations_list: list[ShadowAllocationV1] = []
        for subject in self.subjects:
            requested_by_pool = self._requested_slots_by_pool(subject)
            allocations_list.extend(
                self._allocation_for_pool(
                    subject,
                    pool_id,
                    requested_by_pool[pool_id],
                )
                for pool_id in sorted(subject.profiles)
            )
        allocations = tuple(allocations_list)
        cursors = {
            (item.tier_id, item.phase, item.account_id, item.subject_id): item
            for item in self.value.fairness_cursors
        }
        for (tier_id, phase, subject_scope_account), item in self.next_cursors.items():
            cursors = {
                cursor_key: cursor
                for cursor_key, cursor in cursors.items()
                if cursor.tier_id != tier_id
                or cursor.phase != phase
                or (subject_scope_account is None and cursor.subject_id is not None)
                or (
                    subject_scope_account is not None
                    and (cursor.subject_id is None or cursor.account_id != subject_scope_account)
                )
            }
            cursors[(item.tier_id, item.phase, item.account_id, item.subject_id)] = item
        return ShadowEpochV1(
            configuration=self.value.configuration,
            input_digest=canonical_digest(self.value),
            allocations=allocations,
            next_fairness_cursors=tuple(
                sorted(
                    cursors.values(),
                    key=lambda item: (
                        self.tier_policies[item.tier_id].priority,
                        item.phase,
                        item.account_id or "",
                        item.subject_id.hex if item.subject_id else "",
                    ),
                )
            ),
            hypothetical_launch_rank=launches,
            pool_witnesses=tuple(
                pool.witness for pool in self.pools.values() if pool.witness is not None
            ),
            blockers=tuple(sorted(self.global_blockers)),
        )


def allocate_shadow(
    value: AllocationInputV1,
    *,
    bounds: AllocatorSearchBounds = AllocatorSearchBounds(),  # noqa: B008
) -> ShadowEpochV1:
    """Compute one complete non-executable global fleet allocation epoch."""

    return _AllocationState(value, bounds).allocate()


__all__ = [
    "MAX_ALLOCATION_DECISIONS",
    "AllocatorSearchBounds",
    "ExecutableAllocationError",
    "ExecutableEpochV2",
    "ShadowAllocatorError",
    "allocate_shadow",
    "promote_shadow_epoch",
]
