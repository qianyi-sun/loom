"""Small convergent projections used by the database-backed main loop."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from loom.pipeline.budget import TerminalCause
from loom.pipeline.gates import GateSelection, project_outcome_gate, strict_and_gate_target
from loom.pipeline.projection import StageTerminalProjection, project_pipeline_result
from loom.pipeline.retry import RetryDecision, retry_decision
from loom.pipeline.state import PipelineRunResult, PipelineStageRunState, RetryClass
from loom_pipeline_orchestrator.repository import (
    AttemptProviderBudgetSpec,
    AttemptReservationSpec,
    FrozenReadiness,
    PipelineRepository,
    ReadinessCandidate,
    RunLease,
)


@dataclass(frozen=True, slots=True)
class GateProjection:
    selection: GateSelection
    target_selection: GateSelection


@dataclass(frozen=True, slots=True)
class RenderedAttempt:
    attempt_id: UUID
    stage_request_json: dict[str, object] | None
    stage_request_bytes: bytes | None
    stage_request_digest: str | None
    reservations: tuple[AttemptReservationSpec, ...]
    provider_budget: AttemptProviderBudgetSpec | None = None
    fault_pending: bool = False

    def __post_init__(self) -> None:
        request_group = (
            self.stage_request_json,
            self.stage_request_bytes,
            self.stage_request_digest,
        )
        if any(value is None for value in request_group) and not all(
            value is None for value in request_group
        ):
            raise ValueError("StageRequest snapshot group is incomplete")


class ReadinessResolverV1(Protocol):
    def supports(self, candidate: ReadinessCandidate) -> bool: ...

    async def resolve(self, candidate: ReadinessCandidate) -> FrozenReadiness: ...


class StageRequestRendererV1(Protocol):
    def render(
        self, candidate: ReadinessCandidate, frozen: FrozenReadiness
    ) -> RenderedAttempt: ...


class ReadinessRuntimeV1(ReadinessResolverV1, StageRequestRendererV1, Protocol):
    """One closed adapter owns support, readiness, and rendering for a recipe class."""


class FanoutRuntimeV1(Protocol):
    async def reconcile(self, lease: RunLease) -> int: ...


@dataclass(frozen=True, slots=True)
class PairedReadinessRuntime:
    resolver: ReadinessResolverV1
    renderer: StageRequestRendererV1

    def supports(self, candidate: ReadinessCandidate) -> bool:
        return self.resolver.supports(candidate)

    async def resolve(self, candidate: ReadinessCandidate) -> FrozenReadiness:
        return await self.resolver.resolve(candidate)

    def render(
        self, candidate: ReadinessCandidate, frozen: FrozenReadiness
    ) -> RenderedAttempt:
        return self.renderer.render(candidate, frozen)


class CompositeReadinessRuntime:
    """Fail closed unless exactly one code-owned runtime supports a candidate."""

    def __init__(self, adapters: tuple[ReadinessRuntimeV1, ...]) -> None:
        if not adapters:
            raise ValueError("at least one readiness runtime is required")
        self._adapters = adapters

    def _select(self, candidate: ReadinessCandidate) -> ReadinessRuntimeV1:
        supported = tuple(adapter for adapter in self._adapters if adapter.supports(candidate))
        if len(supported) != 1:
            raise ValueError(
                "readiness candidate must match exactly one code-owned runtime adapter"
            )
        return supported[0]

    def supports(self, candidate: ReadinessCandidate) -> bool:
        supported = sum(adapter.supports(candidate) for adapter in self._adapters)
        if supported > 1:
            raise ValueError(
                "readiness candidate matches multiple code-owned runtime adapters"
            )
        return supported == 1

    async def resolve(self, candidate: ReadinessCandidate) -> FrozenReadiness:
        return await self._select(candidate).resolve(candidate)

    def render(
        self, candidate: ReadinessCandidate, frozen: FrozenReadiness
    ) -> RenderedAttempt:
        return self._select(candidate).render(candidate, frozen)


class PipelineReconciler:
    """Coordinates transaction phases while injected boundary work stays outside them."""

    def __init__(
        self,
        repository: PipelineRepository,
        *,
        readiness_runtime: ReadinessRuntimeV1 | None = None,
        readiness_resolver: ReadinessResolverV1 | None = None,
        request_renderer: StageRequestRendererV1 | None = None,
        fanout_runtime: FanoutRuntimeV1 | None = None,
    ) -> None:
        if readiness_runtime is not None and (
            readiness_resolver is not None or request_renderer is not None
        ):
            raise ValueError("inject a readiness runtime or the legacy resolver/renderer pair")
        if (readiness_resolver is None) != (request_renderer is None):
            raise ValueError("readiness resolver and renderer must be injected together")
        self._repository = repository
        self._fanout_runtime = fanout_runtime
        self._readiness_runtime: ReadinessRuntimeV1 | None
        if readiness_runtime is not None:
            self._readiness_runtime = readiness_runtime
        elif readiness_resolver is not None and request_renderer is not None:
            self._readiness_runtime = PairedReadinessRuntime(
                resolver=readiness_resolver,
                renderer=request_renderer,
            )
        else:
            self._readiness_runtime = None

    async def reconcile(self, lease: RunLease) -> None:
        await self._repository.initialize_run(lease)
        if await self._repository.enforce_wall_deadline(lease):
            await self._repository.project_run_result(lease)
            return
        if self._fanout_runtime is not None:
            await self._fanout_runtime.reconcile(lease)
        await self._repository.reconcile_dependencies_and_gates(lease)
        if self._readiness_runtime is not None:
            candidates = await self._repository.readiness_candidates(lease)
            for candidate in candidates:
                if not self._readiness_runtime.supports(candidate):
                    continue
                if candidate.state == "blocked":
                    frozen = await self._readiness_runtime.resolve(candidate)
                    await self._repository.freeze_readiness(
                        lease,
                        stage_run_id=candidate.stage_run_id,
                        frozen=frozen,
                        terminal_snapshot=candidate.terminal_snapshot,
                    )
                else:
                    if not all(
                        value is not None
                        for value in (
                            candidate.resolved_input_bindings_json,
                            candidate.resolved_input_bindings_digest,
                            candidate.resolved_execution_spec_json,
                            candidate.resolved_execution_spec_bytes,
                            candidate.execution_spec_digest,
                        )
                    ):
                        raise ValueError("retry readiness snapshot is incomplete")
                    frozen = FrozenReadiness(
                        input_bindings_json=candidate.resolved_input_bindings_json or [],
                        input_bindings_digest=candidate.resolved_input_bindings_digest or "",
                        execution_spec_json=candidate.resolved_execution_spec_json or {},
                        execution_spec_bytes=candidate.resolved_execution_spec_bytes or b"",
                        execution_spec_digest=candidate.execution_spec_digest or "",
                        resource_profile_json=candidate.resource_profile_json,
                        resource_profile_digest=candidate.resource_profile_digest,
                        image_runtime_contract_json=candidate.image_runtime_contract_json,
                        image_runtime_contract_digest=candidate.image_runtime_contract_digest,
                        provider_connection_ref=None,
                        secret_refs=(),
                    )
                try:
                    rendered = self._readiness_runtime.render(candidate, frozen)
                except (TypeError, ValueError):
                    await self._repository.fail_renderer(
                        lease,
                        stage_run_id=candidate.stage_run_id,
                    )
                    continue
                await self._repository.create_attempt(
                    lease,
                    stage_run_id=candidate.stage_run_id,
                    attempt_id=rendered.attempt_id,
                    stage_request_json=rendered.stage_request_json,
                    stage_request_bytes=rendered.stage_request_bytes,
                    stage_request_digest=rendered.stage_request_digest,
                    reservations=rendered.reservations,
                    provider_budget=rendered.provider_budget,
                    fault_pending=rendered.fault_pending,
                )
        await self._repository.project_run_result(lease)


def reconcile_gate(
    *,
    subject_state: PipelineStageRunState,
    subject_outcome: str | None,
    match_outcomes: list[str],
    other_target_gates: tuple[GateSelection, ...] = (),
) -> GateProjection:
    own = project_outcome_gate(
        subject_state=subject_state,
        domain_outcome=subject_outcome,
        match_outcomes=match_outcomes,
    )
    return GateProjection(own, strict_and_gate_target([own, *other_target_gates]))


def reconcile_retry(
    *,
    attempt_number: int,
    max_attempts: int,
    retry_class: RetryClass,
    reason_code: str,
    terminal_cause: str | None,
    cleanup_acknowledged: bool = True,
    next_budget_fits: bool = True,
) -> RetryDecision:
    return retry_decision(
        completed_attempt_number=attempt_number,
        max_attempts=max_attempts,
        retry_class=retry_class,
        reason_code=reason_code,
        terminal_cause=terminal_cause,
        cleanup_acknowledged=cleanup_acknowledged,
        next_budget_fits=next_budget_fits,
    )


def reconcile_result(
    stages: list[StageTerminalProjection], *, terminal_cause: str | TerminalCause | None
) -> tuple[PipelineRunResult, str | None]:
    cause = TerminalCause(terminal_cause) if terminal_cause is not None else None
    return project_pipeline_result(stages, terminal_cause=cause)
