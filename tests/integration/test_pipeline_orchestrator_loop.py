from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

from loom.pipeline.keys import canonical_digest, canonical_document
from loom_pipeline_orchestrator.reconciler import (
    CompositeReadinessRuntime,
    PipelineReconciler,
    RenderedAttempt,
)
from loom_pipeline_orchestrator.repository import FrozenReadiness

if TYPE_CHECKING:
    from tests.integration.pipeline_orchestrator_fixtures import OrchestratorSeed


class Resolver:
    def supports(self, _candidate: object) -> bool:
        return True

    async def resolve(self, _candidate: object) -> FrozenReadiness:
        bindings: list[dict[str, object]] = []
        spec = {"schema_version": "test.execution-spec.v1"}
        return FrozenReadiness(
            input_bindings_json=bindings,
            input_bindings_digest=canonical_digest(bindings),
            execution_spec_json=spec,
            execution_spec_bytes=canonical_document(spec),
            execution_spec_digest=canonical_digest(spec),
        )


class Renderer:
    def __init__(self) -> None:
        self.attempt_id = uuid4()

    def render(self, _candidate: object, _frozen: FrozenReadiness) -> RenderedAttempt:
        request = {"schema_version": "test.stage-request.v1"}
        return RenderedAttempt(
            attempt_id=self.attempt_id,
            stage_request_json=request,
            stage_request_bytes=canonical_document(request),
            stage_request_digest=canonical_digest(request),
            reservations=(),
        )


class RequestlessRenderer(Renderer):
    def render(self, _candidate: object, _frozen: FrozenReadiness) -> RenderedAttempt:
        return RenderedAttempt(
            attempt_id=self.attempt_id,
            stage_request_json=None,
            stage_request_bytes=None,
            stage_request_digest=None,
            reservations=(),
        )


class RuntimeAdapter(Resolver, Renderer):
    def __init__(self, *, supported: bool) -> None:
        Renderer.__init__(self)
        self._supported = supported

    def supports(self, _candidate: object) -> bool:
        return self._supported


@pytest.mark.asyncio
async def test_phase_one_and_phase_two_create_exactly_one_attempt(
    orchestrator_seed: OrchestratorSeed,
) -> None:
    seed = orchestrator_seed
    renderer = Renderer()
    reconciler = PipelineReconciler(
        seed.repository,
        readiness_resolver=Resolver(),
        request_renderer=renderer,
    )
    lease = (await seed.repository.claim_runs(controller_id="controller-a"))[0]
    await reconciler.reconcile(lease)
    await reconciler.reconcile(lease)

    async with seed.sessions() as session:
        row = (
            await session.execute(
                text("""
                    SELECT s.state, s.attempt_count, s.execution_spec_digest,
                           count(a.id) AS attempts,
                           (SELECT attempts_created FROM pipeline_budget_ledgers
                             WHERE pipeline_run_id=:run_id) AS attempts_created
                      FROM pipeline_stage_runs s
                      LEFT JOIN execution_attempts a ON a.stage_run_id=s.id
                     WHERE s.pipeline_run_id=:run_id
                     GROUP BY s.id
                """),
                {"run_id": seed.run_id},
            )
        ).mappings().one()
    assert row["state"] == "queued"
    assert row["attempt_count"] == 1
    assert row["execution_spec_digest"] is not None
    assert row["attempts"] == 1
    assert row["attempts_created"] == 1
    await seed.repository.release(lease)


@pytest.mark.asyncio
async def test_requestless_ordinary_attempt_persists_sql_null_group(
    orchestrator_seed: OrchestratorSeed,
) -> None:
    seed = orchestrator_seed
    renderer = RequestlessRenderer()
    reconciler = PipelineReconciler(
        seed.repository,
        readiness_resolver=Resolver(),
        request_renderer=renderer,
    )
    lease = (await seed.repository.claim_runs(controller_id="controller-a"))[0]
    await reconciler.reconcile(lease)

    async with seed.sessions() as session:
        row = (
            await session.execute(
                text("""
                    SELECT stage_request_json, stage_request_bytes, stage_request_digest
                      FROM execution_attempts
                     WHERE id=:attempt_id
                """),
                {"attempt_id": renderer.attempt_id},
            )
        ).mappings().one()
    assert row == {
        "stage_request_json": None,
        "stage_request_bytes": None,
        "stage_request_digest": None,
    }
    await seed.repository.release(lease)


def test_rendered_attempt_rejects_partial_request_group() -> None:
    with pytest.raises(ValueError, match="snapshot group is incomplete"):
        RenderedAttempt(
            attempt_id=uuid4(),
            stage_request_json=None,
            stage_request_bytes=b"{}\n",
            stage_request_digest=None,
            reservations=(),
        )


def test_composite_runtime_requires_exactly_one_adapter() -> None:
    candidate = object()
    none = CompositeReadinessRuntime((RuntimeAdapter(supported=False),))
    assert not none.supports(candidate)  # type: ignore[arg-type]

    ambiguous = CompositeReadinessRuntime(
        (RuntimeAdapter(supported=True), RuntimeAdapter(supported=True))
    )
    with pytest.raises(ValueError, match="multiple code-owned"):
        ambiguous.supports(candidate)  # type: ignore[arg-type]
