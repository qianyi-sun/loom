from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from agentic_data_platform.domain.run_records import EvaluatorResult
from agentic_data_platform.evaluation.types import EvaluatorAdapter, EvaluatorInput


@dataclass(frozen=True)
class MockEvaluatorAdapter(EvaluatorAdapter):
    evaluator_id: str = "mock-judge-v0"
    score: float = 0.75
    verbal_feedback: str = "Mock evaluator feedback: trajectory and workspace artifacts were reviewed."
    failure_reason: str | None = None

    def evaluate(self, evaluator_input: EvaluatorInput) -> EvaluatorResult:
        artifact_refs = [_artifact_pointer(ref) for ref in evaluator_input.all_artifacts()]

        if self.failure_reason:
            return EvaluatorResult(
                evaluator_id=self.evaluator_id,
                status="failed",
                score=None,
                metrics={
                    "task_success": False,
                    "artifact_count": len(artifact_refs),
                    "trajectory_available": _availability(evaluator_input.trajectory_ref),
                    "workspace_available": _availability(evaluator_input.workspace_ref),
                },
                verbal_feedback="",
                judge=evaluator_input.judge,
                artifact_refs=artifact_refs,
                failure_reason=self.failure_reason,
                metadata=_metadata(evaluator_input),
            )

        return EvaluatorResult(
            evaluator_id=self.evaluator_id,
            status="completed",
            score=self.score,
            metrics={
                "task_success": self.score >= 0.5,
                "artifact_count": len(artifact_refs),
                "trajectory_available": _availability(evaluator_input.trajectory_ref),
                "workspace_available": _availability(evaluator_input.workspace_ref),
            },
            verbal_feedback=self.verbal_feedback,
            judge=evaluator_input.judge,
            artifact_refs=artifact_refs,
            metadata=_metadata(evaluator_input),
        )


def _artifact_pointer(ref: Any) -> str:
    return ref.uri or ref.artifact_id


def _availability(ref: Any) -> float:
    return 1.0 if ref is not None else 0.0


def _metadata(evaluator_input: EvaluatorInput) -> dict[str, Any]:
    return {
        "run_id": evaluator_input.run_id,
        "project_id": evaluator_input.project_id,
        "owner_team": evaluator_input.owner_team,
        "rubric_id": evaluator_input.rubric_id,
        "task_instance_id": evaluator_input.task.instance_id,
        **evaluator_input.metadata,
    }
