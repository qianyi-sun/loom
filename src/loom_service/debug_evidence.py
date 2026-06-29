"""User-facing debug evidence projection for batches and trials.

The evidence contract is intentionally derived from already persisted rows.
It gives users and AI agents enough scoped context to diagnose common
benchmark/provider/sandbox/verifier failures without exposing DB internals,
signed object-store URLs, provider secrets, or private cross-team data.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from uuid import UUID

from fastapi import Request

from loom.db.schema import Batch, LlmCall, Task, Trial
from loom.request_params import coerce_request_params
from loom.security.redaction import redact_mapping
from loom_service.usage_accounting import (
    llm_call_counts_by_trial_id,
    project_trial_llm_evidence,
    summarize_llm_evidence_for_trials,
)

_ACTIVE_STATES = {"queued", "claimed", "running", "submitted"}

_TRIAL_FAILURE_META: dict[str, tuple[str, str, list[str]]] = {
    "agent_error": (
        "agent",
        "model",
        ["Inspect the trajectory events around the agent step."],
    ),
    "agent_timeout": (
        "agent",
        "model",
        ["Inspect the trajectory and consider a longer agent timeout."],
    ),
    "env_start_failure": (
        "sandbox",
        "platform",
        ["Check the task image, sandbox backend, and worker capacity."],
    ),
    "env_healthcheck_failed": (
        "sandbox",
        "platform",
        ["Check the task image healthcheck and sandbox startup logs."],
    ),
    "verifier_error": (
        "verifier",
        "benchmark",
        ["Inspect verifier output and benchmark task assets."],
    ),
    "verifier_timeout": (
        "verifier",
        "benchmark",
        ["Inspect verifier output and consider verifier timeout settings."],
    ),
    "artifact_upload_failed": (
        "artifact",
        "platform",
        ["Check artifact upload references and object-store availability."],
    ),
    "trajectory_flush_failed": (
        "artifact",
        "platform",
        ["Check trajectory upload references and object-store availability."],
    ),
    "exhausted_retries": (
        "retry",
        "platform",
        ["Inspect previous attempts and retryable provider or sandbox errors."],
    ),
    "retry_exhausted": (
        "retry",
        "platform",
        ["Inspect previous attempts and retryable provider or sandbox errors."],
    ),
    "worker_lost_claim": (
        "worker",
        "platform",
        ["Retry the trial or contact the operator if workers keep dropping."],
    ),
    "internal_error": (
        "internal",
        "platform",
        ["Contact the operator with the trial id and reason code."],
    ),
    "provider_error": (
        "provider",
        "provider",
        ["Run provider model preflight and check provider entitlement."],
    ),
    "gateway_error": (
        "gateway",
        "provider",
        ["Retry the run and run provider preflight if the error repeats."],
    ),
}


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    return str(value)


def _extract_agent(config: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {"name": None, "model": None}
    if "agent_name" in config or "agent_model" in config:
        return {
            "name": config.get("agent_name"),
            "model": config.get("agent_model"),
        }
    agent = config.get("agent")
    if isinstance(agent, dict):
        return {
            "name": agent.get("name"),
            "model": agent.get("model"),
        }
    return {"name": None, "model": None}


def _aggregate_reward(result: dict[str, Any] | None) -> float | None:
    if not isinstance(result, dict):
        return None
    raw = result.get("aggregate_reward")
    if raw is None:
        raw = result.get("reward")
    if isinstance(raw, dict):
        values = [v for v in raw.values() if isinstance(v, int | float)]
        if not values:
            return None
        return float(sum(values) / len(values))
    if isinstance(raw, int | float | str | Decimal):
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
    return None


def _reward_components(result: dict[str, Any] | None) -> dict[str, float]:
    if not isinstance(result, dict):
        return {}
    raw = result.get("reward")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float] = {}
    for key, value in raw.items():
        if isinstance(key, str) and isinstance(value, int | float):
            out[key] = float(value)
    return out


def _verifier_steps(result: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(result, dict):
        return []
    raw_steps = result.get("steps")
    if not isinstance(raw_steps, list):
        return []
    steps: list[dict[str, Any]] = []
    for raw in raw_steps:
        if not isinstance(raw, dict):
            continue
        verifier = raw.get("verifier_result")
        verifier_d = verifier if isinstance(verifier, dict) else {}
        error = verifier_d.get("error")
        step_error = raw.get("error")
        steps.append(
            {
                "step_name": raw.get("step_name"),
                "rewards": (
                    verifier_d.get("rewards") if isinstance(verifier_d.get("rewards"), dict) else {}
                ),
                "verifier_error": error if isinstance(error, dict) else None,
                "step_error": step_error if isinstance(step_error, dict) else None,
            }
        )
    return steps


def _failure_for_trial(trial: Trial) -> dict[str, Any]:
    reason = trial.failure_reason
    if reason:
        category, attribution, _actions = _TRIAL_FAILURE_META.get(
            reason,
            ("unknown", "unknown", ["Inspect trajectory and artifacts."]),
        )
        return {
            "reason_code": f"trial.{reason}",
            "reason": reason,
            "category": category,
            "attribution": attribution,
            "message": trial.failure_message,
        }
    if trial.state == "succeeded":
        return {
            "reason_code": "trial.succeeded",
            "reason": None,
            "category": "none",
            "attribution": "none",
            "message": None,
        }
    if trial.state == "cancelled":
        return {
            "reason_code": "trial.cancelled",
            "reason": "cancelled",
            "category": "cancelled",
            "attribution": "user_or_platform",
            "message": trial.failure_message,
        }
    if trial.state in _ACTIVE_STATES:
        return {
            "reason_code": f"trial.{trial.state}",
            "reason": None,
            "category": "active",
            "attribution": "pending",
            "message": None,
        }
    return {
        "reason_code": "trial.failed_unknown",
        "reason": reason,
        "category": "unknown",
        "attribution": "unknown",
        "message": trial.failure_message,
    }


def _next_actions_for_trial(trial: Trial) -> list[str]:
    if trial.failure_reason:
        return _TRIAL_FAILURE_META.get(
            trial.failure_reason,
            ("unknown", "unknown", ["Inspect trajectory and artifacts."]),
        )[2]
    if trial.state in _ACTIVE_STATES:
        return ["Wait for the worker to finish or inspect live trajectory events."]
    if trial.state == "succeeded":
        return ["Use the reward and artifacts to judge model/task quality."]
    if trial.state == "cancelled":
        return ["Clone or rerun the same config if the cancellation was accidental."]
    return ["Inspect trajectory, ATIF, and artifacts for the terminal outcome."]


def _artifact_refs(
    request: Request,
    *,
    trial_id: UUID,
    trajectory_index: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not isinstance(trajectory_index, dict):
        return []
    raw = trajectory_index.get("artifacts")
    if not isinstance(raw, list):
        return []
    refs: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if not isinstance(key, str) or not key:
            continue
        size_raw = item.get("size")
        try:
            size = int(size_raw) if size_raw is not None else 0
        except (TypeError, ValueError):
            size = 0
        refs.append(
            {
                "kind": "artifact",
                "step_name": item.get("step_name"),
                "key": key,
                "size": max(size, 0),
                "share_status": item.get("share_status") or "pending_scan",
                "blocked_reason": item.get("blocked_reason"),
                "download_url": str(
                    request.url_for(
                        "download_artifact",
                        trial_id=str(trial_id),
                    ).include_query_params(key=key),
                ),
            }
        )
    return refs


def _provider_summary(llm_calls: Sequence[LlmCall]) -> dict[str, Any]:
    models = sorted({call.model for call in llm_calls if call.model})
    dialects = sorted({call.dialect for call in llm_calls if call.dialect})
    latest = max((call.captured_at for call in llm_calls), default=None)
    max_attempt = max((int(call.attempt or 1) for call in llm_calls), default=0)
    total_cost = sum((call.cost_usd for call in llm_calls), Decimal("0"))
    request_params = [_request_params_summary(call) for call in llm_calls]
    request_param_status_counts = Counter(str(item["status"]) for item in request_params)
    call_status_counts = Counter(_call_status(call) for call in llm_calls)
    failure_category_counts = Counter(
        str((call.provider_extras or {}).get("_loom_failure_category"))
        for call in llm_calls
        if _call_status(call) == "failed"
        and (call.provider_extras or {}).get("_loom_failure_category")
    )
    return {
        "llm_calls_count": len(llm_calls),
        "call_status_counts": dict(call_status_counts),
        "failed_llm_calls_count": int(call_status_counts.get("failed", 0)),
        "failure_category_counts": dict(failure_category_counts),
        "total_prompt_tokens": sum(int(call.input_tokens or 0) for call in llm_calls),
        "total_completion_tokens": sum(int(call.output_tokens or 0) for call in llm_calls),
        "models": models,
        "dialects": dialects,
        "max_attempt": max_attempt,
        "latest_call_at": _iso(latest),
        "total_cost_usd": str(total_cost),
        "request_params_status_counts": dict(request_param_status_counts),
        "request_params": request_params,
    }


def _call_status(call: LlmCall) -> str:
    provider_extras = call.provider_extras or {}
    status = provider_extras.get("_loom_call_status")
    if status == "failed":
        return "failed"
    return "succeeded"


def _request_params_summary(call: LlmCall) -> dict[str, Any]:
    request_params = coerce_request_params(call.request_params)
    return {
        "llm_call_id": str(call.id),
        "step_id": call.step_id,
        "dialect": call.dialect,
        "model": call.model,
        "attempt": int(call.attempt or 1),
        "status": request_params["status"],
        "parameters": request_params["parameters"],
    }


def _task_materialization_state(task: Task | None) -> str:
    if task is None:
        return "missing"
    if isinstance(task.config, dict) and task.config:
        return "ready"
    return "blocked"


def build_trial_debug_evidence(
    request: Request,
    trial: Trial,
    *,
    task: Task | None,
    llm_calls: Sequence[LlmCall],
) -> dict[str, Any]:
    trajectory_index = trial.trajectory_index or {}
    is_terminal = trial.state in {"succeeded", "failed", "cancelled"}
    atif_ready = bool(trajectory_index.get("atif_uri")) or (
        is_terminal and trial.finished_at is not None
    )
    trajectory_ready = bool(trajectory_index.get("trajectory_uri")) or (
        trial.started_at is not None
    )
    result = trial.result if isinstance(trial.result, dict) else None
    provider_summary = {
        **_provider_summary(llm_calls),
        **project_trial_llm_evidence(
            trial,
            llm_calls_count=len(llm_calls),
        ),
        "provider_connection_id": (
            str(trial.provider_connection_id) if trial.provider_connection_id else None
        ),
        "provider_model_id": trial.provider_model_id,
    }
    failure = _failure_for_trial(trial)
    if provider_summary["llm_evidence_status"] == "no_calls_invalid":
        failure = {
            "reason_code": "trial.no_llm_calls",
            "reason": "no_llm_calls",
            "category": "gateway",
            "attribution": "platform",
            "message": ("Terminal model-backed trial did not record any LLM calls."),
        }
    evidence = {
        "schema_version": "1",
        "generated_at": datetime.now(UTC).isoformat(),
        "entity": {
            "type": "trial",
            "id": str(trial.id),
            "team_id": str(trial.team_id),
            "batch_id": str(trial.batch_id) if trial.batch_id else None,
        },
        "lifecycle": {
            "state": trial.state,
            "submitted_at": _iso(trial.submitted_at),
            "claimed_at": _iso(trial.claimed_at),
            "started_at": _iso(trial.started_at),
            "finished_at": _iso(trial.finished_at),
            "cancellation_requested_at": _iso(trial.cancellation_requested_at),
            "cancellation_observed_at": _iso(trial.cancellation_observed_at),
            "attempt_count": trial.attempt_count,
            "next_attempt_at": _iso(trial.next_attempt_at),
            "retry_state": ("scheduled" if trial.next_attempt_at is not None else "none"),
        },
        "worker": {
            "worker_id": str(trial.worker_id) if trial.worker_id else None,
            "requires_caps": trial.requires_caps,
        },
        "agent": _extract_agent(trial.config),
        "provider": provider_summary,
        "failure": failure,
        "task": {
            "task_id": trial.task_id,
            "benchmark_id": task.benchmark_id if task else None,
            "task_checksum": task.checksum if task else None,
            "source": task.source if task else None,
            "materialization_state": _task_materialization_state(task),
        },
        "reward": {
            "aggregate_reward": _aggregate_reward(result),
            "components": _reward_components(result),
            "verifier_steps": _verifier_steps(result),
        },
        "evidence_refs": {
            "atif": {
                "ready": atif_ready,
                "download_url": str(
                    request.url_for("download_atif", trial_id=str(trial.id)),
                ),
            },
            "trajectory": {
                "ready": trajectory_ready,
                "download_url": str(
                    request.url_for(
                        "download_trajectory",
                        trial_id=str(trial.id),
                    ),
                ),
            },
            "artifacts": _artifact_refs(
                request,
                trial_id=trial.id,
                trajectory_index=trajectory_index,
            ),
        },
        "next_actions": (
            [
                "Check subprocess gateway URL, provider route, and worker env.",
                "Rerun after the model path records LLM calls.",
            ]
            if provider_summary["llm_evidence_status"] == "no_calls_invalid"
            else _next_actions_for_trial(trial)
        ),
    }
    return cast(dict[str, Any], redact_mapping(evidence))


def _summary_from_trials(trials: Sequence[Trial]) -> dict[str, int]:
    counts = Counter(str(trial.state) for trial in trials)
    return {
        "queued": counts.get("queued", 0),
        "claimed": counts.get("claimed", 0),
        "running": counts.get("running", 0),
        "succeeded": counts.get("succeeded", 0),
        "failed": counts.get("failed", 0),
        "cancelled": counts.get("cancelled", 0),
    }


def _failure_for_batch(batch: Batch) -> dict[str, Any]:
    if batch.failure_reason == "fanout_submit_failed":
        return {
            "reason_code": "batch.fanout_submit_failed",
            "reason": "fanout_submit_failed",
            "category": "submit",
            "attribution": "platform",
            "message": batch.failure_message,
        }
    if batch.result_status == "all_failed":
        return {
            "reason_code": "batch.all_failed",
            "reason": "all_failed",
            "category": "aggregate",
            "attribution": "mixed",
            "message": "All child trials failed.",
        }
    if batch.result_status == "partial_failed":
        return {
            "reason_code": "batch.partial_failed",
            "reason": "partial_failed",
            "category": "aggregate",
            "attribution": "mixed",
            "message": "Some child trials failed.",
        }
    if batch.state in _ACTIVE_STATES:
        return {
            "reason_code": f"batch.{batch.state}",
            "reason": None,
            "category": "active",
            "attribution": "pending",
            "message": None,
        }
    if batch.result_status == "cancelled":
        return {
            "reason_code": "batch.cancelled",
            "reason": "cancelled",
            "category": "cancelled",
            "attribution": "user_or_platform",
            "message": None,
        }
    return {
        "reason_code": "batch.succeeded",
        "reason": None,
        "category": "none",
        "attribution": "none",
        "message": None,
    }


def _next_actions_for_batch(batch: Batch) -> list[str]:
    if batch.failure_reason == "fanout_submit_failed":
        return [
            "Inspect batch fan-out errors.",
            "Update task filter, provider, backend, or team policy before rerun.",
        ]
    if batch.result_status in {"all_failed", "partial_failed"}:
        return [
            "Open failed child trials and inspect their debug evidence.",
            "Rerun transient gateway failures when available.",
        ]
    if batch.state in _ACTIVE_STATES:
        return ["Wait for child trials to finish or inspect Monitor progress."]
    if batch.result_status == "cancelled":
        return ["Clone or recreate the batch if cancellation was accidental."]
    return ["Use reward and per-benchmark rollups to interpret model quality."]


def build_batch_debug_evidence(
    batch: Batch,
    *,
    trials: Sequence[Trial],
    llm_calls: Sequence[LlmCall],
) -> dict[str, Any]:
    llm_call_counts = llm_call_counts_by_trial_id(llm_calls)
    llm_evidence = summarize_llm_evidence_for_trials(
        trials,
        llm_call_counts=llm_call_counts,
    )
    failed_trials: list[dict[str, Any]] = [
        {
            "id": str(trial.id),
            "task_id": trial.task_id,
            "state": trial.state,
            "reason_code": (
                f"trial.{trial.failure_reason}" if trial.failure_reason else "trial.failed_unknown"
            ),
            "failure_reason": trial.failure_reason,
            "failure_message": trial.failure_message,
        }
        for trial in trials
        if trial.state == "failed" or trial.failure_reason
    ]
    no_call_trials: list[dict[str, Any]] = [
        {
            "id": str(trial.id),
            "task_id": trial.task_id,
            "state": trial.state,
            "reason_code": "trial.no_llm_calls",
            "failure_reason": "no_llm_calls",
            "failure_message": ("Terminal model-backed trial did not record any LLM calls."),
        }
        for trial in trials
        if project_trial_llm_evidence(
            trial,
            llm_calls_count=llm_call_counts.get(trial.id, 0),
        )["no_call"]
    ]
    if no_call_trials:
        seen_failed_ids = {trial["id"] for trial in failed_trials}
        failed_trials.extend(
            trial for trial in no_call_trials if trial["id"] not in seen_failed_ids
        )
    rewards = [
        reward for trial in trials if (reward := _aggregate_reward(trial.result)) is not None
    ]
    evidence = {
        "schema_version": "1",
        "generated_at": datetime.now(UTC).isoformat(),
        "entity": {
            "type": "batch",
            "id": str(batch.id),
            "team_id": str(batch.team_id),
        },
        "lifecycle": {
            "state": batch.state,
            "terminal_status": batch.result_status,
            "created_at": _iso(batch.created_at),
            "finished_at": _iso(batch.finished_at),
        },
        "worker": {"backend": batch.backend},
        "agent": {
            "trial_config": batch.trial_config,
            "combinations": batch.combinations,
        },
        "provider": {
            **_provider_summary(llm_calls),
            **llm_evidence,
            "provider_connection_id": (
                str(batch.provider_connection_id) if batch.provider_connection_id else None
            ),
            "provider_model_id": batch.provider_model_id,
        },
        "failure": _failure_for_batch(batch),
        "task_selection": {
            "task_filter": batch.task_filter,
            "expected_trial_count": batch.expected_trial_count,
            "n_per_task": batch.n_per_task,
            "fanout_errors": batch.fanout_errors,
        },
        "trials": {
            "summary": _summary_from_trials(trials),
            "failed": failed_trials[:50],
            "failed_count": len(failed_trials),
        },
        "reward": {
            "aggregate_reward": (sum(rewards) / len(rewards) if rewards else None),
            "scored_trial_count": len(rewards),
        },
        "next_actions": _next_actions_for_batch(batch),
    }
    return cast(dict[str, Any], redact_mapping(evidence))
