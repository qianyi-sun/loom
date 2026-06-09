"""to_tb2_report — translate Loom TrialResult batches into the
canonical Terminal-Bench-2.0 BenchmarkResults JSON shape.

The shape is verified against
https://github.com/laude-institute/terminal-bench/blob/91e1045.../terminal_bench/harness_models.py
(upstream SHA pinned in upstream.py).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from loom.models.result import FailureReason, TrialResult

_TASK_ID_PREFIX = "terminal-bench-2/"

# Loom FailureReason → TB-2 FailureMode string. Values not in this
# mapping fall through to "unknown" so the shape stays stable.
_FAILURE_MODE: dict[FailureReason, str] = {
    FailureReason.AGENT_TIMEOUT: "agent_timeout",
    FailureReason.VERIFIER_TIMEOUT: "test_timeout",
    FailureReason.VERIFIER_ERROR: "parser_error",
    FailureReason.AGENT_ERROR: "unknown",
    FailureReason.ENV_START_FAILURE: "unknown",
    FailureReason.ENV_HEALTHCHECK_FAILED: "unknown",
    FailureReason.TRAJECTORY_FLUSH_FAILED: "unknown",
    FailureReason.EXHAUSTED_RETRIES: "unknown",
    FailureReason.WORKER_LOST_CLAIM: "unknown",
    FailureReason.INTERNAL_ERROR: "unknown",
}


def _strip_prefix(task_id: str) -> str:
    if task_id.startswith(_TASK_ID_PREFIX):
        return task_id[len(_TASK_ID_PREFIX):]
    return task_id


def _is_resolved(trial: TrialResult) -> bool:
    if trial.reward and trial.reward.get("resolved", 0.0) >= 1.0:
        return True
    return False


def _parser_results(trial: TrialResult) -> dict[str, str]:
    """Flatten check names → passed/failed across every step's verifier
    result. Names colliding across steps overwrite (last-write-wins);
    TB-2's shape is a flat dict."""
    out: dict[str, str] = {}
    for step in trial.steps:
        vr = step.verifier_result
        if vr is None:
            continue
        for check in vr.checks:
            out[check.name] = "passed" if check.passed else "failed"
    return out


def _failure_mode(trial: TrialResult, resolved: bool) -> str:
    if resolved:
        return "none"
    if trial.failure_reason is None:
        return "unknown"
    return _FAILURE_MODE.get(trial.failure_reason, "unknown")


def _trial_entry(trial: TrialResult) -> dict[str, Any]:
    short_id = _strip_prefix(trial.task_id)
    resolved = _is_resolved(trial)
    return {
        "trial_name": f"{short_id}.1",
        "task_id": short_id,
        "task_description": "",
        "is_resolved": resolved,
        "failure_mode": _failure_mode(trial, resolved),
        "parser_results": _parser_results(trial),
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "uuid": str(trial.id),
        "recording_path": None,
    }


def to_tb2_report(trials: Iterable[TrialResult]) -> dict[str, Any]:
    """Build a TB-2 BenchmarkResults JSON-serializable dict from Loom
    TrialResult batches. Compatible with `json.dump(report, fp)`."""
    entries = [_trial_entry(t) for t in trials]
    resolved_ids = [e["task_id"] for e in entries if e["is_resolved"]]
    unresolved_ids = [e["task_id"] for e in entries if not e["is_resolved"]]
    total = len(entries)
    accuracy = (len(resolved_ids) / total) if total else 0.0
    return {
        "results": entries,
        "accuracy": accuracy,
        "n_resolved": len(resolved_ids),
        "n_unresolved": len(unresolved_ids),
        "resolved_ids": resolved_ids,
        "unresolved_ids": unresolved_ids,
        "pass_at_k": {"1": accuracy},
    }
