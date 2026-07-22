"""TB2.1 results plus immutable execution provenance."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom.models.result import FailureReason, TrialResult
from loom_benchmark_terminal_bench_2.upstream import load_tb21_lock

_PHYSICAL_PROFILE = "terminal-bench-2@tb2.1-r6"
_LEGACY_TASK_ID_PREFIX = "terminal-bench-2/"
_PHYSICAL_TASK_ID_PREFIX = f"{_PHYSICAL_PROFILE}/"
_VERIFIER_IDENTITY = "tb21-native-reward-file-v1"

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


@dataclass(frozen=True)
class TB21VerifierOutput:
    """A numeric native reward, or an explicit verifier-evidence failure."""

    reward: float | None
    failure_kind: str | None
    raw: str | None


def parse_tb21_verifier_output(
    root: Path,
    *,
    reward_text: str | None = None,
) -> TB21VerifierOutput:
    """Classify native ``reward.txt`` content without turning bad evidence into 0.

    ``reward_text`` is primarily a pure-test seam. If omitted or ``None``, the
    function reads ``root / 'reward.txt'`` and treats an absent file as a
    platform/verifier failure rather than a benchmark score.
    """
    raw = reward_text
    if raw is None:
        try:
            raw = (root / "reward.txt").read_text(encoding="utf-8")
        except OSError:
            return TB21VerifierOutput(None, "missing_reward", None)
    stripped = raw.strip()
    if not stripped:
        return TB21VerifierOutput(None, "empty_reward", raw)
    try:
        reward = float(stripped)
    except ValueError:
        return TB21VerifierOutput(None, "malformed_reward", raw)
    if not math.isfinite(reward):
        return TB21VerifierOutput(None, "malformed_reward", raw)
    return TB21VerifierOutput(reward, None, raw)


def _strip_prefix(task_id: str) -> str:
    if task_id.startswith(_PHYSICAL_TASK_ID_PREFIX):
        return task_id[len(_PHYSICAL_TASK_ID_PREFIX) :]
    # Historical task reports remain readable; this compatibility is reporting
    # only and never affects new catalog selection or conversion.
    if task_id.startswith(_LEGACY_TASK_ID_PREFIX):
        return task_id[len(_LEGACY_TASK_ID_PREFIX) :]
    return task_id


def _is_resolved(trial: TrialResult) -> bool:
    return bool(trial.reward and trial.reward.get("resolved", 0.0) >= 1.0)


def _parser_results(trial: TrialResult) -> dict[str, str]:
    out: dict[str, str] = {}
    for step in trial.steps:
        verifier_result = step.verifier_result
        if verifier_result is None:
            continue
        for check in verifier_result.checks:
            out[check.name] = "passed" if check.passed else "failed"
    return out


def _failure_mode(trial: TrialResult, resolved: bool) -> str:
    if resolved:
        return "none"
    if trial.failure_reason is None:
        return "unknown"
    return _FAILURE_MODE.get(trial.failure_reason, "unknown")


def _profile_provenance() -> dict[str, Any]:
    lock = load_tb21_lock()
    return {
        "physical_profile": _PHYSICAL_PROFILE,
        "hub_dataset": lock.dataset,
        "hub_revision": lock.revision,
        "hub_metadata_version": lock.hub_metadata_version,
        "source_reference_snapshot": lock.source_revision,
        "source_reference_divergences": lock.source_manifest_divergences,
        "verifier_identity": _VERIFIER_IDENTITY,
    }


def _trial_provenance(trial: TrialResult, *, short_id: str) -> dict[str, Any]:
    lock = load_tb21_lock()
    source_task = f"terminal-bench/{short_id}"
    try:
        package_digest: str | None = lock.digest_for(source_task)
    except ValueError:
        package_digest = None
    divergence = next(
        (entry for entry in lock.source_manifest_divergences if entry["task"] == source_task),
        None,
    )
    return {
        "physical_profile": _PHYSICAL_PROFILE,
        "hub_package_digest": package_digest,
        "hub_metadata_version": lock.hub_metadata_version,
        "source_reference_snapshot": lock.source_revision,
        "source_reference_divergence": divergence,
        "bundle_checksum": trial.task_checksum,
        "verifier_identity": _VERIFIER_IDENTITY,
    }


def _trial_entry(trial: TrialResult) -> dict[str, Any]:
    short_id = _strip_prefix(trial.task_id)
    resolved = _is_resolved(trial)
    entry: dict[str, Any] = {
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
    if trial.task_id.startswith(_PHYSICAL_TASK_ID_PREFIX):
        entry["loom_provenance"] = _trial_provenance(trial, short_id=short_id)
    return entry


def to_tb2_report(
    trials: Iterable[TrialResult],
    *,
    runtime_provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a TB2-compatible report with immutable execution provenance.

    Runtime details are intentionally an independent top-level field: they do
    not change the Harbor package, source-reference snapshot, or Loom bundle
    checksum recorded for a task.
    """
    trial_list = list(trials)
    entries = [_trial_entry(trial) for trial in trial_list]
    resolved_ids = [entry["task_id"] for entry in entries if entry["is_resolved"]]
    unresolved_ids = [entry["task_id"] for entry in entries if not entry["is_resolved"]]
    total = len(entries)
    accuracy = len(resolved_ids) / total if total else 0.0
    report: dict[str, Any] = {
        "results": entries,
        "accuracy": accuracy,
        "n_resolved": len(resolved_ids),
        "n_unresolved": len(unresolved_ids),
        "resolved_ids": resolved_ids,
        "unresolved_ids": unresolved_ids,
        "pass_at_k": {"1": accuracy},
    }
    if any(trial.task_id.startswith(_PHYSICAL_TASK_ID_PREFIX) for trial in trial_list):
        report["tb21_provenance"] = _profile_provenance()
    if runtime_provenance is not None:
        report["runtime_provenance"] = dict(runtime_provenance)
    return report
