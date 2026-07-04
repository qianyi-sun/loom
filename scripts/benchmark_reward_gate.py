#!/usr/bin/env python3
"""Staging benchmark reward acceptance gate.

This script is intentionally API-level. It checks the same benchmark/trial
surface a user sees, without reading database credentials or object-store
secrets from operator machines.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from loom.benchmark_readiness import V1_SUPPORTED_BENCHMARK_IDS
except ModuleNotFoundError:  # pragma: no cover - direct script fallback.
    V1_SUPPORTED_BENCHMARK_IDS = frozenset(
        {
            "aime-24",
            "aime-25",
            "humaneval",
            "livecodebench",
            "mbpp",
            "mmlu-pro",
            "math-500",
            "gpqa",
            "skillflow",
            "skilllearnbench",
            "swe-bench-verified",
            "terminal-bench-2",
        }
    )

TERMINAL_BATCH_STATES = frozenset({
    "finished",
    "succeeded",
    "failed",
    "cancelled",
})
TERMINAL_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})
BENCHMARK_SIDE_FAILURE_REASONS = frozenset({
    "env_start_failure",
    "env_healthcheck_failed",
    "verifier_error",
    "verifier_timeout",
    "internal_error",
})
EXPLICIT_SCOPE_BLOCKER_REASONS = frozenset({
    "unsupported_runtime",
    "deferred_support",
    "not_v1_supported",
})


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    status: str
    detail: str
    remediation: str


class JsonClient(Protocol):
    def get_json(self, path: str) -> dict[str, Any]:
        ...

    def post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        ...


class ApiError(RuntimeError):
    """Raised when the public API cannot return the JSON payload we need."""


class ApiClient:
    def __init__(self, server_url: str, token: str, *, timeout: float = 120.0) -> None:
        self.server_url = server_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def get_json(self, path: str) -> dict[str, Any]:
        req = Request(
            self.server_url + path,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.token}",
            },
        )
        try:
            with urlopen(req, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = _http_error_detail(exc)
            raise ApiError(f"HTTP {exc.code} {exc.reason}: {detail}") from exc
        except TimeoutError as exc:
            raise ApiError(f"network timeout: {exc}") from exc
        except URLError as exc:
            raise ApiError(f"network error: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise ApiError(f"invalid JSON response: {exc}") from exc

    def post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        req = Request(
            self.server_url + path,
            data=body,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urlopen(req, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = _http_error_detail(exc)
            raise ApiError(f"HTTP {exc.code} {exc.reason}: {detail}") from exc
        except TimeoutError as exc:
            raise ApiError(f"network timeout: {exc}") from exc
        except URLError as exc:
            raise ApiError(f"network error: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise ApiError(f"invalid JSON response: {exc}") from exc


def _http_error_detail(exc: HTTPError) -> str:
    raw = exc.read().decode("utf-8", errors="replace").strip()
    if not raw:
        return "empty response body"
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return raw[:500]
    if isinstance(payload, dict):
        detail = payload.get("detail") or payload.get("message") or payload.get("error")
        if isinstance(detail, str):
            return detail[:500]
    return raw[:500]


def _main_error(message: str) -> int:
    print(f"api request failed: {message}", file=sys.stderr)
    return 2


def _is_numeric_reward(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def check_benchmark_readiness(items: list[dict[str, Any]]) -> list[CheckResult]:
    if not items:
        return [
            CheckResult(
                check_id="benchmarks.all_displayed_runnable",
                status="fail",
                detail="benchmark API returned no displayed benchmarks",
                remediation="Provision the supported benchmark catalog before staging validation.",
            )
        ]

    blocked: list[str] = []
    runnable = 0
    unsupported = 0
    for item in items:
        if item.get("blocker_reason") in EXPLICIT_SCOPE_BLOCKER_REASONS:
            unsupported += 1
            continue
        task_count = item.get("task_count")
        is_runnable = (
            item.get("readiness_state") == "runnable"
            and isinstance(task_count, int)
            and task_count > 0
        )
        if is_runnable:
            runnable += 1
            continue
        label = item.get("readiness_label") or item.get("readiness_state") or "blocked"
        blocked.append(f"{item.get('id', '<unknown>')} ({label})")

    if blocked:
        detail = "non-runnable displayed benchmarks: " + ", ".join(blocked[:20])
        if len(blocked) > 20:
            detail += f", ... +{len(blocked) - 20} more"
        return [
            CheckResult(
                check_id="benchmarks.all_displayed_runnable",
                status="fail",
                detail=detail,
                remediation=(
                    "Publish/register valid task configs for these benchmarks, "
                    "or hide them from the displayed supported catalog."
                ),
            )
        ]

    if runnable == 0:
        return [
            CheckResult(
                check_id="benchmarks.all_displayed_runnable",
                status="fail",
                detail=(
                    "benchmark API returned no currently supported runnable "
                    "benchmarks"
                ),
                remediation="Provision at least one supported runnable benchmark.",
            )
        ]

    detail = f"{runnable} runnable benchmarks displayed"
    if unsupported:
        detail += f"; {unsupported} unsupported benchmarks skipped"
    return [
        CheckResult(
            check_id="benchmarks.all_displayed_runnable",
            status="pass",
            detail=detail,
            remediation="",
        )
    ]


def check_batch_rewards(
    *,
    batch: dict[str, Any],
    trials: list[dict[str, Any]],
) -> list[CheckResult]:
    failures: list[str] = []
    batch_state = str(batch.get("state") or "")
    if batch_state not in TERMINAL_BATCH_STATES:
        failures.append(f"batch state is not terminal: {batch_state or '<missing>'}")

    expected = batch.get("expected_trial_count")
    if isinstance(expected, int) and len(trials) != expected:
        failures.append(f"trial count {len(trials)} != expected {expected}")

    for trial in trials:
        trial_id = str(trial.get("id") or "<unknown>")
        state = str(trial.get("state") or "")
        reason = trial.get("failure_reason")
        reward = trial.get("aggregate_reward")
        if state not in TERMINAL_TRIAL_STATES:
            failures.append(f"{trial_id} is not terminal: {state or '<missing>'}")
            continue
        if reason in BENCHMARK_SIDE_FAILURE_REASONS:
            failures.append(f"{trial_id} has benchmark-side failure reason {reason}")
            continue
        if state != "succeeded":
            failures.append(f"{trial_id} ended {state} with failure_reason={reason}")
            continue
        if not _is_numeric_reward(reward):
            failures.append(f"{trial_id} missing numeric reward")

    if failures:
        detail = "; ".join(failures[:25])
        if len(failures) > 25:
            detail += f"; ... +{len(failures) - 25} more"
        return [
            CheckResult(
                check_id="benchmarks.batch_rewards_complete",
                status="fail",
                detail=detail,
                remediation=(
                    "Fix benchmark task bundles, materialization, task images, "
                    "or verifiers until every acceptance trial records a numeric reward."
                ),
            )
        ]
    return [
        CheckResult(
            check_id="benchmarks.batch_rewards_complete",
            status="pass",
            detail=f"{len(trials)} trials have numeric rewards",
            remediation="",
        )
    ]


def _reward_key(reward: float) -> str:
    if reward.is_integer():
        return f"{reward:.1f}"
    return format(reward, ".12g")


def _safe_str(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _trial_base(trial: dict[str, Any]) -> dict[str, Any]:
    return {
        "trial_id": str(trial.get("id") or "<unknown>"),
        "task_id": str(trial.get("task_id") or "<unknown>"),
        "state": str(trial.get("state") or "<missing>"),
        "failure_reason": _safe_str(trial.get("failure_reason")),
        "llm_evidence_status": _safe_str(trial.get("llm_evidence_status")),
    }


def _extract_baseline(batch: dict[str, Any]) -> dict[str, Any]:
    trial_config = batch.get("trial_config")
    if not isinstance(trial_config, dict):
        trial_config = {}
    agent_model = trial_config.get("agent_model")
    if not isinstance(agent_model, dict):
        agent_model = None
    task_filter = batch.get("task_filter")
    if not isinstance(task_filter, dict):
        task_filter = None
    return {
        "batch_id": str(batch.get("id") or "<unknown>"),
        "batch_name": _safe_str(batch.get("name")),
        "batch_state": _safe_str(batch.get("state")),
        "expected_trial_count": batch.get("expected_trial_count")
        if isinstance(batch.get("expected_trial_count"), int)
        else None,
        "agent_name": _safe_str(trial_config.get("agent_name")),
        "agent_model": agent_model,
        "provider_connection_id": _safe_str(batch.get("provider_connection_id")),
        "provider_model_id": _safe_str(batch.get("provider_model_id")),
        "task_filter": task_filter,
    }


def _canary_summary(
    *,
    hard_failures: list[str],
    scored_count: int,
    positive_count: int,
) -> str:
    if hard_failures:
        return "; ".join(hard_failures)
    if scored_count == 0:
        return "blocked: no scored trials with numeric aggregate_reward"
    if positive_count == 0:
        return "blocked: no positive reward among scored trials"
    return f"passed: {positive_count}/{scored_count} scored trials have reward > 0"


def build_score_positive_canary_report(
    *,
    batch: dict[str, Any],
    trials: list[dict[str, Any]],
    override_issue: str | None = None,
    override_rationale: str | None = None,
) -> dict[str, Any]:
    """Build operator evidence for the pre-production score-positive canary.

    The gate is intentionally narrower than the full reward sweep: it accepts a
    representative canary batch only when at least one scored trial has reward
    above zero. Platform success with all-zero scores stays blocked because it
    does not prove the production runner/provider mix can solve any task.
    """
    reward_distribution: dict[str, int] = {}
    failure_taxonomy: dict[str, int] = {}
    scored_trials: list[dict[str, Any]] = []
    unscored_trials: list[dict[str, Any]] = []
    positive_count = 0

    for trial in trials:
        reward = trial.get("aggregate_reward")
        if _is_numeric_reward(reward):
            reward_f = float(reward)
            key = _reward_key(reward_f)
            reward_distribution[key] = reward_distribution.get(key, 0) + 1
            scored = _trial_base(trial)
            scored["reward"] = reward_f
            scored_trials.append(scored)
            if reward_f > 0:
                positive_count += 1
                taxonomy_key = "score_positive"
            else:
                taxonomy_key = "score_zero"
            failure_taxonomy[taxonomy_key] = failure_taxonomy.get(taxonomy_key, 0) + 1
            continue

        unscored = _trial_base(trial)
        unscored_trials.append(unscored)
        reason = unscored["failure_reason"] or "missing_reward"
        taxonomy_key = f"unscored:{reason}"
        failure_taxonomy[taxonomy_key] = failure_taxonomy.get(taxonomy_key, 0) + 1

    hard_failures: list[str] = []
    batch_state = str(batch.get("state") or "")
    if batch_state not in TERMINAL_BATCH_STATES:
        hard_failures.append(f"batch state is not terminal: {batch_state or '<missing>'}")
    expected = batch.get("expected_trial_count")
    if isinstance(expected, int) and len(trials) != expected:
        hard_failures.append(f"trial count {len(trials)} != expected {expected}")

    scored_count = len(scored_trials)
    summary = _canary_summary(
        hard_failures=hard_failures,
        scored_count=scored_count,
        positive_count=positive_count,
    )
    raw_status = "pass" if not hard_failures and scored_count > 0 and positive_count > 0 else "fail"
    override: dict[str, str] | None = None
    status = raw_status
    if raw_status == "fail" and override_issue and override_rationale:
        status = "override"
        override = {"issue": override_issue, "rationale": override_rationale}

    return {
        "gate": "score_positive_canary",
        "status": status,
        "raw_status": raw_status,
        "summary": summary,
        "batch_id": str(batch.get("id") or "<unknown>"),
        "batch_state": batch_state or None,
        "expected_trial_count": expected if isinstance(expected, int) else None,
        "trial_count": len(trials),
        "scored_trial_count": scored_count,
        "positive_reward_trial_count": positive_count,
        "reward_distribution": dict(sorted(reward_distribution.items())),
        "failure_taxonomy": dict(sorted(failure_taxonomy.items())),
        "scored_trials": scored_trials,
        "unscored_trials": unscored_trials,
        "baseline": _extract_baseline(batch),
        "override": override,
    }


def render_score_positive_canary_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Score-positive canary gate",
        "",
        f"- Status: `{report.get('status')}`",
        f"- Batch: `{report.get('batch_id')}`",
        f"- Summary: {report.get('summary')}",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| trial_count | {report.get('trial_count', 0)} |",
        f"| scored_trials | {report.get('scored_trial_count', 0)} |",
        f"| positive_reward_trials | {report.get('positive_reward_trial_count', 0)} |",
        "",
        "## Reward Distribution",
        "",
        "| Reward | Count |",
        "|---|---:|",
    ]
    distribution = report.get("reward_distribution")
    if isinstance(distribution, dict) and distribution:
        for reward, count in distribution.items():
            lines.append(f"| {reward} | {count} |")
    else:
        lines.append("| none | 0 |")

    lines.extend(["", "## Failure Taxonomy", "", "| Class | Count |", "|---|---:|"])
    taxonomy = report.get("failure_taxonomy")
    if isinstance(taxonomy, dict) and taxonomy:
        for key, count in taxonomy.items():
            lines.append(f"| {key} | {count} |")
    else:
        lines.append("| none | 0 |")

    lines.extend(
        [
            "",
            "## Scored Trials",
            "",
            "| Trial | Task | Reward | State | Failure reason | LLM evidence |",
            "|---|---|---:|---|---|---|",
        ]
    )
    scored_trials = report.get("scored_trials")
    if isinstance(scored_trials, list) and scored_trials:
        for trial in scored_trials:
            if not isinstance(trial, dict):
                continue
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(trial.get("trial_id")),
                        str(trial.get("task_id")),
                        str(trial.get("reward")),
                        str(trial.get("state")),
                        str(trial.get("failure_reason") or ""),
                        str(trial.get("llm_evidence_status") or ""),
                    ]
                )
                + " |"
            )
    else:
        lines.append("| none | none |  |  |  |  |")

    unscored_trials = report.get("unscored_trials")
    if isinstance(unscored_trials, list) and unscored_trials:
        lines.extend(
            [
                "",
                "## Unscored Trials",
                "",
                "| Trial | Task | State | Failure reason | LLM evidence |",
                "|---|---|---|---|---|",
            ]
        )
        for trial in unscored_trials:
            if not isinstance(trial, dict):
                continue
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(trial.get("trial_id")),
                        str(trial.get("task_id")),
                        str(trial.get("state")),
                        str(trial.get("failure_reason") or ""),
                        str(trial.get("llm_evidence_status") or ""),
                    ]
                )
                + " |"
            )

    override = report.get("override")
    if isinstance(override, dict):
        lines.extend(
            [
                "",
                "## Operator Override",
                "",
                f"- Issue: {override.get('issue')}",
                f"- Rationale: {override.get('rationale')}",
            ]
        )

    return "\n".join(lines) + "\n"


def _trial_benchmark_id(trial: dict[str, Any]) -> str | None:
    benchmark_id = trial.get("benchmark_id")
    if isinstance(benchmark_id, str) and benchmark_id:
        return benchmark_id
    task_id = trial.get("task_id")
    if not isinstance(task_id, str) or "/" not in task_id:
        return None
    return task_id.split("/", 1)[0]


def _numeric_reward_task_coverage(
    trials_by_batch: dict[str, list[dict[str, Any]]],
) -> dict[str, set[str]]:
    coverage: dict[str, set[str]] = {}
    for trials in trials_by_batch.values():
        for trial in trials:
            if trial.get("state") != "succeeded":
                continue
            if not _is_numeric_reward(trial.get("aggregate_reward")):
                continue
            task_id = trial.get("task_id")
            if not isinstance(task_id, str) or not task_id:
                continue
            benchmark_id = _trial_benchmark_id(trial)
            if not benchmark_id:
                continue
            coverage.setdefault(benchmark_id, set()).add(task_id)
    return coverage


def _task_has_numeric_reward(
    coverage: dict[str, set[str]],
    trial: dict[str, Any],
) -> bool:
    task_id = trial.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        return False
    benchmark_id = _trial_benchmark_id(trial)
    if not benchmark_id:
        return False
    return task_id in coverage.get(benchmark_id, set())


def _sweep_attempt_failures(
    *,
    batches: list[dict[str, Any]],
    trials_by_batch: dict[str, list[dict[str, Any]]],
    coverage: dict[str, set[str]],
) -> list[str]:
    failures: list[str] = []
    for batch in batches:
        batch_id = str(batch.get("id") or "<unknown>")
        batch_state = str(batch.get("state") or "")
        if batch_state not in TERMINAL_BATCH_STATES:
            failures.append(
                f"{batch_id}: batch state is not terminal: "
                f"{batch_state or '<missing>'}",
            )

        trials = trials_by_batch.get(batch_id, [])
        expected = batch.get("expected_trial_count")
        if isinstance(expected, int) and len(trials) != expected:
            failures.append(f"{batch_id}: trial count {len(trials)} != expected {expected}")

        for trial in trials:
            trial_id = str(trial.get("id") or "<unknown>")
            state = str(trial.get("state") or "")
            reason = trial.get("failure_reason")
            reward = trial.get("aggregate_reward")
            prefix = f"{batch_id}: {trial_id}"
            if state not in TERMINAL_TRIAL_STATES:
                failures.append(f"{prefix} is not terminal: {state or '<missing>'}")
                continue
            if reason == "internal_error" and _task_has_numeric_reward(coverage, trial):
                continue
            if reason in BENCHMARK_SIDE_FAILURE_REASONS:
                failures.append(f"{prefix} has benchmark-side failure reason {reason}")
                continue
            if state == "succeeded" and _is_numeric_reward(reward):
                continue
            if _task_has_numeric_reward(coverage, trial):
                continue
            if state == "succeeded":
                failures.append(f"{prefix} missing numeric reward")
            else:
                failures.append(f"{prefix} ended {state} with failure_reason={reason}")
    return failures


def check_reward_sweep(
    *,
    batches: list[dict[str, Any]],
    trials_by_batch: dict[str, list[dict[str, Any]]],
    expected_benchmark_ids: list[str],
    expected_task_counts: dict[str, int] | None = None,
) -> list[CheckResult]:
    coverage = _numeric_reward_task_coverage(trials_by_batch)
    failures = _sweep_attempt_failures(
        batches=batches,
        trials_by_batch=trials_by_batch,
        coverage=coverage,
    )
    for benchmark_id in expected_benchmark_ids:
        covered_tasks = coverage.get(benchmark_id, set())
        expected_count = (
            expected_task_counts.get(benchmark_id)
            if expected_task_counts is not None
            else None
        )
        if expected_count is None:
            if not covered_tasks:
                failures.append(f"{benchmark_id} missing numeric reward evidence")
            continue
        if expected_count <= 0:
            failures.append(f"{benchmark_id} has no runnable tasks according to /tasks/count")
        elif len(covered_tasks) < expected_count:
            failures.append(
                f"{benchmark_id} task coverage {len(covered_tasks)}/{expected_count}"
            )

    if failures:
        detail = "; ".join(failures[:25])
        if len(failures) > 25:
            detail += f"; ... +{len(failures) - 25} more"
        return [
            CheckResult(
                check_id="benchmarks.v1_reward_sweep_complete",
                status="fail",
                detail=detail,
                remediation=(
                    "Run or repair supported-benchmark acceptance batches until "
                    "every v1.0 benchmark has numeric reward evidence for every "
                    "registered runnable task."
                ),
            )
        ]

    covered_task_total = sum(
        len(coverage.get(benchmark_id, set()))
        for benchmark_id in expected_benchmark_ids
    )
    return [
        CheckResult(
            check_id="benchmarks.v1_reward_sweep_complete",
            status="pass",
            detail=(
                f"{len(expected_benchmark_ids)} benchmarks covered; "
                f"{covered_task_total} distinct tasks have numeric rewards"
            ),
            remediation="",
        )
    ]


def collect_batch_trials(
    client: JsonClient,
    batch_id: str,
    *,
    page_limit: int = 200,
) -> list[dict[str, Any]]:
    trials: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        query: dict[str, str | int] = {"batch_id": batch_id, "limit": page_limit}
        if cursor:
            query["cursor"] = cursor
        payload = client.get_json(f"/api/v1/trials?{urlencode(query)}")
        trials.extend(payload.get("items") or [])
        next_cursor = payload.get("next_cursor")
        if not next_cursor:
            return trials
        cursor = str(next_cursor)


def collect_task_counts(
    client: JsonClient,
    benchmark_ids: list[str],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for benchmark_id in benchmark_ids:
        payload = client.post_json(
            "/api/v1/tasks/count",
            {"task_filter": {"benchmark_id": benchmark_id}},
        )
        count = payload.get("count")
        if not isinstance(count, int) or isinstance(count, bool):
            raise ApiError(
                f"/api/v1/tasks/count returned non-integer count for {benchmark_id}"
            )
        counts[benchmark_id] = count
    return counts


def _read_token(ref: str) -> str:
    if ref.startswith("env:"):
        name = ref.removeprefix("env:")
        value = os.environ.get(name)
        if not value:
            raise SystemExit(f"missing token env var {name}")
        return value
    if ref.startswith("file:"):
        return Path(ref.removeprefix("file:")).read_text(encoding="utf-8").strip()
    return ref


def _print_results(results: list[CheckResult]) -> int:
    failed = False
    for result in results:
        print(f"{result.check_id}: {result.status} - {result.detail}")
        if result.status != "pass":
            failed = True
            if result.remediation:
                print(f"  remediation: {result.remediation}")
    return 1 if failed else 0


def _write_text(path: str | None, text: str) -> None:
    if not path:
        return
    Path(path).write_text(text, encoding="utf-8")


def _print_score_positive_canary_report(
    report: dict[str, Any],
    *,
    output_format: str,
) -> None:
    if output_format == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    if output_format == "markdown":
        print(render_score_positive_canary_markdown(report), end="")
        return
    print(
        "benchmarks.score_positive_canary: "
        f"{report['status']} - {report['summary']}"
    )
    print(
        "  scored_trials="
        f"{report['scored_trial_count']} "
        "positive_reward_trials="
        f"{report['positive_reward_trial_count']} "
        "reward_distribution="
        f"{json.dumps(report['reward_distribution'], sort_keys=True)}"
    )
    if report.get("override"):
        print(f"  override: {json.dumps(report['override'], sort_keys=True)}")


def _api_page_limit(value: str) -> int:
    try:
        limit = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if limit < 1 or limit > 200:
        raise argparse.ArgumentTypeError("must be between 1 and 200")
    return limit


def _request_timeout(value: str) -> float:
    try:
        timeout = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if timeout <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return timeout


def _run_readiness(args: argparse.Namespace) -> int:
    client = ApiClient(
        args.server_url,
        _read_token(args.token),
        timeout=args.request_timeout,
    )
    payload = client.get_json(
        "/api/v1/benchmarks?"
        + urlencode({"limit": args.limit, "include_empty": "true"}),
    )
    return _print_results(check_benchmark_readiness(payload.get("items") or []))


def _run_batch(args: argparse.Namespace) -> int:
    client = ApiClient(
        args.server_url,
        _read_token(args.token),
        timeout=args.request_timeout,
    )
    batch = client.get_json(f"/api/v1/batches/{args.batch_id}")
    trials = collect_batch_trials(client, args.batch_id, page_limit=args.limit)
    return _print_results(check_batch_rewards(batch=batch, trials=trials))


def _run_sweep(args: argparse.Namespace) -> int:
    client = ApiClient(
        args.server_url,
        _read_token(args.token),
        timeout=args.request_timeout,
    )
    expected_benchmarks = (
        list(args.expected_benchmark)
        if args.expected_benchmark
        else sorted(V1_SUPPORTED_BENCHMARK_IDS)
    )
    batches: list[dict[str, Any]] = []
    trials_by_batch: dict[str, list[dict[str, Any]]] = {}
    for batch_id in args.batch_id:
        batch = client.get_json(f"/api/v1/batches/{batch_id}")
        resolved_batch_id = str(batch.get("id") or batch_id)
        batches.append(batch)
        trials_by_batch[resolved_batch_id] = collect_batch_trials(
            client,
            batch_id,
            page_limit=args.limit,
        )
    expected_task_counts = (
        None
        if args.skip_task_counts
        else collect_task_counts(client, expected_benchmarks)
    )
    return _print_results(
        check_reward_sweep(
            batches=batches,
            trials_by_batch=trials_by_batch,
            expected_benchmark_ids=expected_benchmarks,
            expected_task_counts=expected_task_counts,
        )
    )


def _run_score_positive_canary(args: argparse.Namespace) -> int:
    if bool(args.override_issue) != bool(args.override_rationale):
        raise SystemExit(
            "--override-issue and --override-rationale must be supplied together"
        )
    client = ApiClient(
        args.server_url,
        _read_token(args.token),
        timeout=args.request_timeout,
    )
    batch = client.get_json(f"/api/v1/batches/{args.batch_id}")
    trials = collect_batch_trials(client, args.batch_id, page_limit=args.limit)
    report = build_score_positive_canary_report(
        batch=batch,
        trials=trials,
        override_issue=args.override_issue,
        override_rationale=args.override_rationale,
    )
    _write_text(args.json_output, json.dumps(report, indent=2, sort_keys=True) + "\n")
    _write_text(args.markdown_output, render_score_positive_canary_markdown(report))
    _print_score_positive_canary_report(report, output_format=args.format)
    return 0 if report["status"] in {"pass", "override"} else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check benchmark readiness and reward-production acceptance.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("readiness", "batch"):
        p = sub.add_parser(command)
        p.add_argument("--server-url", required=True)
        p.add_argument("--token", required=True, help="Bearer token, env:NAME, or file:PATH.")
        p.add_argument(
            "--limit",
            type=_api_page_limit,
            default=200,
            help="API page size, 1-200.",
        )
        p.add_argument(
            "--request-timeout",
            type=_request_timeout,
            default=120.0,
            help="Per-request timeout in seconds.",
        )
        if command == "batch":
            p.add_argument("--batch-id", required=True)
            p.set_defaults(func=_run_batch)
        else:
            p.set_defaults(func=_run_readiness)
    sweep = sub.add_parser("sweep")
    sweep.add_argument("--server-url", required=True)
    sweep.add_argument("--token", required=True, help="Bearer token, env:NAME, or file:PATH.")
    sweep.add_argument(
        "--limit",
        type=_api_page_limit,
        default=200,
        help="API page size for trial pagination, 1-200.",
    )
    sweep.add_argument(
        "--request-timeout",
        type=_request_timeout,
        default=120.0,
        help="Per-request timeout in seconds.",
    )
    sweep.add_argument(
        "--batch-id",
        action="append",
        required=True,
        help="Acceptance batch id. Repeat for one batch per benchmark.",
    )
    sweep.add_argument(
        "--expected-benchmark",
        action="append",
        help=(
            "Expected benchmark id. Repeat to override the default v1.0 "
            "supported allowlist."
        ),
    )
    sweep.add_argument(
        "--skip-task-counts",
        action="store_true",
        help="Only require at least one numeric-reward task per expected benchmark.",
    )
    sweep.set_defaults(func=_run_sweep)

    canary = sub.add_parser(
        "score-positive-canary",
        help=(
            "Fail unless a terminal canary batch has at least one scored trial "
            "with reward > 0."
        ),
    )
    canary.add_argument("--server-url", required=True)
    canary.add_argument("--token", required=True, help="Bearer token, env:NAME, or file:PATH.")
    canary.add_argument("--batch-id", required=True)
    canary.add_argument(
        "--limit",
        type=_api_page_limit,
        default=200,
        help="API page size for trial pagination, 1-200.",
    )
    canary.add_argument(
        "--request-timeout",
        type=_request_timeout,
        default=120.0,
        help="Per-request timeout in seconds.",
    )
    canary.add_argument(
        "--format",
        choices=("text", "json", "markdown"),
        default="text",
        help="Stdout evidence format.",
    )
    canary.add_argument(
        "--json-output",
        help="Optional path for structured JSON evidence.",
    )
    canary.add_argument(
        "--markdown-output",
        help="Optional path for pasteable Markdown evidence.",
    )
    canary.add_argument(
        "--override-issue",
        help="Issue or PR reference documenting an explicit operator override.",
    )
    canary.add_argument(
        "--override-rationale",
        help="Operator rationale for proceeding despite a failing score-positive gate.",
    )
    canary.set_defaults(func=_run_score_positive_canary)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ApiError as exc:
        return _main_error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
