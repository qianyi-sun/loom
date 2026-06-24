#!/usr/bin/env python3
"""Public-beta benchmark reward acceptance gate.

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
            "hendrycks-math",
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
    def __init__(self, server_url: str, token: str) -> None:
        self.server_url = server_url.rstrip("/")
        self.token = token

    def get_json(self, path: str) -> dict[str, Any]:
        req = Request(
            self.server_url + path,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.token}",
            },
        )
        try:
            with urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = _http_error_detail(exc)
            raise ApiError(f"HTTP {exc.code} {exc.reason}: {detail}") from exc
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
            with urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = _http_error_detail(exc)
            raise ApiError(f"HTTP {exc.code} {exc.reason}: {detail}") from exc
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
                remediation="Provision the supported benchmark catalog before public-beta validation.",
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


def check_reward_sweep(
    *,
    batches: list[dict[str, Any]],
    trials_by_batch: dict[str, list[dict[str, Any]]],
    expected_benchmark_ids: list[str],
    expected_task_counts: dict[str, int] | None = None,
) -> list[CheckResult]:
    failures: list[str] = []
    for batch in batches:
        batch_id = str(batch.get("id") or "<unknown>")
        batch_results = check_batch_rewards(
            batch=batch,
            trials=trials_by_batch.get(batch_id, []),
        )
        for result in batch_results:
            if result.status != "pass":
                failures.append(f"{batch_id}: {result.detail}")

    coverage = _numeric_reward_task_coverage(trials_by_batch)
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


def _run_readiness(args: argparse.Namespace) -> int:
    client = ApiClient(args.server_url, _read_token(args.token))
    payload = client.get_json(
        "/api/v1/benchmarks?"
        + urlencode({"limit": args.limit, "include_empty": "true"}),
    )
    return _print_results(check_benchmark_readiness(payload.get("items") or []))


def _run_batch(args: argparse.Namespace) -> int:
    client = ApiClient(args.server_url, _read_token(args.token))
    batch = client.get_json(f"/api/v1/batches/{args.batch_id}")
    trials = collect_batch_trials(client, args.batch_id, page_limit=args.limit)
    return _print_results(check_batch_rewards(batch=batch, trials=trials))


def _run_sweep(args: argparse.Namespace) -> int:
    client = ApiClient(args.server_url, _read_token(args.token))
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check benchmark readiness and reward-production acceptance.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("readiness", "batch"):
        p = sub.add_parser(command)
        p.add_argument("--server-url", required=True)
        p.add_argument("--token", required=True, help="Bearer token, env:NAME, or file:PATH.")
        p.add_argument("--limit", type=int, default=200)
        if command == "batch":
            p.add_argument("--batch-id", required=True)
            p.set_defaults(func=_run_batch)
        else:
            p.set_defaults(func=_run_readiness)
    sweep = sub.add_parser("sweep")
    sweep.add_argument("--server-url", required=True)
    sweep.add_argument("--token", required=True, help="Bearer token, env:NAME, or file:PATH.")
    sweep.add_argument("--limit", type=int, default=200)
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
