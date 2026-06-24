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


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    status: str
    detail: str
    remediation: str


class JsonClient(Protocol):
    def get_json(self, path: str) -> dict[str, Any]:
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
        if item.get("blocker_reason") == "unsupported_runtime":
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
