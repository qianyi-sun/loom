"""`loom qa matrix` — end-to-end agent × benchmark validation against a real provider.

Codifies the manual QA workflow exercised in #316: query live catalogs,
pick one representative task per ready benchmark, submit per-provider-family
batches, poll until terminal, classify outcomes into PASS_PLATFORM /
FAIL_PLATFORM / SKIPPED / STUCK, and emit a matrix table operators can
paste into a tracking issue.

Usage:

    loom qa matrix \\
        --provider-connection qa-relay \\
        --model gpt-4o-mini \\
        [--agent <name>]... [--benchmark <id>]... \\
        [--timeout-min 30] [--output qa-results.md]

The `--provider-connection` must already exist (registered via
`loom providers create --type openai-compatible --base-url ... --api-key env:VAR`).
Cells where the agent's `supported_providers` excludes the connection's
provider family are recorded as SKIPPED with reason="provider mismatch"
— operators see them in the matrix without paying for impossible trials.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

import httpx

from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)
from loom_cli.time_format import format_local_datetime

CellState = Literal[
    "PASS_PLATFORM", "FAIL_PLATFORM", "SUSPECT_PASS",
    "SKIPPED", "STUCK", "PENDING",
]


@dataclass
class MatrixCell:
    agent: str
    benchmark: str
    state: CellState
    reason: str | None = None
    reward: float | None = None
    trial_id: str | None = None
    failure_reason: str | None = None
    llm_calls_count: int | None = None


@dataclass
class MatrixResult:
    started_at: str
    finished_at: str | None
    cluster_url: str
    provider_connection: str
    model: str
    cells: list[MatrixCell] = field(default_factory=list)
    batch_ids: list[str] = field(default_factory=list)

    def by_outcome(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for cell in self.cells:
            counts[cell.state] = counts.get(cell.state, 0) + 1
        return counts


def _fetch_catalogs(
    c: httpx.Client,
    *,
    agent_filter: set[str] | None,
    benchmark_filter: set[str] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Pull /api/v1/agents and /api/v1/benchmarks, apply --agent /
    --benchmark filters, and drop entries that aren't service-mode ready."""
    agents_body = assert_2xx(c.get("/api/v1/agents"), action="GET /agents")
    agents = [
        a for a in agents_body.get("items", [])
        if a.get("service_mode_ready") is True
    ]
    if agent_filter:
        agents = [a for a in agents if a["name"] in agent_filter]
        missing = agent_filter - {a["name"] for a in agents}
        if missing:
            raise SystemExit(
                f"--agent: unknown / not ready: {sorted(missing)}",
            )

    benchmarks_body = assert_2xx(
        c.get("/api/v1/benchmarks"), action="GET /benchmarks",
    )
    # The API exposes benchmark readiness via `readiness_state` ∈
    # {"runnable", "degraded", "blocked", ...} and a parallel
    # `selectable` boolean. A benchmark is matrix-usable when either
    # signal is green; we accept both for forward compat.
    benchmarks = [
        b for b in benchmarks_body.get("items", [])
        if b.get("readiness_state") == "runnable"
        or b.get("selectable") is True
    ]
    if benchmark_filter:
        benchmarks = [b for b in benchmarks if b["id"] in benchmark_filter]
        missing = benchmark_filter - {b["id"] for b in benchmarks}
        if missing:
            raise SystemExit(
                f"--benchmark: unknown / not ready: {sorted(missing)}",
            )

    return agents, benchmarks


def _pick_representative_task(
    c: httpx.Client, *, benchmark_id: str,
) -> str | None:
    """Return one task id for the benchmark, or None if it has no
    runnable tasks. We use the catalog's POST /tasks/count surrogate
    (or fall back to GET /tasks?benchmark_id=X&limit=1) — sorted by
    id ascending for determinism."""
    r = c.get(
        "/api/v1/tasks", params={"benchmark_id": benchmark_id, "limit": 1},
    )
    if r.status_code == 404:
        return None
    body = assert_2xx(r, action=f"GET /tasks?benchmark_id={benchmark_id}")
    items = body.get("items") or []
    if not items:
        return None
    return str(items[0]["id"])


def _provider_compatible(agent: dict[str, Any], provider: str) -> bool:
    """Match the agent's supported_providers against the relay's
    provider family. The wildcard `*` accepts any provider."""
    supported = agent.get("supported_providers") or []
    if "*" in supported:
        return True
    return provider in supported


def _resolve_provider_connection(
    c: httpx.Client, connection_name: str,
) -> dict[str, Any]:
    """List the team's provider connections and return the one with
    matching `name`. The API only exposes get-by-UUID; this is the
    canonical name → record lookup."""
    body = assert_2xx(
        c.get("/api/v1/provider-connections"),
        action="GET /provider-connections",
    )
    for item in body.get("items", []):
        if item.get("name") == connection_name:
            return item  # type: ignore[no-any-return]
    raise SystemExit(
        f"--provider-connection: no connection named {connection_name!r} "
        f"on this team. Available: "
        f"{sorted(i['name'] for i in body.get('items', []))}",
    )


def _agent_provider_family(conn: dict[str, Any]) -> str:
    """Read the relay's rate-card provider family from a resolved
    connection record. Used to evaluate each agent's supported_providers."""
    return str(conn.get("rate_card_provider") or conn.get("type"))


def _build_cells_and_combinations(
    *,
    agents: list[dict[str, Any]],
    benchmarks: list[dict[str, Any]],
    task_ids_by_benchmark: dict[str, str],
    provider_family: str,
    model: str,
) -> tuple[list[MatrixCell], list[dict[str, Any]]]:
    """Walk the agent × benchmark grid. Build:
    - cells: one MatrixCell per cell, initialized PENDING or SKIPPED.
    - combinations: list of {agent_name, agent_model} dicts for the
      batch submission, deduped per agent (one combination per agent
      runs against ALL pending tasks below).
    """
    cells: list[MatrixCell] = []
    runnable_agent_names: set[str] = set()
    for agent in agents:
        name = agent["name"]
        needs_model = bool(agent.get("needs_model"))
        compatible = (not needs_model) or _provider_compatible(
            agent, provider_family,
        )
        for benchmark in benchmarks:
            bid = benchmark["id"]
            task_id = task_ids_by_benchmark.get(bid)
            if task_id is None:
                cells.append(MatrixCell(
                    agent=name, benchmark=bid,
                    state="SKIPPED",
                    reason="benchmark has no runnable tasks",
                ))
                continue
            if not compatible:
                cells.append(MatrixCell(
                    agent=name, benchmark=bid,
                    state="SKIPPED",
                    reason=(
                        f"agent supports {agent.get('supported_providers')}, "
                        f"relay provider family is {provider_family!r}"
                    ),
                ))
                continue
            cells.append(MatrixCell(
                agent=name, benchmark=bid, state="PENDING",
            ))
            runnable_agent_names.add(name)

    combinations: list[dict[str, Any]] = []
    for agent in agents:
        if agent["name"] not in runnable_agent_names:
            continue
        needs_model = bool(agent.get("needs_model"))
        combo: dict[str, Any] = {
            "agent_name": agent["name"],
            "agent_model": (
                None if not needs_model else {
                    "provider": provider_family,
                    "name": model,
                    "source": "api",
                }
            ),
        }
        combinations.append(combo)
    return cells, combinations


def _submit_batch(
    c: httpx.Client,
    *,
    name: str,
    combinations: list[dict[str, Any]],
    task_ids: list[str],
    provider_connection_id: str,
    provider_model_id: str,
) -> str:
    """Submit ONE batch covering every (combination × task_id) pair.
    The CP fans out into individual trials."""
    payload = {
        "name": name,
        "task_filter": {"subset_kind": "explicit", "task_ids": task_ids},
        # When combinations is non-empty, trial_config.agent_name /
        # agent_model MUST be absent — each combination carries its
        # own. Pass an empty dict; the schema requires the key.
        "trial_config": {},
        "combinations": combinations,
        "provider_connection_id": provider_connection_id,
        "provider_model_id": provider_model_id,
    }
    body = assert_2xx(
        c.post("/api/v1/batches", json=payload),
        action=f"POST /batches ({name!r})",
    )
    # POST /batches returns `batch_id` on create; GET /batches lists
    # under `id`. Tolerate both for forward compat.
    return str(body.get("batch_id") or body.get("id"))


async def _wait_for_batches(
    base_url: str, token: str, batch_ids: list[str],
    *, timeout_sec: float, poll_interval_sec: float = 10.0,
) -> dict[str, str]:
    """Poll each batch's state until it terminates or the timeout
    expires. Returns {batch_id: terminal_state}."""
    deadline = time.monotonic() + timeout_sec
    states: dict[str, str] = {}
    async with httpx.AsyncClient(
        base_url=base_url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    ) as ac:
        pending = set(batch_ids)
        while pending and time.monotonic() < deadline:
            for bid in list(pending):
                r = await ac.get(f"/api/v1/batches/{bid}")
                if r.status_code != 200:
                    continue
                body = r.json()
                state = body.get("state")
                # Batch terminal states (see loom_service.batch_runner):
                # `finished` (success/partial — see result_status) and
                # `cancelled`. `succeeded`/`failed` apply to individual
                # TRIALS, not batches.
                if state in {"finished", "cancelled"}:
                    states[bid] = state
                    pending.discard(bid)
            if pending:
                await asyncio.sleep(poll_interval_sec)
        for bid in pending:
            states[bid] = "STUCK"
    return states


def _fetch_trials_for_batch(
    c: httpx.Client, batch_id: str,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    offset = 0
    while True:
        body = assert_2xx(
            c.get(
                "/api/v1/trials",
                params={"batch_id": batch_id, "limit": 100, "offset": offset},
            ),
            action=f"GET /trials?batch_id={batch_id}",
        )
        chunk = body.get("items", [])
        items.extend(chunk)
        if len(chunk) < 100:
            break
        offset += 100
    return items


def _is_capability_mismatch(failure_message: str) -> bool:
    """Heuristic: the failure represents an (agent, benchmark)
    capability mismatch declared at platform level, NOT a Loom bug.
    E.g. `oracle` needs `solution/solve.sh` — benchmarks that don't
    ship one are oracle-incompatible by design, not platform failures.

    These should be SKIPPED in the matrix so PASS/FAIL counts reflect
    real platform health rather than declared incompatibilities.
    """
    msg = failure_message.lower()
    # Oracle's hard requirement: a `solution/solve.sh` script in the
    # task bundle. Benchmarks without one are oracle-incompatible.
    if "oracleagent requires" in msg and "solve.sh" in msg:
        return True
    return False


def _classify_trial(
    trial: dict[str, Any], *, agent_needs_model: bool = False,
) -> tuple[CellState, str | None, float | None]:
    state = trial.get("state")
    # /api/v1/trials returns `aggregate_reward` at the top level.
    # `result.aggregate_reward` is the older detail-view shape; check
    # both for forward-compat with future SPA detail responses.
    reward = trial.get("aggregate_reward")
    if reward is None:
        result = trial.get("result") or {}
        if isinstance(result, dict):
            reward = result.get("aggregate_reward")
    if state == "succeeded" and isinstance(reward, (int, float)):
        # SUSPECT_PASS guard (#388): a model-using agent that
        # "succeeded" without making any LLM call is almost certainly
        # passing on a pre-existing reference solution shipped with
        # the task bundle, not on its own work. mbpp does this today;
        # other benchmarks may too.
        llm_calls = trial.get("llm_calls_count")
        if (
            agent_needs_model
            and isinstance(llm_calls, int)
            and llm_calls == 0
        ):
            return (
                "SUSPECT_PASS",
                "model-using agent succeeded without an LLM call — "
                "likely passing on a pre-shipped reference solution; "
                "verify before trusting (see #388)",
                float(reward),
            )
        return "PASS_PLATFORM", None, float(reward)
    if state in {"failed", "cancelled"}:
        fr = trial.get("failure_reason") or state
        fm = trial.get("failure_message") or ""
        if fm and _is_capability_mismatch(fm):
            # Re-classify "agent doesn't apply to this benchmark" as
            # SKIPPED — these aren't platform failures, they're
            # declared (agent, benchmark) incompatibilities.
            return "SKIPPED", f"capability mismatch: {fm[:160]}", None
        msg = fr if not fm else f"{fr}: {fm[:200]}"
        return "FAIL_PLATFORM", msg, None
    return "STUCK", f"state={state}", None


def _classify_cells(
    cells: list[MatrixCell],
    trials: list[dict[str, Any]],
    *,
    agents_needing_model: set[str] | None = None,
) -> None:
    """Mutate `cells` in place. Match each trial back to its (agent, task)
    via the trial's config + task metadata. Cells without a matched
    trial stay STUCK with reason='no trial recorded'.

    `agents_needing_model` is the set of agent slugs whose runs are
    expected to make at least one LLM call. Used by the SUSPECT_PASS
    guard to flag $0-cost "successes" by agents that should have
    spent something."""
    needs_model = agents_needing_model or set()
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for trial in trials:
        # The /api/v1/trials list shape puts agent_name at the top
        # level. (Some older detail responses nest it under `config`;
        # fall back to that for forward-compat.)
        cfg = trial.get("config") or {}
        agent_name = trial.get("agent_name") or cfg.get("agent_name")
        # Trial's task_id is "{benchmark_id}/{task_id_within_benchmark}"
        # or the benchmark_id itself for atomic benchmarks. We index
        # on the benchmark_id derived from the trial.
        bench = trial.get("benchmark_id") or (
            (cfg.get("task_id") or trial.get("task_id") or "").split("/", 1)[0]
        )
        if agent_name and bench:
            by_key[(str(agent_name), str(bench))] = trial

    for cell in cells:
        if cell.state != "PENDING":
            continue
        matched = by_key.get((cell.agent, cell.benchmark))
        if matched is None:
            cell.state = "STUCK"
            cell.reason = "no trial recorded for this cell"
            continue
        cell.trial_id = str(matched.get("id") or "")
        llm_calls = matched.get("llm_calls_count")
        cell.llm_calls_count = llm_calls if isinstance(llm_calls, int) else None
        state, reason, reward = _classify_trial(
            matched, agent_needs_model=cell.agent in needs_model,
        )
        cell.state = state
        if reason is not None:
            cell.reason = reason
            cell.failure_reason = matched.get("failure_reason")
        if reward is not None:
            cell.reward = reward


def _render_markdown(result: MatrixResult) -> str:
    counts = result.by_outcome()
    lines: list[str] = []
    lines.append("# Agent × benchmark matrix\n")
    lines.append(f"- Cluster: `{result.cluster_url}`")
    lines.append(f"- Provider connection: `{result.provider_connection}`")
    lines.append(f"- Model: `{result.model}`")
    lines.append(f"- Started: {format_local_datetime(result.started_at)}")
    if result.finished_at:
        lines.append(f"- Finished: {format_local_datetime(result.finished_at)}")
    if result.batch_ids:
        lines.append(f"- Batches: {', '.join(result.batch_ids)}")
    lines.append("")
    lines.append("## Summary")
    for state, n in sorted(counts.items()):
        lines.append(f"- {state}: {n}")
    lines.append("")
    lines.append("## Cells (failures + suspect-passes + skips first)\n")
    lines.append(
        "| Agent | Benchmark | State | Reward | LLM calls | Reason | Trial |",
    )
    lines.append("|---|---|---|---|---|---|---|")
    sort_key = {
        "FAIL_PLATFORM": 0,
        "STUCK": 1,
        "SUSPECT_PASS": 2,
        "SKIPPED": 3,
        "PASS_PLATFORM": 4,
        "PENDING": 5,
    }
    for cell in sorted(
        result.cells,
        key=lambda c: (sort_key.get(c.state, 9), c.agent, c.benchmark),
    ):
        reward = "" if cell.reward is None else f"{cell.reward:.3f}"
        llm_calls = (
            "" if cell.llm_calls_count is None
            else str(cell.llm_calls_count)
        )
        reason = (cell.reason or "").replace("|", "\\|")[:120]
        trial = cell.trial_id or ""
        lines.append(
            f"| {cell.agent} | {cell.benchmark} | {cell.state} | "
            f"{reward} | {llm_calls} | {reason} | {trial} |",
        )
    lines.append("")
    return "\n".join(lines)


def _matrix(args: argparse.Namespace) -> int:
    agent_filter = set(args.agent) if args.agent else None
    bench_filter = set(args.benchmark) if args.benchmark else None
    try:
        cfg = require_logged_in()
    except NotLoggedInError:
        sys.stderr.write(
            "error: not logged in. Run `loom auth login` against the cluster.\n",
        )
        return 2

    started = datetime.now(UTC).isoformat()
    result = MatrixResult(
        started_at=started,
        finished_at=None,
        cluster_url=str(cfg.server_url),
        provider_connection=args.provider_connection,
        model=args.model,
    )

    with authed_client(cfg, timeout=60.0) as c:
        agents, benchmarks = _fetch_catalogs(
            c, agent_filter=agent_filter, benchmark_filter=bench_filter,
        )
        if not agents:
            sys.stderr.write("error: no ready agents in scope.\n")
            return 1
        if not benchmarks:
            sys.stderr.write("error: no ready benchmarks in scope.\n")
            return 1

        # One representative task per benchmark.
        task_ids_by_benchmark: dict[str, str] = {}
        for benchmark in benchmarks:
            tid = _pick_representative_task(c, benchmark_id=benchmark["id"])
            if tid:
                task_ids_by_benchmark[benchmark["id"]] = tid

        conn_record = _resolve_provider_connection(
            c, args.provider_connection,
        )
        provider_family = _agent_provider_family(conn_record)
        connection_id = str(conn_record["id"])

        cells, combinations = _build_cells_and_combinations(
            agents=agents,
            benchmarks=benchmarks,
            task_ids_by_benchmark=task_ids_by_benchmark,
            provider_family=provider_family,
            model=args.model,
        )
        result.cells = cells

        runnable_task_ids = sorted(set(task_ids_by_benchmark.values()))
        if combinations and runnable_task_ids:
            batch_name = (
                f"{args.batch_name_prefix}-"
                f"{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"
            )
            try:
                batch_id = _submit_batch(
                    c,
                    name=batch_name,
                    combinations=combinations,
                    task_ids=runnable_task_ids,
                    provider_connection_id=connection_id,
                    provider_model_id=args.model,
                )
            except HttpStatusError as exc:
                sys.stderr.write(f"batch submit failed: {exc}\n")
                return 1
            result.batch_ids.append(batch_id)
            print(f"submitted batch {batch_id} ({batch_name!r})", file=sys.stderr)

            terminal = asyncio.run(_wait_for_batches(
                str(cfg.server_url),
                cfg.auth_token or "",
                result.batch_ids,
                timeout_sec=args.timeout_min * 60.0,
            ))
            for bid, state in terminal.items():
                print(f"batch {bid}: {state}", file=sys.stderr)

            trials: list[dict[str, Any]] = []
            for bid in result.batch_ids:
                trials.extend(_fetch_trials_for_batch(c, bid))
            needs_model = {
                a["name"] for a in agents if a.get("needs_model")
            }
            _classify_cells(
                cells, trials, agents_needing_model=needs_model,
            )

    result.finished_at = datetime.now(UTC).isoformat()

    md = _render_markdown(result)
    if args.output:
        with open(args.output, "w") as f:
            f.write(md)
            if args.json_output:
                pass
        print(f"wrote matrix to {args.output}", file=sys.stderr)
    print(md)

    if args.json_output:
        with open(args.json_output, "w") as f:
            json.dump({
                "started_at": result.started_at,
                "finished_at": result.finished_at,
                "cluster_url": result.cluster_url,
                "provider_connection": result.provider_connection,
                "model": result.model,
                "batch_ids": result.batch_ids,
                "cells": [asdict(c) for c in result.cells],
            }, f, indent=2)
        print(f"wrote JSON to {args.json_output}", file=sys.stderr)

    # Exit code: 0 if every PENDING cell ended up PASS_PLATFORM; 1 otherwise.
    failed = sum(
        1 for c in result.cells if c.state in {"FAIL_PLATFORM", "STUCK"}
    )
    return 0 if failed == 0 else 1


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom qa",
        description=(
            "QA matrix runner — end-to-end agent × benchmark validation "
            "against a real provider via an OpenAI-compatible relay."
        ),
    )
    sub = parser.add_subparsers(dest="qa_cmd", required=True)

    p_matrix = sub.add_parser(
        "matrix",
        help=(
            "Submit one trial per (agent × representative-benchmark-task) "
            "via a real provider connection. Classifies outcomes and emits "
            "a matrix table."
        ),
    )
    p_matrix.add_argument(
        "--provider-connection", required=True,
        help="Name of the registered provider connection (loom providers create ...).",
    )
    p_matrix.add_argument(
        "--model", required=True,
        help="Model id known to the relay (e.g. gpt-4o-mini).",
    )
    p_matrix.add_argument(
        "--agent", action="append", default=None,
        help="Limit to specific agents (repeatable). Default: all ready.",
    )
    p_matrix.add_argument(
        "--benchmark", action="append", default=None,
        help="Limit to specific benchmarks (repeatable). Default: all ready.",
    )
    p_matrix.add_argument(
        "--timeout-min", type=float, default=30.0,
        help="Wall-clock cap for the run, in minutes (default: 30).",
    )
    p_matrix.add_argument(
        "--batch-name-prefix", default="qa-matrix",
        help="Prefix for the batch name (a timestamp is appended).",
    )
    p_matrix.add_argument(
        "--output", default=None,
        help="Path to write the markdown matrix table (also printed to stdout).",
    )
    p_matrix.add_argument(
        "--json-output", default=None,
        help="Path to write the matrix as JSON for programmatic consumption.",
    )
    p_matrix.set_defaults(handler=_matrix)

    args = parser.parse_args(argv)
    return int(args.handler(args))


def _iter_agents_for_test(items: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Test hook — not part of the public CLI."""
    return list(items)
