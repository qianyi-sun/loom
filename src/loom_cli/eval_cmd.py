"""`loom eval {run, batch, trial}` — submit / inspect work on the deployed
Loom server.

Wraps the server-side routes:
- POST /api/v1/trials                  → `loom eval run`
- GET  /api/v1/trials[/{id}]           → `loom eval trial {list, show}`
- POST /api/v1/batches                 → `loom eval batch create`
- GET  /api/v1/batches[/{id}]          → `loom eval batch {list, show}`
- POST /api/v1/batches/{id}/cancel     → `loom eval batch cancel`

`--provider NAME` is the CLI-facing handle; the route layer wants a
UUID. We resolve via `/provider-connections` (matches the lookup
`loom providers show/update/delete` already does).

`loom run` (existing, local-stateless) stays as-is. The distinction
(cluster-deploy.md §CLI surface): `loom run` runs one trial on your
machine with no server; `loom eval` submits to a deployed Loom
(batches, persistence, sharing across the team).
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, cast

from loom_cli.providers_cmd import (
    _resolve_by_name,
    _run_with_error_handling,
)
from loom_cli.server_client import (
    assert_2xx,
    authed_client,
    require_logged_in,
)

# Map provider_connection.type → ModelSpec.provider. The agent uses
# the `provider/name` form for its LLM-call serialization (litellm,
# anthropic-sdk, etc.); the gateway is what actually routes via the
# connection's credentials. For `custom`, default to openai-compatible
# wire shape — operators creating a `custom` connection are typically
# pointing at an OpenAI-compatible endpoint.
_TYPE_TO_AGENT_PROVIDER: dict[str, str] = {
    "openai-compatible": "openai",
    "anthropic": "anthropic",
    "google": "google",
    "custom": "openai",
}


def _build_agent_model(connection_type: str, model_name: str) -> dict[str, Any]:
    """Construct the `agent_model` dict the worker hands to the agent.
    Provider is derived from the connection type; the model name comes
    from `--model`. `source='api'` is the default for cloud / gateway-
    routed models — local-server / hf overrides are out of scope here
    (use `loom run` for local execution)."""
    provider = _TYPE_TO_AGENT_PROVIDER.get(connection_type, "openai")
    return {
        "provider": provider,
        "name": model_name,
        "source": "api",
    }


def _load_task_filter_json(raw: str) -> dict[str, Any]:
    """Parse --task-filter argument. JSON string or @path/to/file."""
    if raw.startswith("@"):
        path = raw[1:]
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except OSError as exc:
            raise argparse.ArgumentTypeError(
                f"--task-filter @{path}: {exc}",
            ) from exc
        except json.JSONDecodeError as exc:
            raise argparse.ArgumentTypeError(
                f"--task-filter @{path}: invalid JSON: {exc}",
            ) from exc
    else:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise argparse.ArgumentTypeError(
                f"--task-filter: invalid JSON: {exc}",
            ) from exc
    if not isinstance(data, dict):
        raise argparse.ArgumentTypeError(
            "--task-filter must be a JSON object",
        )
    return cast(dict[str, Any], data)


# ──────────────────────────────────────────────────────────────────────
# eval run — single trial
# ──────────────────────────────────────────────────────────────────────


def _print_trial_summary(item: dict[str, Any]) -> None:
    print(f"id:               {item.get('id') or item.get('trial_id')}")
    print(f"task_id:          {item.get('task_id', '(unknown)')}")
    print(f"state:            {item.get('state', '(unknown)')}")
    if item.get("agent_name") is not None:
        print(f"agent:            {item['agent_name']}")
    if item.get("model") is not None:
        # `model` from `_trial_row` is the agent_model dict.
        m = item["model"]
        if isinstance(m, dict):
            print(f"model:            {m.get('provider')}/{m.get('name')}")
        else:
            print(f"model:            {m}")
    if item.get("aggregate_reward") is not None:
        print(f"reward:           {item['aggregate_reward']}")
    if item.get("cost_usd") is not None:
        print(f"cost_usd:         {item['cost_usd']}")
    if item.get("submitted_at"):
        print(f"submitted_at:     {item['submitted_at']}")
    if item.get("finished_at"):
        print(f"finished_at:     {item['finished_at']}")
    if item.get("failure_reason"):
        print(f"failure_reason:   {item['failure_reason']}")


def _run(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            conn = _resolve_by_name(c, args.provider)
            trial_config: dict[str, Any] = {
                "agent_name": args.agent,
                "agent_model": _build_agent_model(conn["type"], args.model),
            }
            payload: dict[str, Any] = {
                "task_id": args.task,
                "config": trial_config,
                "provider_connection_id": conn["id"],
                "provider_model_id": args.model,
            }
            resp = c.post("/api/v1/trials", json=payload)
        body = assert_2xx(resp, action=f"submit trial for task {args.task!r}")
        print(
            f"Submitted trial for task {args.task!r} via provider "
            f"{args.provider!r}:",
        )
        _print_trial_summary(body)
        return 0

    return _run_with_error_handling(_body)


# ──────────────────────────────────────────────────────────────────────
# eval batch create / list / show / cancel
# ──────────────────────────────────────────────────────────────────────


def _print_batch_summary(item: dict[str, Any]) -> None:
    print(f"id:                    {item.get('id') or item.get('batch_id')}")
    print(f"name:                  {item.get('name', '(unset)')}")
    print(f"state:                 {item.get('state', '(unknown)')}")
    print(f"expected_trial_count:  {item.get('expected_trial_count', '?')}")
    print(f"n_per_task:            {item.get('n_per_task', 1)}")
    print(f"backend:               {item.get('backend', 'docker')}")
    if item.get("trial_summary"):
        ts = item["trial_summary"]
        print(
            f"trials:                "
            f"q={ts.get('queued', 0)}  "
            f"c={ts.get('claimed', 0)}  "
            f"r={ts.get('running', 0)}  "
            f"s={ts.get('succeeded', 0)}  "
            f"f={ts.get('failed', 0)}  "
            f"x={ts.get('cancelled', 0)}",
        )
    if item.get("aggregate_reward") is not None:
        print(f"aggregate_reward:      {item['aggregate_reward']}")
    if item.get("total_cost_usd") is not None:
        print(f"total_cost_usd:        {item['total_cost_usd']}")
    if item.get("created_at"):
        print(f"created_at:            {item['created_at']}")
    if item.get("finished_at"):
        print(f"finished_at:           {item['finished_at']}")


def _batch_create(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            conn = _resolve_by_name(c, args.provider)
            trial_config: dict[str, Any] = {
                "agent_name": args.agent,
                "agent_model": _build_agent_model(conn["type"], args.model),
            }
            # --benchmark is a SHORTCUT for the most common task_filter:
            # `{"benchmark_id": <slug>}`. Operators wanting richer
            # filters use --task-filter JSON instead. Both: rejected so
            # the precedence is explicit.
            task_filter: dict[str, Any]
            if args.task_filter is not None and args.benchmark is not None:
                sys.stderr.write(
                    "error: --benchmark and --task-filter are mutually "
                    "exclusive (--benchmark B is sugar for "
                    "--task-filter '{\"benchmark_id\":\"B\"}').\n",
                )
                return 2
            if args.task_filter is not None:
                task_filter = args.task_filter
            elif args.benchmark is not None:
                task_filter = {"benchmark_id": args.benchmark}
            else:
                sys.stderr.write(
                    "error: one of --benchmark or --task-filter is required.\n",
                )
                return 2
            payload: dict[str, Any] = {
                "name": args.name,
                "task_filter": task_filter,
                "trial_config": trial_config,
                "provider_connection_id": conn["id"],
                "provider_model_id": args.model,
            }
            if args.n_per_task is not None:
                payload["n_per_task"] = args.n_per_task
            if args.backend is not None:
                payload["backend"] = args.backend
            if args.description is not None:
                payload["description"] = args.description
            resp = c.post("/api/v1/batches", json=payload)
        body = assert_2xx(resp, action=f"create batch {args.name!r}")
        print(f"Created batch {args.name!r}:")
        _print_batch_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _batch_list(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        params: dict[str, Any] = {}
        if args.state is not None:
            params["state"] = args.state
        if args.limit is not None:
            params["limit"] = args.limit
        with authed_client(cfg) as c:
            resp = c.get("/api/v1/batches", params=params)
        body = assert_2xx(resp, action="list batches")
        items = body["items"]
        if args.format == "json":
            print(json.dumps(items, indent=2))
            return 0
        if not items:
            print("(no batches — run `loom eval batch create`)")
            return 0
        for it in items:
            print(
                f"{it['id']:<38}  {it['state']:<10}  "
                f"n={it.get('expected_trial_count', '?'):<5}  "
                f"{it.get('name', '')}",
            )
        # Issue #67: surface the truncation. Server default is 50;
        # without this hint a team with >50 batches saw silent loss.
        # Stderr keeps the table on stdout pipeable while still
        # warning interactive users.
        if body.get("next_cursor"):
            sys.stderr.write(
                f"(more — {len(items)} shown; pass --limit N for more "
                "or filter with --state)\n",
            )
        return 0

    return _run_with_error_handling(_body)


def _batch_show(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/batches/{args.batch_id}")
        body = assert_2xx(resp, action=f"show batch {args.batch_id!r}")
        if args.format == "json":
            print(json.dumps(body, indent=2))
        else:
            _print_batch_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _batch_cancel(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.post(f"/api/v1/batches/{args.batch_id}/cancel")
        body = assert_2xx(resp, action=f"cancel batch {args.batch_id!r}")
        print(
            f"Cancelled batch {body.get('batch_id', args.batch_id)} "
            f"(state={body.get('state', 'cancelled')}).",
        )
        return 0

    return _run_with_error_handling(_body)


# ──────────────────────────────────────────────────────────────────────
# eval trial list / show
# ──────────────────────────────────────────────────────────────────────


def _trial_list(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        params: dict[str, Any] = {}
        if args.task_id is not None:
            params["task_id"] = args.task_id
        if args.state is not None:
            params["state"] = args.state
        if args.limit is not None:
            params["limit"] = args.limit
        with authed_client(cfg) as c:
            resp = c.get("/api/v1/trials", params=params)
        body = assert_2xx(resp, action="list trials")
        items = body["items"]
        if args.format == "json":
            print(json.dumps(items, indent=2))
            return 0
        if not items:
            print("(no trials)")
            return 0
        for it in items:
            reward = it.get("aggregate_reward")
            reward_s = f"{reward:.3f}" if isinstance(reward, (int, float)) else "-"
            print(
                f"{it['id']:<38}  {it['state']:<10}  "
                f"reward={reward_s:<6}  {it.get('task_id', '')}",
            )
        # Issue #67: same truncation hint as batch list. Stderr keeps
        # stdout pipeable; interactive users still see the warning.
        if body.get("next_cursor"):
            sys.stderr.write(
                f"(more — {len(items)} shown; pass --limit N for more "
                "or filter with --state / --task-id)\n",
            )
        return 0

    return _run_with_error_handling(_body)


def _trial_show(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/trials/{args.trial_id}")
        body = assert_2xx(resp, action=f"show trial {args.trial_id!r}")
        if args.format == "json":
            print(json.dumps(body, indent=2))
        else:
            _print_trial_summary(body)
            if body.get("atif_ready") and body.get("atif_url"):
                print(f"atif_url:         {body['atif_url']}")
            if body.get("trajectory_ready") and body.get("trajectory_url"):
                print(f"trajectory_url:   {body['trajectory_url']}")
        return 0

    return _run_with_error_handling(_body)


# ──────────────────────────────────────────────────────────────────────
# Dispatch
# ──────────────────────────────────────────────────────────────────────


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom eval",
        description=(
            "Submit + inspect evaluations on the deployed Loom server. "
            "Requires `loom auth login` first. See `loom run` for "
            "running a single trial locally without a server."
        ),
    )
    sub = parser.add_subparsers(dest="eval_cmd", required=True)

    # --- run (single trial) ---
    p_run = sub.add_parser(
        "run",
        help="Submit a single trial against a task on the deployed Loom.",
    )
    p_run.add_argument(
        "--provider", required=True,
        help="Provider connection name (`loom providers list`).",
    )
    p_run.add_argument(
        "--model", required=True,
        help="Upstream model id the gateway forwards to.",
    )
    p_run.add_argument(
        "--agent", required=True,
        help="Agent name from the catalog (`GET /api/v1/agents`).",
    )
    p_run.add_argument(
        "--task", required=True,
        help="Task id (e.g. `humaneval/HumanEval/0`).",
    )
    p_run.set_defaults(handler=_run)

    # --- batch ---
    p_batch = sub.add_parser(
        "batch", help="Manage batches (create/list/show/cancel).",
    )
    batch_sub = p_batch.add_subparsers(dest="batch_cmd", required=True)

    p_bc = batch_sub.add_parser(
        "create", help="Create a batch + materialize trials.",
    )
    p_bc.add_argument("--provider", required=True)
    p_bc.add_argument("--model", required=True)
    p_bc.add_argument("--agent", required=True)
    p_bc.add_argument(
        "--benchmark", default=None,
        help=(
            "Benchmark slug — shortcut for "
            "--task-filter '{\"benchmark_id\":\"...\"}'."
        ),
    )
    p_bc.add_argument(
        "--task-filter", dest="task_filter",
        type=_load_task_filter_json, default=None,
        help=(
            "Task filter as JSON (object). Pass a literal JSON string "
            "or `@path/to/file.json` to read from disk."
        ),
    )
    p_bc.add_argument("--name", required=True, help="Batch display name.")
    p_bc.add_argument("--description", default=None)
    p_bc.add_argument(
        "--n-per-task", dest="n_per_task", type=int, default=None,
        help="Number of trials per task (1–100).",
    )
    p_bc.add_argument("--backend", default=None,
                      help="Worker backend (default: server default).")
    p_bc.set_defaults(handler=_batch_create)

    p_bl = batch_sub.add_parser("list", help="List batches.")
    p_bl.add_argument(
        "--state", default=None,
        help="Comma-separated state filter (e.g. submitted,running).",
    )
    p_bl.add_argument("--limit", type=int, default=None)
    p_bl.add_argument("--format", choices=["table", "json"], default="table")
    p_bl.set_defaults(handler=_batch_list)

    p_bs = batch_sub.add_parser("show", help="Show batch details + rollup.")
    p_bs.add_argument("batch_id", help="Batch UUID.")
    p_bs.add_argument("--format", choices=["text", "json"], default="text")
    p_bs.set_defaults(handler=_batch_show)

    p_bx = batch_sub.add_parser(
        "cancel",
        help="Cancel a batch + cascade-cancel its still-active trials.",
    )
    p_bx.add_argument("batch_id", help="Batch UUID.")
    p_bx.set_defaults(handler=_batch_cancel)

    # --- trial ---
    p_trial = sub.add_parser(
        "trial", help="Inspect trials (list/show).",
    )
    trial_sub = p_trial.add_subparsers(dest="trial_cmd", required=True)

    p_tl = trial_sub.add_parser("list", help="List trials.")
    p_tl.add_argument(
        "--task-id", dest="task_id", default=None,
        help="Filter to a specific task id.",
    )
    p_tl.add_argument(
        "--state", default=None,
        help="Comma-separated state filter.",
    )
    p_tl.add_argument("--limit", type=int, default=None)
    p_tl.add_argument("--format", choices=["table", "json"], default="table")
    p_tl.set_defaults(handler=_trial_list)

    p_ts = trial_sub.add_parser(
        "show", help="Show trial details + presigned ATIF/trajectory URLs.",
    )
    p_ts.add_argument("trial_id", help="Trial UUID.")
    p_ts.add_argument("--format", choices=["text", "json"], default="text")
    p_ts.set_defaults(handler=_trial_show)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
