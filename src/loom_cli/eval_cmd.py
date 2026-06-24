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
import shlex
import sys
from pathlib import Path
from typing import Any, cast

from loom.security.redaction import redact_mapping
from loom_cli.providers_cmd import (
    _resolve_by_name,
    _run_with_error_handling,
)
from loom_cli.server_client import (
    assert_2xx,
    assert_2xx_response,
    authed_client,
    require_logged_in,
)
from loom_service.agent_catalog import get_agent

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


def _build_agent_model(
    connection_type: str, model_name: str,
    *, agent_provider_override: str | None = None,
) -> dict[str, Any]:
    """Construct the `agent_model` dict the worker hands to the agent.

    Provider resolution priority:
    1. `agent_provider_override` (the operator's explicit
       `--agent-provider` flag) — wins when set.
    2. `_TYPE_TO_AGENT_PROVIDER[connection_type]` — sensible default.
    3. Fallback `"openai"` for unknown types.

    Why the override matters (#69): connections of type
    `openai-compatible` or `custom` pointing at Together / Fireworks /
    Mistral need `agent_model.provider="together"` (etc.) for the
    legacy rate-card lookup to find pricing entries. Without the
    override every such connection became `provider="openai"` and
    silently lost cost attribution. The facade (PR #64) uses
    operator-supplied pricing so this matters mostly for the legacy
    `/v1/chat/completions` path — but operators upgrading partial
    deployments still hit it.
    """
    if agent_provider_override:
        provider = agent_provider_override
    else:
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


def _agent_needs_model(agent_name: str) -> tuple[bool | None, str | None]:
    agent = get_agent(agent_name)
    if agent is None:
        return None, (
            f"error: unknown agent {agent_name!r}. "
            "Run `loom agents list` or check GET /api/v1/agents.\n"
        )
    return agent.needs_model, None


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
    if item.get("llm_calls_count") is not None:
        print(f"llm_calls:        {item['llm_calls_count']}")
    if (
        item.get("total_prompt_tokens") is not None
        or item.get("total_completion_tokens") is not None
    ):
        print(
            "tokens:           "
            f"prompt={item.get('total_prompt_tokens', 0)} "
            f"completion={item.get('total_completion_tokens', 0)}",
        )
    if item.get("submitted_at"):
        print(f"submitted_at:     {item['submitted_at']}")
    if item.get("finished_at"):
        print(f"finished_at:     {item['finished_at']}")
    if item.get("failure_reason"):
        print(f"failure_reason:   {item['failure_reason']}")


def _dump_json(data: Any) -> None:
    """Print JSON safely for public CLI output."""
    print(json.dumps(redact_mapping(data), indent=2))


def _print_debug_evidence_summary(evidence: dict[str, Any]) -> None:
    entity = evidence.get("entity") if isinstance(evidence, dict) else {}
    lifecycle = evidence.get("lifecycle") if isinstance(evidence, dict) else {}
    failure = evidence.get("failure") if isinstance(evidence, dict) else {}
    provider = evidence.get("provider") if isinstance(evidence, dict) else {}
    next_actions = evidence.get("next_actions")
    entity_type = entity.get("type") if isinstance(entity, dict) else "unknown"
    entity_id = entity.get("id") if isinstance(entity, dict) else "unknown"
    print(f"debug_evidence:        {entity_type}")
    print(f"id:                    {entity_id}")
    if isinstance(lifecycle, dict):
        if lifecycle.get("state") is not None:
            print(f"state:                 {lifecycle['state']}")
        terminal = lifecycle.get("terminal_status")
        if terminal is not None:
            print(f"terminal_status:       {terminal}")
        attempts = lifecycle.get("attempt_count")
        if attempts is not None:
            print(f"attempt_count:         {attempts}")
    if isinstance(failure, dict):
        print(f"reason_code:           {failure.get('reason_code', 'unknown')}")
        print(f"category:              {failure.get('category', 'unknown')}")
        print(f"attribution:           {failure.get('attribution', 'unknown')}")
        if failure.get("message"):
            print(f"message:               {failure['message']}")
    if isinstance(provider, dict):
        print(f"llm_calls:             {provider.get('llm_calls_count', 0)}")
        models = provider.get("models")
        if isinstance(models, list) and models:
            print("models:                " + ", ".join(str(m) for m in models))
    if isinstance(next_actions, list) and next_actions:
        print("next_actions:")
        for action in next_actions:
            print(f"  - {action}")


def _format_ratio(value: Any) -> str:
    if isinstance(value, int | float):
        return f"{value:.0%}"
    return "-"


def _print_diagnosis_report(report: dict[str, Any]) -> None:
    entity = report.get("entity") if isinstance(report, dict) else {}
    primary = report.get("primary_cause") if isinstance(report, dict) else {}
    evidence = report.get("evidence")
    actions = report.get("next_actions")
    clusters = report.get("reason_clusters")
    entity_type = entity.get("type") if isinstance(entity, dict) else "unknown"
    entity_id = entity.get("id") if isinstance(entity, dict) else "unknown"
    print(f"Diagnosis: {entity_type}")
    print(f"id:        {entity_id}")
    summary = report.get("summary")
    if isinstance(summary, str) and summary:
        print()
        print(summary)
    if isinstance(primary, dict):
        print()
        print("Primary cause:")
        print(f"  reason_code:     {primary.get('reason_code', 'unknown')}")
        print(f"  category:        {primary.get('category', 'unknown')}")
        print(f"  attribution:     {primary.get('attribution', 'unknown')}")
        print(f"  confidence:      {primary.get('confidence', 'unknown')}")
        affected = primary.get("affected_trials")
        if affected is not None:
            print(
                "  affected:        "
                f"{affected} ({_format_ratio(primary.get('affected_ratio'))})",
            )
    impact = report.get("impact")
    if isinstance(impact, str) and impact:
        print()
        print("Impact:")
        print(f"  {impact}")
    if isinstance(evidence, list) and evidence:
        print()
        print("Evidence:")
        for item in evidence:
            print(f"  - {item}")
    if isinstance(clusters, list) and clusters:
        print()
        print("Reason clusters:")
        for cluster in clusters:
            if not isinstance(cluster, dict):
                continue
            representative = cluster.get("representative_trial_id")
            suffix = f" representative={representative}" if representative else ""
            print(
                "  - "
                f"{cluster.get('reason_code', 'unknown')}: "
                f"{cluster.get('count', 0)} "
                f"({_format_ratio(cluster.get('affected_ratio'))})"
                f"{suffix}",
            )
    if isinstance(actions, list) and actions:
        print()
        print("Next actions:")
        for index, action in enumerate(actions, start=1):
            if isinstance(action, dict):
                label = action.get("label", "Action")
                command = action.get("command")
                action_name = action.get("action")
                if command:
                    print(f"  {index}. {label}: {command}")
                elif action_name:
                    print(f"  {index}. {label} ({action_name})")
                else:
                    print(f"  {index}. {label}")
            else:
                print(f"  {index}. {action}")


def _run(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            conn = _resolve_by_name(c, args.provider)
            trial_config: dict[str, Any] = {
                "agent_name": args.agent,
                "agent_model": _build_agent_model(
                    conn["type"], args.model,
                    agent_provider_override=args.agent_provider,
                ),
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
        body.setdefault("task_id", args.task)
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
    if item.get("failure_reason"):
        print(f"failure_reason:        {item['failure_reason']}")
    if item.get("failure_message"):
        print(f"failure_message:       {item['failure_message']}")
    fanout_errors = item.get("fanout_errors") or []
    if fanout_errors:
        print(f"fanout_errors:         {len(fanout_errors)}")
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
    if item.get("llm_calls_count") is not None:
        print(f"llm_calls:             {item['llm_calls_count']}")
    if (
        item.get("total_prompt_tokens") is not None
        or item.get("total_completion_tokens") is not None
    ):
        print(
            "tokens:                "
            f"prompt={item.get('total_prompt_tokens', 0)} "
            f"completion={item.get('total_completion_tokens', 0)}",
        )
    if item.get("created_at"):
        print(f"created_at:            {item['created_at']}")
    if item.get("finished_at"):
        print(f"finished_at:           {item['finished_at']}")


def _batch_create(args: argparse.Namespace) -> int:
    def _body() -> int:
        needs_model, agent_err = _agent_needs_model(args.agent)
        if agent_err is not None:
            sys.stderr.write(agent_err)
            return 2
        assert needs_model is not None

        if needs_model:
            missing = [
                flag for flag, value in (
                    ("--provider", args.provider),
                    ("--model", args.model),
                ) if not value
            ]
            if missing:
                message = (
                    f"error: agent {args.agent!r} requires --provider and "
                    "--model for batch creation; missing "
                    + ", ".join(missing)
                    + ".\n"
                )
                sys.stderr.write(message)
                return 2
        elif args.provider or args.model or args.agent_provider:
            sys.stderr.write(
                f"error: agent {args.agent!r} does not take a model; omit "
                "--provider, --model, and --agent-provider.\n",
            )
            return 2

        cfg = require_logged_in()
        with authed_client(cfg) as c:
            trial_config: dict[str, Any] = {
                "agent_name": args.agent,
                "agent_model": None,
            }
            conn: dict[str, Any] | None = None
            if needs_model:
                conn = _resolve_by_name(c, args.provider)
                trial_config["agent_model"] = _build_agent_model(
                    conn["type"], args.model,
                    agent_provider_override=args.agent_provider,
                )
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
            }
            if conn is not None:
                payload["provider_connection_id"] = conn["id"]
                payload["provider_model_id"] = args.model
            if args.n_per_task is not None:
                payload["n_per_task"] = args.n_per_task
            if args.backend is not None:
                payload["backend"] = args.backend
            if args.description is not None:
                payload["description"] = args.description
            resp = c.post("/api/v1/batches", json=payload)
        body = assert_2xx(resp, action=f"create batch {args.name!r}")
        if body.get("name") is None:
            body = {**body, "name": args.name}
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
            _dump_json(items)
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
            _dump_json(body)
        else:
            _print_batch_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _batch_debug(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/batches/{args.batch_id}/debug")
        body = assert_2xx(
            resp,
            action=f"fetch batch debug evidence {args.batch_id!r}",
        )
        if args.format == "json":
            _dump_json(body)
        else:
            _print_debug_evidence_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _diagnose_batch(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/batches/{args.batch_id}/diagnosis")
        body = assert_2xx(
            resp,
            action=f"fetch batch diagnosis {args.batch_id!r}",
        )
        if args.format == "json":
            _dump_json(body)
        else:
            _print_diagnosis_report(body)
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
            _dump_json(items)
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
            _dump_json(body)
        else:
            _print_trial_summary(body)
            _print_trial_download_commands(body, args.trial_id)
        return 0

    return _run_with_error_handling(_body)


def _trial_debug(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/trials/{args.trial_id}/debug")
        body = assert_2xx(
            resp,
            action=f"fetch trial debug evidence {args.trial_id!r}",
        )
        if args.format == "json":
            _dump_json(body)
        else:
            _print_debug_evidence_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _diagnose_trial(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/trials/{args.trial_id}/diagnosis")
        body = assert_2xx(
            resp,
            action=f"fetch trial diagnosis {args.trial_id!r}",
        )
        if args.format == "json":
            _dump_json(body)
        else:
            _print_diagnosis_report(body)
        return 0

    return _run_with_error_handling(_body)


def _print_trial_download_commands(
    body: dict[str, Any], trial_id: str,
) -> None:
    commands: list[tuple[str, str]] = []
    if body.get("atif_ready"):
        commands.append((
            "atif",
            f"loom eval trial download {shlex.quote(trial_id)} "
            "--kind atif",
        ))
    if body.get("trajectory_ready"):
        commands.append((
            "trajectory",
            f"loom eval trial download {shlex.quote(trial_id)} "
            "--kind trajectory",
        ))
    artifacts = body.get("artifacts")
    if isinstance(artifacts, list):
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            key = artifact.get("key")
            if not isinstance(key, str) or not key:
                continue
            label = str(artifact.get("path") or key)
            commands.append((
                f"artifact {label}",
                f"loom eval trial download {shlex.quote(trial_id)} "
                f"--kind artifact --artifact-key {shlex.quote(key)}",
            ))
    if not commands:
        return
    print("downloads:")
    for label, command in commands:
        print(f"  {label:<16} {command}")


def _trial_download(args: argparse.Namespace) -> int:
    def _body() -> int:
        if args.kind == "artifact" and not args.artifact_key:
            sys.stderr.write(
                "error: --kind artifact requires --artifact-key.\n",
            )
            return 2
        if args.kind != "artifact" and args.artifact_key:
            sys.stderr.write(
                "error: --artifact-key is only valid with --kind artifact.\n",
            )
            return 2

        cfg = require_logged_in()
        params: dict[str, Any] | None = None
        if args.kind == "atif":
            path = f"/api/v1/trials/{args.trial_id}/atif"
            default_name = f"{args.trial_id}-atif.json"
        elif args.kind == "trajectory":
            path = f"/api/v1/trials/{args.trial_id}/trajectory/download"
            default_name = f"{args.trial_id}-events.jsonl"
        else:
            path = f"/api/v1/trials/{args.trial_id}/artifacts/download"
            params = {"key": args.artifact_key}
            default_name = Path(args.artifact_key).name or (
                f"{args.trial_id}-artifact"
            )

        output = Path(args.output) if args.output else Path(default_name)
        with authed_client(cfg) as c:
            resp = c.get(path, params=params)
        assert_2xx_response(
            resp, action=f"download {args.kind} for trial {args.trial_id!r}",
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(resp.content)
        print(f"Downloaded {args.kind} to {output}")
        return 0

    return _run_with_error_handling(_body)


def _usage(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        params: dict[str, Any] = {
            "start": args.start,
            "end": args.end,
            "group_by": args.group_by,
        }
        if args.team_id is not None:
            params["team_id"] = args.team_id
        with authed_client(cfg) as c:
            resp = c.get("/api/v1/usage", params=params)
        body = assert_2xx(resp, action="fetch usage")
        if args.format == "json":
            _dump_json(body)
            return 0
        buckets = body.get("buckets") or []
        if body.get("degraded"):
            print("(usage unavailable: server returned degraded=true)")
            return 0
        if not buckets:
            print("(no usage in range)")
            return 0
        print("start_at                      trials  success  failed  cost_usd  in_tok  out_tok")
        for bucket in buckets:
            print(
                f"{bucket.get('start_at', '')!s:<29} "
                f"{bucket.get('trial_count', 0):>6} "
                f"{bucket.get('succeeded_count', 0):>8} "
                f"{bucket.get('failed_count', 0):>7} "
                f"{bucket.get('total_cost_usd', 0):>8} "
                f"{bucket.get('llm_input_tokens', 0):>7} "
                f"{bucket.get('llm_output_tokens', 0):>8}",
            )
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
    p_run.add_argument(
        "--agent-provider", dest="agent_provider", default=None,
        help=(
            "Override the agent_model.provider derived from the "
            "connection type. Use this for openai-compatible "
            "connections to Together / Fireworks / Mistral / etc. "
            "(e.g. --agent-provider together). When unset, the "
            "connection's type maps to a default (openai-compatible "
            "→ openai, anthropic → anthropic, google → google, "
            "custom → openai)."
        ),
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
    p_bc.add_argument(
        "--provider",
        default=None,
        help=(
            "Provider connection name. Required for agents that call a "
            "model; omit for no-model agents such as oracle."
        ),
    )
    p_bc.add_argument(
        "--model",
        default=None,
        help=(
            "Upstream model id. Required for agents that call a model; "
            "omit for no-model agents such as oracle."
        ),
    )
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
    p_bc.add_argument(
        "--agent-provider", dest="agent_provider", default=None,
        help="See `loom eval run --agent-provider`.",
    )
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

    p_bd = batch_sub.add_parser(
        "debug",
        help="Fetch structured debug evidence for a batch.",
    )
    p_bd.add_argument("batch_id", help="Batch UUID.")
    p_bd.add_argument("--format", choices=["text", "json"], default="text")
    p_bd.set_defaults(handler=_batch_debug)

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
        "show", help="Show trial details + copyable download commands.",
    )
    p_ts.add_argument("trial_id", help="Trial UUID.")
    p_ts.add_argument("--format", choices=["text", "json"], default="text")
    p_ts.set_defaults(handler=_trial_show)

    p_tdebug = trial_sub.add_parser(
        "debug",
        help="Fetch structured debug evidence for a trial.",
    )
    p_tdebug.add_argument("trial_id", help="Trial UUID.")
    p_tdebug.add_argument("--format", choices=["text", "json"], default="text")
    p_tdebug.set_defaults(handler=_trial_debug)

    p_td = trial_sub.add_parser(
        "download",
        help="Download a trial ATIF, trajectory, or artifact through the public API.",
    )
    p_td.add_argument("trial_id", help="Trial UUID.")
    p_td.add_argument(
        "--kind",
        choices=["atif", "trajectory", "artifact"],
        required=True,
        help="Object to download.",
    )
    p_td.add_argument(
        "--artifact-key",
        default=None,
        help="Artifact key from `loom eval trial show`; required for --kind artifact.",
    )
    p_td.add_argument(
        "--output",
        default=None,
        help="Destination path. Defaults to a filename derived from the trial/key.",
    )
    p_td.set_defaults(handler=_trial_download)

    # --- diagnose ---
    p_diag = sub.add_parser(
        "diagnose",
        help="Fetch human-readable diagnosis reports for batches or trials.",
    )
    diag_sub = p_diag.add_subparsers(dest="diagnose_cmd", required=True)

    p_diag_batch = diag_sub.add_parser(
        "batch",
        help="Diagnose a batch failure summary and reason clusters.",
    )
    p_diag_batch.add_argument("batch_id", help="Batch UUID.")
    p_diag_batch.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
    )
    p_diag_batch.set_defaults(handler=_diagnose_batch)

    p_diag_trial = diag_sub.add_parser(
        "trial",
        help="Diagnose a trial failure or terminal outcome.",
    )
    p_diag_trial.add_argument("trial_id", help="Trial UUID.")
    p_diag_trial.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
    )
    p_diag_trial.set_defaults(handler=_diagnose_trial)

    p_usage = sub.add_parser(
        "usage",
        help="Show usage and cost rollups from the public API.",
    )
    p_usage.add_argument("--start", required=True, help="Start date YYYY-MM-DD.")
    p_usage.add_argument("--end", required=True, help="End date YYYY-MM-DD.")
    p_usage.add_argument(
        "--group-by",
        choices=["day", "week", "month"],
        default="day",
    )
    p_usage.add_argument("--team-id", default=None)
    p_usage.add_argument("--format", choices=["table", "json"], default="table")
    p_usage.set_defaults(handler=_usage)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
