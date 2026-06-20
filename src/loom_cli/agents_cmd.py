"""`loom agents` command surface."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable

from loom_cli.agent_runtime_readiness import (
    AgentRuntimeAuditItem,
    build_runtime_audit_items,
    render_runtime_audit_json,
    render_runtime_audit_table,
)
from loom_cli.agent_runtime_smoke import (
    AgentRuntimeSmokeItem,
    render_agent_smoke_json,
    render_agent_smoke_table,
    run_agent_smoke_matrix,
)


def _add_audit_runtime_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--image",
        default=None,
        help="Trial sandbox image to audit, e.g. python:3.11-slim.",
    )
    p.add_argument(
        "--agent",
        dest="agents",
        action="append",
        default=None,
        help="Agent name to audit. Repeat to limit the matrix.",
    )
    p.add_argument("--json", dest="as_json", action="store_true")
    p.add_argument(
        "--timeout-sec",
        type=float,
        default=20.0,
        help="Per dependency check timeout in seconds.",
    )


def _add_smoke_runtime_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--image",
        default=None,
        help="Trial sandbox image to smoke, e.g. loom-agent-sandbox:dev.",
    )
    p.add_argument(
        "--agent",
        dest="agents",
        action="append",
        default=None,
        help="Agent name to smoke. Repeat to limit the matrix.",
    )
    p.add_argument("--json", dest="as_json", action="store_true")
    p.add_argument(
        "--timeout-sec",
        type=float,
        default=60.0,
        help="Per-agent smoke timeout in seconds.",
    )
    p.add_argument(
        "--model-name",
        default="smoke-model",
        help="Synthetic model name passed to model-backed agents.",
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="loom agents")
    sub = p.add_subparsers(dest="subcmd", required=True)
    _add_audit_runtime_args(
        sub.add_parser(
            "audit-runtime",
            help="Check displayed agent runtime dependencies in a sandbox image.",
        )
    )
    _add_smoke_runtime_args(
        sub.add_parser(
            "smoke-runtime",
            help="Run minimal end-to-end smoke trials for displayed agents.",
        )
    )
    return p


def _not_ready(items: list[AgentRuntimeAuditItem]) -> bool:
    return any(item.readiness_state != "ready" for item in items)


def _smoke_failed(items: list[AgentRuntimeSmokeItem]) -> bool:
    return any(item.smoke_state != "passed" for item in items)


def _cmd_audit_runtime(args: argparse.Namespace) -> int:
    if not args.image:
        print(
            "error: audit-runtime requires --image",
            file=sys.stderr,
        )
        return 2
    try:
        items = build_runtime_audit_items(
            image=args.image,
            agents=args.agents,
            timeout_sec=args.timeout_sec,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.as_json:
        print(render_runtime_audit_json(items))
    else:
        print(render_runtime_audit_table(items))
    return 1 if _not_ready(items) else 0


def _cmd_smoke_runtime(args: argparse.Namespace) -> int:
    if not args.image:
        print(
            "error: smoke-runtime requires --image",
            file=sys.stderr,
        )
        return 2
    try:
        items = run_agent_smoke_matrix(
            image=args.image,
            agents=args.agents,
            timeout_sec=args.timeout_sec,
            model_name=args.model_name,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.as_json:
        print(render_agent_smoke_json(items))
    else:
        print(render_agent_smoke_table(items))
    return 1 if _smoke_failed(items) else 0


_DISPATCH: dict[str, Callable[[argparse.Namespace], int]] = {
    "audit-runtime": _cmd_audit_runtime,
    "smoke-runtime": _cmd_smoke_runtime,
}


def dispatch(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    handler = _DISPATCH.get(args.subcmd)
    if handler is None:  # pragma: no cover - argparse enforces choices.
        parser.error(f"unknown command {args.subcmd!r}")
    return handler(args)
