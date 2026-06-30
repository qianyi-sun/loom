"""One-shot Terminus-2 runner for the `terminus-2` adapter (#248).

The upstream `terminal-bench-core` Python package ships
`terminal_bench.agents.terminus.Terminus`, but no stable CLI entry-point.
Upstream Terminus is also designed to run OUTSIDE the trial sandbox,
driving a docker-py `Container` for `tmux` exec_run calls. Loom's
launcher model is the opposite — the adapter runs INSIDE the sandbox —
so this runner ships a tiny `LocalContainer` shim that satisfies the
exact subset of the docker-py Container API that upstream's
`TmuxSession` actually calls (just `exec_run`). Everything else stays
upstream-verbatim so any future bump of `terminal-bench-core` flows in.

Emits JSONL events on stdout; the worker captures them via
`stream_stdout_jsonl`.

The CLI surface is intentionally minimal: instruction, workdir, model,
optional max_episodes. The LLM-side wiring is `LiteLLM` with the
provider config picked up from `OPENAI_API_KEY` / `OPENAI_BASE_URL`
(set by the subprocess agent to a step-scoped JWT + the gateway URL).
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import subprocess
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class _ExecResult:
    """docker-py `ExecResult`-compatible tuple. `terminal_bench`'s
    TmuxSession reads `.exit_code` and `.output` only."""

    exit_code: int
    output: bytes


class LocalContainer:
    """Subset of docker.models.containers.Container that upstream
    TmuxSession needs (`exec_run`). Runs commands locally because we're
    already inside the sandbox.

    Upstream calls `exec_run` with either a list (for explicit argv,
    no shell) or a string (then it expects shell parsing). We respect
    both. `kwargs` are tolerated and ignored — upstream passes a few
    docker-specific ones (`stdout=`, `stderr=`, etc.) that don't apply.
    """

    def exec_run(
        self,
        cmd: list[str] | str,
        *_: Any,
        **__: Any,
    ) -> _ExecResult:
        if isinstance(cmd, str):
            argv: list[str] = ["bash", "-lc", cmd]
        else:
            argv = list(cmd)
        try:
            result = subprocess.run(
                argv,
                capture_output=True,
                check=False,
            )
        except FileNotFoundError as exc:
            return _ExecResult(exit_code=127, output=str(exc).encode())
        return _ExecResult(
            exit_code=int(result.returncode),
            output=(result.stdout or b"") + (result.stderr or b""),
        )


def _emit(payload: dict[str, object]) -> None:
    """Write a single JSONL line on stdout. Flush so the worker's
    capture loop sees it promptly."""
    sys.stdout.write(json.dumps(payload, default=_json_default) + "\n")
    sys.stdout.flush()


def _json_default(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    return repr(value)


def _propagate_dialect_env() -> None:
    """The subprocess agent only sets `OPENAI_API_KEY` /
    `OPENAI_BASE_URL` (the adapter's configured api_key_env /
    base_url_env). Upstream `LiteLLM` reads its key from `OPENAI_API_KEY`
    natively, but the LiteLLM lookup for the base URL key prefers
    `OPENAI_API_BASE`. Mirror the value across so both names point at
    the gateway."""
    base = os.environ.get("OPENAI_BASE_URL")
    if base and not os.environ.get("OPENAI_API_BASE"):
        os.environ["OPENAI_API_BASE"] = base


def _setup_tmux_session() -> Any:
    """Construct an upstream TmuxSession against the local container
    shim. Imports are deferred via importlib so loom-launcher's package
    isolation test (no `from terminal_bench import ...` at AST level)
    stays green — the dep is sandbox-only and provisioned by
    install_script."""
    tmux_mod = importlib.import_module("terminal_bench.terminal.tmux_session")
    container = LocalContainer()
    return tmux_mod.TmuxSession(
        session_name="loom-terminus-2",
        container=container,
    )


def _build_agent(model: str, max_episodes: int) -> Any:
    """Lazy-importlib for the same reason as `_setup_tmux_session`."""
    terminus_mod = importlib.import_module(
        "terminal_bench.agents.terminus",
    )
    litellm_mod = importlib.import_module("terminal_bench.llms.lite_llm")

    llm = litellm_mod.LiteLLM(model_name=model)
    return terminus_mod.Terminus(llm=llm, max_episodes=max_episodes)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="terminus-2-runner")
    parser.add_argument(
        "--model",
        required=True,
        help=(
            "LiteLLM model id. The adapter formats this as "
            "`openai/<upstream-id>` so the Loom gateway dispatches via "
            "its openai-compatible facade."
        ),
    )
    parser.add_argument(
        "--task",
        required=True,
        help="Task description / instruction passed verbatim to Terminus.",
    )
    parser.add_argument(
        "--workdir",
        required=True,
        help="Sandbox workdir (passed through for symmetry; Terminus "
        "drives tmux which inherits cwd).",
    )
    parser.add_argument(
        "--max-episodes",
        type=int,
        default=50,
        help="Upstream default in terminal-bench-core v0.1.1 is 50.",
    )
    args = parser.parse_args(argv)

    _propagate_dialect_env()

    _emit({
        "kind": "terminus2_start",
        "model": args.model,
        "max_episodes": args.max_episodes,
        "workdir": args.workdir,
    })

    try:
        session = _setup_tmux_session()
        agent = _build_agent(args.model, args.max_episodes)
        result = agent.perform_task(
            task_description=args.task,
            session=session,
        )
    except Exception as exc:
        _emit({
            "kind": "terminus2_error",
            "error_type": type(exc).__name__,
            "message": str(exc)[:1000],
            "traceback": traceback.format_exc()[-2000:],
        })
        return 1

    _emit({
        "kind": "terminus2_end",
        "total_input_tokens": getattr(result, "total_input_tokens", 0),
        "total_output_tokens": getattr(result, "total_output_tokens", 0),
        "failure_mode": str(getattr(result, "failure_mode", None)),
        "marker_count": len(getattr(result, "timestamped_markers", []) or []),
    })
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised via subprocess
    raise SystemExit(main())
