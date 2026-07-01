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
    TmuxSession needs. Runs commands locally because we're already
    inside the sandbox.

    Methods implemented:
    - `exec_run(cmd, ...)` — always. Upstream calls with either a list
      (explicit argv, no shell) or a string (shell parsing).
    - `put_archive(container_dir, tar_bytes)` — used by upstream's
      `DockerComposeManager.copy_to_container` (called from
      `TmuxSession.copy_to_container`) to inject the
      `get-asciinema-timestamp.sh` helper. Since we're already inside
      the sandbox, extraction is just a local `tarfile.extractall`
      into `container_dir`. Returns True to match docker-py's shape.

    `**kwargs` on every method are tolerated and ignored — upstream
    passes a few docker-specific ones (`stdout=`, `stderr=`, etc.)
    that don't apply.
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

    @staticmethod
    def put_archive(
        container_dir: str,
        data: bytes,
        **_: Any,
    ) -> bool:
        """Extract the tar `data` into `container_dir` on the local
        filesystem. docker-py's method uploads into the target
        container; since we ARE the target container, this is just
        local extraction. Returns True (docker-py convention) on
        success, False on failure — never raises so upstream's
        `DockerComposeManager` treats it as a docker-py Container."""
        import io
        import tarfile

        try:
            target = Path(container_dir)
            target.mkdir(parents=True, exist_ok=True)
            with tarfile.open(fileobj=io.BytesIO(data), mode="r|*") as tf:
                tf.extractall(path=target)
        except Exception:
            return False
        return True


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
    """The subprocess agent only sets the adapter's configured
    `api_key_env` / `base_url_env`. LiteLLM's model-family clients
    read their base URL from a slightly different env var:
    `OPENAI_API_BASE` (openai family) and `ANTHROPIC_API_BASE`
    (anthropic family). Mirror both so LiteLLM finds the gateway
    regardless of which dialect the adapter is wired for."""
    for base_env, mirror_env in (
        ("OPENAI_BASE_URL", "OPENAI_API_BASE"),
        ("ANTHROPIC_BASE_URL", "ANTHROPIC_API_BASE"),
    ):
        base = os.environ.get(base_env)
        if base and not os.environ.get(mirror_env):
            os.environ[mirror_env] = base


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
    """Lazy-importlib for the same reason as `_setup_tmux_session`.

    Also wraps upstream LiteLLM.call to fix the tool-use structured-output
    extraction bug: for Anthropic response_format calls, LiteLLM stores
    the parsed JSON in `message.tool_calls[0].function.arguments`, not
    `message.content`. Upstream `LiteLLM.call` returns
    `choices[0].message.content` unconditionally, which is empty for
    Anthropic tool-use responses. Terminus then throws ParseError. See
    `_patched_litellm_call` below."""
    terminus_mod = importlib.import_module(
        "terminal_bench.agents.terminus",
    )
    litellm_mod = importlib.import_module("terminal_bench.llms.lite_llm")

    _patch_litellm_response_extraction(litellm_mod)

    llm = litellm_mod.LiteLLM(model_name=model)
    return terminus_mod.Terminus(llm=llm, max_episodes=max_episodes)


def _patch_litellm_response_extraction(litellm_mod: Any) -> None:
    """Monkey-patch upstream `LiteLLM.call` so it extracts tool-use
    arguments when the assistant's `content` is empty. This makes
    response_format=Pydantic actually work through the Anthropic dialect
    (LiteLLM's translation layer routes structured output through
    Anthropic's tool_use mechanism; upstream Terminus was written
    against the OpenAI shape where `content` carries the JSON directly).

    Idempotent: sets `_loom_patched = True` on the class after the first
    call so re-imports don't wrap twice."""
    LiteLLMCls: Any = litellm_mod.LiteLLM
    if getattr(LiteLLMCls, "_loom_patched", False):
        return

    _original_call = LiteLLMCls.call

    def _patched_call(self: Any, prompt: str, *args: Any, **kwargs: Any) -> str:
        # Run the original wrapper, catching AttributeError / TypeError
        # / IndexError raised by our own extraction fallback below is
        # NOT the goal — we want the ORIGINAL error semantics for real
        # failures, and only substitute the tool-use path when the
        # returned string is empty (the classic Anthropic tool-use +
        # structured-output symptom).
        completion_fn: Any = litellm_mod.completion

        captured_response: dict[str, Any] = {}

        def _wrapper(**call_kwargs: Any) -> Any:
            result = completion_fn(**call_kwargs)
            captured_response["value"] = result
            return result

        litellm_mod.completion = _wrapper
        try:
            content = _original_call(self, prompt, *args, **kwargs)
        finally:
            litellm_mod.completion = completion_fn

        if content:
            return content

        # Empty content — check for tool_use payload. LiteLLM's
        # response objects support BOTH dict-style `["choices"]` and
        # attribute-style `.choices` traversal — handle whichever
        # actually works for the layer at hand (Choice objects have
        # `.message` as an attribute, ModelResponse dicts have it as
        # a key). Belt-and-braces because different LiteLLM versions
        # return different shapes.
        response = captured_response.get("value")
        if response is None:
            return content

        def _get(container: Any, key: Any) -> Any:
            if isinstance(container, dict):
                return container.get(key)
            try:
                return container[key]
            except (KeyError, IndexError, TypeError):
                if isinstance(key, str):
                    return getattr(container, key, None)
                return None

        choices = _get(response, "choices")
        if not choices:
            return content
        choice = choices[0] if len(choices) else None
        if choice is None:
            return content
        message = _get(choice, "message")
        if message is None:
            return content

        tool_calls = _get(message, "tool_calls")
        if not tool_calls:
            return content

        first = tool_calls[0]
        function = _get(first, "function")
        arguments = _get(function, "arguments") if function is not None else None

        if isinstance(arguments, str) and arguments.strip():
            return arguments
        if arguments is not None:
            import json as _json
            return _json.dumps(arguments)
        return content

    LiteLLMCls.call = _patched_call
    LiteLLMCls._loom_patched = True


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
