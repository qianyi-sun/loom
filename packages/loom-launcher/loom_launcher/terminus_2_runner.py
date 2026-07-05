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
import shlex
import subprocess
import sys
import time
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
        started = time.monotonic()
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
            exec_result = _ExecResult(exit_code=127, output=str(exc).encode())
        else:
            exec_result = _ExecResult(
                exit_code=int(result.returncode),
                output=(result.stdout or b"") + (result.stderr or b""),
            )
        _emit(
            {
                "kind": "terminus2_exec_run",
                "cmd_excerpt": _command_excerpt(cmd),
                "exit_code": exec_result.exit_code,
                "output_len": len(exec_result.output),
                "duration_sec": round(time.monotonic() - started, 3),
            },
        )
        return exec_result

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


def _command_excerpt(cmd: list[str] | str, limit: int = 240) -> str:
    if isinstance(cmd, str):
        text = cmd
    else:
        text = shlex.join(str(part) for part in cmd)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


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
    install_script.

    Also monkey-patches `TmuxSession.get_asciinema_timestamp` — upstream
    reads a .cast file recorded by asciinema, which nothing in the Loom
    sandbox produces. It's used only for `_timestamped_markers`
    metadata (not the agent loop's correctness), so returning a
    wall-clock float keeps the loop making progress without breaking
    the AgentResult contract."""
    import time as _time

    tmux_mod = importlib.import_module("terminal_bench.terminal.tmux_session")
    _patch_asciinema_timestamp(tmux_mod, _time.time)
    container = LocalContainer()
    return tmux_mod.TmuxSession(
        session_name="loom-terminus-2",
        container=container,
    )


def _tmux_session_name(session: Any) -> str:
    name = getattr(session, "_session_name", "loom-terminus-2")
    if not isinstance(name, str) or not name:
        return "loom-terminus-2"
    return name


def _output_excerpt(result: Any, *, limit: int = 240) -> str:
    output = getattr(result, "output", b"")
    if isinstance(output, bytes):
        text = output.decode("utf-8", errors="replace")
    else:
        text = str(output)
    text = text.replace("\x00", "")
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _capture_tmux_session(session: Any) -> Any:
    return session.container.exec_run([
        "tmux",
        "capture-pane",
        "-p",
        "-t",
        _tmux_session_name(session),
    ])


def _start_and_verify_tmux_session(session: Any) -> None:
    """Start upstream TmuxSession and verify the actual tmux target exists.

    Live #445 evidence showed `session.start()` could return while every
    subsequent command against `loom-terminus-2` failed. Verify with a
    real `capture-pane`; if upstream start did not create the session,
    create the same session explicitly before allowing provider calls.
    """
    session.start()
    first_check = _capture_tmux_session(session)
    if getattr(first_check, "exit_code", 1) == 0:
        _emit({
            "kind": "terminus2_session_ready",
            "session_name": _tmux_session_name(session),
            "source": "upstream_start",
        })
        return

    name = _tmux_session_name(session)
    _emit({
        "kind": "terminus2_session_start_verify_failed",
        "session_name": name,
        "exit_code": getattr(first_check, "exit_code", None),
        "output_excerpt": _output_excerpt(first_check),
    })
    fallback_result = session.container.exec_run([
        "tmux",
        "new-session",
        "-x",
        "160",
        "-y",
        "40",
        "-d",
        "-s",
        name,
    ])
    if getattr(fallback_result, "exit_code", 1) != 0:
        _emit({
            "kind": "terminus2_session_recovery_failed",
            "session_name": name,
            "exit_code": getattr(fallback_result, "exit_code", None),
            "output_excerpt": _output_excerpt(fallback_result),
        })
        raise RuntimeError(
            f"failed to create tmux session {name!r}: "
            f"{_output_excerpt(fallback_result)}",
        )

    second_check = _capture_tmux_session(session)
    if getattr(second_check, "exit_code", 1) != 0:
        _emit({
            "kind": "terminus2_session_recovery_verify_failed",
            "session_name": name,
            "exit_code": getattr(second_check, "exit_code", None),
            "output_excerpt": _output_excerpt(second_check),
        })
        raise RuntimeError(
            f"tmux session {name!r} is still unavailable after recovery: "
            f"{_output_excerpt(second_check)}",
        )

    _emit({
        "kind": "terminus2_session_recovered",
        "session_name": name,
    })


def _patch_asciinema_timestamp(tmux_mod: Any, now: Any) -> None:
    """Replace `TmuxSession.get_asciinema_timestamp` with a wall-clock
    time stub. Idempotent via `_loom_asciinema_patched` sentinel."""
    tmux_session_cls: Any = tmux_mod.TmuxSession
    if getattr(tmux_session_cls, "_loom_asciinema_patched", False):
        return
    tmux_session_cls.get_asciinema_timestamp = lambda self: float(now())
    tmux_session_cls._loom_asciinema_patched = True


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


def _try_extract_json_object(text: str) -> str | None:
    """Return the first balanced JSON object substring inside `text`,
    or None if no such object is found. Handles the common failure
    modes for models under LiteLLM's prompt-based structured-output
    fallback:

    - Markdown fences: `\\`\\`\\`json\\n{...}\\n\\`\\`\\``.
    - Prefix narration: `Sure! Here's the JSON:\\n{...}`.
    - Trailing chatter after the JSON.

    Falls back to naive brace-matching (bracket-count balance) because
    Python's json module can't re-parse partial JSON. Ignores braces
    that appear inside JSON string literals so quoted `{`/`}` don't
    confuse the balance. Returns None (caller keeps the original text)
    when no object is found — never raises."""
    if not text:
        return None
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escape = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:idx + 1]
    return None


def _normalize_command_batch_response_json(text: str) -> str:
    """Normalize common GLM structured-output slips into Terminus's
    expected CommandBatchResponse JSON shape.

    This is intentionally conservative: if `text` is not JSON, or if it
    is a JSON value that does not look like either a command batch or a
    single terminal command, return it unchanged and let upstream
    Terminus raise its normal parse error.
    """
    try:
        payload = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return text
    if not isinstance(payload, dict):
        return text

    changed = False

    if _looks_like_command(payload):
        command, command_changed = _normalize_command(payload)
        if command is None:
            return text
        return json.dumps({
            "state_analysis": "Model returned a single command object.",
            "explanation": "Executing the command returned by the model.",
            "commands": [command],
            "is_task_complete": False,
        })

    commands = payload.get("commands")
    if isinstance(commands, list):
        normalized_commands: list[dict[str, object]] = []
        for command_raw in commands:
            command, command_changed = _normalize_command(command_raw)
            if command is None:
                return text
            normalized_commands.append(command)
            changed = changed or command_changed
        if normalized_commands != commands:
            payload = dict(payload)
            payload["commands"] = normalized_commands
            changed = True

    if "commands" in payload and "is_task_complete" not in payload:
        payload = dict(payload)
        payload["is_task_complete"] = False
        changed = True

    if "is_task_complete" in payload and isinstance(payload["is_task_complete"], str):
        lowered = payload["is_task_complete"].strip().lower()
        if lowered in {"true", "false"}:
            payload = dict(payload)
            payload["is_task_complete"] = lowered == "true"
            changed = True

    return json.dumps(payload) if changed else text


def _looks_like_command(value: dict[str, object]) -> bool:
    return "keystrokes" in value and "commands" not in value


def _normalize_command(raw: object) -> tuple[dict[str, object] | None, bool]:
    if isinstance(raw, str):
        return {
            "keystrokes": raw,
            "is_blocking": True,
            "timeout_sec": 5,
        }, True
    if not isinstance(raw, dict):
        return None, False
    if "keystrokes" not in raw:
        return None, False

    command = dict(raw)
    changed = False

    if "is_blocking" not in command:
        command["is_blocking"] = False
        changed = True
    if "timeout_sec" not in command:
        command["timeout_sec"] = 5
        changed = True
    if isinstance(command.get("is_blocking"), str):
        lowered = str(command["is_blocking"]).strip().lower()
        if lowered in {"true", "false"}:
            command["is_blocking"] = lowered == "true"
            changed = True

    return command, changed


def _prepare_litellm_response_text(text: str) -> str:
    unwrapped = _try_extract_json_object(text)
    if unwrapped is not None:
        text = unwrapped
    return _normalize_command_batch_response_json(text)


def _patch_litellm_response_extraction(litellm_mod: Any) -> None:
    """Monkey-patch upstream `LiteLLM.call` so it extracts tool-use
    arguments when the assistant's `content` is empty. This makes
    response_format=Pydantic actually work through the Anthropic dialect
    (LiteLLM's translation layer routes structured output through
    Anthropic's tool_use mechanism; upstream Terminus was written
    against the OpenAI shape where `content` carries the JSON directly).

    Idempotent: sets `_loom_patched = True` on the class after the first
    call so re-imports don't wrap twice."""
    litellm_cls: Any = litellm_mod.LiteLLM
    if getattr(litellm_cls, "_loom_patched", False):
        return

    _original_call = litellm_cls.call

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
            raw_content = _original_call(self, prompt, *args, **kwargs)
        finally:
            litellm_mod.completion = completion_fn
        content = (
            raw_content
            if isinstance(raw_content, str)
            else "" if raw_content is None else str(raw_content)
        )

        # Even when content is non-empty, Anthropic Haiku (and other
        # models under LiteLLM's prompt-based structured-output
        # fallback) sometimes wrap the JSON in markdown fences or
        # prefix it with narration. Terminus calls
        # `CommandBatchResponse.model_validate_json(response)` which
        # requires the string to BEGIN at a `{` or `[`. Strip the
        # obvious wrappers so the parse succeeds when the underlying
        # JSON is actually present.
        if content:
            content = _prepare_litellm_response_text(content)

        # Emit diagnostic so operators can see whether the patch fired
        # for this call. Written to stdout as a JSONL line so the
        # worker's `stream_stdout_jsonl` capture reads it into the
        # trajectory. `content` here is what upstream returned; the
        # value we ACTUALLY return may be substituted below.
        try:
            captured_val = captured_response.get("value")
            has_tool_calls = False
            if captured_val is not None:
                choices_v = (
                    captured_val.get("choices") if isinstance(captured_val, dict)
                    else getattr(captured_val, "choices", None)
                )
                if choices_v and len(choices_v):
                    msg_v = getattr(choices_v[0], "message", None)
                    if msg_v is None and isinstance(choices_v[0], dict):
                        msg_v = choices_v[0].get("message")
                    tc_v = getattr(msg_v, "tool_calls", None) if msg_v is not None else None
                    if tc_v is None and isinstance(msg_v, dict):
                        tc_v = msg_v.get("tool_calls")
                    has_tool_calls = bool(tc_v)
            _emit({
                "kind": "terminus2_litellm_patched_call",
                "content_empty": not bool(content),
                "content_len": len(content) if content else 0,
                "has_tool_calls": has_tool_calls,
            })
        except Exception:
            pass

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
            return _prepare_litellm_response_text(arguments)
        if arguments is not None:
            import json as _json
            return _prepare_litellm_response_text(_json.dumps(arguments))
        return content

    litellm_cls.call = _patched_call
    litellm_cls._loom_patched = True


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

    session: Any | None = None
    session_started = False
    try:
        session = _setup_tmux_session()
        _start_and_verify_tmux_session(session)
        session_started = True
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
    finally:
        if session_started and session is not None:
            try:
                session.stop()
            except Exception as exc:
                _emit({
                    "kind": "terminus2_session_stop_error",
                    "error_type": type(exc).__name__,
                    "message": str(exc)[:1000],
                })

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
