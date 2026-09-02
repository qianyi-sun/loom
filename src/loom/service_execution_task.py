"""Small in-Pod task runner used by the automatic Nebius compiler."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path, PurePosixPath
from typing import Any

from loom.agent.litellm import _render_artifact_body


class ServiceExecutionTaskError(RuntimeError):
    pass


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value:
        raise ServiceExecutionTaskError(f"required environment {name} is missing")
    return value


def _safe_workspace_path(root: Path, raw: str) -> Path:
    rel = PurePosixPath(raw)
    if rel.is_absolute() or not rel.parts or ".." in rel.parts:
        raise ServiceExecutionTaskError(f"unsafe workspace path: {raw!r}")
    target = root.joinpath(*rel.parts)
    if not target.resolve().is_relative_to(root.resolve()):
        raise ServiceExecutionTaskError(f"workspace path escapes root: {raw!r}")
    return target


def _json_environment(name: str, expected_type: type[Any]) -> Any:
    try:
        value = json.loads(_required_environment(name))
    except json.JSONDecodeError as exc:
        raise ServiceExecutionTaskError(f"{name} is not valid JSON") from exc
    if not isinstance(value, expected_type):
        raise ServiceExecutionTaskError(f"{name} has the wrong JSON shape")
    return value


def _completion_content(response: dict[str, Any]) -> tuple[str, str]:
    choices = response.get("choices")
    if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
        raise ServiceExecutionTaskError("gateway response has no single completion choice")
    choice = choices[0]
    message = choice.get("message")
    if not isinstance(message, dict):
        raise ServiceExecutionTaskError("gateway completion choice has no message")
    content = message.get("content")
    if not isinstance(content, str):
        raise ServiceExecutionTaskError("gateway completion content is not text")
    finish_reason = choice.get("finish_reason")
    if not isinstance(finish_reason, str):
        raise ServiceExecutionTaskError("gateway completion has no finish reason")
    return content, finish_reason


def run_direct_completion(*, workspace: Path = Path("/workspace")) -> None:
    instruction_path = _safe_workspace_path(
        workspace,
        _required_environment("LOOM_TASK_INSTRUCTION_FILE"),
    )
    instruction = instruction_path.read_text(encoding="utf-8")
    artifact_paths = _json_environment("LOOM_TASK_ARTIFACTS_JSON", list)
    request_params = _json_environment("LOOM_TASK_REQUEST_PARAMS_JSON", dict)
    model = _required_environment("LOOM_TASK_MODEL")
    gateway = _required_environment("LOOM_GATEWAY_URL").rstrip("/")
    messages: list[dict[str, str]] = [{"role": "user", "content": instruction}]
    final = ""
    for _turn in range(8):
        payload = {
            **request_params,
            "model": model,
            "messages": messages,
            "stream": False,
        }
        request = urllib.request.Request(
            gateway + "/v1/chat/completions",
            data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                raw = response.read(16 * 1024 * 1024 + 1)
        except urllib.error.HTTPError as exc:
            detail = exc.read(4096).decode("utf-8", errors="replace")
            raise ServiceExecutionTaskError(
                f"gateway completion failed with HTTP {exc.code}: {detail}"
            ) from exc
        if len(raw) > 16 * 1024 * 1024:
            raise ServiceExecutionTaskError("gateway response exceeds 16 MiB")
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ServiceExecutionTaskError("gateway response is not JSON") from exc
        if not isinstance(parsed, dict):
            raise ServiceExecutionTaskError("gateway response has the wrong shape")
        final, finish_reason = _completion_content(parsed)
        messages.append({"role": "assistant", "content": final})
        if finish_reason == "stop":
            break
    else:
        raise ServiceExecutionTaskError("direct completion exhausted eight turns")

    for raw_path in artifact_paths:
        if not isinstance(raw_path, str):
            raise ServiceExecutionTaskError("artifact paths must be strings")
        target = _safe_workspace_path(workspace, raw_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_render_artifact_body(final, raw_path), encoding="utf-8")
    print(final)


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments != ["direct-completion"]:
        print("usage: python -m loom.service_execution_task direct-completion", file=sys.stderr)
        return 2
    try:
        run_direct_completion()
    except (OSError, ServiceExecutionTaskError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
