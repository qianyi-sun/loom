from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Any

from loom.service_execution_task import run_direct_completion


class _Response:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._body = json.dumps(payload).encode()

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, _limit: int) -> bytes:
        return self._body


def test_direct_completion_uses_provider_native_model_and_writes_artifact(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    (tmp_path / "instruction.md").write_text("Return a greeting", encoding="utf-8")
    monkeypatch.setenv("LOOM_TASK_INSTRUCTION_FILE", "instruction.md")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", '["answer.txt"]')
    monkeypatch.setenv("LOOM_TASK_REQUEST_PARAMS_JSON", '{"temperature":0.2}')
    monkeypatch.setenv("LOOM_TASK_MODEL", "openai/gpt-5")
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://gateway-proxy")
    requests: list[urllib.request.Request] = []

    def _urlopen(request: urllib.request.Request, *, timeout: int) -> _Response:
        requests.append(request)
        assert timeout == 120
        return _Response(
            {
                "choices": [
                    {
                        "message": {"role": "assistant", "content": "hello"},
                        "finish_reason": "stop",
                    }
                ]
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)

    run_direct_completion(workspace=tmp_path)

    assert (tmp_path / "answer.txt").read_text(encoding="utf-8") == "hello"
    assert len(requests) == 1
    request = requests[0]
    assert request.full_url == "http://gateway-proxy/v1/chat/completions"
    assert request.data is not None
    assert json.loads(request.data)["model"] == "openai/gpt-5"
