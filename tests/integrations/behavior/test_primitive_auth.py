from __future__ import annotations

import os
import time
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import jwt
import pytest

from loom.integrations.behavior.provider import (
    PRIMITIVE_DISPATCH_LIMIT,
    PRIMITIVE_STEP_ID,
    PipelineAnthropicClient,
    PipelineProviderAuthError,
    ProviderAttemptBudgetExhaustedError,
    RotatingPipelineStepJwtReader,
)

DIGEST = "sha256:" + "d" * 64


def _token(attempt_id: UUID, marker: str) -> str:
    now = int(time.time())
    body = jwt.encode(
        {
            "iss": "loom-control-plane",
            "sub": "step-session",
            "execution_attempt_id": str(attempt_id),
            "subject_kind": "execution_attempt",
            "step_id": PRIMITIVE_STEP_ID,
            "binding_sha256": DIGEST,
            "marker": marker,
            "iat": now,
            "exp": now + 600,
            "scopes": ["llm:call"],
        },
        "test-only-signing-key-at-least-32-bytes",
        algorithm="HS256",
    )
    return f"loom_step_{body}"


def _install(path: Path, token: str) -> None:
    replacement = path.with_suffix(".next")
    replacement.write_text(token, encoding="utf-8")
    replacement.chmod(0o400)
    os.replace(replacement, path)


def _client(tmp_path: Path) -> tuple[PipelineAnthropicClient, Path, UUID]:
    attempt_id = uuid4()
    path = tmp_path / "step-jwt"
    _install(path, _token(attempt_id, "first"))
    reader = RotatingPipelineStepJwtReader(
        path,
        attempt_id=attempt_id,
        step_id=PRIMITIVE_STEP_ID,
        binding_sha256=DIGEST,
    )
    client = PipelineAnthropicClient(
        messages_url="https://gateway.internal/v1/messages",
        token_reader=reader,
        attempt_id=attempt_id,
        binding_sha256=DIGEST,
    )
    return client, path, attempt_id


def test_every_primitive_request_rereads_token_and_uses_gateway_only(tmp_path: Path) -> None:
    client, path, attempt_id = _client(tmp_path)
    observed: list[tuple[str, str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed.append(
            (
                str(request.url),
                request.headers["authorization"],
                request.headers["x-loom-execution-attempt-id"],
            )
        )
        return httpx.Response(200, json={"id": f"msg-{len(observed)}"})

    client._http.close()
    client._http = httpx.Client(transport=httpx.MockTransport(handler))
    first = _token(attempt_id, "first")
    second = _token(attempt_id, "second")
    _install(path, first)
    assert client.messages.create(model="claude-test", max_tokens=1, messages=[])["id"]
    _install(path, second)
    assert client.messages.create(model="claude-test", max_tokens=1, messages=[])["id"]
    client.close()

    assert [item[0] for item in observed] == [
        "https://gateway.internal/v1/messages",
        "https://gateway.internal/v1/messages",
    ]
    assert [item[1] for item in observed] == [f"Bearer {first}", f"Bearer {second}"]
    assert {item[2] for item in observed} == {str(attempt_id)}


def test_primitive_rejects_credential_payload_and_budget_before_network(tmp_path: Path) -> None:
    client, _path, _attempt_id = _client(tmp_path)
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    client._http.close()
    client._http = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(PipelineProviderAuthError, match="credential field"):
        client.messages.create(
            model="claude-test",
            max_tokens=1,
            messages=[],
            authorization="secret-canary",
        )
    client._dispatches = PRIMITIVE_DISPATCH_LIMIT
    with pytest.raises(
        ProviderAttemptBudgetExhaustedError,
        match="provider_attempt_budget_exhausted",
    ):
        client.messages.create(model="claude-test", max_tokens=1, messages=[])
    client.close()
    assert calls == 0


def test_provider_errors_do_not_render_rotating_bearer(tmp_path: Path) -> None:
    client, path, attempt_id = _client(tmp_path)
    canary = _token(attempt_id, "secret-canary")
    _install(path, canary)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text=canary)

    client._http.close()
    client._http = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(Exception) as captured:
        client.messages.create(model="claude-test", max_tokens=1, messages=[])
    client.close()
    assert canary not in str(captured.value)
    assert "Authorization" not in str(captured.value)
