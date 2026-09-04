from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from loom_llm_gateway.attempt_deadline import deadline_http_exception
from loom_llm_gateway.routes import (
    chat,
    facade_anthropic,
    facade_google,
    facade_openai,
    gemini,
    messages,
    responses,
)


class _Secret:
    def get_secret_value(self) -> str:
        return "x" * 32


class _SessionContext:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *args: object) -> None:
        return None


class _DispatchSpy:
    calls = 0

    async def post(self, *args: object, **kwargs: object) -> None:
        self.calls += 1
        raise AssertionError("expired request reached provider dispatch")


def _request(body: dict[str, Any] | None = None) -> Request:
    encoded = json.dumps(body or {}).encode()
    delivered = False

    async def receive() -> dict[str, Any]:
        nonlocal delivered
        if delivered:
            return {"type": "http.disconnect"}
        delivered = True
        return {"type": "http.request", "body": encoded, "more_body": False}

    dispatch_spy = _DispatchSpy()
    app = SimpleNamespace(
        state=SimpleNamespace(
            settings=SimpleNamespace(step_jwt_signing_key=_Secret()),
            session_factory=lambda: _SessionContext(),
            upstream_client=dispatch_spy,
        )
    )
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "query_string": b"",
            "headers": [],
            "app": app,
        },
        receive,
    )


RouteCall = Callable[[Request], Awaitable[Any]]


async def _chat(request: Request) -> Any:
    return await chat.chat_completions(request, authorization="Bearer signed")


async def _gemini(request: Request) -> Any:
    return await gemini.gemini_generate_content(
        "gemini:generateContent",
        request,
        {"contents": []},
        authorization="Bearer signed",
    )


async def _openai(request: Request) -> Any:
    return await facade_openai.openai_chat_facade(
        request,
        {"model": "m", "messages": []},
        authorization="Bearer signed",
    )


async def _anthropic(request: Request) -> Any:
    return await facade_anthropic.anthropic_messages_facade(
        request,
        {"model": "m", "messages": []},
        authorization="Bearer signed",
    )


async def _google(request: Request) -> Any:
    return await facade_google.google_generate_content_facade(
        "gemini:generateContent",
        request,
        {"contents": []},
        authorization="Bearer signed",
    )


async def _messages(request: Request) -> Any:
    return await messages.messages(
        request,
        {"model": "m", "messages": []},
        authorization="Bearer signed",
    )


async def _responses(request: Request) -> Any:
    return await responses.responses(
        request,
        {"model": "m", "input": "x"},
        authorization="Bearer signed",
    )


@pytest.mark.parametrize(
    ("module", "auth_name", "route_call", "body"),
    [
        (chat, "require_llm_call_bearer", _chat, {"model": "openai/m", "messages": [{}]}),
        (gemini, "require_llm_call_bearer", _gemini, None),
        (facade_openai, "verify_facade_auth", _openai, None),
        (facade_anthropic, "verify_facade_auth", _anthropic, None),
        (facade_google, "verify_facade_auth", _google, None),
        (messages, "verify_facade_auth", _messages, None),
        (responses, "verify_facade_auth", _responses, None),
    ],
    ids=[
        "chat",
        "gemini",
        "openai-facade",
        "anthropic-facade",
        "google-facade",
        "messages",
        "responses",
    ],
)
async def test_every_provider_route_binds_deadline_during_auth_before_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    auth_name: str,
    route_call: RouteCall,
    body: dict[str, Any] | None,
) -> None:
    observed_request: Request | None = None

    async def reject_expired(*args: object, **kwargs: object) -> None:
        nonlocal observed_request
        observed_request = kwargs.get("request")  # type: ignore[assignment]
        raise deadline_http_exception()

    monkeypatch.setattr(module, auth_name, reject_expired)
    request = _request(body)
    with pytest.raises(HTTPException) as exc:
        await route_call(request)

    assert observed_request is request
    assert exc.value.status_code == 504
    assert exc.value.detail == {
        "code": "agent_timeout",
        "reason": "attempt_deadline_reached",
    }
    assert request.app.state.upstream_client.calls == 0
