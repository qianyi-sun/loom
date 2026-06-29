"""POST /v1/responses — OpenAI Responses dialect native passthrough
(Plan 9 Task 8 + A9.1).

LiteLLM does not yet have a stable Responses adapter; we passthrough to
OpenAI's native endpoint and preserve the reasoning content type +
output_tokens_details.reasoning_tokens counter.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request
from starlette.responses import StreamingResponse

from loom.models.types import ModelSpec
from loom_llm_gateway.dialect import DIALECTS, TokenUsage
from loom_llm_gateway.llm_calls import record_call, record_failed_call
from loom_llm_gateway.rate_card import (
    compute_cost_usd,
    hash_table,
    lookup_entry,
)
from loom_llm_gateway.request_params import (
    normalize_request_params,
    sanitize_request_extras,
)
from loom_llm_gateway.retry import send_with_retry
from loom_llm_gateway.routes._facade_common import (
    compute_facade_cost_usd,
    decrypt_facade_api_key,
    http_failure_category,
    redact_api_key,
    resolve_facade_connection,
    resolve_provider_connection_id,
    verify_facade_auth,
)
from loom_llm_gateway.routes.responses_chat_compat import (
    chat_completion_to_responses,
    decode_chat_completion_body,
    responses_payload_to_chat_completion,
    should_fallback_to_chat_completions,
    synthetic_responses_http_response,
)

router = APIRouter()

OPENAI_BASE_URL = "https://api.openai.com"
_OPENAI_SHAPED_TYPES = frozenset({"openai-compatible", "custom"})
_LOOM_REQUEST_PARAMS_QUERY_PARAM = "loom_request_params"


@router.post("/v1/responses", response_model=None)
@router.post("/openai/v1/responses", response_model=None)
async def responses(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
    x_loom_provider_connection_id: str | None = Header(
        default=None,
        alias="x-loom-provider-connection-id",
    ),
) -> dict[str, Any] | StreamingResponse:
    settings = request.app.state.settings
    payload = _merge_query_request_params(payload, request)
    signing_key = settings.step_jwt_signing_key.get_secret_value()
    async with request.app.state.session_factory() as session:
        ctx = await verify_facade_auth(
            session,
            authorization,
            signing_key,
        )
    assert ctx.team_id is not None
    assert ctx.trial_id is not None
    assert ctx.step_id is not None
    team_id = ctx.team_id
    trial_id = ctx.trial_id
    step_id = ctx.step_id
    model_name = payload.get("model")
    if not isinstance(model_name, str) or not model_name:
        raise HTTPException(status_code=400, detail="`model` is required")

    connection_id = None
    if ctx.provider_connection_id is not None or x_loom_provider_connection_id:
        connection_id = resolve_provider_connection_id(
            ctx,
            x_loom_provider_connection_id,
        )

    if connection_id is not None:
        async with request.app.state.session_factory() as session:
            row = await resolve_facade_connection(
                session,
                connection_id,
                team_id,
                supported_types=_OPENAI_SHAPED_TYPES,
                dialect_label="/v1/responses",
            )
            api_key = await decrypt_facade_api_key(session, row)

        upstream_url = f"{row.base_url.rstrip('/')}/responses"
        upstream: httpx.AsyncClient = await request.app.state.egress_client_pool.get(row.id)

        async def _record_responses_transport_failure(
            category: str,
            error_type: str,
        ) -> None:
            await _record_failed_responses_call(
                request=request,
                team_id=team_id,
                trial_id=trial_id,
                step_id=step_id,
                model_name=model_name,
                provider=row.provider_type,
                request_payload=payload,
                failure_category=category,
                failure_error_type=error_type,
            )

        upstream_response, attempt = await _post_upstream_responses(
            upstream=upstream,
            upstream_url=upstream_url,
            payload=payload,
            api_key=api_key,
            request=request,
            settings=settings,
            dialect="facade_openai_responses",
            failure_recorder=_record_responses_transport_failure,
        )
        if upstream_response.status_code >= 400:
            if should_fallback_to_chat_completions(
                upstream_response,
                payload,
            ):
                chat_payload = responses_payload_to_chat_completion(payload)
                chat_response, attempt = await _post_upstream_chat_completion(
                    upstream=upstream,
                    upstream_url=f"{row.base_url.rstrip('/')}/chat/completions",
                    payload=chat_payload,
                    api_key=api_key,
                    request=request,
                    settings=settings,
                    failure_recorder=_record_responses_transport_failure,
                )
                if chat_response.status_code >= 400:
                    excerpt = redact_api_key(chat_response.text, api_key)
                    await _record_failed_responses_call(
                        request=request,
                        team_id=team_id,
                        trial_id=trial_id,
                        step_id=step_id,
                        model_name=model_name,
                        provider=row.provider_type,
                        request_payload=payload,
                        failure_category=http_failure_category(
                            chat_response.status_code,
                        ),
                        failure_status_code=chat_response.status_code,
                        attempt=attempt,
                    )
                    raise HTTPException(
                        status_code=chat_response.status_code,
                        detail=(
                            "chat-completions fallback upstream returned "
                            f"{chat_response.status_code}: {excerpt}"
                        ),
                    )
                chat_body = decode_chat_completion_body(chat_response)
                body_or_stream = chat_completion_to_responses(
                    chat_body,
                    model_name=model_name,
                    stream=bool(payload.get("stream")),
                )
                usage = _extract_responses_usage(body_or_stream)
                cost, rate_card_hash = await compute_facade_cost_usd(
                    row,
                    model_name,
                    usage,
                    rate_card_cache=request.app.state.rate_card_cache,
                )
                await _record_responses_call(
                    request=request,
                    team_id=team_id,
                    trial_id=trial_id,
                    step_id=step_id,
                    model_name=model_name,
                    usage=usage,
                    cost_usd=cost,
                    rate_card_hash=rate_card_hash,
                    provider=row.provider_type,
                    attempt=attempt,
                    request_payload=payload,
                )
                return _responses_result(
                    synthetic_responses_http_response(body_or_stream),
                    body_or_stream,
                )
            excerpt = redact_api_key(upstream_response.text, api_key)
            await _record_failed_responses_call(
                request=request,
                team_id=team_id,
                trial_id=trial_id,
                step_id=step_id,
                model_name=model_name,
                provider=row.provider_type,
                request_payload=payload,
                failure_category=http_failure_category(
                    upstream_response.status_code,
                ),
                failure_status_code=upstream_response.status_code,
                attempt=attempt,
            )
            raise HTTPException(
                status_code=upstream_response.status_code,
                detail=(f"upstream returned {upstream_response.status_code}: {excerpt}"),
            )

        body_or_stream = _decode_response_body(upstream_response)
        usage = _extract_responses_usage(body_or_stream)
        cost, rate_card_hash = await compute_facade_cost_usd(
            row,
            model_name,
            usage,
            rate_card_cache=request.app.state.rate_card_cache,
        )
        await _record_responses_call(
            request=request,
            team_id=team_id,
            trial_id=trial_id,
            step_id=step_id,
            model_name=model_name,
            usage=usage,
            cost_usd=cost,
            rate_card_hash=rate_card_hash,
            provider=row.provider_type,
            attempt=attempt,
            request_payload=payload,
        )
        return _responses_result(upstream_response, body_or_stream)

    if settings.openai_api_key is None:
        raise HTTPException(
            status_code=503,
            detail="openai_api_key not configured on Gateway",
        )
    legacy_upstream: httpx.AsyncClient = request.app.state.upstream_client

    async def _record_legacy_transport_failure(
        category: str,
        error_type: str,
    ) -> None:
        await _record_failed_responses_call(
            request=request,
            team_id=team_id,
            trial_id=trial_id,
            step_id=step_id,
            model_name=model_name,
            provider="openai",
            request_payload=payload,
            failure_category=category,
            failure_error_type=error_type,
        )

    upstream_response, attempt = await _post_upstream_responses(
        upstream=legacy_upstream,
        upstream_url=f"{OPENAI_BASE_URL}/v1/responses",
        payload=payload,
        api_key=settings.openai_api_key.get_secret_value(),
        request=request,
        settings=settings,
        dialect="responses",
        failure_recorder=_record_legacy_transport_failure,
    )
    if upstream_response.status_code >= 400:
        await _record_failed_responses_call(
            request=request,
            team_id=team_id,
            trial_id=trial_id,
            step_id=step_id,
            model_name=model_name,
            provider="openai",
            request_payload=payload,
            failure_category=http_failure_category(upstream_response.status_code),
            failure_status_code=upstream_response.status_code,
            attempt=attempt,
        )
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=f"openai responses upstream returned "
            f"{upstream_response.status_code}: "
            f"{upstream_response.text[:500]}",
        )

    body_or_stream = _decode_response_body(upstream_response)
    usage = _extract_responses_usage(body_or_stream)
    if usage.input_tokens == 0 and usage.output_tokens == 0:
        raise HTTPException(
            status_code=502,
            detail="openai responses 200 missing usage block; cost cannot be attributed",
        )
    table = await request.app.state.rate_card_cache.get()
    entry = lookup_entry(table, ModelSpec(provider="openai", name=model_name))
    cost = compute_cost_usd(
        entry,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cached_input_tokens=usage.cached_input_tokens,
        cache_write_tokens=usage.cache_write_tokens,
    )
    async with request.app.state.session_factory() as session:
        await record_call(
            session,
            team_id=team_id,
            trial_id=trial_id,
            step_id=step_id,
            dialect="openai_responses",
            model=model_name,
            usage=usage,
            cost_usd=cost,
            rate_card_hash=hash_table(table),
            attempt=attempt,
            request_params=normalize_request_params(payload),
        )
    return _responses_result(upstream_response, body_or_stream)


def _merge_query_request_params(
    payload: dict[str, Any],
    request: Request,
) -> dict[str, Any]:
    extras = _request_params_from_query(request)
    if not extras:
        return payload
    return {**payload, **extras}


def _request_params_from_query(request: Request) -> dict[str, Any]:
    raw = request.query_params.get(_LOOM_REQUEST_PARAMS_QUERY_PARAM)
    if raw is None or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{_LOOM_REQUEST_PARAMS_QUERY_PARAM} must be valid JSON",
        ) from exc
    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=400,
            detail=f"{_LOOM_REQUEST_PARAMS_QUERY_PARAM} must be a JSON object",
        )
    return sanitize_request_extras(parsed)


async def _post_upstream_responses(
    *,
    upstream: httpx.AsyncClient,
    upstream_url: str,
    payload: dict[str, Any],
    api_key: str,
    request: Request,
    settings: Any,
    dialect: str,
    failure_recorder: Callable[[str, str], Awaitable[None]] | None = None,
) -> tuple[httpx.Response, int]:
    headers = _upstream_headers(request, api_key)
    try:
        outcome = await send_with_retry(
            lambda: upstream.post(
                upstream_url,
                json=payload,
                headers=headers,
                timeout=settings.upstream_timeout_sec,
                follow_redirects=False,
            ),
            settings=settings,
            dialect=dialect,
        )
    except httpx.TimeoutException as exc:
        if failure_recorder is not None:
            await failure_recorder("upstream_timeout", type(exc).__name__)
        raise HTTPException(
            status_code=504,
            detail=f"upstream timeout against {upstream_url}: {exc}",
        ) from exc
    except httpx.RequestError as exc:
        if failure_recorder is not None:
            await failure_recorder("upstream_transport", type(exc).__name__)
        raise HTTPException(
            status_code=502,
            detail=(f"upstream request error against {upstream_url}: {type(exc).__name__}: {exc}"),
        ) from exc
    return outcome.response, outcome.attempt


async def _post_upstream_chat_completion(
    *,
    upstream: httpx.AsyncClient,
    upstream_url: str,
    payload: dict[str, Any],
    api_key: str,
    request: Request,
    settings: Any,
    failure_recorder: Callable[[str, str], Awaitable[None]] | None = None,
) -> tuple[httpx.Response, int]:
    headers = _upstream_headers(request, api_key)
    headers["accept"] = "application/json"
    try:
        outcome = await send_with_retry(
            lambda: upstream.post(
                upstream_url,
                json=payload,
                headers=headers,
                timeout=settings.upstream_timeout_sec,
                follow_redirects=False,
            ),
            settings=settings,
            dialect="facade_openai_chat_compat",
        )
    except httpx.TimeoutException as exc:
        if failure_recorder is not None:
            await failure_recorder("upstream_timeout", type(exc).__name__)
        raise HTTPException(
            status_code=504,
            detail=f"upstream timeout against {upstream_url}: {exc}",
        ) from exc
    except httpx.RequestError as exc:
        if failure_recorder is not None:
            await failure_recorder("upstream_transport", type(exc).__name__)
        raise HTTPException(
            status_code=502,
            detail=(f"upstream request error against {upstream_url}: {type(exc).__name__}: {exc}"),
        ) from exc
    return outcome.response, outcome.attempt


def _upstream_headers(request: Request, api_key: str) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "content-type": "application/json",
    }
    accept = request.headers.get("accept")
    if accept:
        headers["accept"] = accept
    for name, value in request.headers.items():
        if name.lower().startswith("openai-"):
            headers[name] = value
    return headers


def _decode_response_body(response: httpx.Response) -> dict[str, Any] | str:
    content_type = response.headers.get("content-type", "")
    if "text/event-stream" in content_type:
        return response.text
    try:
        body = response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"upstream returned non-JSON body: {exc}",
        ) from exc
    if not isinstance(body, dict):
        raise HTTPException(
            status_code=502,
            detail="upstream returned non-object JSON body",
        )
    return body


def _extract_responses_usage(body_or_stream: dict[str, Any] | str) -> TokenUsage:
    if isinstance(body_or_stream, dict):
        return DIALECTS["openai_responses"].extract_tokens(body_or_stream)

    usage_body: dict[str, Any] | None = None
    for line in body_or_stream.splitlines():
        if not line.startswith("data:"):
            continue
        data = line.removeprefix("data:").strip()
        if not data or data == "[DONE]":
            continue
        try:
            event = json.loads(data)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        response_obj = event.get("response")
        if isinstance(response_obj, dict) and isinstance(
            response_obj.get("usage"),
            dict,
        ):
            usage_body = response_obj
        elif isinstance(event.get("usage"), dict):
            usage_body = event
    if usage_body is None:
        return TokenUsage(input_tokens=0, output_tokens=0)
    return DIALECTS["openai_responses"].extract_tokens(usage_body)


async def _record_responses_call(
    *,
    request: Request,
    team_id: Any,
    trial_id: Any,
    step_id: str,
    model_name: str,
    usage: TokenUsage,
    cost_usd: float,
    rate_card_hash: str,
    provider: str,
    attempt: int,
    request_payload: dict[str, Any],
) -> None:
    async with request.app.state.session_factory() as session:
        await record_call(
            session,
            team_id=team_id,
            trial_id=trial_id,
            step_id=step_id,
            dialect="openai_responses",
            model=model_name,
            usage=usage,
            cost_usd=cost_usd,
            rate_card_hash=rate_card_hash,
            provider=provider,
            attempt=attempt,
            request_params=normalize_request_params(request_payload),
        )


async def _record_failed_responses_call(
    *,
    request: Request,
    team_id: Any,
    trial_id: Any,
    step_id: str,
    model_name: str,
    provider: str,
    request_payload: dict[str, Any],
    failure_category: str,
    attempt: int = 1,
    failure_status_code: int | None = None,
    failure_error_type: str | None = None,
) -> None:
    async with request.app.state.session_factory() as session:
        await record_failed_call(
            session,
            team_id=team_id,
            trial_id=trial_id,
            step_id=step_id,
            dialect="openai_responses",
            model=model_name,
            provider=provider,
            attempt=attempt,
            request_params=normalize_request_params(request_payload),
            failure_category=failure_category,
            failure_status_code=failure_status_code,
            failure_error_type=failure_error_type,
        )


def _responses_result(
    response: httpx.Response,
    body_or_stream: dict[str, Any] | str,
) -> dict[str, Any] | StreamingResponse:
    if isinstance(body_or_stream, dict):
        return body_or_stream

    async def _body() -> Any:
        yield response.content

    return StreamingResponse(
        _body(),
        media_type=response.headers.get("content-type", "text/event-stream"),
        status_code=response.status_code,
    )
