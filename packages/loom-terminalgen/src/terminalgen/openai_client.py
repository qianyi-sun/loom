from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class OpenAIConfig:
    model: str
    api_key: str
    base_url: str | None = None
    temperature: float = 0.9
    max_retries: int = 3
    timeout_sec: float = 300.0
    call_log_dir: Path | None = None
    input_token_price_usd_per_1m: float | None = None
    output_token_price_usd_per_1m: float | None = None


class OpenAITextGenerator:
    def __init__(self, config: OpenAIConfig) -> None:
        self.config = config
        self._client = None
        self._call_index = 0
        self._lock = threading.Lock()
        self._stats = {
            "call_count": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "estimated_cost_usd": 0.0,
            "cost_tracking_enabled": (
                config.input_token_price_usd_per_1m is not None
                and config.output_token_price_usd_per_1m is not None
            ),
        }

    def _get_client(self):
        if self._client is None:
            try:
                from openai import OpenAI
            except ImportError as exc:
                raise RuntimeError(
                    "openai is not installed. Run `pip install -e /mnt/d/Github/terminalGen` first."
                ) from exc
            self._client = OpenAI(
                api_key=self.config.api_key,
                base_url=self.config.base_url,
                timeout=self.config.timeout_sec,
                max_retries=0,
            )
        return self._client

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict:
        last_error: Exception | None = None
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        for attempt in range(1, self.config.max_retries + 1):
            started_at = time.perf_counter()
            response = None
            content = ""
            retryable = False
            will_retry = False
            try:
                response = self._get_client().chat.completions.create(
                    model=self.config.model,
                    temperature=self.config.temperature,
                    messages=messages,
                )
                content = response.choices[0].message.content or ""
                payload = _extract_json_object(content)
                self._record_call(
                    attempt=attempt,
                    started_at=started_at,
                    messages=messages,
                    response=response,
                    content=content,
                    parsed_payload=payload,
                    error=None,
                    retryable=False,
                    will_retry=False,
                )
                return payload
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                retryable = _is_retryable_error(exc)
                will_retry = retryable and attempt < self.config.max_retries
                self._record_call(
                    attempt=attempt,
                    started_at=started_at,
                    messages=messages,
                    response=response,
                    content=content,
                    parsed_payload=None,
                    error=exc,
                    retryable=retryable,
                    will_retry=will_retry,
                )
                if not will_retry:
                    break
                time.sleep(min(2**attempt, 8))
        assert last_error is not None
        raise last_error

    def stats_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._stats)

    def _record_call(
        self,
        *,
        attempt: int,
        started_at: float,
        messages: list[dict[str, str]],
        response: Any,
        content: str,
        parsed_payload: dict[str, Any] | None,
        error: Exception | None,
        retryable: bool,
        will_retry: bool,
    ) -> None:
        finished_at = time.perf_counter()
        usage = _serialize_usage(getattr(response, "usage", None))
        estimated_cost = _estimate_cost(
            usage=usage,
            input_price_usd_per_1m=self.config.input_token_price_usd_per_1m,
            output_price_usd_per_1m=self.config.output_token_price_usd_per_1m,
        )
        self._update_stats(usage=usage, estimated_cost=estimated_cost)
        if self.config.call_log_dir is None:
            return

        call_index = self._next_call_index()
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        payload = {
            "call_index": call_index,
            "timestamp": timestamp,
            "attempt": attempt,
            "model": self.config.model,
            "base_url": self.config.base_url,
            "temperature": self.config.temperature,
            "duration_sec": round(finished_at - started_at, 6),
            "request": {"messages": messages},
            "response": _serialize_response(response, content),
            "usage": usage,
            "pricing": {
                "currency": "USD",
                "unit": "per_1m_tokens",
                "input_token_price_usd_per_1m": self.config.input_token_price_usd_per_1m,
                "output_token_price_usd_per_1m": self.config.output_token_price_usd_per_1m,
                "estimated_cost_usd": estimated_cost,
            },
            "parsed_payload": parsed_payload,
            "error": _serialize_error(error),
            "retry": {
                "retryable": retryable,
                "will_retry": will_retry,
                "max_retries": self.config.max_retries,
            },
        }

        self.config.call_log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.config.call_log_dir / f"{call_index:06d}.json"
        log_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _next_call_index(self) -> int:
        with self._lock:
            self._call_index += 1
            return self._call_index

    def _update_stats(self, *, usage: dict[str, Any], estimated_cost: float | None) -> None:
        with self._lock:
            self._stats["call_count"] += 1
            self._stats["input_tokens"] += int(usage.get("prompt_tokens") or 0)
            self._stats["output_tokens"] += int(usage.get("completion_tokens") or 0)
            self._stats["total_tokens"] += int(usage.get("total_tokens") or 0)
            if estimated_cost is not None:
                self._stats["estimated_cost_usd"] += estimated_cost


def _extract_json_object(text: str) -> dict:
    stripped = text.strip()
    if not stripped:
        raise ValueError("model returned empty content")

    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        if stripped.startswith("json"):
            stripped = stripped[4:].strip()

    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    start = stripped.find("{")
    if start == -1:
        raise ValueError("no JSON object found in model output")

    depth = 0
    in_string = False
    escaped = False
    for index, char in enumerate(stripped[start:], start=start):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                candidate = stripped[start : index + 1]
                return json.loads(candidate)
    raise ValueError("unterminated JSON object in model output")


def _serialize_response(response: Any, content: str) -> dict[str, Any] | None:
    if response is None:
        return None
    choice = response.choices[0] if getattr(response, "choices", None) else None
    message = getattr(choice, "message", None)
    return {
        "id": getattr(response, "id", None),
        "model": getattr(response, "model", None),
        "finish_reason": getattr(choice, "finish_reason", None),
        "message": {
            "role": getattr(message, "role", "assistant"),
            "content": content,
        },
    }


def _serialize_usage(usage: Any) -> dict[str, Any]:
    if usage is None:
        return {}
    return {
        "prompt_tokens": getattr(usage, "prompt_tokens", None),
        "completion_tokens": getattr(usage, "completion_tokens", None),
        "total_tokens": getattr(usage, "total_tokens", None),
        "prompt_tokens_details": _serialize_optional_struct(
            getattr(usage, "prompt_tokens_details", None)
        ),
        "completion_tokens_details": _serialize_optional_struct(
            getattr(usage, "completion_tokens_details", None)
        ),
    }


def _serialize_optional_struct(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump(exclude_none=True)
    if hasattr(value, "__dict__"):
        return {
            key: item
            for key, item in vars(value).items()
            if not key.startswith("_") and item is not None
        }
    return {"value": str(value)}


def _estimate_cost(
    *,
    usage: dict[str, Any],
    input_price_usd_per_1m: float | None,
    output_price_usd_per_1m: float | None,
) -> float | None:
    if input_price_usd_per_1m is None or output_price_usd_per_1m is None:
        return None
    prompt_tokens = int(usage.get("prompt_tokens") or 0)
    completion_tokens = int(usage.get("completion_tokens") or 0)
    input_cost = prompt_tokens * input_price_usd_per_1m / 1_000_000
    output_cost = completion_tokens * output_price_usd_per_1m / 1_000_000
    return round(input_cost + output_cost, 10)


def _serialize_error(error: Exception | None) -> dict[str, Any] | None:
    if error is None:
        return None
    payload = {
        "type": error.__class__.__name__,
        "message": str(error),
    }
    status_code = getattr(error, "status_code", None)
    if status_code is not None:
        payload["status_code"] = status_code
    request_id = getattr(error, "request_id", None)
    if request_id is not None:
        payload["request_id"] = request_id

    request = getattr(error, "request", None)
    if request is not None:
        payload["request"] = {
            "method": getattr(request, "method", None),
            "url": str(getattr(request, "url", "")) or None,
        }

    response = getattr(error, "response", None)
    if response is not None:
        payload["response"] = {
            "status_code": getattr(response, "status_code", None),
            "headers": {
                key: value
                for key, value in {
                    "x-request-id": getattr(response, "headers", {}).get("x-request-id"),
                    "openai-processing-ms": getattr(response, "headers", {}).get(
                        "openai-processing-ms"
                    ),
                }.items()
                if value is not None
            },
        }

    body = getattr(error, "body", None)
    if body is not None:
        if isinstance(body, (dict, list, str, int, float, bool)) or body is None:
            payload["body"] = body
        else:
            payload["body"] = str(body)

    return payload


def _is_retryable_error(error: Exception) -> bool:
    try:
        from openai import APIConnectionError, APIStatusError, APITimeoutError, InternalServerError, RateLimitError

        if isinstance(error, (APIConnectionError, APITimeoutError, RateLimitError, InternalServerError)):
            return True
        if isinstance(error, APIStatusError):
            status_code = getattr(error, "status_code", None)
            return status_code in {408, 409, 429} or (status_code is not None and status_code >= 500)
    except ImportError:
        pass

    if isinstance(error, (TimeoutError, ConnectionError)):
        return True

    return error.__class__.__name__ in {
        "ConnectError",
        "ConnectTimeout",
        "ReadError",
        "ReadTimeout",
        "RemoteProtocolError",
        "WriteError",
        "WriteTimeout",
    }
