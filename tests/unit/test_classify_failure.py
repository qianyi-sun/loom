"""Unit tests for classify_failure() taxonomy (spec §5.2, issue #164)."""

from __future__ import annotations

import httpx

from loom.errors import (
    AgentSetupTimeoutError,
    DriverError,
    TrajectoryFlushFailedError,
    VerifierError,
    classify_failure,
    classify_failure_message,
)
from loom.models.result import FailureReason

# ─── helpers ──────────────────────────────────────────────────────────────────

def _http_status_error(status_code: int, body: str = "") -> httpx.HTTPStatusError:
    """Build a minimal httpx.HTTPStatusError for the given status code."""
    request = httpx.Request("POST", "http://loom-llm-gateway:9100/v1/chat/completions")
    response = httpx.Response(status_code=status_code, text=body, request=request)
    return httpx.HTTPStatusError(
        f"HTTP {status_code}",
        request=request,
        response=response,
    )


def _gateway_request() -> httpx.Request:
    return httpx.Request("POST", "http://loom-llm-gateway:9100/v1/chat/completions")


# ─── pre-existing error types (must still return (reason, None)) ──────────────

def test_agent_setup_timeout_to_agent_error():
    reason, msg = classify_failure(AgentSetupTimeoutError("x"))
    assert reason == FailureReason.AGENT_ERROR
    assert msg is None


def test_driver_error_to_env_start_failure():
    reason, msg = classify_failure(DriverError("x"))
    assert reason == FailureReason.ENV_START_FAILURE
    assert msg is None


def test_verifier_error():
    reason, msg = classify_failure(VerifierError("x"))
    assert reason == FailureReason.VERIFIER_ERROR
    assert msg is None


def test_trajectory_flush_failed():
    reason, msg = classify_failure(TrajectoryFlushFailedError("x"))
    assert reason == FailureReason.TRAJECTORY_FLUSH_FAILED
    assert msg is None


def test_generic_exception_is_internal():
    reason, msg = classify_failure(RuntimeError("x"))
    assert reason == FailureReason.INTERNAL_ERROR
    assert msg is None


def test_textual_provider_transport_disconnect_is_classified_and_redacted():
    reason, msg = classify_failure(
        RuntimeError(
            "Server disconnected without sending a response. "
            "Authorization: Bearer loom_api_supersecret sk-hidden123"
        )
    )
    assert reason == FailureReason.PROVIDER_TRANSPORT_DISCONNECT
    assert msg is not None
    assert "Server disconnected without sending a response" in msg
    assert "loom_api_supersecret" not in msg
    assert "sk-hidden123" not in msg


def test_provider_transport_disconnect_message_classification_is_idempotent():
    prefix = "Provider transport disconnected before returning a response."
    reason, msg = classify_failure(
        RuntimeError(
            f"{prefix} codex exited rc=1; stderr: "
            "Server disconnected without sending a response."
        )
    )

    assert reason == FailureReason.PROVIDER_TRANSPORT_DISCONNECT
    assert msg is not None
    assert msg.count(prefix) == 1


def test_textual_loom_step_jwt_401_is_actionable_and_redacted() -> None:
    result = classify_failure_message(
        "litellm.AuthenticationError: Error code: 401 - "
        "{'detail': 'not authorized'} Authorization: Bearer secret-token"
    )

    assert result is not None
    reason, msg = result
    assert reason == FailureReason.GATEWAY_ERROR
    assert msg == "Loom gateway rejected or expired the step token (HTTP 401)."
    assert "secret-token" not in msg


def test_timeout_error_classification_is_phase_dependent():
    """Per spec §5.2: TimeoutError at trial-level → AGENT_TIMEOUT is the
    fallback. Phase-local handlers catch first and shouldn't reach this."""
    reason, msg = classify_failure(TimeoutError())
    assert reason == FailureReason.AGENT_TIMEOUT
    assert msg is None


# ─── httpx.HTTPStatusError: 4xx → PROVIDER_ERROR ─────────────────────────────

def test_http_400_is_provider_error():
    exc = _http_status_error(400, "Bad request body")
    reason, msg = classify_failure(exc)
    assert reason == FailureReason.PROVIDER_ERROR
    assert msg is not None
    assert "400" in msg


def test_http_401_is_provider_error():
    reason, _ = classify_failure(_http_status_error(401))
    assert reason == FailureReason.PROVIDER_ERROR


def test_http_429_is_provider_error():
    reason, msg = classify_failure(_http_status_error(429, "rate limited"))
    assert reason == FailureReason.PROVIDER_ERROR
    assert msg is not None
    assert "429" in msg


# ─── httpx.HTTPStatusError: 5xx → GATEWAY_ERROR ──────────────────────────────

def test_http_500_is_gateway_error():
    reason, msg = classify_failure(_http_status_error(500))
    assert reason == FailureReason.GATEWAY_ERROR
    assert msg is not None
    assert "500" in msg


def test_http_502_is_gateway_error():
    reason, msg = classify_failure(_http_status_error(502))
    assert reason == FailureReason.GATEWAY_ERROR
    assert "502" in msg


def test_http_503_is_gateway_error():
    reason, _ = classify_failure(_http_status_error(503))
    assert reason == FailureReason.GATEWAY_ERROR


def test_http_504_is_gateway_error():
    reason, msg = classify_failure(_http_status_error(504))
    assert reason == FailureReason.GATEWAY_ERROR
    assert msg is not None
    assert "504" in msg


def test_gateway_read_timeout_is_gateway_error():
    reason, msg = classify_failure(httpx.ReadTimeout("gateway timed out", request=_gateway_request()))
    assert reason == FailureReason.GATEWAY_ERROR
    assert msg is not None
    assert "timeout" in msg.lower()


def test_gateway_connection_reset_is_gateway_error():
    reason, msg = classify_failure(httpx.ConnectError("connection reset by peer", request=_gateway_request()))
    assert reason == FailureReason.GATEWAY_ERROR
    assert msg is not None
    assert "connection" in msg.lower()


# ─── redaction ────────────────────────────────────────────────────────────────

def test_body_containing_gateway_url_is_stripped():
    body = "upstream error at http://loom-llm-gateway:9100/foo/bar — quota exceeded"
    exc = _http_status_error(400, body)
    _, msg = classify_failure(exc)
    assert msg is not None
    assert "loom-llm-gateway" not in msg
    assert "[redacted]" in msg


def test_body_containing_control_plane_url_is_stripped():
    body = "call to http://loom-control-plane:8080/admin failed"
    exc = _http_status_error(400, body)
    _, msg = classify_failure(exc)
    assert msg is not None
    assert "loom-control-plane" not in msg
    assert "[redacted]" in msg


def test_body_truncated_to_200_chars():
    long_body = "x" * 300
    exc = _http_status_error(400, long_body)
    _, msg = classify_failure(exc)
    assert msg is not None
    # The message starts with "Provider returned HTTP 400. " then the body excerpt.
    # The excerpt itself is capped at 200 chars, so total message is a bit longer
    # but the *excerpt* portion is ≤ 200 chars.
    prefix = "Provider returned HTTP 400. "
    excerpt = msg[len(prefix):]
    assert len(excerpt) <= 200


def test_body_newlines_stripped():
    body = "line one\nline two\r\nline three"
    exc = _http_status_error(400, body)
    _, msg = classify_failure(exc)
    assert msg is not None
    assert "\n" not in msg
    assert "\r" not in msg


# ─── 5xx body is NOT included (could leak stack traces) ──────────────────────

def test_5xx_body_not_in_message():
    body = "Internal traceback: /opt/app/loom_gateway/main.py line 42"
    exc = _http_status_error(500, body)
    _, msg = classify_failure(exc)
    assert msg is not None
    # Only the generic message — no body excerpt for 5xx
    assert "traceback" not in (msg or "").lower()
    assert "/opt/app" not in (msg or "")
