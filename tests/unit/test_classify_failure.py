"""Unit tests for classify_failure() taxonomy (spec §5.2, issue #164)."""

from __future__ import annotations

import httpx

from loom.errors import (
    AgentError,
    AgentSetupTimeoutError,
    DriverError,
    TrajectoryFlushFailedError,
    VerifierError,
    classify_failure,
    classify_failure_message,
    is_platform_setup_agent_failure,
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


def test_driver_error_to_env_start_failure_surfaces_message():
    # #1169: DriverError now propagates a redacted, self-explaining message
    # instead of dropping it to None (empty failure_message hid the cause).
    reason, msg = classify_failure(DriverError("container create failed: no such image"))
    assert reason == FailureReason.ENV_START_FAILURE
    assert msg == "container create failed: no such image"


def test_driver_error_empty_message_stays_none():
    reason, msg = classify_failure(DriverError(""))
    assert reason == FailureReason.ENV_START_FAILURE
    assert msg is None


def test_image_build_forbidden_is_env_start_with_message():
    # #1169: a containment-required worker's build refusal must be
    # self-explaining (it aborts driver.start()).
    from loom.driver.build_containment import ImageBuildForbiddenError

    exc = ImageBuildForbiddenError(
        "refusing to build image 'task:abc' on a containment-required "
        "(non-exclusive Slurm) worker: pre-build and cache the image."
    )
    reason, msg = classify_failure(exc)
    assert reason == FailureReason.ENV_START_FAILURE
    assert msg is not None
    assert "refusing to build image" in msg
    assert "\n" not in msg  # redacted to a single line


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


def test_tmux_no_server_mid_dispatch_is_agent_error():
    reason, msg = classify_failure(
        RuntimeError(
            "loom-trial-main: failed to send non-blocking keys: "
            "command=\"tmux send-keys -t terminus-2 -- 'chmod +x x\\n'\", "
            "return_code=1, stderr='no server running on /tmp/tmux-0/default\\n', "
            "stdout=''"
        )
    )
    assert reason == FailureReason.AGENT_ERROR
    assert msg == "Terminus2 tmux server disappeared mid-dispatch."


def test_tmux_session_lost_guard_message_is_agent_error():
    reason, msg = classify_failure_message("Terminus2 tmux session/server lost mid-dispatch.")
    assert reason == FailureReason.AGENT_ERROR
    assert msg == "Terminus2 tmux server disappeared mid-dispatch."


def test_tmux_soft_recover_recreate_failure_message_is_agent_error():
    """Soft-recover recreate failure must stay actionable agent_error (#1068)."""
    reason, msg = classify_failure(
        AgentError("Terminus2 tmux session/server lost mid-dispatch. Recreate failed: boom")
    )
    assert reason == FailureReason.AGENT_ERROR
    assert msg == "Terminus2 tmux server disappeared mid-dispatch."


def test_tmux_duplicate_session_at_setup_is_agent_error():
    reason, msg = classify_failure(
        RuntimeError("Failed to start tmux session. Error: duplicate session: terminus-2")
    )
    assert reason == FailureReason.AGENT_ERROR
    assert msg == "Terminus2 tmux session already exists at setup."


def test_harbor_worker_image_pin_is_task_compatibility() -> None:
    result = classify_failure_message(
        "terminus-2 requires harbor@527d50d preinstalled in the worker image"
    )
    assert result is not None
    reason, msg = result
    assert reason == FailureReason.TASK_COMPATIBILITY
    assert msg == "Worker image is missing the required Harbor pin for terminus-2."
    assert is_platform_setup_agent_failure(
        "terminus-2 requires harbor@527d50d preinstalled in the worker image"
    )
    # step_runner stores the redacted message; re-classify must stay stable.
    assert is_platform_setup_agent_failure(msg)
    assert classify_failure_message(msg) == (
        FailureReason.TASK_COMPATIBILITY,
        msg,
    )


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
            f"{prefix} codex exited rc=1; stderr: Server disconnected without sending a response."
        )
    )

    assert reason == FailureReason.PROVIDER_TRANSPORT_DISCONNECT
    assert msg is not None
    assert msg.count(prefix) == 1


def test_textual_credential_scope_401_is_actionable_and_redacted() -> None:
    result = classify_failure_message(
        "litellm.AuthenticationError: Error code: 401 - "
        "{'detail': 'not authorized'} Authorization: Bearer secret-token"
    )

    assert result is not None
    reason, msg = result
    assert reason == FailureReason.GATEWAY_ERROR
    assert msg == ("Loom gateway rejected a credential without llm:call scope (HTTP 401).")
    assert "secret-token" not in msg


def test_textual_expired_step_jwt_401_is_distinct_and_redacted() -> None:
    result = classify_failure_message(
        "litellm.AuthenticationError: Error code: 401 - "
        "{'detail': 'invalid bearer token'} "
        "Authorization: Bearer loom_step_secret-token"
    )

    assert result is not None
    reason, msg = result
    assert reason == FailureReason.GATEWAY_ERROR
    assert msg == ("Loom gateway rejected an invalid or expired step token (HTTP 401).")
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


def test_unclassified_http_401_is_provider_error():
    reason, _ = classify_failure(_http_status_error(401))
    assert reason == FailureReason.PROVIDER_ERROR


def test_http_401_scope_rejection_is_gateway_error_and_redacted():
    reason, msg = classify_failure(
        _http_status_error(
            401,
            '{"detail":"not authorized","token":"loom_worker_supersecret"}',
        ),
    )
    assert reason == FailureReason.GATEWAY_ERROR
    assert msg == ("Loom gateway rejected a credential without llm:call scope (HTTP 401).")
    assert "supersecret" not in msg


def test_http_401_expired_step_jwt_is_distinct_and_redacted():
    reason, msg = classify_failure(
        _http_status_error(
            401,
            '{"detail":"invalid bearer token","token":"loom_step_supersecret"}',
        ),
    )
    assert reason == FailureReason.GATEWAY_ERROR
    assert msg == ("Loom gateway rejected an invalid or expired step token (HTTP 401).")
    assert "supersecret" not in msg


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
    reason, msg = classify_failure(
        httpx.ReadTimeout("gateway timed out", request=_gateway_request())
    )
    assert reason == FailureReason.GATEWAY_ERROR
    assert msg is not None
    assert "timeout" in msg.lower()


def test_gateway_connection_reset_is_gateway_error():
    reason, msg = classify_failure(
        httpx.ConnectError("connection reset by peer", request=_gateway_request())
    )
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
    excerpt = msg[len(prefix) :]
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
