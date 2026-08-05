"""Loom error hierarchy. Every Loom-defined exception inherits from `LoomError`.

Public types are stable; internal types are namespaced under their layer.
See spec §5.2 for the canonical taxonomy.
"""

from __future__ import annotations


class LoomError(Exception):
    """Root for all Loom-defined exceptions."""


# Driver layer ─────────────────────────────────────────────────────────────────


class DriverError(LoomError):
    """Base for sandbox-driver failures."""


class DriverAlreadyStartedError(DriverError):
    """`start()` was called twice on the same Driver instance."""


class DriverNotStartedError(DriverError):
    """`exec`/`upload`/`download` was called before `start()` or after `stop()`."""


class DriverExecError(DriverError):
    """A sandboxed exec call returned non-zero AND the caller asked us to raise."""

    def __init__(
        self,
        message: str,
        *,
        return_code: int,
        stdout: bytes,
        stderr: bytes,
    ) -> None:
        super().__init__(message)
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr


# Agent layer ──────────────────────────────────────────────────────────────────


class AgentError(LoomError):
    """Base for agent-runtime failures."""


class AgentSetupTimeoutError(AgentError):
    """`agent.setup()` exceeded the configured setup timeout."""


# Verifier framework ───────────────────────────────────────────────────────────


class VerifierError(LoomError):
    """Raised when the verifier framework itself fails (registry lookup,
    dispatch). NOT raised by `VerifierResult.error` — that's a struct field
    used for verifier-domain failures (missing tests, parse errors).
    """


# Trajectory layer ─────────────────────────────────────────────────────────────


class TrajectoryError(LoomError):
    """Base for trajectory-storage failures."""


class TrajectoryFlushFailedError(TrajectoryError):
    """Final flush to MinIO failed after retry exhaustion."""


# Control plane / worker comm ──────────────────────────────────────────────────


class WorkerLostClaimError(LoomError):
    """A fenced state-update endpoint rejected a worker because the trial no
    longer belongs to that worker (heartbeat lapsed; trial reclaimed)."""


# Configuration ────────────────────────────────────────────────────────────────


class ConfigError(LoomError):
    """Base for configuration failures."""


class TaskSchemaError(ConfigError):
    """A `task.toml` failed schema validation."""


class CapabilityMismatchError(ConfigError):
    """A trial's `requires_caps` cannot be satisfied by any registered worker
    configuration."""


# Failure classification ──────────────────────────────────────────────────────

import re  # noqa: E402

from loom.driver.build_containment import ImageBuildForbiddenError  # noqa: E402
from loom.models.result import FailureReason  # noqa: E402
from loom.security.redaction import redact_text  # noqa: E402

# Patterns for internal URLs that must NOT appear in user-facing messages.
_INTERNAL_URL_RE = re.compile(
    r"https?://[^ ]*loom-llm-gateway[^ ]*"
    r"|https?://[^ ]*loom-control-plane[^ ]*",
    re.IGNORECASE,
)
_MAX_MSG_LEN = 200
_TEXTUAL_PROVIDER_TRANSPORT_RE = re.compile(
    r"server disconnected without sending a response"
    r"|remoteprotocolerror"
    r"|remote protocol error"
    r"|connection closed without (?:a )?response",
    re.IGNORECASE,
)
_PROVIDER_TRANSPORT_DISCONNECT_MESSAGE = (
    "Provider transport disconnected before returning a response."
)
_TEXTUAL_CREDENTIAL_SCOPE_RE = re.compile(
    r"""["']detail["']\s*:\s*["']not authorized["']""",
    re.IGNORECASE,
)
_TEXTUAL_INVALID_BEARER_RE = re.compile(
    r"""["']detail["']\s*:\s*["']invalid bearer token["']"""
    r"|step token (?:is )?(?:invalid|expired)"
    r"|token has expired",
    re.IGNORECASE,
)
_CREDENTIAL_SCOPE_MESSAGE = "Loom gateway rejected a credential without llm:call scope (HTTP 401)."
_STEP_JWT_INVALID_MESSAGE = "Loom gateway rejected an invalid or expired step token (HTTP 401)."

# Terminus2 / Harbor tmux lifecycle (#1068): mid-run server loss vs setup duplicate.
_TMUX_NO_SERVER_RE = re.compile(
    r"no server running"
    r"|tmux session/server lost mid-dispatch",
    re.IGNORECASE,
)
_TMUX_NO_SERVER_MESSAGE = "Terminus2 tmux server disappeared mid-dispatch."
_TMUX_DUPLICATE_SESSION_RE = re.compile(
    r"duplicate session"
    r"|Failed to start tmux session",
    re.IGNORECASE,
)
_TMUX_DUPLICATE_SESSION_MESSAGE = "Terminus2 tmux session already exists at setup."


def _redact_body(raw: str) -> str:
    """Strip internal URLs, collapse whitespace, and truncate to 200 chars."""
    cleaned = _INTERNAL_URL_RE.sub("[redacted]", raw)
    # Replace newlines / CR so the message is single-line in logs + UI.
    cleaned = cleaned.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    cleaned = cleaned.strip()
    if len(cleaned) > _MAX_MSG_LEN:
        cleaned = cleaned[:_MAX_MSG_LEN]
    return cleaned


def _redact_failure_excerpt(raw: str) -> str:
    cleaned = redact_text(raw, limit=_MAX_MSG_LEN)
    cleaned = cleaned.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    return cleaned.strip()


def classify_failure_message(message: str) -> tuple[FailureReason, str | None] | None:
    """Classify text-only failures from agent adapters / Harbor stderr.

    Some SDK/CLI agents collapse HTTP transport exceptions into stderr text,
    for example ``Server disconnected without sending a response.``. By the
    time Loom sees the failure it is no longer an ``httpx`` exception, but it is
    still a provider/gateway transport boundary failure and should not be
    grouped with model/agent logic errors.

    Terminus2 also surfaces Harbor tmux lifecycle failures as plain
    ``RuntimeError`` strings (#1068). Mid-run ``no server running`` is distinct
    from setup-time ``duplicate session``.
    """

    if "401" in message and _TEXTUAL_CREDENTIAL_SCOPE_RE.search(message):
        return FailureReason.GATEWAY_ERROR, _CREDENTIAL_SCOPE_MESSAGE
    if "401" in message and _TEXTUAL_INVALID_BEARER_RE.search(message):
        return FailureReason.GATEWAY_ERROR, _STEP_JWT_INVALID_MESSAGE

    # Mid-run server loss before setup duplicate: a "failed to send keys"
    # message can also mention session names, but "no server running" is the
    # #1068 signature and must not be labeled as duplicate-session.
    if _TMUX_NO_SERVER_RE.search(message):
        return FailureReason.AGENT_ERROR, _TMUX_NO_SERVER_MESSAGE
    if _TMUX_DUPLICATE_SESSION_RE.search(message):
        return FailureReason.AGENT_ERROR, _TMUX_DUPLICATE_SESSION_MESSAGE

    if not _TEXTUAL_PROVIDER_TRANSPORT_RE.search(message):
        return None
    excerpt = _redact_failure_excerpt(message)
    if excerpt.startswith(_PROVIDER_TRANSPORT_DISCONNECT_MESSAGE):
        return FailureReason.PROVIDER_TRANSPORT_DISCONNECT, excerpt
    detail = _PROVIDER_TRANSPORT_DISCONNECT_MESSAGE
    if excerpt:
        detail = f"{detail} {excerpt}"
    return FailureReason.PROVIDER_TRANSPORT_DISCONNECT, detail


def _classify_http_status_error(exc: BaseException) -> tuple[FailureReason, str | None] | None:
    """Return a classified tuple if *exc* is an httpx.HTTPStatusError, else None.

    Imported lazily so that loom-core doesn't hard-depend on httpx being
    installed; in practice every worker environment has it, but unit tests
    for the pure-loom package should not require it.
    """
    try:
        import httpx
    except ImportError:
        return None

    if not isinstance(exc, httpx.HTTPStatusError):
        return None

    status = exc.response.status_code
    if status == 401:
        try:
            body_text = exc.response.text
        except Exception:
            body_text = ""
        if _TEXTUAL_CREDENTIAL_SCOPE_RE.search(body_text):
            return FailureReason.GATEWAY_ERROR, _CREDENTIAL_SCOPE_MESSAGE
        if _TEXTUAL_INVALID_BEARER_RE.search(body_text):
            return FailureReason.GATEWAY_ERROR, _STEP_JWT_INVALID_MESSAGE
    if 400 <= status <= 499:
        try:
            body_text = exc.response.text
        except Exception:
            body_text = ""
        excerpt = _redact_body(body_text)
        msg = f"Provider returned HTTP {status}."
        if excerpt:
            msg = f"Provider returned HTTP {status}. {excerpt}"
        return FailureReason.PROVIDER_ERROR, msg
    if 500 <= status <= 599:
        return FailureReason.GATEWAY_ERROR, f"Loom gateway returned HTTP {status}."
    return None


def _classify_http_transport_error(exc: BaseException) -> tuple[FailureReason, str | None] | None:
    """Classify gateway transport failures that are safe to retry.

    These are failures between the worker and Loom's gateway, before a
    provider can return a semantically meaningful 4xx. They include gateway
    timeouts, connection resets, and remote protocol drops.
    """
    try:
        import httpx
    except ImportError:
        return None

    retryable_types = (
        httpx.TimeoutException,
        httpx.NetworkError,
        httpx.RemoteProtocolError,
    )
    if not isinstance(exc, retryable_types):
        return None

    excerpt = _redact_body(str(exc))
    msg = (
        "Loom gateway timeout."
        if isinstance(exc, httpx.TimeoutException)
        else "Loom gateway transport error."
    )
    if excerpt:
        msg = f"{msg} {excerpt}"
    return FailureReason.GATEWAY_ERROR, msg


# ─── retry classification (#298) ────────────────────────────────────
#
# `is_retryable` is read by the gateway's per-request retry loop
# (`loom_llm_gateway.retry`). Returns True for failures where reissuing
# the same request might succeed: transient gateway/upstream 5xx,
# 429 rate-limits, 408 timeouts, and httpx transport-level errors.
# Returns False for deterministic 4xx (auth, schema, bad model) and
# anything unrelated to the HTTP boundary (verifier/agent/env crashes).
#
# 504 is INCLUDED despite the well-known idempotency caveat (upstream
# may have processed the request and we lost the response → retrying
# can double-bill). Decision logged in plan §D-idempotency:
# - In practice most 504s happen before any tokens stream, so providers
#   don't bill them. The rare cases that do bill are acceptable for v1
#   in exchange for the ~higher recovery rate.
# - We meter every 504 retry in `loom_gateway_llm_retry_ambiguous_504_total`
#   so we can revisit if the rate gets ugly.

_RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


def is_retryable(exc: BaseException) -> bool:
    """True iff `exc` is a transient failure worth reissuing.

    Used by the gateway's retry loop. Mirrors `classify_failure`'s
    dispatch but answers a different question — should we retry?
    """
    try:
        import httpx
    except ImportError:
        return False

    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_HTTP_STATUSES

    if isinstance(
        exc,
        (
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.RemoteProtocolError,
        ),
    ):
        return True

    return False


def classify_failure(exc: BaseException) -> tuple[FailureReason, str | None]:
    """Map an uncaught exception in ``Trial.run()`` to a ``(FailureReason,
    optional user-facing message)`` tuple.

    Phase-local handlers (in ``_run_step``) catch and record ``StepError``
    before this is reached, so most calls here are for env failures, framework
    crashes, or the rare trial-level timeout. See spec §5.2.

    The second element of the tuple is a short, redacted, single-line string
    safe to display directly to end-users.  It is ``None`` for failure reasons
    that have no actionable user-facing detail.
    """
    http_result = _classify_http_status_error(exc)
    if http_result is not None:
        return http_result

    transport_result = _classify_http_transport_error(exc)
    if transport_result is not None:
        return transport_result

    if isinstance(exc, AgentSetupTimeoutError):
        return FailureReason.AGENT_ERROR, None
    # #1169: a containment-required worker refuses to build an uncached image
    # (ImageBuildForbiddenError, a RuntimeError). Surface its self-explaining
    # message BEFORE the generic INTERNAL_ERROR fallthrough — and classify it as
    # an env-start failure since it aborts `driver.start()`.
    if isinstance(exc, ImageBuildForbiddenError):
        return FailureReason.ENV_START_FAILURE, _redact_failure_excerpt(str(exc)) or None
    # #1169: previously every DriverError was reported with a `None` message, so
    # env-start failures (build refusals, cgroup errors, container create/start
    # failures) surfaced with an empty `failure_message` and the cause was only
    # recoverable by inference. Propagate the redacted DriverError text instead.
    if isinstance(exc, DriverError):
        return FailureReason.ENV_START_FAILURE, _redact_failure_excerpt(str(exc)) or None
    if isinstance(exc, VerifierError):
        return FailureReason.VERIFIER_ERROR, None
    if isinstance(exc, TrajectoryFlushFailedError):
        return FailureReason.TRAJECTORY_FLUSH_FAILED, None
    if isinstance(exc, TimeoutError):
        return FailureReason.AGENT_TIMEOUT, None
    message_result = classify_failure_message(str(exc))
    if message_result is not None:
        return message_result
    return FailureReason.INTERNAL_ERROR, None
