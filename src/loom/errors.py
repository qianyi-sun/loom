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

from loom.models.result import FailureReason  # noqa: E402

# Patterns for internal URLs that must NOT appear in user-facing messages.
_INTERNAL_URL_RE = re.compile(
    r"https?://[^ ]*loom-llm-gateway[^ ]*"
    r"|https?://[^ ]*loom-control-plane[^ ]*",
    re.IGNORECASE,
)
_MAX_MSG_LEN = 200


def _redact_body(raw: str) -> str:
    """Strip internal URLs, collapse whitespace, and truncate to 200 chars."""
    cleaned = _INTERNAL_URL_RE.sub("[redacted]", raw)
    # Replace newlines / CR so the message is single-line in logs + UI.
    cleaned = cleaned.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    cleaned = cleaned.strip()
    if len(cleaned) > _MAX_MSG_LEN:
        cleaned = cleaned[:_MAX_MSG_LEN]
    return cleaned


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
    if isinstance(exc, DriverError):
        return FailureReason.ENV_START_FAILURE, None
    if isinstance(exc, VerifierError):
        return FailureReason.VERIFIER_ERROR, None
    if isinstance(exc, TrajectoryFlushFailedError):
        return FailureReason.TRAJECTORY_FLUSH_FAILED, None
    if isinstance(exc, TimeoutError):
        return FailureReason.AGENT_TIMEOUT, None
    return FailureReason.INTERNAL_ERROR, None
