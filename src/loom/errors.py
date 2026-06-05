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

from loom.models.result import FailureReason  # noqa: E402


def classify_failure(exc: BaseException) -> FailureReason:
    """Map an uncaught exception in `Trial.run()` to a `FailureReason`.

    Phase-local handlers (in `_run_step`) catch and record `StepError` before
    this is reached, so most calls here are for env failures, framework
    crashes, or the rare trial-level timeout. See spec §5.2.
    """
    if isinstance(exc, AgentSetupTimeoutError):
        return FailureReason.AGENT_ERROR
    if isinstance(exc, DriverError):
        return FailureReason.ENV_START_FAILURE
    if isinstance(exc, VerifierError):
        return FailureReason.VERIFIER_ERROR
    if isinstance(exc, TrajectoryFlushFailedError):
        return FailureReason.TRAJECTORY_FLUSH_FAILED
    if isinstance(exc, TimeoutError):
        return FailureReason.AGENT_TIMEOUT
    return FailureReason.INTERNAL_ERROR
