"""Step protocol + BaseStep helper (#340).

Every concrete step implements the :class:`Step` protocol. The driver
calls the methods in this order per invocation:

* ``is_done()`` — cheap check: has this step already succeeded?
  Reads the step's own ``result.json`` and returns True iff both:
    - state == "done"
    - inputs_hash matches the current context's hash

* ``verify_done()`` — optional non-mutating revalidation before a persisted
  DONE step is skipped. ``None`` preserves the default cheap-skip behavior;
  steps with live safety contracts may return the same three-state outcome as
  ``verify()``.

* ``verify()`` — non-mutating observation of the world. Called on
  resume when the persisted state is RUNNING/VERIFYING and we need to
  decide whether the interrupted run actually finished, needs a retry,
  or the world is in an unknown state.

* ``run()`` — the mutation itself. Writes its evidence artifacts into
  the step's evidence dir before returning. On success returns a
  :class:`RunResult` with exit_code=0 and any step-specific summary;
  on failure returns exit_code!=0 with an error string.

The Step Protocol keeps the driver decoupled from step-specific
subprocess/kubectl/git plumbing. Each concrete step in
``loom_cli.rollout.steps.sNN_*`` implements this Protocol.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import EvidenceDirectory, StepDir
from loom_cli.rollout.operator.redaction import (
    redact_rollout_mapping,
    redact_rollout_text,
)


class VerifyOutcome(Enum):
    """Result of a step's :meth:`Step.verify` call.

    ``MATCH`` — the observable world already looks like the step
        succeeded. The driver treats the step as done without a re-run.
    ``MISMATCH`` — the world doesn't look like a completed step. The
        driver resets state to RUNNING and re-invokes :meth:`Step.run`.
    ``UNKNOWN`` — we couldn't tell (e.g. the cluster is unreachable).
        The driver refuses to advance and prints a diagnostic asking
        the operator to intervene.
    """

    MATCH = "match"
    MISMATCH = "mismatch"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class RunResult:
    """Structured return from :meth:`Step.run`."""

    exit_code: int
    summary: str = ""
    artifacts: dict[str, str] = field(default_factory=dict)
    error: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "summary", redact_rollout_text(self.summary))
        if self.error is not None:
            object.__setattr__(self, "error", redact_rollout_text(self.error))

    def is_success(self) -> bool:
        return self.exit_code == 0


@runtime_checkable
class Step(Protocol):
    """Contract every rollout step implements."""

    number: int
    name: str

    def inputs_hash(self, ctx: RolloutContext) -> str: ...

    def is_done(self, ctx: RolloutContext, step_dir: StepDir) -> bool: ...

    def verify_done(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome | None: ...

    def requires_strict_live_verification(self) -> bool: ...

    def validate_done_artifacts(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
        artifacts: dict[str, str],
    ) -> bool: ...

    def verify(self, ctx: RolloutContext, step_dir: StepDir) -> VerifyOutcome: ...

    def run(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult: ...


class BaseStep:
    """Shared plumbing for concrete steps.

    Subclasses fill in :meth:`_inputs_fingerprint` and :meth:`_run_impl`
    at minimum. Optional overrides: :meth:`_verify_impl` (defaults to
    UNKNOWN — the safer choice for steps we can't observe cheaply) and
    :meth:`_run_result_ok` (defaults to reading the step's result.json).

    Subclasses set ``number`` and ``name`` at class level so the driver
    can inventory them without instantiating.
    """

    number: int = -1
    name: str = "base"

    # -------- inputs_hash --------

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        """Return the dict of values this step's success depends on.

        Sub-tasks override to include their own inputs. The base
        default hashes the full RolloutContext.
        """
        return ctx.to_inputs_dict()

    def inputs_hash(self, ctx: RolloutContext) -> str:
        """Deterministic sha256 over the step's inputs fingerprint."""
        payload = json.dumps(
            self._inputs_fingerprint(ctx),
            sort_keys=True,
            default=str,
        ).encode()
        return hashlib.sha256(payload).hexdigest()

    # -------- is_done --------

    def is_done(self, ctx: RolloutContext, step_dir: StepDir) -> bool:
        """Cheap: read result.json + compare hash."""
        result_path = step_dir.result_path()
        if not result_path.is_file():
            return False
        try:
            result = json.loads(result_path.read_text())
        except (json.JSONDecodeError, OSError):
            return False
        if not isinstance(result, dict):
            return False
        if type(result.get("number")) is not int or result.get("number") != self.number:
            return False
        if not isinstance(result.get("name"), str) or result.get("name") != self.name:
            return False
        if result.get("state") != "done":
            return False
        if result.get("inputs_hash") != self.inputs_hash(ctx):
            return False
        return True

    def verify_done(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome | None:
        """Optionally revalidate a hash-matching DONE step before skip.

        ``None`` keeps the historical skip behavior. Subclasses should opt in
        only when a persisted success depends on live state that can drift.
        """
        return None

    def requires_strict_live_verification(self) -> bool:
        """Require conclusive MATCH before finalizing a successful run.

        The default preserves the historical behavior where UNKNOWN after a
        successful ``run()`` is accepted. Live safety-critical steps may opt in
        so UNKNOWN or MISMATCH pauses in VERIFYING without another mutation.
        """
        return False

    def validate_done_artifacts(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
        artifacts: dict[str, str],
    ) -> bool:
        """Validate strict DONE artifact semantics beyond the generic schema."""
        return True

    # -------- verify --------

    def _verify_impl(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        """Default: UNKNOWN. Subclasses that can cheaply observe the
        world override this."""
        return VerifyOutcome.UNKNOWN

    def verify(self, ctx: RolloutContext, step_dir: StepDir) -> VerifyOutcome:
        return self._verify_impl(ctx, step_dir)

    # -------- run --------

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        raise NotImplementedError(f"step {self.name!r}: subclass must implement _run_impl")

    def run(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        return self._run_impl(ctx, step_dir)

    # -------- helpers subclasses use --------

    def write_stdout(self, step_dir: StepDir, text: str) -> None:
        step_dir.stdout_path().write_text(redact_rollout_text(text))

    def write_stderr(self, step_dir: StepDir, text: str) -> None:
        step_dir.stderr_path().write_text(redact_rollout_text(text))

    def write_artifact(
        self,
        step_dir: StepDir,
        name: str,
        data: str | bytes,
    ) -> None:
        path = step_dir.artifact_path(name)
        if isinstance(data, bytes):
            path.write_bytes(data)
        else:
            path.write_text(data)


def step_result_dict(
    *,
    step: Step,
    state: str,
    inputs_hash: str,
    started_at: str,
    finished_at: str,
    exit_code: int = 0,
    error: str | None = None,
    summary: str = "",
    artifacts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the canonical result.json shape."""
    payload = {
        "number": step.number,
        "name": step.name,
        "state": state,
        "inputs_hash": inputs_hash,
        "started_at": started_at,
        "finished_at": finished_at,
        "exit_code": exit_code,
        "error": error,
        "summary": summary,
        "artifacts": artifacts or {},
    }
    redacted = redact_rollout_mapping(payload)
    if not isinstance(redacted, dict):  # pragma: no cover - mapping contract
        raise TypeError("redacted step result must remain a mapping")
    return redacted


def get_evidence(ctx: RolloutContext) -> EvidenceDirectory | None:
    """Convenience — return the EvidenceDirectory for a ctx if the
    metadata carries the rollout id. Returns None if not set."""
    rid = ctx.metadata.get("rollout_id")
    if not rid:
        return None
    return EvidenceDirectory(ctx.rollout_root, rid)
