"""Rollout driver orchestrator (#340).

Iterates a static list of :class:`Step` implementations. Persists state
after every transition so a re-run picks up where the previous
invocation stopped. Refuses conflicting operator flags early.

The resume algorithm is documented on the design comment in the issue.
Summary:

1. ``is_done()`` gates persisted DONE records by their inputs hash. A step may
   then opt into ``verify_done()``: MATCH skips, MISMATCH or UNKNOWN refuses to
   mutate, and the default ``None`` preserves the historical skip. Operators
   must explicitly handle/reset a strict DONE step whose result evidence is no
   longer valid or whose live state no longer matches.
2. Persisted state ``RUNNING`` or ``VERIFYING`` → call ``verify()``.
     MATCH   → mark done, continue.
     MISMATCH → reset to RUNNING and call ``run()`` for default steps; strict
                live-contract steps stop in VERIFYING pending explicit reset.
     UNKNOWN → refuse to advance; print a diagnostic.
3. Persisted state ``NOT_STARTED`` or ``FAILED`` → call ``run()``.

After a successful ``run()``, strict live-contract steps also require MATCH
before DONE. Their successful-run evidence is persisted in ``result.json``
while VERIFYING so a later MATCH can finalize without repeating mutation.

Every step's ``run()`` writes its evidence dir before returning; the
driver writes the top-level ``state.json`` after each transition.

Concurrency: the driver expects the caller to have already acquired
the rollout mutation lock (via ``loom_cli.rollout_lock``). This module
is single-writer for the evidence tree.
"""

from __future__ import annotations

import json
import os
import socket
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import EvidenceDirectory, StepDir
from loom_cli.rollout.failure_authority import classify_rollout_failure
from loom_cli.rollout.operator.redaction import (
    known_secrets_from_sources,
    redact_rollout_text,
    rollout_redaction_scope,
)
from loom_cli.rollout.state import DriverRecord, RolloutState, StepState
from loom_cli.rollout.steps.base import (
    Step,
    VerifyOutcome,
    step_result_dict,
)


class DriverError(RuntimeError):
    """Raised by the driver when a step or invariant fails."""


_INPUTS_HASH_UNAVAILABLE_AFTER_CALLBACK = "unavailable-after-callback-exception"
_INPUTS_HASH_UNAVAILABLE_AFTER_STEP_FAILURE = "unavailable-after-step-failure"
_CANONICAL_RESULT_KEYS = frozenset(
    {
        "number",
        "name",
        "state",
        "inputs_hash",
        "started_at",
        "finished_at",
        "exit_code",
        "error",
        "summary",
        "artifacts",
    },
)
_CANONICAL_UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass(frozen=True, slots=True)
class _PendingRunEvidence:
    """Validated successful-run evidence awaiting conclusive live verify."""

    summary: str
    artifacts: dict[str, object]
    started_at: str
    inputs_hash: str


def _load_pending_run_evidence(
    step: Step,
    ctx: RolloutContext,
    step_dir: StepDir,
    *,
    expected_started_at: str | None,
) -> _PendingRunEvidence:
    """Read and fully validate a strict step's successful pending result."""
    try:
        payload = json.loads(step_dir.result_path().read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DriverError(
            "strict VERIFYING result evidence is missing or invalid JSON",
        ) from exc
    if not isinstance(payload, dict) or set(payload) != _CANONICAL_RESULT_KEYS:
        raise DriverError(
            "strict VERIFYING result evidence does not use the canonical schema",
        )
    try:
        expected_inputs_hash = step.inputs_hash(ctx)
    except Exception as exc:
        raise DriverError(
            "strict VERIFYING result inputs hash could not be recomputed",
        ) from exc
    field_checks = {
        "number": type(payload.get("number")) is int and payload.get("number") == step.number,
        "name": isinstance(payload.get("name"), str) and payload.get("name") == step.name,
        "state": isinstance(payload.get("state"), str) and payload.get("state") == "verifying",
        "inputs_hash": isinstance(payload.get("inputs_hash"), str)
        and payload.get("inputs_hash") == expected_inputs_hash,
        "exit_code": type(payload.get("exit_code")) is int and payload.get("exit_code") == 0,
        "error": "error" in payload and payload.get("error") is None,
    }
    mismatched = [key for key, valid in field_checks.items() if not valid]
    if mismatched:
        raise DriverError(
            "strict VERIFYING result evidence failed identity validation for: "
            + ", ".join(sorted(mismatched)),
        )
    summary = payload.get("summary")
    artifacts = payload.get("artifacts")
    started_at = payload.get("started_at")
    finished_at = payload.get("finished_at")
    if not isinstance(summary, str):
        raise DriverError("strict VERIFYING result summary is malformed")
    if not isinstance(artifacts, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in artifacts.items()
    ):
        raise DriverError("strict VERIFYING result artifacts are malformed")
    parsed_started_at = _parse_canonical_utc_timestamp(started_at)
    parsed_finished_at = _parse_canonical_utc_timestamp(finished_at)
    if (
        not isinstance(started_at, str)
        or not isinstance(finished_at, str)
        or parsed_started_at is None
        or parsed_finished_at is None
        or parsed_started_at > parsed_finished_at
        or started_at != expected_started_at
    ):
        raise DriverError("strict VERIFYING result timestamps are malformed")
    validated_artifacts: dict[str, str] = dict(artifacts)
    try:
        artifacts_match = step.validate_done_artifacts(
            ctx,
            step_dir,
            validated_artifacts,
        )
    except Exception as exc:
        raise DriverError(
            "strict VERIFYING result artifact contract could not be validated",
        ) from exc
    if artifacts_match is not True:
        raise DriverError(
            "strict VERIFYING result artifacts do not match the step contract",
        )
    pending_artifacts: dict[str, object] = dict(validated_artifacts)
    return _PendingRunEvidence(
        summary=summary,
        artifacts=pending_artifacts,
        started_at=started_at,
        inputs_hash=expected_inputs_hash,
    )


def _parse_canonical_utc_timestamp(value: object) -> datetime | None:
    """Return a UTC timestamp only for the driver's exact persisted format."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.strptime(value, _CANONICAL_UTC_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None
    if parsed.strftime(_CANONICAL_UTC_FORMAT) != value:
        return None
    return parsed


def _strict_done_result_is_canonical(
    step: Step,
    ctx: RolloutContext,
    step_dir: StepDir,
    *,
    expected_started_at: str | None,
    expected_finished_at: str | None,
) -> bool:
    """Validate strict DONE evidence before any live observation or mutation."""
    try:
        payload = json.loads(step_dir.result_path().read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict) or set(payload) != _CANONICAL_RESULT_KEYS:
        return False
    try:
        expected_inputs_hash = step.inputs_hash(ctx)
    except Exception:
        return False
    if not (
        type(payload["number"]) is int
        and payload["number"] == step.number
        and isinstance(payload["name"], str)
        and payload["name"] == step.name
        and payload["state"] == "done"
        and isinstance(payload["inputs_hash"], str)
        and payload["inputs_hash"] == expected_inputs_hash
        and type(payload["exit_code"]) is int
        and payload["exit_code"] == 0
        and payload["error"] is None
        and isinstance(payload["summary"], str)
    ):
        return False

    artifacts_raw = payload["artifacts"]
    if not isinstance(artifacts_raw, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in artifacts_raw.items()
    ):
        return False
    artifacts: dict[str, str] = dict(artifacts_raw)

    started_at = payload["started_at"]
    finished_at = payload["finished_at"]
    parsed_started_at = _parse_canonical_utc_timestamp(started_at)
    parsed_finished_at = _parse_canonical_utc_timestamp(finished_at)
    if (
        parsed_started_at is None
        or parsed_finished_at is None
        or parsed_started_at > parsed_finished_at
        or started_at != expected_started_at
        or finished_at != expected_finished_at
    ):
        return False

    try:
        return step.validate_done_artifacts(ctx, step_dir, artifacts) is True
    except Exception:
        return False


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _boot_id() -> str | None:
    """Return the Linux boot id when available.

    PID liveness alone can be fooled by PID reuse. The boot id is available on
    the platform-dev Linux host and makes stale-driver detection safer after a
    reboot. Local macOS tests simply get ``None``.
    """
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _current_driver_identity(ctx: RolloutContext) -> DriverRecord:
    now = _utc_now_iso()
    return DriverRecord(
        pid=os.getpid(),
        hostname=socket.gethostname(),
        boot_id=_boot_id(),
        started_at=now,
        updated_at=now,
        attempt_number=ctx.attempt_number,
        attempt_operator=ctx.attempt_operator,
        attempt_uid=ctx.attempt_uid,
    )


def _driver_record_is_alive(
    record: DriverRecord,
    current: DriverRecord,
) -> bool:
    """Best-effort check whether a persisted driver owner is still alive."""
    if record.hostname != current.hostname:
        # The evidence tree may be shared; do not race another host we cannot
        # inspect safely.
        return True
    if record.boot_id and current.boot_id and record.boot_id != current.boot_id:
        return False
    if record.pid == current.pid:
        return True
    try:
        os.kill(record.pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _refresh_driver(
    state: RolloutState,
    driver: DriverRecord,
) -> None:
    started_at = state.driver.started_at if state.driver else driver.started_at
    state.mark_driver_active(
        DriverRecord(
            pid=driver.pid,
            hostname=driver.hostname,
            boot_id=driver.boot_id,
            started_at=started_at,
            updated_at=_utc_now_iso(),
            attempt_number=driver.attempt_number,
            attempt_operator=driver.attempt_operator,
            attempt_uid=driver.attempt_uid,
        )
    )


def _save_state(
    state: RolloutState,
    evidence: EvidenceDirectory,
    driver: DriverRecord,
) -> None:
    _refresh_driver(state, driver)
    evidence.write_state(state.to_dict())


def _clear_driver_and_save(
    state: RolloutState,
    evidence: EvidenceDirectory,
) -> None:
    state.clear_driver()
    evidence.write_state(state.to_dict())


def _emit(evidence: EvidenceDirectory, stream: TextIO, text: str) -> None:
    safe_text = redact_rollout_text(text)
    stream.write(safe_text)
    evidence.append_driver_log(safe_text)


def _scrub_step_diagnostics(step_dir: StepDir) -> None:
    """Re-redact normal text logs even when a step bypassed safe helpers."""
    for path in (step_dir.stdout_path(), step_dir.stderr_path()):
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            continue
        safe = redact_rollout_text(raw)
        if safe != raw:
            path.write_text(safe, encoding="utf-8")


def _inputs_diff(
    persisted: dict[str, object],
    current: dict[str, object],
) -> str:
    """Return a short human-readable diff of two input dicts."""
    lines: list[str] = []
    for key in sorted(set(persisted) | set(current)):
        p = persisted.get(key, "<missing>")
        c = current.get(key, "<missing>")
        if p != c:
            lines.append(f"  {key}: persisted={p!r} current={c!r}")
    rendered = "\n".join(lines) if lines else "  (no differences)"
    return redact_rollout_text(rendered)


def preflight_ctx(ctx: RolloutContext) -> None:
    """Refuse invariant violations before touching anything.

    Currently guards the #340 acceptance criterion:
        --exclude-oldlab is incompatible with scope=full-cluster.
    """
    if ctx.would_falsify_full_cluster_acceptance():
        raise DriverError(
            "refusing to run: --exclude-oldlab is set while --scope="
            f"{ctx.scope!r}. A full-cluster rollout must not exclude "
            "release-managed worker pools; pick --scope=current-gb10 "
            "or drop --exclude-oldlab."
        )


def load_or_init_rollout(
    ctx: RolloutContext,
    evidence: EvidenceDirectory,
    steps: Sequence[Step],
    *,
    preloaded_state: RolloutState | None = None,
    initialize_new: bool = False,
) -> RolloutState:
    """Load the persisted RolloutState if one exists; otherwise create."""
    if preloaded_state is not None:
        return preloaded_state
    if not initialize_new:
        state_path = evidence.state_path()
        if state_path.is_file():
            loaded = RolloutState.load(state_path)
            _bind_state_attribution(ctx, loaded)
            return loaded
    inventory = [(step.number, step.name) for step in steps]
    return RolloutState.new(
        rollout_id=evidence.rollout_id,
        steps=inventory,
        request_id=ctx.request_id,
        initiating_operator=ctx.initiating_operator,
        initiating_uid=ctx.initiating_uid,
        attempt_number=ctx.attempt_number,
        attempt_operator=ctx.attempt_operator,
        attempt_uid=ctx.attempt_uid,
    )


def _bind_state_attribution(ctx: RolloutContext, state: RolloutState) -> None:
    """Validate immutable broker attribution and advance only the current attempt."""
    if ctx.request_id is None:
        return
    if state.request_id is None:
        raise DriverError("brokered resume refuses unattributed legacy state")
    immutable_state = (
        state.request_id,
        state.initiating_operator,
        state.initiating_uid,
    )
    immutable_context = (
        ctx.request_id,
        ctx.initiating_operator,
        ctx.initiating_uid,
    )
    if immutable_state != immutable_context:
        raise DriverError("brokered state attribution does not match request envelope")
    if ctx.attempt_number is None or ctx.attempt_operator is None or ctx.attempt_uid is None:
        raise DriverError("brokered request is missing current attempt attribution")
    if state.attempt_number is not None and ctx.attempt_number < state.attempt_number:
        raise DriverError("brokered request attempt number moved backwards")
    if state.attempt_number == ctx.attempt_number and (
        state.attempt_operator,
        state.attempt_uid,
    ) != (ctx.attempt_operator, ctx.attempt_uid):
        raise DriverError("brokered state attribution does not match request envelope")
    state.attempt_number = ctx.attempt_number
    state.attempt_operator = ctx.attempt_operator
    state.attempt_uid = ctx.attempt_uid


def check_inputs_match(
    ctx: RolloutContext,
    evidence: EvidenceDirectory,
    *,
    preloaded_inputs: dict[str, object] | None = None,
    initialize_missing: bool = False,
) -> None:
    """If a persisted inputs.json exists, ensure it matches the current
    context; otherwise write it. Refuses on mismatch."""
    if initialize_missing:
        evidence.write_inputs(ctx.to_inputs_dict())
        return
    if preloaded_inputs is not None or evidence.inputs_path().is_file():
        persisted = preloaded_inputs if preloaded_inputs is not None else evidence.read_inputs()
        current = ctx.to_inputs_dict()
        # `resume` isn't part of persisted inputs. Compare ignoring it.
        if persisted != current:
            raise DriverError(
                "refusing to run: inputs.json for this rollout differs "
                "from the current invocation. If you intended a fresh "
                "rollout against a different target, use a new "
                "--rollout-root or wait for the previous rollout to "
                "complete.\n" + _inputs_diff(persisted, current)
            )
    else:
        evidence.write_inputs(ctx.to_inputs_dict())


def _validate_resume_state_shape(document: dict[str, object]) -> None:
    if not isinstance(document.get("rollout_id"), str):
        raise ValueError("state rollout_id must be a string")
    if document.get("status") not in {"running", "done", "failed"}:
        raise ValueError("state status is invalid")
    current_step = document.get("current_step")
    if current_step is not None and (type(current_step) is not int or current_step < 0):
        raise ValueError("state current_step is invalid")
    driver = document.get("driver")
    if driver is not None and not isinstance(driver, dict):
        raise ValueError("state driver is invalid")
    raw_steps = document.get("steps")
    if not isinstance(raw_steps, list) or not all(isinstance(item, dict) for item in raw_steps):
        raise ValueError("state steps must be a list of objects")
    for raw_step in raw_steps:
        number = raw_step.get("number")
        if type(number) is not int or number < 0:
            raise ValueError("state step number is invalid")
        name = raw_step.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("state step name is invalid")
        if raw_step.get("state", "not_started") not in {
            "not_started",
            "running",
            "verifying",
            "done",
            "failed",
        }:
            raise ValueError("state step status is invalid")
        for field_name in (
            "inputs_hash",
            "started_at",
            "finished_at",
            "error",
        ):
            value = raw_step.get(field_name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"state step {field_name} is invalid")


def _load_resume_material(
    ctx: RolloutContext,
    evidence: EvidenceDirectory,
    steps: Sequence[Step],
) -> tuple[dict[str, object] | None, RolloutState | None] | None:
    """Preload and validate resume anchors before any evidence write.

    ``(None, None)`` is reserved for a manual resume whose rollout evidence
    directory is genuinely absent. That legacy direct-driver behavior creates
    a fresh tree, but the caller must not re-open either anchor by path after
    this descriptor-bound discovery step.
    """
    if not ctx.resume:
        return None
    try:
        if ctx.request_id is None:
            documents = evidence.read_resume_documents_if_present()
            if documents is None:
                return (None, None)
        else:
            documents = evidence.read_resume_documents()
        inputs, state_document = documents
        _validate_resume_state_shape(state_document)
        state = RolloutState.from_dict(state_document)
        if state.rollout_id != evidence.rollout_id:
            raise ValueError("state rollout id does not match evidence path")
        persisted_steps = [(record.number, record.name) for record in state.steps]
        expected_steps = [(step.number, step.name) for step in steps]
        if persisted_steps != expected_steps:
            raise ValueError("persisted rollout step inventory changed")
        if ctx.request_id is not None:
            _bind_state_attribution(ctx, state)
            current = ctx.to_inputs_dict()
            if inputs != current:
                raise ValueError("persisted rollout inputs changed")
    except DriverError:
        raise
    except Exception:
        subject = "brokered" if ctx.request_id is not None else "manual"
        raise DriverError(f"{subject} resume requires intact inputs.json and state.json") from None
    return inputs, state


def _safe_exception_text(exc: Exception) -> str:
    try:
        rendered = str(exc)
    except Exception:
        rendered = type(exc).__name__
    return redact_rollout_text(rendered, limit=2000)


def _record_rollout_failure(
    *,
    evidence: EvidenceDirectory,
    step: Step,
    reason: str,
) -> None:
    """Persist one normalized stage classification before returning non-zero."""
    safe_reason = redact_rollout_text(reason, limit=512).strip() or "rollout step failed"
    try:
        failure = classify_rollout_failure(
            step_number=step.number,
            step_name=step.name,
            reason=safe_reason,
        )
    except ValueError:
        # Test/plugin step inventories are permitted by ``run_rollout``.  The
        # production inventory is separately proven exact against coverage.
        return
    evidence.write_failure(failure.to_dict())


def _record_step_callback_failure(
    *,
    state: RolloutState,
    step: Step,
    ctx: RolloutContext,
    step_dir: StepDir,
    evidence: EvidenceDirectory,
    stream: TextIO,
    callback: str,
    exc: Exception,
    started_at: str,
) -> int:
    """Convert an ordinary step callback exception into safe terminal evidence."""
    _scrub_step_diagnostics(step_dir)
    safe_error = _safe_exception_text(exc)
    _emit(
        evidence,
        stream,
        f"[fail ] {step.number:02d}-{step.name}: {callback} exception: {safe_error}\n",
    )
    state.mark_step_failed(
        step.number,
        finished_at=_utc_now_iso(),
        error=safe_error,
    )
    _clear_driver_and_save(state, evidence)
    _write_step_result(
        step,
        step_dir,
        evidence,
        state="failed",
        error=safe_error,
        summary="",
        exit_code=1,
        started_at=started_at,
        inputs_hash=_INPUTS_HASH_UNAVAILABLE_AFTER_CALLBACK,
    )
    _record_rollout_failure(
        evidence=evidence,
        step=step,
        reason=f"{callback} exception: {safe_error}",
    )
    return 1


def _record_strict_callback_stop(
    *,
    state: RolloutState,
    step: Step,
    step_dir: StepDir,
    evidence: EvidenceDirectory,
    stream: TextIO,
    callback: str,
    exc: Exception,
) -> int:
    """Contain a strict verification callback without losing trusted evidence.

    A strict step may already have mutated the live environment and persisted a
    canonical VERIFYING or DONE result. Converting a verifier exception into
    FAILED would make the next invocation eligible to repeat that mutation.
    Preserve the strict state/result, redact the diagnostic, and require an
    explicit operator reset instead.
    """
    _scrub_step_diagnostics(step_dir)
    safe_error = _safe_exception_text(exc)
    _emit(
        evidence,
        stream,
        f"[stop ] {step.number:02d}-{step.name}: {callback} exception: "
        f"{safe_error}. Strict evidence was preserved and no mutation was "
        "attempted; investigate before an explicit reset.\n",
    )
    _clear_driver_and_save(state, evidence)
    _record_rollout_failure(
        evidence=evidence,
        step=step,
        reason=f"{callback} strict verification exception: {safe_error}",
    )
    return 2


def run_rollout(
    ctx: RolloutContext,
    steps: Sequence[Step],
    evidence: EvidenceDirectory,
    stream: TextIO | None = None,
) -> int:
    known_secrets = known_secrets_from_sources(
        (
            ctx.admin_token_source,
            ctx.worker_token_source,
            ctx.service_token_source,
            ctx.smoke_api_token_source,
        )
    )
    with rollout_redaction_scope(known_secrets):
        return _run_rollout_scoped(ctx, steps, evidence, stream)


def _run_rollout_scoped(
    ctx: RolloutContext,
    steps: Sequence[Step],
    evidence: EvidenceDirectory,
    stream: TextIO | None = None,
) -> int:
    """Execute (or resume) a rollout end-to-end.

    Returns 0 if every step reaches DONE; non-zero if any step fails or
    an invariant refuses to advance. State is persisted after every
    transition; the caller may safely interrupt and resume.
    """
    stream = stream or sys.stderr
    preflight_ctx(ctx)
    resume_material = _load_resume_material(ctx, evidence, steps)
    initializing_manual_resume = resume_material == (None, None)
    evidence.ensure()
    check_inputs_match(
        ctx,
        evidence,
        preloaded_inputs=resume_material[0] if resume_material is not None else None,
        initialize_missing=initializing_manual_resume,
    )

    state = load_or_init_rollout(
        ctx,
        evidence,
        steps,
        preloaded_state=resume_material[1] if resume_material is not None else None,
        initialize_new=initializing_manual_resume,
    )
    driver = _current_driver_identity(ctx)
    if state.driver and _driver_record_is_alive(state.driver, driver):
        owner = state.driver
        raise DriverError(
            "refusing to run: rollout already active under driver "
            f"pid={owner.pid} host={owner.hostname} "
            f"updated_at={owner.updated_at}. Wait for it to finish, "
            "or resume after that process exits."
        )
    if state.driver:
        _emit(
            evidence,
            stream,
            "[stale] previous rollout driver "
            f"pid={state.driver.pid} host={state.driver.hostname} "
            "is no longer active; resuming from persisted state\n",
        )
        state.clear_driver()
    _save_state(state, evidence, driver)

    for step in steps:
        record = next(
            (r for r in state.steps if r.number == step.number),
            None,
        )
        if record is None:
            # steps list changed vs. what was persisted — refuse.
            raise DriverError(
                f"step {step.number}:{step.name} is not in the persisted "
                "state.json. The step sequence changed between runs."
            )

        # Fast path — already done and inputs match. A strict step must never
        # turn missing/malformed DONE evidence into an implicit mutation.
        step_dir = evidence.step_dir(step.number, step.name)
        try:
            strict_live_verify = step.requires_strict_live_verification()
        except Exception as exc:
            return _record_step_callback_failure(
                state=state,
                step=step,
                ctx=ctx,
                step_dir=step_dir,
                evidence=evidence,
                stream=stream,
                callback="requires_strict_live_verification",
                exc=exc,
                started_at=record.started_at or _utc_now_iso(),
            )
        if record.state is StepState.DONE:
            try:
                done_evidence_matches = step.is_done(ctx, step_dir)
            except Exception as exc:
                if strict_live_verify:
                    return _record_strict_callback_stop(
                        state=state,
                        step=step,
                        step_dir=step_dir,
                        evidence=evidence,
                        stream=stream,
                        callback="is_done",
                        exc=exc,
                    )
                return _record_step_callback_failure(
                    state=state,
                    step=step,
                    ctx=ctx,
                    step_dir=step_dir,
                    evidence=evidence,
                    stream=stream,
                    callback="is_done",
                    exc=exc,
                    started_at=record.started_at or _utc_now_iso(),
                )
            if done_evidence_matches and strict_live_verify:
                done_evidence_matches = _strict_done_result_is_canonical(
                    step,
                    ctx,
                    step_dir,
                    expected_started_at=record.started_at,
                    expected_finished_at=record.finished_at,
                )
            if not done_evidence_matches and strict_live_verify:
                _emit(
                    evidence,
                    stream,
                    f"[stop ] {step.number:02d}-{step.name}: persisted DONE "
                    "result evidence is missing, malformed, or no longer matches "
                    "the canonical success and artifact contract. No live verify "
                    "or mutation was attempted. Investigate the sanitized evidence "
                    "and explicitly reset the step before any new run.\n",
                )
                _clear_driver_and_save(state, evidence)
                _record_rollout_failure(
                    evidence=evidence,
                    step=step,
                    reason="persisted DONE evidence failed the strict contract",
                )
                return 2
            if done_evidence_matches:
                try:
                    done_outcome = step.verify_done(ctx, step_dir)
                except Exception as exc:
                    if strict_live_verify:
                        return _record_strict_callback_stop(
                            state=state,
                            step=step,
                            step_dir=step_dir,
                            evidence=evidence,
                            stream=stream,
                            callback="verify_done",
                            exc=exc,
                        )
                    return _record_step_callback_failure(
                        state=state,
                        step=step,
                        ctx=ctx,
                        step_dir=step_dir,
                        evidence=evidence,
                        stream=stream,
                        callback="verify_done",
                        exc=exc,
                        started_at=record.started_at or _utc_now_iso(),
                    )
                if done_outcome is None or done_outcome is VerifyOutcome.MATCH:
                    _emit(evidence, stream, f"[skip ] {step.number:02d}-{step.name}\n")
                    continue
                if done_outcome in (VerifyOutcome.MISMATCH, VerifyOutcome.UNKNOWN):
                    outcome_label = done_outcome.value.upper()
                    diagnostic = (
                        "The completed step's live state has drifted from its "
                        "recorded contract. Explicitly investigate and reset the "
                        "step before allowing mutation."
                        if done_outcome is VerifyOutcome.MISMATCH
                        else (
                            "The world's state can't be confirmed. Investigate "
                            "before explicitly resetting or re-running the step."
                        )
                    )
                    _emit(
                        evidence,
                        stream,
                        f"[stop ] {step.number:02d}-{step.name}: completed-step "
                        f"revalidation returned {outcome_label}. No mutation was "
                        f"attempted. {diagnostic}\n",
                    )
                    _clear_driver_and_save(state, evidence)
                    _record_rollout_failure(
                        evidence=evidence,
                        step=step,
                        reason=f"completed-step revalidation returned {outcome_label}",
                    )
                    return 2

        # Recovery path — persisted state says something was running.
        if record.state in (StepState.RUNNING, StepState.VERIFYING):
            pending: _PendingRunEvidence | None = None
            if strict_live_verify:
                try:
                    pending = _load_pending_run_evidence(
                        step,
                        ctx,
                        step_dir,
                        expected_started_at=record.started_at,
                    )
                except DriverError as exc:
                    _emit(
                        evidence,
                        stream,
                        f"[stop ] {step.number:02d}-{step.name}: {exc}. "
                        "No live verification or mutation was attempted; "
                        "investigate the pending evidence before an explicit "
                        "reset.\n",
                    )
                    _clear_driver_and_save(state, evidence)
                    _record_rollout_failure(
                        evidence=evidence,
                        step=step,
                        reason=f"strict pending evidence is invalid: {exc}",
                    )
                    return 2
            _emit(
                evidence,
                stream,
                f"[verify] {step.number:02d}-{step.name} (persisted state={record.state.value})\n",
            )
            state.mark_step_verifying(step.number)
            _save_state(state, evidence, driver)
            try:
                outcome = step.verify(ctx, step_dir)
            except Exception as exc:
                if strict_live_verify:
                    return _record_strict_callback_stop(
                        state=state,
                        step=step,
                        step_dir=step_dir,
                        evidence=evidence,
                        stream=stream,
                        callback="verify",
                        exc=exc,
                    )
                return _record_step_callback_failure(
                    state=state,
                    step=step,
                    ctx=ctx,
                    step_dir=step_dir,
                    evidence=evidence,
                    stream=stream,
                    callback="verify",
                    exc=exc,
                    started_at=record.started_at or _utc_now_iso(),
                )
            if outcome is VerifyOutcome.MATCH:
                if strict_live_verify:
                    if pending is None:  # Defensive: strict validation set it above.
                        raise DriverError("strict pending evidence was not validated")
                    failure_rc = _finalise_done(
                        state,
                        step,
                        ctx,
                        step_dir,
                        evidence,
                        driver,
                        stream=stream,
                        summary=pending.summary,
                        started_at=pending.started_at,
                        artifacts=pending.artifacts,
                        inputs_hash=pending.inputs_hash,
                    )
                else:
                    failure_rc = _finalise_done(
                        state,
                        step,
                        ctx,
                        step_dir,
                        evidence,
                        driver,
                        stream=stream,
                    )
                if failure_rc is not None:
                    return failure_rc
                _emit(evidence, stream, f"[done ] {step.number:02d}-{step.name}\n")
                continue
            if strict_live_verify:
                _emit(
                    evidence,
                    stream,
                    f"[stop ] {step.number:02d}-{step.name}: strict recovery "
                    f"verification returned {outcome.value.upper()}. The step "
                    "remains VERIFYING and no mutation was attempted. Investigate "
                    "and explicitly reset it if another run is required.\n",
                )
                _clear_driver_and_save(state, evidence)
                _record_rollout_failure(
                    evidence=evidence,
                    step=step,
                    reason=f"strict recovery verification returned {outcome.value}",
                )
                return 2
            if outcome is VerifyOutcome.UNKNOWN:
                _emit(
                    evidence,
                    stream,
                    f"[stop ] {step.number:02d}-{step.name}: verify "
                    "returned UNKNOWN. The world's state can't be "
                    "confirmed. Investigate before re-running.\n",
                )
                _clear_driver_and_save(state, evidence)
                _record_rollout_failure(
                    evidence=evidence,
                    step=step,
                    reason="recovery verification returned unknown",
                )
                return 2
            # MISMATCH → drop back to RUNNING and re-run below.

        # Run (or re-run) the step.
        state.reset_step_for_retry(step.number, started_at=_utc_now_iso())
        _save_state(state, evidence, driver)
        _emit(evidence, stream, f"[run  ] {step.number:02d}-{step.name}\n")

        try:
            result = step.run(ctx, step_dir)
        except Exception as exc:
            _scrub_step_diagnostics(step_dir)
            safe_error = _safe_exception_text(exc)
            _emit(
                evidence,
                stream,
                f"[fail ] {step.number:02d}-{step.name}: exception: {safe_error}\n",
            )
            state.mark_step_failed(
                step.number,
                finished_at=_utc_now_iso(),
                error=safe_error,
            )
            _clear_driver_and_save(state, evidence)
            _write_step_result(
                step,
                step_dir,
                evidence,
                state="failed",
                error=safe_error,
                summary="",
                exit_code=1,
                started_at=record.started_at or _utc_now_iso(),
                inputs_hash=_INPUTS_HASH_UNAVAILABLE_AFTER_STEP_FAILURE,
            )
            _record_rollout_failure(
                evidence=evidence,
                step=step,
                reason=f"run exception: {safe_error}",
            )
            return 1

        _scrub_step_diagnostics(step_dir)

        if not result.is_success():
            _emit(
                evidence,
                stream,
                f"[fail ] {step.number:02d}-{step.name}: "
                f"exit={result.exit_code} error={result.error!r}\n",
            )
            state.mark_step_failed(
                step.number,
                finished_at=_utc_now_iso(),
                error=result.error or f"exit_code={result.exit_code}",
            )
            _clear_driver_and_save(state, evidence)
            _write_step_result(
                step,
                step_dir,
                evidence,
                state="failed",
                exit_code=result.exit_code,
                error=result.error,
                summary=result.summary,
                started_at=record.started_at or _utc_now_iso(),
                inputs_hash=_INPUTS_HASH_UNAVAILABLE_AFTER_STEP_FAILURE,
            )
            _record_rollout_failure(
                evidence=evidence,
                step=step,
                reason=result.error or f"exit code {result.exit_code}",
            )
            return result.exit_code

        # Step run() succeeded. Strict live-contract steps persist the run's
        # summary/artifacts before observation so an UNKNOWN pause can resume
        # without losing candidate-bound evidence or repeating mutation.
        strict_inputs_hash: str | None = None
        if strict_live_verify:
            try:
                strict_inputs_hash = step.inputs_hash(ctx)
            except Exception as exc:
                return _record_step_callback_failure(
                    state=state,
                    step=step,
                    ctx=ctx,
                    step_dir=step_dir,
                    evidence=evidence,
                    stream=stream,
                    callback="inputs_hash",
                    exc=exc,
                    started_at=record.started_at or _utc_now_iso(),
                )
            _write_step_result(
                step,
                step_dir,
                evidence,
                state="verifying",
                exit_code=0,
                error=None,
                summary=result.summary,
                started_at=record.started_at or _utc_now_iso(),
                artifacts=dict(result.artifacts),
                inputs_hash=strict_inputs_hash,
            )

        # Move to VERIFYING and observe the live contract.
        state.mark_step_verifying(step.number)
        _save_state(state, evidence, driver)
        try:
            outcome = step.verify(ctx, step_dir)
        except Exception as exc:
            if strict_live_verify:
                return _record_strict_callback_stop(
                    state=state,
                    step=step,
                    step_dir=step_dir,
                    evidence=evidence,
                    stream=stream,
                    callback="verify",
                    exc=exc,
                )
            return _record_step_callback_failure(
                state=state,
                step=step,
                ctx=ctx,
                step_dir=step_dir,
                evidence=evidence,
                stream=stream,
                callback="verify",
                exc=exc,
                started_at=record.started_at or _utc_now_iso(),
            )
        if strict_live_verify and outcome is not VerifyOutcome.MATCH:
            _emit(
                evidence,
                stream,
                f"[stop ] {step.number:02d}-{step.name}: strict post-run "
                f"verification returned {outcome.value.upper()}. Successful-run "
                "evidence was preserved, the step remains VERIFYING, and no "
                "further mutation was attempted. Investigate and explicitly "
                "reset it if another run is required.\n",
            )
            _clear_driver_and_save(state, evidence)
            _record_rollout_failure(
                evidence=evidence,
                step=step,
                reason=f"strict post-run verification returned {outcome.value}",
            )
            return 2
        if outcome is VerifyOutcome.MISMATCH:
            # Rare — the run reported success but verify says the
            # observable state doesn't match. Refuse without retrying
            # so an operator can look at what happened.
            _emit(
                evidence,
                stream,
                f"[stop ] {step.number:02d}-{step.name}: run() reported "
                "success but verify() said MISMATCH. Investigate.\n",
            )
            state.mark_step_failed(
                step.number,
                finished_at=_utc_now_iso(),
                error="run reported success but verify said MISMATCH",
            )
            _clear_driver_and_save(state, evidence)
            _record_rollout_failure(
                evidence=evidence,
                step=step,
                reason="run succeeded but verification mismatched",
            )
            return 2
        # Non-strict steps retain the historical UNKNOWN-accept behavior.
        # Strict steps reach here only on MATCH.
        failure_rc = _finalise_done(
            state,
            step,
            ctx,
            step_dir,
            evidence,
            driver,
            stream=stream,
            summary=result.summary,
            started_at=record.started_at or _utc_now_iso(),
            artifacts=dict(result.artifacts),
            inputs_hash=strict_inputs_hash,
        )
        if failure_rc is not None:
            return failure_rc
        _emit(evidence, stream, f"[done ] {step.number:02d}-{step.name}\n")

    _clear_driver_and_save(state, evidence)
    return 0


def _finalise_done(
    state: RolloutState,
    step: Step,
    ctx: RolloutContext,
    step_dir: StepDir,
    evidence: EvidenceDirectory,
    driver: DriverRecord,
    *,
    stream: TextIO,
    summary: str = "",
    started_at: str | None = None,
    artifacts: dict[str, object] | None = None,
    inputs_hash: str | None = None,
) -> int | None:
    effective_started_at = started_at or _utc_now_iso()
    effective_inputs_hash = inputs_hash
    if effective_inputs_hash is None:
        try:
            effective_inputs_hash = step.inputs_hash(ctx)
        except Exception as exc:
            return _record_step_callback_failure(
                state=state,
                step=step,
                ctx=ctx,
                step_dir=step_dir,
                evidence=evidence,
                stream=stream,
                callback="inputs_hash",
                exc=exc,
                started_at=effective_started_at,
            )
    finished_at = _utc_now_iso()
    # Persist the completed result before advancing state to DONE. A crash can
    # therefore leave an observable non-DONE state, but never a DONE state that
    # points at stale VERIFYING evidence and could trigger an implicit rerun.
    _write_step_result(
        step,
        step_dir,
        evidence,
        state="done",
        exit_code=0,
        error=None,
        summary=summary,
        started_at=effective_started_at,
        finished_at=finished_at,
        artifacts=artifacts or {},
        inputs_hash=effective_inputs_hash,
    )
    state.mark_step_done(
        step.number,
        finished_at=finished_at,
        inputs_hash=effective_inputs_hash,
    )
    if state.status == "done":
        _clear_driver_and_save(state, evidence)
    else:
        _save_state(state, evidence, driver)
    return None


def _write_step_result(
    step: Step,
    step_dir: StepDir,
    evidence: EvidenceDirectory,
    *,
    state: str,
    exit_code: int,
    error: str | None,
    summary: str,
    started_at: str,
    finished_at: str | None = None,
    artifacts: dict[str, object] | None = None,
    inputs_hash: str,
) -> None:
    payload = step_result_dict(
        step=step,
        state=state,
        inputs_hash=inputs_hash,
        started_at=started_at,
        finished_at=finished_at or _utc_now_iso(),
        exit_code=exit_code,
        error=error,
        summary=summary,
        artifacts=artifacts or {},
    )
    evidence.write_step_result(step_dir, payload)
