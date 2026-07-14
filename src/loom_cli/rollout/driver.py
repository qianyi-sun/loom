"""Rollout driver orchestrator (#340).

Iterates a static list of :class:`Step` implementations. Persists state
after every transition so a re-run picks up where the previous
invocation stopped. Refuses conflicting operator flags early.

The resume algorithm is documented on the design comment in the issue.
Summary:

1. ``is_done()`` → skip (with an inputs-hash gate against stale runs).
2. Persisted state ``RUNNING`` or ``VERIFYING`` → call ``verify()``.
     MATCH   → mark done, continue.
     MISMATCH → reset to RUNNING, call ``run()``.
     UNKNOWN → refuse to advance; print a diagnostic.
3. Persisted state ``NOT_STARTED`` or ``FAILED`` → call ``run()``.

Every step's ``run()`` writes its evidence dir before returning; the
driver writes the top-level ``state.json`` after each transition.

Concurrency: the driver expects the caller to have already acquired
the rollout mutation lock (via ``loom_cli.rollout_lock``). This module
is single-writer for the evidence tree.
"""

from __future__ import annotations

import os
import socket
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import EvidenceDirectory, StepDir
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
    return 1


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

        # Fast path — already done and inputs match.
        step_dir = evidence.step_dir(step.number, step.name)
        if record.state is StepState.DONE:
            try:
                already_done = step.is_done(ctx, step_dir)
            except Exception as exc:
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
            if already_done:
                _emit(evidence, stream, f"[skip ] {step.number:02d}-{step.name}\n")
                continue

        # Recovery path — persisted state says something was running.
        if record.state in (StepState.RUNNING, StepState.VERIFYING):
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
            if outcome is VerifyOutcome.UNKNOWN:
                _emit(
                    evidence,
                    stream,
                    f"[stop ] {step.number:02d}-{step.name}: verify "
                    "returned UNKNOWN. The world's state can't be "
                    "confirmed. Investigate before re-running.\n",
                )
                _clear_driver_and_save(state, evidence)
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
            return result.exit_code

        # Step run() succeeded. Move to VERIFYING → DONE.
        state.mark_step_verifying(step.number)
        _save_state(state, evidence, driver)
        try:
            outcome = step.verify(ctx, step_dir)
        except Exception as exc:
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
            return 2
        # UNKNOWN or MATCH both accept the success from run(). MATCH is
        # the ideal; UNKNOWN means "we didn't verify but the step self-
        # reported success" — this is the norm for steps whose observable
        # state is expensive/impossible to poll (e.g. write to disk).
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
) -> int | None:
    effective_started_at = started_at or _utc_now_iso()
    try:
        inputs_hash = step.inputs_hash(ctx)
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
    state.mark_step_done(
        step.number,
        finished_at=finished_at,
        inputs_hash=inputs_hash,
    )
    if state.status == "done":
        _clear_driver_and_save(state, evidence)
    else:
        _save_state(state, evidence, driver)
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
        inputs_hash=inputs_hash,
    )
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
