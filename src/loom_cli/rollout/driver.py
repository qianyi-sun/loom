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
from loom_cli.rollout.state import DriverRecord, RolloutState, StepState
from loom_cli.rollout.steps.base import (
    Step,
    VerifyOutcome,
    step_result_dict,
)


class DriverError(RuntimeError):
    """Raised by the driver when a step or invariant fails."""


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


def _current_driver_identity() -> DriverRecord:
    now = _utc_now_iso()
    return DriverRecord(
        pid=os.getpid(),
        hostname=socket.gethostname(),
        boot_id=_boot_id(),
        started_at=now,
        updated_at=now,
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
        )
    )


def _save_state(
    state: RolloutState,
    evidence: EvidenceDirectory,
    driver: DriverRecord,
) -> None:
    _refresh_driver(state, driver)
    state.save(evidence.state_path())


def _clear_driver_and_save(
    state: RolloutState,
    evidence: EvidenceDirectory,
) -> None:
    state.clear_driver()
    state.save(evidence.state_path())


def _emit(evidence: EvidenceDirectory, stream: TextIO, text: str) -> None:
    stream.write(text)
    log_path = evidence.driver_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(text)


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
    return "\n".join(lines) if lines else "  (no differences)"


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
) -> RolloutState:
    """Load the persisted RolloutState if one exists; otherwise create."""
    state_path = evidence.state_path()
    if state_path.is_file():
        loaded = RolloutState.load(state_path)
        return loaded
    inventory = [(step.number, step.name) for step in steps]
    return RolloutState.new(rollout_id=evidence.rollout_id, steps=inventory)


def check_inputs_match(
    ctx: RolloutContext,
    evidence: EvidenceDirectory,
) -> None:
    """If a persisted inputs.json exists, ensure it matches the current
    context; otherwise write it. Refuses on mismatch."""
    if evidence.inputs_path().is_file():
        persisted = evidence.read_inputs()
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


def run_rollout(
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
    evidence.ensure()
    check_inputs_match(ctx, evidence)

    state = load_or_init_rollout(ctx, evidence, steps)
    driver = _current_driver_identity()
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
        if record.state is StepState.DONE and step.is_done(ctx, step_dir):
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
            outcome = step.verify(ctx, step_dir)
            if outcome is VerifyOutcome.MATCH:
                _finalise_done(state, step, ctx, step_dir, evidence, driver)
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
            _emit(
                evidence,
                stream,
                f"[fail ] {step.number:02d}-{step.name}: exception: {exc}\n",
            )
            state.mark_step_failed(
                step.number,
                finished_at=_utc_now_iso(),
                error=str(exc),
            )
            _clear_driver_and_save(state, evidence)
            _write_step_result(
                step,
                ctx,
                step_dir,
                evidence,
                state="failed",
                error=str(exc),
                summary="",
                exit_code=1,
                started_at=record.started_at or _utc_now_iso(),
            )
            return 1

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
                ctx,
                step_dir,
                evidence,
                state="failed",
                exit_code=result.exit_code,
                error=result.error,
                summary=result.summary,
                started_at=record.started_at or _utc_now_iso(),
            )
            return result.exit_code

        # Step run() succeeded. Move to VERIFYING → DONE.
        state.mark_step_verifying(step.number)
        _save_state(state, evidence, driver)
        outcome = step.verify(ctx, step_dir)
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
        _finalise_done(
            state,
            step,
            ctx,
            step_dir,
            evidence,
            driver,
            summary=result.summary,
            started_at=record.started_at or _utc_now_iso(),
            artifacts=dict(result.artifacts),
        )
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
    summary: str = "",
    started_at: str | None = None,
    artifacts: dict[str, object] | None = None,
) -> None:
    inputs_hash = step.inputs_hash(ctx)
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
        ctx,
        step_dir,
        evidence,
        state="done",
        exit_code=0,
        error=None,
        summary=summary,
        started_at=started_at or finished_at,
        finished_at=finished_at,
        artifacts=artifacts or {},
    )


def _write_step_result(
    step: Step,
    ctx: RolloutContext,
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
) -> None:
    payload = step_result_dict(
        step=step,
        state=state,
        inputs_hash=step.inputs_hash(ctx),
        started_at=started_at,
        finished_at=finished_at or _utc_now_iso(),
        exit_code=exit_code,
        error=error,
        summary=summary,
        artifacts=artifacts or {},
    )
    evidence.write_step_result(step_dir, payload)
