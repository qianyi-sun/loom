"""Rollout state machine + JSON persistence (#340).

The driver executes a sequence of numbered steps. Each step has a small
finite state machine: ``not_started → running → verifying → done | failed``.
The state of every step + the overall rollout is persisted to
``state.json`` in the evidence directory after every state transition
so that a re-run after an interrupted SSH session can pick up exactly
where it left off.

This module owns:

* :class:`StepState` — the per-step FSM enum.
* :class:`StepRecord` — one row in ``state.json``'s ``steps`` list.
* :class:`RolloutState` — the top-level ``state.json`` document + the
  transition methods the driver calls.

Persistence semantics: writes go through :meth:`RolloutState.save`,
which serialises the whole state.json atomically (tmp-file + rename)
so that a partial write during a crash never leaves an unparseable
document behind.
"""

from __future__ import annotations

import enum
import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loom_cli.rollout.operator.redaction import (
    redact_rollout_mapping,
    redact_rollout_text,
)

STATE_VERSION = 2

_ATTRIBUTION_FIELDS = (
    "request_id",
    "initiating_operator",
    "initiating_uid",
    "attempt_number",
    "attempt_operator",
    "attempt_uid",
)


def _strict_positive_int(value: object, *, field_name: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _strict_uid(value: object, *, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _strict_nonempty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _validate_attempt_attribution(
    attempt_number: object,
    attempt_operator: object,
    attempt_uid: object,
) -> tuple[int | None, str | None, int | None]:
    values = (attempt_number, attempt_operator, attempt_uid)
    if all(value is None for value in values):
        return None, None, None
    if any(value is None for value in values):
        raise ValueError("attempt attribution must be present all-or-none")
    try:
        return (
            _strict_positive_int(attempt_number, field_name="attempt_number"),
            _strict_nonempty_string(
                attempt_operator,
                field_name="attempt_operator",
            ),
            _strict_uid(attempt_uid, field_name="attempt_uid"),
        )
    except ValueError as exc:
        raise ValueError(f"invalid attempt attribution: {exc}") from None


def _validate_rollout_attribution(
    request_id: object,
    initiating_operator: object,
    initiating_uid: object,
    attempt_number: object,
    attempt_operator: object,
    attempt_uid: object,
) -> tuple[str | None, str | None, int | None, int | None, str | None, int | None]:
    values = (
        request_id,
        initiating_operator,
        initiating_uid,
        attempt_number,
        attempt_operator,
        attempt_uid,
    )
    if all(value is None for value in values):
        return None, None, None, None, None, None
    if any(value is None for value in values):
        raise ValueError("rollout attribution must be present all-or-none")
    try:
        checked_attempt = _validate_attempt_attribution(
            attempt_number,
            attempt_operator,
            attempt_uid,
        )
        return (
            _strict_nonempty_string(request_id, field_name="request_id"),
            _strict_nonempty_string(
                initiating_operator,
                field_name="initiating_operator",
            ),
            _strict_uid(initiating_uid, field_name="initiating_uid"),
            *checked_attempt,
        )
    except ValueError as exc:
        raise ValueError(f"invalid rollout attribution: {exc}") from None


class StepState(enum.StrEnum):
    """The per-step finite state machine (#340).

    ``NOT_STARTED`` — the driver has not attempted the step yet.
    ``RUNNING`` — the step's ``run()`` has been dispatched. Interrupted
        rollouts most commonly leave the current step here.
    ``VERIFYING`` — ``run()`` returned success and the driver is
        asking the world "does the observable state match?" before
        marking the step done.
    ``DONE`` — verified and finalised. Skipped on resume as long as
        the ``inputs_hash`` still matches.
    ``FAILED`` — non-retryable failure. Resume re-enters the step
        via ``reset_step_for_retry`` after the operator addresses
        whatever the diagnostic surfaced.
    """

    NOT_STARTED = "not_started"
    RUNNING = "running"
    VERIFYING = "verifying"
    DONE = "done"
    FAILED = "failed"

    def is_terminal(self) -> bool:
        return self in (StepState.DONE, StepState.FAILED)

    def is_success(self) -> bool:
        return self is StepState.DONE


@dataclass
class DriverRecord:
    """Best-effort owner/heartbeat for the rollout driver process.

    The rollout evidence tree is a single-writer state machine. This record
    lets a later invocation distinguish "the previous driver is still alive"
    from "state.json was left running by a dead SSH/session process".
    """

    pid: int
    hostname: str
    boot_id: str | None
    started_at: str
    updated_at: str
    attempt_number: int | None = None
    attempt_operator: str | None = None
    attempt_uid: int | None = None

    def __post_init__(self) -> None:
        checked = _validate_attempt_attribution(
            self.attempt_number,
            self.attempt_operator,
            self.attempt_uid,
        )
        self.attempt_number, self.attempt_operator, self.attempt_uid = checked

    def to_dict(self) -> dict[str, Any]:
        return {
            "pid": self.pid,
            "hostname": self.hostname,
            "boot_id": self.boot_id,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "attempt_number": self.attempt_number,
            "attempt_operator": self.attempt_operator,
            "attempt_uid": self.attempt_uid,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DriverRecord:
        return cls(
            pid=int(data["pid"]),
            hostname=str(data["hostname"]),
            boot_id=data.get("boot_id"),
            started_at=str(data["started_at"]),
            updated_at=str(data["updated_at"]),
            attempt_number=data.get("attempt_number"),
            attempt_operator=data.get("attempt_operator"),
            attempt_uid=data.get("attempt_uid"),
        )


@dataclass
class StepRecord:
    """Persisted state for one step."""

    number: int
    name: str
    state: StepState = StepState.NOT_STARTED
    inputs_hash: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if self.error is not None:
            self.error = redact_rollout_text(self.error)

    def to_dict(self) -> dict[str, Any]:
        return {
            "number": self.number,
            "name": self.name,
            "state": self.state.value,
            "inputs_hash": self.inputs_hash,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StepRecord:
        state_str = data.get("state", "not_started")
        try:
            state = StepState(state_str)
        except ValueError:
            raise ValueError(
                f"unknown step state {state_str!r}; "
                "must be one of not_started/running/verifying/done/failed"
            ) from None
        return cls(
            number=int(data["number"]),
            name=str(data["name"]),
            state=state,
            inputs_hash=data.get("inputs_hash"),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            error=data.get("error"),
        )


@dataclass
class RolloutState:
    """Top-level state.json owner (#340).

    A rollout is ``running`` until either every step reaches
    :data:`StepState.DONE` (transition to ``done``) or a step reaches
    :data:`StepState.FAILED` (transition to ``failed``).
    """

    rollout_id: str
    steps: list[StepRecord] = field(default_factory=list)
    status: str = "running"  # "running" | "done" | "failed"
    current_step: int | None = None  # number of the last step marked running
    driver: DriverRecord | None = None
    request_id: str | None = None
    initiating_operator: str | None = None
    initiating_uid: int | None = None
    attempt_number: int | None = None
    attempt_operator: str | None = None
    attempt_uid: int | None = None

    def __post_init__(self) -> None:
        checked = _validate_rollout_attribution(
            self.request_id,
            self.initiating_operator,
            self.initiating_uid,
            self.attempt_number,
            self.attempt_operator,
            self.attempt_uid,
        )
        (
            self.request_id,
            self.initiating_operator,
            self.initiating_uid,
            self.attempt_number,
            self.attempt_operator,
            self.attempt_uid,
        ) = checked
        self._validate_driver_attempt_attribution()

    def _validate_driver_attempt_attribution(self) -> None:
        if self.request_id is None or self.driver is None:
            return
        state_attempt = (
            self.attempt_number,
            self.attempt_operator,
            self.attempt_uid,
        )
        driver_attempt = (
            self.driver.attempt_number,
            self.driver.attempt_operator,
            self.driver.attempt_uid,
        )
        if driver_attempt != state_attempt:
            raise ValueError(
                "driver current attempt attribution must match rollout attempt attribution"
            )

    @classmethod
    def new(
        cls,
        *,
        rollout_id: str,
        steps: list[tuple[int, str]],
        request_id: str | None = None,
        initiating_operator: str | None = None,
        initiating_uid: int | None = None,
        attempt_number: int | None = None,
        attempt_operator: str | None = None,
        attempt_uid: int | None = None,
    ) -> RolloutState:
        return cls(
            rollout_id=rollout_id,
            steps=[StepRecord(number=n, name=name) for n, name in steps],
            request_id=request_id,
            initiating_operator=initiating_operator,
            initiating_uid=initiating_uid,
            attempt_number=attempt_number,
            attempt_operator=attempt_operator,
            attempt_uid=attempt_uid,
        )

    def _find(self, number: int) -> StepRecord:
        for record in self.steps:
            if record.number == number:
                return record
        raise KeyError(f"no step with number {number}")

    def mark_step_running(self, number: int, *, started_at: str) -> None:
        record = self._find(number)
        record.state = StepState.RUNNING
        record.started_at = started_at
        record.finished_at = None
        record.error = None
        self.status = "running"
        self.current_step = number

    def mark_step_verifying(self, number: int) -> None:
        record = self._find(number)
        record.state = StepState.VERIFYING

    def reset_step_for_retry(self, number: int, *, started_at: str) -> None:
        """After verify() said MISMATCH: drop back to RUNNING to retry."""
        record = self._find(number)
        record.state = StepState.RUNNING
        record.started_at = started_at
        record.finished_at = None
        record.error = None
        self.status = "running"
        self.current_step = number

    def mark_step_done(
        self,
        number: int,
        *,
        finished_at: str,
        inputs_hash: str,
    ) -> None:
        record = self._find(number)
        record.state = StepState.DONE
        record.finished_at = finished_at
        record.inputs_hash = inputs_hash
        record.error = None
        # If every step is now DONE the rollout is DONE.
        if all(r.state is StepState.DONE for r in self.steps):
            self.status = "done"
            self.current_step = None

    def mark_step_failed(
        self,
        number: int,
        *,
        finished_at: str,
        error: str,
    ) -> None:
        record = self._find(number)
        record.state = StepState.FAILED
        record.finished_at = finished_at
        record.error = redact_rollout_text(error)
        self.status = "failed"

    def mark_driver_active(self, record: DriverRecord) -> None:
        if self.request_id is not None and (
            record.attempt_number,
            record.attempt_operator,
            record.attempt_uid,
        ) != (self.attempt_number, self.attempt_operator, self.attempt_uid):
            raise ValueError(
                "driver current attempt attribution must match rollout attempt attribution"
            )
        self.driver = record

    def clear_driver(self) -> None:
        self.driver = None

    def current_running_step(self) -> int | None:
        """Return the number of the step currently in RUNNING/VERIFYING,
        or None if no step is mid-flight."""
        for record in self.steps:
            if record.state in (StepState.RUNNING, StepState.VERIFYING):
                return record.number
        return None

    def to_dict(self) -> dict[str, Any]:
        self._validate_driver_attempt_attribution()
        payload = {
            "version": STATE_VERSION,
            "rollout_id": self.rollout_id,
            "status": self.status,
            "current_step": self.current_step,
            "driver": self.driver.to_dict() if self.driver else None,
            "request_id": self.request_id,
            "initiating_operator": self.initiating_operator,
            "initiating_uid": self.initiating_uid,
            "attempt_number": self.attempt_number,
            "attempt_operator": self.attempt_operator,
            "attempt_uid": self.attempt_uid,
            "steps": [r.to_dict() for r in self.steps],
        }
        redacted = redact_rollout_mapping(payload)
        if not isinstance(redacted, dict):  # pragma: no cover - mapping contract
            raise TypeError("redacted rollout state must remain a mapping")
        return redacted

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RolloutState:
        version = data.get("version", 0)
        if type(version) is not int:
            raise ValueError("state.json version must be an integer")
        if version not in {1, STATE_VERSION}:
            raise ValueError(
                f"unsupported state.json version {version}; driver expects 1 or {STATE_VERSION}"
            )
        attribution: tuple[
            str | None,
            str | None,
            int | None,
            int | None,
            str | None,
            int | None,
        ]
        if version == 1:
            attribution = (None, None, None, None, None, None)
        else:
            if not set(_ATTRIBUTION_FIELDS).issubset(data):
                raise ValueError("state.json version 2 attribution fields are required")
            attribution = _validate_rollout_attribution(
                data.get("request_id"),
                data.get("initiating_operator"),
                data.get("initiating_uid"),
                data.get("attempt_number"),
                data.get("attempt_operator"),
                data.get("attempt_uid"),
            )
        return cls(
            rollout_id=str(data["rollout_id"]),
            steps=[StepRecord.from_dict(s) for s in data.get("steps", [])],
            status=str(data.get("status", "running")),
            current_step=data.get("current_step"),
            driver=(
                DriverRecord.from_dict(data["driver"])
                if isinstance(data.get("driver"), dict)
                else None
            ),
            request_id=attribution[0],
            initiating_operator=attribution[1],
            initiating_uid=attribution[2],
            attempt_number=attribution[3],
            attempt_operator=attribution[4],
            attempt_uid=attribution[5],
        )

    def save(self, path: Path) -> None:
        """Atomic write. Fully write to a tmp file then rename."""
        path.parent.mkdir(parents=True, exist_ok=True)
        # NamedTemporaryFile → rename gives us atomicity on same-fs
        # writes. Crash between write and rename leaves the previous
        # state.json intact, which is the desired failure mode.
        with tempfile.NamedTemporaryFile(
            "w",
            dir=str(path.parent),
            suffix=".tmp",
            delete=False,
            encoding="utf-8",
        ) as f:
            json.dump(self.to_dict(), f, indent=2)
            f.write("\n")
            tmp_path = Path(f.name)
        os.replace(str(tmp_path), str(path))

    @classmethod
    def load(cls, path: Path) -> RolloutState:
        return cls.from_dict(json.loads(path.read_text()))
