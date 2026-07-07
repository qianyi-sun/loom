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

STATE_VERSION = 1


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

    def to_dict(self) -> dict[str, Any]:
        return {
            "pid": self.pid,
            "hostname": self.hostname,
            "boot_id": self.boot_id,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DriverRecord:
        return cls(
            pid=int(data["pid"]),
            hostname=str(data["hostname"]),
            boot_id=data.get("boot_id"),
            started_at=str(data["started_at"]),
            updated_at=str(data["updated_at"]),
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

    @classmethod
    def new(cls, *, rollout_id: str, steps: list[tuple[int, str]]) -> RolloutState:
        return cls(
            rollout_id=rollout_id,
            steps=[StepRecord(number=n, name=name) for n, name in steps],
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
        record.error = error
        self.status = "failed"

    def mark_driver_active(self, record: DriverRecord) -> None:
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
        return {
            "version": STATE_VERSION,
            "rollout_id": self.rollout_id,
            "status": self.status,
            "current_step": self.current_step,
            "driver": self.driver.to_dict() if self.driver else None,
            "steps": [r.to_dict() for r in self.steps],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RolloutState:
        version = int(data.get("version", 0))
        if version != STATE_VERSION:
            raise ValueError(
                f"unsupported state.json version {version}; driver expects {STATE_VERSION}"
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
