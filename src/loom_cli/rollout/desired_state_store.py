"""Versioned desired-state store for the rollout reconciler (#1097 / #1085 phase 4).

Holds the *pinned desired version* for one environment — "deploy X" is a
compare-and-set of this store to point at X, and the reconciler reads it to learn
its target. The store is optimistic-concurrency guarded: every write carries the
generation it expected to overwrite, so two actors racing to change the target see
each other's intent instead of one silently clobbering the other.

The store is pure intent — it never touches a cluster. It is the "desired-state"
half of the reconciler's desired-vs-live model; the live half is read read-only by
the shadow observer (`shadow_reconcile`).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


class DesiredStateError(RuntimeError):
    """Raised when the desired-state store is malformed or misused."""


class ConcurrentUpdateError(DesiredStateError):
    """Raised when a compare-and-set loses the race (the generation moved)."""


@dataclass(frozen=True)
class DesiredState:
    """The pinned desired version for one environment, plus its CAS generation."""

    environment: str
    version: str
    generation: int
    updated_at: str
    updated_by: str
    note: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "environment": self.environment,
            "version": self.version,
            "generation": self.generation,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
            "note": self.note,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> DesiredState:
        if raw.get("schema_version") != 1:
            raise DesiredStateError("desired-state schema_version must be 1")
        try:
            environment = str(raw["environment"])
            version = str(raw["version"])
            generation = raw["generation"]
            updated_at = str(raw["updated_at"])
            updated_by = str(raw["updated_by"])
        except KeyError as exc:
            raise DesiredStateError(f"desired-state record missing key: {exc}") from exc
        if type(generation) is not int or generation < 1:
            raise DesiredStateError("desired-state generation must be a positive integer")
        if not version:
            raise DesiredStateError("desired-state version must be non-empty")
        return cls(
            environment=environment,
            version=version,
            generation=generation,
            updated_at=updated_at,
            updated_by=updated_by,
            note=str(raw.get("note", "")),
        )


class DesiredStateStore:
    """A file-backed, atomically-updated, CAS-guarded desired-state pointer.

    Concurrency is optimistic: `compare_and_set` refuses to write unless the caller
    named the generation it read. This is a small read-check-write window rather
    than a hard cross-process lock — enough for the single-writer-per-environment
    deploy path, and it turns a lost race into an explicit `ConcurrentUpdateError`
    rather than a silent overwrite.
    """

    def __init__(self, path: Path, *, environment: str) -> None:
        self._path = path
        self._environment = environment

    @property
    def environment(self) -> str:
        return self._environment

    def read(self) -> DesiredState | None:
        """Return the current desired state, or None if nothing is pinned yet."""
        try:
            raw_bytes = self._path.read_bytes()
        except FileNotFoundError:
            return None
        try:
            raw = json.loads(raw_bytes)
        except json.JSONDecodeError as exc:
            raise DesiredStateError(f"desired-state store is not valid JSON: {exc}") from exc
        if not isinstance(raw, dict):
            raise DesiredStateError("desired-state store must be a JSON object")
        state = DesiredState.from_dict(raw)
        if state.environment != self._environment:
            raise DesiredStateError(
                f"desired-state store environment {state.environment!r} "
                f"does not match {self._environment!r}"
            )
        return state

    def current_generation(self) -> int:
        """The generation to pass as `expected_generation`; 0 when nothing is pinned."""
        state = self.read()
        return state.generation if state is not None else 0

    def compare_and_set(
        self,
        version: str,
        *,
        expected_generation: int,
        updated_by: str,
        updated_at: str,
        note: str = "",
    ) -> DesiredState:
        """Pin `version` iff the store is still at `expected_generation`.

        Raises `ConcurrentUpdateError` if the generation moved (someone else wrote
        first). On success the generation advances by one.
        """
        if not version:
            raise DesiredStateError("desired-state version must be non-empty")
        current = self.current_generation()
        if expected_generation != current:
            raise ConcurrentUpdateError(
                f"desired-state generation moved: expected {expected_generation}, found {current}"
            )
        new_state = DesiredState(
            environment=self._environment,
            version=version,
            generation=current + 1,
            updated_at=updated_at,
            updated_by=updated_by,
            note=note,
        )
        self._atomic_write(new_state)
        return new_state

    def _atomic_write(self, state: DesiredState) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = (json.dumps(state.to_dict(), indent=2, sort_keys=True) + "\n").encode("utf-8")
        tmp = self._path.with_name(f".{self._path.name}.tmp.{os.getpid()}")
        try:
            with open(tmp, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self._path)
        finally:
            tmp.unlink(missing_ok=True)
