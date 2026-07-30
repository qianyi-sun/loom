"""Forward-only version ledger for imperative rollout ops (#1097 / #1085 phase 4).

The reconciler's declarative parts converge by re-reading live state, but ordered,
irreversible operations — DB migrations, the mutation-epoch, external-supervisor
transitions — must NOT be "re-run to converge". This ledger is their idempotency:
it records the highest applied ordinal per component, advances forward only, and
refuses to regress. The reconciler consults it to skip an op that is already
applied instead of naively replaying it (the #1061/#1081 crash-on-re-apply class).

Distinct from the desired-state store: that holds one *desired* pointer per
environment; this holds a map of *applied* positions per component. File-backed,
atomically written. Pure record-keeping — it never performs the op itself.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


class VersionLedgerError(RuntimeError):
    """Raised when the ledger is malformed or an update would regress."""


@dataclass(frozen=True)
class AppliedVersion:
    """The highest applied position for one imperative component."""

    component: str
    # A caller-owned monotonic ordinal (mutation epoch, migration ordinal, …).
    # Forward-only is enforced on this.
    ordinal: int
    # A human/audit label for the applied version (a SHA, alembic revision, …).
    label: str
    applied_at: str
    applied_by: str

    def to_dict(self) -> dict[str, object]:
        return {
            "component": self.component,
            "ordinal": self.ordinal,
            "label": self.label,
            "applied_at": self.applied_at,
            "applied_by": self.applied_by,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> AppliedVersion:
        try:
            component = str(raw["component"])
            ordinal = raw["ordinal"]
            label = str(raw["label"])
            applied_at = str(raw["applied_at"])
            applied_by = str(raw["applied_by"])
        except KeyError as exc:
            raise VersionLedgerError(f"ledger entry missing key: {exc}") from exc
        if type(ordinal) is not int or ordinal < 0:
            raise VersionLedgerError("ledger ordinal must be a non-negative integer")
        return cls(component, ordinal, label, applied_at, applied_by)


class VersionLedger:
    """A file-backed, forward-only record of applied positions per component."""

    def __init__(self, path: Path, *, environment: str) -> None:
        self._path = path
        self._environment = environment

    @property
    def environment(self) -> str:
        return self._environment

    def _load(self) -> dict[str, AppliedVersion]:
        try:
            raw_bytes = self._path.read_bytes()
        except FileNotFoundError:
            return {}
        try:
            raw = json.loads(raw_bytes)
        except json.JSONDecodeError as exc:
            raise VersionLedgerError(f"version ledger is not valid JSON: {exc}") from exc
        if not isinstance(raw, dict) or raw.get("schema_version") != 1:
            raise VersionLedgerError("version ledger schema_version must be 1")
        if raw.get("environment") != self._environment:
            raise VersionLedgerError(
                f"version ledger environment {raw.get('environment')!r} "
                f"does not match {self._environment!r}"
            )
        entries = raw.get("entries", {})
        if not isinstance(entries, dict):
            raise VersionLedgerError("version ledger entries must be an object")
        return {
            component: AppliedVersion.from_dict({"component": component, **entry})
            for component, entry in entries.items()
        }

    def applied(self, component: str) -> AppliedVersion | None:
        """The highest applied position for `component`, or None if never applied."""
        return self._load().get(component)

    def needs_apply(self, component: str, target_ordinal: int) -> bool:
        """True iff `target_ordinal` is ahead of what's applied (so must be run)."""
        current = self.applied(component)
        return current is None or target_ordinal > current.ordinal

    def record_applied(
        self,
        component: str,
        *,
        ordinal: int,
        label: str,
        applied_at: str,
        applied_by: str,
    ) -> AppliedVersion:
        """Record that `component` reached `ordinal`. Forward-only: refuses to regress.

        Re-recording the exact same ordinal+label is a tolerated no-op (idempotent
        retry of the same apply); a lower ordinal, or the same ordinal with a
        different label, is a regression and raises.
        """
        if ordinal < 0:
            raise VersionLedgerError("ledger ordinal must be a non-negative integer")
        entries = self._load()
        current = entries.get(component)
        if current is not None:
            if ordinal < current.ordinal:
                raise VersionLedgerError(
                    f"version ledger cannot regress {component}: "
                    f"applied {current.ordinal}, asked {ordinal}"
                )
            if ordinal == current.ordinal and label != current.label:
                raise VersionLedgerError(
                    f"version ledger conflict for {component} at ordinal {ordinal}: "
                    f"applied label differs from the requested one"
                )
        new_entry = AppliedVersion(component, ordinal, label, applied_at, applied_by)
        entries[component] = new_entry
        self._atomic_write(entries)
        return new_entry

    def _atomic_write(self, entries: dict[str, AppliedVersion]) -> None:
        document = {
            "schema_version": 1,
            "environment": self._environment,
            "entries": {
                component: {k: v for k, v in entry.to_dict().items() if k != "component"}
                for component, entry in sorted(entries.items())
            },
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode("utf-8")
        tmp = self._path.with_name(f".{self._path.name}.tmp.{os.getpid()}")
        try:
            with open(tmp, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self._path)
        finally:
            tmp.unlink(missing_ok=True)
