"""Create and retain the permanent legacy-writer fence under the cutover guard.

The enclosing installed cutover excludes other policy writers and their pending
requests. This helper creates only absent objects, retains their exact UIDs and
never replaces or removes them. Typechecked readback is not process/SQL retirement
or proof of API enforcement; the cutover must probe the live policies separately.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Protocol
from uuid import UUID

from .protected_execution_preparation_journal import (
    ExecutionPreparationOperationJournal,
    _require_directory,
)
from .protected_legacy_writer_fence import render_legacy_writer_fence

_RECORDS = frozenset({"installation.intent.json", "installation.terminal.json"} | {
    f"fence-{index:02d}.{phase}.json" for index in range(12) for phase in ("intent", "terminal")})
_TEMP = re.compile(r"^\.\.(?P<final>[a-z0-9.-]+\.json)\.loom-[0-9a-f]{32}\.tmp$")


def _wire(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False) + "\n").encode("ascii")


def _digest(value: object) -> str:
    return hashlib.sha256(_wire(value)).hexdigest()


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(type(key) is not str for key in value):
        raise ValueError("legacy writer fence mapping is invalid")
    return value


def _decode(payload: bytes) -> dict[str, object]:
    if not payload or len(payload) > 256 * 1024:
        raise ValueError("legacy writer fence object is absent or oversized")
    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("legacy writer fence object repeats fields")
            value[key] = item
        return value
    return _mapping(json.loads(payload, object_pairs_hook=unique))


@dataclass(frozen=True, slots=True)
class LegacyWriterFenceJournal(ExecutionPreparationOperationJournal):
    allowed_records: ClassVar[frozenset[str]] = _RECORDS

    @property
    def root(self) -> Path:
        return self.state_root / "protected-capacity" / "legacy-writer-fence-journals" / self.request_id / str(self.attempt_number)

    def _directories(self) -> tuple[Path, ...]:
        return (self.state_root, self.root.parents[2], self.root.parents[1], self.root.parent, self.root)

    def _ensure_directories(self) -> None:
        for path in self._directories():
            path.mkdir(mode=0o700, exist_ok=True)
            _require_directory(path, service_uid=self.service_uid)

    def _validate_directories(self) -> None:
        for path in self._directories():
            _require_directory(path, service_uid=self.service_uid)

    def read(self, name: str) -> dict[str, object] | None:
        if name not in self.allowed_records:
            raise ValueError("legacy writer fence record name is invalid")
        try:
            payload = self._read(name)
        except FileNotFoundError:
            return None
        value = _decode(payload)
        if _wire(value) != payload:
            raise RuntimeError("legacy writer fence record is not canonical")
        return value

    def retain(self, name: str, value: dict[str, object]) -> None:
        if name not in self.allowed_records:
            raise ValueError("legacy writer fence record name is invalid")
        self._publish(name, _wire(value))

    @contextmanager
    def exclusive(self) -> Iterator[None]:
        self._ensure_directories()
        descriptor = os.open(self.root.parents[1], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._validate_directories()
            for name in self._entry_names():
                temporary = _TEMP.fullmatch(name)
                if name not in self.allowed_records and (temporary is None or temporary.group("final") not in self.allowed_records):
                    raise RuntimeError("legacy writer fence journal contains unknown records")
            for name in self.allowed_records:
                if name.endswith(".terminal.json") and self.read(name) is not None and self.read(name.replace(".terminal.", ".intent.")) is None:
                    raise RuntimeError("legacy writer fence terminal lacks its intent")
            yield
        finally:
            os.close(descriptor)


class LegacyWriterFenceRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...
    def capture_stdout(self, argv: Sequence[str], *, env: Mapping[str, str], timeout_seconds: float) -> bytes: ...
    def capture_stdout_with_input(self, argv: Sequence[str], *, env: Mapping[str, str],
                                 input_payload: bytes, timeout_seconds: float) -> bytes: ...


class LegacyWriterFenceTypecheckPendingError(RuntimeError):
    """The exact new policy has not completed Kubernetes type checking."""


@dataclass(frozen=True, slots=True)
class LegacyWriterFenceInstallation:
    journal: LegacyWriterFenceJournal
    runner: LegacyWriterFenceRunner
    plan_digest: str
    control_plane_image: str
    guard: Callable[[], None]

    def __post_init__(self) -> None:
        self.documents()
        if not callable(self.guard):
            raise ValueError("legacy writer fence guard is invalid")

    def documents(self) -> tuple[dict[str, object], ...]:
        return render_legacy_writer_fence(intent_digest=self.plan_digest, control_plane_image=self.control_plane_image)

    def _intent(self) -> dict[str, object]:
        return {"schema_version": 1, "plan_digest": self.plan_digest,
            "documents_sha256": _digest(self.documents()), "control_plane_image": self.control_plane_image}

    def _read_object(self, document: dict[str, object]) -> bytes:
        self.guard()
        return self.runner.capture_stdout(("kubectl", "get", str(document["kind"]),
            str(_mapping(document["metadata"])["name"]), "--ignore-not-found=true", "--output=json", "--request-timeout=30s"),
            env=self.runner.environment, timeout_seconds=30)

    def _inspect(self, document: dict[str, object], payload: bytes, expected_uid: str | None, *, checked: bool) -> str:
        observed = _decode(payload)
        meta = _mapping(observed.get("metadata"))
        expected = _mapping(document["metadata"])
        uid = meta.get("uid")
        try:
            if not isinstance(uid, str) or str(UUID(uid)) != uid:
                raise ValueError
        except ValueError:
            raise ValueError("legacy writer fence UID is invalid") from None
        if (any(observed.get(key) != document[key] for key in ("apiVersion", "kind", "spec"))
            or meta.get("name") != expected["name"] or meta.get("annotations") != expected["annotations"]
            or meta.get("namespace") not in (None, "") or meta.get("generation") != 1
            or type(meta.get("generation")) is not int or not isinstance(meta.get("resourceVersion"), str)
            or re.fullmatch(r"[1-9][0-9]{0,31}", str(meta.get("resourceVersion"))) is None
            or meta.get("labels", {}) != {} or meta.get("ownerReferences", []) != [] or meta.get("finalizers", []) != []
            or meta.get("deletionTimestamp") is not None or meta.get("deletionGracePeriodSeconds") is not None
            or (expected_uid is not None and uid != expected_uid)):
            raise ValueError("legacy writer fence identity or specification drifted")
        if checked and document["kind"] == "ValidatingAdmissionPolicy":
            status = _mapping(observed.get("status", {}))
            if type(status.get("observedGeneration")) is not int or status.get("observedGeneration") != 1 or not isinstance(status.get("typeChecking"), dict):
                raise LegacyWriterFenceTypecheckPendingError("legacy writer fence type checking is pending")
            if _mapping(status["typeChecking"]).get("expressionWarnings", []) != []:
                raise ValueError("legacy writer fence has type checking warnings")
        return uid

    def install(self) -> tuple[dict[str, object], ...]:
        self.guard()
        with self.journal.exclusive():
            self.journal.retain("installation.intent.json", self._intent())
            for index, document in enumerate(self.documents()):
                intent_name, terminal_name = f"fence-{index:02d}.intent.json", f"fence-{index:02d}.terminal.json"
                pending, known = self.journal.read(intent_name), self.journal.read(terminal_name)
                payload = self._read_object(document)
                if known is not None and not payload:
                    raise RuntimeError("legacy writer fence retained object disappeared")
                intent = {"schema_version": 1, "plan_digest": self.plan_digest,
                    "document_sha256": _digest(document), "observed_state": "absent"}
                if pending is None and payload:
                    raise RuntimeError("legacy writer fence existing object is unowned")
                self.journal.retain(intent_name, intent)
                if not payload:
                    self.guard()
                    payload = self.runner.capture_stdout_with_input(("kubectl", "create", "--filename=-",
                        "--field-manager=loom-legacy-writer-fence", "--validate=strict", "--output=json", "--request-timeout=30s"),
                        env=self.runner.environment, input_payload=_wire(document), timeout_seconds=30)
                uid = self._inspect(document, payload, str(known["uid"]) if known else None, checked=False)
                self.journal.retain(terminal_name, {**intent, "uid": uid})
            deadline = time.monotonic() + 30
            while True:
                try:
                    evidence = self._observe()
                    break
                except LegacyWriterFenceTypecheckPendingError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.1)
            self.journal.retain("installation.terminal.json", {**self._intent(), "objects": list(evidence)})
            return evidence

    def observe(self) -> tuple[dict[str, object], ...]:
        self.guard()
        with self.journal.exclusive():
            if self.journal.read("installation.terminal.json") is None:
                raise RuntimeError("legacy writer fence is not installed")
            evidence = self._observe()
            if self.journal.read("installation.terminal.json") != {**self._intent(), "objects": list(evidence)}:
                raise RuntimeError("legacy writer fence terminal drifted")
            return evidence

    def _observe(self) -> tuple[dict[str, object], ...]:
        if self.journal.read("installation.intent.json") != self._intent():
            raise RuntimeError("legacy writer fence installation drifted")
        evidence = []
        for index, document in enumerate(self.documents()):
            known = self.journal.read(f"fence-{index:02d}.terminal.json")
            intent = {"schema_version": 1, "plan_digest": self.plan_digest,
                "document_sha256": _digest(document), "observed_state": "absent"}
            if (known is None or known != {**intent, "uid": known.get("uid")}
                or self.journal.read(f"fence-{index:02d}.intent.json") != intent):
                raise RuntimeError("legacy writer fence retained object is absent or changed")
            self._inspect(document, self._read_object(document), str(known["uid"]), checked=True)
            evidence.append(known)
        self.guard()
        return tuple(evidence)
