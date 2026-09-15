"""Retained one-slot activation over the installed prepared controller channels."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from loom_capacity_manager.contracts import SubjectConfigurationV1, canonical_digest
from loom_capacity_manager.executable_contracts import (
    ExecutionActivationV2,
    ExecutionAuthorityV2,
    ExecutionContextV2,
    ExecutionDrainV2,
    ExecutionPreparationAbortV2,
    canonical_executable_digest,
)

from .final_gate_plan import FinalGatePlan
from .protected_active_controller import ActiveControllerEvidence, ActiveControllerRequest
from .protected_capacity_execution_preparation_component import (
    PreparedControllerTransport,
    _controller_evidence_matches,
    _parse_manager_status,
    _readiness_is_exact,
    _require_shadow_readback,
)
from .protected_capacity_manager_client import (
    ProtectedExecutionPreparationAbortResult,
    ProtectedExecutionPreparationStatus,
)
from .protected_controller_prerequisite_component import _validate_prerequisite_binding
from .protected_execution_preparation_journal import (
    _MAX_RECORD_BYTES,
    _TEMPORARY_RE,
    ExecutionPreparationOperationJournal,
    _require_directory,
)
from .protected_execution_prerequisites import ProtectedExecutionPrerequisiteArtifact

_POOLS = ("gb10", "oldlab")
_RECORDS = frozenset({"inputs.intent.json", "manager.intent.json", "manager.terminal.json",
    "drain.intent.json", "drain.terminal.json", "activation.terminal.json",
    "abort.intent.json", "abort.terminal.json",
    "stopped-gb10.terminal.json", "stopped-oldlab.terminal.json"})


class ActiveControllerTransport(Protocol):
    @property
    def authority_sha256(self) -> str: ...
    def observe(self, request: ActiveControllerRequest) -> ActiveControllerEvidence | None: ...
    def converge_files(self, request: ActiveControllerRequest) -> ActiveControllerEvidence: ...
    def refresh_preparation(self, request: ActiveControllerRequest) -> ActiveControllerEvidence: ...
    def enable_timer(self, request: ActiveControllerRequest) -> ActiveControllerEvidence: ...


class ActivationPreparedTransport(PreparedControllerTransport, Protocol):
    @property
    def authority_sha256(self) -> str: ...


class ActivationManagerClient(Protocol):
    def get_status(self) -> dict[str, object]: ...
    def get_execution_preparation_status(self) -> ProtectedExecutionPreparationStatus: ...
    def activate_execution(self, activation: ExecutionActivationV2, idempotency_key: UUID) -> ExecutionAuthorityV2: ...
    def drain_execution(self, drain: ExecutionDrainV2, idempotency_key: UUID) -> ExecutionAuthorityV2: ...
    def abort_execution_preparation(self, abort: ExecutionPreparationAbortV2, idempotency_key: UUID) -> ProtectedExecutionPreparationAbortResult: ...


class ActivationPreparationAbortedError(RuntimeError):
    """The exact prepared epoch was retired; a successor preparation is required."""


def _wire(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n").encode("ascii")


@dataclass(frozen=True, slots=True)
class ExecutionActivationJournal(ExecutionPreparationOperationJournal):
    """Separate namespace using the existing private immutable publication primitive."""

    @property
    def root(self) -> Path:
        return self.state_root / "protected-capacity" / "execution-activation-journals" / self.request_id / str(self.attempt_number)

    def _directories(self) -> tuple[Path, ...]:
        return (self.state_root, self.root.parents[2], self.root.parents[1], self.root.parent, self.root)

    def _ensure_directories(self) -> None:
        for path in self._directories():
            try:
                path.mkdir(mode=0o700)
            except FileExistsError:
                pass
            _require_directory(path, service_uid=self.service_uid)

    def _validate_directories(self) -> None:
        for path in self._directories():
            _require_directory(path, service_uid=self.service_uid)

    def read(self, name: str) -> dict[str, object] | None:
        if name not in _RECORDS:
            raise ValueError("activation record name is invalid")
        try:
            payload = self._read(name)
        except FileNotFoundError:
            return None
        value = json.loads(payload)
        if not isinstance(value, dict) or _wire(value) != payload:
            raise RuntimeError("activation record is not canonical")
        return value

    def retain(self, name: str, value: dict[str, object]) -> None:
        if name not in _RECORDS:
            raise ValueError("activation record name is invalid")
        self._publish(name, _wire(value))
        if self.read(name) != value:
            raise RuntimeError("activation record readback changed")

    def validate_inventory(self) -> None:
        for name in self._entry_names():
            temporary = _TEMPORARY_RE.fullmatch(name)
            if name not in _RECORDS and (temporary is None or temporary.group("final") not in _RECORDS):
                raise RuntimeError("activation journal inventory is invalid")
        for terminal, intent in (("manager.terminal.json", "manager.intent.json"),
                ("drain.terminal.json", "drain.intent.json"),
                ("abort.terminal.json", "abort.intent.json"),
                ("activation.terminal.json", "manager.terminal.json")):
            if self.read(terminal) is not None and self.read(intent) is None:
                raise RuntimeError("activation journal terminal lacks intent")

    @contextmanager
    def exclusive(self) -> Iterator[None]:
        self._ensure_directories()
        # One installed activation at a time, including different attempts.
        descriptor = os.open(self.root.parents[1], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._validate_directories()
            self.validate_inventory()
            yield
        finally:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class ProtectedExecutionActivation:
    """Activate only after both installed controllers and fresh readiness agree.

    Input and manager-transition intent precede effects. A lost activation reply
    reuses the exact payload/key. A failed controller enable durably selects
    drain-only recovery before sending that transition; replay cannot reactivate.
    """

    plan: FinalGatePlan
    artifact: ProtectedExecutionPrerequisiteArtifact
    requests: Mapping[str, ActiveControllerRequest]
    journal: ExecutionActivationJournal
    manager: ActivationManagerClient
    prepared: Mapping[str, ActivationPreparedTransport]
    active: Mapping[str, ActiveControllerTransport]
    dependency_guard: Callable[[], None]
    subject: SubjectConfigurationV1

    def __post_init__(self) -> None:
        _validate_prerequisite_binding(self.plan, self.artifact)
        if (tuple(sorted(self.requests)) != _POOLS or set(self.prepared) != set(_POOLS)
                or set(self.active) != set(_POOLS) or not callable(self.dependency_guard)
                or self.plan.execution_prerequisite_artifact_sha256 != self.artifact.artifact_sha256
                or self.journal.request_id != self.plan.request_id
                or self.journal.attempt_number != self.plan.attempt_number):
            raise ValueError("activation composition binding is invalid")
        subject = SubjectConfigurationV1.model_validate_json(self.subject.model_dump_json())
        acknowledgement = next(item for item in self.artifact.execution_policy.subject_acknowledgements
            if item.subject_id == self.artifact.staging_subject_id)
        if (subject.subject_id != self.artifact.staging_subject_id
                or canonical_digest(subject) != self.artifact.desired_subject_sha256[str(subject.subject_id)]
                or subject.subject_incarnation != acknowledgement.subject_incarnation
                or subject.configuration_generation != acknowledgement.configuration_generation
                or subject.deployment_generation != acknowledgement.deployment_generation
                or subject.demand_reporter_incarnation != acknowledgement.reporter_incarnation):
            raise ValueError("activation staging subject differs from prerequisite authority")
        contexts = []
        admissions = []
        for pool in _POOLS:
            request = ActiveControllerRequest.from_bytes(self.requests[pool].to_bytes())
            if request.pool_id != pool or request.admission is None:
                raise ValueError("activation requires both installed controller admission bundles")
            prerequisite = request.prepared.prerequisite
            entry = request.admission.entry
            if (request.profile != self.artifact.executor_profile_seed.realize(request.prepared.execution)
                    or prerequisite.source_sha != self.artifact.candidate_sha
                    or dict(prerequisite.credential_metadata_sha256) != {
                        key: self.artifact.credential_metadata_sha256[key]
                        for key in (f"pool-executor-{pool}", f"pool-ownership-{pool}")}
                    or entry.subject_id != subject.subject_id
                    or entry.subject_incarnation != subject.subject_incarnation
                    or entry.configuration_generation != subject.configuration_generation
                    or entry.deployment_generation != subject.deployment_generation
                    or entry.candidate_generation != subject.candidate_generation
                    or entry.protected_admission_sha256 != acknowledgement.protected_admission_sha256
                    or request.transport_authority_sha256 != self.prepared[pool].authority_sha256
                    or request.transport_authority_sha256 != self.active[pool].authority_sha256):
                raise ValueError("activation controller differs from prerequisite authority")
            contexts.append(request.document.execution)
            admissions.append((request.admission.issuance_digest, request.admission.database_url,
                request.admission.ca_certificate))
        if (contexts[0] != contexts[1] or contexts[0].execution_state != "active"
                or contexts[0].executable_new_capacity_ceiling != 1 or admissions[0] != admissions[1]):
            raise ValueError("activation requires one shared retained one-slot authority")
        policy = self.artifact.execution_policy
        if (contexts[0].executable_new_capacity_ceiling != policy.executable_new_capacity_ceiling
                or contexts[0].executable_new_capacity_rate_per_minute != policy.executable_new_capacity_rate_per_minute):
            raise ValueError("activation policy capacity differs from prepared limits")
        if len(_wire(self._inputs())) > _MAX_RECORD_BYTES:
            raise ValueError("activation retained inputs exceed journal bound")

    @property
    def expected(self) -> ExecutionContextV2:
        return self.requests["gb10"].document.execution

    def _inputs(self) -> dict[str, object]:
        return {"schema_version": 1, "plan_digest": self.plan.plan_digest,
            "artifact_sha256": self.artifact.artifact_sha256,
            "subject_sha256": canonical_digest(self.subject),
            "controllers": {pool: json.loads(self.requests[pool].to_bytes()) for pool in _POOLS}}

    def _guard(self) -> None:
        self._forward_dependency()
        self._retained_guard()

    def _forward_dependency(self) -> None:
        try:
            self.dependency_guard()
        except Exception:
            if self.journal.read("manager.intent.json") is not None:
                self._retained_guard()
                context = self._status().readiness.execution
                if context is not None and context.execution_state == "active":
                    self._drain()
            raise

    def _retained_guard(self) -> None:
        self.journal.validate_inventory()
        if self.journal.read("inputs.intent.json") != self._inputs():
            raise RuntimeError("activation retained inputs changed")

    def _key(self, purpose: str) -> UUID:
        digest = hashlib.sha256(_wire(self._inputs())).hexdigest()
        return uuid5(NAMESPACE_URL, f"loom:installed-activation:{purpose}:{self.plan.plan_digest}:{digest}")

    def _status(self) -> ProtectedExecutionPreparationStatus:
        status = self.manager.get_execution_preparation_status()
        context = status.readiness.execution
        expected = self.expected
        if context is None or context.model_dump(exclude={"execution_state", "executable_new_capacity_ceiling", "executable_new_capacity_rate_per_minute"}) != expected.model_dump(exclude={"execution_state", "executable_new_capacity_ceiling", "executable_new_capacity_rate_per_minute"}):
            raise RuntimeError("activation manager epoch changed")
        if context.execution_state == "active" and context != expected:
            raise RuntimeError("activation manager capacity changed")
        return status

    def _evidence(self, pool: str, *, state: str) -> ActiveControllerEvidence:
        request = self.requests[pool]
        observed = self.active[pool].observe(request)
        if (observed is None or observed.state != state
                or observed.request_sha256 != request.request_sha256
                or observed.operation_id != request.operation_id
                or observed.pool_id != pool
                or observed.transport_authority_sha256 != request.transport_authority_sha256
                or dict(observed.file_sha256) != {path: hashlib.sha256(payload).hexdigest() for path, payload in request.files.items()}):
            raise RuntimeError("activation controller readback is not exact")
        return observed

    def _abort(self) -> None:
        self._retained_guard()
        expected = self.expected
        request = ExecutionPreparationAbortV2(authority_incarnation=expected.authority_incarnation,
            expected_writer_epoch=expected.writer_epoch, execution_epoch=expected.execution_epoch,
            execution_manifest_sha256=expected.execution_manifest_sha256)
        if self.journal.read("manager.intent.json") is None:
            raise RuntimeError("activation abort lacks retained manager intent")
        self.journal.retain("abort.intent.json", request.model_dump(mode="json"))
        try:
            result = self.manager.abort_execution_preparation(request, self._key("abort"))
        except Exception:
            # An earlier activation request can have won the manager's locked
            # transition. Never infer that an ambiguous HTTP failure aborted it.
            status = self.manager.get_execution_preparation_status()
            if status.readiness.execution == expected:
                self._drain()
            raise
        status = self.manager.get_execution_preparation_status()
        manager = _parse_manager_status(self.manager.get_status(), artifact=self.artifact)
        _require_shadow_readback(manager, status, artifact=self.artifact)
        if (manager.authority_incarnation != expected.authority_incarnation
                or manager.writer_epoch != expected.writer_epoch + 1
                or result.execution_epoch != expected.execution_epoch
                or result.execution_manifest_sha256 != expected.execution_manifest_sha256):
            raise RuntimeError("activation abort readback changed")
        self.journal.retain("abort.terminal.json", {"execution_epoch": result.execution_epoch,
            "execution_manifest_sha256": result.execution_manifest_sha256,
            "writer_epoch": manager.writer_epoch})
        raise ActivationPreparationAbortedError("activation preparation aborted; successor preparation required")

    def _drain(self) -> ExecutionContextV2:
        expected = self.expected
        request = ExecutionDrainV2(authority_incarnation=expected.authority_incarnation,
            expected_writer_epoch=expected.writer_epoch, execution_epoch=expected.execution_epoch,
            execution_manifest_sha256=expected.execution_manifest_sha256,
            expected_executable_new_capacity_ceiling=expected.executable_new_capacity_ceiling,
            expected_executable_new_capacity_rate_per_minute=expected.executable_new_capacity_rate_per_minute)
        self.journal.retain("drain.intent.json", request.model_dump(mode="json"))
        # Forward readiness may have disappeared after an enable. Compensation
        # binds the saved inputs and the exact manager epoch independently.
        self._retained_guard()
        retained = self.journal.read("manager.intent.json")
        if retained is None:
            raise RuntimeError("activation drain lacks retained manager intent")
        activation = ExecutionActivationV2.model_validate_json(_wire(retained))
        if (activation.authority_incarnation != expected.authority_incarnation
                or activation.expected_writer_epoch != expected.writer_epoch
                or activation.execution_epoch != expected.execution_epoch
                or activation.execution_manifest_sha256 != expected.execution_manifest_sha256
                or activation.executable_new_capacity_ceiling != expected.executable_new_capacity_ceiling
                or activation.executable_new_capacity_rate_per_minute != expected.executable_new_capacity_rate_per_minute):
            raise RuntimeError("activation drain retained authority changed")
        context = self._status().readiness.execution
        if context is None or context.execution_state not in {"active", "drain-only"}:
            raise RuntimeError("activation drain requires the retained active epoch")
        result = self.manager.drain_execution(request, self._key("drain"))
        observed = self._status().readiness.execution
        assert observed is not None
        if observed.execution_state != "drain-only" or result.model_dump(exclude={"executable"}) != observed.model_dump():
            raise RuntimeError("activation drain readback is not exact")
        self.journal.retain("drain.terminal.json", result.model_dump(mode="json"))
        return observed

    def execute(self) -> ExecutionContextV2:
        with self.journal.exclusive():
            return self._execute()

    def _execute(self) -> ExecutionContextV2:
        if self.journal.read("drain.intent.json") is not None:
            return self._drain()
        if self.journal.read("abort.intent.json") is not None:
            self._abort()
        self._forward_dependency()
        self.journal.retain("inputs.intent.json", self._inputs())
        self._guard()
        if self.journal.read("drain.intent.json") is not None:
            return self._drain()
        status = self._status()
        context = status.readiness.execution
        assert context is not None
        retained = self.journal.read("manager.intent.json")
        if context.execution_state == "drain-only":
            raise RuntimeError("activation epoch was externally drained")
        if context.execution_state == "active" and retained is None:
            raise RuntimeError("active manager lacks retained activation intent")
        if retained is None:
            for pool in _POOLS:
                self._guard()
                request = self.requests[pool]
                stop_record: dict[str, object] = {"prepared_request_sha256": request.prepared.request_sha256,
                    "active_request_sha256": request.request_sha256}
                retained_stop = self.journal.read(f"stopped-{pool}.terminal.json")
                if retained_stop is None:
                    stopped = self.prepared[pool].disable_timer(request.prepared)
                    if not _controller_evidence_matches(stopped, request.prepared, require_timer=False, require_tick=False):
                        raise RuntimeError("activation requires stopped exact prepared controllers")
                    self.journal.retain(f"stopped-{pool}.terminal.json", stop_record)
                elif retained_stop != stop_record:
                    raise RuntimeError("activation stopped controller binding changed")
                # The active installer independently checks all units stopped.
                # Once its intent exists, prepared operations deliberately refuse.
                self.active[pool].converge_files(request)
                self._evidence(pool, state="staged")
            for pool in _POOLS:
                self._guard()
                self.active[pool].refresh_preparation(self.requests[pool])
                self._evidence(pool, state="staged")
            status = self._status()
            if not _readiness_is_exact(status, execution=self.requests["gb10"].prepared.execution, artifact=self.artifact):
                raise RuntimeError("activation requires exact fresh prepared readiness")
            expected = self.expected
            activation = ExecutionActivationV2(authority_incarnation=expected.authority_incarnation,
                expected_writer_epoch=expected.writer_epoch, execution_epoch=expected.execution_epoch,
                execution_manifest_sha256=expected.execution_manifest_sha256,
                prepared_readiness_sha256=status.readiness_sha256,
                executable_new_capacity_ceiling=1,
                executable_new_capacity_rate_per_minute=expected.executable_new_capacity_rate_per_minute)
            self.journal.retain("manager.intent.json", activation.model_dump(mode="json"))
        else:
            activation = ExecutionActivationV2.model_validate_json(_wire(retained))
            if context.execution_state == "prepared" and (
                    not _readiness_is_exact(status, execution=self.requests["gb10"].prepared.execution, artifact=self.artifact)
                    or status.readiness_sha256 != activation.prepared_readiness_sha256):
                self._abort()
        self._guard()
        # Replay also resolves a commit whose HTTP reply was lost. Never mint a
        # new idempotency key or substitute a later readiness digest here.
        result = self.manager.activate_execution(activation, self._key("activate"))
        try:
            if result.model_dump(exclude={"executable"}) != self.expected.model_dump():
                raise RuntimeError("activation response authority changed")
            self.journal.retain("manager.terminal.json", result.model_dump(mode="json"))
            for pool in _POOLS:
                self._guard()
                if self._status().readiness.execution != self.expected:
                    raise RuntimeError("activation manager changed before timer enable")
                self.active[pool].enable_timer(self.requests[pool])
                self._evidence(pool, state="active")
            self._guard()
            if self._status().readiness.execution != self.expected:
                raise RuntimeError("activation manager changed after timer enable")
            self.journal.retain("activation.terminal.json", {"execution_sha256": canonical_executable_digest(self.expected),
                "controllers": {pool: self.requests[pool].request_sha256 for pool in _POOLS}})
        except Exception:
            self._drain()
            raise
        return self.expected
