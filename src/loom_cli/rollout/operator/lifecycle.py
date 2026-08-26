"""Full-lifecycle serialization and reconciliation for staging attempts."""

from __future__ import annotations

import contextlib
import fcntl
import os
import re
import stat
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Protocol

from ..state import RolloutState
from .config import OperatorConfig
from .model import (
    ActivePointer,
    DriverEnvelope,
    EventStatus,
    RequestEvent,
    RequestEventType,
    RolloutRequest,
)
from .store import RequestStore, RequestStoreError
from .systemd import SystemdQueryError, SystemdUnitStatus, UnitLaunchError

Clock = Callable[[], datetime]
BootIdReader = Callable[[], str]
ReconciliationOutcome = Literal["idle", "busy", "done", "failed", "stale"]
_SAFE_STEP_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_UTC_TIMESTAMP_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?Z$"
)


class SystemdManager(Protocol):
    """Narrow lifecycle dependency implemented by ``SystemdUserManager``."""

    def start_attempt(self, envelope_path: Path, unit_name: str) -> None: ...

    def show(self, unit_name: str) -> SystemdUnitStatus | None: ...


class LifecycleError(RuntimeError):
    """Raised when lifecycle state cannot be changed safely."""


class LifecycleBusyError(LifecycleError):
    """Raised when an existing full-lifecycle owner must not be displaced."""

    def __init__(self, message: str, safe_status: dict[str, object]) -> None:
        super().__init__(message)
        self.safe_status = safe_status


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """Safe reconciliation decision for the currently persisted pointer."""

    outcome: ReconciliationOutcome
    pointer: ActivePointer | None
    cleared: bool
    safe_status: dict[str, object]


class LifecycleCoordinator:
    """Coordinate the singleton pointer, lifecycle locks, and unit state."""

    def __init__(
        self,
        config: OperatorConfig,
        *,
        store: RequestStore,
        systemd: SystemdManager,
        now: Clock | None = None,
        boot_id: BootIdReader | None = None,
        maintenance_owner_uid: int = 0,
        maintenance_owner_gid: int = 0,
    ) -> None:
        if store.root != config.state_root:
            raise LifecycleError("request store root does not match operator configuration")
        self.config = config
        self.store = store
        self.systemd = systemd
        self._now = now or (lambda: datetime.now(UTC))
        self._boot_id = boot_id or _read_boot_id
        self._maintenance_owner_uid = maintenance_owner_uid
        self._maintenance_owner_gid = maintenance_owner_gid
        self._held_guards: ContextVar[frozenset[str]] = ContextVar(
            f"lifecycle_guards_{id(self)}",
            default=frozenset(),
        )

    def _safe_status(self, pointer: ActivePointer) -> dict[str, object]:
        status: dict[str, object] = {
            "request_id": pointer.request_id,
            "attempt_number": pointer.attempt_number,
            "unit_name": pointer.unit_name,
            "status": pointer.status,
        }
        try:
            request = self.store.read_request(pointer.request_id)
        except RequestStoreError:
            request = None
        if request is not None:
            status.update(
                {
                    "initiator": request.caller.username,
                    "resolved_sha": request.candidate.resolved_sha,
                    "image_tag": request.candidate.image_tag,
                }
            )
        try:
            events = self.store.read_events(pointer.request_id)
        except RequestStoreError:
            events = []
        matching_events = [
            event for event in events if event.attempt_number == pointer.attempt_number
        ]
        for event in reversed(matching_events):
            if (
                event.current_step is not None
                and _SAFE_STEP_RE.fullmatch(event.current_step) is not None
            ):
                status["current_step"] = event.current_step
                break
        for event in reversed(matching_events):
            if event.event in {
                "launch_pending",
                "attempt_pending",
                "attempt_running",
            } and _safe_utc_timestamp(event.occurred_at):
                status["started_at"] = event.occurred_at
                break
        return status

    def reserve_active(self, pointer: ActivePointer) -> None:
        current = self.store.read_active()
        if current is not None:
            raise LifecycleBusyError(
                "a staging rollout attempt is already pending or running",
                self._safe_status(current),
            )
        try:
            self.store.set_active(pointer)
        except RequestStoreError as exc:
            current = self.store.read_active()
            if current is not None:
                raise LifecycleBusyError(
                    "a staging rollout attempt is already pending or running",
                    self._safe_status(current),
                ) from None
            raise LifecycleError("active rollout pointer could not be reserved safely") from exc

    @contextmanager
    def _guard(self, filename: str) -> Iterator[None]:
        held_guards = self._held_guards.get()
        if filename in held_guards:
            yield
            return
        try:
            runtime_metadata = os.lstat(self.config.runtime_root)
        except OSError as exc:
            raise LifecycleError("lifecycle runtime directory is unavailable") from exc
        if (
            not stat.S_ISDIR(runtime_metadata.st_mode)
            or runtime_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(runtime_metadata.st_mode) & 0o022
        ):
            raise LifecycleError("lifecycle runtime directory is not service-owned")
        path = self.config.runtime_root / filename
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags, 0o600)
        except OSError as exc:
            raise LifecycleError(f"could not open protected {filename}") from exc
        locked = False
        guard_token = None
        try:
            try:
                os.fchmod(fd, 0o600)
                metadata = os.fstat(fd)
                if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
                    raise LifecycleError(f"{filename} is not a service-owned regular file")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    locked = True
                except BlockingIOError:
                    raise LifecycleBusyError(
                        "the protected lifecycle lock is already held",
                        {"status": "busy", "lock": filename},
                    ) from None
            except LifecycleError:
                raise
            except OSError as exc:
                raise LifecycleError(f"protected {filename} operation failed") from exc
            guard_token = self._held_guards.set(held_guards | {filename})
            yield
        finally:
            if guard_token is not None:
                self._held_guards.reset(guard_token)
            if locked:
                with contextlib.suppress(OSError):
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def launch_guard(self) -> AbstractContextManager[None]:
        return self._guard("launch.lock")

    def assert_admission_open(self) -> None:
        if "launch.lock" not in self._held_guards.get():
            raise LifecycleError("admission state must be checked under the launch lock")
        marker = self.config.runtime_root / "maintenance"
        try:
            metadata = os.lstat(marker)
        except FileNotFoundError:
            metadata = None
        except OSError as exc:
            raise LifecycleError("maintenance admission marker is unavailable") from exc
        if metadata is not None:
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self._maintenance_owner_uid
                or metadata.st_gid != self._maintenance_owner_gid
                or stat.S_IMODE(metadata.st_mode) != 0o600
            ):
                raise LifecycleError("maintenance admission marker is unsafe")
            raise LifecycleBusyError(
                "staging rollout admission is disabled for maintenance",
                {"status": "busy", "reason": "maintenance"},
            )
        if self.store.read_backup_retention_claim() is not None:
            raise LifecycleBusyError(
                "backup retention maintenance is still in progress",
                {"status": "busy", "reason": "backup_retention_busy"},
            )
        if self.store.read_preflight_artifact_retention_claim() is not None:
            raise LifecycleBusyError(
                "preflight artifact retention maintenance is still in progress",
                {
                    "status": "busy",
                    "reason": "preflight_artifact_retention_busy",
                },
            )

    def assert_maintenance_active(self) -> None:
        """Require the root-owned admission freeze for protected maintenance."""

        if "launch.lock" not in self._held_guards.get():
            raise LifecycleError("maintenance state must be checked under the launch lock")
        marker = self.config.runtime_root / "maintenance"
        try:
            metadata = os.lstat(marker)
        except OSError as exc:
            raise LifecycleError("maintenance admission marker is unavailable") from exc
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != self._maintenance_owner_uid
            or metadata.st_gid != self._maintenance_owner_gid
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise LifecycleError("maintenance admission marker is unsafe")

    def assert_maintenance_idle(self) -> None:
        """Prove maintenance has no active pointer without reconciling state."""

        self.assert_maintenance_active()
        pointer = self.store.read_active()
        if pointer is not None:
            raise LifecycleBusyError(
                "a staging rollout attempt is already pending or running",
                self._safe_status(pointer),
            )

    def driver_guard(self) -> AbstractContextManager[None]:
        return self._guard("staging.driver.lock")

    def release_active(self, pointer: ActivePointer) -> bool:
        return self.store.clear_active_if_matches(pointer)

    def _attempt_event(
        self,
        envelope: DriverEnvelope,
        pointer: ActivePointer,
        *,
        event: RequestEventType,
        status: EventStatus,
        reason: str | None = None,
        current_step: str | None = None,
    ) -> RequestEvent:
        return RequestEvent(
            request_id=envelope.request_id,
            event=event,
            occurred_at=_utc_timestamp(self._now()),
            operator=envelope.attempt_operator,
            operator_uid=envelope.attempt_uid,
            attempt_number=envelope.attempt_number,
            unit_name=pointer.unit_name,
            status=status,
            reason=reason,
            current_step=current_step,
        )

    def launch(self, envelope: DriverEnvelope) -> ActivePointer:
        """Reserve a finalized attempt immediately before detached unit start."""
        with self.launch_guard():
            return self._launch_under_guard(envelope)

    def _launch_under_guard(self, envelope: DriverEnvelope) -> ActivePointer:
        try:
            persisted = self.store.read_attempt_envelope(
                envelope.request_id,
                envelope.attempt_number,
            )
        except RequestStoreError as exc:
            raise LifecycleError("driver envelope is not immutably published") from exc
        if persisted != envelope:
            raise LifecycleError("driver envelope does not match immutable persisted attempt")

        pointer = ActivePointer(
            request_id=envelope.request_id,
            attempt_number=envelope.attempt_number,
            unit_name=(
                f"loom-staging-rollout-{envelope.request_id}-{envelope.attempt_number}.service"
            ),
            status="pending",
        )
        self.reserve_active(pointer)
        try:
            self.store.append_event(
                self._attempt_event(
                    envelope,
                    pointer,
                    event="launch_pending",
                    status="pending",
                )
            )
        except RequestStoreError as exc:
            self.release_active(pointer)
            raise LifecycleError("launch-pending event could not be persisted") from exc

        envelope_path = (
            self.config.state_root
            / "requests"
            / envelope.request_id
            / "attempts"
            / str(envelope.attempt_number)
            / "envelope.json"
        )
        try:
            self.systemd.start_attempt(envelope_path, pointer.unit_name)
        except UnitLaunchError:
            try:
                self.store.append_event(
                    self._attempt_event(
                        envelope,
                        pointer,
                        event="launch_failed",
                        status="failed",
                        reason="unit_launch_failed",
                    )
                )
            except RequestStoreError as exc:
                raise LifecycleError("unit launch failure could not be persisted") from exc
            self.release_active(pointer)
            raise
        return pointer

    def reconcile_active(self) -> ReconciliationResult:
        """Cross-check one active attempt without trusting systemd alone."""
        try:
            with self.launch_guard():
                return self._reconcile_active_under_guard()
        except LifecycleBusyError:
            try:
                pointer = self.store.read_active()
            except RequestStoreError:
                pointer = None
            if pointer is None:
                return ReconciliationResult(
                    outcome="busy",
                    pointer=None,
                    cleared=False,
                    safe_status={"status": "busy", "reason": "launch_in_progress"},
                )
            return self._busy_result(pointer, reason="launch_in_progress")

    def _reconcile_active_under_guard(self) -> ReconciliationResult:
        pointer = self.store.read_active()
        if pointer is None:
            return ReconciliationResult(
                outcome="idle",
                pointer=None,
                cleared=False,
                safe_status={},
            )

        try:
            request = self.store.read_request(pointer.request_id)
            envelope = self.store.read_attempt_envelope(
                pointer.request_id,
                pointer.attempt_number,
            )
        except RequestStoreError:
            return self._busy_result(
                pointer,
                reason="immutable_attempt_unavailable",
            )

        state = self._load_rollout_state(envelope)
        current_step = _current_step(state)
        if not _binding_matches(request, envelope):
            return self._busy_result(
                pointer,
                reason="immutable_attempt_binding_mismatch",
                current_step=current_step,
            )
        expected_unit = (
            f"loom-staging-rollout-{envelope.request_id}-{envelope.attempt_number}.service"
        )
        if pointer.unit_name != expected_unit:
            return self._busy_result(
                pointer,
                reason="active_pointer_identity_mismatch",
                current_step=current_step,
            )

        try:
            unit = self.systemd.show(pointer.unit_name)
        except SystemdQueryError:
            return self._busy_result(
                pointer,
                reason="unit_status_unavailable",
                current_step=current_step,
            )
        if unit is not None and unit.unit_name != pointer.unit_name:
            return self._busy_result(
                pointer,
                reason="unit_status_identity_mismatch",
                current_step=current_step,
            )

        if unit is not None and unit.is_running:
            return self._reconcile_running(pointer, unit, state, current_step=current_step)

        if state is not None and state.status == "done":
            if unit is None:
                if not self._has_attempt_done_event(pointer):
                    return self._record_stale(
                        pointer,
                        envelope,
                        reason="unit_missing_without_corroborated_success",
                        current_step=current_step,
                    )
            elif not (
                unit.active_state == "inactive"
                and unit.result == "success"
                and unit.exec_main_status == 0
            ):
                return self._record_stale(
                    pointer,
                    envelope,
                    reason="terminal_state_conflicts_with_unit_failure",
                    current_step=current_step,
                )
            cleared = self.release_active(pointer)
            safe_status = self._safe_status(pointer)
            safe_status["reason"] = "terminal_rollout_state"
            safe_status["rollout_status"] = state.status
            if current_step is not None:
                safe_status["current_step"] = current_step
            return ReconciliationResult(
                outcome="done",
                pointer=pointer,
                cleared=cleared,
                safe_status=safe_status,
            )
        if state is not None and state.status == "failed":
            cleared = self.release_active(pointer)
            safe_status = self._safe_status(pointer)
            safe_status["reason"] = "terminal_rollout_state"
            safe_status["rollout_status"] = state.status
            if current_step is not None:
                safe_status["current_step"] = current_step
            return ReconciliationResult(
                outcome="failed",
                pointer=pointer,
                cleared=cleared,
                safe_status=safe_status,
            )

        reason = (
            "unit_missing_without_terminal_rollout_state"
            if unit is None
            else "unit_inactive_without_terminal_rollout_state"
        )
        return self._record_stale(
            pointer,
            envelope,
            reason=reason,
            current_step=current_step,
        )

    def _load_rollout_state(self, envelope: DriverEnvelope) -> RolloutState | None:
        state_path = self.config.rollout_root / "rollouts" / envelope.rollout_id / "state.json"
        try:
            state = RolloutState.load(state_path)
        except (AttributeError, OSError, KeyError, TypeError, ValueError):
            return None
        if state.rollout_id != envelope.rollout_id or state.status not in {
            "running",
            "done",
            "failed",
        }:
            return None
        return state

    def _has_attempt_done_event(self, pointer: ActivePointer) -> bool:
        try:
            events = self.store.read_events(pointer.request_id)
        except RequestStoreError:
            return False
        matching_events = [
            event
            for event in events
            if event.attempt_number == pointer.attempt_number
            and event.unit_name == pointer.unit_name
        ]
        if any(
            event.event in {"attempt_failed", "launch_failed", "cancelled"}
            for event in matching_events
        ):
            return False
        return any(
            event.event == "attempt_done"
            and event.status == "done"
            and _safe_utc_timestamp(event.occurred_at)
            for event in matching_events
        )

    def _reconcile_running(
        self,
        pointer: ActivePointer,
        unit: SystemdUnitStatus,
        state: RolloutState | None,
        *,
        current_step: str | None,
    ) -> ReconciliationResult:
        if state is None:
            reason = "rollout_state_unavailable"
        elif state.status != "running":
            reason = "unit_running_with_terminal_state"
        elif state.driver is None:
            reason = "driver_identity_unavailable"
        elif unit.main_pid < 1:
            reason = "driver_pid_unavailable"
        elif state.driver.pid != unit.main_pid:
            reason = "driver_pid_mismatch"
        else:
            try:
                current_boot_id = self._boot_id()
            except (LifecycleError, OSError, ValueError):
                reason = "boot_id_unavailable"
            else:
                if not state.driver.boot_id:
                    reason = "driver_boot_id_unavailable"
                elif state.driver.boot_id != current_boot_id:
                    reason = "driver_boot_id_mismatch"
                else:
                    reason = "unit_running"
        return self._busy_result(
            pointer,
            reason=reason,
            current_step=current_step,
        )

    def _busy_result(
        self,
        pointer: ActivePointer,
        *,
        reason: str,
        current_step: str | None = None,
    ) -> ReconciliationResult:
        safe_status = self._safe_status(pointer)
        safe_status["reason"] = reason
        if current_step is not None:
            safe_status["current_step"] = current_step
        return ReconciliationResult(
            outcome="busy",
            pointer=pointer,
            cleared=False,
            safe_status=safe_status,
        )

    def _record_stale(
        self,
        pointer: ActivePointer,
        envelope: DriverEnvelope,
        *,
        reason: str,
        current_step: str | None,
    ) -> ReconciliationResult:
        try:
            self.store.append_event(
                self._attempt_event(
                    envelope,
                    pointer,
                    event="attempt_failed",
                    status="failed",
                    reason=reason,
                    current_step=current_step,
                )
            )
        except RequestStoreError:
            return self._busy_result(
                pointer,
                reason="reconciliation_event_persistence_failed",
                current_step=current_step,
            )
        cleared = self.release_active(pointer)
        safe_status = self._safe_status(pointer)
        safe_status["reason"] = reason
        if current_step is not None:
            safe_status["current_step"] = current_step
        return ReconciliationResult(
            outcome="stale",
            pointer=pointer,
            cleared=cleared,
            safe_status=safe_status,
        )


def _read_boot_id() -> str:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    except OSError as exc:
        raise LifecycleError("current boot identity is unavailable") from exc


def _utc_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _binding_matches(request: RolloutRequest, envelope: DriverEnvelope) -> bool:
    return (
        request.request_id == envelope.request_id
        and request.rollout_id == envelope.rollout_id
        and request.caller.username == envelope.initiating_operator
        and request.caller.uid == envelope.initiating_uid
        and request.candidate.remote_url == envelope.remote_url
        and request.candidate.target_ref == envelope.target_ref
        and request.candidate.resolved_sha == envelope.resolved_sha
        and request.candidate.image_tag == envelope.image_tag
        and request.candidate.fetched_at == envelope.fetched_at
        and request.candidate.source_mode == envelope.source_mode
        and request.candidate.resolved_tree == envelope.resolved_tree
        and request.candidate.approved_base_sha == envelope.approved_base_sha
        and request.runner_config_sha256 == envelope.runner_config_sha256
        and request.preflight_attestation_sha256 == envelope.preflight_attestation_sha256
        and request.preflight_registry_sha256 == envelope.preflight_registry_sha256
        and request.preflight_coverage_sha256 == envelope.preflight_coverage_sha256
    )


def _current_step(state: RolloutState | None) -> str | None:
    if state is None or state.current_step is None:
        return None
    if type(state.current_step) is not int or not 1 <= state.current_step <= 9999:
        return None
    for record in state.steps:
        if record.number != state.current_step:
            continue
        if _SAFE_STEP_RE.fullmatch(record.name) is not None:
            return f"{record.number:02d}-{record.name}"
        return str(record.number)
    return str(state.current_step)


def _safe_utc_timestamp(value: str) -> bool:
    if _UTC_TIMESTAMP_RE.fullmatch(value) is None:
        return False
    try:
        datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        return False
    return True


__all__ = [
    "BootIdReader",
    "Clock",
    "LifecycleBusyError",
    "LifecycleCoordinator",
    "LifecycleError",
    "ReconciliationOutcome",
    "ReconciliationResult",
    "SystemdManager",
]
