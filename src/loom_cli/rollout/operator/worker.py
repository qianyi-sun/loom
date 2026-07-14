"""Service-only detached worker for one finalized staging attempt."""

from __future__ import annotations

import argparse
import os
import pwd
import signal
import subprocess
import sys
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Never, TextIO

from .config import OperatorConfig
from .envelope import fixed_operator_config_path, load_validated_envelope
from .lifecycle import LifecycleCoordinator
from .model import ActivePointer, DriverEnvelope, RequestEvent
from .policy import sanitized_child_environment
from .redaction import redact_rollout_text
from .store import RequestStore
from .systemd import SystemdUserManager


class _ArgumentError(ValueError):
    pass


class _CancellationSignal(BaseException):
    pass


class _Parser(argparse.ArgumentParser):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("allow_abbrev", False)
        super().__init__(*args, **kwargs)

    def error(self, message: str) -> Never:
        raise _ArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="loom-staging-rollout-worker")
    commands = parser.add_subparsers(dest="command", required=True)
    attempt = commands.add_parser("run-attempt")
    attempt.add_argument("--envelope", type=Path, required=True)
    return parser


@dataclass(slots=True)
class WorkerDependencies:
    store: Any
    lifecycle: Any
    run_driver: Callable[[Path, bool], int]
    now: Callable[[], str]
    stderr: TextIO
    envelope_path: Callable[[DriverEnvelope], Path] | None = None
    load_envelope: Callable[[Path], DriverEnvelope] | None = None


@dataclass(slots=True)
class _SignalController:
    requested: bool = False
    driver_interruptible: bool = False
    sealed: bool = False

    def handle(self, signum: int, frame: object) -> None:
        del signum, frame
        if self.sealed:
            return
        self.requested = True
        if self.driver_interruptible:
            raise _CancellationSignal

    @contextmanager
    def driver_window(self) -> Iterator[None]:
        self.driver_interruptible = True
        try:
            if self.requested:
                raise _CancellationSignal
            yield
        finally:
            self.driver_interruptible = False

    def seal_terminal(self, *, event_cancelled: bool) -> bool:
        cancelled = self.requested or event_cancelled
        self.sealed = True
        return cancelled


def _unit_name(envelope: DriverEnvelope) -> str:
    return f"loom-staging-rollout-{envelope.request_id}-{envelope.attempt_number}.service"


def _path(dependencies: WorkerDependencies, envelope: DriverEnvelope) -> Path:
    if dependencies.envelope_path is not None:
        return dependencies.envelope_path(envelope)
    root = getattr(dependencies.store, "root", None)
    if isinstance(root, Path):
        return (
            root
            / "requests"
            / envelope.request_id
            / "attempts"
            / str(envelope.attempt_number)
            / "envelope.json"
        )
    return Path(
        f"/var/lib/loom-staging-rollout/requests/{envelope.request_id}/"
        f"attempts/{envelope.attempt_number}/envelope.json"
    )


def _event(
    envelope: DriverEnvelope,
    *,
    dependencies: WorkerDependencies,
    event: str,
    status: str,
    reason: str | None = None,
) -> RequestEvent:
    return RequestEvent(
        request_id=envelope.request_id,
        event=event,  # type: ignore[arg-type]
        occurred_at=dependencies.now(),
        operator=envelope.attempt_operator,
        operator_uid=envelope.attempt_uid,
        attempt_number=envelope.attempt_number,
        unit_name=_unit_name(envelope),
        status=status,  # type: ignore[arg-type]
        reason=reason,
    )


def _cancel_requested(dependencies: WorkerDependencies, envelope: DriverEnvelope) -> bool:
    events = dependencies.store.read_events(envelope.request_id)
    directives = [
        event
        for event in events
        if event.attempt_number == envelope.attempt_number
        and event.event in {"cancel_requested", "cancel_failed"}
    ]
    return bool(directives and directives[-1].event == "cancel_requested")


def run_attempt(
    envelope: DriverEnvelope,
    dependencies: WorkerDependencies,
    *,
    signals: _SignalController | None = None,
) -> int:
    """Run one immutable attempt while holding the full-driver lifecycle lock."""
    pointer = ActivePointer(
        request_id=envelope.request_id,
        attempt_number=envelope.attempt_number,
        unit_name=_unit_name(envelope),
        status="pending",
    )
    running_pointer = replace(pointer, status="running")
    signal_controller = signals or _SignalController()
    envelope_path = _path(dependencies, envelope)
    with dependencies.lifecycle.driver_guard():
        persisted = dependencies.store.read_attempt_envelope(
            envelope.request_id,
            envelope.attempt_number,
        )
        if persisted != envelope:
            raise ValueError("worker envelope does not match immutable attempt")
        active = dependencies.store.read_active()
        if active is None or (
            active.request_id,
            active.attempt_number,
            active.unit_name,
        ) != (
            pointer.request_id,
            pointer.attempt_number,
            pointer.unit_name,
        ):
            raise ValueError("worker attempt does not own the active staging pointer")
        dependencies.store.set_active(running_pointer)
        dependencies.store.append_event(
            _event(
                envelope,
                dependencies=dependencies,
                event="attempt_running",
                status="running",
            )
        )
        driver_rc = 1
        cancelled_by_signal = False
        if signal_controller.requested:
            cancelled_by_signal = True
        else:
            try:
                with signal_controller.driver_window():
                    driver_rc = dependencies.run_driver(envelope_path, envelope.resume)
            except _CancellationSignal:
                cancelled_by_signal = True
            except BaseException:
                driver_rc = 1

        cancelled = signal_controller.seal_terminal(
            event_cancelled=(cancelled_by_signal or _cancel_requested(dependencies, envelope))
        )
        if cancelled:
            terminal_event = _event(
                envelope,
                dependencies=dependencies,
                event="cancelled",
                status="cancelled",
                reason="cancel_requested",
            )
            return_code = 130
        elif driver_rc == 0:
            terminal_event = _event(
                envelope,
                dependencies=dependencies,
                event="attempt_done",
                status="done",
            )
            return_code = 0
        else:
            terminal_event = _event(
                envelope,
                dependencies=dependencies,
                event="attempt_failed",
                status="failed",
                reason="driver_failed",
            )
            return_code = 1
        dependencies.store.append_event(terminal_event)
        dependencies.lifecycle.release_active(running_pointer)
        return return_code


def _run(
    argv: list[str],
    *,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        env=environment,
    )


def _default_dependencies(config: OperatorConfig, *, service_uid: int) -> WorkerDependencies:
    store = RequestStore(config.state_root)
    child_environment = sanitized_child_environment(config, service_uid=service_uid)
    systemd = SystemdUserManager(
        config,
        service_uid=service_uid,
        run=lambda argv: _run(argv, environment=child_environment),
    )
    lifecycle = LifecycleCoordinator(config, store=store, systemd=systemd)

    def run_driver(envelope_path: Path, resume: bool) -> int:
        from loom_cli.cluster_cmd import dispatch

        argv = [
            "rollout",
            "staging",
            "--request-envelope",
            str(envelope_path),
        ]
        if resume:
            argv.append("--resume")
        return dispatch(argv)

    return WorkerDependencies(
        store=store,
        lifecycle=lifecycle,
        run_driver=run_driver,
        now=lambda: datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        stderr=sys.stderr,
        envelope_path=lambda envelope: (
            config.state_root
            / "requests"
            / envelope.request_id
            / "attempts"
            / str(envelope.attempt_number)
            / "envelope.json"
        ),
        load_envelope=lambda path: load_validated_envelope(
            path,
            config,
            effective_uid=service_uid,
        ),
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    dependencies: WorkerDependencies | None = None,
) -> int:
    """Expose only the service-owned ``run-attempt --envelope`` surface."""
    try:
        args = _parser().parse_args(list(argv) if argv is not None else None)
    except (_ArgumentError, SystemExit):
        return 2
    if args.command != "run-attempt":
        return 2
    signal_controller = _SignalController()
    previous_term = signal.signal(signal.SIGTERM, signal_controller.handle)
    previous_int = signal.signal(signal.SIGINT, signal_controller.handle)
    try:
        if dependencies is None:
            config = OperatorConfig.load(fixed_operator_config_path())
            service_uid = pwd.getpwnam(config.service_user).pw_uid
            if os.geteuid() != service_uid:
                raise ValueError("worker effective UID does not match service account")
            dependencies = _default_dependencies(config, service_uid=service_uid)
        if dependencies.load_envelope is None:
            raise ValueError("worker envelope loader is unavailable")
        envelope = dependencies.load_envelope(args.envelope)
        return run_attempt(envelope, dependencies, signals=signal_controller)
    except BaseException as exc:
        if isinstance(exc, KeyboardInterrupt):
            raise
        if dependencies is not None:
            dependencies.stderr.write(
                f"error: {redact_rollout_text('worker attempt failed safely')}\n"
            )
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)


if __name__ == "__main__":  # pragma: no cover - service entrypoint
    raise SystemExit(main())


__all__ = ["WorkerDependencies", "main", "run_attempt"]
