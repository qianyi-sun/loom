from __future__ import annotations

import io
import signal
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest

from loom_cli.rollout.operator import worker as worker_module
from loom_cli.rollout.operator.model import ActivePointer, DriverEnvelope, RequestEvent
from loom_cli.rollout.operator.policy import sanitized_child_environment
from loom_cli.rollout.operator.worker import WorkerDependencies, run_attempt
from loom_cli.rollout.operator.worker import main as worker_main

REQUEST_ID = "req-alpha"


def valid_envelope() -> DriverEnvelope:
    return DriverEnvelope(
        schema_version=1,
        request_id=REQUEST_ID,
        rollout_id="rollout-alpha",
        initiating_operator="hongjian",
        initiating_uid=2002,
        attempt_number=1,
        attempt_operator="hongjian",
        attempt_uid=2002,
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-14T12:00:00Z",
        backup_manifest_path="/data/loom-staging/backups/fixed/backup-manifest.json",
        backup_manifest_sha256="2" * 64,
        runner_config_sha256="1" * 64,
        preflight_attestation_sha256="3" * 64,
        preflight_registry_sha256="4" * 64,
        preflight_coverage_sha256="5" * 64,
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        cluster_config_path="/opt/loom-staging-runner/repo/deploy/environments/staging.cluster.toml",
        rollout_root="/data/loom-staging",
        admin_token_source="file:/var/lib/loom-staging-rollout/credentials/admin-token",
        worker_token_source="file:/var/lib/loom-staging-rollout/credentials/worker-token",
        service_token_source="file:/var/lib/loom-staging-rollout/credentials/service-token",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        resume=False,
    )


class FakeStore:
    def __init__(self, envelope: DriverEnvelope, order: list[str]) -> None:
        self.envelope = envelope
        self.order = order
        self.events: list[RequestEvent] = []
        self.active = ActivePointer(
            request_id=envelope.request_id,
            attempt_number=envelope.attempt_number,
            unit_name=f"loom-staging-rollout-{envelope.request_id}-{envelope.attempt_number}.service",
            status="pending",
        )
        self.active_history: list[ActivePointer | None] = [self.active]

    def read_attempt_envelope(self, request_id: str, attempt_number: int) -> DriverEnvelope:
        return self.envelope

    def append_event(self, event: RequestEvent) -> Path:
        self.events.append(event)
        return Path("/events")

    def read_events(self, request_id: str) -> list[RequestEvent]:
        return list(self.events)

    def read_active(self) -> ActivePointer | None:
        return self.active

    def set_active(self, pointer: ActivePointer) -> Path:
        current = self.active
        if current is None or (
            current.request_id,
            current.attempt_number,
            current.unit_name,
        ) != (
            pointer.request_id,
            pointer.attempt_number,
            pointer.unit_name,
        ):
            raise RuntimeError("active pointer identity mismatch")
        self.active = pointer
        self.active_history.append(pointer)
        self.order.append(f"active-{pointer.status}")
        return Path("/active")

    def clear_active_if_matches(self, pointer: ActivePointer) -> bool:
        if self.active != pointer:
            return False
        self.active = None
        self.active_history.append(None)
        return True


class FakeLifecycle:
    def __init__(self, store: FakeStore, order: list[str]) -> None:
        self.store = store
        self.order = order

    @contextmanager
    def driver_guard(self):  # type: ignore[no-untyped-def]
        self.order.append("driver-lock-acquire")
        try:
            yield
        finally:
            self.order.append("driver-lock-release")

    def release_active(self, pointer: ActivePointer) -> bool:
        self.order.append(f"active-clear-{pointer.status}")
        return self.store.clear_active_if_matches(pointer)


@dataclass
class Bundle:
    deps: WorkerDependencies
    store: FakeStore
    order: list[str]


def worker_fakes(*, driver_rc: int = 0) -> Bundle:
    envelope = valid_envelope()
    order: list[str] = []
    store = FakeStore(envelope, order)
    lifecycle = FakeLifecycle(store, order)
    persist_event = store.append_event

    def append_event(event: RequestEvent) -> Path:
        order.append(event.event)
        return persist_event(event)

    store.append_event = append_event  # type: ignore[method-assign]

    def run_driver(envelope_path: Path, resume: bool) -> int:
        order.append("driver-run")
        return driver_rc

    deps = WorkerDependencies(
        store=store,  # type: ignore[arg-type]
        lifecycle=lifecycle,  # type: ignore[arg-type]
        run_driver=run_driver,
        now=lambda: "2026-07-14T12:00:00Z",
        stderr=io.StringIO(),
    )
    return Bundle(deps, store, order)


def test_worker_holds_lifecycle_lock_and_runs_only_finalized_envelope() -> None:
    bundle = worker_fakes()
    assert run_attempt(valid_envelope(), bundle.deps) == 0
    assert bundle.order == [
        "driver-lock-acquire",
        "active-running",
        "attempt_running",
        "driver-run",
        "attempt_done",
        "active-clear-running",
        "driver-lock-release",
    ]
    assert [
        pointer.status if pointer is not None else None for pointer in bundle.store.active_history
    ] == [
        "pending",
        "running",
        None,
    ]


def test_worker_records_failed_driver_and_clears_active() -> None:
    bundle = worker_fakes(driver_rc=2)
    assert run_attempt(valid_envelope(), bundle.deps) == 1
    assert "attempt_failed" in bundle.order
    assert bundle.store.active is None


def test_worker_observes_cancel_marker_without_editing_rollout_state() -> None:
    bundle = worker_fakes(driver_rc=2)
    bundle.store.events.append(
        RequestEvent(
            request_id=REQUEST_ID,
            event="cancel_requested",
            occurred_at="2026-07-14T12:00:00Z",
            operator="devansh",
            operator_uid=2003,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="running",
            reason="validation abandoned",
        )
    )

    assert run_attempt(valid_envelope(), bundle.deps) == 130
    assert bundle.store.events[-1].event == "cancelled"


def test_worker_ignores_cancel_intent_compensated_after_termination_failure() -> None:
    bundle = worker_fakes()
    for event_type, reason in (
        ("cancel_requested", "validation abandoned"),
        ("cancel_failed", "unit_termination_failed"),
    ):
        bundle.store.events.append(
            RequestEvent(
                request_id=REQUEST_ID,
                event=event_type,  # type: ignore[arg-type]
                occurred_at="2026-07-14T12:00:00Z",
                operator="devansh",
                operator_uid=2003,
                attempt_number=1,
                unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
                status="running",
                reason=reason,
            )
        )

    assert run_attempt(valid_envelope(), bundle.deps) == 0
    assert bundle.store.events[-1].event == "attempt_done"


def test_driver_window_rechecks_pending_signal_before_entering_driver() -> None:
    controller = worker_module._SignalController(requested=True)
    entered = False

    with pytest.raises(worker_module._CancellationSignal):
        with controller.driver_window():
            entered = True

    assert entered is False
    assert controller.driver_interruptible is False


def test_worker_parser_exposes_only_internal_run_attempt_surface() -> None:
    bundle = worker_fakes()
    assert worker_main(["start"], dependencies=bundle.deps) == 2
    assert worker_main(["run-attempt"], dependencies=bundle.deps) == 2
    assert (
        worker_main(
            ["run-attempt", "--env", "/tmp/envelope.json"],
            dependencies=bundle.deps,
        )
        == 2
    )


def _install_fake_signal_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[int, object]:
    handlers: dict[int, object] = {}

    def fake_signal(signum: int, handler: object) -> object:
        previous = handlers.get(signum, signal.SIG_DFL)
        handlers[signum] = handler
        return previous

    monkeypatch.setattr(signal, "signal", fake_signal)
    return handlers


def _deliver(handlers: dict[int, object], signum: int = signal.SIGTERM) -> None:
    handler = handlers[signum]
    assert callable(handler)
    handler(signum, None)


def test_worker_sigterm_during_envelope_load_is_bookkept_deterministically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = worker_fakes()
    handlers = _install_fake_signal_handlers(monkeypatch)

    def load_envelope(path: Path) -> DriverEnvelope:
        del path
        _deliver(handlers)
        return valid_envelope()

    bundle.deps.load_envelope = load_envelope
    rc = worker_main(
        ["run-attempt", "--envelope", "/protected/envelope.json"],
        dependencies=bundle.deps,
    )

    assert rc == 130
    assert bundle.store.events[-1].event == "cancelled"
    assert bundle.store.active is None


@pytest.mark.parametrize(
    ("signal_event", "expected_rc", "terminal_event"),
    [
        ("attempt_running", 130, "cancelled"),
        ("attempt_done", 0, "attempt_done"),
    ],
)
def test_worker_sigterm_cannot_interrupt_terminal_bookkeeping_or_cas_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    signal_event: str,
    expected_rc: int,
    terminal_event: str,
) -> None:
    bundle = worker_fakes()
    handlers = _install_fake_signal_handlers(monkeypatch)
    bundle.deps.load_envelope = lambda path: valid_envelope()
    persist = bundle.store.append_event

    def append_event(event: RequestEvent) -> Path:
        path = persist(event)
        if event.event == signal_event:
            _deliver(handlers)
        return path

    bundle.store.append_event = append_event  # type: ignore[method-assign]

    rc = worker_main(
        ["run-attempt", "--envelope", "/protected/envelope.json"],
        dependencies=bundle.deps,
    )

    assert rc == expected_rc
    assert bundle.store.events[-1].event == terminal_event
    assert bundle.store.active is None
    assert bundle.order[-2:] == ["active-clear-running", "driver-lock-release"]


def test_default_worker_run_uses_exact_sanitized_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.loom_cli.rollout.operator.test_broker import make_config

    config = make_config(tmp_path)
    expected = sanitized_child_environment(config, service_uid=1234)
    environments: list[dict[str, str] | None] = []
    timeouts: list[object] = []

    def fake_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        environments.append(kwargs.get("env"))  # type: ignore[arg-type]
        timeouts.append(kwargs.get("timeout"))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    worker_module._run(["systemctl", "--user", "show"], environment=expected)

    assert environments == [expected]
    assert timeouts == [120]
