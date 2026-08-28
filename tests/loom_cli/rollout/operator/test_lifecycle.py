from __future__ import annotations

import json
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from threading import Event

import pytest

from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.lifecycle import (
    LifecycleBusyError,
    LifecycleCoordinator,
    LifecycleError,
    ReconciliationResult,
)
from loom_cli.rollout.operator.model import (
    ActivePointer,
    CallerIdentity,
    CandidateBinding,
    DriverEnvelope,
    EventStatus,
    RequestEvent,
    RequestEventType,
    RolloutRequest,
)
from loom_cli.rollout.operator.store import RequestStore, RequestStoreError
from loom_cli.rollout.operator.systemd import (
    ActiveState,
    SystemdQueryError,
    SystemdUnitStatus,
    UnitLaunchError,
)
from loom_cli.rollout.state import DriverRecord, RolloutState, StepRecord, StepState
from loom_cli.rollout_lock import RolloutLeaseManager

NOW = datetime(2026, 7, 13, 20, 0, tzinfo=UTC)
BOOT_ID = "11111111-2222-4333-8444-555555555555"


def make_config(tmp_path: Path) -> OperatorConfig:
    runtime_root = tmp_path / "runtime"
    rollout_root = tmp_path / "rollout"
    runtime_root.mkdir(mode=0o700)
    rollout_root.mkdir(mode=0o700)
    runner_repo = tmp_path / "runner" / "repo"
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="refs/heads/dev",
        runner_repo=runner_repo,
        state_root=tmp_path / "state",
        runtime_root=runtime_root,
        rollout_root=rollout_root,
        kubeconfig_path=tmp_path / "kubeconfig",
        cluster_config_path=runner_repo / "deploy/environments/staging.cluster.toml",
        admin_token_source=f"file:{tmp_path}/credentials/admin-token",
        worker_token_source=f"file:{tmp_path}/credentials/worker-token",
        service_token_source=f"file:{tmp_path}/credentials/service-token",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        config_path=tmp_path / "staging-rollout.toml",
        config_sha256="1" * 64,
    )


def make_request(
    config: OperatorConfig,
    request_id: str,
    *,
    sha_char: str,
    operator: str = "alice",
) -> RolloutRequest:
    sha = sha_char * 40
    return RolloutRequest(
        request_id=request_id,
        rollout_id=f"rollout-{request_id}",
        caller=CallerIdentity(operator, 1001),
        candidate=CandidateBinding(
            remote_url="https://github.com/qianyi-sun/loom.git",
            target_ref="origin/dev",
            resolved_sha=sha,
            image_tag=f"staging-{sha[:7]}",
            fetched_at="2026-07-13T19:59:00Z",
        ),
        requested_at="2026-07-13T19:59:30Z",
        runner_config_sha256=config.config_sha256,
        preflight_attestation_sha256="3" * 64,
        preflight_registry_sha256="4" * 64,
        preflight_coverage_sha256="5" * 64,
    )


def make_envelope(
    config: OperatorConfig,
    request: RolloutRequest,
    *,
    attempt_number: int = 1,
) -> DriverEnvelope:
    return DriverEnvelope(
        schema_version=1,
        request_id=request.request_id,
        rollout_id=request.rollout_id,
        initiating_operator=request.caller.username,
        initiating_uid=request.caller.uid,
        attempt_number=attempt_number,
        attempt_operator=request.caller.username,
        attempt_uid=request.caller.uid,
        remote_url=request.candidate.remote_url,
        target_ref=request.candidate.target_ref,
        resolved_sha=request.candidate.resolved_sha,
        image_tag=request.candidate.image_tag,
        fetched_at=request.candidate.fetched_at,
        backup_manifest_path=str(
            config.rollout_root / "backups/20260713-request/backup-manifest.json"
        ),
        backup_manifest_sha256="2" * 64,
        runner_config_sha256=request.runner_config_sha256,
        preflight_attestation_sha256=request.preflight_attestation_sha256,
        preflight_registry_sha256=request.preflight_registry_sha256,
        preflight_coverage_sha256=request.preflight_coverage_sha256,
        cluster_name=config.cluster_name,
        namespace=config.namespace,
        environment=config.environment,
        cp_url=config.cp_url,
        cluster_config_path=str(config.cluster_config_path),
        rollout_root=str(config.rollout_root),
        admin_token_source=config.admin_token_source,
        worker_token_source=config.worker_token_source,
        service_token_source=config.service_token_source,
        expect_admin_token_fingerprint=config.expect_admin_token_fingerprint,
        smoke_on_behalf_username=config.smoke_on_behalf_username,
        smoke_on_behalf_team_id=config.smoke_on_behalf_team_id,
        scope=config.scope,
        gb10_prep_concurrency=config.gb10_prep_concurrency,
        resume=attempt_number > 1,
    )


def pointer(request_id: str, *, attempt_number: int = 1) -> ActivePointer:
    return ActivePointer(
        request_id=request_id,
        attempt_number=attempt_number,
        unit_name=f"loom-staging-rollout-{request_id}-{attempt_number}.service",
        status="pending",
    )


class FakeSystemd:
    def __init__(self) -> None:
        self.status: SystemdUnitStatus | None = None
        self.started: list[tuple[Path, str]] = []
        self.queried: list[str] = []

    def start_attempt(self, envelope_path: Path, unit_name: str) -> None:
        self.started.append((envelope_path, unit_name))

    def show(self, unit_name: str) -> SystemdUnitStatus | None:
        self.queried.append(unit_name)
        return self.status


class FailingSystemd(FakeSystemd):
    def start_attempt(self, envelope_path: Path, unit_name: str) -> None:
        self.started.append((envelope_path, unit_name))
        raise UnitLaunchError("captured stderr contains SECRET=do-not-leak")


class ReplacingFailingSystemd(FakeSystemd):
    def __init__(self, store: RequestStore, replacement: ActivePointer) -> None:
        super().__init__()
        self.store = store
        self.replacement = replacement

    def start_attempt(self, envelope_path: Path, unit_name: str) -> None:
        original = self.store.read_active()
        assert original is not None
        assert self.store.clear_active_if_matches(original)
        self.store.set_active(self.replacement)
        raise UnitLaunchError("unit launch failed")


class FailingLaunchEventStore(RequestStore):
    def append_event(self, event: RequestEvent) -> Path:
        if event.event == "launch_failed":
            raise RequestStoreError("launch failure evidence fsync failed")
        return super().append_event(event)


def make_coordinator(
    config: OperatorConfig,
    *,
    store: RequestStore | None = None,
    systemd: FakeSystemd | None = None,
) -> LifecycleCoordinator:
    return LifecycleCoordinator(
        config,
        store=store or RequestStore(config.state_root),
        systemd=systemd or FakeSystemd(),
        now=lambda: NOW,
        boot_id=lambda: BOOT_ID,
        maintenance_owner_uid=os.geteuid(),
        maintenance_owner_gid=os.getegid(),
    )


def persist_attempt(
    config: OperatorConfig,
    store: RequestStore,
    request_id: str,
    *,
    sha_char: str,
) -> tuple[RolloutRequest, DriverEnvelope]:
    request = make_request(config, request_id, sha_char=sha_char)
    envelope = make_envelope(config, request)
    store.create_request(request)
    store.publish_attempt_envelope(envelope)
    return request, envelope


def test_coordinator_rejects_store_root_that_differs_from_config(tmp_path: Path) -> None:
    config = make_config(tmp_path)

    with pytest.raises(LifecycleError):
        LifecycleCoordinator(
            config,
            store=RequestStore(tmp_path / "other-state"),
            systemd=FakeSystemd(),
        )


def unit_status(
    *,
    active_state: ActiveState = "active",
    main_pid: int = 4321,
) -> SystemdUnitStatus:
    return SystemdUnitStatus(
        unit_name="loom-staging-rollout-req-alpha-1.service",
        active_state=active_state,
        sub_state="running" if active_state == "active" else "dead",
        result="success",
        exec_main_status=0,
        main_pid=main_pid,
        exec_main_start_timestamp="Mon 2026-07-13 20:00:00 UTC",
        exec_main_exit_timestamp=(
            None if active_state == "active" else "Mon 2026-07-13 20:05:00 UTC"
        ),
    )


def write_rollout_state(
    config: OperatorConfig,
    envelope: DriverEnvelope,
    *,
    status: str,
    pid: int = 4321,
    boot_id: str | None = BOOT_ID,
) -> Path:
    if status == "done":
        steps = [
            StepRecord(10, "env-state", state=StepState.DONE),
            StepRecord(11, "cluster-up", state=StepState.DONE),
        ]
        current_step = None
        driver = None
    elif status == "failed":
        steps = [
            StepRecord(10, "env-state", state=StepState.DONE),
            StepRecord(11, "cluster-up", state=StepState.FAILED),
        ]
        current_step = 11
        driver = None
    else:
        steps = [
            StepRecord(10, "env-state", state=StepState.DONE),
            StepRecord(
                11,
                "cluster-up",
                state=StepState.RUNNING,
                started_at="2026-07-13T20:00:00Z",
            ),
        ]
        current_step = 11
        driver = DriverRecord(
            pid=pid,
            hostname="platform-dev",
            boot_id=boot_id,
            started_at="2026-07-13T20:00:00Z",
            updated_at="2026-07-13T20:00:30Z",
        )
    state = RolloutState(
        rollout_id=envelope.rollout_id,
        steps=steps,
        status=status,
        current_step=current_step,
        driver=driver,
    )
    path = config.rollout_root / "rollouts" / envelope.rollout_id / "state.json"
    state.save(path)
    return path


def test_second_request_fails_even_when_image_tags_differ(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    request, _ = persist_attempt(config, store, "req-alpha", sha_char="a")
    store.append_event(
        RequestEvent(
            request_id=request.request_id,
            event="attempt_running",
            occurred_at="2026-07-13T20:00:00Z",
            operator=request.caller.username,
            operator_uid=request.caller.uid,
            attempt_number=1,
            unit_name=pointer(request.request_id).unit_name,
            status="running",
            current_step="11-cluster-up",
        )
    )
    coordinator = make_coordinator(config, store=store)
    coordinator.reserve_active(pointer("req-alpha"))

    with pytest.raises(LifecycleBusyError) as caught:
        coordinator.reserve_active(pointer("req-bravo"))

    assert caught.value.safe_status == {
        "request_id": "req-alpha",
        "initiator": "alice",
        "resolved_sha": "a" * 40,
        "image_tag": "staging-aaaaaaa",
        "attempt_number": 1,
        "unit_name": "loom-staging-rollout-req-alpha-1.service",
        "status": "pending",
        "current_step": "11-cluster-up",
        "started_at": "2026-07-13T20:00:00Z",
    }
    rendered = json.dumps(caught.value.safe_status)
    assert "admin-token" not in rendered
    assert "worker-token" not in rendered
    assert "service-token" not in rendered


def test_busy_status_uses_available_request_metadata_without_inventing_attempt_data(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    store.create_request(make_request(config, "req-alpha", sha_char="a"))
    coordinator = make_coordinator(config, store=store)
    coordinator.reserve_active(pointer("req-alpha"))

    with pytest.raises(LifecycleBusyError) as caught:
        coordinator.reserve_active(pointer("req-bravo"))

    assert caught.value.safe_status == {
        "request_id": "req-alpha",
        "initiator": "alice",
        "resolved_sha": "a" * 40,
        "image_tag": "staging-aaaaaaa",
        "attempt_number": 1,
        "unit_name": "loom-staging-rollout-req-alpha-1.service",
        "status": "pending",
    }


def test_busy_status_omits_malformed_event_step_and_start_time(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    request, _ = persist_attempt(config, store, "req-alpha", sha_char="a")
    store.append_event(
        RequestEvent(
            request_id=request.request_id,
            event="attempt_running",
            occurred_at="TOKEN=secret-start-time",
            operator=request.caller.username,
            operator_uid=request.caller.uid,
            attempt_number=1,
            unit_name=pointer(request.request_id).unit_name,
            status="running",
            current_step="SECRET=secret-step",
        )
    )
    coordinator = make_coordinator(config, store=store)
    coordinator.reserve_active(pointer("req-alpha"))

    with pytest.raises(LifecycleBusyError) as caught:
        coordinator.reserve_active(pointer("req-bravo"))

    assert "current_step" not in caught.value.safe_status
    assert "started_at" not in caught.value.safe_status
    rendered = json.dumps(caught.value.safe_status)
    assert "secret" not in rendered.lower()


@pytest.mark.parametrize(
    ("guard_name", "filename"),
    [("launch_guard", "launch.lock"), ("driver_guard", "staging.driver.lock")],
)
def test_lifecycle_guards_are_private_nonblocking_locks(
    tmp_path: Path,
    guard_name: str,
    filename: str,
) -> None:
    config = make_config(tmp_path)
    coordinator = make_coordinator(config)
    competitor = make_coordinator(config)
    guard = getattr(coordinator, guard_name)
    competing_guard = getattr(competitor, guard_name)

    with guard():
        metadata = os.lstat(config.runtime_root / filename)
        assert stat.S_ISREG(metadata.st_mode)
        assert metadata.st_uid == os.geteuid()
        assert stat.S_IMODE(metadata.st_mode) == 0o600
        with pytest.raises(LifecycleBusyError) as caught:
            with competing_guard():
                pass
        assert caught.value.safe_status == {
            "status": "busy",
            "lock": filename,
        }

    with competing_guard():
        pass


def test_launch_and_driver_guards_use_independent_lock_files(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    coordinator = make_coordinator(config)

    with coordinator.launch_guard():
        with coordinator.driver_guard():
            assert (config.runtime_root / "launch.lock").is_file()
            assert (config.runtime_root / "staging.driver.lock").is_file()


def test_maintenance_marker_blocks_admission_under_launch_guard(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    marker = config.runtime_root / "maintenance"
    marker.touch(mode=0o600)
    coordinator = make_coordinator(config)

    with coordinator.launch_guard():
        with pytest.raises(LifecycleBusyError) as caught:
            coordinator.assert_admission_open()

    assert caught.value.safe_status == {"status": "busy", "reason": "maintenance"}


def test_durable_backup_retention_claim_blocks_admission_without_maintenance_marker(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    store.claim_backup_retention("a" * 64, ("payload-retire01",))
    coordinator = make_coordinator(config, store=store)

    assert not (config.runtime_root / "maintenance").exists()
    with coordinator.launch_guard():
        with pytest.raises(LifecycleBusyError) as caught:
            coordinator.assert_admission_open()

    assert caught.value.safe_status == {
        "status": "busy",
        "reason": "backup_retention_busy",
    }


def test_durable_artifact_retention_claim_blocks_admission_without_maintenance_marker(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    store.claim_preflight_artifact_retention("a" * 64, ("b" * 64,))
    coordinator = make_coordinator(config, store=store)

    assert not (config.runtime_root / "maintenance").exists()
    with coordinator.launch_guard():
        with pytest.raises(LifecycleBusyError) as caught:
            coordinator.assert_admission_open()

    assert caught.value.safe_status == {
        "status": "busy",
        "reason": "preflight_artifact_retention_busy",
    }


def test_admission_check_rejects_unsafe_maintenance_marker(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    (config.runtime_root / "maintenance").symlink_to(tmp_path / "outside")
    coordinator = make_coordinator(config)

    with coordinator.launch_guard():
        with pytest.raises(LifecycleError, match="maintenance admission marker is unsafe"):
            coordinator.assert_admission_open()


def test_protected_maintenance_requires_exact_marker_under_launch_guard(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    coordinator = make_coordinator(config)
    with coordinator.launch_guard():
        with pytest.raises(LifecycleError, match="marker is unavailable"):
            coordinator.assert_maintenance_active()

    marker = config.runtime_root / "maintenance"
    marker.write_text("maintenance\n")
    marker.chmod(0o600)
    with coordinator.launch_guard():
        coordinator.assert_maintenance_active()

    marker.chmod(0o644)
    with coordinator.launch_guard():
        with pytest.raises(LifecycleError, match="marker is unsafe"):
            coordinator.assert_maintenance_active()


def test_protected_maintenance_idle_check_never_reconciles_active_pointer(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    marker = config.runtime_root / "maintenance"
    marker.write_text("maintenance\n")
    marker.chmod(0o600)
    store = RequestStore(config.state_root)
    active = pointer("req-alpha")
    store.set_active(active)
    coordinator = make_coordinator(config, store=store)

    with coordinator.launch_guard():
        with pytest.raises(LifecycleBusyError) as caught:
            coordinator.assert_maintenance_idle()

    assert caught.value.safe_status["request_id"] == "req-alpha"
    assert store.read_active() == active


def test_lifecycle_guard_never_follows_a_lock_symlink(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    target = tmp_path / "outside-lock"
    target.touch(mode=0o600)
    (config.runtime_root / "launch.lock").symlink_to(target)

    with pytest.raises(LifecycleError):
        with make_coordinator(config).launch_guard():
            pass

    assert stat.S_IMODE(target.stat().st_mode) == 0o600


def test_lifecycle_guard_rejects_group_writable_runtime_root(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.runtime_root.chmod(0o720)

    with pytest.raises(LifecycleError):
        with make_coordinator(config).launch_guard():
            pass


def test_lifecycle_guard_rejects_symlinked_runtime_root(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    real_runtime_root = config.runtime_root
    symlinked_runtime_root = tmp_path / "runtime-link"
    symlinked_runtime_root.symlink_to(real_runtime_root, target_is_directory=True)
    symlinked_config = replace(config, runtime_root=symlinked_runtime_root)

    with pytest.raises(LifecycleError):
        with make_coordinator(symlinked_config).launch_guard():
            pass


def test_lifecycle_guard_does_not_reclassify_body_oserror(tmp_path: Path) -> None:
    coordinator = make_coordinator(make_config(tmp_path))
    driver_error = OSError("driver root cause")

    with pytest.raises(OSError) as caught:
        with coordinator.driver_guard():
            raise driver_error

    assert caught.value is driver_error


def test_mutation_lease_releases_while_driver_guard_remains_held(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    coordinator = make_coordinator(config)
    competitor = make_coordinator(config)
    mutation_manager = RolloutLeaseManager(tmp_path / "mutation-locks")

    with coordinator.driver_guard():
        lease = mutation_manager.acquire(
            environment="staging",
            owner_id="step-owner",
            ttl_seconds=60,
            command=["loom", "cluster", "up"],
        )
        lease.release()
        with pytest.raises(LifecycleBusyError):
            with competitor.driver_guard():
                pass


def test_release_active_is_compare_and_delete_only(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    coordinator = make_coordinator(config, store=store)
    first = pointer("req-alpha")
    replacement = pointer("req-bravo")
    coordinator.reserve_active(first)
    assert store.clear_active_if_matches(first)
    store.set_active(replacement)

    assert not coordinator.release_active(first)
    assert store.read_active() == replacement


def test_launch_reserves_finalized_envelope_immediately_before_systemd_start(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")

    class ObservingSystemd(FakeSystemd):
        active_at_start: ActivePointer | None = None

        def start_attempt(self, envelope_path: Path, unit_name: str) -> None:
            self.active_at_start = store.read_active()
            super().start_attempt(envelope_path, unit_name)

    systemd = ObservingSystemd()
    coordinator = make_coordinator(config, store=store, systemd=systemd)

    launched = coordinator.launch(envelope)

    expected_pointer = pointer("req-alpha")
    expected_path = config.state_root / "requests/req-alpha/attempts/1/envelope.json"
    assert launched == expected_pointer
    assert systemd.active_at_start == expected_pointer
    assert systemd.started == [(expected_path, expected_pointer.unit_name)]
    assert store.read_active() == expected_pointer


def test_attempt_launch_rechecks_maintenance_admission_under_launch_lock(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    systemd = FakeSystemd()
    marker = config.runtime_root / "maintenance"
    marker.write_text("maintenance\n")
    marker.chmod(0o600)

    with pytest.raises(LifecycleBusyError, match="maintenance"):
        make_coordinator(config, store=store, systemd=systemd).launch(envelope)

    assert systemd.started == []
    assert store.read_active() is None


def test_launch_rejects_an_envelope_that_was_not_immutably_published(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    request = make_request(config, "req-alpha", sha_char="a")
    store.create_request(request)
    envelope = make_envelope(config, request)
    systemd = FakeSystemd()

    with pytest.raises(LifecycleError):
        make_coordinator(config, store=store, systemd=systemd).launch(envelope)

    assert systemd.started == []
    assert store.read_active() is None


def test_failed_unit_launch_records_safe_failure_and_clears_matching_pointer(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    coordinator = make_coordinator(config, store=store, systemd=FailingSystemd())

    with pytest.raises(UnitLaunchError):
        coordinator.launch(envelope)

    assert store.read_active() is None
    failure = store.read_events("req-alpha")[-1]
    assert failure.event == "launch_failed"
    assert failure.status == "failed"
    assert failure.reason == "unit_launch_failed"
    assert "SECRET" not in json.dumps(failure.to_dict())


def test_failed_unit_launch_never_clears_a_replacement_pointer(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    replacement = pointer("req-bravo")
    systemd = ReplacingFailingSystemd(store, replacement)

    with pytest.raises(UnitLaunchError):
        make_coordinator(config, store=store, systemd=systemd).launch(envelope)

    assert store.read_active() == replacement


def test_failed_unit_launch_preserves_pointer_when_failure_event_cannot_persist(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = FailingLaunchEventStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")

    with pytest.raises(LifecycleError):
        make_coordinator(config, store=store, systemd=FailingSystemd()).launch(envelope)

    assert store.read_active() == pointer("req-alpha")
    assert [event.event for event in store.read_events("req-alpha")] == ["launch_pending"]


def test_reconcile_cannot_clear_pointer_during_unit_launch(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    start_entered = Event()
    allow_start = Event()

    class BlockingSystemd(FakeSystemd):
        def start_attempt(self, envelope_path: Path, unit_name: str) -> None:
            self.started.append((envelope_path, unit_name))
            start_entered.set()
            assert allow_start.wait(timeout=2)

    launcher = make_coordinator(config, store=store, systemd=BlockingSystemd())
    reconciler = make_coordinator(config, store=store)

    def launch_under_broker_guard() -> ActivePointer:
        with launcher.launch_guard():
            return launcher.launch(envelope)

    with ThreadPoolExecutor(max_workers=1) as pool:
        launch = pool.submit(launch_under_broker_guard)
        assert start_entered.wait(timeout=2)
        try:
            result = reconciler.reconcile_active()
        finally:
            allow_start.set()
        launched = launch.result(timeout=2)

    assert result.outcome == "busy"
    assert result.safe_status["reason"] == "launch_in_progress"
    assert not result.cleared
    assert store.read_active() == launched == pointer("req-alpha")


def test_reconcile_without_active_pointer_is_idle(tmp_path: Path) -> None:
    result = make_coordinator(make_config(tmp_path)).reconcile_active()

    assert result == ReconciliationResult(
        outcome="idle",
        pointer=None,
        cleared=False,
        safe_status={},
    )


def test_reconcile_missing_unit_without_state_records_stale_failure_for_resume(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)

    result = make_coordinator(config, store=store).reconcile_active()

    assert result.outcome == "stale"
    assert result.cleared
    assert result.pointer == active
    assert result.safe_status["reason"] == "unit_missing_without_terminal_rollout_state"
    assert store.read_active() is None
    failure = store.read_events("req-alpha")[-1]
    assert failure.event == "attempt_failed"
    assert failure.reason == "unit_missing_without_terminal_rollout_state"


def test_reconcile_running_unit_with_matching_driver_remains_busy(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status="running")
    systemd = FakeSystemd()
    systemd.status = unit_status()

    result = make_coordinator(config, store=store, systemd=systemd).reconcile_active()

    assert result.outcome == "busy"
    assert not result.cleared
    assert result.safe_status["reason"] == "unit_running"
    assert result.safe_status["current_step"] == "11-cluster-up"
    assert store.read_active() == active


@pytest.mark.parametrize(
    ("unit", "state_status", "expected_outcome"),
    [
        (unit_status(active_state="inactive", main_pid=0), "done", "done"),
        (None, "failed", "failed"),
    ],
)
def test_reconcile_uses_terminal_rollout_state_for_completed_or_missing_unit(
    tmp_path: Path,
    unit: SystemdUnitStatus | None,
    state_status: str,
    expected_outcome: str,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status=state_status)
    systemd = FakeSystemd()
    systemd.status = unit

    result = make_coordinator(config, store=store, systemd=systemd).reconcile_active()

    assert result.outcome == expected_outcome
    assert result.cleared
    assert store.read_active() is None


def test_reconcile_done_state_with_failed_unit_records_stale_not_success(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status="done")
    systemd = FakeSystemd()
    systemd.status = replace(
        unit_status(active_state="failed", main_pid=0),
        result="exit-code",
        exec_main_status=1,
    )

    result = make_coordinator(config, store=store, systemd=systemd).reconcile_active()

    assert result.outcome == "stale"
    assert result.safe_status["reason"] == "terminal_state_conflicts_with_unit_failure"
    assert result.cleared
    assert store.read_active() is None
    failure = store.read_events("req-alpha")[-1]
    assert failure.event == "attempt_failed"
    assert failure.reason == "terminal_state_conflicts_with_unit_failure"


def test_reconcile_done_state_with_missing_unit_requires_done_event(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status="done")

    result = make_coordinator(config, store=store).reconcile_active()

    assert result.outcome == "stale"
    assert result.safe_status["reason"] == "unit_missing_without_corroborated_success"
    assert result.cleared
    assert store.read_active() is None


def test_reconcile_done_state_with_missing_unit_and_done_event_is_done(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    request, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status="done")
    store.append_event(
        RequestEvent(
            request_id=request.request_id,
            event="attempt_done",
            occurred_at="2026-07-13T20:05:00Z",
            operator=request.caller.username,
            operator_uid=request.caller.uid,
            attempt_number=1,
            unit_name=active.unit_name,
            status="done",
        )
    )

    result = make_coordinator(config, store=store).reconcile_active()

    assert result.outcome == "done"
    assert result.cleared
    assert store.read_active() is None


@pytest.mark.parametrize(
    "failure_event",
    ["attempt_failed", "launch_failed"],
)
@pytest.mark.parametrize(
    "failure_first",
    [False, True],
    ids=["done-then-failed", "failed-then-done"],
)
def test_reconcile_missing_unit_rejects_contradictory_terminal_event_in_any_order(
    tmp_path: Path,
    failure_event: RequestEventType,
    failure_first: bool,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    request, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status="done")
    terminal_events: list[tuple[RequestEventType, EventStatus]] = [
        ("attempt_done", "done"),
        (failure_event, "failed"),
    ]
    if failure_first:
        terminal_events.reverse()
    for index, (event, status) in enumerate(terminal_events, start=5):
        store.append_event(
            RequestEvent(
                request_id=request.request_id,
                event=event,
                occurred_at=f"2026-07-13T20:0{index}:00Z",
                operator=request.caller.username,
                operator_uid=request.caller.uid,
                attempt_number=1,
                unit_name=active.unit_name,
                status=status,
            )
        )

    result = make_coordinator(config, store=store).reconcile_active()

    assert result.outcome == "stale"
    assert result.safe_status["reason"] == "unit_missing_without_corroborated_success"
    assert result.cleared
    assert store.read_active() is None


@pytest.mark.parametrize(
    ("failure_event", "unrelated_identity"),
    [
        ("attempt_failed", "attempt"),
        ("launch_failed", "unit"),
    ],
)
def test_reconcile_missing_unit_ignores_unrelated_terminal_failure(
    tmp_path: Path,
    failure_event: RequestEventType,
    unrelated_identity: str,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    request, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status="done")
    store.append_event(
        RequestEvent(
            request_id=request.request_id,
            event="attempt_done",
            occurred_at="2026-07-13T20:05:00Z",
            operator=request.caller.username,
            operator_uid=request.caller.uid,
            attempt_number=1,
            unit_name=active.unit_name,
            status="done",
        )
    )
    store.append_event(
        RequestEvent(
            request_id=request.request_id,
            event=failure_event,
            occurred_at="2026-07-13T20:06:00Z",
            operator=request.caller.username,
            operator_uid=request.caller.uid,
            attempt_number=2 if unrelated_identity == "attempt" else 1,
            unit_name=(
                "loom-staging-rollout-req-alpha-2.service"
                if unrelated_identity == "attempt"
                else "loom-staging-rollout-req-bravo-1.service"
            ),
            status="failed",
        )
    )

    result = make_coordinator(config, store=store).reconcile_active()

    assert result.outcome == "done"
    assert result.cleared
    assert store.read_active() is None


@pytest.mark.parametrize(
    "cancelled_first",
    [False, True],
    ids=["done-then-cancelled", "cancelled-then-done"],
)
def test_reconcile_missing_unit_rejects_matching_cancelled_event_in_any_order(
    tmp_path: Path,
    cancelled_first: bool,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    request, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status="done")
    terminal_events: list[tuple[RequestEventType, EventStatus]] = [
        ("attempt_done", "done"),
        ("cancelled", "cancelled"),
    ]
    if cancelled_first:
        terminal_events.reverse()
    for index, (event, status) in enumerate(terminal_events, start=5):
        store.append_event(
            RequestEvent(
                request_id=request.request_id,
                event=event,
                occurred_at=f"2026-07-13T20:0{index}:00Z",
                operator=request.caller.username,
                operator_uid=request.caller.uid,
                attempt_number=1,
                unit_name=active.unit_name,
                status=status,
            )
        )

    result = make_coordinator(config, store=store).reconcile_active()

    assert result.outcome == "stale"
    assert result.safe_status["reason"] == "unit_missing_without_corroborated_success"
    assert result.cleared
    assert store.read_active() is None


@pytest.mark.parametrize("unrelated_identity", ["attempt", "unit"])
def test_reconcile_missing_unit_ignores_unrelated_cancelled_event(
    tmp_path: Path,
    unrelated_identity: str,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    request, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status="done")
    store.append_event(
        RequestEvent(
            request_id=request.request_id,
            event="attempt_done",
            occurred_at="2026-07-13T20:05:00Z",
            operator=request.caller.username,
            operator_uid=request.caller.uid,
            attempt_number=1,
            unit_name=active.unit_name,
            status="done",
        )
    )
    store.append_event(
        RequestEvent(
            request_id=request.request_id,
            event="cancelled",
            occurred_at="2026-07-13T20:06:00Z",
            operator=request.caller.username,
            operator_uid=request.caller.uid,
            attempt_number=2 if unrelated_identity == "attempt" else 1,
            unit_name=(
                "loom-staging-rollout-req-alpha-2.service"
                if unrelated_identity == "attempt"
                else "loom-staging-rollout-req-bravo-1.service"
            ),
            status="cancelled",
        )
    )

    result = make_coordinator(config, store=store).reconcile_active()

    assert result.outcome == "done"
    assert result.cleared
    assert store.read_active() is None


def test_reconcile_completed_unit_without_terminal_state_is_stale_not_success(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status="running")
    systemd = FakeSystemd()
    systemd.status = unit_status(active_state="inactive", main_pid=0)

    result = make_coordinator(config, store=store, systemd=systemd).reconcile_active()

    assert result.outcome == "stale"
    assert result.safe_status["reason"] == "unit_inactive_without_terminal_rollout_state"
    assert result.cleared
    assert store.read_active() is None


@pytest.mark.parametrize(
    ("pid", "boot_id", "expected_reason"),
    [
        (9999, BOOT_ID, "driver_pid_mismatch"),
        (4321, "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", "driver_boot_id_mismatch"),
    ],
)
def test_reconcile_running_unit_pid_or_boot_mismatch_fails_closed(
    tmp_path: Path,
    pid: int,
    boot_id: str,
    expected_reason: str,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status="running", pid=pid, boot_id=boot_id)
    systemd = FakeSystemd()
    systemd.status = unit_status()

    result = make_coordinator(config, store=store, systemd=systemd).reconcile_active()

    assert result.outcome == "busy"
    assert result.safe_status["reason"] == expected_reason
    assert not result.cleared
    assert store.read_active() == active


def test_reconcile_unit_query_failure_preserves_active_pointer(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    write_rollout_state(config, envelope, status="running")

    class UnavailableSystemd(FakeSystemd):
        def show(self, unit_name: str) -> SystemdUnitStatus | None:
            raise SystemdQueryError("captured output SECRET=do-not-leak")

    result = make_coordinator(
        config,
        store=store,
        systemd=UnavailableSystemd(),
    ).reconcile_active()

    assert result.outcome == "busy"
    assert result.safe_status["reason"] == "unit_status_unavailable"
    assert "SECRET" not in json.dumps(result.safe_status)
    assert store.read_active() == active


@pytest.mark.parametrize(
    "malformed_state",
    [
        "[]\n",
        (
            '{"current_step":1,"driver":null,"rollout_id":"rollout-req-alpha",'
            '"status":"running","steps":[1]}\n'
        ),
    ],
)
def test_reconcile_malformed_state_shape_fails_closed_as_busy(
    tmp_path: Path,
    malformed_state: str,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    state_path = config.rollout_root / "rollouts" / envelope.rollout_id / "state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(malformed_state, encoding="utf-8")
    systemd = FakeSystemd()
    systemd.status = unit_status()

    result = make_coordinator(config, store=store, systemd=systemd).reconcile_active()

    assert result.outcome == "busy"
    assert result.safe_status["reason"] == "rollout_state_unavailable"
    assert not result.cleared
    assert store.read_active() == active


def test_reconcile_never_exposes_malformed_state_current_step(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    _, envelope = persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    store.set_active(active)
    state_path = write_rollout_state(config, envelope, status="running")
    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
    state_payload["current_step"] = "TOKEN=secret-step"
    state_path.write_text(json.dumps(state_payload) + "\n", encoding="utf-8")

    result = make_coordinator(config, store=store).reconcile_active()

    assert result.outcome == "stale"
    rendered = json.dumps(
        {
            "safe_status": result.safe_status,
            "events": [event.to_dict() for event in store.read_events("req-alpha")],
        }
    )
    assert "secret-step" not in rendered
    assert "current_step" not in result.safe_status


def test_reconcile_preserves_pointer_unit_that_does_not_match_immutable_attempt(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    persist_attempt(config, store, "req-alpha", sha_char="a")
    active = ActivePointer("req-alpha", 1, "generic-safe.service", "pending")
    store.set_active(active)
    systemd = FakeSystemd()

    result = make_coordinator(config, store=store, systemd=systemd).reconcile_active()

    assert result.outcome == "busy"
    assert result.safe_status["reason"] == "active_pointer_identity_mismatch"
    assert not result.cleared
    assert store.read_active() == active
    assert systemd.queried == []


def test_reconcile_preserves_pointer_when_request_and_envelope_binding_mismatch(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    persist_attempt(config, store, "req-alpha", sha_char="a")
    request_path = config.state_root / "requests/req-alpha/request.json"
    request_payload = json.loads(request_path.read_text(encoding="utf-8"))
    request_payload["candidate"]["resolved_sha"] = "b" * 40
    request_payload["candidate"]["image_tag"] = "staging-bbbbbbb"
    request_path.write_text(json.dumps(request_payload) + "\n", encoding="utf-8")
    active = pointer("req-alpha")
    store.set_active(active)
    systemd = FakeSystemd()
    systemd.status = unit_status()

    result = make_coordinator(config, store=store, systemd=systemd).reconcile_active()

    assert result.outcome == "busy"
    assert result.safe_status["reason"] == "immutable_attempt_binding_mismatch"
    assert not result.cleared
    assert store.read_active() == active
    assert systemd.queried == []


def test_reconcile_clear_is_cas_protected_against_replacement_pointer(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = RequestStore(config.state_root)
    persist_attempt(config, store, "req-alpha", sha_char="a")
    active = pointer("req-alpha")
    replacement = pointer("req-bravo")
    store.set_active(active)

    class ReplacingShowSystemd(FakeSystemd):
        def show(self, unit_name: str) -> SystemdUnitStatus | None:
            assert store.clear_active_if_matches(active)
            store.set_active(replacement)
            return None

    result = make_coordinator(
        config,
        store=store,
        systemd=ReplacingShowSystemd(),
    ).reconcile_active()

    assert result.outcome == "stale"
    assert not result.cleared
    assert store.read_active() == replacement
