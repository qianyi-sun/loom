from __future__ import annotations

import grp
import hashlib
import io
import json
import os
import pwd
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

import pytest

from loom_cli.rollout.operator import broker as broker_module
from loom_cli.rollout.operator.backup import (
    BackupError,
    BackupPolicyLimitError,
    VerifiedBackup,
)
from loom_cli.rollout.operator.backup_rotation import begin_candidate, fail_candidate
from loom_cli.rollout.operator.broker import BrokerDependencies
from loom_cli.rollout.operator.broker import main as broker_main
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.final_gate_store import FinalGateExecutionStore
from loom_cli.rollout.operator.lifecycle import LifecycleBusyError, ReconciliationResult
from loom_cli.rollout.operator.model import (
    ActivePointer,
    CallerIdentity,
    CandidateBinding,
    DriverEnvelope,
    PreflightRequest,
    RequestEvent,
    RolloutRequest,
)
from loom_cli.rollout.operator.policy import PolicyError, sanitized_child_environment
from loom_cli.rollout.operator.preflight import PreflightCheck, PreflightReport
from loom_cli.rollout.operator.store import RequestStore, RequestStoreError
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_contract import (
    CheckExecution,
    CheckOperation,
    CheckOutcome,
    StageCapability,
)
from loom_cli.rollout.preflight_pipeline import PreflightAssessment, PreflightPipeline
from tests.loom_cli.rollout.test_preflight_pipeline import (
    _context as pipeline_context,
)
from tests.loom_cli.rollout.test_preflight_pipeline import _registry as pipeline_registry
from tests.loom_cli.rollout.test_preflight_runtime import _runtime as preflight_runtime

NOW = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
SHA = "a" * 40
REQUEST_ID = "req-alpha"
ROLLOUT_ID = "rollout-alpha"
ARTIFACT_BUNDLE_SHA256 = "9" * 64
PREFLIGHT_ARTIFACT_BUNDLE_SHA256 = "1" * 64


def _published_assessment(tmp_path: Path) -> PreflightAssessment:
    runtime = preflight_runtime(tmp_path)
    plan = runtime.prebackup_plan(runtime.candidate)
    return PreflightPipeline(
        registry=plan.registry,
        store=PreflightAttestationStore(tmp_path / "published-assessment-attestations"),
        now=lambda: NOW,
    ).assess(context=plan.context)


def make_config(tmp_path: Path) -> OperatorConfig:
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="refs/heads/dev",
        runner_repo=tmp_path / "runner/repo",
        state_root=tmp_path / "state",
        runtime_root=tmp_path / "runtime",
        rollout_root=tmp_path / "data",
        kubeconfig_path=tmp_path / "kubeconfig",
        cluster_config_path=tmp_path / "runner/repo/deploy/environments/staging.cluster.toml",
        admin_token_source=f"file:{tmp_path}/admin-token",
        worker_token_source=f"file:{tmp_path}/worker-token",
        service_token_source=f"file:{tmp_path}/service-token",
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


class FakeStore:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.requests: dict[str, RolloutRequest] = {}
        self.preflight_requests: dict[str, PreflightRequest] = {}
        self.events: dict[str, list[RequestEvent]] = {}
        self.envelopes: dict[tuple[str, int], DriverEnvelope] = {}
        self.active: ActivePointer | None = None

    def create_request(self, request: RolloutRequest) -> Path:
        self.order.append("request")
        self.requests[request.request_id] = request
        self.preflight_requests[request.request_id] = PreflightRequest(
            request_id=request.request_id,
            rollout_id=request.rollout_id,
            caller=request.caller,
            candidate=request.candidate,
            candidate_tree=request.candidate.resolved_tree or "b" * 40,
            requested_at=request.requested_at,
            runner_config_sha256=request.runner_config_sha256,
            preflight_assessment_sha256="6" * 64,
            preflight_registry_sha256=request.preflight_registry_sha256,
            preflight_coverage_sha256=request.preflight_coverage_sha256,
            mutation_epoch=7,
            environment="staging",
            namespace="loom-staging",
        )
        self.events[request.request_id] = []
        return Path("/request.json")

    def read_request(self, request_id: str) -> RolloutRequest:
        if request_id not in self.requests:
            raise RuntimeError("request does not exist")
        return self.requests[request_id]

    def read_preflight_request(self, request_id: str) -> PreflightRequest:
        if request_id not in self.preflight_requests:
            raise RuntimeError("preflight request does not exist")
        return self.preflight_requests[request_id]

    def append_event(self, event: RequestEvent) -> Path:
        self.events[event.request_id].append(event)
        return Path("/events.jsonl")

    def read_events(self, request_id: str) -> list[RequestEvent]:
        if request_id not in self.events:
            raise RuntimeError("request does not exist")
        return list(self.events[request_id])

    def publish_attempt_envelope(self, envelope: DriverEnvelope) -> Path:
        self.order.append("envelope-finalize")
        self.envelopes[(envelope.request_id, envelope.attempt_number)] = envelope
        return Path(
            f"/state/requests/{envelope.request_id}/attempts/"
            f"{envelope.attempt_number}/envelope.json"
        )

    def read_attempt_envelope(self, request_id: str, attempt_number: int) -> DriverEnvelope:
        return self.envelopes[(request_id, attempt_number)]

    def next_attempt_number(self, request_id: str) -> int:
        attempts = [number for (stored, number) in self.envelopes if stored == request_id]
        return max(attempts, default=0) + 1

    def read_active(self) -> ActivePointer | None:
        return self.active


class FakeCandidate:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.fetch_count = 0

    def bind(self) -> CandidateBinding:
        self.order.append("fetch")
        self.fetch_count += 1
        return CandidateBinding(
            remote_url="https://github.com/qianyi-sun/loom.git",
            target_ref="origin/dev",
            resolved_sha=SHA,
            image_tag="staging-aaaaaaa",
            fetched_at="2026-07-14T12:00:00Z",
            resolved_tree="b" * 40,
        )


class FakeBackup:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.create_count = 0
        self.cleanup_count = 0

    def create(
        self,
        request: RolloutRequest,
        *,
        created_at: datetime | None = None,
    ) -> VerifiedBackup:
        assert created_at == NOW
        self.order.append("backup-create")
        self.create_count += 1
        return VerifiedBackup(
            Path("/data/loom-staging/backups/fixed/backup-manifest.json"), "2" * 64
        )

    def cleanup_incomplete(self, request_id: str, *, bundle_name: str | None = None) -> bool:
        assert request_id == REQUEST_ID
        assert bundle_name == "20260714T120000Z-req-alpha"
        self.cleanup_count += 1
        return self.cleanup_count == 1


class FailingBackup(FakeBackup):
    def create(
        self,
        request: RolloutRequest,
        *,
        created_at: datetime | None = None,
    ) -> VerifiedBackup:
        assert created_at == NOW
        self.order.append("backup-create")
        self.create_count += 1
        raise BackupError("postgres_dump_failed")


class ObjectLimitBackup(FailingBackup):
    def create(
        self,
        request: RolloutRequest,
        *,
        created_at: datetime | None = None,
    ) -> VerifiedBackup:
        assert created_at == NOW
        self.order.append("backup-create")
        self.create_count += 1
        raise BackupPolicyLimitError(
            "minio_object_limit_exceeded",
            public_reason="backup_object_limit_exceeded",
            message="MinIO mirror exceeded object limit",
        )


class TransportFailingBackup(FailingBackup):
    def create(
        self,
        request: RolloutRequest,
        *,
        created_at: datetime | None = None,
    ) -> VerifiedBackup:
        assert created_at == NOW
        self.order.append("backup-create")
        self.create_count += 1
        raise BackupError(
            "minio_transport_failed",
            public_reason="backup_transport_failed",
        )


class CrashingBackup(FakeBackup):
    def create(
        self,
        request: RolloutRequest,
        *,
        created_at: datetime | None = None,
    ) -> VerifiedBackup:
        assert created_at == NOW
        self.order.append("backup-create")
        self.create_count += 1
        raise KeyboardInterrupt


class CleanupFailingBackup(ObjectLimitBackup):
    def cleanup_incomplete(self, request_id: str, *, bundle_name: str | None = None) -> bool:
        assert request_id == REQUEST_ID
        assert bundle_name == "20260714T120000Z-req-alpha"
        self.cleanup_count += 1
        raise BackupError("backup_cleanup_failed")


class FakeSystemd:
    def __init__(self, state_root: Path) -> None:
        self.state_root = state_root
        self.start_count = 0
        self.terminated: list[str] = []
        self.journal = ("token=known-secret\n",)
        self.visible_units: set[str] = set()
        self.terminate_error: Exception | None = None
        self.on_terminate = None
        self.backup_starts: list[tuple[Path, str]] = []
        self.backup_start_error: Exception | None = None

    def start_backup(self, job_path: Path, unit_name: str) -> None:
        self.backup_starts.append((job_path, unit_name))
        if self.backup_start_error is not None:
            raise self.backup_start_error

    def terminate(self, unit_name: str) -> None:
        if self.on_terminate is not None:
            self.on_terminate()
        if self.terminate_error is not None:
            raise self.terminate_error
        self.terminated.append(unit_name)

    def show(self, unit_name: str):  # type: ignore[no-untyped-def]
        assert unit_name.startswith("loom-staging-rollout-")
        return SimpleNamespace(is_running=True) if unit_name in self.visible_units else None

    def show_backup(self, job_path: Path, unit_name: str):  # type: ignore[no-untyped-def]
        request_id = unit_name.removeprefix("loom-staging-backup-").removesuffix(".service")
        assert job_path == (
            self.state_root / "requests" / request_id / "preflight-backup" / "job.json"
        )
        return SimpleNamespace(is_running=True) if unit_name in self.visible_units else None

    def stream_journal(self, unit_name: str, follow: bool):  # type: ignore[no-untyped-def]
        return iter(self.journal)


class FakeMutationGuard:
    def __init__(self, order: list[str], *, mutation_epoch: int = 7) -> None:
        self.order = order
        self.mutation_epoch = mutation_epoch
        self.acquired: list[str] = []
        self.released: list[str] = []

    def acquire(self, request_id: str):  # type: ignore[no-untyped-def]
        self.order.append("guard-acquire")
        self.acquired.append(request_id)
        return SimpleNamespace(
            request_id=request_id,
            candidate_sha=SHA,
            candidate_tree="b" * 40,
            mutation_epoch=self.mutation_epoch,
            state="ready",
        )

    def assert_ready(self, request_id: str):  # type: ignore[no-untyped-def]
        raise AssertionError(f"broker must not assert worker readiness for {request_id}")

    def release(self, request_id: str):  # type: ignore[no-untyped-def]
        self.order.append("guard-release")
        self.released.append(request_id)
        return SimpleNamespace(request_id=request_id, state="released")


class FakeLifecycle:
    def __init__(self, store: FakeStore, systemd: FakeSystemd, order: list[str]) -> None:
        self.store = store
        self.systemd = systemd
        self.order = order
        self.guard_depth = 0
        self.reconciled: ReconciliationResult | None = None
        self.maintenance = False
        self.retention_claim = False

    @contextmanager
    def launch_guard(self):  # type: ignore[no-untyped-def]
        self.guard_depth += 1
        try:
            yield
        finally:
            self.guard_depth -= 1

    def reconcile_active(self) -> ReconciliationResult:
        if self.reconciled is not None:
            return self.reconciled
        pointer = self.store.active
        return ReconciliationResult(
            outcome="busy" if pointer else "idle",
            pointer=pointer,
            cleared=False,
            safe_status={} if pointer is None else {"request_id": pointer.request_id},
        )

    def assert_admission_open(self) -> None:
        assert self.guard_depth > 0
        if self.maintenance:
            raise LifecycleBusyError(
                "staging rollout admission is disabled for maintenance",
                {"status": "busy", "reason": "maintenance"},
            )
        if self.retention_claim:
            raise LifecycleBusyError(
                "backup retention maintenance is still in progress",
                {"status": "busy", "reason": "backup_retention_busy"},
            )

    def assert_maintenance_active(self) -> None:
        assert self.guard_depth > 0
        if not self.maintenance:
            raise RuntimeError("maintenance marker is unavailable")

    def assert_maintenance_idle(self) -> None:
        self.assert_maintenance_active()
        if self.store.active is not None:
            raise LifecycleBusyError(
                "a staging rollout attempt is already pending or running",
                {"request_id": self.store.active.request_id},
            )

    def launch(self, envelope: DriverEnvelope) -> ActivePointer:
        pointer = ActivePointer(
            request_id=envelope.request_id,
            attempt_number=envelope.attempt_number,
            unit_name=f"loom-staging-rollout-{envelope.request_id}-{envelope.attempt_number}.service",
            status="pending",
        )
        self.order.append("active")
        self.store.active = pointer
        self.order.append("systemd")
        self.systemd.start_count += 1
        return pointer


@dataclass
class FakeBundle:
    dependencies: BrokerDependencies
    config: OperatorConfig
    store: FakeStore
    candidate: FakeCandidate
    backup: FakeBackup
    systemd: FakeSystemd
    lifecycle: FakeLifecycle
    order: list[str]
    stdout: io.StringIO
    stderr: io.StringIO


def fakes(tmp_path: Path, *, backup: FakeBackup | None = None) -> FakeBundle:
    order: list[str] = []
    config = make_config(tmp_path)
    store = FakeStore(order)
    candidate = FakeCandidate(order)
    selected_backup = backup or FakeBackup(order)
    selected_backup.order = order
    systemd = FakeSystemd(config.state_root)
    lifecycle = FakeLifecycle(store, systemd, order)
    stdout, stderr = io.StringIO(), io.StringIO()

    def preflight() -> PreflightReport:
        order.append("preflight")
        return PreflightReport((PreflightCheck("all", True, None),))

    deps = BrokerDependencies(
        config=config,
        authenticate=lambda: CallerIdentity("hongjian", 2002),
        preflight=preflight,
        bind_candidate=candidate.bind,
        backup=selected_backup,
        store=store,  # type: ignore[arg-type]
        lifecycle=lifecycle,  # type: ignore[arg-type]
        systemd=systemd,  # type: ignore[arg-type]
        now=lambda: NOW,
        new_request_id=lambda: REQUEST_ID,
        new_rollout_id=lambda _: ROLLOUT_ID,
        stdout=stdout,
        stderr=stderr,
        known_secrets=lambda: ("known-secret",),
        authorize_preflight=lambda _candidate: SimpleNamespace(
            passed=True,
            attestation=SimpleNamespace(attestation_digest="3" * 64),
            registry_digest="4" * 64,
            coverage_digest="5" * 64,
            to_dict=lambda: {"passed": True},
        ),
    )
    return FakeBundle(
        deps,
        config,
        store,
        candidate,
        selected_backup,
        systemd,
        lifecycle,
        order,
        stdout,
        stderr,
    )


def _enable_guarded_resume(bundle: FakeBundle, *, mutation_epoch: int = 7) -> FakeMutationGuard:
    guard = FakeMutationGuard(bundle.order, mutation_epoch=mutation_epoch)
    bundle.dependencies.mutation_guard = guard
    bundle.dependencies.read_mutation_epoch = lambda: 7
    return guard


@pytest.mark.parametrize(
    "argv",
    [
        ["start", "--ref", "origin/dev"],
        ["start", "--image-tag", "staging-deadbee"],
        ["start", "--config", "/tmp/config"],
        ["start", "--force"],
        ["resume", REQUEST_ID, "--ref", "origin/dev"],
        ["cancel", REQUEST_ID],
        ["start", "--dry"],
        ["cancel", REQUEST_ID, "--rea", "because"],
    ],
)
def test_public_surface_rejects_unapproved_arguments(tmp_path: Path, argv: list[str]) -> None:
    deps = fakes(tmp_path)
    assert broker_main(argv, dependencies=deps.dependencies) == 2
    assert deps.order == []


def test_selected_environment_must_match_authenticated_config_before_action(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    authenticated = False

    def authenticate() -> CallerIdentity:
        nonlocal authenticated
        authenticated = True
        return CallerIdentity("hongjian", 2002)

    deps.dependencies.authenticate = authenticate

    assert (
        broker_main(
            ["--env", "prod", "status"],
            dependencies=deps.dependencies,
        )
        == 1
    )
    assert authenticated is False
    assert deps.order == []
    assert "authorization or validation failed" in deps.stderr.getvalue()


def test_explicit_staging_environment_preserves_legacy_broker_behavior(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)

    assert (
        broker_main(
            ["--env", "staging", "status"],
            dependencies=deps.dependencies,
        )
        == 0
    )
    assert json.loads(deps.stdout.getvalue())["status"] == "idle"


def test_installed_broker_authenticates_before_constructing_rollout_dependencies(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    dependencies_constructed = False

    monkeypatch.setattr(
        broker_module,
        "fixed_operator_config_path",
        lambda *, environment: config.config_path,
    )
    monkeypatch.setattr(
        broker_module.OperatorConfig,
        "load",
        lambda *_args, **_kwargs: config,
    )
    monkeypatch.setattr(
        broker_module,
        "caller_from_sudo",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(PolicyError("wrong environment group")),
    )

    def construct_dependencies(_config: OperatorConfig) -> BrokerDependencies:
        nonlocal dependencies_constructed
        dependencies_constructed = True
        raise AssertionError("unauthorized callers must not construct rollout dependencies")

    monkeypatch.setattr(broker_module, "_default_dependencies", construct_dependencies)

    assert broker_main(["--env", "staging", "status"]) == 1
    assert dependencies_constructed is False
    assert "authorization or validation failed" in capsys.readouterr().err


class _ManifestOwnership:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def inventory(
        self,
        candidate: CandidateBinding,
        *,
        artifact_bundle_sha256: str,
    ) -> dict[str, object]:
        self.calls.append(("inventory", candidate.resolved_sha, artifact_bundle_sha256))
        return {"action": "inventory", "inventory_sha256": "d" * 64}

    def apply(
        self,
        candidate: CandidateBinding,
        *,
        artifact_bundle_sha256: str,
        request_id: str,
        approved_inventory_sha256: str,
    ) -> dict[str, object]:
        self.calls.append(
            (
                "apply",
                candidate.resolved_sha,
                artifact_bundle_sha256,
                request_id,
                approved_inventory_sha256,
            )
        )
        return {"action": "apply", "request_id": request_id}


def test_manifest_ownership_requires_frozen_exact_coordinator_lane(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    service = _ManifestOwnership()
    sealed_config = replace(
        deps.config,
        source_mode="sealed-cumulative",
        source_commit_sha=SHA,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )
    candidate = CandidateBinding(
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="origin/dev",
        resolved_sha=SHA,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-14T12:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("hongjian", 2002),
        bind_candidate=lambda: candidate,
        config=sealed_config,
        manifest_ownership=service,
    )

    inventory_argv = [
        "manifest-ownership",
        "inventory",
        "--artifact-bundle-sha256",
        ARTIFACT_BUNDLE_SHA256,
    ]
    assert broker_main(inventory_argv, dependencies=dependencies) == 1
    assert service.calls == []

    deps.lifecycle.maintenance = True
    assert broker_main(inventory_argv, dependencies=dependencies) == 0
    assert service.calls == [("inventory", SHA, ARTIFACT_BUNDLE_SHA256)]
    assert _last_json(deps.stdout)["inventory_sha256"] == "d" * 64

    assert (
        broker_main(
            [
                "manifest-ownership",
                "apply",
                "--artifact-bundle-sha256",
                ARTIFACT_BUNDLE_SHA256,
                "--request-id",
                "req-manifest-ownership-12345678",
                "--approved-inventory-sha256",
                "d" * 64,
            ],
            dependencies=dependencies,
        )
        == 0
    )
    assert service.calls[-1] == (
        "apply",
        SHA,
        ARTIFACT_BUNDLE_SHA256,
        "req-manifest-ownership-12345678",
        "d" * 64,
    )


def test_manifest_ownership_rejects_non_coordinator_before_candidate_read(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    deps.lifecycle.maintenance = True
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("outsider", 2999),
        manifest_ownership=_ManifestOwnership(),
    )
    assert (
        broker_main(
            [
                "manifest-ownership",
                "inventory",
                "--artifact-bundle-sha256",
                ARTIFACT_BUNDLE_SHA256,
            ],
            dependencies=dependencies,
        )
        == 1
    )
    assert deps.order == []


class _LifecycleCapacity:
    def __init__(self, lifecycle: FakeLifecycle) -> None:
        self.lifecycle = lifecycle
        self.calls: list[tuple[str, object]] = []
        self.plan = SimpleNamespace(
            plan_digest="e" * 64,
            to_dict=lambda: {"plan_digest": "e" * 64, "schema_version": 1},
        )

    def inventory(self, *, artifact_bundle_sha256):  # type: ignore[no-untyped-def]
        self.calls.append(("inventory", self.lifecycle.guard_depth, artifact_bundle_sha256))
        return self.plan

    def prepare_apply(  # type: ignore[no-untyped-def]
        self,
        *,
        artifact_bundle_sha256,
        approved_plan_digest: str,
    ):
        self.calls.append(("prepare", self.lifecycle.guard_depth, artifact_bundle_sha256))
        assert approved_plan_digest == "e" * 64
        return self.plan

    def execute_claimed(self, plan):  # type: ignore[no-untyped-def]
        self.calls.append(("execute", self.lifecycle.guard_depth))
        assert plan is self.plan
        return {"evidence_sha256": "f" * 64, "schema_version": 1}


def test_lifecycle_capacity_uses_digest_approval_and_releases_launch_lock(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    sealed_config = replace(
        deps.config,
        source_mode="sealed-cumulative",
        source_commit_sha=SHA,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )
    service = _LifecycleCapacity(deps.lifecycle)
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("hongjian", 2002),
        config=sealed_config,
        lifecycle_capacity=service,
    )

    inventory_argv = [
        "lifecycle-capacity",
        "inventory",
        "--artifact-bundle-sha256",
        ARTIFACT_BUNDLE_SHA256,
    ]
    assert broker_main(inventory_argv, dependencies=dependencies) == 0
    assert service.calls == [("inventory", 0, ARTIFACT_BUNDLE_SHA256)]

    assert (
        broker_main(
            [
                "lifecycle-capacity",
                "apply",
                "--artifact-bundle-sha256",
                ARTIFACT_BUNDLE_SHA256,
                "--approved-plan-sha256",
                "e" * 64,
            ],
            dependencies=dependencies,
        )
        == 1
    )
    assert service.calls == [("inventory", 0, ARTIFACT_BUNDLE_SHA256)]

    deps.lifecycle.maintenance = True
    assert (
        broker_main(
            [
                "lifecycle-capacity",
                "apply",
                "--artifact-bundle-sha256",
                ARTIFACT_BUNDLE_SHA256,
                "--approved-plan-sha256",
                "e" * 64,
            ],
            dependencies=dependencies,
        )
        == 0
    )
    assert service.calls[-2:] == [
        ("prepare", 1, ARTIFACT_BUNDLE_SHA256),
        ("execute", 0),
    ]


def test_lifecycle_capacity_rejects_non_coordinator(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    service = _LifecycleCapacity(deps.lifecycle)
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("outsider", 2999),
        lifecycle_capacity=service,
    )

    assert (
        broker_main(
            [
                "lifecycle-capacity",
                "inventory",
                "--artifact-bundle-sha256",
                ARTIFACT_BUNDLE_SHA256,
            ],
            dependencies=dependencies,
        )
        == 1
    )
    assert service.calls == []


@pytest.mark.parametrize(
    "argv",
    [
        ["manifest-ownership", "inventory"],
        [
            "manifest-ownership",
            "apply",
            "--request-id",
            "req-manifest-ownership-12345678",
            "--approved-inventory-sha256",
            "d" * 64,
        ],
        ["lifecycle-capacity", "inventory"],
        ["lifecycle-capacity", "apply", "--approved-plan-sha256", "e" * 64],
        [
            "manifest-ownership",
            "inventory",
            "--artifact-bundle-sha256",
            "A" * 64,
        ],
        [
            "lifecycle-capacity",
            "inventory",
            "--artifact-bundle-sha256",
            "9" * 63,
        ],
    ],
)
def test_maintenance_artifact_digest_is_required_and_strict(
    tmp_path: Path,
    argv: list[str],
) -> None:
    deps = fakes(tmp_path)

    assert broker_main(argv, dependencies=deps.dependencies) == 2
    assert deps.order == []


class _BackupRetention:
    def __init__(self, lifecycle: FakeLifecycle) -> None:
        self.lifecycle = lifecycle
        self.calls: list[tuple[str, object]] = []
        self.plan = SimpleNamespace(
            plan_digest="f" * 64,
            to_dict=lambda: {"rotation_generation": 7, "schema_version": 1},
        )

    def inventory(self):  # type: ignore[no-untyped-def]
        self.calls.append(("inventory", self.lifecycle.guard_depth))
        return self.plan

    def load_claim(self, digest: str):  # type: ignore[no-untyped-def]
        self.calls.append(("load", self.lifecycle.guard_depth))
        assert digest == "f" * 64
        return self.plan

    def claim(self, plan):  # type: ignore[no-untyped-def]
        self.calls.append(("claim", self.lifecycle.guard_depth))
        assert plan is self.plan

    def apply(self, plan):  # type: ignore[no-untyped-def]
        self.calls.append(("apply", self.lifecycle.guard_depth))
        assert plan is self.plan
        return {"retired_payload_ids": ["payload-failed01"], "schema_version": 1}


class _BackupRecovery(_BackupRetention):
    def apply(self, plan):  # type: ignore[no-untyped-def]
        self.calls.append(("apply", self.lifecycle.guard_depth))
        assert plan is self.plan
        return {"recovered_payload_id": "payload-candidate01", "schema_version": 1}


def test_default_dependencies_wire_backup_maintenance_for_merged_dev(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resume_runtime_upgrade = SimpleNamespace(resolve=lambda *_args, **_kwargs: None)
    authority = SimpleNamespace(
        assess=lambda _candidate, _epoch: None,
        current_mutation_epoch=lambda: 0,
    )
    monkeypatch.setattr(
        pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(pw_uid=os.geteuid(), pw_gid=os.getegid()),
    )
    monkeypatch.setattr(
        broker_module,
        "sanitized_child_environment",
        lambda _config, *, service_uid: {
            "HOME": "/var/lib/loom-rollout",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin",
            "USER": "loom-rollout",
        },
    )
    monkeypatch.setattr(
        broker_module,
        "build_installed_deep_preflight_composition",
        lambda *_args, **_kwargs: SimpleNamespace(authority=lambda: authority),
    )
    monkeypatch.setattr(
        broker_module,
        "build_installed_resume_runtime_upgrade_authority",
        lambda _config, *, service_uid, run: (
            resume_runtime_upgrade
            if service_uid == os.geteuid() and callable(run)
            else None
        ),
    )

    dependencies = broker_module._default_dependencies(make_config(tmp_path))

    assert dependencies.backup_retention is not None
    assert dependencies.backup_recovery is not None
    assert dependencies.resume_runtime_upgrade is resume_runtime_upgrade


@pytest.mark.parametrize(
    ("command", "service_type", "result_key", "result_value"),
    (
        ("backup-retention", _BackupRetention, "retired_payload_ids", ["payload-failed01"]),
        ("backup-recovery", _BackupRecovery, "recovered_payload_id", "payload-candidate01"),
    ),
)
def test_merged_dev_backup_maintenance_uses_the_configured_service(
    tmp_path: Path,
    command: str,
    service_type: type[_BackupRetention],
    result_key: str,
    result_value: object,
) -> None:
    deps = fakes(tmp_path)
    deps.lifecycle.maintenance = True
    service = service_type(deps.lifecycle)
    dependencies = (
        replace(
            deps.dependencies,
            authenticate=lambda: CallerIdentity("hongjian", 2002),
            backup_retention=service,
        )
        if command == "backup-retention"
        else replace(
            deps.dependencies,
            authenticate=lambda: CallerIdentity("hongjian", 2002),
            backup_recovery=service,
        )
    )

    assert broker_main([command, "inventory"], dependencies=dependencies) == 0
    assert _last_json(deps.stdout) == {
        "plan": {"rotation_generation": 7, "schema_version": 1},
        "plan_sha256": "f" * 64,
    }

    assert (
        broker_main(
            [command, "apply", "--approved-plan-sha256", "f" * 64],
            dependencies=dependencies,
        )
        == 0
    )
    assert _last_json(deps.stdout)[result_key] == result_value


def test_backup_retention_requires_maintenance_and_digest_approval(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    sealed_config = replace(
        deps.config,
        source_mode="sealed-cumulative",
        source_commit_sha=SHA,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )
    service = _BackupRetention(deps.lifecycle)
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("hongjian", 2002),
        config=sealed_config,
        backup_retention=service,
    )

    assert broker_main(["backup-retention", "inventory"], dependencies=dependencies) == 1
    assert service.calls == []

    deps.lifecycle.maintenance = True
    assert broker_main(["backup-retention", "inventory"], dependencies=dependencies) == 0
    assert service.calls == [("inventory", 1)]
    assert _last_json(deps.stdout)["plan_sha256"] == "f" * 64

    assert (
        broker_main(
            [
                "backup-retention",
                "apply",
                "--approved-plan-sha256",
                "f" * 64,
            ],
            dependencies=dependencies,
        )
        == 0
    )
    assert service.calls[-3:] == [("load", 1), ("claim", 1), ("apply", 0)]


def test_backup_retention_rejects_non_coordinator(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    service = _BackupRetention(deps.lifecycle)
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("outsider", 2999),
        backup_retention=service,
    )

    assert broker_main(["backup-retention", "inventory"], dependencies=dependencies) == 1
    assert service.calls == []


class _PreflightArtifactRetention:
    def __init__(self, lifecycle: FakeLifecycle) -> None:
        self.lifecycle = lifecycle
        self.calls: list[tuple[str, object]] = []
        self.plan = SimpleNamespace(
            plan_digest="e" * 64,
            to_dict=lambda: {
                "candidates": [{"bundle_digest": "a" * 64}],
                "environment": "staging",
                "schema_version": 1,
            },
        )

    def inventory(self):  # type: ignore[no-untyped-def]
        self.calls.append(("inventory", self.lifecycle.guard_depth))
        return self.plan

    def load_claim(self, digest: str):  # type: ignore[no-untyped-def]
        self.calls.append(("load", self.lifecycle.guard_depth))
        assert digest == "e" * 64
        return self.plan

    def claim(self, plan):  # type: ignore[no-untyped-def]
        self.calls.append(("claim", self.lifecycle.guard_depth))
        assert plan is self.plan

    def apply(self, plan):  # type: ignore[no-untyped-def]
        self.calls.append(("apply", self.lifecycle.guard_depth))
        assert plan is self.plan
        return {
            "approved_plan_sha256": "e" * 64,
            "retirements": [{"bundle_digest": "a" * 64}],
            "schema_version": 1,
        }


def test_preflight_artifact_retention_holds_launch_guard_through_local_apply(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    sealed_config = replace(
        deps.config,
        source_mode="sealed-cumulative",
        source_commit_sha=SHA,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )
    service = _PreflightArtifactRetention(deps.lifecycle)
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("hongjian", 2002),
        config=sealed_config,
        preflight_artifact_retention=service,
    )
    deps.lifecycle.maintenance = True

    assert (
        broker_main(
            ["preflight-artifact-retention", "inventory"],
            dependencies=dependencies,
        )
        == 0
    )
    inventory_output = deps.stdout.getvalue()
    assert service.calls == [("inventory", 1)]
    assert inventory_output == (
        '{"plan":{"candidates":[{"bundle_digest":"'
        + "a" * 64
        + '"}],"environment":"staging","schema_version":1},"plan_sha256":"'
        + "e" * 64
        + '"}\n'
    )
    assert "known-secret" not in inventory_output
    deps.stdout.seek(0)
    deps.stdout.truncate(0)

    assert (
        broker_main(
            [
                "preflight-artifact-retention",
                "apply",
                "--approved-plan-sha256",
                "e" * 64,
            ],
            dependencies=dependencies,
        )
        == 0
    )
    apply_output = deps.stdout.getvalue()
    assert service.calls[-3:] == [("load", 1), ("claim", 1), ("apply", 1)]
    assert json.loads(apply_output) == {
        "approved_plan_sha256": "e" * 64,
        "retirements": [{"bundle_digest": "a" * 64}],
        "schema_version": 1,
    }
    assert (
        apply_output
        == json.dumps(
            json.loads(apply_output),
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    assert "known-secret" not in apply_output


@pytest.mark.parametrize(
    "rejection",
    ("non-coordinator", "non-sealed", "unconfigured", "no-maintenance", "active"),
)
def test_preflight_artifact_retention_rejects_unsealed_or_busy_authority(
    tmp_path: Path,
    rejection: str,
) -> None:
    deps = fakes(tmp_path)
    sealed_config = replace(
        deps.config,
        source_mode="sealed-cumulative",
        source_commit_sha=SHA,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )
    service = _PreflightArtifactRetention(deps.lifecycle)
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("hongjian", 2002),
        config=sealed_config,
        preflight_artifact_retention=service,
    )
    if rejection == "non-coordinator":
        dependencies = replace(
            dependencies,
            authenticate=lambda: CallerIdentity("outsider", 2999),
        )
    elif rejection == "non-sealed":
        dependencies = replace(dependencies, config=deps.config)
    elif rejection == "unconfigured":
        dependencies = replace(dependencies, preflight_artifact_retention=None)
    elif rejection == "no-maintenance":
        pass
    elif rejection == "active":
        deps.lifecycle.maintenance = True
        deps.store.active = ActivePointer("req-active", 1, "unit-active", "pending")

    assert (
        broker_main(
            ["preflight-artifact-retention", "inventory"],
            dependencies=dependencies,
        )
        == 1
    )
    assert service.calls == []


@pytest.mark.parametrize(
    "argv",
    (
        ["preflight-artifact-retention", "apply"],
        [
            "preflight-artifact-retention",
            "apply",
            "--approved-plan-sha256",
            "A" * 64,
        ],
        [
            "preflight-artifact-retention",
            "apply",
            "--approved-plan-sha256",
            "e" * 63,
        ],
    ),
)
def test_preflight_artifact_retention_rejects_malformed_approval(
    tmp_path: Path,
    argv: list[str],
) -> None:
    deps = fakes(tmp_path)

    assert broker_main(argv, dependencies=deps.dependencies) == 2
    assert deps.order == []


def test_backup_recovery_requires_maintenance_and_digest_approval(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    sealed_config = replace(
        deps.config,
        source_mode="sealed-cumulative",
        source_commit_sha=SHA,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )
    service = _BackupRecovery(deps.lifecycle)
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("hongjian", 2002),
        config=sealed_config,
        backup_recovery=service,
    )

    assert broker_main(["backup-recovery", "inventory"], dependencies=dependencies) == 1
    assert service.calls == []

    deps.lifecycle.maintenance = True
    assert broker_main(["backup-recovery", "inventory"], dependencies=dependencies) == 0
    assert service.calls == [("inventory", 1)]
    assert _last_json(deps.stdout)["plan_sha256"] == "f" * 64

    assert (
        broker_main(
            [
                "backup-recovery",
                "apply",
                "--approved-plan-sha256",
                "f" * 64,
            ],
            dependencies=dependencies,
        )
        == 0
    )
    assert service.calls[-3:] == [("load", 1), ("claim", 1), ("apply", 0)]


def test_backup_recovery_rejects_non_coordinator(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    service = _BackupRecovery(deps.lifecycle)
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("outsider", 2999),
        backup_recovery=service,
    )

    assert broker_main(["backup-recovery", "inventory"], dependencies=dependencies) == 1
    assert service.calls == []


@pytest.mark.parametrize("reason", ["", "   ", "x" * 501])
def test_cancel_reason_is_nonempty_and_bounded(tmp_path: Path, reason: str) -> None:
    deps = fakes(tmp_path)
    assert (
        broker_main(["cancel", REQUEST_ID, "--reason", reason], dependencies=deps.dependencies) == 2
    )


def test_devansh_can_start_sealed_cumulative_dry_run_without_launch(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("devansh", 2003),
        config=replace(
            deps.config,
            source_mode="sealed-cumulative",
            source_commit_sha=SHA,
            source_tree_sha="b" * 40,
            source_base_sha="c" * 40,
        ),
    )

    rc = broker_main(["start", "--dry-run"], dependencies=dependencies)
    assert rc == 0
    assert deps.candidate.fetch_count == 1
    assert deps.backup.create_count == 0
    assert deps.systemd.start_count == 0
    assert deps.store.read_active() is None
    assert deps.store.read_request(REQUEST_ID).status == "preview"
    assert deps.store.read_events(REQUEST_ID)[-1].event == "preview"


def test_devansh_can_preflight_sealed_cumulative_candidate_without_request(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    assessment = _published_assessment(tmp_path)
    expected_candidate = deps.candidate.bind()
    deps.order.clear()

    def assess(candidate: CandidateBinding, epoch: int):  # type: ignore[no-untyped-def]
        assert candidate == expected_candidate
        assert epoch == 7
        return assessment

    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("devansh", 2003),
        config=replace(
            deps.config,
            source_mode="sealed-cumulative",
            source_commit_sha=SHA,
            source_tree_sha="b" * 40,
            source_base_sha="c" * 40,
        ),
        assess_preflight=assess,
        read_mutation_epoch=lambda: 7,
    )

    assert broker_main(["preflight"], dependencies=dependencies) == 0
    assert deps.order == ["preflight", "fetch"]
    assert deps.store.requests == {}
    assert deps.backup.create_count == 0
    assert deps.systemd.start_count == 0
    result = _last_json(deps.stdout)
    assert result == {
        "candidate_sha": SHA,
        "candidate_tree": "b" * 40,
        "coverage_sha256": assessment.coverage_digest,
        "mutation_epoch": 7,
        "preflight_artifact_bundle_sha256": PREFLIGHT_ARTIFACT_BUNDLE_SHA256,
        "preflight_assessment_sha256": assessment.assessment_digest,
        "registry_sha256": assessment.registry_digest,
        "status": "passed",
    }


def test_preflight_reports_all_deep_blockers_without_publishing_request(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    registry = pipeline_registry(failed_check="candidate.identity")
    assessment = PreflightPipeline(
        registry=registry,
        store=PreflightAttestationStore(tmp_path / "preflight-attestations"),
        now=lambda: NOW,
    ).assess(context=pipeline_context(registry))
    dependencies = replace(
        deps.dependencies,
        assess_preflight=lambda _candidate, _epoch: assessment,
        read_mutation_epoch=lambda: 7,
    )

    assert broker_main(["preflight"], dependencies=dependencies) == 1
    assert deps.order == ["preflight", "fetch"]
    assert deps.store.requests == {}
    assert deps.backup.create_count == 0
    assert deps.systemd.start_count == 0
    result = _last_json(deps.stderr)
    assert result["passed"] is False
    assert "candidate.identity" in {blocker["check_id"] for blocker in result["blockers"]}


def test_sealed_cumulative_preflight_rejects_non_coordinator_without_side_effects(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    dependencies = replace(
        deps.dependencies,
        authenticate=lambda: CallerIdentity("outsider", 2999),
        config=replace(
            deps.config,
            source_mode="sealed-cumulative",
            source_commit_sha=SHA,
            source_tree_sha="b" * 40,
            source_base_sha="c" * 40,
        ),
    )

    assert broker_main(["preflight"], dependencies=dependencies) == 1
    assert deps.order == []
    assert deps.store.requests == {}
    assert deps.backup.create_count == 0
    assert deps.systemd.start_count == 0
    assert "coordinator authority" in deps.stderr.getvalue()


def test_start_refuses_missing_deep_preflight_before_request_or_backup(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    deps.dependencies.authorize_preflight = None

    assert broker_main(["start"], dependencies=deps.dependencies) == 1
    assert deps.store.requests == {}
    assert deps.backup.create_count == 0
    assert "deep rollout preflight is not configured" in deps.stderr.getvalue()


def test_staged_start_rejects_malformed_artifact_evidence_before_publication(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    store = RequestStore(tmp_path / "malformed-staged-state")
    assessment = _published_assessment(tmp_path)
    malformed = replace(
        assessment,
        executions=tuple(
            execution
            for execution in assessment.executions
            if execution.check_id != "artifacts.publish"
        ),
    )
    dependencies = replace(
        deps.dependencies,
        store=store,
        bind_candidate=lambda: replace(
            deps.candidate.bind(),
            resolved_tree="b" * 40,
        ),
        assess_preflight=lambda _candidate, _epoch: malformed,
        read_mutation_epoch=lambda: 7,
        mutation_guard=FakeMutationGuard(deps.order),
        new_request_id=lambda: "req-malformed01",
        new_backup_job_id=lambda: "job-malformed01",
        new_payload_id=lambda: "payload-malform01",
    )
    deps.lifecycle.reconciled = ReconciliationResult(
        outcome="idle",
        pointer=None,
        cleared=False,
        safe_status={},
    )

    assert broker_main(["start"], dependencies=dependencies) == 1
    with pytest.raises(RequestStoreError, match="does not exist"):
        store.read_preflight_request("req-malformed01")
    assert deps.backup.create_count == 0
    assert deps.systemd.backup_starts == []


def test_sealed_cumulative_start_rejects_non_coordinator_before_preflight_or_request(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    sealed_config = replace(
        deps.config,
        source_mode="sealed-cumulative",
        source_commit_sha=SHA,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )
    dependencies = replace(deps.dependencies, config=sealed_config)
    dependencies = replace(
        dependencies,
        authenticate=lambda: CallerIdentity("outsider", 2999),
    )

    assert broker_main(["start", "--dry-run"], dependencies=dependencies) == 1
    assert deps.order == []
    assert deps.store.requests == {}
    assert "coordinator authority" in deps.stderr.getvalue()


def test_maintenance_marker_blocks_start_before_preflight(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    deps.lifecycle.maintenance = True

    rc = broker_main(["start", "--dry-run"], dependencies=deps.dependencies)

    assert rc == 1
    assert deps.order == []
    assert '"reason":"maintenance"' in deps.stderr.getvalue()


@pytest.mark.parametrize(
    "command",
    [
        ["start"],
        ["resume", REQUEST_ID],
        ["cleanup-incomplete-backup", REQUEST_ID],
    ],
)
def test_durable_retention_claim_blocks_start_and_resume_before_side_effects(
    tmp_path: Path,
    command: list[str],
) -> None:
    deps = fakes(tmp_path)
    deps.lifecycle.retention_claim = True
    if command[0] == "resume":
        _enable_guarded_resume(deps)

    assert broker_main(command, dependencies=deps.dependencies) == 1

    assert deps.order == []
    assert deps.store.requests == {}
    assert '"reason":"backup_retention_busy"' in deps.stderr.getvalue()


def test_start_reserves_before_launch_and_returns_detached_request(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    rc = broker_main(["start"], dependencies=deps.dependencies)
    assert rc == 0
    assert deps.order == [
        "preflight",
        "fetch",
        "request",
        "backup-create",
        "envelope-finalize",
        "active",
        "systemd",
    ]
    assert REQUEST_ID in deps.stdout.getvalue()
    assert SHA in deps.stdout.getvalue()


def test_devansh_staged_start_publishes_short_lock_detached_checkpoint_job(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    store = RequestStore(tmp_path / "staged-state")
    initial_rotation = store.read_backup_rotation()
    reserved_failed = begin_candidate(
        initial_rotation,
        payload_id="payload-failed00",
        request_id="req-failed000",
        bundle_name="20260714T110000Z-req-failed000",
        created_at=NOW,
    ).state
    store.replace_backup_rotation(
        reserved_failed,
        expected_generation=initial_rotation.generation,
    )
    failed_rotation = fail_candidate(
        reserved_failed,
        payload_id="payload-failed00",
        failure_code="rehearsal_failed",
    ).state
    store.replace_backup_rotation(
        failed_rotation,
        expected_generation=reserved_failed.generation,
    )
    assessment = _published_assessment(tmp_path)

    class StagedLifecycle:
        depth = 0

        @contextmanager
        def launch_guard(self):  # type: ignore[no-untyped-def]
            self.depth += 1
            try:
                yield
            finally:
                self.depth -= 1

        def assert_admission_open(self) -> None:
            assert self.depth == 1
            if store.read_backup_retention_claim() is not None:
                raise LifecycleBusyError(
                    "backup retention maintenance is still in progress",
                    {"status": "busy", "reason": "backup_retention_busy"},
                )

        def reconcile_active(self) -> ReconciliationResult:
            return ReconciliationResult(
                outcome="idle",
                pointer=None,
                cleared=False,
                safe_status={},
            )

    candidate = replace(
        deps.candidate.bind(),
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )

    def assess(found: CandidateBinding, epoch: int):  # type: ignore[no-untyped-def]
        assert found == candidate
        assert epoch == 7
        return assessment

    staged = replace(
        deps.dependencies,
        config=replace(
            deps.config,
            source_mode="sealed-cumulative",
            source_commit_sha=SHA,
            source_tree_sha="b" * 40,
            source_base_sha="c" * 40,
        ),
        authenticate=lambda: CallerIdentity("devansh", 2003),
        store=store,
        lifecycle=StagedLifecycle(),
        bind_candidate=lambda: candidate,
        assess_preflight=assess,
        read_mutation_epoch=lambda: 7,
        mutation_guard=FakeMutationGuard(deps.order),
        new_request_id=lambda: "req-staged0001",
        new_backup_job_id=lambda: "job-staged0001",
        new_payload_id=lambda: "payload-staged01",
    )

    store.claim_backup_retention("e" * 64, ("payload-failed00",))
    assert broker_main(["start"], dependencies=staged) == 1
    assert store.read_backup_rotation().candidate is None
    with pytest.raises(RequestStoreError, match="does not exist"):
        store.read_preflight_request("req-staged0001")
    assert deps.systemd.backup_starts == []
    assert store.clear_backup_retention_claim("e" * 64) is True

    preview_store = RequestStore(tmp_path / "staged-preview-state")
    preview_staged = replace(
        staged,
        store=preview_store,
        new_request_id=lambda: "req-preview0001",
    )
    assert broker_main(["start", "--dry-run"], dependencies=preview_staged) == 0
    assert _last_json(deps.stdout)["preflight_artifact_bundle_sha256"] == (
        PREFLIGHT_ARTIFACT_BUNDLE_SHA256
    )
    assert deps.systemd.backup_starts == []

    rc = broker_main(["start"], dependencies=staged)

    assert rc == 0, deps.stderr.getvalue()
    preliminary = store.read_preflight_request("req-staged0001")
    assert preliminary.candidate == candidate
    assert preliminary.preflight_assessment_sha256 == assessment.assessment_digest
    assert store.read_preflight_assessment("req-staged0001") == assessment
    job = store.read_preflight_backup_job("req-staged0001")
    assert job.payload_id == "payload-staged01"
    rotation = store.read_backup_rotation()
    assert rotation.candidate is not None
    assert rotation.candidate.payload_id == job.payload_id
    assert tuple(record.payload_id for record in rotation.retirements) == ("payload-failed00",)
    assert rotation.payload_count == 2
    assert deps.backup.create_count == 0
    assert deps.systemd.backup_starts == [
        (
            tmp_path / "staged-state/requests/req-staged0001/preflight-backup/job.json",
            "loom-staging-backup-req-staged0001.service",
        )
    ]
    pending = _last_json(deps.stdout)
    assert pending["status"] == "backup_pending"
    assert pending["preflight_artifact_bundle_sha256"] == (PREFLIGHT_ARTIFACT_BUNDLE_SHA256)

    class StagedCleanupBackup:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None]] = []

        def cleanup_incomplete(
            self,
            request_id: str,
            *,
            bundle_name: str | None = None,
        ) -> bool:
            self.calls.append((request_id, bundle_name))
            return len(self.calls) == 1

    cleanup = StagedCleanupBackup()
    cleanup_dependencies = replace(staged, backup=cleanup)
    backup_unit = "loom-staging-backup-req-staged0001.service"
    deps.systemd.visible_units.add(backup_unit)
    assert (
        broker_main(
            ["cleanup-incomplete-backup", "req-staged0001"],
            dependencies=cleanup_dependencies,
        )
        == 1
    )
    assert cleanup.calls == []
    assert store.read_preflight_backup_job_state("req-staged0001").phase.value == ("backup_pending")

    deps.systemd.visible_units.remove(backup_unit)
    assert (
        broker_main(
            ["cleanup-incomplete-backup", "req-staged0001"],
            dependencies=cleanup_dependencies,
        )
        == 0
    )
    cleaned_job = store.read_preflight_backup_job_state("req-staged0001")
    assert cleaned_job.phase.value == "backup_failed"
    assert cleaned_job.failure_code == "backup_cleanup_requested"
    assert cleanup.calls == [("req-staged0001", "20260714T120000Z-req-staged0001")]
    cleaned_rotation = store.read_backup_rotation()
    assert cleaned_rotation.candidate is None
    assert tuple(record.payload_id for record in cleaned_rotation.retirements) == (
        "payload-failed00",
        "payload-staged01",
    )
    assert [event.event for event in store.read_events("req-staged0001")[-3:]] == [
        "backup_failed",
        "backup_cleanup_started",
        "backup_cleanup_done",
    ]

    compacted_rotation = replace(
        cleaned_rotation,
        generation=cleaned_rotation.generation + 1,
        retirements=tuple(
            record
            for record in cleaned_rotation.retirements
            if record.payload_id != "payload-staged01"
        ),
    )
    store.replace_backup_rotation(
        compacted_rotation,
        expected_generation=cleaned_rotation.generation,
    )
    deps.stdout.seek(0)
    deps.stdout.truncate(0)

    assert (
        broker_main(
            ["cleanup-incomplete-backup", "req-staged0001"],
            dependencies=cleanup_dependencies,
        )
        == 0
    )
    assert cleanup.calls == [
        ("req-staged0001", "20260714T120000Z-req-staged0001"),
        ("req-staged0001", "20260714T120000Z-req-staged0001"),
    ]
    assert '"cleanup":"already_absent"' in deps.stdout.getvalue()
    assert store.read_events("req-staged0001")[-1].event == "backup_cleanup_done"

    failed_store = RequestStore(tmp_path / "failed-staged-state")
    deps.systemd.backup_start_error = RuntimeError("secret-bearing systemd detail")
    failed_staged = replace(
        staged,
        store=failed_store,
        new_request_id=lambda: "req-staged0002",
        new_backup_job_id=lambda: "job-staged0002",
        new_payload_id=lambda: "payload-staged02",
    )

    assert broker_main(["start"], dependencies=failed_staged) == 1
    failed_event = failed_store.read_events("req-staged0002")[-1]
    assert failed_event.event == "backup_failed"
    assert failed_event.reason == "backup_precondition_failed"
    failed_job = failed_store.read_preflight_backup_job_state("req-staged0002")
    assert failed_job.phase.value == "backup_failed"
    assert failed_job.failure_code == "backup_launch_failed"
    failed_rotation = failed_store.read_backup_rotation()
    assert failed_rotation.candidate is None
    assert tuple(record.payload_id for record in failed_rotation.retirements) == (
        "payload-staged02",
    )
    assert failed_rotation.retirements[0].reason == "failed"
    assert failed_store.read_active() is None


def _guarded_staged_start(
    tmp_path: Path,
    *,
    epochs: tuple[int, ...] = (7, 7),
    store: RequestStore | None = None,
) -> tuple[FakeBundle, BrokerDependencies, RequestStore, FakeMutationGuard]:
    bundle = fakes(tmp_path)
    staged_store = store or RequestStore(tmp_path / "guarded-staged-state")
    assessment = _published_assessment(tmp_path)
    candidate = replace(bundle.candidate.bind(), resolved_tree="b" * 40)
    bundle.order.clear()
    observed_epochs = iter(epochs)
    guard = FakeMutationGuard(bundle.order)

    class StagedLifecycle:
        depth = 0

        @contextmanager
        def launch_guard(self):  # type: ignore[no-untyped-def]
            self.depth += 1
            bundle.order.append("launch-guard-acquire")
            try:
                yield
            finally:
                bundle.order.append("launch-guard-release")
                self.depth -= 1

        def assert_admission_open(self) -> None:
            assert self.depth == 1

        def reconcile_active(self) -> ReconciliationResult:
            return ReconciliationResult(
                outcome="idle",
                pointer=None,
                cleared=False,
                safe_status={},
            )

    def assess(found: CandidateBinding, epoch: int) -> PreflightAssessment:
        assert found == candidate
        assert epoch == 7
        bundle.order.append("tier-0-2")
        return assessment

    def read_epoch() -> int:
        value = next(observed_epochs)
        bundle.order.append(f"epoch-{value}")
        return value

    dependencies = replace(
        bundle.dependencies,
        store=staged_store,
        lifecycle=StagedLifecycle(),
        bind_candidate=lambda: candidate,
        assess_preflight=assess,
        read_mutation_epoch=read_epoch,
        mutation_guard=guard,
        new_request_id=lambda: "req-guarded001",
        new_backup_job_id=lambda: "job-guarded001",
        new_payload_id=lambda: "payload-guarded1",
    )
    return bundle, dependencies, staged_store, guard


def test_staged_start_acquires_guard_after_tier_0_2_and_hands_off_to_backup(
    tmp_path: Path,
) -> None:
    bundle, dependencies, store, guard = _guarded_staged_start(tmp_path)

    assert broker_main(["start"], dependencies=dependencies) == 0

    assert guard.acquired == ["req-guarded001"]
    assert guard.released == []
    assert bundle.order.index("tier-0-2") < bundle.order.index("guard-acquire")
    second_epoch = [index for index, value in enumerate(bundle.order) if value == "epoch-7"][1]
    assert bundle.order.index("guard-acquire") < second_epoch
    assert store.read_preflight_request("req-guarded001").mutation_epoch == 7
    assert bundle.systemd.backup_starts[-1][1] == ("loom-staging-backup-req-guarded001.service")


def test_staged_dry_run_never_acquires_mutation_guard(tmp_path: Path) -> None:
    bundle, dependencies, store, guard = _guarded_staged_start(tmp_path)

    assert broker_main(["start", "--dry-run"], dependencies=dependencies) == 0

    assert guard.acquired == []
    assert guard.released == []
    assert bundle.systemd.backup_starts == []
    assert store.read_preflight_request("req-guarded001").status == "preview"


def test_staged_start_releases_guard_on_post_readiness_epoch_drift_without_publication(
    tmp_path: Path,
) -> None:
    _bundle, dependencies, store, guard = _guarded_staged_start(tmp_path, epochs=(7, 8))

    assert broker_main(["start"], dependencies=dependencies) == 1

    assert guard.acquired == ["req-guarded001"]
    assert guard.released == ["req-guarded001"]
    with pytest.raises(RequestStoreError, match="does not exist"):
        store.read_preflight_request("req-guarded001")


@pytest.mark.parametrize("failure", ["persistence", "launch"])
def test_staged_start_releases_guard_on_every_pre_handoff_failure(
    tmp_path: Path,
    failure: str,
) -> None:
    class FailingRequestStore(RequestStore):
        def create_preflight_request(self, request):  # type: ignore[no-untyped-def]
            raise RequestStoreError("injected request persistence failure")

    staged_store = (
        FailingRequestStore(tmp_path / "guarded-staged-state")
        if failure == "persistence"
        else RequestStore(tmp_path / "guarded-staged-state")
    )
    bundle, dependencies, _store, guard = _guarded_staged_start(
        tmp_path,
        store=staged_store,
    )
    if failure == "launch":
        bundle.systemd.backup_start_error = RuntimeError("injected backup launch failure")

    assert broker_main(["start"], dependencies=dependencies) == 1

    assert guard.acquired == ["req-guarded001"]
    assert guard.released == ["req-guarded001"]


def test_backup_failure_never_publishes_envelope_or_starts_unit(tmp_path: Path) -> None:
    deps = fakes(tmp_path, backup=FailingBackup([]))
    assert broker_main(["start"], dependencies=deps.dependencies) == 1
    assert deps.systemd.start_count == 0
    assert deps.store.read_active() is None
    assert deps.store.envelopes == {}
    assert deps.store.read_events(REQUEST_ID)[-1].event == "backup_failed"
    # FailingBackup fails the postgres stage; the durable reason names it end to
    # end through the broker instead of collapsing to a generic backup_failed.
    assert deps.store.read_events(REQUEST_ID)[-1].reason == "backup_postgres_failed"
    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0
    status = _last_json(deps.stdout)
    assert status["stage"] == "backup_failed"
    assert status["reason"] == "backup_postgres_failed"

    assert (
        broker_main(
            ["cleanup-incomplete-backup", REQUEST_ID],
            dependencies=deps.dependencies,
        )
        == 0
    )
    cleaned = deps.store.read_events(REQUEST_ID)[-1]
    assert cleaned.event == "backup_cleanup_done"
    assert cleaned.reason == "backup_postgres_failed"


def test_object_limit_failure_has_stable_public_reason_and_supported_cleanup(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path, backup=ObjectLimitBackup([]))

    assert broker_main(["start"], dependencies=deps.dependencies) == 1
    failed = deps.store.read_events(REQUEST_ID)[-1]
    assert failed.event == "backup_failed"
    assert failed.reason == "backup_object_limit_exceeded"
    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0
    status = _last_json(deps.stdout)
    assert status["stage"] == "backup_failed"
    assert status["reason"] == "backup_object_limit_exceeded"

    deps.stdout.seek(0)
    deps.stdout.truncate(0)
    assert (
        broker_main(
            ["cleanup-incomplete-backup", REQUEST_ID],
            dependencies=deps.dependencies,
        )
        == 0
    )
    cleaned = deps.store.read_events(REQUEST_ID)[-1]
    assert cleaned.event == "backup_cleanup_done"
    assert cleaned.status == "failed"
    assert cleaned.reason == "backup_object_limit_exceeded"
    assert '"cleanup":"removed"' in deps.stdout.getvalue()
    assert '"status":"failed"' in deps.stdout.getvalue()

    deps.stdout.seek(0)
    deps.stdout.truncate(0)
    assert (
        broker_main(
            ["cleanup-incomplete-backup", REQUEST_ID],
            dependencies=deps.dependencies,
        )
        == 0
    )
    assert '"cleanup":"already_absent"' in deps.stdout.getvalue()


def test_transport_failure_has_stable_public_reason_and_no_launch(tmp_path: Path) -> None:
    deps = fakes(tmp_path, backup=TransportFailingBackup([]))

    assert broker_main(["start"], dependencies=deps.dependencies) == 1
    failed = deps.store.read_events(REQUEST_ID)[-1]
    assert failed.event == "backup_failed"
    assert failed.reason == "backup_transport_failed"
    assert deps.systemd.start_count == 0
    assert deps.store.read_active() is None
    assert deps.store.envelopes == {}

    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0
    status = _last_json(deps.stdout)
    assert status["stage"] == "backup_failed"
    assert status["reason"] == "backup_transport_failed"


def test_cleanup_refuses_envelope_crash_window_without_deleting_backup(tmp_path: Path) -> None:
    deps = fakes(tmp_path, backup=ObjectLimitBackup([]))
    assert broker_main(["start"], dependencies=deps.dependencies) == 1
    deps.store.envelopes[(REQUEST_ID, 1)] = object()  # type: ignore[assignment]

    assert (
        broker_main(
            ["cleanup-incomplete-backup", REQUEST_ID],
            dependencies=deps.dependencies,
        )
        == 1
    )

    assert deps.backup.cleanup_count == 0
    assert deps.store.read_events(REQUEST_ID)[-1].event == "backup_failed"


def test_cleanup_recovers_backup_started_power_loss_without_envelope(tmp_path: Path) -> None:
    deps = fakes(tmp_path, backup=CrashingBackup([]))
    with pytest.raises(KeyboardInterrupt):
        broker_main(["start"], dependencies=deps.dependencies)
    assert deps.store.read_events(REQUEST_ID)[-1].event == "backup_started"

    assert (
        broker_main(
            ["cleanup-incomplete-backup", REQUEST_ID],
            dependencies=deps.dependencies,
        )
        == 0
    )

    events = deps.store.read_events(REQUEST_ID)
    assert [event.event for event in events[-3:]] == [
        "backup_failed",
        "backup_cleanup_started",
        "backup_cleanup_done",
    ]
    assert all(event.reason == "backup_failed" for event in events[-3:])


def test_cleanup_failure_is_audited_and_preserves_public_backup_reason(tmp_path: Path) -> None:
    deps = fakes(tmp_path, backup=CleanupFailingBackup([]))
    assert broker_main(["start"], dependencies=deps.dependencies) == 1

    assert (
        broker_main(
            ["cleanup-incomplete-backup", REQUEST_ID],
            dependencies=deps.dependencies,
        )
        == 1
    )

    events = deps.store.read_events(REQUEST_ID)
    assert [event.event for event in events[-2:]] == [
        "backup_cleanup_started",
        "backup_cleanup_failed",
    ]
    assert all(event.reason == "backup_object_limit_exceeded" for event in events[-2:])
    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0
    status = _last_json(deps.stdout)
    assert status["stage"] == "backup_cleanup_failed"
    assert status["reason"] == "backup_object_limit_exceeded"


def test_start_refuses_when_another_request_is_active(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    deps.store.active = ActivePointer(
        request_id="req-other",
        attempt_number=1,
        unit_name="loom-staging-rollout-req-other-1.service",
        status="running",
    )
    assert broker_main(["start"], dependencies=deps.dependencies) == 1
    assert deps.order == []


def test_unknown_request_preview_resume_and_done_resume_are_rejected(tmp_path: Path) -> None:
    unknown = fakes(tmp_path / "unknown")
    assert broker_main(["resume", REQUEST_ID], dependencies=unknown.dependencies) == 1

    preview = fakes(tmp_path / "preview")
    assert broker_main(["start", "--dry-run"], dependencies=preview.dependencies) == 0
    assert broker_main(["resume", REQUEST_ID], dependencies=preview.dependencies) == 1

    done = fakes(tmp_path / "done")
    assert broker_main(["start"], dependencies=done.dependencies) == 0
    done.store.active = None
    done.store.append_event(
        RequestEvent(
            request_id=REQUEST_ID,
            event="attempt_done",
            occurred_at="2026-07-14T12:10:00Z",
            operator="hongjian",
            operator_uid=2002,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="done",
        )
    )
    _enable_guarded_resume(done)
    assert broker_main(["resume", REQUEST_ID], dependencies=done.dependencies) == 1


def test_resume_reuses_original_sha_backup_and_rollout_id(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    guard = _enable_guarded_resume(deps)
    deps.store.active = None
    deps.store.append_event(
        RequestEvent(
            request_id=REQUEST_ID,
            event="attempt_failed",
            occurred_at="2026-07-14T12:10:00Z",
            operator="hongjian",
            operator_uid=2002,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="failed",
            reason="driver_failed",
        )
    )
    first = deps.store.read_attempt_envelope(REQUEST_ID, 1)
    deps.order.clear()

    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 0
    envelope = deps.store.read_attempt_envelope(REQUEST_ID, 2)
    assert envelope.resolved_sha == first.resolved_sha
    assert envelope.backup_manifest_path == first.backup_manifest_path
    assert envelope.backup_manifest_sha256 == first.backup_manifest_sha256
    assert envelope.rollout_id == first.rollout_id
    assert envelope.resume is True
    assert guard.acquired == [REQUEST_ID]
    assert guard.released == []
    assert deps.order.index("guard-acquire") < deps.order.index("systemd")
    assert deps.candidate.fetch_count == 1
    assert deps.backup.create_count == 1


@pytest.mark.parametrize("drift", ["guard", "database"])
def test_resume_requires_original_epoch_and_releases_guard_on_drift(
    tmp_path: Path,
    drift: str,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    guard = _enable_guarded_resume(deps)
    deps.store.active = None
    deps.store.append_event(
        RequestEvent(
            request_id=REQUEST_ID,
            event="attempt_failed",
            occurred_at="2026-07-14T12:10:00Z",
            operator="hongjian",
            operator_uid=2002,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="failed",
            reason="driver_failed",
        )
    )
    if drift == "guard":
        guard.mutation_epoch = 8
    else:
        deps.dependencies.read_mutation_epoch = lambda: 8
    starts_before = deps.systemd.start_count

    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 1

    assert guard.acquired == [REQUEST_ID]
    assert guard.released == [REQUEST_ID]
    assert deps.store.next_attempt_number(REQUEST_ID) == 2
    assert deps.store.active is None
    assert deps.systemd.start_count == starts_before


def test_resume_accepts_exact_advanced_epoch_recovery_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    deps.store.active = None
    deps.store.append_event(
        RequestEvent(
            request_id=REQUEST_ID,
            event="attempt_failed",
            occurred_at="2026-07-14T12:10:00Z",
            operator="hongjian",
            operator_uid=2002,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="failed",
            reason="driver_failed",
        )
    )
    guard = _enable_guarded_resume(deps, mutation_epoch=8)
    deps.dependencies.read_mutation_epoch = lambda: 8
    recovery_calls: list[dict[str, object]] = []

    def find_recovery(state_root: Path, **bindings: object) -> int:
        recovery_calls.append({"state_root": state_root, **bindings})
        return 1

    monkeypatch.setattr(
        broker_module,
        "find_advanced_epoch_attempt",
        find_recovery,
        raising=False,
    )

    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 0

    assert recovery_calls == [
        {
            "state_root": deps.config.state_root,
            "request_id": REQUEST_ID,
            "through_attempt": 1,
            "candidate_sha": SHA,
            "attestation_digest": "3" * 64,
            "starting_mutation_epoch": 7,
            "service_uid": os.geteuid(),
        }
    ]
    assert guard.acquired == [REQUEST_ID]
    assert guard.released == []
    assert deps.store.read_active() is not None
    assert deps.store.read_active().attempt_number == 2
    assert deps.store.read_attempt_envelope(REQUEST_ID, 2).resume is True


def test_resume_accepts_exact_forward_runner_upgrade_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deps = fakes(tmp_path)
    original_config = deps.dependencies.config
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    deps.store.active = None
    deps.store.append_event(
        RequestEvent(
            request_id=REQUEST_ID,
            event="attempt_failed",
            occurred_at="2026-07-14T12:10:00Z",
            operator="hongjian",
            operator_uid=2002,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="failed",
            reason="driver_failed",
        )
    )
    current_repo = tmp_path / ("c" * 40) / "repo"
    deps.dependencies.config = replace(
        original_config,
        runner_repo=current_repo,
        cluster_config_path=current_repo / "deploy/environments/staging.cluster.toml",
        config_sha256="2" * 64,
    )
    resolved: list[dict[str, object]] = []

    def resolve_runtime(config: OperatorConfig, **bindings: object) -> OperatorConfig:
        resolved.append({"config": config, **bindings})
        return original_config

    deps.dependencies.resume_runtime_upgrade = SimpleNamespace(resolve=resolve_runtime)
    guard = _enable_guarded_resume(deps, mutation_epoch=8)
    deps.dependencies.read_mutation_epoch = lambda: 8
    monkeypatch.setattr(
        broker_module,
        "find_advanced_epoch_attempt",
        lambda *_args, **_kwargs: 1,
    )

    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 0

    first = deps.store.read_attempt_envelope(REQUEST_ID, 1)
    assert resolved == [
        {
            "config": deps.dependencies.config,
            "candidate_sha": SHA,
            "candidate_tree": "b" * 40,
            "runner_config_sha256": original_config.config_sha256,
            "cluster_config_path": first.cluster_config_path,
        }
    ]
    assert guard.acquired == [REQUEST_ID]
    assert deps.store.read_attempt_envelope(REQUEST_ID, 2).resume is True


def test_resume_rejects_advanced_epoch_without_exact_recovery_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    deps.store.active = None
    deps.store.append_event(
        RequestEvent(
            request_id=REQUEST_ID,
            event="attempt_failed",
            occurred_at="2026-07-14T12:10:00Z",
            operator="hongjian",
            operator_uid=2002,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="failed",
            reason="driver_failed",
        )
    )
    guard = _enable_guarded_resume(deps, mutation_epoch=8)
    deps.dependencies.read_mutation_epoch = lambda: 8
    monkeypatch.setattr(
        broker_module,
        "find_advanced_epoch_attempt",
        lambda *_args, **_kwargs: None,
        raising=False,
    )
    starts_before = deps.systemd.start_count

    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 1

    assert guard.acquired == [REQUEST_ID]
    assert guard.released == [REQUEST_ID]
    assert deps.store.next_attempt_number(REQUEST_ID) == 2
    assert deps.store.active is None
    assert deps.systemd.start_count == starts_before


def test_resume_rejects_invalid_advanced_epoch_recovery_before_guard_or_launch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    deps.store.active = None
    deps.store.append_event(
        RequestEvent(
            request_id=REQUEST_ID,
            event="attempt_failed",
            occurred_at="2026-07-14T12:10:00Z",
            operator="hongjian",
            operator_uid=2002,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="failed",
            reason="driver_failed",
        )
    )
    guard = _enable_guarded_resume(deps)

    def reject_recovery(*_args: object, **_kwargs: object) -> None:
        raise ValueError("protected apply recovery plan binding drifted")

    monkeypatch.setattr(
        broker_module,
        "find_advanced_epoch_attempt",
        reject_recovery,
        raising=False,
    )
    starts_before = deps.systemd.start_count

    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 1

    assert guard.acquired == []
    assert guard.released == []
    assert deps.store.next_attempt_number(REQUEST_ID) == 2
    assert deps.store.active is None
    assert deps.systemd.start_count == starts_before


def test_cancel_records_actor_reason_and_terminates_known_unit(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0

    assert (
        broker_main(
            ["cancel", REQUEST_ID, "--reason", "staging validation is abandoned"],
            dependencies=deps.dependencies,
        )
        == 0
    )

    event = deps.store.read_events(REQUEST_ID)[-1]
    assert event.event == "cancel_requested"
    assert event.operator == "hongjian"
    assert event.reason == "staging validation is abandoned"
    assert deps.systemd.terminated == [f"loom-staging-rollout-{REQUEST_ID}-1.service"]


def test_logs_redacts_exact_known_secret(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0

    assert broker_main(["logs", REQUEST_ID], dependencies=deps.dependencies) == 0
    assert "token=known-secret" not in deps.stdout.getvalue()
    assert "[REDACTED" in deps.stdout.getvalue()


def _private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)


def _private_file(path: Path, payload: str = "{}\n") -> None:
    path.write_text(payload)
    path.chmod(0o600)


def test_status_reports_incomplete_protected_component_without_reading_payload(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    root = deps.config.state_root / "requests" / REQUEST_ID / "attempts" / "1" / "protected-apply"
    _private_directory(root)
    _private_file(root / "execution.lock", "")
    epoch = root / "00-mutation-epoch-claim"
    _private_directory(epoch)
    _private_file(epoch / "intent.json")
    _private_file(epoch / "terminal.json")
    gb10 = root / "03-gb10-candidate"
    _private_directory(gb10)
    _private_file(gb10 / "intent.json", '{"private":"must-not-be-read"}\n')
    _private_file(
        gb10 / "failure.json",
        (
            '{"component_id":"gb10-candidate",'
            '"failed_hosts":["trt-gb10-10","trt-gb10-2"],'
            '"failure_code":"gb10-convergence-failed","schema_version":1}\n'
        ),
    )

    deps.stdout.seek(0)
    deps.stdout.truncate()
    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0

    payload = _last_json(deps.stdout)
    assert payload["protected_component"] == "gb10-candidate"
    assert payload["protected_component_status"] == "protected_component_incomplete"
    assert payload["protected_failed_hosts"] == ["trt-gb10-10", "trt-gb10-2"]
    assert "must-not-be-read" not in deps.stdout.getvalue()


def test_status_reports_certified_protected_component_failure_diagnostic(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    root = deps.config.state_root / "requests" / REQUEST_ID / "attempts" / "1" / "protected-apply"
    _private_directory(root)
    _private_file(root / "execution.lock", "")
    manifests = root / "02-staging-manifests"
    _private_directory(manifests)
    _private_file(manifests / "intent.json")
    diagnostic = (
        "unclassified staging-manifests failure: RuntimeError "
        "at protected_apply_executor.py:245 in _run"
    )
    _private_file(
        manifests / "failure-diagnostic.json",
        json.dumps(
            {
                "schema_version": 1,
                "component_id": "staging-manifests",
                "ordinal": 2,
                "failure_code": "apply-failed",
                "diagnostic": diagnostic,
            }
        ),
    )

    deps.stdout.seek(0)
    deps.stdout.truncate()
    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0

    payload = _last_json(deps.stdout)
    assert payload["protected_component"] == "staging-manifests"
    assert payload["protected_component_status"] == "protected_component_incomplete"
    assert payload["protected_failure_code"] == "apply-failed"
    assert payload["protected_failure_diagnostic"] == diagnostic


@pytest.mark.parametrize(
    "diagnostic_record",
    [
        {
            "schema_version": 2,
            "component_id": "staging-manifests",
            "ordinal": 2,
            "failure_code": "apply-failed",
            "diagnostic": "unclassified staging-manifests failure: RuntimeError",
        },
        {
            "schema_version": 1,
            "component_id": "staging-manifests",
            "ordinal": 2,
            "failure_code": "unbounded-runtime-detail",
            "diagnostic": "unclassified staging-manifests failure: RuntimeError",
        },
        {
            "schema_version": 1,
            "component_id": "staging-manifests",
            "ordinal": 2,
            "failure_code": "apply-failed",
            "diagnostic": "secret-bearing-detail\nsecond line",
        },
        {
            "schema_version": 1,
            "component_id": "staging-manifests",
            "ordinal": 2,
            "failure_code": "apply-failed",
            "diagnostic": "x" * 513,
        },
        {
            "schema_version": 1,
            "component_id": "staging-manifests",
            "ordinal": 2,
            "failure_code": "apply-failed",
            "diagnostic": "unclassified staging-manifests failure: RuntimeError",
            "private_detail": "secret-bearing-detail",
        },
        {
            "schema_version": 1,
            "component_id": "database-migration",
            "ordinal": 2,
            "failure_code": "apply-failed",
            "diagnostic": "unclassified database-migration failure: RuntimeError",
        },
        {
            "schema_version": 1,
            "component_id": "staging-manifests",
            "ordinal": 3,
            "failure_code": "apply-failed",
            "diagnostic": "unclassified staging-manifests failure: RuntimeError",
        },
    ],
)
def test_status_hides_uncertified_protected_component_failure_diagnostic(
    tmp_path: Path,
    diagnostic_record: dict[str, object],
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    root = deps.config.state_root / "requests" / REQUEST_ID / "attempts" / "1" / "protected-apply"
    _private_directory(root)
    _private_file(root / "execution.lock", "")
    manifests = root / "02-staging-manifests"
    _private_directory(manifests)
    _private_file(manifests / "intent.json")
    _private_file(
        manifests / "failure-diagnostic.json",
        json.dumps(diagnostic_record),
    )

    deps.stdout.seek(0)
    deps.stdout.truncate()
    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0

    payload = _last_json(deps.stdout)
    assert payload["protected_component"] == "staging-manifests"
    assert payload["protected_component_status"] == "protected_component_incomplete"
    assert "protected_failure_code" not in payload
    assert "protected_failure_diagnostic" not in payload
    assert "secret-bearing-detail" not in deps.stdout.getvalue()


def test_status_ignores_unsafe_protected_failure_diagnostic_metadata(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    root = deps.config.state_root / "requests" / REQUEST_ID / "attempts" / "1" / "protected-apply"
    _private_directory(root)
    _private_file(root / "execution.lock", "")
    manifests = root / "02-staging-manifests"
    _private_directory(manifests)
    _private_file(manifests / "intent.json")
    outside = tmp_path / "secret-bearing-detail.json"
    _private_file(outside, '{"private":"secret-bearing-detail"}')
    (manifests / "failure-diagnostic.json").symlink_to(outside)

    deps.stdout.seek(0)
    deps.stdout.truncate()
    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0

    payload = _last_json(deps.stdout)
    assert payload["protected_component"] == "staging-manifests"
    assert payload["protected_component_status"] == "protected_component_incomplete"
    assert "protected_failure_code" not in payload
    assert "protected_failure_diagnostic" not in payload
    assert "secret-bearing-detail" not in deps.stdout.getvalue()


def test_status_fails_closed_on_unsafe_protected_progress_metadata(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    root = deps.config.state_root / "requests" / REQUEST_ID / "attempts" / "1" / "protected-apply"
    _private_directory(root)
    _private_file(root / "execution.lock", "")
    unsafe = root / "03-gb10-candidate"
    unsafe.symlink_to(tmp_path)

    deps.stdout.seek(0)
    deps.stdout.truncate()
    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0

    payload = _last_json(deps.stdout)
    assert "protected_component" not in payload
    assert "protected_component_status" not in payload


def test_status_reports_only_normalized_final_gate_failure_metadata(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    attempt = deps.config.state_root / "requests" / REQUEST_ID / "attempts" / "1"
    _private_directory(attempt)
    evidence = MappingProxyType({"ready": False, "evidence-digest": "3" * 64})
    execution = CheckExecution(
        check_id="final.convergence",
        failure_code="final.convergence.failed",
        tier=4,
        stage=StageCapability.FINAL_ONLY,
        operation=CheckOperation.VERIFY,
        outcome=CheckOutcome.FAIL,
        input_fingerprint="1" * 64,
        implementation_digest="2" * 64,
        evidence=evidence,
        evidence_hash=hashlib.sha256(
            json.dumps(dict(evidence), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        started_at=NOW,
        finished_at=NOW,
        expires_at=NOW + timedelta(hours=1),
        remediation="private remediation must not be exposed",
    )
    FinalGateExecutionStore(
        deps.config.state_root,
        request_id=REQUEST_ID,
        attempt_number=1,
    ).publish(execution)

    deps.stdout.seek(0)
    deps.stdout.truncate()
    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0

    payload = _last_json(deps.stdout)
    assert payload["final_gate_check"] == "final.convergence"
    assert payload["final_gate_outcome"] == "fail"
    assert payload["final_gate_failure_code"] == "final.convergence.failed"
    assert "private remediation" not in deps.stdout.getvalue()


def test_status_fails_closed_on_unsafe_final_gate_progress(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    root = deps.config.state_root / "requests" / REQUEST_ID / "attempts" / "1" / "final-gates"
    _private_directory(root)
    (root / "final.convergence.json").symlink_to(tmp_path)

    deps.stdout.seek(0)
    deps.stdout.truncate()
    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0

    payload = _last_json(deps.stdout)
    assert "final_gate_check" not in payload
    assert "final_gate_outcome" not in payload
    assert "final_gate_failure_code" not in payload


def _last_json(stream: io.StringIO) -> dict[str, object]:
    return json.loads(stream.getvalue().splitlines()[-1])


def test_explicit_status_reconciles_and_preserves_safe_lifecycle_details(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    pointer = deps.store.active
    assert pointer is not None
    deps.lifecycle.reconciled = ReconciliationResult(
        outcome="busy",
        pointer=pointer,
        cleared=False,
        safe_status={
            "request_id": REQUEST_ID,
            "attempt_number": 1,
            "unit_name": pointer.unit_name,
            "status": "running",
            "current_step": "S07_release_gate",
            "reason": "unit_running",
        },
    )

    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0
    payload = _last_json(deps.stdout)
    assert payload["status"] == "running"
    assert payload["current_step"] == "S07_release_gate"
    assert payload["reason"] == "unit_running"
    assert payload["unit_name"] == pointer.unit_name


def test_implicit_status_returns_terminal_attempt_cleared_during_same_reconcile(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    pointer = deps.store.active
    assert pointer is not None
    deps.store.active = None
    deps.lifecycle.reconciled = ReconciliationResult(
        outcome="done",
        pointer=pointer,
        cleared=True,
        safe_status={
            "request_id": REQUEST_ID,
            "status": "done",
            "reason": "terminal_rollout_state",
            "current_step": "S99_summary",
        },
    )

    assert broker_main(["status"], dependencies=deps.dependencies) == 0
    payload = _last_json(deps.stdout)
    assert payload["request_id"] == REQUEST_ID
    assert payload["status"] == "done"
    assert payload["reason"] == "terminal_rollout_state"
    assert payload["current_step"] == "S99_summary"


def test_status_preserves_busy_safe_status_before_active_pointer_is_visible(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    deps.lifecycle.reconciled = ReconciliationResult(
        outcome="busy",
        pointer=None,
        cleared=False,
        safe_status={"status": "busy", "reason": "launch_in_progress"},
    )

    assert broker_main(["status"], dependencies=deps.dependencies) == 0
    assert _last_json(deps.stdout) == {
        "status": "busy",
        "reason": "launch_in_progress",
    }


def test_explicit_status_does_not_overlay_unattributed_global_busy_state(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start", "--dry-run"], dependencies=deps.dependencies) == 0
    deps.lifecycle.reconciled = ReconciliationResult(
        outcome="busy",
        pointer=None,
        cleared=False,
        safe_status={"status": "busy", "reason": "launch_in_progress"},
    )

    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0
    payload = _last_json(deps.stdout)
    assert payload["request_id"] == REQUEST_ID
    assert payload["status"] == "preview"
    assert "reason" not in payload


def test_start_continues_after_reconcile_clears_terminal_pointer(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    old_pointer = ActivePointer(
        request_id="req-terminal",
        attempt_number=1,
        unit_name="loom-staging-rollout-req-terminal-1.service",
        status="running",
    )
    deps.lifecycle.reconciled = ReconciliationResult(
        outcome="done",
        pointer=old_pointer,
        cleared=True,
        safe_status={"request_id": "req-terminal", "status": "done"},
    )

    assert broker_main(["start", "--dry-run"], dependencies=deps.dependencies) == 0
    assert deps.candidate.fetch_count == 1


def test_terminal_request_status_preserves_latest_event_step_and_reason(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    deps.store.active = None
    deps.store.append_event(
        RequestEvent(
            request_id=REQUEST_ID,
            event="attempt_failed",
            occurred_at="2026-07-14T12:10:00Z",
            operator="hongjian",
            operator_uid=2002,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="failed",
            reason="driver_failed",
            current_step="S07_release_gate",
        )
    )

    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0
    payload = _last_json(deps.stdout)
    assert payload["reason"] == "driver_failed"
    assert payload["current_step"] == "S07_release_gate"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("config_sha256", "f" * 64),
        ("cluster_name", "other-cluster"),
        ("namespace", "other-namespace"),
        ("environment", "other"),
        ("cp_url", "http://127.0.0.1:9999"),
        ("cluster_config_path", Path("/tmp/other.cluster.toml")),
        ("rollout_root", Path("/tmp/other-rollout")),
        ("admin_token_source", "file:/tmp/other-admin"),
        ("worker_token_source", "file:/tmp/other-worker"),
        ("service_token_source", "file:/tmp/other-service"),
        ("expect_admin_token_fingerprint", "sha256:ffffffffffff len=64"),
        ("smoke_on_behalf_username", "other-user"),
        ("smoke_on_behalf_team_id", "22222222-2222-4222-8222-222222222222"),
        ("scope", "other-scope"),
        ("gb10_prep_concurrency", 7),
    ],
)
def test_resume_rejects_every_config_bound_drift_before_publication_or_launch(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    deps.store.active = None
    deps.store.append_event(
        RequestEvent(
            request_id=REQUEST_ID,
            event="attempt_failed",
            occurred_at="2026-07-14T12:10:00Z",
            operator="hongjian",
            operator_uid=2002,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="failed",
            reason="driver_failed",
        )
    )
    envelopes_before = dict(deps.store.envelopes)
    starts_before = deps.systemd.start_count
    deps.dependencies.config = replace(deps.config, **{field: value})  # type: ignore[arg-type]
    _enable_guarded_resume(deps)

    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 1
    assert deps.store.envelopes == envelopes_before
    assert deps.store.active is None
    assert deps.systemd.start_count == starts_before


def test_resume_recovers_finalized_prelaunch_orphan_without_creating_new_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    first = deps.store.read_attempt_envelope(REQUEST_ID, 1)
    deps.store.active = None
    deps.systemd.start_count = 0
    _enable_guarded_resume(deps)

    def reject_finalized_recovery(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("prelaunch orphan must not use finalized-attempt recovery")

    monkeypatch.setattr(
        broker_module,
        "find_advanced_epoch_attempt",
        reject_finalized_recovery,
    )

    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 0
    assert deps.store.read_attempt_envelope(REQUEST_ID, 1) == first
    assert deps.store.next_attempt_number(REQUEST_ID) == 2
    assert deps.store.active is not None
    assert deps.store.active.attempt_number == 1
    assert deps.systemd.start_count == 1


def test_resume_recovers_orphan_when_crash_preceded_publication_event(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    deps.store.active = None
    deps.systemd.start_count = 0
    deps.store.events[REQUEST_ID] = [
        event for event in deps.store.events[REQUEST_ID] if event.event != "envelope_published"
    ]
    _enable_guarded_resume(deps)

    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 0
    assert deps.store.active is not None
    assert deps.store.active.attempt_number == 1
    assert deps.systemd.start_count == 1


def test_resume_does_not_recover_orphan_when_expected_unit_exists(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    deps.store.active = None
    deps.systemd.start_count = 0
    unit = f"loom-staging-rollout-{REQUEST_ID}-1.service"
    deps.systemd.visible_units.add(unit)
    _enable_guarded_resume(deps)

    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 1
    assert deps.store.active is None
    assert deps.systemd.start_count == 0


def test_resume_rejects_orphan_whose_backup_drifted_from_first_attempt(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    deps.store.active = None
    deps.store.append_event(
        RequestEvent(
            request_id=REQUEST_ID,
            event="attempt_failed",
            occurred_at="2026-07-14T12:10:00Z",
            operator="hongjian",
            operator_uid=2002,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="failed",
            reason="driver_failed",
        )
    )
    _enable_guarded_resume(deps)
    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 0
    deps.store.active = None
    second = deps.store.read_attempt_envelope(REQUEST_ID, 2)
    deps.store.envelopes[(REQUEST_ID, 2)] = replace(
        second,
        backup_manifest_path="/data/loom-staging/backups/other/backup-manifest.json",
        backup_manifest_sha256="9" * 64,
    )
    deps.systemd.start_count = 0

    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 1
    assert deps.store.active is None
    assert deps.systemd.start_count == 0


def test_cancel_redacts_reason_and_terminates_under_launch_guard(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    deps.systemd.on_terminate = lambda: (
        None
        if deps.lifecycle.guard_depth > 0
        else (_ for _ in ()).throw(AssertionError("cancel was not serialized"))
    )

    assert (
        broker_main(
            ["cancel", REQUEST_ID, "--reason", "abandoned known-secret validation"],
            dependencies=deps.dependencies,
        )
        == 0
    )
    event = deps.store.read_events(REQUEST_ID)[-1]
    assert event.event == "cancel_requested"
    assert "abandoned known-secret validation" != event.reason
    assert "[REDACTED" in (event.reason or "")


def test_failed_termination_compensates_persisted_cancel_intent(tmp_path: Path) -> None:
    from loom_cli.rollout.operator.systemd import SystemdOperationError

    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    deps.systemd.terminate_error = SystemdOperationError("unit already completed")

    assert (
        broker_main(
            ["cancel", REQUEST_ID, "--reason", "abandoned"],
            dependencies=deps.dependencies,
        )
        == 1
    )
    events = deps.store.read_events(REQUEST_ID)
    assert [event.event for event in events[-2:]] == ["cancel_requested", "cancel_failed"]
    assert events[-1].reason == "unit_termination_failed"


def test_terminal_event_wins_if_termination_reports_failure_after_worker_exit(
    tmp_path: Path,
) -> None:
    from loom_cli.rollout.operator.systemd import SystemdOperationError

    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0
    pointer = deps.store.read_active()
    assert pointer is not None

    def finish_worker() -> None:
        deps.store.append_event(
            RequestEvent(
                request_id=REQUEST_ID,
                event="cancelled",
                occurred_at="2026-07-14T12:01:00Z",
                operator="hongjian",
                operator_uid=2002,
                attempt_number=pointer.attempt_number,
                unit_name=pointer.unit_name,
                status="cancelled",
                reason="cancel_requested",
            )
        )
        deps.store.active = None

    deps.systemd.on_terminate = finish_worker
    deps.systemd.terminate_error = SystemdOperationError("unit already completed")

    assert (
        broker_main(
            ["cancel", REQUEST_ID, "--reason", "abandoned"],
            dependencies=deps.dependencies,
        )
        == 1
    )
    assert deps.store.read_events(REQUEST_ID)[-1].event == "cancel_failed"

    deps.stdout.seek(0)
    deps.stdout.truncate(0)
    assert broker_main(["status", REQUEST_ID], dependencies=deps.dependencies) == 0
    assert _last_json(deps.stdout)["status"] == "cancelled"
    _enable_guarded_resume(deps)
    assert broker_main(["resume", REQUEST_ID], dependencies=deps.dependencies) == 0
    assert deps.store.read_active() is not None
    assert deps.store.read_active().attempt_number == 2


def test_cancel_redaction_failure_occurs_before_termination(tmp_path: Path) -> None:
    deps = fakes(tmp_path)
    assert broker_main(["start"], dependencies=deps.dependencies) == 0

    def fail_secret_read() -> tuple[str, ...]:
        raise RuntimeError("secret source unavailable")

    deps.dependencies.known_secrets = fail_secret_read

    assert (
        broker_main(
            ["cancel", REQUEST_ID, "--reason", "abandoned"],
            dependencies=deps.dependencies,
        )
        == 1
    )
    assert deps.systemd.terminated == []
    assert all(event.event != "cancel_requested" for event in deps.store.read_events(REQUEST_ID))


def test_operator_known_secrets_include_catalog_environment_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    monkeypatch.setattr(
        broker_module,
        "known_secrets_from_sources",
        lambda sources: ("token-value",),
    )
    monkeypatch.setattr(
        broker_module,
        "catalog_secret_values",
        lambda config, service_uid: ("catalog-value", "minio-secret"),
        raising=False,
    )

    assert hasattr(broker_module, "_operator_known_secrets")
    values = broker_module._operator_known_secrets(  # type: ignore[attr-defined]
        config, service_uid=1234
    )
    assert values == ("token-value", "catalog-value", "minio-secret")


def test_default_broker_run_and_stream_use_exact_sanitized_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli.rollout.operator.systemd import SystemdUserManager

    config = make_config(tmp_path)
    expected = sanitized_child_environment(config, service_uid=1234)
    run_environments: list[dict[str, str] | None] = []
    run_timeouts: list[object] = []
    popen_environments: list[dict[str, str] | None] = []

    def fake_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        run_environments.append(kwargs.get("env"))  # type: ignore[arg-type]
        run_timeouts.append(kwargs.get("timeout"))
        return subprocess.CompletedProcess(argv, 0, "", "")

    class FakePopen:
        def __init__(self, argv: list[str], **kwargs: object) -> None:
            del argv
            popen_environments.append(kwargs.get("env"))  # type: ignore[arg-type]
            self.stdout = io.StringIO("")

        def poll(self) -> int:
            return 0

        def terminate(self) -> None:
            pass

        def wait(self, timeout: int) -> int:
            del timeout
            return 0

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(subprocess, "Popen", FakePopen)

    guard_launch = SystemdUserManager(
        config,
        service_uid=1234,
        run=lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
    ).start_mutation_guard_argv(REQUEST_ID, "1" * 32)
    service_stop_timeout = int(
        next(item for item in guard_launch if item.startswith("TimeoutStopSec="))
        .removeprefix("TimeoutStopSec=")
        .removesuffix("s")
    )

    broker_module._run(["git", "status"], environment=expected)
    broker_module._run(["systemd-run", "--user"], environment=expected)
    broker_module._run(["systemctl", "--user", "show"], environment=expected)
    stream = broker_module._stream(["journalctl"], environment=expected)
    stream.close()
    assert run_environments == [expected, expected, expected]
    assert run_timeouts[0] == 30
    assert all(
        type(timeout) is int and timeout > service_stop_timeout + 3 * 30
        for timeout in run_timeouts[1:]
    )
    assert popen_environments == [expected]


def test_main_scopes_restrictive_umask_and_restores_caller(
    tmp_path: Path,
) -> None:
    deps = fakes(tmp_path)
    created = tmp_path / "broker-created"
    observed_umasks: list[int] = []
    authenticate = deps.dependencies.authenticate
    original_umask = os.umask(0o002)

    def capture_umask() -> CallerIdentity:
        current = os.umask(0o077)
        os.umask(current)
        observed_umasks.append(current)
        created.write_text("broker\n", encoding="utf-8")
        return authenticate()

    deps.dependencies.authenticate = capture_umask
    try:
        assert broker_main(["status"], dependencies=deps.dependencies) == 0
        restored_umask = os.umask(0o077)
        os.umask(restored_umask)
        assert restored_umask == 0o002
    finally:
        os.umask(original_umask)

    assert observed_umasks == [0o077]
    assert created.stat().st_mode & 0o777 == 0o600


def test_group_resolution_includes_primary_and_supplementary_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pwd,
        "getpwnam",
        lambda username: SimpleNamespace(pw_gid=200),
    )
    monkeypatch.setattr(
        grp,
        "getgrgid",
        lambda gid: SimpleNamespace(gr_name="loom-staging-operators"),
    )
    monkeypatch.setattr(
        grp,
        "getgrall",
        lambda: [SimpleNamespace(gr_name="docker", gr_mem=["hongjian"])],
    )

    assert broker_module._groups("hongjian") == {
        "loom-staging-operators",
        "docker",
    }
