from __future__ import annotations

import json
import os
import shlex
import subprocess
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
from types import SimpleNamespace

import pytest
from scripts.ops import staging_external_slurm_acceptance_authority as external_authority

from loom_cli.external_slurm_acceptance import load_authority_config
from loom_cli.rollout.final_attestation_admission import validate_final_attestation
from loom_cli.rollout.gb10_readiness import GB10ProbeTarget
from loom_cli.rollout.operator.backup_lease import BackupLease
from loom_cli.rollout.operator.deep_preflight_authority import (
    AdmissionPreparationLifecycle,
)
from loom_cli.rollout.operator.final_gate_action_source import FinalGateActionSource
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlanStore
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyJournal,
)
from loom_cli.rollout.operator.protected_gb10_component import ProtectedGB10CandidateComponent
from loom_cli.rollout.operator.protected_gb10_transport import (
    FixedGB10SSHTransport,
    GB10TransportTarget,
    _retirement_identity,
)
from loom_cli.rollout.preflight_artifact_store import (
    PreflightArtifactPublication,
    PreflightArtifactStore,
)
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_authority import CandidatePreflightPlan
from loom_cli.rollout.preflight_bindings import derive_attestation_bindings
from loom_cli.rollout.preflight_contract import (
    CheckContext,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    PreflightAttestation,
    PreflightDag,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
)
from loom_cli.rollout.preflight_pipeline import PreflightRehearsal
from loom_cli.rollout.preflight_registered_checks import (
    build_external_gb10_stage_boundary_checks,
    gb10_target_inventory_digest,
)
from loom_cli.rollout.preflight_registry import PreflightRegistry
from tests.loom_cli.rollout.operator.test_final_gate_plan import (
    _envelope,
    _lease,
    _predecessor_evidence,
    _systemd_evidence,
)
from tests.loom_cli.rollout.test_preflight_artifact_store import (
    _images,
    _manifests,
    _migration,
    _production_defaults,
)
from tests.loom_cli.rollout.test_preflight_bindings import _executions as binding_executions
from tests.ops.test_staging_external_slurm_acceptance_authority import (
    CONFIG,
    SHA,
    TREE,
    _infrastructure_payload,
)

NOW = datetime(2026, 7, 29, 12, tzinfo=UTC)
HOSTS = tuple(f"trt-gb10-{number}" for number in range(1, 16))
TARGETS = tuple(GB10ProbeTarget(host, "loom-gb10-node-agent.service") for host in HOSTS)
FIXED_RECEIPT_ROOT = Path("/var/lib/loom-developer-sandbox-node-authority/staging-infrastructure")


class _InfrastructureProducer:
    """Exercise the installed producer load/validation/summary path in-process."""

    def __init__(
        self,
        root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self.root = root
        self.monkeypatch = monkeypatch
        self.config = load_authority_config(CONFIG)
        self.summary: dict[str, object] | None = None
        self.generation = 0
        self.root.mkdir(mode=0o700)

    def publish(
        self,
        generation: int,
        *,
        drift: str | None = None,
    ) -> dict[str, object]:
        payload = _infrastructure_payload(self.config, now=NOW)
        payload["generation"] = generation
        path = self.root / f"{SHA}.json"
        path.write_bytes(external_authority.canonical_json_bytes(payload))
        path.chmod(0o600)

        self.monkeypatch.setattr(
            external_authority,
            "_INFRASTRUCTURE_RECEIPT_ROOT",
            self.root,
        )
        loaded, payload_sha256 = external_authority._load_infrastructure_receipt(
            self.config,
            candidate_sha=SHA,
            candidate_tree=TREE,
            now=NOW,
            enforce_root_security=False,
        )
        # The installed consumer binds the canonical persistent receipt path.
        # Switch only while deriving the producer's verification summary; the
        # next publication switches the loader back to this test-owned root.
        self.monkeypatch.setattr(
            external_authority,
            "_INFRASTRUCTURE_RECEIPT_ROOT",
            FIXED_RECEIPT_ROOT,
        )
        summary = external_authority._infrastructure_summary(
            loaded,
            payload_sha256=payload_sha256,
        )
        if drift == "mount":
            summary["mount_digest"] = "f" * 64
        elif drift == "source":
            summary["source_digest"] = "f" * 64
        elif drift == "boot":
            boot_ids = dict(summary["boot_ids"])  # type: ignore[arg-type]
            boot_ids["trt-gb10-7"] = "ffffffff-ffff-4fff-8fff-ffffffffffff"
            summary["boot_ids"] = boot_ids
        elif drift is not None:
            raise AssertionError(f"unknown drift fixture: {drift}")
        self.summary = summary
        self.generation = generation
        return summary

    def run(self, argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        assert argv[:4] == (
            "/usr/bin/sudo",
            "-n",
            "/usr/local/libexec/loom-staging-external-slurm-authority",
            "verify-infrastructure",
        )
        assert argv[-4:] == ("--candidate-sha", SHA, "--candidate-tree", TREE)
        if self.summary is None:
            return subprocess.CompletedProcess(argv, 1, "", "")
        return subprocess.CompletedProcess(argv, 0, json.dumps(self.summary), "")


def _schema(evidence: dict[str, object]) -> tuple[EvidenceField, ...]:
    def kind(value: object) -> str:
        if type(value) is bool:
            return "boolean"
        if type(value) is int:
            return "integer"
        if isinstance(value, dict):
            return "string-map"
        return "string"

    return tuple(EvidenceField(name, kind(value)) for name, value in evidence.items())


def _passing_check(
    check_id: str,
    evidence: dict[str, object],
    *,
    tier: int = 0,
    dependencies: tuple[str, ...] = (),
) -> RegisteredCheck:
    return RegisteredCheck(
        spec=CheckSpec(
            check_id=check_id,
            failure_code=f"{check_id}.failed",
            tier=tier,
            stage=(StageCapability.BASELINE_LIVE_READONLY if tier == 2 else StageCapability.STATIC),
            dependencies=dependencies,
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha",),
            evidence_schema=_schema(evidence),
            timeout_seconds=5,
            freshness_ttl_seconds=600,
            remediation=f"restore {check_id}",
            secret_redaction_policy=(
                SecretRedactionPolicy.METADATA_FINGERPRINTS_ONLY
                if check_id == "credentials.metadata"
                else SecretRedactionPolicy.NO_SECRET_INPUTS
            ),
        ),
        implementation_version="contract-v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence=evidence,  # type: ignore[arg-type]
            )
        },
    )


def _context() -> CheckContext:
    return CheckContext(
        {
            "candidate.base.sha": "c" * 40,
            "candidate.sha": SHA,
            "candidate.source-mode": "sealed-cumulative",
            "candidate.tree": TREE,
            "runner.config.sha256": "5" * 64,
            "staging.mutation-epoch": 7,
            "backup.lease.sha256": "6" * 64,
            "backup.manifest.sha256": "7" * 64,
            "backup.component-set.sha256": "8" * 64,
            "db.snapshot-identity": "lsn-1",
            "schema.revision": "0066",
            "object.inventory-root": "9" * 64,
            "environment": "staging",
            "namespace": "loom-staging",
            "route": "https://yylx.world/dev",
            "gb10.external-profile.sha256": "6" * 64,
            "gb10.inventory-digest": gb10_target_inventory_digest(TARGETS),
            "gb10.mount-binding.sha256": "7" * 64,
        }
    )


def _checks(
    producer: _InfrastructureProducer,
    monkeypatch: pytest.MonkeyPatch,
    *,
    publication: PreflightArtifactPublication | None = None,
) -> tuple[RegisteredCheck, ...]:
    monkeypatch.setattr(
        "loom_cli.rollout.preflight_registered_checks.inspect_systemd_unit_sources",
        lambda _path: SimpleNamespace(ready=True, unit_set_digest="9" * 64),
    )
    mount, candidate_source, host = build_external_gb10_stage_boundary_checks(
        producer.run,
        targets=TARGETS,
        expected_profile_digest="6" * 64,
        expected_mount_binding_digest="7" * 64,
        candidate_root=Path("/srv/loom/staging-shared/candidates") / SHA,
        expected_candidate_sha=SHA,
        expected_candidate_tree=TREE,
        now=lambda: NOW,
    )
    canned = {
        execution.check_id: dict(execution.evidence)
        for execution in binding_executions()
        if execution.check_id
        not in {"gb10.shared-mount", "gb10.candidate-source", "gb10.host-readiness"}
    }
    if publication is not None:
        images = _images()
        canned["images.contract"] = {"image-digests": dict(images.image_digests)}
        canned["migration.plan"] = {"plan-digest": "4" * 64}
        canned["systemd.render"] = _systemd_evidence()
        canned["external-supervisor.predecessor"] = _predecessor_evidence()
        canned["browser.runtime"] = {
            "image-id": images.image_digests["loom-staging-admin-browser-smoke"],
            "report-schema-digest": "8" * 64,
        }
    tier0 = (
        _passing_check("candidate.identity", canned.pop("candidate.identity")),
        _passing_check("runner.install", canned.pop("runner.install")),
        _passing_check("credentials.metadata", canned.pop("credentials.metadata")),
        _passing_check(
            "external-supervisor.predecessor",
            canned.pop("external-supervisor.predecessor"),
        ),
        _passing_check("gb10.ssh-topology", {"ready": True}),
        _passing_check("systemd.user-manager", {"ready": True}),
        mount,
        candidate_source,
        host,
    )
    binding_support: tuple[RegisteredCheck, ...] = tuple(
        _passing_check(check_id, evidence) for check_id, evidence in canned.items()
    )
    if publication is not None:
        binding_support += (
            _passing_check(
                "artifacts.publish",
                {
                    "bundle-digest": publication.bundle_digest,
                    "image-artifact-digest": publication.image_artifact_sha256,
                    "manifest-artifact-digest": publication.manifest_artifact_sha256,
                    "rendered-manifest-digest": publication.rendered_manifest_sha256,
                    "migration-manifest-digest": publication.migration_manifest_sha256,
                    "migration-artifact-digest": (publication.migration_manifest_artifact_sha256),
                    "production-defaults-digest": publication.production_defaults_sha256,
                },
                tier=1,
            ),
        )
    baseline = tuple(
        _passing_check(
            check_id,
            {
                "ready": True,
                "observed-epoch": 7,
                "readonly-principal": "system:serviceaccount:loom-staging:readonly",
                "resource-digest": f"{index:064x}",
                "blockers": {},
            },
            tier=2,
        )
        for index, check_id in enumerate(
            (
                "staging.health",
                "staging.auth",
                "staging.catalog-task",
                "staging.storage-db",
                "staging.network",
                "staging.release-baseline",
            ),
            1,
        )
    )
    return tier0 + binding_support + baseline


def _candidate():
    from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding

    return CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha=SHA,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-29T11:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree=TREE,
        approved_base_sha="c" * 40,
    )


def _plan(checks: tuple[RegisteredCheck, ...]) -> CandidatePreflightPlan:
    check_ids = [check.spec.check_id for check in checks]
    assert len(check_ids) == len(set(check_ids))
    assert check_ids.count("staging.health") == 1
    return CandidatePreflightPlan(
        candidate=_candidate(),
        registry=PreflightRegistry(
            checks=checks,
            through_tier=3,
            coverage_digest="d" * 64,
            registry_digest="e" * 64,
        ),
        context=_context(),
    )


def _publish_artifacts(
    state_root: Path,
) -> tuple[PreflightArtifactStore, PreflightArtifactPublication]:
    store = PreflightArtifactStore(state_root)
    images = _images()
    publication = store.publish(
        candidate_sha=SHA,
        candidate_tree=TREE,
        mutation_epoch=7,
        images=images,
        manifests=_manifests(images),
        migration=_migration(
            images,
            candidate_tree=TREE,
            migration_plan_sha256="4" * 64,
        ),
        production_defaults=_production_defaults(candidate_tree=TREE),
        migration_plan_sha256="4" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="8" * 64,
    )
    return store, publication


@dataclass(frozen=True)
class _FinalGateRequestStore:
    rehearsal: PreflightRehearsal
    lease: BackupLease

    def read_preflight_rehearsal(self, request_id: str) -> PreflightRehearsal:
        assert request_id == "req-alpha"
        return self.rehearsal

    def read_backup_lease(self, digest: str) -> BackupLease:
        assert digest == self.lease.evidence_digest
        return self.lease


class _FixedRetirementSSH:
    """Replace only the outer SSH boundary while exercising the fixed transport."""

    def __init__(self, boot_ids: dict[str, str]) -> None:
        self.boot_ids = boot_ids
        self.retired_hosts: set[str] = set()
        self.observations = 0
        self.applies = 0
        self._lock = Lock()

    def __call__(self, argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        host = argv[-2]
        command = argv[-1]
        assert host in self.boot_ids
        assert command.startswith("python3 -c ")
        source = shlex.split(command)[2]
        with self._lock:
            if "print(json.dumps" in source:
                self.observations += 1
                exact = host in self.retired_hosts
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    json.dumps(
                        {
                            "baseline_ready": True,
                            "boot_id": self.boot_ids[host],
                            "candidate_source_exact": True,
                            "checkout_exact": True,
                            "environment_exact": True,
                            "legacy_absent": exact,
                            "service_timer_exact": exact,
                            "service_timer_transient": False,
                            "units_exact": True,
                        }
                    ),
                    "",
                )
            assert "operations = ('legacy-retire', 'service-timer')" in source
            assert f"expected_boot_id = {self.boot_ids[host]!r}" in source
            self.applies += 1
            self.retired_hosts.add(host)
            return subprocess.CompletedProcess(argv, 0, "", "")


def _final_gate_source(
    *,
    tmp_path: Path,
    artifact_store: PreflightArtifactStore,
    rehearsal: PreflightRehearsal,
    lease: BackupLease,
) -> FinalGateActionSource:
    def unexpected_helper_run(*_args: object) -> subprocess.CompletedProcess[str]:
        raise AssertionError("final-gate helper execution is outside this contract test")

    executable = tmp_path / "final-gate-helper"
    executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    executable.chmod(0o755)
    return FinalGateActionSource(
        request_store=_FinalGateRequestStore(rehearsal, lease),
        artifact_store=artifact_store,
        state_root=tmp_path / "final-gate-state",
        service_uid=os.geteuid(),
        run=unexpected_helper_run,  # type: ignore[arg-type]
        read_mutation_epoch=lambda: 8,
        now=lambda: NOW + timedelta(seconds=5),
        executable=executable,
        executable_owner_uid=os.geteuid(),
    )


def _execute_external_dag(plan: CandidatePreflightPlan):
    return PreflightDag(plan.registry.checks).run(
        plan.context,
        through_tier=2,
        now=lambda: NOW - timedelta(minutes=1),
    )


def _external_evidence(executions):
    by_id = {execution.check_id: execution for execution in executions}
    return {
        check_id: by_id[check_id]
        for check_id in (
            "gb10.shared-mount",
            "gb10.candidate-source",
            "gb10.host-readiness",
        )
    }


def _stable_external_identity(executions) -> tuple[object, object, object]:
    trio = _external_evidence(executions)
    return (
        trio["gb10.shared-mount"].evidence["mount-digest"],
        trio["gb10.candidate-source"].evidence["source-digest"],
        dict(trio["gb10.host-readiness"].evidence["boot-ids"]),  # type: ignore[arg-type]
    )


def _receipt_identity(executions) -> tuple[int, str]:
    trio = _external_evidence(executions)
    generations = {int(execution.evidence["receipt-generation"]) for execution in trio.values()}
    digests = {
        str(
            execution.evidence[
                (
                    "infrastructure-receipt-digest"
                    if execution.check_id == "gb10.candidate-source"
                    else "receipt-digest"
                )
            ]
        )
        for execution in trio.values()
    }
    assert len(generations) == 1
    assert len(digests) == 1
    return generations.pop(), digests.pop()


def test_external_profile_contract_survives_receipt_refresh_and_reuses_retirement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_store, publication = _publish_artifacts(tmp_path / "preflight-state")
    lease = _lease()
    producer = _InfrastructureProducer(tmp_path / "producer", monkeypatch)
    broker_preparation = AdmissionPreparationLifecycle(
        prepare=lambda _candidate: producer.publish(1),
        now=lambda: NOW,
    )
    candidate = _candidate()
    broker_preparation.prepare_admission(candidate)

    plan = _plan(_checks(producer, monkeypatch, publication=publication))
    executions = _execute_external_dag(plan)
    trio = _external_evidence(executions)
    assert all(execution.passed for execution in trio.values())
    assert trio["gb10.host-readiness"].evidence["boot-ids"] == producer.summary["boot_ids"]  # type: ignore[index]

    bindings = derive_attestation_bindings(plan.context, executions, backup_lease=lease)
    assert set(bindings.gb10_boot_ids) == set(HOSTS)
    assert "trt-gb10-7" in bindings.gb10_boot_ids
    assert bindings.gb10_mount_digest == producer.summary["mount_digest"]  # type: ignore[index]
    assert bindings.gb10_unit_digest == trio["gb10.candidate-source"].evidence["source-digest"]

    attestation = PreflightAttestation.issue(
        bindings=bindings,
        executions=executions,
        issued_at=NOW,
        registry_digest=plan.registry.registry_digest,
        coverage_digest=plan.registry.coverage_digest,
    )
    store = PreflightAttestationStore(tmp_path / "worker-state")
    store.publish(attestation)

    worker_preparation = AdmissionPreparationLifecycle(
        prepare=lambda _candidate: producer.publish(2),
        now=lambda: NOW + timedelta(seconds=1),
    )
    worker_preparation.prepare_admission(candidate)
    worker_preparation.require_fresh(candidate)
    admission = validate_final_attestation(
        attestation=store.read(attestation.attestation_digest),
        candidate=candidate,
        plan=plan,
        current_mutation_epoch=7,
        now=NOW + timedelta(seconds=1),
    )
    refreshed = {execution.check_id: execution for execution in admission.tier0_executions}
    assert all(refreshed[check_id].evidence["receipt-generation"] == 2 for check_id in trio)
    assert refreshed["gb10.shared-mount"].evidence["mount-digest"] == bindings.gb10_mount_digest
    assert refreshed["gb10.candidate-source"].evidence["source-digest"] == bindings.gb10_unit_digest
    assert refreshed["gb10.host-readiness"].evidence["boot-ids"] == dict(bindings.gb10_boot_ids)

    rehearsal = PreflightRehearsal.from_executions(
        registry_digest=attestation.registry_digest,
        coverage_digest=attestation.coverage_digest,
        executions=executions,
    )
    first_source = _final_gate_source(
        tmp_path=tmp_path,
        artifact_store=artifact_store,
        rehearsal=rehearsal,
        lease=lease,
    )
    envelope = replace(
        _envelope(attestation),
        source_mode=candidate.source_mode,
        resolved_tree=candidate.resolved_tree,
        approved_base_sha=candidate.approved_base_sha,
        fetched_at=candidate.fetched_at,
        runner_config_sha256=bindings.runner_config_hash,
        preflight_registry_sha256=attestation.registry_digest,
        preflight_coverage_sha256=attestation.coverage_digest,
    )
    first_plan_root = tmp_path / "final-gate-state/requests/req-alpha/attempts/1"
    first_plan_root.mkdir(parents=True, mode=0o700)
    first_source(envelope, attestation, 7, admission)
    first = FinalGatePlanStore(
        tmp_path / "final-gate-state",
        request_id=envelope.request_id,
        attempt_number=envelope.attempt_number,
        service_uid=os.geteuid(),
    ).read()
    assert first.attestation_digest == attestation.attestation_digest
    assert dict(first.gb10_boot_ids) == dict(bindings.gb10_boot_ids)
    assert first.gb10_mount_digest == bindings.gb10_mount_digest
    assert first.gb10_unit_digest == bindings.gb10_unit_digest

    ssh = _FixedRetirementSSH(dict(first.gb10_boot_ids))
    transport = FixedGB10SSHTransport(
        targets=tuple(
            GB10TransportTarget(
                ssh_target=host,
                repo_path=None,
                env_file_path=None,
                node_agent_service="loom-gb10-node-agent.service",
                retirement_only=True,
            )
            for host in HOSTS
        ),
        ssh_config=tmp_path / "ssh-config",
        identity=tmp_path / "identity",
        run=ssh,
        max_concurrency=4,
    )
    component = ProtectedGB10CandidateComponent(
        transport=transport,
        epoch_guard=lambda current: ComponentObservation(
            state=ComponentState.EXACT,
            evidence_digest="a" * 64,
            observed_epoch=current.starting_mutation_epoch + 1,
        ),
    )
    protected_state = tmp_path / "protected-state"
    first_attempt_root = (
        protected_state / "requests" / first.request_id / "attempts" / str(first.attempt_number)
    )
    first_attempt_root.mkdir(parents=True, mode=0o700)
    first_result = ProtectedApplyJournal(
        protected_state,
        request_id=first.request_id,
        attempt_number=first.attempt_number,
        service_uid=os.geteuid(),
    ).execute(first, (component.component(first),))
    assert first_result["gb10-candidate"].applied is True
    assert ssh.applies == len(HOSTS)
    assert ssh.retired_hosts == set(HOSTS)

    retry_broker_preparation = AdmissionPreparationLifecycle(
        prepare=lambda _candidate: producer.publish(3),
        now=lambda: NOW + timedelta(seconds=2),
    )
    retry_broker_preparation.prepare_admission(candidate)
    retry_plan = _plan(_checks(producer, monkeypatch, publication=publication))
    retry_executions = _execute_external_dag(retry_plan)
    retry_bindings = derive_attestation_bindings(
        retry_plan.context,
        retry_executions,
        backup_lease=lease,
    )
    retry_attestation = PreflightAttestation.issue(
        bindings=retry_bindings,
        executions=retry_executions,
        issued_at=NOW + timedelta(seconds=2),
        registry_digest=retry_plan.registry.registry_digest,
        coverage_digest=retry_plan.registry.coverage_digest,
    )
    store.publish(retry_attestation)
    assert retry_attestation.attestation_digest != attestation.attestation_digest

    retry_worker_preparation = AdmissionPreparationLifecycle(
        prepare=lambda _candidate: producer.publish(4),
        now=lambda: NOW + timedelta(seconds=3),
    )
    retry_worker_preparation.prepare_admission(candidate)
    retry_worker_preparation.require_fresh(candidate)
    retry_admission = validate_final_attestation(
        attestation=store.read(retry_attestation.attestation_digest),
        candidate=candidate,
        plan=retry_plan,
        current_mutation_epoch=7,
        now=NOW + timedelta(seconds=3),
    )
    retry_refreshed = retry_admission.tier0_executions

    receipt_rounds = (
        _receipt_identity(executions),
        _receipt_identity(admission.tier0_executions),
        _receipt_identity(retry_executions),
        _receipt_identity(retry_refreshed),
    )
    assert tuple(generation for generation, _digest in receipt_rounds) == (1, 2, 3, 4)
    assert len({digest for _generation, digest in receipt_rounds}) == 4
    stable_rounds = (
        _stable_external_identity(executions),
        _stable_external_identity(admission.tier0_executions),
        _stable_external_identity(retry_executions),
        _stable_external_identity(retry_refreshed),
    )
    assert stable_rounds == (stable_rounds[0],) * 4
    assert retry_bindings.gb10_mount_digest == bindings.gb10_mount_digest
    assert retry_bindings.gb10_unit_digest == bindings.gb10_unit_digest
    assert dict(retry_bindings.gb10_boot_ids) == dict(bindings.gb10_boot_ids)

    retry_rehearsal = PreflightRehearsal.from_executions(
        registry_digest=retry_attestation.registry_digest,
        coverage_digest=retry_attestation.coverage_digest,
        executions=retry_executions,
    )
    retry_source = _final_gate_source(
        tmp_path=tmp_path,
        artifact_store=artifact_store,
        rehearsal=retry_rehearsal,
        lease=lease,
    )
    retry_envelope = replace(
        envelope,
        attempt_number=2,
        resume=True,
        preflight_attestation_sha256=retry_attestation.attestation_digest,
        preflight_registry_sha256=retry_attestation.registry_digest,
        preflight_coverage_sha256=retry_attestation.coverage_digest,
    )
    retry_plan_root = tmp_path / "final-gate-state/requests/req-alpha/attempts/2"
    retry_plan_root.mkdir(parents=True, mode=0o700)
    retry_source(retry_envelope, retry_attestation, 7, retry_admission)
    second = FinalGatePlanStore(
        tmp_path / "final-gate-state",
        request_id=retry_envelope.request_id,
        attempt_number=retry_envelope.attempt_number,
        service_uid=os.geteuid(),
    ).read()
    assert second.attestation_digest == retry_attestation.attestation_digest
    assert dict(second.gb10_boot_ids) == dict(retry_bindings.gb10_boot_ids)
    assert second.gb10_mount_digest == retry_bindings.gb10_mount_digest
    assert second.gb10_unit_digest == retry_bindings.gb10_unit_digest
    assert second.plan_digest != first.plan_digest
    assert _retirement_identity(
        first,
        (
            "loom-gb10-worker.service",
            "loom-gb10-node-agent.service",
            "loom-gb10-node-agent.timer",
        ),
    ) == _retirement_identity(
        second,
        (
            "loom-gb10-worker.service",
            "loom-gb10-node-agent.service",
            "loom-gb10-node-agent.timer",
        ),
    )
    second_attempt_root = (
        protected_state / "requests" / second.request_id / "attempts" / str(second.attempt_number)
    )
    second_attempt_root.mkdir(parents=True, mode=0o700)
    second_result = ProtectedApplyJournal(
        protected_state,
        request_id=second.request_id,
        attempt_number=second.attempt_number,
        service_uid=os.geteuid(),
    ).execute(second, (component.component(second),))
    assert second_result["gb10-candidate"].applied is False
    assert ssh.applies == len(HOSTS)
    assert ssh.observations >= 3 * len(HOSTS)
    assert producer.generation == 4


@pytest.mark.parametrize("drift", ("mount", "source", "boot"))
def test_external_profile_contract_rejects_stable_authority_drift(
    drift: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = _InfrastructureProducer(tmp_path / drift, monkeypatch)
    producer.publish(1, drift=drift)
    plan = _plan(_checks(producer, monkeypatch))

    results = {execution.check_id: execution for execution in _execute_external_dag(plan)}

    assert not results["gb10.shared-mount"].passed
    assert not results["gb10.candidate-source"].passed
    assert not results["gb10.host-readiness"].passed
