from __future__ import annotations

import json
import os
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import staging_external_slurm_acceptance_authority as external_authority

from loom_cli.external_slurm_acceptance import load_authority_config
from loom_cli.rollout.gb10_convergence import (
    GB10ConvergenceState,
    GB10FleetCandidateObservation,
    GB10HostCandidateObservation,
    GB10MutationKind,
)
from loom_cli.rollout.gb10_readiness import GB10ProbeTarget
from loom_cli.rollout.operator.deep_preflight_authority import (
    AdmissionPreparationLifecycle,
    DeepPreflightAuthority,
)
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyJournal,
)
from loom_cli.rollout.operator.protected_gb10_component import (
    ProtectedGB10CandidateComponent,
)
from loom_cli.rollout.operator.protected_gb10_transport import _retirement_identity
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
from loom_cli.rollout.preflight_registered_checks import (
    build_external_gb10_stage_boundary_checks,
    gb10_target_inventory_digest,
)
from loom_cli.rollout.preflight_registry import PreflightRegistry
from tests.loom_cli.rollout.operator.test_protected_migration_component import (
    _published_plan,
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
    binding_support = tuple(
        _passing_check(check_id, evidence) for check_id, evidence in canned.items()
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


class _WorkerSources:
    def __init__(self, plan: CandidatePreflightPlan) -> None:
        self.candidate = plan.candidate
        self.loaded_artifacts = None
        self._plan = plan

    def build(self, *, mutation_epoch: int):
        assert mutation_epoch == 7
        return SimpleNamespace(prebackup_plan=lambda candidate: self._checked_plan(candidate))

    def _checked_plan(self, candidate):
        assert candidate == self.candidate
        return self._plan


class _RetirementFleet:
    def __init__(self) -> None:
        self.exact = False
        self.mutations = 0

    def observe(self, plan):
        return GB10FleetCandidateObservation(
            hosts={
                host: GB10HostCandidateObservation(
                    host=host,
                    boot_id=boot_id,
                    baseline_ready=True,
                    candidate_source_exact=True,
                    checkout_exact=True,
                    environment_exact=True,
                    units_exact=True,
                    legacy_absent=self.exact,
                    service_timer_exact=self.exact,
                    evidence_digest=f"{index:064x}",
                )
                for index, (host, boot_id) in enumerate(
                    sorted(plan.gb10_boot_ids.items()),
                    1,
                )
            },
            candidate_source_digest=plan.gb10_unit_digest,
        )

    def apply(self, _plan, convergence):
        assert convergence.state is GB10ConvergenceState.READY
        assert all(
            mutation.operations == (GB10MutationKind.LEGACY_RETIRE, GB10MutationKind.SERVICE_TIMER)
            for mutation in convergence.mutations
        )
        self.mutations += 1
        self.exact = True


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
    producer = _InfrastructureProducer(tmp_path / "producer", monkeypatch)
    broker_preparation = AdmissionPreparationLifecycle(
        prepare=lambda _candidate: producer.publish(1),
        now=lambda: NOW,
    )
    candidate = _candidate()
    broker_preparation.prepare_admission(candidate)

    plan = _plan(_checks(producer, monkeypatch))
    executions = _execute_external_dag(plan)
    trio = _external_evidence(executions)
    assert all(execution.passed for execution in trio.values())
    assert trio["gb10.host-readiness"].evidence["boot-ids"] == producer.summary["boot_ids"]  # type: ignore[index]

    bindings = derive_attestation_bindings(plan.context, executions)
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

    worker_preparations: list[int] = []

    def refresh_for_worker(_candidate) -> None:
        worker_preparations.append(2)
        producer.publish(2)

    worker = DeepPreflightAuthority(
        sources_factory=lambda _candidate, _epoch, _purpose: _WorkerSources(plan),  # type: ignore[arg-type]
        attestation_store=store,
        read_mutation_epoch=lambda: 7,
        now=lambda: NOW + timedelta(seconds=1),
        admission_preparation=AdmissionPreparationLifecycle(
            prepare=refresh_for_worker,
            now=lambda: NOW + timedelta(seconds=1),
        ),
    )
    admission = worker.admit_final(
        candidate,
        attestation_digest=attestation.attestation_digest,
        expected_registry_digest=plan.registry.registry_digest,
        expected_coverage_digest=plan.registry.coverage_digest,
    )
    refreshed = {execution.check_id: execution for execution in admission.tier0_executions}
    assert worker_preparations == [2]
    assert all(refreshed[check_id].evidence["receipt-generation"] == 2 for check_id in trio)
    assert refreshed["gb10.shared-mount"].evidence["mount-digest"] == bindings.gb10_mount_digest
    assert refreshed["gb10.candidate-source"].evidence["source-digest"] == bindings.gb10_unit_digest
    assert refreshed["gb10.host-readiness"].evidence["boot-ids"] == dict(bindings.gb10_boot_ids)

    retry_broker_preparation = AdmissionPreparationLifecycle(
        prepare=lambda _candidate: producer.publish(3),
        now=lambda: NOW + timedelta(seconds=2),
    )
    retry_broker_preparation.prepare_admission(candidate)
    retry_plan = _plan(_checks(producer, monkeypatch))
    retry_executions = _execute_external_dag(retry_plan)
    retry_bindings = derive_attestation_bindings(
        retry_plan.context,
        retry_executions,
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

    retry_worker_preparations: list[int] = []

    def refresh_for_retry_worker(_candidate) -> None:
        retry_worker_preparations.append(4)
        producer.publish(4)

    retry_worker = DeepPreflightAuthority(
        sources_factory=lambda _candidate, _epoch, _purpose: _WorkerSources(  # type: ignore[arg-type]
            retry_plan
        ),
        attestation_store=store,
        read_mutation_epoch=lambda: 7,
        now=lambda: NOW + timedelta(seconds=3),
        admission_preparation=AdmissionPreparationLifecycle(
            prepare=refresh_for_retry_worker,
            now=lambda: NOW + timedelta(seconds=3),
        ),
    )
    retry_admission = retry_worker.admit_final(
        candidate,
        attestation_digest=retry_attestation.attestation_digest,
        expected_registry_digest=retry_plan.registry.registry_digest,
        expected_coverage_digest=retry_plan.registry.coverage_digest,
    )
    retry_refreshed = retry_admission.tier0_executions
    assert retry_worker_preparations == [4]

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

    first = replace(
        _published_plan(tmp_path),
        attestation_digest=attestation.attestation_digest,
        gb10_inventory_digest=bindings.gb10_inventory_digest,
        gb10_boot_ids=dict(bindings.gb10_boot_ids),
        gb10_mount_digest=bindings.gb10_mount_digest,
        gb10_unit_digest=bindings.gb10_unit_digest,
    )
    fleet = _RetirementFleet()
    component = ProtectedGB10CandidateComponent(
        transport=fleet,
        epoch_guard=lambda current: ComponentObservation(
            state=ComponentState.EXACT,
            evidence_digest="a" * 64,
            observed_epoch=current.starting_mutation_epoch + 1,
        ),
    )
    first_terminal = ProtectedApplyJournal(
        tmp_path / "state",
        request_id=first.request_id,
        attempt_number=first.attempt_number,
        service_uid=os.geteuid(),
    )
    first_attempt_root = (
        tmp_path / "state/requests" / first.request_id / "attempts" / str(first.attempt_number)
    )
    first_attempt_root.mkdir(parents=True, mode=0o700)
    first_result = first_terminal.execute(first, (component.component(first),))
    assert first_result["gb10-candidate"].applied is True
    assert fleet.mutations == 1

    second = replace(
        first,
        attempt_number=first.attempt_number + 1,
        attestation_digest=retry_attestation.attestation_digest,
        gb10_inventory_digest=retry_bindings.gb10_inventory_digest,
        gb10_boot_ids=dict(retry_bindings.gb10_boot_ids),
        gb10_mount_digest=retry_bindings.gb10_mount_digest,
        gb10_unit_digest=retry_bindings.gb10_unit_digest,
        plan_digest="f" * 64,
    )
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
        tmp_path / "state/requests" / second.request_id / "attempts" / str(second.attempt_number)
    )
    second_attempt_root.mkdir(parents=True, mode=0o700)
    second_result = ProtectedApplyJournal(
        tmp_path / "state",
        request_id=second.request_id,
        attempt_number=second.attempt_number,
        service_uid=os.geteuid(),
    ).execute(second, (component.component(second),))
    assert second_result["gb10-candidate"].applied is False
    assert fleet.mutations == 1
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
