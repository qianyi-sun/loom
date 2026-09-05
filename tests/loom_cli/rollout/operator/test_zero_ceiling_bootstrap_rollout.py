from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

from loom_cli.rollout.operator.deep_preflight_authority import RuntimePurpose
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan
from loom_cli.rollout.operator.installed_execution_prerequisite import (
    InstalledExecutionPrerequisitePublisherFactory,
)
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.preflight_bindings import derive_attestation_bindings
from loom_cli.rollout.preflight_contract import (
    CheckExecution,
    PreflightAttestation,
    PreflightDag,
)
from loom_cli.rollout.preflight_registered_checks import (
    build_execution_prerequisite_check,
)
from loom_cli.rollout.systemd_unit_readiness import UNIT_PATHS
from tests.loom_cli.rollout.operator.test_final_gate_plan import (
    _artifacts,
    _baseline,
    _envelope,
)
from tests.loom_cli.rollout.operator.test_installed_execution_prerequisite import (
    _candidate,
    _images,
)
from tests.loom_cli.rollout.operator.test_protected_execution_prerequisite_source import (
    _source_fixture,
)
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_runtime import (
    _runtime,
    _write_bootstrap,
)
from tests.loom_cli.rollout.test_preflight_bindings import (
    NOW,
    _context,
    _executions,
    _schema_three_lease,
)


def _evidence(
    executions: tuple[CheckExecution, ...],
    check_id: str,
) -> dict[str, object]:
    return dict(next(item for item in executions if item.check_id == check_id).evidence)


def test_legacy_manager_bootstrap_admits_only_frozen_foundation_components(
    tmp_path: Path,
) -> None:
    """Catch first-rollout bootstrap accidentally admitting executable capacity."""
    source = _source_fixture(tmp_path / "source")
    candidate = _candidate(source.plan)
    images = _images(candidate_sha=candidate.resolved_sha)
    loaded = SimpleNamespace(
        images=images,
        publication=SimpleNamespace(
            bundle_digest=source.plan.artifact_bundle_digest,
            candidate_sha=candidate.resolved_sha,
            candidate_tree=candidate.resolved_tree,
            mutation_epoch=source.plan.starting_mutation_epoch,
            container_registry="registry.example",
        ),
    )
    capacity_runtime_root = tmp_path / "capacity-runtime"
    capacity_runtime_root.mkdir()
    capacity_runtime = _runtime(capacity_runtime_root)
    _write_bootstrap(capacity_runtime)
    assert not capacity_runtime.credential_seed_path.exists()

    manager_route_calls = 0

    def legacy_manager_configuration() -> dict[str, object]:
        nonlocal manager_route_calls
        manager_route_calls += 1
        raise RuntimeError("legacy manager has no /v1/configuration route")

    publisher_factory = InstalledExecutionPrerequisitePublisherFactory(
        store=source.store,
        container_registry="registry.example",
        manager_configuration_source=legacy_manager_configuration,
        configuration_seed_source=capacity_runtime.read_credential_seed,
        staging_protected_admission_source=(
            lambda _candidate, _bundle, _epoch, _seed: source.protected_admission
        ),
        authority_source_factory=(
            lambda _candidate, _image: lambda _desired: source.authority
        ),
        now=source.source.now,
        zero_ceiling_bootstrap_authority_source=(
            capacity_runtime.zero_ceiling_bootstrap_authority
        ),
    )
    publisher = publisher_factory(
        candidate,
        source.plan.starting_mutation_epoch,
        RuntimePurpose.DETACHED_REHEARSAL,
        loaded,  # type: ignore[arg-type]
    )
    lease = _schema_three_lease()
    prerequisite_check = build_execution_prerequisite_check(
        lambda found_lease: publisher(found_lease, images),
        lease=lease,
        candidate_sha=candidate.resolved_sha,
        candidate_tree=candidate.resolved_tree or "",
        mutation_epoch=source.plan.starting_mutation_epoch,
    )
    prerequisite_execution = PreflightDag(
        (prerequisite_check,),
        attested_dependencies=frozenset(prerequisite_check.spec.dependencies),
    ).run(_context(), now=lambda: NOW)[0]

    assert prerequisite_execution.passed
    assert prerequisite_execution.evidence["mode"] == "zero-ceiling-bootstrap"
    assert manager_route_calls == 1
    assert not source.store.state_root.exists()

    artifacts = _artifacts(tmp_path / "final")
    base_executions = _executions(include_execution_prerequisites=False)
    executions = (
        *(
            replace(
                execution,
                evidence=MappingProxyType(
                    {
                        "image-digests": {
                            "browser": f"sha256:{'e' * 64}",
                            "loom-control-plane": artifacts.migration_image_id,
                        }
                    }
                ),
            )
            if execution.check_id == "images.contract"
            else execution
            for execution in base_executions
        ),
        prerequisite_execution,
    )
    bindings = derive_attestation_bindings(
        _context(),
        executions,
        backup_lease=lease,
    )
    attestation = PreflightAttestation.issue(
        bindings=bindings,
        executions=executions,
        issued_at=NOW.replace(minute=NOW.minute + 1),
        registry_digest="9" * 64,
        coverage_digest="a" * 64,
    )

    assert lease.manager_execution_state == "shadow"
    assert lease.manager_execution_epoch == 0
    assert lease.manager_execution_manifest_sha256 is None
    assert lease.manager_executable_new_capacity_ceiling == 0
    assert lease.manager_increase_freeze is True
    assert bindings.execution_prerequisite_schema_version is None
    assert attestation.schema_version == 4

    systemd_evidence = _evidence(executions, "systemd.render")
    unit_digests = {
        **{path: "1" * 64 for path in UNIT_PATHS},
        **dict(systemd_evidence["supervisor-unit-digests"]),  # type: ignore[arg-type]
    }
    systemd_evidence.update(
        {
            "failed-units": {},
            "unit-count": len(unit_digests),
            "unit-digests": unit_digests,
            "unit-set-digest": hashlib.sha256(
                json.dumps(
                    {"failed": {}, "units": unit_digests},
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest(),
        }
    )
    predecessor_evidence = {
        **_evidence(executions, "external-supervisor.predecessor"),
        "pool-identity-digest": "b" * 64,
    }
    artifacts = replace(
        artifacts,
        migration_plan_sha256=bindings.migration_plan_digest,
        migration_target_revision=bindings.schema_revision,
        browser_report_schema_sha256=bindings.browser_report_schema,
    )
    envelope = replace(
        _envelope(attestation),
        backup_manifest_sha256=lease.manifest_sha256,
        runner_config_sha256=bindings.runner_config_hash,
        preflight_attestation_sha256=attestation.attestation_digest,
        preflight_registry_sha256=attestation.registry_digest,
        preflight_coverage_sha256=attestation.coverage_digest,
    )
    plan = FinalGatePlan.build(
        envelope,
        attestation,
        artifacts,
        lease,
        _baseline(),
        systemd_evidence,
        predecessor_evidence,
    )
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    component_ids = tuple(
        component.component_id
        for component in capacity_runtime.components(
            plan,
            epoch_guard=lambda _plan: epoch,
        )
    )

    assert plan.schema_version == 6
    assert component_ids == (
        "staging-capacity-credentials",
        "staging-capacity-database",
        "staging-protected-runtime-secret",
        "capacity-manager-runtime",
        "capacity-manager-configuration",
        "staging-capacity-agent",
    )
    assert not {
        "oldlab-controller-prerequisite",
        "gb10-controller-prerequisite",
        "staging-capacity-execution-credentials",
        "capacity-execution-preparation",
    } & set(component_ids)
