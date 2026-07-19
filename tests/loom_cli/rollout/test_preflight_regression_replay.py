from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.browser_runtime_readiness import browser_report_schema_digest
from loom_cli.rollout.gb10_readiness import GB10ProbeTarget
from loom_cli.rollout.image_readiness import (
    ALL_BUILD_IMAGES,
    ImageArtifactSet,
    ImageDescriptor,
)
from loom_cli.rollout.preflight_contract import (
    CheckContext,
    CheckOperation,
    CheckProbe,
    RegisteredCheck,
)
from loom_cli.rollout.preflight_registered_checks import (
    build_browser_runtime_check,
    build_capacity_high_water_check,
    build_gb10_candidate_source_check,
    build_gb10_host_readiness_check,
    build_rehearsal_checks,
    build_staging_baseline_checks,
    build_systemd_user_manager_check,
    gb10_target_inventory_digest,
)
from loom_cli.rollout.preflight_regressions import (
    RegressionReplayCase,
    replay_regression_manifest,
)
from loom_cli.rollout.readonly_authority import readonly_authority_policy_digest
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS, RehearsalResult
from loom_cli.rollout.staging_baseline_readiness import BaselineProbeResult

_CANDIDATE = "a" * 40
_EPOCH = 8
_ROUTE = "https://staging.example.test/dev"


def _images() -> ImageArtifactSet:
    return ImageArtifactSet(
        descriptors={
            name: ImageDescriptor(
                image_id="sha256:" + f"{index + 1:064x}",
                revision=_CANDIDATE,
                os="linux",
                architecture="amd64",
                entrypoint=(
                    ("node", "/opt/loom/web/scripts/staging-admin-browser-smoke.mjs")
                    if name == "loom-staging-admin-browser-smoke"
                    else ()
                ),
            )
            for index, (name, _dockerfile) in enumerate(ALL_BUILD_IMAGES)
        },
        plan_digest="1" * 64,
        artifact_digest="2" * 64,
    )


def _gb10_payload() -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "boot_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            "manager_version": "255.4-1ubuntu8.14",
            "linger_enabled": True,
            "timer_enabled": True,
            "service": {
                "LoadState": "loaded",
                "Type": "oneshot",
                "Result": "success",
                "ExecMainStatus": "0",
                "ActiveState": "inactive",
                "SubState": "dead",
                "NeedDaemonReload": "no",
            },
            "timer": {
                "LoadState": "loaded",
                "ActiveState": "active",
                "SubState": "running",
                "Unit": "loom-gb10-node-agent.service",
                "NeedDaemonReload": "no",
            },
        }
    )


def _baseline_result(check_id: str) -> BaselineProbeResult:
    return BaselineProbeResult(
        check_id=check_id,
        environment="staging",
        namespace="loom-staging",
        route=_ROUTE,
        readonly_principal="loom-rollout-readonly",
        observed_mutation_epoch=_EPOCH,
        resource_digest="3" * 64,
        blockers={"baseline": "historical-drift"} if check_id == "staging.storage-db" else {},
    )


def _rehearsal_result(check_id: str) -> RehearsalResult:
    return RehearsalResult(
        check_id=check_id,
        isolation_id="rehearsal-" + "4" * 24,
        candidate_sha=_CANDIDATE,
        mutation_epoch=_EPOCH,
        evidence_digest="5" * 64,
        journal_digest="6" * 64,
        protected_mutation=False,
        cleanup_verified=check_id == "rehearsal.cleanup",
        blockers=(
            {"binding": "candidate-route-mismatch"}
            if check_id in {"rehearsal.api-smoke", "rehearsal.browser"}
            else {}
        ),
    )


def _cases(tmp_path: Path) -> tuple[RegressionReplayCase, ...]:
    repo_root = Path(__file__).resolve().parents[3]
    config_digest = "7" * 64
    capacity = build_capacity_high_water_check(
        lambda: StagingCapacity(
            object_count=10_000_000,
            bytes_used=10**15,
            disk_free_percent=1,
            inode_free_percent=1,
        )
    )

    clock = iter((0.0, 6.0))
    outputs = iter(
        (
            "255.4-1ubuntu8.14\n",
            "yes\n",
            "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee\n",
        )
    )
    systemd = build_systemd_user_manager_check(
        lambda argv: subprocess.CompletedProcess(argv, 0, next(outputs), ""),
        service_uid=1001,
        monotonic=lambda: next(clock),
    )

    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    gb10 = build_gb10_host_readiness_check(
        lambda argv: subprocess.CompletedProcess(argv, 0, _gb10_payload(), ""),
        targets=(target,),
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        settle_attempts=2,
        settle_interval_seconds=0,
    )
    candidate_source = build_gb10_candidate_source_check(
        lambda argv: subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "candidate_sha": _CANDIDATE,
                    "candidate_tree": "b" * 40,
                    "unit_sha256": {},
                }
            ),
            "",
        ),
        targets=(target,),
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        candidate_root=repo_root,
        expected_candidate_sha=_CANDIDATE,
        expected_candidate_tree="c" * 40,
        image_tag="staging-aaaaaaa",
    )

    browser = build_browser_runtime_check(
        lambda argv: subprocess.CompletedProcess(argv, 1, "", ""),
        _images,
        token_path=tmp_path / "missing-token",
        service_uid=1001,
        service_gid=1001,
        expected_candidate_sha=_CANDIDATE,
        expected_source_set_digest="8" * 64,
    )

    probes = {
        check_id: lambda check_id=check_id: _baseline_result(check_id)
        for check_id in (
            "staging.health",
            "staging.auth",
            "staging.catalog-task",
            "staging.storage-db",
            "staging.network",
        )
    }
    baseline_checks = build_staging_baseline_checks(
        probes,
        environment="staging",
        namespace="loom-staging",
        route=_ROUTE,
        mutation_epoch=_EPOCH,
    )

    rehearsal_actions = {
        check_id: lambda check_id=check_id: _rehearsal_result(check_id)
        for check_id in REHEARSAL_CHECK_IDS
    }
    rehearsal_checks = {
        check.spec.check_id: check
        for check in build_rehearsal_checks(
            rehearsal_actions,
            isolation_id="rehearsal-" + "4" * 24,
            candidate_sha=_CANDIDATE,
            mutation_epoch=_EPOCH,
            checkpoint_evidence_digest="9" * 64,
            rehearsal_plan_digest="a" * 64,
        )
    }
    baseline_context = CheckContext(
        {
            "environment": "staging",
            "namespace": "loom-staging",
            "readonly.principal.sha256": readonly_authority_policy_digest(),
            "route": _ROUTE,
            "staging.mutation-epoch": _EPOCH,
        }
    )
    for check in baseline_checks:
        if check.spec.check_id != "staging.release-baseline":
            check.operations[CheckOperation.PROBE](baseline_context)
    baseline = next(
        check for check in baseline_checks if check.spec.check_id == "staging.release-baseline"
    )
    rehearsal_context = CheckContext(
        {
            "candidate.sha": _CANDIDATE,
            "checkpoint.evidence.sha256": "9" * 64,
            "rehearsal.plan.sha256": "a" * 64,
            "staging.mutation-epoch": _EPOCH,
        }
    )
    return (
        RegressionReplayCase(
            "browser-token-authority-mismatch",
            browser,
            CheckContext(
                {
                    "browser.report-schema.sha256": browser_report_schema_digest(),
                    "candidate.sha": _CANDIDATE,
                    "protected-inputs.sha256": "8" * 64,
                }
            ),
        ),
        RegressionReplayCase(
            "gb10-timer-transient-state",
            gb10,
            CheckContext(
                {
                    "gb10.inventory-digest": gb10_target_inventory_digest((target,)),
                    "runner.config.sha256": config_digest,
                }
            ),
        ),
        RegressionReplayCase(
            "gb10-candidate-source-drift",
            candidate_source,
            CheckContext(
                {
                    "candidate.sha": _CANDIDATE,
                    "candidate.tree": "c" * 40,
                    "gb10.inventory-digest": gb10_target_inventory_digest((target,)),
                    "runner.config.sha256": config_digest,
                }
            ),
        ),
        RegressionReplayCase(
            "systemd-user-manager-latency",
            systemd,
            CheckContext({"runner.config.sha256": config_digest, "service.uid": 1001}),
        ),
        RegressionReplayCase(
            "backup-object-inode-growth",
            capacity,
            CheckContext(
                {
                    "capacity.policy.sha256": staging_capacity_policy_digest(),
                    "runner.config.sha256": config_digest,
                }
            ),
        ),
        RegressionReplayCase("release-baseline-drift", baseline, baseline_context),
        RegressionReplayCase(
            "candidate-api-smoke-binding",
            rehearsal_checks["rehearsal.api-smoke"],
            rehearsal_context,
        ),
        RegressionReplayCase(
            "candidate-browser-binding",
            rehearsal_checks["rehearsal.browser"],
            rehearsal_context,
        ),
    )


def test_all_historical_blockers_replay_through_production_checks(tmp_path: Path) -> None:
    evidence = replay_regression_manifest(_cases(tmp_path))

    assert len(evidence.implementation_digests) == 8
    assert set(evidence.implementation_digests) == set(evidence.evidence_hashes)


def test_replay_rejects_missing_fixture_or_a_fault_that_now_passes(tmp_path: Path) -> None:
    cases = _cases(tmp_path)
    with pytest.raises(ValueError, match="coverage is incomplete"):
        replay_regression_manifest(cases[:-1])

    first = cases[0]
    failed_probe = first.check.operations[CheckOperation.PROBE](first.context)
    passing = RegisteredCheck(
        spec=first.check.spec,
        implementation_version=first.check.implementation_version,
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence=failed_probe.evidence,
            )
        },
    )
    changed = (replace(first, check=passing), *cases[1:])
    with pytest.raises(ValueError, match="no longer fails at preflight"):
        replay_regression_manifest(changed)
